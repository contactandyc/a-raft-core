// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define _GNU_SOURCE
#include "a-raft-library/araft.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

typedef struct {
    uv_write_t req;
    araft_net_header_t header;
} write_req_t;

static void alloc_cb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    buf->base = malloc(suggested_size);
    buf->len = suggested_size;
}

static void on_write_done(uv_write_t *req, int status) {
    write_req_t *wr = (write_req_t *)req;
    free(wr);
}

static void router_send_rpc(peer_connection_t* peer, uint64_t group_id, uint8_t type, void* payload, uint32_t len) {
    if (peer->server->network_isolated) return; // --- CHAOS: Drop outgoing ---

    write_req_t *wr = malloc(sizeof(write_req_t));
    wr->header.payload_len = len;
    wr->header.type = type;
    wr->header.group_id = group_id;
    wr->header.sender_id = peer->server->physical_node_id;

    uv_buf_t bufs[2];
    bufs[0] = uv_buf_init((char*)&wr->header, sizeof(araft_net_header_t));
    bufs[1] = uv_buf_init((char*)payload, len);
    uv_write(&wr->req, (uv_stream_t*)&peer->handle, bufs, 2, on_write_done);
}

static void defer_msg_internal(physical_server_t* server, uint64_t group_id, uint64_t target_node_id, uint8_t type, void* payload, uint32_t len) {
    pthread_mutex_lock(&server->queue_mutex);
    if (server->deferred_msg_count >= ARAFT_MAX_DEFERRED_MSGS) {
        pthread_mutex_unlock(&server->queue_mutex);
        return;
    }
    deferred_msg_t* msg = &server->deferred_msgs[server->deferred_msg_count++];
    msg->target_group_id = group_id;
    msg->target_node_id = target_node_id;
    msg->type = type;
    msg->payload_len = len;
    msg->payload = malloc(len);
    if (msg->payload) memcpy(msg->payload, payload, len);
    pthread_mutex_unlock(&server->queue_mutex);
}

static void defer_broadcast_rpc(physical_server_t* server, uint64_t group_id, uint8_t type, void* payload, uint32_t len) {
    defer_msg_internal(server, group_id, 0, type, payload, len);
}

static void defer_target_rpc(physical_server_t* server, uint64_t group_id, uint64_t target_node_id, uint8_t type, void* payload, uint32_t len) {
    defer_msg_internal(server, group_id, target_node_id, type, payload, len);
}

static void on_event_loop_check(uv_check_t* handle) {
    physical_server_t* server = (physical_server_t*)handle->data;

    if (server->metadata_is_dirty) {
        pwrite(server->meta_fd, server->meta_ram_array, server->meta_block_size, 0);
#ifdef __APPLE__
        fcntl(server->meta_fd, F_FULLFSYNC, 0);
#else
        fdatasync(server->meta_fd);
#endif
        server->metadata_is_dirty = false;
    }

    pthread_mutex_lock(&server->queue_mutex);
    for (uint32_t i = 0; i < server->deferred_msg_count; i++) {
        deferred_msg_t* msg = &server->deferred_msgs[i];
        if (msg->payload) {
            for (uint32_t p = 0; p < server->active_peer_count; p++) {
                if (msg->target_node_id == 0 || server->active_peers[p]->remote_node_id == msg->target_node_id) {
                    router_send_rpc(server->active_peers[p], msg->target_group_id, msg->type, msg->payload, msg->payload_len);
                }
            }
            free(msg->payload);
            msg->payload = NULL;
        }
    }
    server->deferred_msg_count = 0;
    pthread_mutex_unlock(&server->queue_mutex);
}

static raft_peer_state_t* get_peer_state(araft_node_t* node, uint64_t node_id) {
    for (uint32_t i = 0; i < node->peer_state_count; i++) {
        if (node->peer_state[i].node_id == node_id) return &node->peer_state[i];
    }
    if (node->peer_state_count < ARAFT_MAX_PEERS) {
        raft_peer_state_t* p = &node->peer_state[node->peer_state_count++];
        p->node_id = node_id;
        p->next_index = node->last_log_index + 1;
        p->match_index = 0;
        return p;
    }
    return NULL;
}

static void araft_reset_election_timer(araft_node_t* node) {
    uv_timer_stop(&node->election_timer);
    uint64_t timeout = 150 + (rand() % 150);
    uv_timer_start(&node->election_timer, (uv_timer_cb)araft_node_start, timeout, 0);
}

static void check_term(araft_node_t* node, uint64_t message_term) {
    if (message_term > node->meta->current_term) {
        node->meta->current_term = message_term;
        node->meta->voted_for = 0;
        node->state = ARAFT_STATE_FOLLOWER;
        node->server->metadata_is_dirty = true;
        uv_timer_stop(&node->heartbeat_timer);
    }
}

static void araft_step(araft_node_t* node, uint64_t sender_id, uint8_t msg_type, uint8_t* payload, uint32_t len) {
    if (msg_type == MSG_REQUEST_VOTE) {
        msg_request_vote_t* rpc = (msg_request_vote_t*)payload;
        check_term(node, rpc->term);
        msg_request_vote_res_t res = { .term = node->meta->current_term, .vote_granted = 0 };

        if (rpc->term >= node->meta->current_term && (node->meta->voted_for == 0 || node->meta->voted_for == rpc->candidate_id)) {
            uint64_t my_last_log_term = 0;
            if (node->last_log_index > 0) {
                uint8_t* p_p; uint32_t p_l;
                if (awal_read_entry(&node->wal, node->last_log_index, &my_last_log_term, &p_p, &p_l)) {
                    if (p_p) free(p_p);
                }
            }

            bool log_ok = (rpc->last_log_term > my_last_log_term) ||
                          (rpc->last_log_term == my_last_log_term && rpc->last_log_index >= node->last_log_index);

            if (log_ok) {
                node->meta->voted_for = rpc->candidate_id;
                res.vote_granted = 1;
                node->server->metadata_is_dirty = true;
                araft_reset_election_timer(node);
            }
        }
        defer_target_rpc(node->server, node->group_id, sender_id, MSG_REQUEST_VOTE_RES, &res, sizeof(res));
    }
    else if (msg_type == MSG_REQUEST_VOTE_RES && node->state == ARAFT_STATE_CANDIDATE) {
        msg_request_vote_res_t* rpc = (msg_request_vote_res_t*)payload;
        check_term(node, rpc->term);

        if (node->state == ARAFT_STATE_CANDIDATE && rpc->term == node->meta->current_term && rpc->vote_granted) {
            node->votes_received++;
            uint32_t majority = (node->cluster_size / 2) + 1;

            if (node->votes_received >= majority) {
                printf("[Node %llu|Grp %llu] WON ELECTION! Leader for Term %llu\n",
                       node->server->physical_node_id, node->group_id, node->meta->current_term);
                node->state = ARAFT_STATE_LEADER;
                node->current_leader_id = node->server->physical_node_id;

                for (uint32_t i = 0; i < node->peer_state_count; i++) {
                    node->peer_state[i].next_index = node->last_log_index + 1;
                    node->peer_state[i].match_index = 0;
                }

                uv_timer_stop(&node->election_timer);
                uv_timer_start(&node->heartbeat_timer, (uv_timer_cb)araft_node_start, 50, 50);
                araft_node_start((araft_node_t*)&node->heartbeat_timer);
            }
        }
    }
    else if (msg_type == MSG_APPEND_ENTRIES) {
        msg_append_entries_t* rpc = (msg_append_entries_t*)payload;
        check_term(node, rpc->term);
        msg_append_entries_res_t res = { .term = node->meta->current_term, .success = 0, .match_index = 0 };

        if (rpc->term >= node->meta->current_term) {
            if (node->state == ARAFT_STATE_CANDIDATE) node->state = ARAFT_STATE_FOLLOWER;
            node->current_leader_id = rpc->leader_id;

            node->known_leader_commit = rpc->leader_commit;

            araft_reset_election_timer(node);

            bool log_ok = true;
            if (rpc->prev_log_index > node->last_log_index) {
                log_ok = false;
            } else if (rpc->prev_log_index > 0) {
                uint64_t prev_term = 0;
                uint8_t* p_p; uint32_t p_l;
                if (awal_read_entry(&node->wal, rpc->prev_log_index, &prev_term, &p_p, &p_l)) {
                    if (prev_term != rpc->prev_log_term) log_ok = false;
                    if (p_p) free(p_p);
                }
            }

            if (log_ok) {
                if (rpc->entry_count > 0) {
                    uint8_t* log_data = payload + sizeof(msg_append_entries_t);
                    uint32_t log_len = len - sizeof(msg_append_entries_t);

                    awal_append(&node->wal, rpc->term, rpc->prev_log_index + 1, log_data, log_len);
                    if (awal_flush_batch(&node->wal) > 0) {
                        node->last_log_index = rpc->prev_log_index + rpc->entry_count;
                    }
                }
                if (rpc->leader_commit > node->commit_index) {
                    node->commit_index = (rpc->leader_commit < node->last_log_index) ? rpc->leader_commit : node->last_log_index;
                }
                res.success = 1;
                res.match_index = node->last_log_index;
            }
        }
        defer_target_rpc(node->server, node->group_id, sender_id, MSG_APPEND_ENTRIES_RES, &res, sizeof(res));
    }
    else if (msg_type == MSG_APPEND_ENTRIES_RES && node->state == ARAFT_STATE_LEADER) {
        msg_append_entries_res_t* rpc = (msg_append_entries_res_t*)payload;
        check_term(node, rpc->term);
        raft_peer_state_t* peer = get_peer_state(node, sender_id);
        if (peer && rpc->term == node->meta->current_term) {
            if (rpc->success) {
                if (rpc->match_index > peer->match_index) {
                    peer->match_index = rpc->match_index;
                    peer->next_index = rpc->match_index + 1;

                    // In a 3 node cluster, if we (Leader) + 1 peer have it, we hit majority (2)
                    if (peer->match_index > node->commit_index) {
                        node->commit_index = peer->match_index;
                    }
                }
            } else {
                if (peer->next_index > 1) peer->next_index--;
            }
        }
    }
    else if (msg_type == MSG_CLIENT_FORWARD && node->state == ARAFT_STATE_LEADER) {
        msg_client_forward_t* rpc = (msg_client_forward_t*)payload;
        uint8_t* actual_data = payload + sizeof(msg_client_forward_t);
        uint32_t actual_len = len - sizeof(msg_client_forward_t);
        araft_propose(node, actual_data, actual_len);
        msg_client_forward_res_t res = { .request_id = rpc->request_id, .success = 1 };
        defer_target_rpc(node->server, node->group_id, sender_id, MSG_CLIENT_FORWARD_RES, &res, sizeof(res));
    }
    else if (msg_type == MSG_CLIENT_FORWARD_RES) {
        msg_client_forward_res_t* rpc = (msg_client_forward_res_t*)payload;
        if (node->server->forward_cb) node->server->forward_cb(rpc->request_id, rpc->success);
    }
}

static void remove_peer(physical_server_t* server, peer_connection_t* peer) {
    for (uint32_t i = 0; i < server->active_peer_count; i++) {
        if (server->active_peers[i] == peer) {
            server->active_peers[i] = server->active_peers[--server->active_peer_count];
            return;
        }
    }
}

static void on_read(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
    peer_connection_t *peer = (peer_connection_t *)client;

    if (nread <= 0 || peer->server->network_isolated) { // --- CHAOS: Drop incoming ---
        remove_peer(peer->server, peer);
        uv_close((uv_handle_t*)client, (uv_close_cb)free);
        if (buf->base) free(buf->base);
        return;
    }

    memcpy(peer->buffer + peer->buffer_len, buf->base, nread);
    peer->buffer_len += nread;
    size_t offset = 0;

    while (peer->buffer_len - offset >= sizeof(araft_net_header_t)) {
        araft_net_header_t *h = (araft_net_header_t *)(peer->buffer + offset);
        uint32_t frame_size = sizeof(araft_net_header_t) + h->payload_len;

        if (peer->buffer_len - offset < frame_size) break;
        if (peer->remote_node_id == 0 && h->sender_id != 0) peer->remote_node_id = h->sender_id;

        uint8_t *payload = peer->buffer + offset + sizeof(araft_net_header_t);
        if (h->group_id < peer->server->max_groups && peer->server->groups[h->group_id]) {
            araft_step(peer->server->groups[h->group_id], h->sender_id, h->type, payload, h->payload_len);
        }
        offset += frame_size;
    }

    if (offset > 0 && offset < peer->buffer_len) {
        memmove(peer->buffer, peer->buffer + offset, peer->buffer_len - offset);
    }
    peer->buffer_len -= offset;
    if (buf->base) free(buf->base);
}

static void register_peer(physical_server_t* server, peer_connection_t* peer) {
    if (server->active_peer_count < ARAFT_MAX_PEERS) server->active_peers[server->active_peer_count++] = peer;
}

static void on_new_connection(uv_stream_t *server_stream, int status) {
    if (status < 0) return;
    physical_server_t *server = (physical_server_t*)server_stream->data;
    peer_connection_t *peer = calloc(1, sizeof(peer_connection_t));

    uv_tcp_init(server->loop, &peer->handle);
    peer->server = server;

    if (uv_accept(server_stream, (uv_stream_t*)&peer->handle) == 0) {
        if (server->network_isolated) { // --- CHAOS: Reject new connections instantly ---
            uv_close((uv_handle_t*)&peer->handle, (uv_close_cb)free);
            return;
        }
        register_peer(server, peer);
        uv_read_start((uv_stream_t*)&peer->handle, alloc_cb, on_read);
    } else {
        uv_close((uv_handle_t*)&peer->handle, (uv_close_cb)free);
    }
}

static void on_connect(uv_connect_t* req, int status) {
    peer_connection_t* peer = (peer_connection_t*)req->data;
    if (status == 0 && !peer->server->network_isolated) { // --- CHAOS: Check isolation ---
        register_peer(peer->server, peer);
        uv_read_start((uv_stream_t*)&peer->handle, alloc_cb, on_read);
    } else {
        uv_close((uv_handle_t*)&peer->handle, (uv_close_cb)free);
    }
    free(req);
}

int araft_server_init(physical_server_t* server, uv_loop_t* loop, uint64_t node_id, uint32_t max_groups, const char* data_dir) {
    srand(time(NULL) ^ node_id);

    memset(server, 0, sizeof(*server));
    server->physical_node_id = node_id;
    server->loop = loop;
    server->max_groups = max_groups;
    server->network_isolated = false;
    strncpy(server->data_dir, data_dir, sizeof(server->data_dir) - 1);

    pthread_mutex_init(&server->queue_mutex, NULL);

    size_t needed_bytes = max_groups * sizeof(raft_hard_state_t);
    server->meta_block_size = (needed_bytes + 4095) & ~4095;
    server->groups = calloc(max_groups, sizeof(araft_node_t*));
    if (posix_memalign((void**)&server->meta_ram_array, 4096, server->meta_block_size) != 0) return -1;
    memset(server->meta_ram_array, 0, server->meta_block_size);

    char meta_path[512];
    snprintf(meta_path, sizeof(meta_path), "%s/meta.dat", server->data_dir);
    server->meta_fd = open(meta_path, O_RDWR | O_CREAT, 0644);
    if (server->meta_fd < 0) return -1;

    ssize_t bytes_read = read(server->meta_fd, server->meta_ram_array, server->meta_block_size);
    if (bytes_read < server->meta_block_size) pwrite(server->meta_fd, server->meta_ram_array, server->meta_block_size, 0);

    uv_check_init(loop, &server->loop_check);
    server->loop_check.data = server;
    uv_check_start(&server->loop_check, on_event_loop_check);

    uv_tcp_init(loop, &server->listener);
    server->listener.data = server;
    return 0;
}

int araft_server_listen(physical_server_t* server, const char* ip, int port) {
    struct sockaddr_in addr;
    uv_ip4_addr(ip, port, &addr);
    uv_tcp_bind(&server->listener, (const struct sockaddr*)&addr, 0);
    return uv_listen((uv_stream_t*)&server->listener, 128, on_new_connection);
}

void araft_server_connect(physical_server_t* server, const char* ip, int port, uint64_t target_node_id) {
    if (server->network_isolated) return;

    peer_connection_t* peer = calloc(1, sizeof(peer_connection_t));
    uv_tcp_init(server->loop, &peer->handle);
    peer->server = server;
    peer->remote_node_id = target_node_id;

    struct sockaddr_in dest;
    uv_ip4_addr(ip, port, &dest);

    uv_connect_t* req = malloc(sizeof(uv_connect_t));
    req->data = peer;
    uv_tcp_connect(req, &peer->handle, (const struct sockaddr*)&dest, on_connect);
}

void araft_node_init(araft_node_t* node, physical_server_t* server, uint64_t group_id, uint32_t cluster_size) {
    memset(node, 0, sizeof(*node));
    node->group_id = group_id;
    node->state = ARAFT_STATE_FOLLOWER;
    node->server = server;
    node->cluster_size = cluster_size;
    node->current_leader_id = 0;
    node->last_log_index = 0;

    node->known_leader_commit = (uint64_t)-1;

    if (group_id < server->max_groups) {
        node->meta = &server->meta_ram_array[group_id];
        server->groups[group_id] = node;
    }

    char wal_path[512];
    snprintf(wal_path, sizeof(wal_path), "%s/wal_grp%llu", server->data_dir, group_id);
    awal_init(&node->wal, wal_path);

    if (node->wal.demux_expected_raft_index > 1) {
        node->last_log_index = node->wal.demux_expected_raft_index - 1;
        node->commit_index = node->last_log_index;
    }

    uv_timer_init(server->loop, &node->election_timer);
    node->election_timer.data = node;

    uv_timer_init(server->loop, &node->heartbeat_timer);
    node->heartbeat_timer.data = node;
}

void araft_node_start(araft_node_t* handle_node) {
    araft_node_t* node = (araft_node_t*)((uv_timer_t*)handle_node)->data;

    if (node->state == ARAFT_STATE_LEADER) {
        for (uint32_t p = 0; p < node->server->active_peer_count; p++) {
            uint64_t peer_id = node->server->active_peers[p]->remote_node_id;
            if (peer_id == 0) continue;

            raft_peer_state_t* peer_state = get_peer_state(node, peer_id);
            uint64_t next_idx = peer_state ? peer_state->next_index : node->last_log_index + 1;

            if (node->last_log_index >= next_idx && next_idx > 0) {
                uint64_t term;
                uint8_t* payload;
                uint32_t len;
                if (awal_read_entry(&node->wal, next_idx, &term, &payload, &len)) {
                    size_t total_len = sizeof(msg_append_entries_t) + len;
                    uint8_t* rpc_buf = malloc(total_len);

                    msg_append_entries_t* hb = (msg_append_entries_t*)rpc_buf;
                    hb->term = node->meta->current_term;
                    hb->leader_id = node->server->physical_node_id;
                    hb->prev_log_index = next_idx - 1;

                    uint64_t prev_term = 0;
                    if (next_idx > 1) {
                        uint8_t* p_p; uint32_t p_l;
                        awal_read_entry(&node->wal, next_idx - 1, &prev_term, &p_p, &p_l);
                        if (p_p) free(p_p);
                    }
                    hb->prev_log_term = prev_term;
                    hb->leader_commit = node->commit_index;
                    hb->entry_count = 1;

                    if (len > 0) memcpy(rpc_buf + sizeof(msg_append_entries_t), payload, len);

                    defer_target_rpc(node->server, node->group_id, peer_id, MSG_APPEND_ENTRIES, rpc_buf, total_len);
                    if (payload) free(payload);
                    free(rpc_buf);
                }
            } else {
                msg_append_entries_t hb = {
                    .term = node->meta->current_term,
                    .leader_id = node->server->physical_node_id,
                    .prev_log_index = node->last_log_index,
                    .leader_commit = node->commit_index,
                    .entry_count = 0
                };
                defer_target_rpc(node->server, node->group_id, peer_id, MSG_APPEND_ENTRIES, &hb, sizeof(hb));
            }
        }
    } else {
        node->state = ARAFT_STATE_CANDIDATE;
        node->meta->current_term++;
        node->meta->voted_for = node->server->physical_node_id;
        node->current_leader_id = 0;
        node->votes_received = 1;
        node->server->metadata_is_dirty = true;

        printf("[Node %llu|Grp %llu] Election timeout! Candidate for term %llu\n",
               node->server->physical_node_id, node->group_id, node->meta->current_term);

        uint64_t my_last_log_term = 0;
        if (node->last_log_index > 0) {
            uint8_t* p_p; uint32_t p_l;
            if (awal_read_entry(&node->wal, node->last_log_index, &my_last_log_term, &p_p, &p_l)) {
                if (p_p) free(p_p);
            }
        }

        msg_request_vote_t req = {
            .term = node->meta->current_term,
            .candidate_id = node->server->physical_node_id,
            .last_log_index = node->last_log_index,
            .last_log_term = my_last_log_term
        };
        defer_broadcast_rpc(node->server, node->group_id, MSG_REQUEST_VOTE, &req, sizeof(req));
        araft_reset_election_timer(node);
    }
}

void araft_propose(araft_node_t* node, const uint8_t* payload, uint32_t len) {
    if (node->state != ARAFT_STATE_LEADER) return;
    node->last_log_index++;
    awal_append(&node->wal, node->meta->current_term, node->last_log_index, payload, len);
    awal_flush_batch(&node->wal);
}

void araft_forward_request(araft_node_t* node, uint64_t request_id, const uint8_t* payload, uint32_t len) {
    size_t total_len = sizeof(msg_client_forward_t) + len;
    uint8_t* rpc_buf = malloc(total_len);
    msg_client_forward_t* fwd = (msg_client_forward_t*)rpc_buf;
    fwd->request_id = request_id;
    fwd->follower_node_id = node->server->physical_node_id;
    memcpy(rpc_buf + sizeof(msg_client_forward_t), payload, len);

    defer_target_rpc(node->server, node->group_id, node->current_leader_id, MSG_CLIENT_FORWARD, rpc_buf, total_len);
    free(rpc_buf);
}

void araft_server_set_forward_cb(physical_server_t* server, araft_forward_response_cb cb) {
    server->forward_cb = cb;
}

// --- NEW: Implement Chaos Network Functions ---
void araft_server_isolate(physical_server_t* server) {
    server->network_isolated = true;
    for (uint32_t i = 0; i < server->active_peer_count; i++) {
        uv_close((uv_handle_t*)&server->active_peers[i]->handle, (uv_close_cb)free);
    }
    server->active_peer_count = 0;
}

void araft_server_reconnect(physical_server_t* server) {
    server->network_isolated = false;
}
