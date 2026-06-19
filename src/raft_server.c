// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-raft-library/raft_server.h"
#include "a-raft-library/raft_codec.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>

void raft_node_pump(raft_node_t* node);
static void attempt_reconnect(uv_timer_t* handle);

static bool save_hardstate(raft_server_t* server, uint64_t group_id, uint64_t term, uint64_t vote, uint64_t commit, uint64_t applied) {
    char tmp_path[512], meta_path[512], dir_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s/meta_grp%llu.tmp", server->data_dir, group_id);
    snprintf(meta_path, sizeof(meta_path), "%s/meta_grp%llu.dat", server->data_dir, group_id);
    snprintf(dir_path, sizeof(dir_path), "%s", server->data_dir);

    FILE* f = fopen(tmp_path, "wb");
    if (!f) return false;

    if (fwrite(&term, sizeof(uint64_t), 1, f) != 1 ||
        fwrite(&vote, sizeof(uint64_t), 1, f) != 1 ||
        fwrite(&commit, sizeof(uint64_t), 1, f) != 1 ||
        fwrite(&applied, sizeof(uint64_t), 1, f) != 1) {
        fclose(f);
        return false;
    }

    fflush(f);
    fsync(fileno(f));
    fclose(f);

    if (rename(tmp_path, meta_path) != 0) return false;

    int dir_fd = open(dir_path, O_RDONLY);
    if (dir_fd >= 0) {
#ifdef __APPLE__
        fcntl(dir_fd, F_FULLFSYNC, 0);
#else
        fsync(dir_fd);
#endif
        close(dir_fd);
    }
    return true;
}

static void on_write_done(uv_write_t* req, int status) {
    (void)status;
    free(req->data);
    free(req);
}

static void on_client_close(uv_handle_t* handle) {
    peer_connection_t* peer = (peer_connection_t*)handle;

    if (peer->kp) {
        peer->kp->conn = NULL;
        uv_timer_start(&peer->kp->reconnect_timer, (uv_timer_cb)attempt_reconnect, 1000, 0);
    }

    if (peer->buffer) free(peer->buffer);
    free(peer);
}

static void on_election_timeout(uv_timer_t* handle);

static void reset_election_timer(raft_node_t* node) {
    uv_timer_stop(&node->election_timer);
    uint64_t timeout = 150 + (rand() % 150);
    uv_timer_start(&node->election_timer, (uv_timer_cb)on_election_timeout, timeout, 0);
}

static void on_election_timeout(uv_timer_t* handle) {
    raft_node_t* n = (raft_node_t*)handle->data;

    // PHASE 4 (Gap 14): Leader keeps its timer ticking to check quorum!
    if (raft_core_state(n->core) == RAFT_STATE_LEADER) {
        raft_msg_t chk = { .type = MSG_CHECK_QUORUM };
        raft_core_step(n->core, &chk);
    } else {
        raft_msg_t hup = { .type = MSG_HUP };
        raft_core_step(n->core, &hup);
    }
    raft_node_pump(n);
}

static void on_heartbeat_tick(uv_timer_t* handle) {
    raft_node_t* n = (raft_node_t*)handle->data;
    raft_msg_t tick = { .type = MSG_TICK };
    raft_core_step(n->core, &tick);

    raft_node_pump(n);
}

static void alloc_cb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    (void)handle;
    buf->base = malloc(suggested_size);
    if (!buf->base) buf->len = 0;
    else buf->len = suggested_size;
}

static void flush_outbound(known_peer_t* kp) {
    if (!kp->conn || kp->out_queue_len == 0) return;

    uv_buf_t buf = uv_buf_init((char*)kp->out_queue, kp->out_queue_len);
    uv_write_t* req = malloc(sizeof(uv_write_t));
    req->data = kp->out_queue;

    uv_write(req, (uv_stream_t*)&kp->conn->handle, &buf, 1, on_write_done);

    kp->out_queue_cap = 65536;
    kp->out_queue = malloc(kp->out_queue_cap);
    kp->out_queue_len = 0;
}

static void router_send_rpc(raft_server_t* server, uint64_t group_id, uint64_t target_node_id, uint8_t* payload, uint32_t len) {
    if (server->network_isolated) {
        free(payload);
        return;
    }

    uint32_t frame_size = sizeof(raft_net_header_t) + len;
    uint8_t* frame = malloc(frame_size);
    raft_net_header_t* h = (raft_net_header_t*)frame;
    h->payload_len = len;
    h->group_id = group_id;
    h->sender_id = server->physical_node_id;
    memcpy(frame + sizeof(raft_net_header_t), payload, len);

    for (uint32_t i = 0; i < server->known_peer_count; i++) {
        known_peer_t* kp = server->known_peers[i];
        if (kp->node_id == target_node_id) {

            if (kp->out_queue_len + frame_size > kp->out_queue_cap) {
                if (kp->out_queue_cap > 5 * 1024 * 1024) {
                    free(frame); free(payload); return;
                }
                kp->out_queue_cap *= 2;
                kp->out_queue = realloc(kp->out_queue, kp->out_queue_cap);
            }
            memcpy(kp->out_queue + kp->out_queue_len, frame, frame_size);
            kp->out_queue_len += frame_size;

            if (kp->conn) {
                flush_outbound(kp);
            }
            free(frame); free(payload);
            return;
        }
    }

    for (uint32_t i = 0; i < server->active_peer_count; i++) {
        peer_connection_t* peer = server->active_peers[i];
        if (peer->remote_node_id == target_node_id) {
            uv_buf_t buf = uv_buf_init((char*)frame, frame_size);
            uv_write_t* req = malloc(sizeof(uv_write_t));
            req->data = frame;
            uv_write(req, (uv_stream_t*)&peer->handle, &buf, 1, on_write_done);
            break;
        }
    }
    free(payload);
}

void raft_node_pump(raft_node_t* node) {
    raft_ready_t ready = raft_core_get_ready(node->core);

    uint64_t actual_saved_idx = raft_core_last_index(node->core) - ready.num_entries_to_save;
    uint64_t actual_applied_idx = raft_core_commit_index(node->core) - ready.num_committed_entries;

    if (ready.num_entries_to_save > 0) {
        if (!raft_io_save(&node->wal, &ready)) {
            fprintf(stderr, "[ERROR] Raft Disk I/O failed! Halting pump cycle.\n");
            return;
        }
        actual_saved_idx = raft_core_last_index(node->core);
    }

    uint64_t current_term = raft_core_term(node->core);
    uint64_t voted_for = raft_core_voted_for(node->core);
    uint64_t commit_idx = raft_core_commit_index(node->core);
    uint64_t last_applied = raft_core_last_applied(node->core);

    if (current_term != node->saved_term || voted_for != node->saved_vote || commit_idx > actual_applied_idx || last_applied > node->saved_applied) {
        if (!save_hardstate(node->server, node->group_id, current_term, voted_for, commit_idx, last_applied)) {
            fprintf(stderr, "[ERROR] Meta Disk I/O failed! Halting pump to prevent amnesia.\n");
            return;
        }
        node->saved_term = current_term;
        node->saved_vote = voted_for;
        node->saved_applied = last_applied;
    }

    for (size_t i = 0; i < ready.num_messages; i++) {
        uint8_t* payload;
        uint32_t len;
        if (raft_codec_serialize_msg(&ready.messages[i], &payload, &len) == 0) {
            router_send_rpc(node->server, node->group_id, ready.messages[i].to, payload, len);
        } else {
            fprintf(stderr, "[ERROR] Failed to serialize outgoing Raft message.\n");
        }
    }

    if (ready.num_committed_entries > 0) {
        for (size_t i = 0; i < ready.num_committed_entries; i++) {
            // Apply entries securely
        }
        actual_applied_idx = commit_idx;
    }

    // PHASE 4 (Gap 13): Deliver validated linearizable read contexts
    if (ready.num_read_states > 0) {
        for (size_t i = 0; i < ready.num_read_states; i++) {
            // Future Application logic: Serve the read_seq context to the client!
        }
    }

    raft_state_t state = raft_core_state(node->core);
    if (state == RAFT_STATE_LEADER) {
        if (!uv_is_active((uv_handle_t*)&node->heartbeat_timer)) {
            uv_timer_start(&node->heartbeat_timer, (uv_timer_cb)on_heartbeat_tick, 50, 50);
        }
        // PHASE 4 (Gap 14): Keep election timer active on Leader to guard against stale state
        if (!uv_is_active((uv_handle_t*)&node->election_timer)) {
            reset_election_timer(node);
        }
    } else {
        if (uv_is_active((uv_handle_t*)&node->heartbeat_timer)) {
            uv_timer_stop(&node->heartbeat_timer);
        }
        if (!uv_is_active((uv_handle_t*)&node->election_timer)) {
            reset_election_timer(node);
        }
    }

    raft_core_advance(node->core, actual_saved_idx, actual_applied_idx);
}

static void remove_peer(raft_server_t* server, peer_connection_t* peer) {
    for (uint32_t i = 0; i < server->active_peer_count; i++) {
        if (server->active_peers[i] == peer) {
            server->active_peers[i] = server->active_peers[--server->active_peer_count];
            return;
        }
    }
}

static void on_read(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
    peer_connection_t *peer = (peer_connection_t *)client;

    if (nread <= 0 || peer->server->network_isolated) {
        remove_peer(peer->server, peer);
        uv_close((uv_handle_t*)client, on_client_close);
        if (buf->base) free(buf->base);
        return;
    }

    if (peer->buffer_len + nread > peer->buffer_cap) {
        size_t new_cap = peer->buffer_cap * 2;
        while (peer->buffer_len + nread > new_cap) new_cap *= 2;

        size_t max_allowed = RAFT_MAX_FRAME_SIZE + sizeof(raft_net_header_t);
        if (new_cap > max_allowed) new_cap = max_allowed;

        if (peer->buffer_len + nread > new_cap) {
            fprintf(stderr, "[WARN] Disconnecting peer: Buffer capacity exceeded max frame size.\n");
            remove_peer(peer->server, peer);
            uv_close((uv_handle_t*)client, on_client_close);
            if (buf->base) free(buf->base);
            return;
        }
        peer->buffer = realloc(peer->buffer, new_cap);
    }

    memcpy(peer->buffer + peer->buffer_len, buf->base, nread);
    peer->buffer_len += nread;
    size_t offset = 0;

    while (peer->buffer_len - offset >= sizeof(raft_net_header_t)) {
        raft_net_header_t *h = (raft_net_header_t *)(peer->buffer + offset);

        if (h->payload_len > RAFT_MAX_FRAME_SIZE) {
            fprintf(stderr, "[WARN] Disconnecting peer: Frame size exceeds limit.\n");
            remove_peer(peer->server, peer);
            uv_close((uv_handle_t*)client, on_client_close);
            if (buf->base) free(buf->base);
            return;
        }

        uint32_t frame_size = sizeof(raft_net_header_t) + h->payload_len;
        if (peer->buffer_len - offset < frame_size) break;

        if (peer->remote_node_id == 0 && h->sender_id != 0) peer->remote_node_id = h->sender_id;

        uint8_t *payload = peer->buffer + offset + sizeof(raft_net_header_t);
        if (h->group_id < peer->server->max_groups && peer->server->groups[h->group_id]) {
            raft_node_t* target_node = peer->server->groups[h->group_id];

            raft_msg_t msg;
            if (raft_codec_deserialize_msg(payload, h->payload_len, &msg) == 0) {

                raft_core_step(target_node->core, &msg);

                if (raft_core_activity_accepted(target_node->core)) {
                    reset_election_timer(target_node);
                }

                raft_codec_free_msg_entries(&msg);
                raft_node_pump(target_node);
            } else {
                fprintf(stderr, "[WARN] Dropped malformed Raft frame from peer.\n");
            }
        }
        offset += frame_size;
    }

    if (offset > 0 && offset < peer->buffer_len) {
        memmove(peer->buffer, peer->buffer + offset, peer->buffer_len - offset);
    }
    peer->buffer_len -= offset;
    if (buf->base) free(buf->base);
}

static void register_peer(raft_server_t* server, peer_connection_t* peer) {
    if (server->active_peer_count < RAFT_MAX_PEERS) {
        server->active_peers[server->active_peer_count++] = peer;
    }
}

static void on_new_connection(uv_stream_t *server_stream, int status) {
    if (status < 0) return;
    raft_server_t *server = (raft_server_t*)server_stream->data;

    peer_connection_t *peer = calloc(1, sizeof(peer_connection_t));
    peer->buffer_cap = 65536;
    peer->buffer = malloc(peer->buffer_cap);

    uv_tcp_init(server->loop, &peer->handle);
    peer->server = server;

    if (uv_accept(server_stream, (uv_stream_t*)&peer->handle) == 0) {
        if (server->network_isolated) {
            uv_close((uv_handle_t*)&peer->handle, on_client_close);
            return;
        }
        register_peer(server, peer);
        uv_read_start((uv_stream_t*)&peer->handle, alloc_cb, on_read);
    } else {
        uv_close((uv_handle_t*)&peer->handle, on_client_close);
    }
}

static void on_connect(uv_connect_t* req, int status) {
    peer_connection_t* peer = (peer_connection_t*)req->data;
    if (status == 0 && !peer->server->network_isolated) {
        register_peer(peer->server, peer);
        if (peer->kp) {
            peer->kp->conn = peer;
            flush_outbound(peer->kp);
        }
        uv_read_start((uv_stream_t*)&peer->handle, alloc_cb, on_read);
    } else {
        uv_close((uv_handle_t*)&peer->handle, on_client_close);
    }
    free(req);
}

static void attempt_reconnect(uv_timer_t* handle) {
    known_peer_t* kp = (known_peer_t*)handle->data;

    if (kp->server->network_isolated || kp->conn) return;

    peer_connection_t* peer = calloc(1, sizeof(peer_connection_t));
    peer->buffer_cap = 65536;
    peer->buffer = malloc(peer->buffer_cap);

    uv_tcp_init(kp->server->loop, &peer->handle);
    peer->server = kp->server;
    peer->remote_node_id = kp->node_id;
    peer->kp = kp;

    struct sockaddr_in dest;
    uv_ip4_addr(kp->ip, kp->port, &dest);

    uv_connect_t* req = malloc(sizeof(uv_connect_t));
    req->data = peer;
    uv_tcp_connect(req, &peer->handle, (const struct sockaddr*)&dest, on_connect);
}

int raft_server_init(raft_server_t* server, uv_loop_t* loop, uint64_t node_id, uint32_t max_groups, const char* data_dir) {
    memset(server, 0, sizeof(*server));
    server->physical_node_id = node_id;
    server->loop = loop;
    server->max_groups = max_groups;
    server->network_isolated = false;
    strncpy(server->data_dir, data_dir, sizeof(server->data_dir) - 1);

    server->groups = calloc(max_groups, sizeof(raft_node_t*));

    uv_tcp_init(loop, &server->listener);
    server->listener.data = server;
    return 0;
}

int raft_server_listen(raft_server_t* server, const char* ip, int port) {
    struct sockaddr_in addr;
    uv_ip4_addr(ip, port, &addr);
    uv_tcp_bind(&server->listener, (const struct sockaddr*)&addr, 0);
    return uv_listen((uv_stream_t*)&server->listener, 128, on_new_connection);
}

void raft_server_connect(raft_server_t* server, const char* ip, int port, uint64_t target_node_id) {
    if (server->network_isolated || server->known_peer_count >= RAFT_MAX_PEERS) return;

    known_peer_t* kp = calloc(1, sizeof(known_peer_t));
    kp->server = server;
    kp->node_id = target_node_id;
    strncpy(kp->ip, ip, 63);
    kp->port = port;

    kp->out_queue_cap = 65536;
    kp->out_queue = malloc(kp->out_queue_cap);

    uv_timer_init(server->loop, &kp->reconnect_timer);
    kp->reconnect_timer.data = kp;

    server->known_peers[server->known_peer_count++] = kp;

    attempt_reconnect(&kp->reconnect_timer);
}

void raft_node_init(raft_node_t* node, raft_server_t* server, uint64_t group_id, uint64_t* init_peers, size_t num_peers) {
    memset(node, 0, sizeof(*node));
    node->group_id = group_id;
    node->server = server;
    server->groups[group_id] = node;

    char wal_path[512];
    snprintf(wal_path, sizeof(wal_path), "%s/wal_grp%llu", server->data_dir, group_id);

    raft_wal_init(&node->wal, wal_path, 16, 4);

    uint64_t saved_commit = 0;
    uint64_t saved_applied = 0;
    char meta_path[512];
    snprintf(meta_path, sizeof(meta_path), "%s/meta_grp%llu.dat", server->data_dir, group_id);
    FILE* f = fopen(meta_path, "rb");
    if (f) {
        if (fread(&node->saved_term, sizeof(uint64_t), 1, f) != 1) node->saved_term = 0;
        if (fread(&node->saved_vote, sizeof(uint64_t), 1, f) != 1) node->saved_vote = 0;
        if (fread(&saved_commit, sizeof(uint64_t), 1, f) != 1) saved_commit = 0;
        if (fread(&saved_applied, sizeof(uint64_t), 1, f) != 1) saved_applied = 0;
        fclose(f);
    }

    node->saved_applied = saved_applied;
    node->core = raft_io_boot(&node->wal, server->physical_node_id, init_peers, num_peers, node->saved_term, node->saved_vote, saved_commit, saved_applied);

    uv_timer_init(server->loop, &node->election_timer);
    node->election_timer.data = node;
    uv_timer_init(server->loop, &node->heartbeat_timer);
    node->heartbeat_timer.data = node;

    reset_election_timer(node);
}

void raft_node_propose(raft_node_t* node, const uint8_t* payload, uint32_t len) {
    raft_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)payload, .data_len = len };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };

    raft_core_step(node->core, &prop);
    raft_node_pump(node);
}

// PHASE 4 (Gap 13): Expose safe linearizable read interface
void raft_node_read_index(raft_node_t* node, uint64_t read_seq) {
    raft_msg_t req = { .type = MSG_READ_INDEX, .read_seq = read_seq };
    raft_core_step(node->core, &req);
    raft_node_pump(node);
}
