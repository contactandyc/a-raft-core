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

#pragma pack(push, 1)
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t term;
    uint64_t voted_for;
    uint64_t commit_index;
    uint64_t last_applied;
    uint64_t snapshot_index;
    uint64_t snapshot_term;
    uint32_t num_peers;
} raft_meta_header_t;

typedef struct {
    uint64_t peer_id;
    uint8_t is_learner;
} raft_meta_peer_t;
#pragma pack(pop)

void raft_node_pump(raft_node_t* node);
static void attempt_reconnect(uv_timer_t* handle);

static bool save_hardstate(raft_node_t* node, uint64_t term, uint64_t vote, uint64_t commit, uint64_t applied, uint64_t snap_idx, uint64_t snap_term) {
    char tmp_path[512], meta_path[512], dir_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s/meta_grp%llu.tmp", node->server->data_dir, (unsigned long long)node->group_id);
    snprintf(meta_path, sizeof(meta_path), "%s/meta_grp%llu.dat", node->server->data_dir, (unsigned long long)node->group_id);
    snprintf(dir_path, sizeof(dir_path), "%s", node->server->data_dir);

    FILE* f = fopen(tmp_path, "wb");
    if (!f) return false;

    raft_meta_header_t hdr = {
        .magic = 0x4D455441, .version = 1,
        .term = term, .voted_for = vote, .commit_index = commit, .last_applied = applied,
        .snapshot_index = snap_idx,
        .snapshot_term = snap_term
    };

    uint64_t peers[RAFT_MAX_PEERS];
    bool is_learner[RAFT_MAX_PEERS];
    hdr.num_peers = (uint32_t)raft_peers_ext(node->core, peers, is_learner);

    if (fwrite(&hdr, sizeof(raft_meta_header_t), 1, f) != 1) { fclose(f); return false; }
    for (uint32_t i = 0; i < hdr.num_peers; i++) {
        raft_meta_peer_t p = { .peer_id = peers[i], .is_learner = is_learner[i] ? 1 : 0 };
        if (fwrite(&p, sizeof(raft_meta_peer_t), 1, f) != 1) { fclose(f); return false; }
    }

    fflush(f); fsync(fileno(f)); fclose(f);
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

    if (raft_state(n->core) == RAFT_STATE_LEADER) {
        raft_msg_t chk = { .type = MSG_CHECK_QUORUM };
        raft_step(n->core, &chk);
    } else {
        raft_msg_t hup = { .type = MSG_HUP };
        raft_step(n->core, &hup);
    }
    raft_node_pump(n);
}

static void on_heartbeat_tick(uv_timer_t* handle) {
    raft_node_t* n = (raft_node_t*)handle->data;
    raft_msg_t tick = { .type = MSG_TICK };
    raft_step(n->core, &tick);

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
    if (!req) return;

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
    if (!frame) {
        free(payload);
        return;
    }

    raft_net_header_t* h = (raft_net_header_t*)frame;
    h->payload_len = len;
    h->group_id = group_id;
    h->sender_id = server->physical_node_id;
    memcpy(frame + sizeof(raft_net_header_t), payload, len);

    bool handed_to_uv = false;

    for (uint32_t i = 0; i < server->known_peer_count; i++) {
        known_peer_t* kp = server->known_peers[i];
        if (kp->node_id == target_node_id) {
            size_t needed = kp->out_queue_len + frame_size;

            if (needed < kp->out_queue_len) {
                free(frame); free(payload); return;
            }

            size_t new_cap = kp->out_queue_cap > 0 ? kp->out_queue_cap : 65536;

            while (new_cap < needed) {
                size_t old_cap = new_cap;
                new_cap *= 2;
                if (new_cap < old_cap || new_cap > 50 * 1024 * 1024) {
                    free(frame); free(payload); return;
                }
            }

            if (new_cap != kp->out_queue_cap) {
                uint8_t* new_q = realloc(kp->out_queue, new_cap);
                if (!new_q) {
                    free(frame); free(payload); return;
                }
                kp->out_queue = new_q;
                kp->out_queue_cap = new_cap;
            }

            memcpy(kp->out_queue + kp->out_queue_len, frame, frame_size);
            kp->out_queue_len += frame_size;

            if (kp->conn) {
                flush_outbound(kp);
            }

            free(frame);
            free(payload);
            return;
        }
    }

    for (uint32_t i = 0; i < server->active_peer_count; i++) {
        peer_connection_t* peer = server->active_peers[i];
        if (peer->remote_node_id == target_node_id) {
            uv_buf_t buf = uv_buf_init((char*)frame, frame_size);
            uv_write_t* req = malloc(sizeof(uv_write_t));
            if (req) {
                req->data = frame;
                if (uv_write(req, (uv_stream_t*)&peer->handle, &buf, 1, on_write_done) == 0) {
                    handed_to_uv = true;
                } else {
                    free(req);
                }
            }
            break;
        }
    }

    if (!handed_to_uv) {
        free(frame);
    }
    free(payload);
}

void raft_node_pump(raft_node_t* node) {
    if (node->fatal_error) return;

    raft_ready_t ready = raft_get_ready(node->core);
    uint64_t actual_saved_idx = raft_last_index(node->core) - ready.num_entries_to_save;
    uint64_t actual_applied_idx = node->saved_applied;

    for (size_t i = 0; i < ready.num_read_states; i++) {
        if (node->num_pending_reads < 128) {
            node->pending_reads[node->num_pending_reads++] = ready.read_states[i];
        }
    }

    // PHASE 13: True Deferral & Multi-Stage Snapshot Acknowledgement
    if (ready.install_snapshot) {
        bool snap_success = false;

        char tmp_snap[512], dat_snap[512];
        snprintf(tmp_snap, sizeof(tmp_snap), "%s/snap_grp%llu.tmp", node->server->data_dir, (unsigned long long)node->group_id);
        snprintf(dat_snap, sizeof(dat_snap), "%s/snap_grp%llu.dat", node->server->data_dir, (unsigned long long)node->group_id);

        FILE* sf = fopen(tmp_snap, "wb");
        if (sf) {
            if (ready.snapshot_len > 0) fwrite(ready.snapshot_data, 1, ready.snapshot_len, sf);
            fflush(sf); fsync(fileno(sf)); fclose(sf);

            if (rename(tmp_snap, dat_snap) == 0) {
                // Defer to the host application callback
                if (!node->snap_cb || node->snap_cb(node->snap_ctx, ready.snapshot_index, ready.snapshot_term, ready.snapshot_data, ready.snapshot_len) == RAFT_SNAPSHOT_OK) {

                    if (save_hardstate(node, raft_term(node->core), raft_voted_for(node->core), ready.snapshot_index, ready.snapshot_index, ready.snapshot_index, ready.snapshot_term)) {
                        node->saved_snap_idx = ready.snapshot_index;
                        node->saved_snap_term = ready.snapshot_term;
                        node->saved_applied = ready.snapshot_index;
                        node->saved_commit = ready.snapshot_index;

                        // We are successfully durable. Safe to truncate.
                        raft_wal_purge_head(&node->wal, ready.snapshot_index);

                        actual_applied_idx = ready.snapshot_index > actual_applied_idx ? ready.snapshot_index : actual_applied_idx;
                        actual_saved_idx = ready.snapshot_index > actual_saved_idx ? ready.snapshot_index : actual_saved_idx;
                        snap_success = true;
                    }
                }
            }
        }

        // Pass the final verdict to the core so it can issue the MSG_APPEND_RES
        raft_snapshot_acked(node->core, snap_success);
        if (!snap_success) {
            fprintf(stderr, "[ERROR] Snapshot installation failed. WAL intact.\n");
        }
    }

    if (ready.num_entries_to_save > 0) {
        if (!raft_io_save(&node->wal, &ready)) {
            fprintf(stderr, "[ERROR] Raft Disk I/O failed! Halting pump cycle.\n");
            goto pump_cleanup;
        }
        actual_saved_idx = raft_last_index(node->core);
    }

    uint64_t current_term = raft_term(node->core);
    uint64_t voted_for = raft_voted_for(node->core);
    uint64_t commit_idx = raft_commit_index(node->core);
    uint64_t snap_idx = raft_snapshot_index(node->core);
    uint64_t snap_term = raft_snapshot_term(node->core);

    if (current_term != node->saved_term || voted_for != node->saved_vote ||
        commit_idx > node->saved_commit || snap_idx > node->saved_snap_idx) {

        if (!save_hardstate(node, current_term, voted_for, commit_idx, actual_applied_idx, snap_idx, snap_term)) {
            fprintf(stderr, "[ERROR] Pre-Network Meta Disk I/O failed! Halting pump.\n");
            goto pump_cleanup;
        }

        node->saved_term = current_term; node->saved_vote = voted_for;
        node->saved_commit = commit_idx;
        node->saved_snap_idx = snap_idx; node->saved_snap_term = snap_term;
    }

    for (size_t i = 0; i < ready.num_messages; i++) {
        if (ready.messages[i].type == MSG_INSTALL_SNAPSHOT && ready.messages[i].snapshot_len == 0) {
            char dat_snap[512];
            snprintf(dat_snap, sizeof(dat_snap), "%s/snap_grp%llu.dat", node->server->data_dir, (unsigned long long)node->group_id);
            FILE* sf = fopen(dat_snap, "rb");
            if (sf) {
                fseek(sf, 0, SEEK_END);
                long fsize = ftell(sf);
                fseek(sf, 0, SEEK_SET);
                if (fsize > 0) {
                    ready.messages[i].snapshot_data = malloc(fsize);
                    if (ready.messages[i].snapshot_data) {
                        ready.messages[i].snapshot_len = fsize;
                        fread(ready.messages[i].snapshot_data, 1, fsize, sf);
                    }
                }
                fclose(sf);
            }
        }

        uint8_t* payload; uint32_t len;
        if (raft_codec_serialize_msg(&ready.messages[i], &payload, &len) == 0) {
            router_send_rpc(node->server, node->group_id, ready.messages[i].to, payload, len);
        }

        if (ready.messages[i].type == MSG_INSTALL_SNAPSHOT && ready.messages[i].snapshot_data) {
            free(ready.messages[i].snapshot_data);
            ready.messages[i].snapshot_data = NULL;
        }
    }

    bool applied_changed = false;
    if (ready.num_committed_entries > 0) {
        for (size_t i = 0; i < ready.num_committed_entries; i++) {
            raft_entry_t* e = &ready.committed_entries[i];

            if (e->index <= actual_applied_idx) continue;

            if (node->apply_cb) {
                int res = node->apply_cb(node->apply_ctx, e, current_term);
                if (res == RAFT_APPLY_TRANSIENT) break;
                else if (res == RAFT_APPLY_FATAL) {
                    node->fatal_error = true;
                    raft_msg_t hup = { .type = MSG_CHECK_QUORUM };
                    raft_step(node->core, &hup);
                    break;
                }
            }
            actual_applied_idx = e->index;
            applied_changed = true;
        }
    }

    for (size_t i = 0; i < node->num_pending_reads; ) {
        if (node->pending_reads[i].index <= actual_applied_idx) {
            if (node->read_cb) {
                node->read_cb(node->read_ctx, node->pending_reads[i].read_seq);
            }
            node->num_pending_reads--;
            if (i < node->num_pending_reads) {
                node->pending_reads[i] = node->pending_reads[node->num_pending_reads];
            }
        } else {
            i++;
        }
    }

    raft_advance(node->core, actual_saved_idx, actual_applied_idx);

    if (applied_changed || actual_applied_idx > node->saved_applied) {
        if (!save_hardstate(node, node->saved_term, node->saved_vote, node->saved_commit, actual_applied_idx, node->saved_snap_idx, node->saved_snap_term)) {
            fprintf(stderr, "[ERROR] Post-Apply Meta Disk I/O failed! Halting pump.\n");
            goto pump_cleanup;
        }
        node->saved_applied = actual_applied_idx;
    }

    raft_state_t state = raft_state(node->core);
    if (state == RAFT_STATE_LEADER) {
        if (!uv_is_active((uv_handle_t*)&node->heartbeat_timer)) uv_timer_start(&node->heartbeat_timer, (uv_timer_cb)on_heartbeat_tick, 50, 50);
        if (!uv_is_active((uv_handle_t*)&node->election_timer)) reset_election_timer(node);
    } else {
        if (uv_is_active((uv_handle_t*)&node->heartbeat_timer)) uv_timer_stop(&node->heartbeat_timer);
        if (!uv_is_active((uv_handle_t*)&node->election_timer)) reset_election_timer(node);
    }

pump_cleanup:
    if (ready.num_entries_to_save > 0 && ready.entries_to_save) free(ready.entries_to_save);
    if (ready.num_committed_entries > 0 && ready.committed_entries) free(ready.committed_entries);
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
        size_t needed = peer->buffer_len + nread;
        if (needed < peer->buffer_len) {
            remove_peer(peer->server, peer);
            uv_close((uv_handle_t*)client, on_client_close);
            if (buf->base) free(buf->base);
            return;
        }

        size_t new_cap = peer->buffer_cap > 0 ? peer->buffer_cap * 2 : 65536;
        while (new_cap < needed) {
            size_t old_cap = new_cap;
            new_cap *= 2;
            if (new_cap < old_cap) {
                new_cap = needed;
                break;
            }
        }

        size_t max_allowed = RAFT_MAX_FRAME_SIZE + sizeof(raft_net_header_t);
        if (new_cap > max_allowed) new_cap = max_allowed;

        if (needed > new_cap) {
            fprintf(stderr, "[WARN] Disconnecting peer: Buffer capacity exceeded max frame size.\n");
            remove_peer(peer->server, peer);
            uv_close((uv_handle_t*)client, on_client_close);
            if (buf->base) free(buf->base);
            return;
        }

        uint8_t* temp = realloc(peer->buffer, new_cap);
        if (!temp) {
             fprintf(stderr, "[ERROR] Memory allocation failure on read buffer. Disconnecting.\n");
             remove_peer(peer->server, peer);
             uv_close((uv_handle_t*)client, on_client_close);
             if (buf->base) free(buf->base);
             return;
        }
        peer->buffer = temp;
        peer->buffer_cap = new_cap;
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

                raft_step(target_node->core, &msg);

                if (raft_activity_accepted(target_node->core)) {
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
    if (req) {
        req->data = peer;
        uv_tcp_connect(req, &peer->handle, (const struct sockaddr*)&dest, on_connect);
    }
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

void raft_node_init(raft_node_t* node, raft_server_t* server, uint64_t group_id, uint64_t* init_peers, size_t num_peers,
                    raft_apply_fn apply_cb, void* apply_ctx, raft_read_cb read_cb, void* read_ctx,
                    raft_snapshot_fn snap_cb, void* snap_ctx) {
    memset(node, 0, sizeof(*node));
    node->group_id = group_id;
    node->server = server;
    node->apply_cb = apply_cb;
    node->apply_ctx = apply_ctx;
    node->read_cb = read_cb;
    node->read_ctx = read_ctx;
    node->snap_cb = snap_cb;
    node->snap_ctx = snap_ctx;
    server->groups[group_id] = node;

    char wal_path[512];
    snprintf(wal_path, sizeof(wal_path), "%s/wal_grp%llu", server->data_dir, (unsigned long long)group_id);
    raft_wal_init(&node->wal, wal_path, 16, 4);

    uint64_t saved_term = 0, saved_vote = 0, saved_commit = 0, saved_applied = 0;
    uint64_t snap_idx = 0, snap_term = 0;

    uint64_t load_peers[RAFT_MAX_PEERS]; bool load_learners[RAFT_MAX_PEERS];
    size_t active_peers = num_peers;
    for (size_t i = 0; i < num_peers; i++) { load_peers[i] = init_peers[i]; load_learners[i] = false; }

    char meta_path[512];
    snprintf(meta_path, sizeof(meta_path), "%s/meta_grp%llu.dat", server->data_dir, (unsigned long long)group_id);
    FILE* f = fopen(meta_path, "rb");
    if (f) {
        uint32_t magic;
        if (fread(&magic, sizeof(uint32_t), 1, f) == 1 && magic == 0x4D455441) {
            fseek(f, 0, SEEK_SET);
            raft_meta_header_t hdr;
            if (fread(&hdr, sizeof(raft_meta_header_t), 1, f) == 1 && hdr.version == 1) {
                saved_term = hdr.term; saved_vote = hdr.voted_for;
                saved_commit = hdr.commit_index; saved_applied = hdr.last_applied;
                snap_idx = hdr.snapshot_index; snap_term = hdr.snapshot_term;

                if (hdr.num_peers > 0 && hdr.num_peers <= RAFT_MAX_PEERS) {
                    active_peers = hdr.num_peers;
                    for (uint32_t i = 0; i < hdr.num_peers; i++) {
                        raft_meta_peer_t p;
                        if (fread(&p, sizeof(raft_meta_peer_t), 1, f) == 1) {
                            load_peers[i] = p.peer_id; load_learners[i] = (p.is_learner != 0);
                        }
                    }
                }
            }
        }
        fclose(f);
    }

    node->saved_term = saved_term; node->saved_vote = saved_vote;
    node->saved_commit = saved_commit; node->saved_applied = saved_applied;
    node->saved_snap_idx = snap_idx; node->saved_snap_term = snap_term;

    node->core = raft_io_boot(&node->wal, server->physical_node_id, load_peers, load_learners, active_peers, saved_term, saved_vote, saved_commit, saved_applied, snap_idx, snap_term);

    uv_timer_init(server->loop, &node->election_timer); node->election_timer.data = node;
    uv_timer_init(server->loop, &node->heartbeat_timer); node->heartbeat_timer.data = node;
    reset_election_timer(node);
}

int raft_node_propose(raft_node_t* node, const uint8_t* payload, uint32_t len, uint64_t client_id, uint64_t client_seq, uint64_t* out_leader_id) {
    if (raft_state(node->core) != RAFT_STATE_LEADER) {
        if (out_leader_id) *out_leader_id = raft_leader_id(node->core);
        return RAFT_ERR_NOT_LEADER;
    }

    if (raft_last_index(node->core) - raft_commit_index(node->core) > 2000) {
        return RAFT_ERR_QUEUE_FULL;
    }

    if (raft_uncommitted_bytes(node->core) > 10 * 1024 * 1024) {
        return RAFT_ERR_QUEUE_FULL;
    }

    raft_entry_t e = { .type = ENTRY_NORMAL, .client_id = client_id, .client_seq = client_seq, .data = (uint8_t*)payload, .data_len = len };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };

    raft_step(node->core, &prop);
    raft_node_pump(node);

    return RAFT_OK;
}

int raft_node_read_index(raft_node_t* node, uint64_t read_seq, uint64_t* out_leader_id) {
    if (raft_state(node->core) != RAFT_STATE_LEADER) {
        if (out_leader_id) *out_leader_id = raft_leader_id(node->core);
        return RAFT_ERR_NOT_LEADER;
    }

    raft_msg_t req = { .type = MSG_READ_INDEX, .read_seq = read_seq };
    raft_step(node->core, &req);
    raft_node_pump(node);

    return RAFT_OK;
}

// PHASE 13: Decoupled Local Compaction API
int raft_node_compact(raft_node_t* node, uint64_t compact_index) {
    if (compact_index <= raft_snapshot_index(node->core)) return RAFT_OK;

    // Core memory truncation
    raft_compact(node->core, compact_index);

    // Explicit hardstate binding to the new snapshot horizon
    if (!save_hardstate(node, node->saved_term, node->saved_vote, node->saved_commit, node->saved_applied, raft_snapshot_index(node->core), raft_snapshot_term(node->core))) {
        return -1;
    }

    // Local historical garbage collection is now mathematically safe
    raft_wal_purge_head(&node->wal, compact_index);
    return RAFT_OK;
}
