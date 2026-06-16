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
#include <fcntl.h> // PHASE 5: For O_RDONLY and directory fsync

// Forward declarations
void raft_node_pump(raft_node_t* node);

// PHASE 5: Atomic HardState Saver
static bool save_hardstate(raft_server_t* server, uint64_t group_id, uint64_t term, uint64_t vote, uint64_t commit) {
    char tmp_path[512], meta_path[512], dir_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s/meta_grp%llu.tmp", server->data_dir, group_id);
    snprintf(meta_path, sizeof(meta_path), "%s/meta_grp%llu.dat", server->data_dir, group_id);
    snprintf(dir_path, sizeof(dir_path), "%s", server->data_dir);

    FILE* f = fopen(tmp_path, "wb");
    if (!f) return false;

    if (fwrite(&term, sizeof(uint64_t), 1, f) != 1 ||
        fwrite(&vote, sizeof(uint64_t), 1, f) != 1 ||
        fwrite(&commit, sizeof(uint64_t), 1, f) != 1) {
        fclose(f);
        return false;
    }

    fflush(f);
    fsync(fileno(f));
    fclose(f);

    // Atomic overwrite
    if (rename(tmp_path, meta_path) != 0) return false;

    // Fsync the parent directory so the rename survives a power crash
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

// ============================================================================
// 1. C CALLBACKS (Replacing Lambdas)
// ============================================================================

static void on_write_done(uv_write_t* req, int status) {
    (void)status;
    free(req->data);
    free(req);
}

static void on_client_close(uv_handle_t* handle) {
    free(handle);
}

static void on_election_timeout(uv_timer_t* handle) {
    raft_node_t* n = (raft_node_t*)handle->data;
    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(n->core, &hup);
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

// ============================================================================
// 2. THE EVENT PUMP (Brain -> Disk -> Network -> State Machine)
// ============================================================================

static void router_send_rpc(raft_server_t* server, uint64_t group_id, uint64_t target_node_id, uint8_t* payload, uint32_t len) {
    if (server->network_isolated) {
        free(payload);
        return;
    }

    for (uint32_t i = 0; i < server->active_peer_count; i++) {
        peer_connection_t* peer = server->active_peers[i];
        if (peer->remote_node_id == target_node_id) {
            uint32_t frame_size = sizeof(raft_net_header_t) + len;
            uint8_t* frame = malloc(frame_size);

            raft_net_header_t* h = (raft_net_header_t*)frame;
            h->payload_len = len;
            h->group_id = group_id;
            h->sender_id = server->physical_node_id;

            memcpy(frame + sizeof(raft_net_header_t), payload, len);

            uv_buf_t buf = uv_buf_init((char*)frame, frame_size);
            uv_write_t* req = malloc(sizeof(uv_write_t));
            req->data = frame;

            uv_write(req, (uv_stream_t*)&peer->handle, &buf, 1, on_write_done);
            break;
        }
    }
    free(payload);
}

static void reset_election_timer(raft_node_t* node) {
    uv_timer_stop(&node->election_timer);
    uint64_t timeout = 150 + (rand() % 150); // 150-300ms randomized
    uv_timer_start(&node->election_timer, on_election_timeout, timeout, 0);
}

// The core loop that glues the system together
void raft_node_pump(raft_node_t* node) {
    raft_ready_t ready = raft_core_get_ready(node->core);

    // PHASE 5: Track exactly what we achieved so we don't over-advance
    uint64_t actual_saved_idx = raft_core_last_index(node->core) - ready.num_entries_to_save;
    uint64_t actual_applied_idx = raft_core_commit_index(node->core) - ready.num_committed_entries;

    // 1. DISK I/O (WAL)
    if (ready.num_entries_to_save > 0) {
        if (!raft_io_save(&node->wal, &ready)) {
            fprintf(stderr, "[ERROR] Raft Disk I/O failed! Halting pump cycle.\n");
            return;
        }
        actual_saved_idx = raft_core_last_index(node->core);
    }

    // 2. DISK I/O (HardState)
    uint64_t current_term = raft_core_term(node->core);
    uint64_t voted_for = raft_core_voted_for(node->core);
    uint64_t commit_idx = raft_core_commit_index(node->core);

    // PHASE 5: Fail-Closed Atomic Meta Writes
    if (current_term != node->saved_term || voted_for != node->saved_vote || commit_idx > actual_applied_idx) {
        if (!save_hardstate(node->server, node->group_id, current_term, voted_for, commit_idx)) {
            fprintf(stderr, "[ERROR] Meta Disk I/O failed! Halting pump to prevent amnesia.\n");
            return;
        }
        node->saved_term = current_term;
        node->saved_vote = voted_for;
    }

    // 3. NETWORK I/O
    for (size_t i = 0; i < ready.num_messages; i++) {
        uint8_t* payload;
        uint32_t len;
        if (raft_codec_serialize_msg(&ready.messages[i], &payload, &len) == 0) {
            router_send_rpc(node->server, node->group_id, ready.messages[i].to, payload, len);
        } else {
            fprintf(stderr, "[ERROR] Failed to serialize outgoing Raft message.\n");
        }
    }

    // 4. STATE MACHINE I/O
    if (ready.num_committed_entries > 0) {
        for (size_t i = 0; i < ready.num_committed_entries; i++) {
            // Future Application Logic: Apply ready.committed_entries[i].data here
        }
        raft_core_apply(node->core);
        actual_applied_idx = commit_idx;
    }

    // 5. Timer Management based on Brain State
    raft_state_t state = raft_core_state(node->core);
    if (state == RAFT_STATE_LEADER) {
        if (!uv_is_active((uv_handle_t*)&node->heartbeat_timer)) {
            uv_timer_stop(&node->election_timer);
            uv_timer_start(&node->heartbeat_timer, on_heartbeat_tick, 50, 50); // 50ms steady heartbeats
        }
    } else {
        if (uv_is_active((uv_handle_t*)&node->heartbeat_timer)) {
            uv_timer_stop(&node->heartbeat_timer);
        }
        if (!uv_is_active((uv_handle_t*)&node->election_timer)) {
            reset_election_timer(node);
        }
    }

    // PHASE 5: Safe Explicit Advancement
    raft_core_advance(node->core, actual_saved_idx, actual_applied_idx);
}

// ============================================================================
// 4. TCP NETWORKING (Dumb pipes)
// ============================================================================

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

    if (peer->buffer_len + nread > sizeof(peer->buffer)) {
        remove_peer(peer->server, peer);
        uv_close((uv_handle_t*)client, on_client_close);
        if (buf->base) free(buf->base);
        return;
    }

    memcpy(peer->buffer + peer->buffer_len, buf->base, nread);
    peer->buffer_len += nread;
    size_t offset = 0;

    while (peer->buffer_len - offset >= sizeof(raft_net_header_t)) {
        raft_net_header_t *h = (raft_net_header_t *)(peer->buffer + offset);

        // PHASE 3 SECURITY: Check Max Frame Bounds
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
            // PHASE 3 SECURITY: Deserialize safely
            if (raft_codec_deserialize_msg(payload, h->payload_len, &msg) == 0) {

                raft_core_step(target_node->core, &msg);

                // PHASE 5: ONLY reset the election timer if the brain verified the leader/vote!
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
        uv_read_start((uv_stream_t*)&peer->handle, alloc_cb, on_read);
    } else {
        uv_close((uv_handle_t*)&peer->handle, on_client_close);
    }
    free(req);
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

void raft_node_init(raft_node_t* node, raft_server_t* server, uint64_t group_id, uint64_t* init_peers, size_t num_peers) {
    memset(node, 0, sizeof(*node));
    node->group_id = group_id;
    node->server = server;
    server->groups[group_id] = node;

    char wal_path[512];
    snprintf(wal_path, sizeof(wal_path), "%s/wal_grp%llu", server->data_dir, group_id);

    // Boot the new Raft WAL with 16MB segments and a max of 4 standby files
    raft_wal_init(&node->wal, wal_path, 16, 4);

    uint64_t saved_commit = 0;
    char meta_path[512];
    snprintf(meta_path, sizeof(meta_path), "%s/meta_grp%llu.dat", server->data_dir, group_id);
    FILE* f = fopen(meta_path, "rb");
    if (f) {
        if (fread(&node->saved_term, sizeof(uint64_t), 1, f) != 1) node->saved_term = 0;
        if (fread(&node->saved_vote, sizeof(uint64_t), 1, f) != 1) node->saved_vote = 0;
        if (fread(&saved_commit, sizeof(uint64_t), 1, f) != 1) saved_commit = 0;
        fclose(f);
    }

    // PHASE 5: Boot safely restores term, vote, and commit index
    node->core = raft_io_boot(&node->wal, server->physical_node_id, init_peers, num_peers, node->saved_term, node->saved_vote, saved_commit);

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
