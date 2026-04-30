// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef ARAFT_H
#define ARAFT_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <uv.h>
#include "a-raft-library/awal.h"

#define ARAFT_MAX_PEERS 64
#define ARAFT_MAX_DEFERRED_MSGS 4096

#define MSG_REQUEST_VOTE 1
#define MSG_REQUEST_VOTE_RES 2
#define MSG_APPEND_ENTRIES 3
#define MSG_APPEND_ENTRIES_RES 4
#define MSG_CLIENT_FORWARD 5
#define MSG_CLIENT_FORWARD_RES 6

#pragma pack(push, 1)

typedef struct {
    uint32_t payload_len;
    uint8_t  type;
    uint64_t group_id;
    uint64_t sender_id;
} araft_net_header_t;

typedef struct {
    uint64_t current_term;
    uint64_t voted_for;
} raft_hard_state_t;

typedef struct {
    uint64_t term;
    uint64_t candidate_id;
    uint64_t last_log_index;
    uint64_t last_log_term;
} msg_request_vote_t;

typedef struct {
    uint64_t term;
    uint8_t  vote_granted;
} msg_request_vote_res_t;

typedef struct {
    uint64_t term;
    uint64_t leader_id;
    uint64_t prev_log_index;
    uint64_t prev_log_term;
    uint64_t leader_commit;
    uint32_t entry_count;
} msg_append_entries_t;

typedef struct {
    uint64_t term;
    uint8_t  success;
    uint64_t match_index;
} msg_append_entries_res_t;

typedef struct {
    uint64_t request_id;
    uint64_t follower_node_id;
} msg_client_forward_t;

typedef struct {
    uint64_t request_id;
    uint8_t  success;
} msg_client_forward_res_t;

#pragma pack(pop)

typedef struct araft_node_s araft_node_t;
typedef struct physical_server_s physical_server_t;
typedef struct peer_connection_s peer_connection_t;

struct peer_connection_s {
    uv_tcp_t handle;
    physical_server_t* server;
    uint8_t buffer[65536];
    size_t buffer_len;
    uint64_t remote_node_id;
};

typedef struct {
    uint64_t target_group_id;
    uint8_t  type;
    uint32_t payload_len;
    uint8_t* payload;
    uint64_t target_node_id;
} deferred_msg_t;

typedef void (*araft_forward_response_cb)(uint64_t request_id, uint8_t success);

struct physical_server_s {
    uint64_t physical_node_id;
    uv_loop_t* loop;
    uv_tcp_t listener;

    char data_dir[256];

    uint32_t max_groups;
    araft_node_t** groups;

    peer_connection_t* active_peers[ARAFT_MAX_PEERS];
    uint32_t active_peer_count;

    int meta_fd;
    size_t meta_block_size;
    raft_hard_state_t* meta_ram_array;
    bool metadata_is_dirty;
    uv_check_t loop_check;

    araft_forward_response_cb forward_cb;

    pthread_mutex_t queue_mutex;
    deferred_msg_t deferred_msgs[ARAFT_MAX_DEFERRED_MSGS];
    uint32_t deferred_msg_count;

    // --- NEW: Chaos Engineering Flags ---
    bool network_isolated;
};

typedef enum {
    ARAFT_STATE_FOLLOWER,
    ARAFT_STATE_CANDIDATE,
    ARAFT_STATE_LEADER
} araft_state_t;

typedef struct {
    uint64_t node_id;
    uint64_t next_index;
    uint64_t match_index;
} raft_peer_state_t;

struct araft_node_s {
    uint64_t group_id;
    araft_state_t state;
    physical_server_t* server;
    raft_hard_state_t* meta;
    uint64_t current_leader_id;

    awal_engine_t wal;

    uint64_t last_log_index;
    uint64_t commit_index;

    uint64_t known_leader_commit;

    uint32_t cluster_size;
    uint32_t votes_received;

    raft_peer_state_t peer_state[ARAFT_MAX_PEERS];
    uint32_t peer_state_count;

    uv_timer_t election_timer;
    uv_timer_t heartbeat_timer;
};

int  araft_server_init(physical_server_t* server, uv_loop_t* loop, uint64_t node_id, uint32_t max_groups, const char* data_dir);
int  araft_server_listen(physical_server_t* server, const char* ip, int port);

// --- UPDATED: Pass target_node_id so we know who we are dialing ---
void araft_server_connect(physical_server_t* server, const char* ip, int port, uint64_t target_node_id);

void araft_node_init(araft_node_t* node, physical_server_t* server, uint64_t group_id, uint32_t cluster_size);
void araft_node_start(araft_node_t* node);

void araft_propose(araft_node_t* node, const uint8_t* payload, uint32_t len);
void araft_forward_request(araft_node_t* node, uint64_t request_id, const uint8_t* payload, uint32_t len);
void araft_server_set_forward_cb(physical_server_t* server, araft_forward_response_cb cb);

// --- NEW: Chaos API ---
void araft_server_isolate(physical_server_t* server);
void araft_server_reconnect(physical_server_t* server);

#endif // ARAFT_H
