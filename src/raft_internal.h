// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef RAFT_INTERNAL_H
#define RAFT_INTERNAL_H

#include "a-raft-library/raft.h"

#define MAX_PEERS 64
#define MAX_REMOTE_PEERS (MAX_PEERS - 1)
#define MAX_PENDING_READS 128

// BLOCKER 6: Unified Batch Limit for Public API and Internal Replication
#define RAFT_MAX_PAYLOAD_SIZE (1048576)

typedef struct {
    uint64_t read_seq;
    uint64_t client_ctx;
    uint64_t index;
    uint64_t from;
    size_t acks;
    bool acked_by[MAX_PEERS];
    bool active;
} pending_read_t;

struct raft_s {
    uint64_t id;
    raft_state_t state;

    uint64_t current_term;
    uint64_t voted_for;

    raft_entry_t* log;
    size_t log_cap;
    size_t log_len;
    uint64_t snapshot_index;
    uint64_t snapshot_term;

    uint64_t commit_index;
    uint64_t last_applied;

    uint64_t peers[MAX_REMOTE_PEERS];
    size_t num_peers;

    uint64_t next_index[MAX_PEERS];
    uint64_t match_index[MAX_PEERS];
    uint64_t snapshot_offset[MAX_PEERS];

    size_t votes_received;
    bool voted_for_me[MAX_PEERS];

    raft_msg_t* msg_queue;
    size_t msg_queue_cap;
    size_t msg_queue_len;

    uint64_t last_saved_index;
    bool activity_accepted;

    uint64_t uncommitted_bytes;

    bool recent_active[MAX_PEERS];
    bool is_learner[MAX_PEERS];
    bool is_learner_self;
    bool removed;

    pending_read_t pending_reads[MAX_PENDING_READS];
    uint64_t current_read_seq;
    uint64_t peer_read_seq[MAX_PEERS];

    raft_read_state_t* read_states;
    size_t read_states_cap;
    size_t num_read_states;

    uint64_t term_start_index;
    uint64_t leader_id;

    bool pending_snapshot;
    uint8_t* pending_snapshot_data;
    size_t pending_snapshot_len;
    uint64_t pending_snapshot_offset;
    bool pending_snapshot_done;
    uint64_t pending_snapshot_from;
    uint64_t pending_snapshot_msg_index;
    uint64_t pending_snapshot_msg_term;

    uint64_t pending_snapshot_peers[MAX_PEERS];
    bool pending_snapshot_is_learner[MAX_PEERS];
    size_t pending_snapshot_num_peers;

    // BLOCKERS 2 & 3: Snapshot chunk tracking
    uint64_t expected_snapshot_offset;
    bool pending_snapshot_chunk_ready;

    // BLOCKER 5: Topology cache for snapshot artifact binding
    uint64_t snapshot_peers_cache[MAX_PEERS];
    bool snapshot_learners_cache[MAX_PEERS];
    size_t snapshot_peers_count;

    bool fatal_error;
};

// Internal Submodule APIs
void raft_send_msg(raft_t* r, raft_msg_t msg);
uint64_t raft_log_last_index(raft_t* r);
uint64_t raft_log_term(raft_t* r, uint64_t index);
raft_entry_t* raft_log_get(raft_t* r, uint64_t index);

bool raft_log_append(raft_t* r, uint64_t term, entry_type_t type, uint64_t cid, uint64_t cseq, const uint8_t* data, size_t data_len);
void raft_log_truncate(raft_t* r, uint64_t index);
uint64_t raft_uncommitted_bytes(raft_t* r);

void raft_election_step(raft_t* r, raft_msg_t* msg);
void raft_election_become_leader(raft_t* r);

void raft_replication_step(raft_t* r, raft_msg_t* msg);
void raft_replication_bcast_append(raft_t* r);
void raft_replication_advance_commit(raft_t* r, uint64_t new_commit);

void raft_snapshot_step(raft_t* r, raft_msg_t* msg);
void raft_read_index_step(raft_t* r, raft_msg_t* msg);
void raft_membership_apply_config(raft_t* r, uint64_t index);

void raft_advance_all_for_tests_only(raft_t* r);

void raft_read_index_ack(raft_t* r, size_t peer_idx, uint64_t read_seq);

void raft_add_learner(raft_t* r, uint64_t peer_id);
void raft_promote_learner(raft_t* r, uint64_t peer_id);

#endif // RAFT_INTERNAL_H
