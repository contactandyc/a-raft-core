// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef RAFT_CORE_H
#define RAFT_CORE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

typedef enum {
    RAFT_STATE_FOLLOWER,
    RAFT_STATE_PRE_CANDIDATE, // PHASE 3: Gap 10
    RAFT_STATE_CANDIDATE,
    RAFT_STATE_LEADER
} raft_state_t;

typedef enum {
    MSG_HUP,
    MSG_TICK,
    MSG_PROPOSE,
    MSG_APPEND_ENTRIES,
    MSG_APPEND_RES,
    MSG_REQUEST_VOTE,
    MSG_REQUEST_VOTE_RES,
    MSG_PRE_VOTE,         // PHASE 3: Gap 10
    MSG_PRE_VOTE_RES,     // PHASE 3: Gap 10
    MSG_INSTALL_SNAPSHOT
} msg_type_t;

typedef enum {
    ENTRY_NORMAL = 0,
    ENTRY_CONF_ADD = 1,
    ENTRY_CONF_REMOVE = 2
} entry_type_t;

typedef struct {
    uint64_t term;
    uint64_t index;
    entry_type_t type;
    uint8_t* data;
    size_t   data_len;
} raft_entry_t;

typedef struct {
    msg_type_t type;
    uint64_t to;
    uint64_t from;
    uint64_t term;
    uint64_t log_term;
    uint64_t index;
    uint64_t commit;

    // PHASE 3 (Gap 9): Conflict Hints for O(1) Backtracking
    uint64_t conflict_term;
    uint64_t conflict_index;

    raft_entry_t* entries;
    size_t num_entries;

    uint8_t* snapshot_data;
    size_t snapshot_len;

    bool reject;
} raft_msg_t;

typedef struct {
    raft_msg_t* messages;
    size_t num_messages;

    raft_entry_t* entries_to_save;
    size_t num_entries_to_save;

    raft_entry_t* committed_entries;
    size_t num_committed_entries;
} raft_ready_t;

typedef struct raft_core_s raft_core_t;

raft_core_t* raft_core_create(uint64_t id, uint64_t* peers, size_t num_peers);
void         raft_core_destroy(raft_core_t* r);

raft_core_t* raft_core_restore(uint64_t id, uint64_t* peers, size_t num_peers,
                               uint64_t term, uint64_t voted_for, uint64_t commit_index, uint64_t applied_index,
                               raft_entry_t* entries, size_t num_entries);

void         raft_core_step(raft_core_t* r, raft_msg_t* msg);
raft_ready_t raft_core_get_ready(raft_core_t* r);

void         raft_core_advance(raft_core_t* r, uint64_t saved_index, uint64_t applied_index);
void         raft_core_advance_all(raft_core_t* r);

void         raft_core_compact(raft_core_t* r, uint64_t compact_index);

raft_state_t raft_core_state(raft_core_t* r);
uint64_t     raft_core_term(raft_core_t* r);
uint64_t     raft_core_voted_for(raft_core_t* r);
uint64_t     raft_core_commit_index(raft_core_t* r);
uint64_t     raft_core_last_index(raft_core_t* r);
uint64_t     raft_core_last_applied(raft_core_t* r);
bool         raft_core_activity_accepted(raft_core_t* r);

size_t       raft_core_peers(raft_core_t* r, uint64_t* out_peers);

#endif // RAFT_CORE_H
