// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "raft_internal.h"
#include <stdlib.h>
#include <string.h>

// ============================================================================
// MESSAGE OWNERSHIP & MEMORY HELPERS
// ============================================================================

static void free_message_allocations(raft_msg_t* msg) {
    if (msg->entries) {
        for (size_t i = 0; i < msg->num_entries; i++) {
            free(msg->entries[i].data);
        }
        free(msg->entries);
    }

    free(msg->snapshot_data);
    free(msg->snapshot_peers);
    free(msg->snapshot_is_learner);

    msg->entries = NULL;
    msg->num_entries = 0;
    msg->snapshot_data = NULL;
    msg->snapshot_len = 0;
    msg->snapshot_peers = NULL;
    msg->snapshot_is_learner = NULL;
    msg->snapshot_num_peers = 0;
}

static bool ensure_msg_queue_capacity(raft_t* r) {
    if (r->msg_queue_len < r->msg_queue_cap) return true;

    if (r->msg_queue_cap > (SIZE_MAX / 2) / sizeof(raft_msg_t)) return false;

    size_t new_cap = r->msg_queue_cap == 0 ? 16 : r->msg_queue_cap * 2;
    raft_msg_t* new_q = realloc(r->msg_queue, new_cap * sizeof(raft_msg_t));
    if (!new_q) return false;
    r->msg_queue = new_q;
    r->msg_queue_cap = new_cap;
    return true;
}

static raft_entry_t* create_shallow_log_slice(raft_t* r, uint64_t start_idx, uint64_t end_idx, size_t* out_count) {
    *out_count = 0;
    if (end_idx <= start_idx) return NULL;

    uint64_t diff = end_idx - start_idx;
    if (diff > SIZE_MAX / sizeof(raft_entry_t)) {
        r->fatal_error = true;
        return NULL;
    }

    size_t count = (size_t)diff;
    raft_entry_t* slice = calloc(count, sizeof(raft_entry_t));
    if (!slice) {
        r->fatal_error = true;
        return NULL;
    }

    for (size_t i = 0; i < count; i++) {
        raft_entry_t* src = raft_log_get(r, start_idx + 1 + i);
        if (!src) {
            free(slice);
            r->fatal_error = true;
            return NULL;
        }
        slice[i] = *src;
    }

    *out_count = count;
    return slice;
}

static void free_all_pending_messages(raft_t* r) {
    if (!r->msg_queue) return;
    for (size_t i = 0; i < r->msg_queue_len; i++) {
        free_message_allocations(&r->msg_queue[i]);
    }
    r->msg_queue_len = 0;
}

static void free_entire_log_array(raft_t* r) {
    for (size_t i = 0; i < r->log_len; i++) {
        free(r->log[i].data);
    }
    free(r->log);
    r->log = NULL;
    r->log_len = 0;
    r->log_cap = 0;
}

static size_t calculate_total_topology_size(raft_t* r) {
    return r->num_peers + (!r->removed ? 1 : 0);
}

static bool load_restored_log_entries(raft_t* r, raft_entry_t* entries, size_t num_entries) {
    for (size_t i = 0; i < num_entries; i++) {
        if (entries[i].index <= r->snapshot_index) continue;

        raft_entry_t* e = &r->log[r->log_len++];
        *e = entries[i];
        e->data = NULL;

        if (entries[i].data_len > 0) {
            if (!entries[i].data) return false;
            e->data = malloc(entries[i].data_len);
            if (!e->data) return false;
            memcpy(e->data, entries[i].data, entries[i].data_len);
        }
    }
    return true;
}

// ============================================================================
// RESTORE & MEMBERSHIP HELPERS
// ============================================================================

static bool is_valid_initial_topology(uint64_t self_id, uint64_t* peers, size_t num_peers) {
    if (num_peers > MAX_REMOTE_PEERS) return false;
    if (num_peers > 0 && !peers) return false;

    // Phase 1: Mathematically forbid Node ID 0
    if (self_id == 0) return false;

    for (size_t i = 0; i < num_peers; i++) {
        if (peers[i] == 0 || peers[i] == self_id) return false;
        for (size_t j = i + 1; j < num_peers; j++) {
            if (peers[i] == peers[j]) return false;
        }
    }
    return true;
}

static bool is_valid_restore_topology(uint64_t self_id, uint64_t* peers, size_t num_peers) {
    if (num_peers > MAX_PEERS) return false;
    if (num_peers > 0 && !peers) return false;

    // Phase 1: Mathematically forbid Node ID 0
    if (self_id == 0) return false;

    size_t remote_count = 0;
    for (size_t i = 0; i < num_peers; i++) {
        if (peers[i] == 0) return false;
        if (peers[i] != self_id) remote_count++;
        for (size_t j = i + 1; j < num_peers; j++) {
            if (peers[i] == peers[j]) return false;
        }
    }
    return remote_count <= MAX_REMOTE_PEERS;
}

static bool is_valid_restore_log_sequence(uint64_t snapshot_index, uint64_t snapshot_term, uint64_t current_term, raft_entry_t* entries, size_t num_entries) {
    if (snapshot_index == UINT64_MAX) return false;
    if (num_entries == 0) return true;
    if (!entries) return false;

    if (entries[0].index != snapshot_index + 1) return false;
    if (entries[0].term == 0) return false;
    if (snapshot_index > 0 && entries[0].term < snapshot_term) return false;
    if (entries[0].term > current_term) return false;

    for (size_t i = 1; i < num_entries; i++) {
        if (entries[i].index != entries[i-1].index + 1) return false;
        if (entries[i].term == 0) return false;
        if (entries[i].term < entries[i-1].term) return false;
        if (entries[i].term > current_term) return false;
    }
    return true;
}

static void apply_restore_topology(raft_t* r, uint64_t id, uint64_t* peers, bool* is_learners, size_t num_peers) {
    bool found_self = false;
    for (size_t i = 0; i < num_peers; i++) {
        if (peers[i] == id) {
            r->is_learner_self = is_learners ? is_learners[i] : false;
            found_self = true;
        } else if (r->num_peers < MAX_REMOTE_PEERS) {
            r->peers[r->num_peers] = peers[i];
            r->is_learner[r->num_peers] = is_learners ? is_learners[i] : false;
            r->next_index[r->num_peers] = r->snapshot_index + 1;
            r->num_peers++;
        }
    }

    if (!found_self && num_peers > 0) {
        r->removed = true;
        r->is_learner_self = true;
    }
}

static bool is_known_cluster_member(raft_t* r, uint64_t node_id) {
    if (node_id == r->id && !r->removed) return true;
    for (size_t i = 0; i < r->num_peers; i++) {
        if (r->peers[i] == node_id) return true;
    }
    return false;
}

static bool is_known_voting_member(raft_t* r, uint64_t node_id) {
    if (node_id == r->id && !r->removed && !r->is_learner_self) return true;
    for (size_t i = 0; i < r->num_peers; i++) {
        if (r->peers[i] == node_id && !r->is_learner[i]) return true;
    }
    return false;
}

static bool is_local_message_type(msg_type_t type) {
    return type == MSG_HUP || type == MSG_TICK || type == MSG_PROPOSE ||
           type == MSG_READ_INDEX || type == MSG_CHECK_QUORUM;
}

static bool is_remote_message_sender_allowed(raft_t* r, raft_msg_t* msg) {
    switch (msg->type) {
        case MSG_REQUEST_VOTE:
        case MSG_PRE_VOTE:
        case MSG_REQUEST_VOTE_RES:
        case MSG_PRE_VOTE_RES:
        case MSG_APPEND_ENTRIES:
        case MSG_INSTALL_SNAPSHOT:
        case MSG_READ_INDEX_RES:
            return is_known_voting_member(r, msg->from);

        case MSG_APPEND_RES:
        case MSG_READ_INDEX:
            return is_known_cluster_member(r, msg->from);
        default:
            return false;
    }
}

static bool message_can_advance_term(raft_t* r, raft_msg_t* msg) {
    switch (msg->type) {
        case MSG_APPEND_RES:
            return is_known_voting_member(r, msg->from);
        default:
            return true;
    }
}

static bool is_message_term_stale(raft_t* r, uint64_t msg_term) {
    return msg_term > 0 && msg_term < r->current_term;
}

static void reject_stale_message(raft_t* r, raft_msg_t* msg) {
    raft_msg_t res = { .to = msg->from, .term = r->current_term, .reject = true };
    if (msg->type == MSG_APPEND_ENTRIES || msg->type == MSG_INSTALL_SNAPSHOT) {
        res.type = MSG_APPEND_RES;
        res.index = raft_log_last_index(r);
        raft_send_msg(r, res);
    } else if (msg->type == MSG_REQUEST_VOTE) {
        res.type = MSG_REQUEST_VOTE_RES;
        raft_send_msg(r, res);
    } else if (msg->type == MSG_PRE_VOTE) {
        res.type = MSG_PRE_VOTE_RES;
        raft_send_msg(r, res);
    }
}

static void step_down_to_follower(raft_t* r, uint64_t new_term) {
    r->current_term = new_term;
    r->voted_for = 0;
    r->state = RAFT_STATE_FOLLOWER;
}

static void apply_committed_configurations(raft_t* r, uint64_t applied_index) {
    for (uint64_t i = r->last_applied + 1; i <= applied_index; i++) {
        raft_entry_t* e = raft_log_get(r, i);
        if (!e) {
            r->fatal_error = true;
            return;
        }
        if (e->type == ENTRY_CONF_ADD || e->type == ENTRY_CONF_ADD_LEARNER ||
            e->type == ENTRY_CONF_REMOVE || e->type == ENTRY_CONF_PROMOTE_LEARNER) {
            raft_membership_apply_config(r, i);
        }
    }
}

// ============================================================================
// PUBLIC RAFT API
// ============================================================================

void raft_send_msg(raft_t* r, raft_msg_t msg) {
    if (!ensure_msg_queue_capacity(r)) {
        free_message_allocations(&msg);
        r->fatal_error = true;
        return;
    }
    msg.from = r->id;
    r->msg_queue[r->msg_queue_len++] = msg;
}

raft_t* raft_create(uint64_t id, uint64_t* peers, size_t num_peers) {
    if (!is_valid_initial_topology(id, peers, num_peers)) return NULL;

    raft_t* r = calloc(1, sizeof(raft_t));
    if (!r) return NULL;

    r->id = id;
    r->state = RAFT_STATE_FOLLOWER;
    r->num_peers = num_peers;

    for (size_t i = 0; i < num_peers; i++) {
        r->peers[i] = peers[i];
        r->is_learner[i] = false;
        r->next_index[i] = 1;
        r->snapshot_offset[i] = 0;
    }

    r->log_cap = 16;
    r->log = calloc(r->log_cap, sizeof(raft_entry_t));
    if (!r->log) {
        free(r);
        return NULL;
    }

    r->log[0].index = 0;
    r->log[0].term = 0;
    r->log[0].type = ENTRY_NORMAL;
    r->log[0].client_id = 0;
    r->log[0].client_seq = 0;
    r->log[0].data = NULL;
    r->log[0].data_len = 0;
    r->log_len = 1;

    return r;
}

void raft_destroy(raft_t* r) {
    if (!r) return;
    free_entire_log_array(r);
    free_all_pending_messages(r);
    if (r->msg_queue) free(r->msg_queue);
    if (r->read_states) free(r->read_states);
    if (r->pending_snapshot_data) free(r->pending_snapshot_data);
    free(r);
}

raft_t* raft_restore(uint64_t id, uint64_t* peers, bool* is_learners, size_t num_peers,
                     uint64_t term, uint64_t voted_for, uint64_t commit_index, uint64_t applied_index,
                     uint64_t snapshot_index, uint64_t snapshot_term,
                     raft_entry_t* entries, size_t num_entries) {

    if (num_entries == SIZE_MAX) return NULL;
    if (!is_valid_restore_topology(id, peers, num_peers)) return NULL;
    if (!is_valid_restore_log_sequence(snapshot_index, snapshot_term, term, entries, num_entries)) return NULL;

    if (snapshot_index > 0 && snapshot_term == 0) return NULL;
    if (snapshot_index == 0 && snapshot_term != 0) return NULL;
    if (snapshot_term > term) return NULL;
    if (applied_index < snapshot_index) return NULL;
    if (commit_index < applied_index) return NULL;

    uint64_t last_index = num_entries > 0 ? entries[num_entries - 1].index : snapshot_index;
    if (commit_index > last_index) return NULL;

    raft_t* r = calloc(1, sizeof(raft_t));
    if (!r) return NULL;

    r->id = id;
    r->state = RAFT_STATE_FOLLOWER;
    r->snapshot_index = snapshot_index;
    r->snapshot_term = snapshot_term;
    r->current_term = term;
    r->voted_for = voted_for;

    apply_restore_topology(r, id, peers, is_learners, num_peers);

    if (voted_for != 0 && !is_known_voting_member(r, voted_for)) {
        raft_destroy(r);
        return NULL;
    }

    size_t needed = num_entries + 1;
    if (needed > SIZE_MAX / 2) {
        raft_destroy(r);
        return NULL;
    }

    r->log_cap = needed > 16 ? needed * 2 : 16;
    r->log = calloc(r->log_cap, sizeof(raft_entry_t));
    if (!r->log) {
        raft_destroy(r);
        return NULL;
    }

    r->log[0].index = r->snapshot_index;
    r->log[0].term = r->snapshot_term;
    r->log[0].type = ENTRY_NORMAL;
    r->log[0].client_id = 0;
    r->log[0].client_seq = 0;
    r->log[0].data = NULL;
    r->log[0].data_len = 0;
    r->log_len = 1;

    if (!load_restored_log_entries(r, entries, num_entries)) {
        raft_destroy(r);
        return NULL;
    }

    r->last_saved_index = raft_log_last_index(r);
    r->commit_index = commit_index;
    r->last_applied = applied_index;

    return r;
}

void raft_step_local(raft_t* r, raft_msg_t* msg) {
    if (!r || r->fatal_error) return; // Phase 1: Hard Boundary Protection
    if (msg->to != 0 && msg->to != r->id) return;
    if (msg->from != 0) return;
    if (msg->term != 0) return;
    if (!is_local_message_type(msg->type)) return;

    switch(msg->type) {
        case MSG_HUP:
        case MSG_CHECK_QUORUM:
            raft_election_step(r, msg);
            break;
        case MSG_TICK:
        case MSG_PROPOSE:
            raft_replication_step(r, msg);
            break;
        case MSG_READ_INDEX:
            raft_read_index_step(r, msg);
            break;
        default:
            return;
    }
}

void raft_step_remote(raft_t* r, raft_msg_t* msg) {
    if (!r || r->fatal_error) return; // Phase 1: Hard Boundary Protection
    if (msg->to != r->id) return;
    if (msg->from == 0) return;
    if (msg->term == 0) return;
    if (!is_remote_message_sender_allowed(r, msg)) return;

    if (is_message_term_stale(r, msg->term)) {
        reject_stale_message(r, msg);
        return;
    }

    for (size_t i = 0; i < r->num_peers; i++) {
        if (r->peers[i] == msg->from) {
            r->recent_active[i] = true;
            break;
        }
    }

    if (msg->term > r->current_term &&
        msg->type != MSG_PRE_VOTE &&
        msg->type != MSG_PRE_VOTE_RES) {

        if (!message_can_advance_term(r, msg)) {
            return;
        }
        step_down_to_follower(r, msg->term);
    }

    switch(msg->type) {
        case MSG_PRE_VOTE:
        case MSG_PRE_VOTE_RES:
        case MSG_REQUEST_VOTE:
        case MSG_REQUEST_VOTE_RES:
            raft_election_step(r, msg);
            break;
        case MSG_APPEND_ENTRIES:
        case MSG_APPEND_RES:
            raft_replication_step(r, msg);
            break;
        case MSG_INSTALL_SNAPSHOT:
            raft_snapshot_step(r, msg);
            break;
        case MSG_READ_INDEX:
        case MSG_READ_INDEX_RES:
            raft_read_index_step(r, msg);
            break;
        default:
            return;
    }
}

raft_ready_t raft_get_ready(raft_t* r) {
    raft_ready_t ready;
    memset(&ready, 0, sizeof(ready));
    if (!r || r->fatal_error) return ready;

    ready.messages = r->msg_queue;
    ready.num_messages = r->msg_queue_len;

    uint64_t save_from = r->last_saved_index < r->snapshot_index ? r->snapshot_index : r->last_saved_index;
    ready.entries_to_save = create_shallow_log_slice(r, save_from, raft_log_last_index(r), &ready.num_entries_to_save);
    if (r->fatal_error) {
        memset(&ready, 0, sizeof(ready));
        return ready;
    }

    uint64_t apply_from = r->last_applied < r->snapshot_index ? r->snapshot_index : r->last_applied;
    ready.committed_entries = create_shallow_log_slice(r, apply_from, r->commit_index, &ready.num_committed_entries);
    if (r->fatal_error) {
        if (ready.entries_to_save) free(ready.entries_to_save);
        memset(&ready, 0, sizeof(ready));
        return ready;
    }

    ready.read_states = r->read_states;
    ready.num_read_states = r->num_read_states;

    ready.install_snapshot = r->pending_snapshot;
    ready.snapshot_index = r->pending_snapshot_msg_index;
    ready.snapshot_term = r->pending_snapshot_msg_term;
    ready.snapshot_data = r->pending_snapshot_data;
    ready.snapshot_len = r->pending_snapshot_len;
    ready.snapshot_offset = r->pending_snapshot_offset;
    ready.snapshot_done = r->pending_snapshot_done;

    return ready;
}

void raft_advance(raft_t* r, uint64_t saved_index, uint64_t applied_index) {
    if (!r || r->fatal_error) return; // Phase 1: Hard Boundary Protection

    uint64_t last = raft_log_last_index(r);

    if (saved_index > last || applied_index > r->commit_index || applied_index > last) {
        r->fatal_error = true;
        return;
    }
    if (saved_index < r->last_saved_index) saved_index = r->last_saved_index;
    if (applied_index < r->last_applied) applied_index = r->last_applied;

    uint64_t effective_saved = saved_index > r->last_saved_index ? saved_index : r->last_saved_index;
    if (applied_index > effective_saved) {
        r->fatal_error = true;
        return;
    }

    if (saved_index > r->last_saved_index) r->last_saved_index = saved_index;

    apply_committed_configurations(r, applied_index);
    if (r->fatal_error) return;

    if (applied_index > r->last_applied) r->last_applied = applied_index;

    free_all_pending_messages(r);

    r->num_read_states = 0;
    r->activity_accepted = false;
}

void raft_advance_all_for_tests_only(raft_t* r) {
    if (!r || r->fatal_error) return;
    raft_ready_t ready = raft_get_ready(r);
    if (ready.num_entries_to_save > 0) free(ready.entries_to_save);
    if (ready.num_committed_entries > 0) free(ready.committed_entries);
    raft_advance(r, raft_log_last_index(r), raft_commit_index(r));
}

void raft_compact_after_snapshot(raft_t* r, uint64_t compact_index, uint64_t compact_term) {
    if (!r || r->fatal_error) return;
    if (compact_index <= r->snapshot_index || compact_index > r->last_applied) return;

    if (compact_index > raft_log_last_index(r)) return;
    uint64_t term = raft_log_term(r, compact_index);
    if (term != compact_term || (compact_index > 0 && term == 0)) {
        r->fatal_error = true;
        return;
    }

    uint64_t offset = compact_index - r->snapshot_index;
    if (offset >= r->log_len) {
        r->fatal_error = true;
        return;
    }

    if (r->last_saved_index < compact_index) r->last_saved_index = compact_index;
    if (r->commit_index < compact_index) r->commit_index = compact_index;
    if (r->last_applied < compact_index) r->last_applied = compact_index;

    r->snapshot_term = term;
    size_t keep_len = r->log_len - offset;

    for (uint64_t i = r->snapshot_index + 1; i <= compact_index; i++) {
        raft_entry_t* e = raft_log_get(r, i);
        if (e && e->data) free(e->data);
    }

    memmove(&r->log[1], &r->log[offset + 1], (keep_len - 1) * sizeof(raft_entry_t));
    r->log_len = keep_len;
    r->snapshot_index = compact_index;

    r->log[0].index = compact_index;
    r->log[0].term = r->snapshot_term;
    r->log[0].type = ENTRY_NORMAL;
    r->log[0].client_id = 0;
    r->log[0].client_seq = 0;
    r->log[0].data = NULL;
    r->log[0].data_len = 0;
}

// ----------------------------------------------------------------------------
// GETTERS
// ----------------------------------------------------------------------------

raft_state_t raft_state(raft_t* r) { return r ? r->state : RAFT_STATE_FOLLOWER; }
uint64_t raft_term(raft_t* r) { return r ? r->current_term : 0; }
uint64_t raft_voted_for(raft_t* r) { return r ? r->voted_for : 0; }
uint64_t raft_commit_index(raft_t* r) { return r ? r->commit_index : 0; }
uint64_t raft_last_index(raft_t* r) { return r ? raft_log_last_index(r) : 0; }
uint64_t raft_last_applied(raft_t* r) { return r ? r->last_applied : 0; }
bool raft_activity_accepted(raft_t* r) { return r ? r->activity_accepted : false; }
uint64_t raft_leader_id(raft_t* r) { return r ? r->leader_id : 0; }
uint64_t raft_snapshot_index(raft_t* r) { return r ? r->snapshot_index : 0; }
uint64_t raft_snapshot_term(raft_t* r) { return r ? r->snapshot_term : 0; }
bool raft_has_fatal_error(raft_t* r) { return r ? r->fatal_error : true; }

size_t raft_peers(raft_t* r, uint64_t* out_peers) {
    if (!r) return 0;
    if (out_peers) {
        for (size_t i = 0; i < r->num_peers; i++) out_peers[i] = r->peers[i];
    }
    return r->num_peers;
}

size_t raft_peers_ext(raft_t* r, uint64_t* out_peers, bool* out_is_learners, size_t out_cap) {
    if (!r) return 0;
    size_t required = calculate_total_topology_size(r);

    if ((!out_peers && !out_is_learners) || out_cap < required) {
        return required;
    }

    size_t count = 0;
    for (size_t i = 0; i < r->num_peers; i++) {
        if (out_peers) out_peers[count] = r->peers[i];
        if (out_is_learners) out_is_learners[count] = r->is_learner[i];
        count++;
    }

    if (!r->removed) {
        if (out_peers) out_peers[count] = r->id;
        if (out_is_learners) out_is_learners[count] = r->is_learner_self;
        count++;
    }

    return required;
}
