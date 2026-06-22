// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "raft_internal.h"
#include <string.h>

// ============================================================================
// MEMBERSHIP DATA HELPERS
// ============================================================================

// Safely extracts a uint64_t node ID from the payload of a configuration entry.
// Rejects node ID 0 as it is reserved for 'no sender/no vote'.
static bool extract_node_id(raft_entry_t* e, uint64_t* out_id) {
    if (!e->data || e->data_len != sizeof(uint64_t)) return false;
    memcpy(out_id, e->data, sizeof(uint64_t));
    return *out_id != 0;
}

// Searches the active remote peer topology for a specific node ID.
static bool find_peer_index(raft_t* r, uint64_t node_id, size_t* out_idx) {
    for (size_t i = 0; i < r->num_peers; i++) {
        if (r->peers[i] == node_id) {
            *out_idx = i;
            return true;
        }
    }
    return false;
}

// Clears pending ReadIndex requests. Because membership changes alter quorum,
// any reads collected under the old voter set are no longer safe to serve.
static void clear_read_index_state(raft_t* r) {
    for (size_t i = 0; i < MAX_PENDING_READS; i++) {
        r->pending_reads[i].active = false;
        r->pending_reads[i].acks = 0;
        memset(r->pending_reads[i].acked_by, 0, sizeof(r->pending_reads[i].acked_by));
    }
    r->num_read_states = 0;
}

// ============================================================================
// TOPOLOGY MUTATORS
// ============================================================================

void raft_add_learner(raft_t* r, uint64_t peer_id) {
    if (peer_id == r->id) {
        if (r->removed) {
            r->removed = false;
            r->is_learner_self = true;
        }
        return;
    }

    size_t idx;
    if (find_peer_index(r, peer_id, &idx)) {
        // Node already exists. Do not demote a voter!
        return;
    }

    // Add brand new remote learner to the topology array
    if (r->num_peers < MAX_REMOTE_PEERS) {
        size_t i = r->num_peers;
        r->peers[i] = peer_id;
        r->is_learner[i] = true;

        // Assume the new learner has no logs, start matching from the current tail
        r->next_index[i] = raft_log_last_index(r) + 1;
        r->match_index[i] = 0;

        r->recent_active[i] = true; // Assume active to allow initial probe
        r->peer_read_seq[i] = 0;
        r->voted_for_me[i] = false;
        r->num_peers++;
    }
}

void raft_promote_learner(raft_t* r, uint64_t peer_id) {
    if (peer_id == r->id) {
        if (r->is_learner_self && !r->removed) {
            r->is_learner_self = false;
        }
        return;
    }

    size_t idx;
    if (find_peer_index(r, peer_id, &idx)) {
        if (r->is_learner[idx]) {
            r->is_learner[idx] = false; // Grants the peer voting rights
            r->recent_active[idx] = false; // Must freshly prove activity as a voter
        }
    }
}

static void execute_remove_node(raft_t* r, uint64_t peer_id) {
    if (peer_id == r->id) {
        if (r->state == RAFT_STATE_LEADER) {
            r->state = RAFT_STATE_FOLLOWER;
            r->leader_id = 0; // Explicitly drop authority
        }
        r->is_learner_self = true;
        r->removed = true;
        clear_read_index_state(r);
        return;
    }

    size_t idx;
    if (!find_peer_index(r, peer_id, &idx)) return;

    size_t last = r->num_peers - 1;

    if (idx != last) {
        r->peers[idx] = r->peers[last];
        r->is_learner[idx] = r->is_learner[last];
        r->next_index[idx] = r->next_index[last];
        r->match_index[idx] = r->match_index[last];
        r->recent_active[idx] = r->recent_active[last];
        r->peer_read_seq[idx] = r->peer_read_seq[last];
        r->voted_for_me[idx] = r->voted_for_me[last];
    }

    // Clear the trailing slot to prevent stale data ghosts
    r->peers[last] = 0;
    r->is_learner[last] = false;
    r->next_index[last] = 0;
    r->match_index[last] = 0;
    r->recent_active[last] = false;
    r->peer_read_seq[last] = 0;
    r->voted_for_me[last] = false;

    r->num_peers--;
    clear_read_index_state(r);
}

// ============================================================================
// PUBLIC CONFIGURATION ROUTER
// ============================================================================

void raft_membership_apply_config(raft_t* r, uint64_t index) {
    raft_entry_t* e = raft_log_get(r, index);
    if (!e) return;

    if (e->type == ENTRY_CONF_ADD_LEARNER ||
        e->type == ENTRY_CONF_PROMOTE_LEARNER ||
        e->type == ENTRY_CONF_REMOVE) {

        uint64_t node_id;
        if (!extract_node_id(e, &node_id)) {
            r->fatal_error = true;
            return;
        }

        switch (e->type) {
            case ENTRY_CONF_ADD_LEARNER:
                raft_add_learner(r, node_id);
                clear_read_index_state(r);
                break;

            case ENTRY_CONF_PROMOTE_LEARNER:
                raft_promote_learner(r, node_id);
                clear_read_index_state(r);
                break;

            case ENTRY_CONF_REMOVE:
                execute_remove_node(r, node_id);
                break;

            default:
                break;
        }
    }
}
