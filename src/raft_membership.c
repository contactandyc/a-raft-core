// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "raft_internal.h"

void raft_membership_apply_config(raft_t* r, uint64_t index) {
    size_t arr_idx = index - r->snapshot_index;
    if (arr_idx >= r->log_len) return;

    uint8_t type = r->log[arr_idx].type;
    uint8_t* data = r->log[arr_idx].data;
    uint32_t data_len = r->log[arr_idx].data_len;

    if (type == ENTRY_CONF_ADD_LEARNER) {
        if (data_len == sizeof(uint64_t)) {
            uint64_t node_id = *(uint64_t*)data;
            if (node_id == r->id) r->is_learner_self = true;

            bool found = false;
            for (size_t j = 0; j < r->num_peers; j++) {
                if (r->peers[j] == node_id) {
                    r->is_learner[j] = true;
                    found = true;
                    break;
                }
            }
            if (!found && node_id != r->id && r->num_peers < MAX_PEERS) {
                r->peers[r->num_peers] = node_id;
                r->is_learner[r->num_peers] = true;
                r->next_index[r->num_peers] = raft_log_last_index(r) + 1;
                r->match_index[r->num_peers] = 0;
                r->recent_active[r->num_peers] = true;
                r->num_peers++;
            }
        }
    } else if (type == ENTRY_CONF_PROMOTE_LEARNER) {
        if (data_len == sizeof(uint64_t)) {
            uint64_t node_id = *(uint64_t*)data;
            if (node_id == r->id) r->is_learner_self = false;
            for (size_t j = 0; j < r->num_peers; j++) {
                if (r->peers[j] == node_id) {
                    r->is_learner[j] = false;
                    break;
                }
            }
        }
    } else if (type == ENTRY_CONF_REMOVE) {
        if (data_len == sizeof(uint64_t)) {
            uint64_t node_id = *(uint64_t*)data;
            if (node_id == r->id) {
                if (r->state == RAFT_STATE_LEADER) r->state = RAFT_STATE_FOLLOWER;
                r->is_learner_self = true;
                r->removed = true;
            } else {
                for (size_t i = 0; i < r->num_peers; i++) {
                    if (r->peers[i] == node_id) {
                        r->num_peers--;
                        r->peers[i] = r->peers[r->num_peers];
                        r->is_learner[i] = r->is_learner[r->num_peers];
                        r->next_index[i] = r->next_index[r->num_peers];
                        r->match_index[i] = r->match_index[r->num_peers];
                        r->recent_active[i] = r->recent_active[r->num_peers];
                        break;
                    }
                }
            }
        }
    }
}

void raft_add_learner(raft_t* r, uint64_t peer_id) {
    for (size_t i = 0; i < r->num_peers; i++) {
        if (r->peers[i] == peer_id) {
            r->is_learner[i] = true;
            return;
        }
    }
    if (r->num_peers < MAX_PEERS) {
        r->peers[r->num_peers] = peer_id;
        r->is_learner[r->num_peers] = true;
        r->next_index[r->num_peers] = raft_log_last_index(r) + 1;
        r->match_index[r->num_peers] = 0;
        r->recent_active[r->num_peers] = false;
        r->peer_read_seq[r->num_peers] = 0;
        r->num_peers++;
    }
}

void raft_promote_learner(raft_t* r, uint64_t peer_id) {
    for (size_t i = 0; i < r->num_peers; i++) {
        if (r->peers[i] == peer_id) {
            r->is_learner[i] = false;
            return;
        }
    }
}
