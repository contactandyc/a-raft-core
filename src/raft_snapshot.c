// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "raft_internal.h"
#include <stdlib.h>
#include <string.h>

void raft_snapshot_step(raft_t* r, raft_msg_t* msg) {
    if (msg->type == MSG_INSTALL_SNAPSHOT) {
        if (msg->term >= r->current_term) {
            r->state = RAFT_STATE_FOLLOWER;
            r->activity_accepted = true;
            r->leader_id = msg->from;

            if (msg->index > r->snapshot_index) {
                r->pending_snapshot = true;
                r->pending_snapshot_from = msg->from;
                r->pending_snapshot_msg_index = msg->index;
                r->pending_snapshot_msg_term = msg->log_term;

                if (r->pending_snapshot_data) free(r->pending_snapshot_data);
                r->pending_snapshot_data = msg->snapshot_len > 0 ? malloc(msg->snapshot_len) : NULL;
                if (msg->snapshot_len > 0 && r->pending_snapshot_data) {
                    memcpy(r->pending_snapshot_data, msg->snapshot_data, msg->snapshot_len);
                }
                r->pending_snapshot_len = msg->snapshot_len;

                // PHASE 17: Stage the newly received ConfState topology!
                r->pending_snapshot_num_peers = msg->snapshot_num_peers;
                for (size_t i = 0; i < msg->snapshot_num_peers; i++) {
                    r->pending_snapshot_peers[i] = msg->snapshot_peers[i];
                    r->pending_snapshot_is_learner[i] = msg->snapshot_is_learner[i];
                }

            } else {
                raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = false, .index = msg->index };
                raft_send_msg(r, res);
            }
        } else {
            raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = raft_log_last_index(r) };
            raft_send_msg(r, res);
        }
    }
}

void raft_snapshot_acked(raft_t* r, bool success) {
    if (!r->pending_snapshot) return;

    if (success) {
        bool suffix_match = false;
        uint64_t my_last_idx = raft_log_last_index(r);

        if (r->pending_snapshot_msg_index <= my_last_idx && raft_log_term(r, r->pending_snapshot_msg_index) == r->pending_snapshot_msg_term) {
            suffix_match = true;
        }

        if (suffix_match) {
            size_t keep_len = my_last_idx - r->pending_snapshot_msg_index + 1;
            for (uint64_t i = r->snapshot_index + 1; i <= r->pending_snapshot_msg_index; i++) {
                raft_entry_t* e = raft_log_get(r, i);
                if (e && e->data) free(e->data);
            }
            memmove(&r->log[1], &r->log[r->pending_snapshot_msg_index - r->snapshot_index + 1], (keep_len - 1) * sizeof(raft_entry_t));
            r->log_len = keep_len;
        } else {
            for (size_t i = 0; i < r->log_len; i++) {
                if (r->log[i].data) free(r->log[i].data);
            }
            r->log_len = 1;
        }

        r->snapshot_index = r->pending_snapshot_msg_index;
        r->snapshot_term = r->pending_snapshot_msg_term;
        r->log[0].index = r->snapshot_index;
        r->log[0].term = r->snapshot_term;
        r->log[0].data = NULL;
        r->log[0].data_len = 0;

        if (r->last_saved_index < r->snapshot_index) r->last_saved_index = r->snapshot_index;
        if (r->commit_index < r->snapshot_index) r->commit_index = r->snapshot_index;
        if (r->last_applied < r->snapshot_index) r->last_applied = r->snapshot_index;

        // PHASE 17: Execute Atomic Topology Overwrite!
        if (r->pending_snapshot_num_peers > 0) {
            r->num_peers = 0;
            bool found_self = false;
            for (size_t i = 0; i < r->pending_snapshot_num_peers; i++) {
                if (r->pending_snapshot_peers[i] == r->id) {
                    r->is_learner_self = r->pending_snapshot_is_learner[i];
                    r->removed = false;
                    found_self = true;
                } else {
                    if (r->num_peers < MAX_PEERS) {
                        r->peers[r->num_peers] = r->pending_snapshot_peers[i];
                        r->is_learner[r->num_peers] = r->pending_snapshot_is_learner[i];
                        // Boot new nodes at snapshot_index + 1, retaining match_index for existing ones if possible
                        r->next_index[r->num_peers] = r->snapshot_index + 1;
                        r->match_index[r->num_peers] = 0;
                        r->recent_active[r->num_peers] = false;
                        r->num_peers++;
                    }
                }
            }
            if (!found_self) {
                // We are no longer part of the cluster according to the latest snapshot
                r->removed = true;
                r->is_learner_self = true;
                r->state = RAFT_STATE_FOLLOWER;
            }
        }
    }

    raft_msg_t res = { .type = MSG_APPEND_RES, .to = r->pending_snapshot_from, .term = r->current_term,
                       .reject = !success, .index = success ? r->snapshot_index : raft_log_last_index(r) };
    raft_send_msg(r, res);

    if (r->pending_snapshot_data) free(r->pending_snapshot_data);
    r->pending_snapshot_data = NULL;
    r->pending_snapshot_len = 0;
    r->pending_snapshot = false;
    r->pending_snapshot_num_peers = 0;
}
