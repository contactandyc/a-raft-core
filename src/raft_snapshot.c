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

            // Release the leader from stale snapshots instantly
            if (msg->index <= r->last_applied) {
                uint64_t clamp_idx = msg->index > r->last_applied ? msg->index : r->last_applied;
                raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = false, .index = clamp_idx, .snapshot_done = true };
                raft_send_msg(r, res);
                return;
            }

            if (msg->index > r->snapshot_index) {
                // Reject new chunks if the pump hasn't consumed the current one
                if (r->pending_snapshot_chunk_ready) {
                    raft_msg_t res = {
                        .type = MSG_APPEND_RES,
                        .to = msg->from,
                        .term = r->current_term,
                        .reject = true,
                        .conflict_index = r->expected_snapshot_offset - r->pending_snapshot_len
                    };
                    raft_send_msg(r, res);
                    return;
                }

                // Reject explicitly bad payloads before taking action
                if (msg->snapshot_len > 0 && !msg->snapshot_data) {
                    raft_msg_t res = {
                        .type = MSG_APPEND_RES,
                        .to = msg->from,
                        .term = r->current_term,
                        .reject = true,
                        .conflict_index = r->expected_snapshot_offset
                    };
                    raft_send_msg(r, res);
                    return;
                }

                if (msg->snapshot_offset == 0) {
                    r->expected_snapshot_offset = 0;
                    r->pending_snapshot = true;

                    r->pending_snapshot_from = msg->from;
                    r->pending_snapshot_msg_index = msg->index;
                    r->pending_snapshot_msg_term = msg->log_term;
                    r->pending_snapshot_num_peers = msg->snapshot_num_peers;
                    for (size_t i = 0; i < msg->snapshot_num_peers; i++) {
                        r->pending_snapshot_peers[i] = msg->snapshot_peers[i];
                        r->pending_snapshot_is_learner[i] = msg->snapshot_is_learner[i];
                    }
                } else if (!r->pending_snapshot || msg->snapshot_offset != r->expected_snapshot_offset) {
                    // Reject out-of-order chunk and hint expected offset
                    raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .conflict_index = r->expected_snapshot_offset };
                    raft_send_msg(r, res);
                    return;
                }

                // Safely allocate FIRST. If it fails, abort before corrupting state.
                uint8_t* chunk = NULL;
                if (msg->snapshot_len > 0) {
                    chunk = malloc(msg->snapshot_len);
                    if (!chunk) {
                        r->fatal_error = true;
                        return;
                    }
                    memcpy(chunk, msg->snapshot_data, msg->snapshot_len);
                }

                if (r->pending_snapshot_data) free(r->pending_snapshot_data);
                r->pending_snapshot_data = chunk;

                r->expected_snapshot_offset += msg->snapshot_len; // Advance expected offset
                r->pending_snapshot_len = msg->snapshot_len;
                r->pending_snapshot_offset = msg->snapshot_offset;
                r->pending_snapshot_done = msg->snapshot_done;
                r->pending_snapshot_chunk_ready = true;

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

    // Blocker 2: Mark chunk consumed so we don't re-process it!
    r->pending_snapshot_chunk_ready = false;

    raft_msg_t res = { .type = MSG_APPEND_RES, .to = r->pending_snapshot_from, .term = r->current_term,
                       .reject = !success,
                       .index = (success && r->pending_snapshot_done) ? r->pending_snapshot_msg_index : raft_log_last_index(r),
                       .snapshot_done = r->pending_snapshot_done,
                       .conflict_index = r->pending_snapshot_offset + r->pending_snapshot_len };

    if (success && r->pending_snapshot_done) {
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
            r->uncommitted_bytes = 0;
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
                        r->next_index[r->num_peers] = r->snapshot_index + 1;
                        r->match_index[r->num_peers] = 0;
                        r->snapshot_offset[r->num_peers] = 0;
                        r->recent_active[r->num_peers] = false;
                        r->num_peers++;
                    }
                }
            }
            if (!found_self) {
                r->removed = true;
                r->is_learner_self = true;
                r->state = RAFT_STATE_FOLLOWER;
            }
        }
    }

    raft_send_msg(r, res);

    if (r->pending_snapshot_data) free(r->pending_snapshot_data);
    r->pending_snapshot_data = NULL;
    r->pending_snapshot_len = 0;

    if (r->pending_snapshot_done || !success) {
        r->pending_snapshot = false;
        r->pending_snapshot_num_peers = 0;
        r->pending_snapshot_offset = 0;
        r->pending_snapshot_done = false;
    }
}
