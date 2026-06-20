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
    }

    raft_msg_t res = { .type = MSG_APPEND_RES, .to = r->pending_snapshot_from, .term = r->current_term,
                       .reject = !success, .index = success ? r->snapshot_index : raft_log_last_index(r) };
    raft_send_msg(r, res);

    if (r->pending_snapshot_data) free(r->pending_snapshot_data);
    r->pending_snapshot_data = NULL;
    r->pending_snapshot_len = 0;
    r->pending_snapshot = false;
}
