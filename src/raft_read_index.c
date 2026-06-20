// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "raft_internal.h"
#include <stdlib.h>

void raft_read_index_step(raft_t* r, raft_msg_t* msg) {
    if (msg->type == MSG_READ_INDEX) {
        if (r->state != RAFT_STATE_LEADER) return;
        if (r->commit_index < r->term_start_index || raft_log_term(r, r->commit_index) != r->current_term) return;

        r->current_read_seq++;
        bool slot_found = false;
        for (int pr = 0; pr < MAX_PENDING_READS; pr++) {
            if (!r->pending_reads[pr].active) {
                r->pending_reads[pr].active = true;
                r->pending_reads[pr].read_seq = r->current_read_seq;
                r->pending_reads[pr].client_ctx = msg->read_seq;
                r->pending_reads[pr].index = r->commit_index;
                r->pending_reads[pr].from = msg->from != 0 ? msg->from : r->id;
                r->pending_reads[pr].acks = 1;
                for(int z=0; z<MAX_PEERS; z++) r->pending_reads[pr].acked_by[z] = false;
                slot_found = true;
                break;
            }
        }

        if (slot_found) {
            size_t total_voters = r->is_learner_self ? 0 : 1;
            for(size_t v = 0; v < r->num_peers; v++) if (!r->is_learner[v]) total_voters++;

            if (total_voters <= 1) {
                for (int pr = 0; pr < MAX_PENDING_READS; pr++) {
                    if (r->pending_reads[pr].active && r->pending_reads[pr].read_seq == r->current_read_seq) {
                        if (r->pending_reads[pr].from == r->id) {
                            if (r->num_read_states >= r->read_states_cap) {
                                r->read_states_cap = r->read_states_cap == 0 ? 16 : r->read_states_cap * 2;
                                raft_read_state_t* new_rs = realloc(r->read_states, r->read_states_cap * sizeof(raft_read_state_t));
                                if (new_rs) r->read_states = new_rs;
                            }
                            if (r->read_states) {
                                r->read_states[r->num_read_states].index = r->pending_reads[pr].index;
                                r->read_states[r->num_read_states].read_seq = r->pending_reads[pr].client_ctx;
                                r->num_read_states++;
                            }
                        } else {
                            raft_msg_t res = { .type = MSG_READ_INDEX_RES, .to = r->pending_reads[pr].from,
                                               .read_seq = r->pending_reads[pr].client_ctx, .index = r->pending_reads[pr].index, .reject = false };
                            raft_send_msg(r, res);
                        }
                        r->pending_reads[pr].active = false;
                    }
                }
            } else {
                raft_replication_bcast_append(r);
            }
        }
    }
    else if (msg->type == MSG_READ_INDEX_RES) {
        if (msg->reject || msg->from != r->leader_id || msg->term != r->current_term) return;

        if (r->num_read_states >= r->read_states_cap) {
            r->read_states_cap = r->read_states_cap == 0 ? 16 : r->read_states_cap * 2;
            raft_read_state_t* new_rs = realloc(r->read_states, r->read_states_cap * sizeof(raft_read_state_t));
            if (new_rs) r->read_states = new_rs;
        }
        if (r->read_states) {
            r->read_states[r->num_read_states].index = msg->index;
            r->read_states[r->num_read_states].read_seq = msg->read_seq;
            r->num_read_states++;
        }
    }
}
