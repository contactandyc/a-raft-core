// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "raft_internal.h"
#include <stdlib.h>
#include <string.h>

// ============================================================================
// MEMORY & MATH HELPERS
// ============================================================================

static bool self_is_voter(raft_t* r) {
    return !r->removed && !r->is_learner_self;
}

// Counts the total number of active voting members in the cluster.
static size_t count_total_voters(raft_t* r) {
    size_t total = self_is_voter(r) ? 1 : 0;
    for (size_t i = 0; i < r->num_peers; i++) {
        if (!r->is_learner[i]) total++;
    }
    return total;
}

// Safely appends a successfully verified ReadIndex state to the ready queue.
// Fails closed by triggering a fatal error if the host is out of memory.
static void queue_local_read_state(raft_t* r, uint64_t read_seq, uint64_t index) {
    if (r->num_read_states >= r->read_states_cap) {
        size_t new_cap = r->read_states_cap == 0 ? 16 : r->read_states_cap * 2;

        // Strict boundary protection against integer overflow
        if (new_cap < r->read_states_cap || new_cap > SIZE_MAX / sizeof(raft_read_state_t)) {
            r->fatal_error = true;
            return;
        }

        raft_read_state_t* new_rs = realloc(r->read_states, new_cap * sizeof(raft_read_state_t));
        if (!new_rs) {
            r->fatal_error = true;
            return;
        }
        r->read_states = new_rs;
        r->read_states_cap = new_cap;
    }

    r->read_states[r->num_read_states].read_seq = read_seq;
    r->read_states[r->num_read_states].index = index;
    r->num_read_states++;
}

// Dispatches a completed read. If the read originated on this node, it queues
// it locally. If it was forwarded by a follower, it sends the response packet back.
static void complete_pending_read(raft_t* r, pending_read_t* pr) {
    if (pr->from == r->id) {
        queue_local_read_state(r, pr->client_ctx, pr->index);
    } else {
        raft_msg_t res = {
            .type = MSG_READ_INDEX_RES,
            .to = pr->from,
            .term = r->current_term,
            .read_seq = pr->client_ctx,
            .index = pr->index,
            .reject = false
        };
        raft_send_msg(r, res);
    }
    pr->active = false;
}

// ============================================================================
// INBOUND HANDLERS
// ============================================================================

// Evaluates a new ReadIndex request from a client or a forwarded follower.
static void handle_read_index_request(raft_t* r, raft_msg_t* msg) {
    if (r->state != RAFT_STATE_LEADER || !self_is_voter(r)) return;

    // Raft Safety: A leader must commit at least one entry in its current term
    // before it can establish a definitive read horizon.
    if (r->commit_index < r->term_start_index || raft_log_term(r, r->commit_index) != r->current_term) {
        return;
    }

    if (r->current_read_seq == UINT64_MAX) {
        r->fatal_error = true;
        return;
    }

    r->current_read_seq++;
    pending_read_t* pr = NULL;

    for (int i = 0; i < MAX_PENDING_READS; i++) {
        if (!r->pending_reads[i].active) {
            pr = &r->pending_reads[i];
            break;
        }
    }

    // If the queue is saturated, silently drop the read to apply backpressure.
    if (!pr) return;

    pr->active = true;
    pr->read_seq = r->current_read_seq;
    pr->client_ctx = msg->read_seq;
    pr->index = r->commit_index;
    pr->from = msg->from != 0 ? msg->from : r->id;
    pr->acks = 1; // The leader trivially acknowledges its own read
    memset(pr->acked_by, 0, sizeof(pr->acked_by));

    size_t total_voters = count_total_voters(r);
    if (total_voters == 1) {
        // Single-node clusters inherently possess quorum
        complete_pending_read(r, pr);
    } else if (total_voters > 1) {
        // Multi-node clusters must broadcast an empty AppendEntries heartbeat
        // to prove they have not been deposed by a network partition.
        raft_replication_bcast_append(r);
    } else {
        // Total voters == 0 is impossible if self_is_voter passed, but fail closed.
        pr->active = false;
    }
}

// Handles the response to a forwarded ReadIndex request on a follower.
static void handle_read_index_response(raft_t* r, raft_msg_t* msg) {
    if (msg->reject || msg->from != r->leader_id || msg->term != r->current_term) {
        return;
    }

    // Strict isolation: Followers must verify they actually asked for this read.
    // We repurpose the pending_reads array locally for followers to track forwarded reads.
    bool valid_match = false;
    for (int i = 0; i < MAX_PENDING_READS; i++) {
        if (r->pending_reads[i].active && r->pending_reads[i].client_ctx == msg->read_seq) {
            valid_match = true;
            r->pending_reads[i].active = false;
            break;
        }
    }

    if (valid_match) {
        queue_local_read_state(r, msg->read_seq, msg->index);
    }
}

// ============================================================================
// PUBLIC ROUTER & HOOKS
// ============================================================================

void raft_read_index_step(raft_t* r, raft_msg_t* msg) {
    switch (msg->type) {
        case MSG_READ_INDEX:
            // Since this API now accepts remote MSG_READ_INDEX via raft_step_remote,
            // we track the remote forwarder using msg->from.
            handle_read_index_request(r, msg);
            break;
        case MSG_READ_INDEX_RES:
            handle_read_index_response(r, msg);
            break;
        default:
            break;
    }
}

// Process valid AppendResponses forwarded from the Replication module.
void raft_read_index_ack(raft_t* r, size_t peer_idx, uint64_t read_seq) {
    if (r->state != RAFT_STATE_LEADER) return;

    if (read_seq > r->peer_read_seq[peer_idx]) {
        r->peer_read_seq[peer_idx] = read_seq;
    }

    for (int pr = 0; pr < MAX_PENDING_READS; pr++) {
        if (r->pending_reads[pr].active && !r->pending_reads[pr].acked_by[peer_idx]) {
            if (r->peer_read_seq[peer_idx] >= r->pending_reads[pr].read_seq) {

                r->pending_reads[pr].acked_by[peer_idx] = true;

                // Exclude learners from read quorums
                if (!r->is_learner[peer_idx]) {
                    r->pending_reads[pr].acks++;
                }

                size_t voters = count_total_voters(r);
                if (r->pending_reads[pr].acks >= (voters / 2) + 1) {
                    complete_pending_read(r, &r->pending_reads[pr]);
                }
            }
        }
    }
}
