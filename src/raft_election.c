// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "raft_internal.h"
#include <string.h>

// ============================================================================
// QUORUM & MATH HELPERS
// ============================================================================

static bool self_is_voter(raft_t* r) {
    return !r->removed && !r->is_learner_self;
}

// Counts the total number of active voting members in the cluster.
// Learners and removed nodes are strictly excluded from the denominator.
static size_t count_total_voters(raft_t* r) {
    size_t total = self_is_voter(r) ? 1 : 0;
    for (size_t i = 0; i < r->num_peers; i++) {
        if (!r->is_learner[i]) total++;
    }
    return total;
}

// Determines if a candidate's log is up-to-date enough to grant them a vote.
static bool is_candidate_log_up_to_date(raft_t* r, uint64_t candidate_index, uint64_t candidate_term) {
    uint64_t my_last_idx = raft_log_last_index(r);
    uint64_t my_last_term = raft_log_term(r, my_last_idx);

    if (candidate_term != my_last_term) {
        return candidate_term > my_last_term;
    }
    return candidate_index >= my_last_idx;
}

// Registers an incoming vote from a specific peer. Protects against double-counting
// and ignores votes from non-voting learners. Returns true if a quorum is reached.
static bool register_vote_and_check_quorum(raft_t* r, uint64_t peer_id) {
    for (size_t i = 0; i < r->num_peers; i++) {
        if (r->peers[i] == peer_id && !r->voted_for_me[i] && !r->is_learner[i]) {
            r->voted_for_me[i] = true;
            r->recent_active[i] = true;
            r->votes_received++;

            size_t quorum_required = (count_total_voters(r) / 2) + 1;
            return r->votes_received >= quorum_required;
        }
    }
    return false;
}

// ============================================================================
// STATE TRANSITIONS & CAMPAIGNING
// ============================================================================

// Transitions the node to the Leader state. Asserts authority, initializes
// tracking, appends a no-op, and broadcasts its new status.
// Guarded against dynamic membership mid-flight demotions.
void raft_election_become_leader(raft_t* r) {
    if (!self_is_voter(r)) return;

    r->state = RAFT_STATE_LEADER;
    r->activity_accepted = true;
    r->leader_id = r->id;

    uint8_t dummy = 0;
    raft_log_append(r, r->current_term, ENTRY_NORMAL, 0, 0, &dummy, 0);
    r->term_start_index = raft_log_last_index(r);

    for (size_t i = 0; i < r->num_peers; i++) {
        r->next_index[i] = r->term_start_index + 1;
        r->match_index[i] = 0;
    }

    size_t total_voters = count_total_voters(r);
    if (total_voters <= 1) {
        raft_replication_advance_commit(r, r->term_start_index);
    }

    raft_replication_bcast_append(r);
}

// Initiates the Pre-Vote phase to see if a real campaign would be viable.
static void campaign_for_pre_votes(raft_t* r) {
    r->state = RAFT_STATE_PRE_CANDIDATE;
    r->votes_received = 1; // We always pre-vote for ourselves
    memset(r->voted_for_me, 0, sizeof(r->voted_for_me));

    uint64_t last_idx = raft_log_last_index(r);
    uint64_t last_term = raft_log_term(r, last_idx);

    for (size_t i = 0; i < r->num_peers; i++) {
        if (r->is_learner[i]) continue;
        raft_msg_t req = { .type = MSG_PRE_VOTE, .to = r->peers[i], .term = r->current_term + 1,
                           .index = last_idx, .log_term = last_term };
        raft_send_msg(r, req);
    }
}

// Initiates a formal Raft election. Guarded against dynamic mid-flight demotions.
static void campaign_for_hard_votes(raft_t* r) {
    if (!self_is_voter(r)) return;

    r->state = RAFT_STATE_CANDIDATE;
    r->current_term++;
    r->voted_for = r->id;
    r->votes_received = 1; // We always vote for ourselves
    memset(r->voted_for_me, 0, sizeof(r->voted_for_me));

    uint64_t last_idx = raft_log_last_index(r);
    uint64_t last_term = raft_log_term(r, last_idx);

    for (size_t i = 0; i < r->num_peers; i++) {
        if (r->is_learner[i]) continue;
        raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = r->peers[i], .term = r->current_term,
                           .index = last_idx, .log_term = last_term };
        raft_send_msg(r, req);
    }
}

// ============================================================================
// MESSAGE HANDLERS
// ============================================================================

static void handle_hup(raft_t* r) {
    if (r->state == RAFT_STATE_LEADER || !self_is_voter(r)) return;

    if (count_total_voters(r) == 1) {
        r->state = RAFT_STATE_CANDIDATE;
        r->current_term++;
        r->voted_for = r->id;
        raft_election_become_leader(r);
    } else {
        campaign_for_pre_votes(r);
    }
}

static void handle_pre_vote(raft_t* r, raft_msg_t* msg) {
    raft_msg_t res = { .type = MSG_PRE_VOTE_RES, .to = msg->from, .term = r->current_term, .reject = true };

    bool log_ok = is_candidate_log_up_to_date(r, msg->index, msg->log_term);

    if (msg->term > r->current_term && log_ok && self_is_voter(r)) {
        res.reject = false;
        res.term = msg->term;
    }

    raft_send_msg(r, res);
}

static void handle_pre_vote_response(raft_t* r, raft_msg_t* msg) {
    if (r->state != RAFT_STATE_PRE_CANDIDATE) return;

    if (msg->reject) {
        if (msg->term > r->current_term) {
            r->current_term = msg->term;
            r->voted_for = 0;
            r->state = RAFT_STATE_FOLLOWER;
        }
        return;
    }

    if (msg->term == r->current_term + 1) {
        if (register_vote_and_check_quorum(r, msg->from)) {
            campaign_for_hard_votes(r);
        }
    }
}

static void handle_request_vote(raft_t* r, raft_msg_t* msg) {
    raft_msg_t res = { .type = MSG_REQUEST_VOTE_RES, .to = msg->from, .term = r->current_term, .reject = true };

    bool log_ok = is_candidate_log_up_to_date(r, msg->index, msg->log_term);
    bool can_vote = (r->voted_for == 0 || r->voted_for == msg->from);

    if (msg->term == r->current_term && can_vote && log_ok && self_is_voter(r)) {
        r->voted_for = msg->from;
        res.reject = false;
        r->activity_accepted = true;
    }

    raft_send_msg(r, res);
}

static void handle_request_vote_response(raft_t* r, raft_msg_t* msg) {
    if (r->state != RAFT_STATE_CANDIDATE || msg->reject) return;
    if (msg->term != r->current_term) return;

    if (register_vote_and_check_quorum(r, msg->from)) {
        raft_election_become_leader(r);
    }
}

static void handle_check_quorum(raft_t* r) {
    if (r->state != RAFT_STATE_LEADER) return;

    size_t active_voters = self_is_voter(r) ? 1 : 0;
    size_t total_voters = count_total_voters(r);

    for (size_t i = 0; i < r->num_peers; i++) {
        if (!r->is_learner[i] && r->recent_active[i]) {
            active_voters++;
        }
        r->recent_active[i] = false;
    }

    if (active_voters < (total_voters / 2) + 1) {
        r->state = RAFT_STATE_FOLLOWER;
        r->leader_id = 0; // Explicitly clear leader identity upon partition failure
    }
}

// ============================================================================
// PUBLIC ROUTER
// ============================================================================

void raft_election_step(raft_t* r, raft_msg_t* msg) {
    switch (msg->type) {
        case MSG_HUP:
            handle_hup(r);
            break;
        case MSG_PRE_VOTE:
            handle_pre_vote(r, msg);
            break;
        case MSG_PRE_VOTE_RES:
            handle_pre_vote_response(r, msg);
            break;
        case MSG_REQUEST_VOTE:
            handle_request_vote(r, msg);
            break;
        case MSG_REQUEST_VOTE_RES:
            handle_request_vote_response(r, msg);
            break;
        case MSG_CHECK_QUORUM:
            handle_check_quorum(r);
            break;
        default:
            break;
    }
}
