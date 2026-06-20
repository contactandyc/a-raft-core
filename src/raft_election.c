// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "raft_internal.h"
#include <string.h>

void raft_election_become_leader(raft_t* r) {
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

    size_t total_voters = r->is_learner_self ? 0 : 1;
    for (size_t i = 0; i < r->num_peers; i++) if (!r->is_learner[i]) total_voters++;

    if (total_voters <= 1 && !r->is_learner_self) raft_replication_advance_commit(r, r->term_start_index);

    raft_replication_bcast_append(r);
}

void raft_election_step(raft_t* r, raft_msg_t* msg) {
    if (msg->type == MSG_HUP) {
        if (r->state == RAFT_STATE_LEADER || r->is_learner_self || r->removed) return;

        r->state = RAFT_STATE_PRE_CANDIDATE;
        r->votes_received = 1;
        memset(r->voted_for_me, 0, sizeof(r->voted_for_me));

        size_t total_voters = 1;
        for (size_t i = 0; i < r->num_peers; i++) if (!r->is_learner[i]) total_voters++;

        if (total_voters == 1) {
            r->state = RAFT_STATE_CANDIDATE;
            r->current_term++;
            r->voted_for = r->id;
            raft_election_become_leader(r);
            return;
        }

        uint64_t last_idx = raft_log_last_index(r);
        for (size_t i = 0; i < r->num_peers; i++) {
            if (r->is_learner[i]) continue;
            raft_msg_t req = { .type = MSG_PRE_VOTE, .to = r->peers[i], .term = r->current_term + 1,
                               .index = last_idx, .log_term = raft_log_term(r, last_idx) };
            raft_send_msg(r, req);
        }
    }
    else if (msg->type == MSG_PRE_VOTE) {
        raft_msg_t res = { .type = MSG_PRE_VOTE_RES, .to = msg->from, .term = r->current_term, .reject = true };
        uint64_t my_last_idx = raft_log_last_index(r);
        uint64_t my_last_term = raft_log_term(r, my_last_idx);
        bool log_ok = (msg->log_term > my_last_term) || (msg->log_term == my_last_term && msg->index >= my_last_idx);

        if (msg->term > r->current_term && log_ok && !r->is_learner_self) {
            res.reject = false;
            res.term = msg->term;
        }
        raft_send_msg(r, res);
    }
    else if (msg->type == MSG_PRE_VOTE_RES && r->state == RAFT_STATE_PRE_CANDIDATE) {
        if (msg->reject && msg->term > r->current_term) {
            r->current_term = msg->term;
            r->voted_for = 0;
            r->state = RAFT_STATE_FOLLOWER;
        } else if (!msg->reject && msg->term == r->current_term + 1) {
            for (size_t i = 0; i < r->num_peers; i++) {
                if (r->peers[i] == msg->from && !r->voted_for_me[i] && !r->is_learner[i]) {
                    r->voted_for_me[i] = true;
                    r->votes_received++;

                    size_t total_voters = 1;
                    for (size_t j = 0; j < r->num_peers; j++) if (!r->is_learner[j]) total_voters++;

                    if (r->votes_received >= (total_voters / 2) + 1) {
                        r->state = RAFT_STATE_CANDIDATE;
                        r->current_term++;
                        r->voted_for = r->id;
                        r->votes_received = 1;
                        memset(r->voted_for_me, 0, sizeof(r->voted_for_me));

                        uint64_t last_idx = raft_log_last_index(r);
                        for (size_t j = 0; j < r->num_peers; j++) {
                            if (r->is_learner[j]) continue;
                            raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = r->peers[j], .term = r->current_term,
                                               .index = last_idx, .log_term = raft_log_term(r, last_idx) };
                            raft_send_msg(r, req);
                        }
                    }
                    break;
                }
            }
        }
    }
    else if (msg->type == MSG_REQUEST_VOTE) {
        raft_msg_t res = { .type = MSG_REQUEST_VOTE_RES, .to = msg->from, .term = r->current_term, .reject = true };

        uint64_t my_last_idx = raft_log_last_index(r);
        uint64_t my_last_term = raft_log_term(r, my_last_idx);
        bool log_ok = (msg->log_term > my_last_term) || (msg->log_term == my_last_term && msg->index >= my_last_idx);

        if (msg->term == r->current_term && (r->voted_for == 0 || r->voted_for == msg->from) && log_ok && !r->is_learner_self) {
            r->voted_for = msg->from;
            res.reject = false;
            r->activity_accepted = true;
        }
        raft_send_msg(r, res);
    }
    else if (msg->type == MSG_REQUEST_VOTE_RES && r->state == RAFT_STATE_CANDIDATE && !msg->reject) {
        for (size_t i = 0; i < r->num_peers; i++) {
            if (r->peers[i] == msg->from && !r->voted_for_me[i] && !r->is_learner[i]) {
                r->voted_for_me[i] = true;
                r->votes_received++;

                size_t total_voters = 1;
                for (size_t j = 0; j < r->num_peers; j++) if (!r->is_learner[j]) total_voters++;

                if (r->votes_received >= (total_voters / 2) + 1) {
                    raft_election_become_leader(r);
                }
                break;
            }
        }
    }
    else if (msg->type == MSG_CHECK_QUORUM) {
        if (r->state == RAFT_STATE_LEADER) {
            size_t active = 1;
            size_t total = r->is_learner_self ? 0 : 1;
            for (size_t i = 0; i < r->num_peers; i++) {
                if (!r->is_learner[i]) {
                    total++;
                    if (r->recent_active[i]) active++;
                }
                r->recent_active[i] = false;
            }
            if (active < (total / 2 + 1)) {
                r->state = RAFT_STATE_FOLLOWER;
            }
        }
    }
}
