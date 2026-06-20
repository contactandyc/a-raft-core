// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <string.h>
#include "a-raft-library/raft.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(raft_initial_state) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 0);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 0);

    raft_destroy(r);
}

MACRO_TEST(raft_campaign_becomes_candidate) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_PRE_CANDIDATE);
    MACRO_ASSERT_EQ_INT(raft_term(r), 0);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 2);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_PRE_VOTE);

    raft_destroy(r);
}

MACRO_TEST(raft_win_election_and_noop) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_advance_all(r);

    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    raft_advance_all(r);

    raft_msg_t vote1 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &vote1);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 2);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_ENTRIES);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_steps_down_on_higher_term) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &vote);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    raft_advance_all(r);

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 3, .term = 2,
                       .index = 0, .log_term = 0, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_step(r, &app);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 2);

    raft_destroy(r);
}

MACRO_TEST(raft_voter_rejects_stale_candidate) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t entry = { .term = 2, .index = 1, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 2,
                       .index = 0, .log_term = 0, .entries = &entry, .num_entries = 1, .commit = 0 };
    raft_step(r, &app);
    raft_advance_all(r);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .from = 3, .term = 3,
                       .index = 0, .log_term = 0 };
    raft_step(r, &req);

    MACRO_ASSERT_EQ_INT(raft_term(r), 3);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_REQUEST_VOTE_RES);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_ignores_stale_vote_response) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);

    raft_step(r, &hup);
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_step(r, &pv2);
    raft_advance_all(r);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &vote);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_does_not_double_count_vote) {
    uint64_t peers[] = {2, 3, 4};
    raft_t* r = raft_create(1, peers, 3);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv1 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv1);
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .from = 3, .term = 1, .reject = false };
    raft_step(r, &pv2);
    raft_advance_all(r);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };

    raft_step(r, &vote);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_step(r, &vote);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_rejects_second_candidate_same_term) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t req2 = { .type = MSG_REQUEST_VOTE, .from = 2, .term = 1, .index = 0, .log_term = 0 };
    raft_step(r, &req2);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == false);
    raft_advance_all(r);

    raft_msg_t req3 = { .type = MSG_REQUEST_VOTE, .from = 3, .term = 1, .index = 0, .log_term = 0 };
    raft_step(r, &req3);

    ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_steps_down_on_same_term_append) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_step(r, &app);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);

    raft_destroy(r);
}

MACRO_TEST(raft_single_node_becomes_leader_and_commits_noop) {
    uint64_t* peers = NULL;
    raft_t* r = raft_create(1, peers, 0);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 1);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_does_not_count_rejected_vote) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    raft_advance_all(r);

    raft_msg_t rej = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = true };
    raft_step(r, &rej);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_msg_t acc = { .type = MSG_REQUEST_VOTE_RES, .from = 3, .term = 1, .reject = false };
    raft_step(r, &acc);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_steps_down_on_higher_term_vote_res) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);

    raft_msg_t v1 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_step(r, &v1);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 2);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_ignores_hup) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &vote);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 1);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);

    raft_advance_all(r);
    raft_step(r, &hup);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 1);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_steps_down_on_higher_term_request_vote) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    raft_advance_all(r);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .from = 2, .term = 2, .index = 0, .log_term = 0 };
    raft_step(r, &req);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 2);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_REQUEST_VOTE_RES);
    MACRO_ASSERT_FALSE(ready.messages[0].reject);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_steps_down_on_higher_term_request_vote) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &vote);
    raft_advance_all(r);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .from = 3, .term = 2, .index = raft_last_index(r), .log_term = 1 };
    raft_step(r, &req);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 2);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_steps_down_on_higher_term_append_res) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &vote);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    raft_msg_t res = { .type = MSG_APPEND_RES, .from = 2, .term = 2, .reject = true };
    raft_step(r, &res);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 2);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_ignores_unknown_peer_vote_response) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    raft_advance_all(r);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 99, .term = 1, .reject = false };
    raft_step(r, &vote);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_split_vote_recampaign) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv1 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv1);
    MACRO_ASSERT_EQ_INT(raft_term(r), 1);

    raft_step(r, &hup);
    MACRO_ASSERT_EQ_INT(raft_term(r), 1);

    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_step(r, &pv2);
    MACRO_ASSERT_EQ_INT(raft_term(r), 2);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_step(r, &vote);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    raft_destroy(r);
}

MACRO_TEST(raft_pre_vote_prevents_disruption) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_PRE_CANDIDATE);

    raft_msg_t pv_res = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv_res);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_msg_t v_res = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &v_res);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    raft_advance_all(r);

    raft_msg_t rogue_pv = { .type = MSG_PRE_VOTE, .from = 3, .term = 100, .index = 0, .log_term = 0 };
    raft_step(r, &rogue_pv);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 1);

    raft_destroy(r);
}

MACRO_TEST(raft_check_quorum_steps_down_stale_leader) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &vote);
    raft_advance_all(r);

    raft_msg_t chk = { .type = MSG_CHECK_QUORUM };
    raft_step(r, &chk);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    raft_step(r, &chk);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);

    raft_destroy(r);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, raft_initial_state);
    MACRO_ADD(tests, raft_campaign_becomes_candidate);
    MACRO_ADD(tests, raft_win_election_and_noop);
    MACRO_ADD(tests, raft_leader_steps_down_on_higher_term);
    MACRO_ADD(tests, raft_voter_rejects_stale_candidate);
    MACRO_ADD(tests, raft_candidate_ignores_stale_vote_response);
    MACRO_ADD(tests, raft_candidate_does_not_double_count_vote);
    MACRO_ADD(tests, raft_follower_rejects_second_candidate_same_term);
    MACRO_ADD(tests, raft_candidate_steps_down_on_same_term_append);
    MACRO_ADD(tests, raft_single_node_becomes_leader_and_commits_noop);
    MACRO_ADD(tests, raft_candidate_does_not_count_rejected_vote);
    MACRO_ADD(tests, raft_candidate_steps_down_on_higher_term_vote_res);
    MACRO_ADD(tests, raft_leader_ignores_hup);
    MACRO_ADD(tests, raft_candidate_steps_down_on_higher_term_request_vote);
    MACRO_ADD(tests, raft_leader_steps_down_on_higher_term_request_vote);
    MACRO_ADD(tests, raft_leader_steps_down_on_higher_term_append_res);
    MACRO_ADD(tests, raft_candidate_ignores_unknown_peer_vote_response);
    MACRO_ADD(tests, raft_candidate_split_vote_recampaign);
    MACRO_ADD(tests, raft_pre_vote_prevents_disruption);
    MACRO_ADD(tests, raft_check_quorum_steps_down_stale_leader);

    macro_run_all("raft_election", tests, test_count);
    return 0;
}
