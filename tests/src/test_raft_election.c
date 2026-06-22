// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define RAFT_TESTING 1
#include <stdio.h>
#include <string.h>
#include "raft_internal.h"
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
    raft_step_local(r, &hup);

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
    raft_step_local(r, &hup);
    raft_advance_all_for_tests_only(r);

    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_advance_all_for_tests_only(r);

    raft_msg_t vote1 = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote1);

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
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    raft_advance_all_for_tests_only(r);

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 3, .term = 2,
                       .index = 0, .log_term = 0, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_step_remote(r, &app);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 2);

    raft_destroy(r);
}

MACRO_TEST(raft_voter_rejects_stale_candidate) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t entry = { .term = 2, .index = 1, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 2,
                       .index = 0, .log_term = 0, .entries = &entry, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app);
    raft_advance_all_for_tests_only(r);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = 1, .from = 3, .term = 3,
                       .index = 0, .log_term = 0 };
    raft_step_remote(r, &req);

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
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);

    raft_step_local(r, &hup);
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 2, .reject = false };
    raft_step_remote(r, &pv2);
    raft_advance_all_for_tests_only(r);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_does_not_double_count_vote) {
    uint64_t peers[] = {2, 3, 4};
    raft_t* r = raft_create(1, peers, 3);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv1 = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv1);
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 3, .term = 1, .reject = false };
    raft_step_remote(r, &pv2);
    raft_advance_all_for_tests_only(r);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };

    raft_step_remote(r, &vote);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_step_remote(r, &vote);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_rejects_second_candidate_same_term) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t req2 = { .type = MSG_REQUEST_VOTE, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0 };
    raft_step_remote(r, &req2);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == false);
    raft_advance_all_for_tests_only(r);

    raft_msg_t req3 = { .type = MSG_REQUEST_VOTE, .to = 1, .from = 3, .term = 1, .index = 0, .log_term = 0 };
    raft_step_remote(r, &req3);

    ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_steps_down_on_same_term_append) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_step_remote(r, &app);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);

    raft_destroy(r);
}

MACRO_TEST(raft_single_node_becomes_leader_and_commits_noop) {
    uint64_t* peers = NULL;
    raft_t* r = raft_create(1, peers, 0);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 1);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_does_not_count_rejected_vote) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_advance_all_for_tests_only(r);

    raft_msg_t rej = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = true };
    raft_step_remote(r, &rej);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_msg_t acc = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 3, .term = 1, .reject = false };
    raft_step_remote(r, &acc);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_steps_down_on_higher_term_vote_res) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);

    raft_msg_t v1 = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 2, .reject = false };
    raft_step_remote(r, &v1);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 2);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_ignores_hup) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 1);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);

    raft_advance_all_for_tests_only(r);
    raft_step_local(r, &hup);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 1);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_steps_down_on_higher_term_request_vote) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_advance_all_for_tests_only(r);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = 1, .from = 2, .term = 2, .index = 0, .log_term = 0 };
    raft_step_remote(r, &req);

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
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = 1, .from = 3, .term = 2, .index = raft_last_index(r), .log_term = 1 };
    raft_step_remote(r, &req);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 2);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_steps_down_on_higher_term_append_res) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    raft_msg_t res = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 2, .reject = true };
    raft_step_remote(r, &res);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 2);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_ignores_unknown_peer_vote_response) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_advance_all_for_tests_only(r);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 99, .term = 1, .reject = false };
    raft_step_remote(r, &vote);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_split_vote_recampaign) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv1 = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv1);
    MACRO_ASSERT_EQ_INT(raft_term(r), 1);

    raft_step_local(r, &hup);
    MACRO_ASSERT_EQ_INT(raft_term(r), 1);

    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 2, .reject = false };
    raft_step_remote(r, &pv2);
    MACRO_ASSERT_EQ_INT(raft_term(r), 2);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 2, .reject = false };
    raft_step_remote(r, &vote);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    raft_destroy(r);
}

MACRO_TEST(raft_pre_vote_prevents_disruption) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_PRE_CANDIDATE);

    raft_msg_t pv_res = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv_res);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_msg_t v_res = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &v_res);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    raft_advance_all_for_tests_only(r);

    raft_msg_t rogue_pv = { .type = MSG_PRE_VOTE, .to = 1, .from = 3, .term = 100, .index = 0, .log_term = 0 };
    raft_step_remote(r, &rogue_pv);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 1);

    raft_destroy(r);
}

MACRO_TEST(raft_check_quorum_steps_down_stale_leader) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    raft_msg_t chk = { .type = MSG_CHECK_QUORUM };
    raft_step_local(r, &chk);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    raft_step_local(r, &chk);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);

    raft_destroy(r);
}

// -----------------------------------------------------------------------------
// EDGE CASE TESTS
// -----------------------------------------------------------------------------

MACRO_TEST(raft_request_vote_allows_repeat_vote_for_same_candidate) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0 };
    raft_step_remote(r, &req);
    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_EQ_INT(raft_voted_for(r), 2);

    // Node 2 drops the response and requests again in the same term
    raft_step_remote(r, &req);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_FALSE(ready.messages[0].reject);

    raft_destroy(r);
}

MACRO_TEST(raft_request_vote_rejects_when_self_is_learner) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    // Demote self to learner
    r->is_learner_self = true;

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0 };
    raft_step_remote(r, &req);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject); // Must safely reject since it cannot cast a binding vote

    raft_destroy(r);
}

MACRO_TEST(raft_request_vote_rejects_when_self_is_removed) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    // Logically isolate the node
    r->removed = true;
    r->is_learner_self = true;

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0 };
    raft_step_remote(r, &req);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);

    raft_destroy(r);
}

MACRO_TEST(raft_pre_vote_response_duplicate_not_counted) {
    uint64_t peers[] = {2, 3, 4};
    raft_t* r = raft_create(1, peers, 3);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);

    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_step_remote(r, &pv); // Duplicated packet in the network

    // Node should still be waiting for quorum since it only received a vote from Node 2
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_PRE_CANDIDATE);

    raft_destroy(r);
}

MACRO_TEST(raft_pre_vote_response_from_learner_not_counted) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);
    raft_add_learner(r, 4);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);

    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 4, .term = 1, .reject = false };
    raft_step_remote(r, &pv);

    // Node 4 is a learner, so its pre-vote response should not achieve quorum
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_PRE_CANDIDATE);

    raft_destroy(r);
}

MACRO_TEST(raft_hard_vote_response_from_learner_not_counted) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);
    raft_add_learner(r, 4);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);

    raft_msg_t pv_res = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv_res);
    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_msg_t v_res = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 4, .term = 1, .reject = false };
    raft_step_remote(r, &v_res);

    // Node 4 is a learner, so its hard vote response should not achieve quorum
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_destroy(r);
}

MACRO_TEST(raft_check_quorum_excludes_learners) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);
    raft_add_learner(r, 3); // Node 3 is a learner

    // Force leader state
    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t v = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &v);
    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    // Consume the initial election activity for Node 2 so we have a clean slate
    raft_msg_t chk = { .type = MSG_CHECK_QUORUM };
    raft_step_local(r, &chk);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    // Fake an append response from the learner (Node 3) to mark it as active
    raft_msg_t learner_ack = { .type = MSG_APPEND_RES, .to = 1, .from = 3, .term = 1, .reject = false, .index = 1 };
    raft_step_remote(r, &learner_ack);
    raft_advance_all_for_tests_only(r);

    // Node 2 (voter) is inactive. Node 3 (learner) is active. Total voters = 2 (Self + Node 2).
    // Active voters should only equal 1 (Self). Quorum is 2. The leader MUST step down.
    raft_step_local(r, &chk);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);

    raft_destroy(r);
}

MACRO_TEST(raft_check_quorum_preserves_leader_with_majority_activity) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t v = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &v);
    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    // Clear election activity to ensure a clean slate
    raft_msg_t chk = { .type = MSG_CHECK_QUORUM };
    raft_step_local(r, &chk);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    // Node 2 is active, Node 3 is partitioned
    raft_msg_t ack2 = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 1 };
    raft_step_remote(r, &ack2);
    raft_advance_all_for_tests_only(r);

    // Active voters = Self (1) + Node 2 (1) = 2. Total voters = 3. Quorum = 2.
    // The leader should NOT step down.
    raft_step_local(r, &chk);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    raft_destroy(r);
}

// -----------------------------------------------------------------------------
// NEW INTERNAL ISOLATION TESTS
// -----------------------------------------------------------------------------

MACRO_TEST(raft_pre_vote_rejects_removed_node) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);
    r->removed = true;

    raft_msg_t pv = { .type = MSG_PRE_VOTE, .to = 1, .from = 2, .term = 2, .index = 0, .log_term = 0 };
    raft_step_remote(r, &pv);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_PRE_VOTE_RES);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true); // Removed nodes must reject all votes

    raft_destroy(r);
}

MACRO_TEST(raft_pre_vote_rejects_self_learner) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);
    r->is_learner_self = true;

    raft_msg_t pv = { .type = MSG_PRE_VOTE, .to = 1, .from = 2, .term = 2, .index = 0, .log_term = 0 };
    raft_step_remote(r, &pv);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_PRE_VOTE_RES);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);

    raft_destroy(r);
}

MACRO_TEST(raft_pre_vote_rejects_stale_candidate_log) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t entry = { .term = 2, .index = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 2,
                       .index = 0, .log_term = 0, .entries = &entry, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app);
    raft_advance_all_for_tests_only(r);

    raft_msg_t pv = { .type = MSG_PRE_VOTE, .to = 1, .from = 3, .term = 3, .index = 0, .log_term = 0 };
    raft_step_remote(r, &pv);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true); // Log is stale

    raft_destroy(r);
}

MACRO_TEST(raft_pre_vote_grants_up_to_date_candidate_log) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t entry = { .term = 2, .index = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 2,
                       .index = 0, .log_term = 0, .entries = &entry, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app);
    raft_advance_all_for_tests_only(r);

    raft_msg_t pv = { .type = MSG_PRE_VOTE, .to = 1, .from = 3, .term = 3, .index = 1, .log_term = 2 };
    raft_step_remote(r, &pv);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == false);

    raft_destroy(r);
}

MACRO_TEST(raft_request_vote_rejects_stale_candidate_log) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t entry = { .term = 2, .index = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 2,
                       .index = 0, .log_term = 0, .entries = &entry, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app);
    raft_advance_all_for_tests_only(r);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = 1, .from = 3, .term = 3, .index = 0, .log_term = 0 };
    raft_step_remote(r, &req);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true); // Log is stale

    raft_destroy(r);
}

MACRO_TEST(raft_request_vote_grants_up_to_date_candidate_log) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t entry = { .term = 2, .index = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 2,
                       .index = 0, .log_term = 0, .entries = &entry, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app);
    raft_advance_all_for_tests_only(r);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = 1, .from = 3, .term = 3, .index = 1, .log_term = 2 };
    raft_step_remote(r, &req);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == false);

    raft_destroy(r);
}

MACRO_TEST(raft_request_vote_rejects_future_or_stale_response_term) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);
    MACRO_ASSERT_EQ_INT(raft_term(r), 1);

    // Manually inject a stale and future vote response
    raft_msg_t stale_res = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 0, .reject = false };
    raft_msg_t future_res = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 3, .term = 2, .reject = false };
    raft_step_remote(r, &stale_res);
    raft_step_remote(r, &future_res);

    // The node should have ignored the stale vote and stepped down on the future vote
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);

    raft_destroy(r);
}

MACRO_TEST(raft_removed_leader_steps_down_on_check_quorum) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    // Simulate node logically isolated from cluster
    r->removed = true;
    r->is_learner_self = true;

    raft_msg_t chk = { .type = MSG_CHECK_QUORUM };
    raft_step_local(r, &chk);

    // A removed leader has an active voter count of 0, so it must instantly step down
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(r->leader_id, 0); // Proof identity was cleared

    raft_destroy(r);
}

MACRO_TEST(raft_removed_self_not_counted_in_total_voters_even_if_not_marked_learner) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    // Buggy host integration sets removed but forgets to set is_learner_self
    r->removed = true;
    r->is_learner_self = false;

    // Node 2 votes for us, which gives us 1 vote. If self is counted, we have 2 out of 3 (Quorum!).
    // If self is properly ignored via self_is_voter(), we have 1 out of 2 (No Quorum).
    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);

    // The fail-closed math should prevent the node from reaching CANDIDATE
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);

    raft_destroy(r);
}

MACRO_TEST(raft_candidate_removed_before_vote_response_does_not_become_leader) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    // Simulate mid-campaign dynamic demotion
    r->removed = true;
    r->is_learner_self = true;

    // Final vote arrives
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);

    // Even though it has a mathematical quorum, the state transition guard blocks it
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_CANDIDATE);

    raft_destroy(r);
}

MACRO_TEST(raft_precandidate_removed_before_prevote_response_does_not_start_hard_election) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_PRE_CANDIDATE);

    // Simulate mid-campaign dynamic demotion
    r->removed = true;
    r->is_learner_self = true;

    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);

    // The guard prevents it from advancing to the hard election phase
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_PRE_CANDIDATE);

    raft_destroy(r);
}

MACRO_TEST(raft_same_term_request_vote_to_leader_is_rejected_and_leader_stays_leader) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    // A partitioned node initiates an election in the exact same term
    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = 1, .from = 3, .term = 1, .index = 0, .log_term = 0 };
    raft_step_remote(r, &req);

    // The leader should actively reject it and maintain its own leadership
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_REQUEST_VOTE_RES);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);

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

    // NEW EDGE CASE TESTS
    MACRO_ADD(tests, raft_request_vote_allows_repeat_vote_for_same_candidate);
    MACRO_ADD(tests, raft_request_vote_rejects_when_self_is_learner);
    MACRO_ADD(tests, raft_request_vote_rejects_when_self_is_removed);
    MACRO_ADD(tests, raft_pre_vote_response_duplicate_not_counted);
    MACRO_ADD(tests, raft_pre_vote_response_from_learner_not_counted);
    MACRO_ADD(tests, raft_hard_vote_response_from_learner_not_counted);
    MACRO_ADD(tests, raft_check_quorum_excludes_learners);
    MACRO_ADD(tests, raft_check_quorum_preserves_leader_with_majority_activity);

    MACRO_ADD(tests, raft_pre_vote_rejects_removed_node);
    MACRO_ADD(tests, raft_pre_vote_rejects_self_learner);
    MACRO_ADD(tests, raft_pre_vote_rejects_stale_candidate_log);
    MACRO_ADD(tests, raft_pre_vote_grants_up_to_date_candidate_log);
    MACRO_ADD(tests, raft_request_vote_rejects_stale_candidate_log);
    MACRO_ADD(tests, raft_request_vote_grants_up_to_date_candidate_log);
    MACRO_ADD(tests, raft_request_vote_rejects_future_or_stale_response_term);
    MACRO_ADD(tests, raft_removed_leader_steps_down_on_check_quorum);

    MACRO_ADD(tests, raft_removed_self_not_counted_in_total_voters_even_if_not_marked_learner);
    MACRO_ADD(tests, raft_candidate_removed_before_vote_response_does_not_become_leader);
    MACRO_ADD(tests, raft_precandidate_removed_before_prevote_response_does_not_start_hard_election);
    MACRO_ADD(tests, raft_same_term_request_vote_to_leader_is_rejected_and_leader_stays_leader);

    macro_run_all("raft_election", tests, test_count);
    return 0;
}
