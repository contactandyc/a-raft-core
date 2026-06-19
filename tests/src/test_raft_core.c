// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <string.h>
#include "a-raft-library/raft_core.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(raft_initial_state) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 0);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_campaign_becomes_candidate) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);

    // PHASE 3: MSG_HUP now triggers PRE_CANDIDATE, preventing disruptive term bumps!
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_PRE_CANDIDATE);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 0);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 2);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_PRE_VOTE);

    raft_core_destroy(r);
}

MACRO_TEST(raft_win_election_and_noop) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_core_advance_all(r);

    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_core_advance_all(r);

    raft_msg_t vote1 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote1);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 2);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_ENTRIES);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_rejects_log_gaps) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    uint8_t payload[] = "data";
    raft_entry_t entry = { .term = 1, .index = 5, .data = payload, .data_len = 4 };

    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 4, .log_term = 1, .entries = &entry, .num_entries = 1, .commit = 0 };

    raft_core_step(r, &app1);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 0);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_RES);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_truncates_on_conflict) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t entry1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &entry1, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app1);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);
    raft_core_advance_all(r);

    raft_entry_t conflict = { .term = 2, .index = 1, .data = (uint8_t*)"B", .data_len = 1 };
    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .from = 3, .term = 2,
                        .index = 0, .log_term = 0, .entries = &conflict, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app2);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);
    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == false);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_steps_down_on_higher_term) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);
    raft_core_advance_all(r);

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 3, .term = 2,
                       .index = 0, .log_term = 0, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_core_step(r, &app);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_voter_rejects_stale_candidate) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t entry = { .term = 2, .index = 1, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 2,
                       .index = 0, .log_term = 0, .entries = &entry, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app);
    raft_core_advance_all(r);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .from = 3, .term = 3,
                       .index = 0, .log_term = 0 };
    raft_core_step(r, &req);

    MACRO_ASSERT_EQ_INT(raft_core_term(r), 3);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_REQUEST_VOTE_RES);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);

    raft_core_destroy(r);
}

MACRO_TEST(raft_figure_8_anomaly_prevention) {
    uint64_t peers[] = {2, 3, 4, 5};
    raft_core_t* r = raft_core_create(1, peers, 4);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    for (int i = 2; i <= 3; i++) {
        raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = i, .term = 1, .reject = false };
        raft_core_step(r, &pv);
    }
    for (int i = 2; i <= 3; i++) {
        raft_msg_t v = { .type = MSG_REQUEST_VOTE_RES, .from = i, .term = 1, .reject = false };
        raft_core_step(r, &v);
    }
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);
    raft_core_advance_all(r);

    uint8_t data[] = "data";
    raft_entry_t entry = { .data = data, .data_len = 4 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &entry, .num_entries = 1 };
    raft_core_step(r, &prop);

    raft_msg_t app_fake = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 3,
                            .index = 0, .log_term = 0, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_core_step(r, &app_fake);

    raft_msg_t hup2 = { .type = MSG_HUP };
    raft_core_step(r, &hup2);
    for (int i = 2; i <= 3; i++) {
        raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = i, .term = 4, .reject = false };
        raft_core_step(r, &pv);
    }
    for (int i = 2; i <= 3; i++) {
        raft_msg_t v = { .type = MSG_REQUEST_VOTE_RES, .from = i, .term = 4, .reject = false };
        raft_core_step(r, &v);
    }
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    raft_core_advance_all(r);

    raft_msg_t ack1 = { .type = MSG_APPEND_RES, .from = 2, .term = 4, .reject = false, .index = 2 };
    raft_core_step(r, &ack1);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0);

    raft_msg_t ack2 = { .type = MSG_APPEND_RES, .from = 3, .term = 4, .reject = false, .index = 2 };
    raft_core_step(r, &ack2);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0);

    raft_msg_t ack3 = { .type = MSG_APPEND_RES, .from = 3, .term = 4, .reject = false, .index = 3 };
    raft_core_step(r, &ack3);

    raft_msg_t ack4 = { .type = MSG_APPEND_RES, .from = 2, .term = 4, .reject = false, .index = 3 };
    raft_core_step(r, &ack4);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 3);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_backtracks_next_index_on_reject) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    uint8_t data[] = "X";
    raft_entry_t entry = { .data = data, .data_len = 1 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &entry, .num_entries = 1 };
    raft_core_step(r, &prop);
    raft_core_step(r, &prop);
    raft_core_advance_all(r);

    // PHASE 3 (Gap 9): Injected .conflict_index = 3
    raft_msg_t rej = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = true, .index = 3, .conflict_index = 3 };
    raft_core_step(r, &rej);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_ENTRIES);

    MACRO_ASSERT_EQ_INT(ready.messages[0].index, 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_ignores_lower_term_append) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);

    // Do NOT send the real vote yet. It remains a CANDIDATE in Term 1.

    raft_core_step(r, &hup); // Trigger new election. Moves to PRE_CANDIDATE for term 2.
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_core_step(r, &pv2);
    raft_msg_t vote2 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_core_step(r, &vote2); // NOW it becomes LEADER in Term 2
    raft_core_advance_all(r);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    // Assert that a message from an old Term 1 leader is rejected
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 3, .term = 1 };
    raft_core_step(r, &app);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 2);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_RES);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);
    MACRO_ASSERT_EQ_INT(ready.messages[0].term, 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_candidate_ignores_stale_vote_response) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);

    raft_core_step(r, &hup);
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_core_step(r, &pv2);
    raft_core_advance_all(r);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_CANDIDATE);

    raft_core_destroy(r);
}

MACRO_TEST(raft_candidate_does_not_double_count_vote) {
    uint64_t peers[] = {2, 3, 4};
    raft_core_t* r = raft_core_create(1, peers, 3);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv1 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv1);
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .from = 3, .term = 1, .reject = false };
    raft_core_step(r, &pv2);
    raft_core_advance_all(r);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };

    raft_core_step(r, &vote);
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_CANDIDATE);

    raft_core_step(r, &vote);
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_CANDIDATE);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_rejects_second_candidate_same_term) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t req2 = { .type = MSG_REQUEST_VOTE, .from = 2, .term = 1, .index = 0, .log_term = 0 };
    raft_core_step(r, &req2);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == false);
    raft_core_advance_all(r);

    raft_msg_t req3 = { .type = MSG_REQUEST_VOTE, .from = 3, .term = 1, .index = 0, .log_term = 0 };
    raft_core_step(r, &req3);

    ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_updates_commit_from_heartbeat) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t entry1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t entry2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };
    raft_entry_t batch[] = {entry1, entry2};

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = batch, .num_entries = 2, .commit = 0 };
    raft_core_step(r, &app);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0);
    raft_core_advance_all(r);

    raft_msg_t hb = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                      .index = 2, .log_term = 1, .entries = NULL, .num_entries = 0, .commit = 2 };
    raft_core_step(r, &hb);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 2);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_appends_multiple_entries) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };
    raft_entry_t e3 = { .term = 1, .index = 3, .data = (uint8_t*)"3", .data_len = 1 };
    raft_entry_t batch[] = {e1, e2, e3};

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = batch, .num_entries = 3, .commit = 0 };

    raft_core_step(r, &app);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 3);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_backtracks_multiple_times) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    raft_entry_t d = { .data = NULL, .data_len = 0 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &d, .num_entries = 1 };
    raft_core_step(r, &p);
    raft_core_step(r, &p);
    raft_core_advance_all(r);

    raft_msg_t r1 = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = true, .index = 3, .conflict_index = 3 };
    raft_core_step(r, &r1);
    raft_ready_t rd1 = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(rd1.messages[0].index, 2);
    raft_core_advance_all(r);

    raft_msg_t r2 = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = true, .index = 2, .conflict_index = 2 };
    raft_core_step(r, &r2);
    raft_ready_t rd2 = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(rd2.messages[0].index, 1);
    raft_core_advance_all(r);

    raft_msg_t r3 = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = true, .index = 1, .conflict_index = 1 };
    raft_core_step(r, &r3);
    raft_ready_t rd3 = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(rd3.messages[0].index, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_advance_clears_messages_and_committed_entries) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(ready.num_messages > 0);

    raft_core_advance_all(r);
    ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_candidate_steps_down_on_same_term_append) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_CANDIDATE);

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_core_step(r, &app);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);

    raft_core_destroy(r);
}

MACRO_TEST(raft_single_node_becomes_leader_and_commits_noop) {
    uint64_t* peers = NULL;
    raft_core_t* r = raft_core_create(1, peers, 0);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_conflict_replacement_multiple_entries) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_entry_t e3 = { .term = 1, .index = 3, .data = (uint8_t*)"C", .data_len = 1 };
    raft_entry_t batch1[] = { e1, e2, e3 };

    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = batch1, .num_entries = 3, .commit = 0 };
    raft_core_step(r, &app1);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 3);
    raft_core_advance_all(r);

    raft_entry_t x2 = { .term = 2, .index = 2, .data = (uint8_t*)"X", .data_len = 1 };
    raft_entry_t x3 = { .term = 2, .index = 3, .data = (uint8_t*)"Y", .data_len = 1 };
    raft_entry_t batch2[] = { x2, x3 };

    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 2,
                        .index = 1, .log_term = 1, .entries = batch2, .num_entries = 2, .commit = 0 };
    raft_core_step(r, &app2);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 3);
    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_EQ_INT(ready.messages[0].index, 3);
    MACRO_ASSERT_FALSE(ready.messages[0].reject);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_duplicate_append_is_idempotent) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };

    raft_core_step(r, &app);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);
    raft_core_advance_all(r);

    raft_core_step(r, &app);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_FALSE(ready.messages[0].reject);
    MACRO_ASSERT_EQ_INT(ready.messages[0].index, 1);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_ignores_propose) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };

    raft_core_step(r, &p);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 0);
    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_candidate_does_not_count_rejected_vote) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_core_advance_all(r);

    raft_msg_t rej = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = true };
    raft_core_step(r, &rej);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_CANDIDATE);

    raft_msg_t acc = { .type = MSG_REQUEST_VOTE_RES, .from = 3, .term = 1, .reject = false };
    raft_core_step(r, &acc);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_ignores_unknown_peer_append_res) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t v1 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &v1);
    raft_core_advance_all(r);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(r, &p);
    raft_core_advance_all(r);

    raft_msg_t ack = { .type = MSG_APPEND_RES, .from = 99, .term = 1, .reject = false, .index = 2 };
    raft_core_step(r, &ack);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_ignores_stale_append_res) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv1 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv1);

    raft_core_step(r, &hup);
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_core_step(r, &pv2);
    raft_msg_t v1 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_core_step(r, &v1);
    raft_core_advance_all(r);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(r, &p);
    raft_core_advance_all(r);

    raft_msg_t ack = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_core_step(r, &ack);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_candidate_steps_down_on_higher_term_vote_res) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);

    raft_msg_t v1 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_core_step(r, &v1);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_commit_never_decreases) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 1 };
    raft_core_step(r, &app);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);
    raft_core_advance_all(r);

    raft_msg_t hb = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                      .index = 1, .log_term = 1, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_core_step(r, &hb);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_commit_clamped_to_last_index) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 99 };
    raft_core_step(r, &app);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);

    raft_core_destroy(r);
}

MACRO_TEST(raft_apply_clears_committed_entries) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };
    raft_entry_t batch[] = { e1, e2 };

    raft_msg_t app = {
        .type = MSG_APPEND_ENTRIES,
        .from = 2, .term = 1, .index = 0, .log_term = 0,
        .entries = batch, .num_entries = 2, .commit = 2
    };

    raft_core_step(r, &app);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 2);

    raft_core_advance_all(r);

    ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_ignores_messages_not_addressed_to_self) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 3, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = NULL, .num_entries = 0, .commit = 0 };

    raft_core_step(r, &app);

    MACRO_ASSERT_EQ_INT(raft_core_term(r), 0);
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_ignores_hup) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 1);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);

    raft_core_advance_all(r);
    raft_core_step(r, &hup);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 1);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);

    raft_core_destroy(r);
}

MACRO_TEST(raft_candidate_steps_down_on_higher_term_request_vote) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_core_advance_all(r);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .from = 2, .term = 2, .index = 0, .log_term = 0 };
    raft_core_step(r, &req);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 2);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_REQUEST_VOTE_RES);
    MACRO_ASSERT_FALSE(ready.messages[0].reject);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_steps_down_on_higher_term_request_vote) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .from = 3, .term = 2, .index = raft_core_last_index(r), .log_term = 1 };
    raft_core_step(r, &req);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_steps_down_on_higher_term_append_res) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    raft_msg_t res = { .type = MSG_APPEND_RES, .from = 2, .term = 2, .reject = true };
    raft_core_step(r, &res);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_rejects_append_entries_with_null_entries_and_count) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = NULL, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 0);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_rejects_empty_proposal) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = NULL, .num_entries = 0 };
    raft_core_step(r, &prop);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);

    raft_core_destroy(r);
}

MACRO_TEST(raft_ready_populates_entries_to_save) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 1);

    raft_core_advance_all(r);

    ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_committed_entry_content_is_correct) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"HELLO", .data_len = 5 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 1 };

    raft_core_step(r, &app);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 1);
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].index, 1);
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].term, 1);
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].data_len, 5);
    MACRO_ASSERT_TRUE(memcmp(ready.committed_entries[0].data, "HELLO", 5) == 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_ready_populates_entries_to_save_on_follower_append) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };

    raft_core_step(r, &app);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 1);
    MACRO_ASSERT_EQ_INT(ready.entries_to_save[0].index, 1);
    MACRO_ASSERT_EQ_INT(ready.entries_to_save[0].term, 1);

    raft_core_advance_all(r);
    ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_ignores_zero_entry_proposal_even_with_entries_pointer) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 0 };

    raft_core_step(r, &p);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_appends_multiple_proposed_entries) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    raft_entry_t e1 = { .data = (uint8_t*)"X", .data_len = 1 };
    raft_entry_t e2 = { .data = (uint8_t*)"Y", .data_len = 1 };
    raft_entry_t batch[] = { e1, e2 };

    raft_msg_t p = { .type = MSG_PROPOSE, .entries = batch, .num_entries = 2 };
    raft_core_step(r, &p);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 3);

    raft_core_destroy(r);
}

MACRO_TEST(raft_ready_returns_only_newly_committed_entries_after_apply) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };

    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &e1, .num_entries = 1, .commit = 1 };
    raft_core_step(r, &app1);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 1);
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].index, 1);

    raft_core_advance_all(r);

    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 1, .log_term = 1, .entries = &e2, .num_entries = 1, .commit = 2 };
    raft_core_step(r, &app2);

    ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 1);
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].index, 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_duplicate_append_with_higher_commit_advances_commit) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app1);
    raft_core_advance_all(r);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0);

    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 1 };
    raft_core_step(r, &app2);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_rejects_wrong_prev_log_term) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &e1, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app1);
    raft_core_advance_all(r);

    raft_entry_t e2 = { .term = 2, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 2,
                        .index = 1, .log_term = 2, .entries = &e2, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app2);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);
    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);

    raft_core_destroy(r);
}

MACRO_TEST(raft_append_reject_reports_last_index) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_entry_t batch[] = { e1, e2 };

    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = batch, .num_entries = 2, .commit = 0 };
    raft_core_step(r, &app1);
    raft_core_advance_all(r);

    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 4, .log_term = 1, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_core_step(r, &app2);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);
    // NEW CONTRACT: Follower reports conflict_index!
    MACRO_ASSERT_EQ_INT(ready.messages[0].conflict_index, 3);

    raft_core_destroy(r);
}

MACRO_TEST(raft_create_rejects_too_many_peers_and_self) {
    uint64_t invalid_self_peers[] = {1, 2};

    raft_core_t* r1 = raft_core_create(1, invalid_self_peers, 2);
    MACRO_ASSERT_TRUE(r1 == NULL);

    uint64_t too_many_peers[100] = {0};
    raft_core_t* r2 = raft_core_create(1, too_many_peers, 100);
    MACRO_ASSERT_TRUE(r2 == NULL);
}

MACRO_TEST(raft_candidate_ignores_unknown_peer_vote_response) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_core_advance_all(r);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 99, .term = 1, .reject = false };
    raft_core_step(r, &vote);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_CANDIDATE);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_heartbeat_clamps_commit_to_prev_index) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };
    raft_entry_t batch[] = { e1, e2 };

    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = batch, .num_entries = 2, .commit = 0 };
    raft_core_step(r, &app1);
    raft_core_advance_all(r);

    raft_msg_t hb = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                      .index = 1, .log_term = 1, .entries = NULL, .num_entries = 0, .commit = 5 };
    raft_core_step(r, &hb);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_backoff_floor) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    raft_msg_t rj = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = true, .index = 99, .conflict_index = 0 };
    raft_core_step(r, &rj);
    raft_core_step(r, &rj);
    raft_core_step(r, &rj);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(ready.num_messages > 0);
    MACRO_ASSERT_EQ_INT(ready.messages[ready.num_messages - 1].index, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_ignores_out_of_bounds_match_index) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    raft_msg_t ack = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 999 };
    raft_core_step(r, &ack);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_mixed_conflict_batch) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_entry_t e3 = { .term = 1, .index = 3, .data = (uint8_t*)"C", .data_len = 1 };
    raft_entry_t b1[] = { e1, e2, e3 };
    raft_msg_t a1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 0, .log_term = 0, .entries = b1, .num_entries = 3 };
    raft_core_step(r, &a1);
    raft_core_advance_all(r);

    raft_entry_t nx2 = { .term = 1, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_entry_t nx3 = { .term = 1, .index = 3, .data = (uint8_t*)"C", .data_len = 1 };
    raft_entry_t nx4 = { .term = 2, .index = 4, .data = (uint8_t*)"D", .data_len = 1 };
    raft_entry_t b2[] = { nx2, nx3, nx4 };

    raft_msg_t a2 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 2, .index = 1, .log_term = 1, .entries = b2, .num_entries = 3 };
    raft_core_step(r, &a2);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 4);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 1);
    MACRO_ASSERT_EQ_INT(ready.entries_to_save[0].index, 4);

    raft_core_destroy(r);
}

MACRO_TEST(raft_candidate_split_vote_recampaign) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv1 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv1);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 1);

    raft_core_step(r, &hup);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 1);

    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_core_step(r, &pv2);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 2);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_core_step(r, &vote);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_requires_peer_ack_to_commit_noop) {
    uint64_t peers[] = {2};
    raft_core_t* r = raft_core_create(1, peers, 1);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_multi_node_smoke_test) {
    uint64_t peers1[] = {2, 3};
    uint64_t peers2[] = {1, 3};
    uint64_t peers3[] = {1, 2};

    raft_core_t* nodes[3] = {
        raft_core_create(1, peers1, 2),
        raft_core_create(2, peers2, 2),
        raft_core_create(3, peers3, 2)
    };

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(nodes[0], &hup);

    bool active = true;
    int cycles = 0;
    while (active && cycles < 20) {
        active = false;
        cycles++;

        for (int i = 0; i < 3; i++) {
            raft_ready_t rd = raft_core_get_ready(nodes[i]);
            for (size_t m = 0; m < rd.num_messages; m++) {
                active = true;
                int target = rd.messages[m].to - 1;
                raft_core_step(nodes[target], &rd.messages[m]);
            }
            raft_core_advance_all(nodes[i]);
        }
    }

    MACRO_ASSERT_TRUE(raft_core_state(nodes[0]) == RAFT_STATE_LEADER);
    MACRO_ASSERT_TRUE(raft_core_state(nodes[1]) == RAFT_STATE_FOLLOWER);

    raft_entry_t e = { .data = (uint8_t*)"Hello", .data_len = 5 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(nodes[0], &p);

    active = true;
    while (active) {
        active = false;
        for (int i = 0; i < 3; i++) {
            raft_ready_t rd = raft_core_get_ready(nodes[i]);
            for (size_t m = 0; m < rd.num_messages; m++) {
                active = true;
                int target = rd.messages[m].to - 1;
                raft_core_step(nodes[target], &rd.messages[m]);
            }
            raft_core_advance_all(nodes[i]);
        }
    }

    raft_msg_t tick = { .type = MSG_TICK };
    raft_core_step(nodes[0], &tick);

    active = true;
    while (active) {
        active = false;
        for (int i = 0; i < 3; i++) {
            raft_ready_t rd = raft_core_get_ready(nodes[i]);
            for (size_t m = 0; m < rd.num_messages; m++) {
                active = true;
                int target = rd.messages[m].to - 1;
                raft_core_step(nodes[target], &rd.messages[m]);
            }
            raft_core_advance_all(nodes[i]);
        }
    }

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(nodes[0]), 2);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(nodes[1]), 2);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(nodes[2]), 2);

    raft_core_destroy(nodes[0]);
    raft_core_destroy(nodes[1]);
    raft_core_destroy(nodes[2]);
}

MACRO_TEST(raft_conf_change_applies_only_on_commit) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    uint64_t new_node = 4;
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_CONF_ADD, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = &e, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app);

    uint64_t active_peers[16];
    size_t num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);

    raft_msg_t hb = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 1, .log_term = 1,
                       .entries = NULL, .num_entries = 0, .commit = 1 };
    raft_core_step(r, &hb);

    raft_core_advance_all(r);

    num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 3);
    MACRO_ASSERT_EQ_INT(active_peers[2], 4);

    raft_core_destroy(r);
}

MACRO_TEST(raft_conf_truncate_uncommitted_config_is_safe) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    uint64_t new_node = 4;
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_CONF_ADD, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app);

    raft_entry_t conflict = { .term = 2, .index = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t overwrite = { .type = MSG_APPEND_ENTRIES, .from = 3, .term = 2,
                             .index = 0, .log_term = 0, .entries = &conflict, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &overwrite);

    uint64_t active_peers[16];
    size_t num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_conf_add_node_applies_on_commit) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    uint64_t active_peers[16];
    size_t num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    uint64_t new_node = 4;
    raft_entry_t e = { .type = ENTRY_CONF_ADD, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(r, &p);

    num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);

    raft_msg_t ack = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_core_step(r, &ack);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 2);

    raft_core_advance_all(r);

    num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 3);
    MACRO_ASSERT_EQ_INT(active_peers[2], 4);

    raft_core_destroy(r);
}

MACRO_TEST(raft_conf_remove_node_applies_on_commit) {
    uint64_t peers[] = {2, 3, 4};
    raft_core_t* r = raft_core_create(1, peers, 3);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv1 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv1);
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .from = 3, .term = 1, .reject = false };
    raft_core_step(r, &pv2);

    raft_msg_t vote1 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_msg_t vote2 = { .type = MSG_REQUEST_VOTE_RES, .from = 3, .term = 1, .reject = false };
    raft_core_step(r, &vote1);
    raft_core_step(r, &vote2);
    raft_core_advance_all(r);

    uint64_t rm_node = 3;
    raft_entry_t e = { .type = ENTRY_CONF_REMOVE, .data = (uint8_t*)&rm_node, .data_len = sizeof(uint64_t) };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(r, &p);

    uint64_t active_peers[16];
    size_t num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 3);

    raft_msg_t ack1 = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_msg_t ack2 = { .type = MSG_APPEND_RES, .from = 4, .term = 1, .reject = false, .index = 2 };
    raft_core_step(r, &ack1);
    raft_core_step(r, &ack2);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 2);

    raft_core_advance_all(r);

    num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);
    MACRO_ASSERT_EQ_INT(active_peers[0], 2);
    MACRO_ASSERT_EQ_INT(active_peers[1], 4);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_ignores_append_res_beyond_log_end) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    raft_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(r, &prop);
    raft_core_advance_all(r);

    raft_msg_t bogus = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 999 };
    raft_core_step(r, &bogus);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_create_rejects_duplicate_peers) {
    uint64_t peers[] = {2, 2};
    raft_core_t* r = raft_core_create(1, peers, 2);
    MACRO_ASSERT_TRUE(r == NULL);
}

MACRO_TEST(raft_create_rejects_null_peers_when_num_peers_nonzero) {
    raft_core_t* r = raft_core_create(1, NULL, 2);
    MACRO_ASSERT_TRUE(r == NULL);
}

MACRO_TEST(raft_follower_rejects_conflict_before_commit_index) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 0, .log_term = 0,
                        .entries = &e1, .num_entries = 1, .commit = 1 };
    raft_core_step(r, &app1);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);
    raft_core_advance_all(r);

    raft_entry_t conflict = { .term = 2, .type = ENTRY_NORMAL, .data = (uint8_t*)"B", .data_len = 1 };
    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .from = 3, .term = 2, .index = 0, .log_term = 0,
                        .entries = &conflict, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app2);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_tick_sends_heartbeat_to_all_peers) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    raft_msg_t tick = { .type = MSG_TICK };
    raft_core_step(r, &tick);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 2);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_ENTRIES);
    MACRO_ASSERT_TRUE(ready.messages[1].type == MSG_APPEND_ENTRIES);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_tick_does_nothing) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t tick = { .type = MSG_TICK };
    raft_core_step(r, &tick);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_tick_sends_pending_entries_to_lagging_peer) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(r, &prop);

    raft_core_advance_all(r);

    raft_msg_t tick = { .type = MSG_TICK };
    raft_core_step(r, &tick);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 2);

    raft_msg_t* to_node_3 = ready.messages[0].to == 3 ? &ready.messages[0] : &ready.messages[1];

    MACRO_ASSERT_EQ_INT(to_node_3->num_entries, 0);

    uint64_t hb_prev_index = to_node_3->index;
    raft_core_advance_all(r);

    raft_msg_t rej = { .type = MSG_APPEND_RES, .from = 3, .term = 1, .reject = true, .index = hb_prev_index, .conflict_index = 1 };
    raft_core_step(r, &rej);

    ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].num_entries > 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_pre_vote_prevents_disruption) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    // Initial leader election
    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    // Node 1 becomes Pre-Candidate, sends Pre-Votes.
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_PRE_CANDIDATE);

    // Peer 2 grants Pre-Vote
    raft_msg_t pv_res = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv_res);

    // Now it's a real Candidate
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_CANDIDATE);

    // Peer 2 grants real Vote
    raft_msg_t v_res = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &v_res);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);
    raft_core_advance_all(r);

    // Isolated Node 3 comes back from a network partition and spammed its election timer
    // It sends a Pre-Vote with term 100.
    raft_msg_t rogue_pv = { .type = MSG_PRE_VOTE, .from = 3, .term = 100, .index = 0, .log_term = 0 };
    raft_core_step(r, &rogue_pv);

    // Node 1 (Leader) should process the Pre-Vote without stepping down or advancing its term!
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 1); // Stayed stable!

    raft_core_destroy(r);
}

MACRO_TEST(raft_learner_does_not_vote_or_count_in_quorum) {
    uint64_t peers[] = {2};
    raft_core_t* r = raft_core_create(1, peers, 1);
    raft_core_add_learner(r, 3); // Node 3 is a Learner

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    // FIX: Node 2 must ACK the leader's No-Op to establish the initial commit quorum!
    raft_msg_t ack2_noop = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 1 };
    raft_core_step(r, &ack2_noop);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);

    // Propose the actual payload
    raft_entry_t e_data = { .term = 1, .index = 2, .type = ENTRY_NORMAL, .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p_data = { .type = MSG_PROPOSE, .entries = &e_data, .num_entries = 1 };
    raft_core_step(r, &p_data);
    raft_core_advance_all(r);

    // Learner (Node 3) ACKs the payload
    raft_msg_t ack3 = { .type = MSG_APPEND_RES, .from = 3, .term = 1, .reject = false, .index = 2 };
    raft_core_step(r, &ack3);

    // Learner ACK does NOT safely advance the commit_index to 2! It stays at 1.
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);

    raft_core_destroy(r);
}

MACRO_TEST(raft_read_index_success_on_heartbeat_quorum) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    // Propose an entry to assert authority for the term
    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(r, &p);
    raft_core_advance_all(r);
    raft_msg_t ack2 = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_core_step(r, &ack2);
    raft_core_advance_all(r);

    // Fire the ReadIndex pipeline (Phase 4)
    raft_msg_t ri = { .type = MSG_READ_INDEX, .read_seq = 12345 };
    raft_core_step(r, &ri);
    raft_core_advance_all(r);

    // Provide proof of life via an empty append ACK mimicking a heartbeat response
    raft_msg_t ack3 = { .type = MSG_APPEND_RES, .from = 3, .term = 1, .reject = false, .index = 2, .read_seq = 1 };
    raft_core_step(r, &ack3);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 1);
    MACRO_ASSERT_EQ_INT(ready.read_states[0].read_seq, 12345);
    MACRO_ASSERT_EQ_INT(ready.read_states[0].index, 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_check_quorum_steps_down_stale_leader) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    // First CheckQuorum validates successfully because votes registered network activity
    raft_msg_t chk = { .type = MSG_CHECK_QUORUM };
    raft_core_step(r, &chk);
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    // Second CheckQuorum fails! The active flags were cleared and no new pings arrived.
    raft_core_step(r, &chk);
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_stepdown_on_self_removal) {
    uint64_t peers[] = {2};
    raft_core_t* r = raft_core_create(1, peers, 1);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance_all(r);

    uint64_t rm_node = 1; // Leader votes itself out
    raft_entry_t e = { .term = 1, .index = 2, .type = ENTRY_CONF_REMOVE, .data = (uint8_t*)&rm_node, .data_len = sizeof(uint64_t) };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(r, &p);

    raft_msg_t ack2 = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_core_step(r, &ack2);

    raft_core_advance_all(r); // Conf processes natively on advance

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);

    raft_core_destroy(r);
}

// ============================================================================
// PHASE 11: EXTREME FAULT INJECTION TESTS
// ============================================================================

MACRO_TEST(raft_fault_duplicate_snapshot_install_is_safe) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    uint8_t snap_data[] = "STATE";
    raft_msg_t snap1 = {
        .type = MSG_INSTALL_SNAPSHOT, .from = 2, .term = 2,
        .index = 100, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 5
    };

    // First snapshot is accepted and shifts the horizon
    raft_core_step(r, &snap1);
    raft_ready_t rd1 = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(rd1.install_snapshot);
    MACRO_ASSERT_EQ_INT(rd1.snapshot_index, 100);
    raft_core_advance_all(r);

    // Simulate network duplication: Same snapshot arrives again!
    raft_msg_t snap2 = {
        .type = MSG_INSTALL_SNAPSHOT, .from = 2, .term = 2,
        .index = 100, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 5
    };
    raft_core_step(r, &snap2);

    raft_ready_t rd2 = raft_core_get_ready(r);

    // Engine must aggressively REJECT redundant snapshots without corrupting state
    MACRO_ASSERT_FALSE(rd2.install_snapshot);
    MACRO_ASSERT_EQ_INT(raft_core_snapshot_index(r), 100);
    MACRO_ASSERT_TRUE(rd2.messages[0].reject); // Responds with failure to stop transmission

    raft_core_destroy(r);
}

MACRO_TEST(raft_fault_stale_read_index_response_ignored) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    // Establish Term 2 Leader
    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup); // Term 1 pre-candidate
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote); // Term 1 Leader

    raft_core_step(r, &hup); // Trigger new election
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_core_step(r, &pv2);
    raft_msg_t vote2 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_core_step(r, &vote2); // Term 2 Leader
    raft_core_advance_all(r);

    // Leader receives a ReadIndex ACK that was delayed in the network from Term 1
    raft_msg_t stale_ack = {
        .type = MSG_APPEND_RES, .from = 2, .term = 1, // STALE TERM!
        .reject = false, .index = 2, .read_seq = 55
    };
    raft_core_step(r, &stale_ack);

    // Engine must actively ignore the stale sequence ACK to prevent serving partitioned reads
    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_fault_learner_promotion_after_leader_crash) {
    uint64_t peers[] = {2};
    raft_core_t* r = raft_core_create(1, peers, 1);

    // Add Node 3 as a learner dynamically
    uint64_t node3 = 3;
    raft_entry_t conf_add = { .type = ENTRY_CONF_ADD_LEARNER, .data = (uint8_t*)&node3, .data_len = sizeof(uint64_t) };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 0, .log_term = 0, .entries = &conf_add, .num_entries = 1, .commit = 1 };
    raft_core_step(r, &app);
    raft_core_advance_all(r);

    // Node 1 verified Node 3 is a learner
    uint64_t act_peers[16]; bool is_learner[16];
    size_t num = raft_core_peers_ext(r, act_peers, is_learner);
    MACRO_ASSERT_EQ_INT(num, 2);
    MACRO_ASSERT_TRUE(is_learner[1]);

    // Leader 2 crashes! Node 1 attempts to become leader in Term 2
    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 3, .term = 2, .reject = false };
    raft_core_step(r, &pv); // Learner responds to pre-vote

    // PHASE 11 FIX: The node safely stalls in PRE_CANDIDATE because Learner votes are ignored!
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_PRE_CANDIDATE);

    raft_core_destroy(r);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, raft_initial_state);
    MACRO_ADD(tests, raft_campaign_becomes_candidate);
    MACRO_ADD(tests, raft_win_election_and_noop);
    MACRO_ADD(tests, raft_follower_rejects_log_gaps);
    MACRO_ADD(tests, raft_follower_truncates_on_conflict);
    MACRO_ADD(tests, raft_leader_steps_down_on_higher_term);
    MACRO_ADD(tests, raft_voter_rejects_stale_candidate);
    MACRO_ADD(tests, raft_figure_8_anomaly_prevention);
    MACRO_ADD(tests, raft_leader_backtracks_next_index_on_reject);
    MACRO_ADD(tests, raft_leader_ignores_lower_term_append);
    MACRO_ADD(tests, raft_candidate_ignores_stale_vote_response);
    MACRO_ADD(tests, raft_candidate_does_not_double_count_vote);
    MACRO_ADD(tests, raft_follower_rejects_second_candidate_same_term);
    MACRO_ADD(tests, raft_follower_updates_commit_from_heartbeat);
    MACRO_ADD(tests, raft_follower_appends_multiple_entries);
    MACRO_ADD(tests, raft_leader_backtracks_multiple_times);
    MACRO_ADD(tests, raft_advance_clears_messages_and_committed_entries);
    MACRO_ADD(tests, raft_candidate_steps_down_on_same_term_append);
    MACRO_ADD(tests, raft_single_node_becomes_leader_and_commits_noop);
    MACRO_ADD(tests, raft_follower_conflict_replacement_multiple_entries);
    MACRO_ADD(tests, raft_follower_duplicate_append_is_idempotent);
    MACRO_ADD(tests, raft_follower_ignores_propose);
    MACRO_ADD(tests, raft_candidate_does_not_count_rejected_vote);
    MACRO_ADD(tests, raft_leader_ignores_unknown_peer_append_res);
    MACRO_ADD(tests, raft_leader_ignores_stale_append_res);
    MACRO_ADD(tests, raft_candidate_steps_down_on_higher_term_vote_res);
    MACRO_ADD(tests, raft_follower_commit_never_decreases);
    MACRO_ADD(tests, raft_follower_commit_clamped_to_last_index);
    MACRO_ADD(tests, raft_apply_clears_committed_entries);
    MACRO_ADD(tests, raft_ignores_messages_not_addressed_to_self);
    MACRO_ADD(tests, raft_leader_ignores_hup);
    MACRO_ADD(tests, raft_candidate_steps_down_on_higher_term_request_vote);
    MACRO_ADD(tests, raft_leader_steps_down_on_higher_term_request_vote);
    MACRO_ADD(tests, raft_leader_steps_down_on_higher_term_append_res);
    MACRO_ADD(tests, raft_rejects_append_entries_with_null_entries_and_count);
    MACRO_ADD(tests, raft_leader_rejects_empty_proposal);
    MACRO_ADD(tests, raft_ready_populates_entries_to_save);
    MACRO_ADD(tests, raft_committed_entry_content_is_correct);
    MACRO_ADD(tests, raft_ready_populates_entries_to_save_on_follower_append);
    MACRO_ADD(tests, raft_leader_ignores_zero_entry_proposal_even_with_entries_pointer);
    MACRO_ADD(tests, raft_leader_appends_multiple_proposed_entries);
    MACRO_ADD(tests, raft_ready_returns_only_newly_committed_entries_after_apply);
    MACRO_ADD(tests, raft_follower_duplicate_append_with_higher_commit_advances_commit);
    MACRO_ADD(tests, raft_follower_rejects_wrong_prev_log_term);
    MACRO_ADD(tests, raft_append_reject_reports_last_index);
    MACRO_ADD(tests, raft_create_rejects_too_many_peers_and_self);
    MACRO_ADD(tests, raft_candidate_ignores_unknown_peer_vote_response);
    MACRO_ADD(tests, raft_follower_heartbeat_clamps_commit_to_prev_index);
    MACRO_ADD(tests, raft_leader_backoff_floor);
    MACRO_ADD(tests, raft_leader_ignores_out_of_bounds_match_index);
    MACRO_ADD(tests, raft_follower_mixed_conflict_batch);
    MACRO_ADD(tests, raft_candidate_split_vote_recampaign);
    MACRO_ADD(tests, raft_leader_requires_peer_ack_to_commit_noop);
    MACRO_ADD(tests, raft_multi_node_smoke_test);
    MACRO_ADD(tests, raft_conf_add_node_applies_on_commit);
    MACRO_ADD(tests, raft_conf_remove_node_applies_on_commit);
    MACRO_ADD(tests, raft_conf_change_applies_only_on_commit);
    MACRO_ADD(tests, raft_conf_truncate_uncommitted_config_is_safe);
    MACRO_ADD(tests, raft_leader_ignores_append_res_beyond_log_end);
    MACRO_ADD(tests, raft_create_rejects_duplicate_peers);
    MACRO_ADD(tests, raft_create_rejects_null_peers_when_num_peers_nonzero);
    MACRO_ADD(tests, raft_follower_rejects_conflict_before_commit_index);
    MACRO_ADD(tests, raft_leader_tick_sends_heartbeat_to_all_peers);
    MACRO_ADD(tests, raft_follower_tick_does_nothing);
    MACRO_ADD(tests, raft_leader_tick_sends_pending_entries_to_lagging_peer);
    MACRO_ADD(tests, raft_pre_vote_prevents_disruption);
    MACRO_ADD(tests, raft_learner_does_not_vote_or_count_in_quorum);
    MACRO_ADD(tests, raft_read_index_success_on_heartbeat_quorum);
    MACRO_ADD(tests, raft_check_quorum_steps_down_stale_leader);
    MACRO_ADD(tests, raft_leader_stepdown_on_self_removal);
    MACRO_ADD(tests, raft_fault_duplicate_snapshot_install_is_safe);
    MACRO_ADD(tests, raft_fault_stale_read_index_response_ignored);
    MACRO_ADD(tests, raft_fault_learner_promotion_after_leader_crash);

    macro_run_all("raft_core", tests, test_count);
    return 0;
}
