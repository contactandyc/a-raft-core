// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "raft_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(raft_follower_rejects_log_gaps) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    uint8_t payload[] = "data";
    raft_entry_t entry = { .term = 1, .index = 5, .data = payload, .data_len = 4 };

    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                        .index = 4, .log_term = 1, .entries = &entry, .num_entries = 1, .commit = 0 };

    raft_step_remote(r, &app1);

    MACRO_ASSERT_EQ_INT(raft_last_index(r), 0);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_RES);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_truncates_on_conflict) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t entry1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &entry1, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app1);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);
    raft_advance_all_for_tests_only(r);

    raft_entry_t conflict = { .term = 2, .index = 1, .data = (uint8_t*)"B", .data_len = 1 };
    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 3, .term = 2,
                        .index = 0, .log_term = 0, .entries = &conflict, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app2);

    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);
    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == false);

    raft_destroy(r);
}

MACRO_TEST(raft_figure_8_anomaly_prevention) {
    uint64_t peers[] = {2, 3, 4, 5};
    raft_t* r = raft_create(1, peers, 4);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    for (int i = 2; i <= 3; i++) {
        raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = i, .term = 1, .reject = false };
        raft_step_remote(r, &pv);
    }
    for (int i = 2; i <= 3; i++) {
        raft_msg_t v = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = i, .term = 1, .reject = false };
        raft_step_remote(r, &v);
    }
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    raft_advance_all_for_tests_only(r);

    uint8_t data[] = "data";
    raft_entry_t entry = { .data = data, .data_len = 4 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &entry, .num_entries = 1 };
    raft_step_local(r, &prop);

    raft_msg_t app_fake = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 3,
                            .index = 0, .log_term = 0, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_step_remote(r, &app_fake);

    raft_msg_t hup2 = { .type = MSG_HUP };
    raft_step_local(r, &hup2);
    for (int i = 2; i <= 3; i++) {
        raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = i, .term = 4, .reject = false };
        raft_step_remote(r, &pv);
    }
    for (int i = 2; i <= 3; i++) {
        raft_msg_t v = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = i, .term = 4, .reject = false };
        raft_step_remote(r, &v);
    }
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    raft_advance_all_for_tests_only(r);

    raft_msg_t ack1 = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 4, .reject = false, .index = 2 };
    raft_step_remote(r, &ack1);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 0);

    raft_msg_t ack2 = { .type = MSG_APPEND_RES, .to = 1, .from = 3, .term = 4, .reject = false, .index = 2 };
    raft_step_remote(r, &ack2);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 0);

    raft_msg_t ack3 = { .type = MSG_APPEND_RES, .to = 1, .from = 3, .term = 4, .reject = false, .index = 3 };
    raft_step_remote(r, &ack3);
    raft_msg_t ack4 = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 4, .reject = false, .index = 3 };
    raft_step_remote(r, &ack4);

    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 3);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_backtracks_next_index_on_reject) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    uint8_t data[] = "X";
    raft_entry_t entry = { .data = data, .data_len = 1 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &entry, .num_entries = 1 };
    raft_step_local(r, &prop);
    raft_step_local(r, &prop);
    raft_advance_all_for_tests_only(r);

    raft_msg_t rej = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = true, .index = 3, .conflict_index = 3 };
    raft_step_remote(r, &rej);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_ENTRIES);
    MACRO_ASSERT_EQ_INT(ready.messages[0].index, 2);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_ignores_lower_term_append) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);

    raft_step_local(r, &hup);
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 2, .reject = false };
    raft_step_remote(r, &pv2);
    raft_msg_t vote2 = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 2, .reject = false };
    raft_step_remote(r, &vote2);
    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 3, .term = 1 };
    raft_step_remote(r, &app);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_term(r), 2);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_RES);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);
    MACRO_ASSERT_EQ_INT(ready.messages[0].term, 2);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_updates_commit_from_heartbeat) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t entry1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t entry2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };
    raft_entry_t batch[] = {entry1, entry2};

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = batch, .num_entries = 2, .commit = 0 };
    raft_step_remote(r, &app);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 0);
    raft_advance_all_for_tests_only(r);

    raft_msg_t hb = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                      .index = 2, .log_term = 1, .entries = NULL, .num_entries = 0, .commit = 2 };
    raft_step_remote(r, &hb);

    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 2);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 2);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_appends_multiple_entries) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };
    raft_entry_t e3 = { .term = 1, .index = 3, .data = (uint8_t*)"3", .data_len = 1 };
    raft_entry_t batch[] = {e1, e2, e3};

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = batch, .num_entries = 3, .commit = 0 };

    raft_step_remote(r, &app);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 3);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_backtracks_multiple_times) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    raft_entry_t d = { .data = NULL, .data_len = 0 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &d, .num_entries = 1 };
    raft_step_local(r, &p);
    raft_step_local(r, &p);
    raft_advance_all_for_tests_only(r);

    raft_msg_t r1 = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = true, .index = 3, .conflict_index = 3 };
    raft_step_remote(r, &r1);
    raft_ready_t rd1 = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(rd1.messages[0].index, 2);
    raft_advance_all_for_tests_only(r);

    raft_msg_t r2 = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = true, .index = 2, .conflict_index = 2 };
    raft_step_remote(r, &r2);
    raft_ready_t rd2 = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(rd2.messages[0].index, 1);
    raft_advance_all_for_tests_only(r);

    raft_msg_t r3 = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = true, .index = 1, .conflict_index = 1 };
    raft_step_remote(r, &r3);
    raft_ready_t rd3 = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(rd3.messages[0].index, 0);

    raft_destroy(r);
}

MACRO_TEST(raft_advance_clears_messages_and_committed_entries) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.num_messages > 0);

    raft_advance_all_for_tests_only(r);
    ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 0);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_conflict_replacement_multiple_entries) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_entry_t e3 = { .term = 1, .index = 3, .data = (uint8_t*)"C", .data_len = 1 };
    raft_entry_t batch1[] = { e1, e2, e3 };

    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = batch1, .num_entries = 3, .commit = 0 };
    raft_step_remote(r, &app1);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 3);
    raft_advance_all_for_tests_only(r);

    raft_entry_t nx2 = { .term = 1, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_entry_t nx3 = { .term = 1, .index = 3, .data = (uint8_t*)"C", .data_len = 1 };
    raft_entry_t nx4 = { .term = 2, .index = 4, .data = (uint8_t*)"D", .data_len = 1 };
    raft_entry_t batch2[] = { nx2, nx3, nx4 };

    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 2,
                        .index = 1, .log_term = 1, .entries = batch2, .num_entries = 3, .commit = 0 };
    raft_step_remote(r, &app2);

    MACRO_ASSERT_EQ_INT(raft_last_index(r), 4);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 1);
    MACRO_ASSERT_EQ_INT(ready.entries_to_save[0].index, 4);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_duplicate_append_is_idempotent) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };

    raft_step_remote(r, &app);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);
    raft_advance_all_for_tests_only(r);

    raft_step_remote(r, &app);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_FALSE(ready.messages[0].reject);
    MACRO_ASSERT_EQ_INT(ready.messages[0].index, 1);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_ignores_propose) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };

    raft_step_local(r, &p);

    MACRO_ASSERT_EQ_INT(raft_last_index(r), 0);
    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 0);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_ignores_unknown_peer_append_res) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t v1 = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &v1);
    raft_advance_all_for_tests_only(r);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step_local(r, &p);
    raft_advance_all_for_tests_only(r);

    raft_msg_t ack = { .type = MSG_APPEND_RES, .to = 1, .from = 99, .term = 1, .reject = false, .index = 2 };
    raft_step_remote(r, &ack);

    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 0);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_ignores_stale_append_res) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv1 = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv1);

    raft_step_local(r, &hup);
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 2, .reject = false };
    raft_step_remote(r, &pv2);
    raft_msg_t v1 = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 2, .reject = false };
    raft_step_remote(r, &v1);
    raft_advance_all_for_tests_only(r);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step_local(r, &p);
    raft_advance_all_for_tests_only(r);

    raft_msg_t ack = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_step_remote(r, &ack);

    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 0);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_commit_never_decreases) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 1 };
    raft_step_remote(r, &app);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 1);
    raft_advance_all_for_tests_only(r);

    raft_msg_t hb = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                      .index = 1, .log_term = 1, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_step_remote(r, &hb);

    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 1);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_commit_clamped_to_last_index) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 99 };
    raft_step_remote(r, &app);

    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 1);

    raft_destroy(r);
}

MACRO_TEST(raft_apply_clears_committed_entries) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };
    raft_entry_t batch[] = { e1, e2 };

    raft_msg_t app = {
        .type = MSG_APPEND_ENTRIES, .to = 1,
        .from = 2, .term = 1, .index = 0, .log_term = 0,
        .entries = batch, .num_entries = 2, .commit = 2
    };

    raft_step_remote(r, &app);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 2);

    raft_advance_all_for_tests_only(r);

    ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 0);

    raft_destroy(r);
}

MACRO_TEST(raft_ignores_messages_not_addressed_to_self) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 3, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = NULL, .num_entries = 0, .commit = 0 };

    raft_step_remote(r, &app);

    MACRO_ASSERT_EQ_INT(raft_term(r), 0);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 0);

    raft_destroy(r);
}

MACRO_TEST(raft_rejects_append_entries_with_null_entries_and_count) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = NULL, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app);

    MACRO_ASSERT_EQ_INT(raft_last_index(r), 0);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_rejects_empty_proposal) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = NULL, .num_entries = 0 };
    raft_step_local(r, &prop);

    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);

    raft_destroy(r);
}

MACRO_TEST(raft_ready_populates_entries_to_save) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 1);

    raft_advance_all_for_tests_only(r);

    ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 0);

    raft_destroy(r);
}

MACRO_TEST(raft_committed_entry_content_is_correct) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"HELLO", .data_len = 5 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 1 };

    raft_step_remote(r, &app);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 1);
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].index, 1);
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].term, 1);
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].data_len, 5);
    MACRO_ASSERT_TRUE(memcmp(ready.committed_entries[0].data, "HELLO", 5) == 0);

    raft_destroy(r);
}

MACRO_TEST(raft_ready_populates_entries_to_save_on_follower_append) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };

    raft_step_remote(r, &app);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 1);
    MACRO_ASSERT_EQ_INT(ready.entries_to_save[0].index, 1);
    MACRO_ASSERT_EQ_INT(ready.entries_to_save[0].term, 1);

    raft_advance_all_for_tests_only(r);
    ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 0);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_ignores_zero_entry_proposal_even_with_entries_pointer) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 0 };

    raft_step_local(r, &p);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_appends_multiple_proposed_entries) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    raft_entry_t e1 = { .data = (uint8_t*)"X", .data_len = 1 };
    raft_entry_t e2 = { .data = (uint8_t*)"Y", .data_len = 1 };
    raft_entry_t batch[] = { e1, e2 };

    raft_msg_t p = { .type = MSG_PROPOSE, .entries = batch, .num_entries = 2 };
    raft_step_local(r, &p);

    MACRO_ASSERT_EQ_INT(raft_last_index(r), 3);

    raft_destroy(r);
}

MACRO_TEST(raft_ready_returns_only_newly_committed_entries_after_apply) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };

    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &e1, .num_entries = 1, .commit = 1 };
    raft_step_remote(r, &app1);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 1);
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].index, 1);

    raft_advance_all_for_tests_only(r);

    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                        .index = 1, .log_term = 1, .entries = &e2, .num_entries = 1, .commit = 2 };
    raft_step_remote(r, &app2);

    ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 1);
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].index, 2);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_duplicate_append_with_higher_commit_advances_commit) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app1);
    raft_advance_all_for_tests_only(r);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 0);

    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 1 };
    raft_step_remote(r, &app2);

    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 1);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_rejects_wrong_prev_log_term) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &e1, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app1);
    raft_advance_all_for_tests_only(r);

    raft_entry_t e2 = { .term = 2, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 2,
                        .index = 1, .log_term = 2, .entries = &e2, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app2);

    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);
    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);

    raft_destroy(r);
}

MACRO_TEST(raft_append_reject_reports_last_index) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_entry_t batch[] = { e1, e2 };

    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = batch, .num_entries = 2, .commit = 0 };
    raft_step_remote(r, &app1);
    raft_advance_all_for_tests_only(r);

    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                        .index = 4, .log_term = 1, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_step_remote(r, &app2);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);
    MACRO_ASSERT_EQ_INT(ready.messages[0].conflict_index, 3);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_heartbeat_clamps_commit_to_prev_index) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };
    raft_entry_t batch[] = { e1, e2 };

    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = batch, .num_entries = 2, .commit = 0 };
    raft_step_remote(r, &app1);
    raft_advance_all_for_tests_only(r);

    raft_msg_t hb = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                      .index = 1, .log_term = 1, .entries = NULL, .num_entries = 0, .commit = 5 };
    raft_step_remote(r, &hb);

    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 1);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_backoff_floor) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    raft_msg_t rj = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = true, .index = raft_last_index(r), .conflict_index = 0 };
    raft_step_remote(r, &rj);
    raft_step_remote(r, &rj);
    raft_step_remote(r, &rj);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.num_messages > 0);
    MACRO_ASSERT_EQ_INT(ready.messages[ready.num_messages - 1].index, 0);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_ignores_out_of_bounds_match_index) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    raft_msg_t ack = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 999 };
    raft_step_remote(r, &ack);

    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 0);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_mixed_conflict_batch) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_entry_t e3 = { .term = 1, .index = 3, .data = (uint8_t*)"C", .data_len = 1 };
    raft_entry_t b1[] = { e1, e2, e3 };
    raft_msg_t a1 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0, .entries = b1, .num_entries = 3 };
    raft_step_remote(r, &a1);
    raft_advance_all_for_tests_only(r);

    raft_entry_t nx2 = { .term = 1, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_entry_t nx3 = { .term = 1, .index = 3, .data = (uint8_t*)"C", .data_len = 1 };
    raft_entry_t nx4 = { .term = 2, .index = 4, .data = (uint8_t*)"D", .data_len = 1 };
    raft_entry_t b2[] = { nx2, nx3, nx4 };

    raft_msg_t a2 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 2, .index = 1, .log_term = 1, .entries = b2, .num_entries = 3 };
    raft_step_remote(r, &a2);

    MACRO_ASSERT_EQ_INT(raft_last_index(r), 4);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 1);
    MACRO_ASSERT_EQ_INT(ready.entries_to_save[0].index, 4);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_requires_peer_ack_to_commit_noop) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 0);

    raft_destroy(r);
}

MACRO_TEST(raft_multi_node_smoke_test) {
    uint64_t peers1[] = {2, 3};
    uint64_t peers2[] = {1, 3};
    uint64_t peers3[] = {1, 2};

    raft_t* nodes[3] = {
        raft_create(1, peers1, 2),
        raft_create(2, peers2, 2),
        raft_create(3, peers3, 2)
    };

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(nodes[0], &hup);

    bool active = true;
    int cycles = 0;
    while (active && cycles < 20) {
        active = false;
        cycles++;

        for (int i = 0; i < 3; i++) {
            raft_ready_t rd = raft_get_ready(nodes[i]);
            for (size_t m = 0; m < rd.num_messages; m++) {
                active = true;
                int target = rd.messages[m].to - 1;
                raft_step_remote(nodes[target], &rd.messages[m]);
            }
            raft_advance_all_for_tests_only(nodes[i]);
        }
    }

    MACRO_ASSERT_TRUE(raft_state(nodes[0]) == RAFT_STATE_LEADER);
    MACRO_ASSERT_TRUE(raft_state(nodes[1]) == RAFT_STATE_FOLLOWER);

    raft_entry_t e = { .data = (uint8_t*)"Hello", .data_len = 5 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step_local(nodes[0], &p);

    active = true;
    while (active) {
        active = false;
        for (int i = 0; i < 3; i++) {
            raft_ready_t rd = raft_get_ready(nodes[i]);
            for (size_t m = 0; m < rd.num_messages; m++) {
                active = true;
                int target = rd.messages[m].to - 1;
                raft_step_remote(nodes[target], &rd.messages[m]);
            }
            raft_advance_all_for_tests_only(nodes[i]);
        }
    }

    raft_msg_t tick = { .type = MSG_TICK };
    raft_step_local(nodes[0], &tick);

    active = true;
    while (active) {
        active = false;
        for (int i = 0; i < 3; i++) {
            raft_ready_t rd = raft_get_ready(nodes[i]);
            for (size_t m = 0; m < rd.num_messages; m++) {
                active = true;
                int target = rd.messages[m].to - 1;
                raft_step_remote(nodes[target], &rd.messages[m]);
            }
            raft_advance_all_for_tests_only(nodes[i]);
        }
    }

    MACRO_ASSERT_EQ_INT(raft_commit_index(nodes[0]), 2);
    MACRO_ASSERT_EQ_INT(raft_commit_index(nodes[1]), 2);
    MACRO_ASSERT_EQ_INT(raft_commit_index(nodes[2]), 2);

    raft_destroy(nodes[0]);
    raft_destroy(nodes[1]);
    raft_destroy(nodes[2]);
}

MACRO_TEST(raft_leader_ignores_append_res_beyond_log_end) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    raft_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step_local(r, &prop);
    raft_advance_all_for_tests_only(r);

    raft_msg_t bogus = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 999 };
    raft_step_remote(r, &bogus);

    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 0);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_rejects_conflict_before_commit_index) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0,
                        .entries = &e1, .num_entries = 1, .commit = 1 };
    raft_step_remote(r, &app1);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 1);
    raft_advance_all_for_tests_only(r);

    raft_entry_t conflict = { .term = 2, .index = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"B", .data_len = 1 };
    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 3, .term = 2, .index = 0, .log_term = 0,
                        .entries = &conflict, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app2);

    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 1);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_tick_sends_heartbeat_to_all_peers) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    raft_msg_t tick = { .type = MSG_TICK };
    raft_step_local(r, &tick);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 2);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_ENTRIES);
    MACRO_ASSERT_TRUE(ready.messages[1].type == MSG_APPEND_ENTRIES);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_tick_does_nothing) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t tick = { .type = MSG_TICK };
    raft_step_local(r, &tick);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 0);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_tick_sends_pending_entries_to_lagging_peer) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step_local(r, &prop);

    raft_advance_all_for_tests_only(r);

    raft_msg_t tick = { .type = MSG_TICK };
    raft_step_local(r, &tick);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 2);

    raft_msg_t* to_node_3 = ready.messages[0].to == 3 ? &ready.messages[0] : &ready.messages[1];

    MACRO_ASSERT_EQ_INT(to_node_3->num_entries, 0);

    uint64_t hb_prev_index = to_node_3->index;
    raft_advance_all_for_tests_only(r);

    raft_msg_t rej = { .type = MSG_APPEND_RES, .to = 1, .from = 3, .term = 1, .reject = true, .index = hb_prev_index, .conflict_index = 1 };
    raft_step_remote(r, &rej);

    ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].num_entries > 0);

    raft_destroy(r);
}

// ----------------------------------------------------------------------------
// BOUNDARY DEFENSE TESTS
// ----------------------------------------------------------------------------

MACRO_TEST(raft_success_append_res_beyond_log_end_does_not_update_match_or_commit) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    // Leader has exactly 1 entry (the no-op).
    // Peer attempts to trick leader into committing index 5
    raft_msg_t ack = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 5 };
    raft_step_remote(r, &ack);

    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 0);
    raft_destroy(r);
}

MACRO_TEST(raft_success_append_res_beyond_log_end_does_not_satisfy_readindex) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    // Ack the noop so ReadIndex is allowed
    raft_msg_t valid_ack = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 1 };
    raft_step_remote(r, &valid_ack);

    raft_msg_t ri = { .type = MSG_READ_INDEX, .read_seq = 123 };
    raft_step_local(r, &ri);

    // Malicious or broken peer tries to answer the read sequence but lies about the index it appended
    raft_msg_t bad_ack = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 999, .read_seq = 123 };
    raft_step_remote(r, &bad_ack);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 0); // Read is ignored
    raft_destroy(r);
}

MACRO_TEST(raft_valid_append_res_marks_recent_active) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    // Clear recent activity
    raft_msg_t chk = { .type = MSG_CHECK_QUORUM };
    raft_step_local(r, &chk);
    MACRO_ASSERT_FALSE(r->recent_active[0]); // Node 2

    raft_msg_t ack = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 1 };
    raft_step_remote(r, &ack);

    MACRO_ASSERT_TRUE(r->recent_active[0]); // Successfully restored activity!
    raft_destroy(r);
}

MACRO_TEST(raft_rejected_append_res_marks_recent_active) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    // Clear recent activity
    raft_msg_t chk = { .type = MSG_CHECK_QUORUM };
    raft_step_local(r, &chk);

    // A rejection still proves the node is alive
    raft_msg_t rej = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = true, .index = 1, .conflict_index = 0 };
    raft_step_remote(r, &rej);

    MACRO_ASSERT_TRUE(r->recent_active[0]);
    raft_destroy(r);
}

MACRO_TEST(raft_append_entries_rejects_entry_index_mismatch) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    // The leader claims index=0, so the first payload should have index=1
    // The malicious payload explicitly declares index=99
    raft_entry_t e = { .term = 1, .index = 99, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };

    raft_step_remote(r, &app);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject); // The engine rejects the mismatch safely
    raft_destroy(r);
}

MACRO_TEST(raft_append_entries_rejects_entry_term_zero) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    // Term 0 is reserved mathematically for baseline snapshots
    raft_entry_t e = { .term = 0, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };

    raft_step_remote(r, &app);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);
    raft_destroy(r);
}

MACRO_TEST(raft_append_entries_rejects_entry_term_greater_than_leader_term) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    // The payload declares a term higher than the packet envelope
    raft_entry_t e = { .term = 5, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };

    raft_step_remote(r, &app);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);
    raft_destroy(r);
}

MACRO_TEST(raft_append_entries_rejects_data_len_with_null_data) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    // Packet declares 100 bytes of data but supplies a NULL pointer
    raft_entry_t e = { .term = 1, .index = 1, .data = NULL, .data_len = 100 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };

    raft_step_remote(r, &app);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);
    raft_destroy(r);
}

MACRO_TEST(raft_reject_conflict_index_clamped_to_log_bounds) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    // Leader has exactly 1 entry. Follower rejects and demands we backtrack to index 99.
    raft_msg_t rej = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = true, .index = 1, .conflict_index = 99 };
    raft_step_remote(r, &rej);

    // Engine must clamp backtrack target to LastLogIndex + 1 (which is 2)
    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.messages[0].index, 1);
    raft_destroy(r);
}

MACRO_TEST(raft_oversized_single_entry_is_rejected_before_replication) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    // FIX: Safely allocate a real 2MB buffer so memcpy doesn't crash
    uint32_t huge_len = 2000000;
    uint8_t* huge_data = calloc(1, huge_len);

    raft_entry_t e = { .type = ENTRY_NORMAL, .data = huge_data, .data_len = huge_len };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step_local(r, &prop);

    raft_ready_t ready = raft_get_ready(r);

    MACRO_ASSERT_EQ_INT(ready.num_messages, 0);
    MACRO_ASSERT_FALSE(raft_has_fatal_error(r));

    free(huge_data);
    raft_destroy(r);
}

MACRO_TEST(leader_uses_reject_hints_even_when_follower_log_is_longer) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    // Leader has 1 entry. Follower has 20 uncommitted entries and rejects with index=20, conflict=6
    raft_msg_t rej = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = true, .index = 20, .conflict_index = 6 };
    raft_step_remote(r, &rej);

    // The leader MUST NOT ignore the rejection. It must clamp the backtrack to its own tail + 1.
    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_ENTRIES);

    // Leader's last index is 1. Backtrack target was 6. Clamped target is 2. Therefore, it sends next_index = 2.
    MACRO_ASSERT_EQ_INT(ready.messages[0].index, 1); // prevLogIndex = next - 1 = 1

    raft_destroy(r);
}

MACRO_TEST(leader_resends_snapshot_from_conflict_index_on_chunk_reject) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    // Become Leader
    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    // Seed the log with 10 entries so it can actually be compacted!
    for (int i=0; i<10; i++) {
        raft_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
        raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
        raft_step_local(r, &p);
        raft_msg_t ack = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 2+i };
        raft_step_remote(r, &ack);
    }
    raft_advance_all_for_tests_only(r);

    // FIX: Enforce exact boundary required by the new safety rules!
    r->last_applied = 10;

    // Now we can successfully compact at index 10
    raft_compact_after_snapshot(r, 10, 1);

    // Force node 2 to require a snapshot
    raft_msg_t req = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = true, .index = 0, .conflict_index = 0 };
    raft_step_remote(r, &req);
    raft_advance_all_for_tests_only(r); // Clears the initial snapshot dispatch

    // Simulate Node 2 rejecting an intermediate chunk and requesting offset 1024
    raft_msg_t rej = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = true, .index = 10, .conflict_index = 1024 };
    raft_step_remote(r, &rej);

    raft_ready_t ready = raft_get_ready(r);

    // Assert the leader resends starting specifically from the requested conflict index
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_INSTALL_SNAPSHOT);
    MACRO_ASSERT_EQ_INT(ready.messages[0].snapshot_offset, 1024);

    raft_destroy(r);
}

MACRO_TEST(raft_follower_rejects_payload_exceeding_max_payload_size) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    uint32_t huge_len = RAFT_MAX_PAYLOAD_SIZE + 1;
    uint8_t* huge_data = calloc(1, huge_len);

    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_NORMAL, .data = huge_data, .data_len = huge_len };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = &e, .num_entries = 1, .commit = 0 };

    raft_step_remote(r, &app);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);

    // Core MUST NOT crash or alter state!
    MACRO_ASSERT_FALSE(raft_has_fatal_error(r));
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 0);

    free(huge_data);
    raft_destroy(r);
}

MACRO_TEST(raft_follower_rejects_wrapped_append_index) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_NORMAL, .data_len = 0 };
    // Maliciously set index just below UINT64_MAX to cause math overflow during log iteration
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = UINT64_MAX - 1, .log_term = 0,
                       .entries = &e, .num_entries = 5, .commit = 0 };

    raft_step_remote(r, &app);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);
    raft_destroy(r);
}

MACRO_TEST(raft_follower_rejects_invalid_entry_type) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .type = 99, .data_len = 0 }; // 99 is not a valid enum type
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = &e, .num_entries = 1, .commit = 0 };

    raft_step_remote(r, &app);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);
    raft_destroy(r);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, raft_follower_rejects_log_gaps);
    MACRO_ADD(tests, raft_follower_truncates_on_conflict);
    MACRO_ADD(tests, raft_figure_8_anomaly_prevention);
    MACRO_ADD(tests, raft_leader_backtracks_next_index_on_reject);
    MACRO_ADD(tests, raft_leader_ignores_lower_term_append);
    MACRO_ADD(tests, raft_follower_updates_commit_from_heartbeat);
    MACRO_ADD(tests, raft_follower_appends_multiple_entries);
    MACRO_ADD(tests, raft_leader_backtracks_multiple_times);
    MACRO_ADD(tests, raft_advance_clears_messages_and_committed_entries);
    MACRO_ADD(tests, raft_follower_conflict_replacement_multiple_entries);
    MACRO_ADD(tests, raft_follower_duplicate_append_is_idempotent);
    MACRO_ADD(tests, raft_follower_ignores_propose);
    MACRO_ADD(tests, raft_leader_ignores_unknown_peer_append_res);
    MACRO_ADD(tests, raft_leader_ignores_stale_append_res);
    MACRO_ADD(tests, raft_follower_commit_never_decreases);
    MACRO_ADD(tests, raft_follower_commit_clamped_to_last_index);
    MACRO_ADD(tests, raft_apply_clears_committed_entries);
    MACRO_ADD(tests, raft_ignores_messages_not_addressed_to_self);
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
    MACRO_ADD(tests, raft_follower_heartbeat_clamps_commit_to_prev_index);
    MACRO_ADD(tests, raft_leader_backoff_floor);
    MACRO_ADD(tests, raft_leader_ignores_out_of_bounds_match_index);
    MACRO_ADD(tests, raft_follower_mixed_conflict_batch);
    MACRO_ADD(tests, raft_leader_requires_peer_ack_to_commit_noop);
    MACRO_ADD(tests, raft_multi_node_smoke_test);
    MACRO_ADD(tests, raft_leader_ignores_append_res_beyond_log_end);
    MACRO_ADD(tests, raft_follower_rejects_conflict_before_commit_index);
    MACRO_ADD(tests, raft_leader_tick_sends_heartbeat_to_all_peers);
    MACRO_ADD(tests, raft_follower_tick_does_nothing);
    MACRO_ADD(tests, raft_leader_tick_sends_pending_entries_to_lagging_peer);

    // BOUNDARY DEFENSE TESTS
    MACRO_ADD(tests, raft_success_append_res_beyond_log_end_does_not_update_match_or_commit);
    MACRO_ADD(tests, raft_success_append_res_beyond_log_end_does_not_satisfy_readindex);
    MACRO_ADD(tests, raft_valid_append_res_marks_recent_active);
    MACRO_ADD(tests, raft_rejected_append_res_marks_recent_active);
    MACRO_ADD(tests, raft_append_entries_rejects_entry_index_mismatch);
    MACRO_ADD(tests, raft_append_entries_rejects_entry_term_zero);
    MACRO_ADD(tests, raft_append_entries_rejects_entry_term_greater_than_leader_term);
    MACRO_ADD(tests, raft_append_entries_rejects_data_len_with_null_data);
    MACRO_ADD(tests, raft_reject_conflict_index_clamped_to_log_bounds);
    MACRO_ADD(tests, raft_oversized_single_entry_is_rejected_before_replication);

    MACRO_ADD(tests, leader_uses_reject_hints_even_when_follower_log_is_longer);
    MACRO_ADD(tests, leader_resends_snapshot_from_conflict_index_on_chunk_reject);
    MACRO_ADD(tests, raft_follower_rejects_payload_exceeding_max_payload_size);

    MACRO_ADD(tests, raft_follower_rejects_wrapped_append_index);
    MACRO_ADD(tests, raft_follower_rejects_invalid_entry_type);

    macro_run_all("raft_replication", tests, test_count);
    return 0;
}
