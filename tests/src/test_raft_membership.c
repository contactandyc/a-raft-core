// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <string.h>
#include "a-raft-library/raft.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(raft_create_rejects_too_many_peers_and_self) {
    uint64_t invalid_self_peers[] = {1, 2};
    raft_t* r1 = raft_create(1, invalid_self_peers, 2);
    MACRO_ASSERT_TRUE(r1 == NULL);

    uint64_t too_many_peers[100] = {0};
    raft_t* r2 = raft_create(1, too_many_peers, 100);
    MACRO_ASSERT_TRUE(r2 == NULL);
}

MACRO_TEST(raft_create_rejects_duplicate_peers) {
    uint64_t peers[] = {2, 2};
    raft_t* r = raft_create(1, peers, 2);
    MACRO_ASSERT_TRUE(r == NULL);
}

MACRO_TEST(raft_create_rejects_null_peers_when_num_peers_nonzero) {
    raft_t* r = raft_create(1, NULL, 2);
    MACRO_ASSERT_TRUE(r == NULL);
}

MACRO_TEST(raft_conf_change_applies_only_on_commit) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    uint64_t new_node = 4;
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_CONF_ADD_LEARNER, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = &e, .num_entries = 1, .commit = 0 };
    raft_step(r, &app);

    uint64_t active_peers[16];
    size_t num = raft_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);

    raft_msg_t hb = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 1, .log_term = 1,
                       .entries = NULL, .num_entries = 0, .commit = 1 };
    raft_step(r, &hb);

    raft_advance_all(r);

    num = raft_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 3);
    MACRO_ASSERT_EQ_INT(active_peers[2], 4);

    raft_destroy(r);
}

MACRO_TEST(raft_conf_truncate_uncommitted_config_is_safe) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    uint64_t new_node = 4;
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_CONF_ADD_LEARNER, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };
    raft_step(r, &app);

    raft_entry_t conflict = { .term = 2, .index = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t overwrite = { .type = MSG_APPEND_ENTRIES, .from = 3, .term = 2,
                             .index = 0, .log_term = 0, .entries = &conflict, .num_entries = 1, .commit = 0 };
    raft_step(r, &overwrite);

    uint64_t active_peers[16];
    size_t num = raft_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);

    raft_destroy(r);
}

MACRO_TEST(raft_conf_add_node_applies_on_commit) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    uint64_t active_peers[16];
    size_t num = raft_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &vote);
    raft_advance_all(r);

    uint64_t new_node = 4;
    raft_entry_t e = { .type = ENTRY_CONF_ADD_LEARNER, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step(r, &p);

    num = raft_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);

    raft_msg_t ack = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_step(r, &ack);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 2);

    raft_advance_all(r);

    num = raft_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 3);
    MACRO_ASSERT_EQ_INT(active_peers[2], 4);

    raft_destroy(r);
}

MACRO_TEST(raft_conf_remove_node_applies_on_commit) {
    uint64_t peers[] = {2, 3, 4};
    raft_t* r = raft_create(1, peers, 3);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv1 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv1);
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .from = 3, .term = 1, .reject = false };
    raft_step(r, &pv2);

    raft_msg_t vote1 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_msg_t vote2 = { .type = MSG_REQUEST_VOTE_RES, .from = 3, .term = 1, .reject = false };
    raft_step(r, &vote1);
    raft_step(r, &vote2);
    raft_advance_all(r);

    uint64_t rm_node = 3;
    raft_entry_t e = { .type = ENTRY_CONF_REMOVE, .data = (uint8_t*)&rm_node, .data_len = sizeof(uint64_t) };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step(r, &p);

    uint64_t active_peers[16];
    size_t num = raft_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 3);

    raft_msg_t ack1 = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_msg_t ack2 = { .type = MSG_APPEND_RES, .from = 4, .term = 1, .reject = false, .index = 2 };
    raft_step(r, &ack1);
    raft_step(r, &ack2);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 2);

    raft_advance_all(r);

    num = raft_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);
    MACRO_ASSERT_EQ_INT(active_peers[0], 2);
    MACRO_ASSERT_EQ_INT(active_peers[1], 4);

    raft_destroy(r);
}

MACRO_TEST(raft_learner_does_not_vote_or_count_in_quorum) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);
    raft_add_learner(r, 3);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &vote);
    raft_advance_all(r);

    raft_msg_t ack2_noop = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 1 };
    raft_step(r, &ack2_noop);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 1);

    raft_entry_t e_data = { .term = 1, .index = 2, .type = ENTRY_NORMAL, .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p_data = { .type = MSG_PROPOSE, .entries = &e_data, .num_entries = 1 };
    raft_step(r, &p_data);
    raft_advance_all(r);

    raft_msg_t ack3 = { .type = MSG_APPEND_RES, .from = 3, .term = 1, .reject = false, .index = 2 };
    raft_step(r, &ack3);

    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 1);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_stepdown_on_self_removal) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &vote);
    raft_advance_all(r);

    uint64_t rm_node = 1;
    raft_entry_t e = { .term = 1, .index = 2, .type = ENTRY_CONF_REMOVE, .data = (uint8_t*)&rm_node, .data_len = sizeof(uint64_t) };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step(r, &p);

    raft_msg_t ack2 = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_step(r, &ack2);

    raft_advance_all(r);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);

    raft_destroy(r);
}

MACRO_TEST(raft_fault_learner_promotion_after_leader_crash) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    uint64_t node3 = 3;
    raft_entry_t conf_add = { .type = ENTRY_CONF_ADD_LEARNER, .data = (uint8_t*)&node3, .data_len = sizeof(uint64_t) };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 0, .log_term = 0, .entries = &conf_add, .num_entries = 1, .commit = 1 };
    raft_step(r, &app);
    raft_advance_all(r);

    uint64_t act_peers[16]; bool is_learner[16];
    size_t num = raft_peers_ext(r, act_peers, is_learner);

    MACRO_ASSERT_EQ_INT(num, 3);
    MACRO_ASSERT_TRUE(is_learner[1]);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 3, .term = 2, .reject = false };
    raft_step(r, &pv);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_PRE_CANDIDATE);

    raft_destroy(r);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, raft_create_rejects_too_many_peers_and_self);
    MACRO_ADD(tests, raft_create_rejects_duplicate_peers);
    MACRO_ADD(tests, raft_create_rejects_null_peers_when_num_peers_nonzero);
    MACRO_ADD(tests, raft_conf_change_applies_only_on_commit);
    MACRO_ADD(tests, raft_conf_truncate_uncommitted_config_is_safe);
    MACRO_ADD(tests, raft_conf_add_node_applies_on_commit);
    MACRO_ADD(tests, raft_conf_remove_node_applies_on_commit);
    MACRO_ADD(tests, raft_learner_does_not_vote_or_count_in_quorum);
    MACRO_ADD(tests, raft_leader_stepdown_on_self_removal);
    MACRO_ADD(tests, raft_fault_learner_promotion_after_leader_crash);
    macro_run_all("raft_membership", tests, test_count);
    return 0;
}
