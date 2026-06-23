// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <string.h>
#include "raft_internal.h"
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

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = &e, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app);

    uint64_t active_peers[16];
    size_t num = raft_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);

    raft_msg_t hb = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 1, .log_term = 1,
                       .entries = NULL, .num_entries = 0, .commit = 1 };
    raft_step_remote(r, &hb);

    raft_advance_all_for_tests_only(r);

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
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app);

    raft_entry_t conflict = { .term = 2, .index = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t overwrite = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 3, .term = 2,
                             .index = 0, .log_term = 0, .entries = &conflict, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &overwrite);

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
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    uint64_t new_node = 4;
    raft_entry_t e = { .type = ENTRY_CONF_ADD_LEARNER, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step_local(r, &p);

    num = raft_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);

    raft_msg_t ack = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_step_remote(r, &ack);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 2);

    raft_advance_all_for_tests_only(r);

    num = raft_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 3);
    MACRO_ASSERT_EQ_INT(active_peers[2], 4);

    raft_destroy(r);
}

MACRO_TEST(raft_conf_remove_node_applies_on_commit) {
    uint64_t peers[] = {2, 3, 4};
    raft_t* r = raft_create(1, peers, 3);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv1 = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv1);
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 3, .term = 1, .reject = false };
    raft_step_remote(r, &pv2);

    raft_msg_t vote1 = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_msg_t vote2 = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 3, .term = 1, .reject = false };
    raft_step_remote(r, &vote1);
    raft_step_remote(r, &vote2);
    raft_advance_all_for_tests_only(r);

    uint64_t rm_node = 3;
    raft_entry_t e = { .type = ENTRY_CONF_REMOVE, .data = (uint8_t*)&rm_node, .data_len = sizeof(uint64_t) };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step_local(r, &p);

    uint64_t active_peers[16];
    size_t num = raft_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 3);

    raft_msg_t ack1 = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_msg_t ack2 = { .type = MSG_APPEND_RES, .to = 1, .from = 4, .term = 1, .reject = false, .index = 2 };
    raft_step_remote(r, &ack1);
    raft_step_remote(r, &ack2);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 2);

    raft_advance_all_for_tests_only(r);

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
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    raft_msg_t ack2_noop = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 1 };
    raft_step_remote(r, &ack2_noop);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 1);

    raft_entry_t e_data = { .term = 1, .index = 2, .type = ENTRY_NORMAL, .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p_data = { .type = MSG_PROPOSE, .entries = &e_data, .num_entries = 1 };
    raft_step_local(r, &p_data);
    raft_advance_all_for_tests_only(r);

    raft_msg_t ack3 = { .type = MSG_APPEND_RES, .to = 1, .from = 3, .term = 1, .reject = false, .index = 2 };
    raft_step_remote(r, &ack3);

    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 1);

    raft_destroy(r);
}

MACRO_TEST(raft_leader_stepdown_on_self_removal) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    uint64_t rm_node = 1;
    raft_entry_t e = { .term = 1, .index = 2, .type = ENTRY_CONF_REMOVE, .data = (uint8_t*)&rm_node, .data_len = sizeof(uint64_t) };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step_local(r, &p);

    raft_msg_t ack2 = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_step_remote(r, &ack2);

    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);

    raft_destroy(r);
}

MACRO_TEST(raft_fault_learner_promotion_after_leader_crash) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    uint64_t node3 = 3;
    // FIX: Add term and index so it passes validation!
    raft_entry_t conf_add = { .term = 1, .index = 1, .type = ENTRY_CONF_ADD_LEARNER, .data = (uint8_t*)&node3, .data_len = sizeof(uint64_t) };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0, .entries = &conf_add, .num_entries = 1, .commit = 1 };
    raft_step_remote(r, &app);
    raft_advance_all_for_tests_only(r);

    uint64_t act_peers[16]; bool is_learner[16];
    size_t num = raft_peers_ext(r, act_peers, is_learner, 16);

    MACRO_ASSERT_EQ_INT(num, 3);
    MACRO_ASSERT_TRUE(is_learner[1]);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 3, .term = 2, .reject = false };
    raft_step_remote(r, &pv);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_PRE_CANDIDATE);

    raft_destroy(r);
}

// ----------------------------------------------------------------------------
// NEW MEMBERSHIP DEFENSE TESTS
// ----------------------------------------------------------------------------

MACRO_TEST(raft_add_learner_existing_voter_does_not_demote) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    raft_add_learner(r, 2); // 2 is already a voter

    uint64_t act_peers[16]; bool is_learner[16];
    raft_peers_ext(r, act_peers, is_learner, 16);

    // Node 2 should still be a voter (index 0 in peers array)
    MACRO_ASSERT_FALSE(is_learner[0]);
    raft_destroy(r);
}

MACRO_TEST(raft_add_learner_self_voter_does_not_demote) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    raft_add_learner(r, 1); // 1 is already a voter (self)

    MACRO_ASSERT_FALSE(r->is_learner_self);
    raft_destroy(r);
}

MACRO_TEST(raft_add_learner_zero_node_id_rejected_or_fatal) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    uint64_t zero_node = 0;
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_CONF_ADD_LEARNER, .data = (uint8_t*)&zero_node, .data_len = sizeof(uint64_t) };

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = &e, .num_entries = 1, .commit = 1 };

    raft_step_remote(r, &app);
    raft_advance_all_for_tests_only(r);

    // The membership applier should refuse to add node 0 and trip fatal_error
    MACRO_ASSERT_TRUE(raft_has_fatal_error(r));
    raft_destroy(r);
}

MACRO_TEST(raft_malformed_config_payload_sets_fatal_error) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    // Payload is only 3 bytes, not a full uint64_t
    uint8_t bad_data[] = {1, 2, 3};
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_CONF_REMOVE, .data = bad_data, .data_len = 3 };

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = &e, .num_entries = 1, .commit = 1 };

    raft_step_remote(r, &app);
    raft_advance_all_for_tests_only(r);

    // Applier must fail safely instead of reading uninitialized memory
    MACRO_ASSERT_TRUE(raft_has_fatal_error(r));
    raft_destroy(r);
}

MACRO_TEST(raft_promote_missing_learner_is_noop) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    raft_promote_learner(r, 99); // 99 doesn't exist

    uint64_t act_peers[16]; bool is_learner[16];
    size_t num = raft_peers_ext(r, act_peers, is_learner, 16);

    MACRO_ASSERT_EQ_INT(num, 2); // Just 1 and 2
    raft_destroy(r);
}

MACRO_TEST(raft_promote_existing_voter_is_noop) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    raft_promote_learner(r, 2); // 2 is already a voter

    uint64_t act_peers[16]; bool is_learner[16];
    raft_peers_ext(r, act_peers, is_learner, 16);

    MACRO_ASSERT_FALSE(is_learner[0]);
    raft_destroy(r);
}

MACRO_TEST(raft_promote_learner_resets_recent_active) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);
    raft_add_learner(r, 3);

    r->recent_active[1] = true; // Spoof learner activity

    raft_promote_learner(r, 3);

    // Upon promotion, recent_active should be reset so it proves itself as a voter
    MACRO_ASSERT_FALSE(r->recent_active[1]);
    raft_destroy(r);
}

MACRO_TEST(raft_remove_peer_clears_trailing_slot_metadata) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    // Artificially dirty the trailing slot (index 1)
    r->next_index[1] = 99;
    r->match_index[1] = 50;
    r->recent_active[1] = true;

    uint64_t rm_node = 2; // Remove index 0
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_CONF_REMOVE, .data = (uint8_t*)&rm_node, .data_len = sizeof(uint64_t) };

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = &e, .num_entries = 1, .commit = 1 };

    raft_step_remote(r, &app);
    raft_advance_all_for_tests_only(r);

    // Index 1 should now be completely zeroed out
    MACRO_ASSERT_EQ_INT(r->next_index[1], 0);
    MACRO_ASSERT_EQ_INT(r->match_index[1], 0);
    MACRO_ASSERT_FALSE(r->recent_active[1]);
    raft_destroy(r);
}

MACRO_TEST(raft_remove_peer_moves_voted_for_me_slot) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    r->voted_for_me[1] = true; // Node 3 voted for us

    uint64_t rm_node = 2; // Remove Node 2 (index 0)
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_CONF_REMOVE, .data = (uint8_t*)&rm_node, .data_len = sizeof(uint64_t) };

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = &e, .num_entries = 1, .commit = 1 };

    raft_step_remote(r, &app);
    raft_advance_all_for_tests_only(r);

    // Node 3 moved from index 1 to index 0. voted_for_me should follow it.
    MACRO_ASSERT_TRUE(r->voted_for_me[0]);
    MACRO_ASSERT_FALSE(r->voted_for_me[1]); // Trailing slot cleared
    raft_destroy(r);
}

MACRO_TEST(raft_membership_change_clears_pending_read_index_state) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    r->state = RAFT_STATE_LEADER;
    r->pending_reads[0].active = true;
    r->pending_reads[0].acks = 1;

    uint64_t new_node = 3;
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_CONF_ADD_LEARNER, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = &e, .num_entries = 1, .commit = 1 };

    raft_step_remote(r, &app);
    raft_advance_all_for_tests_only(r);

    // Changing membership alters quorum, any pending reads must be aborted
    MACRO_ASSERT_FALSE(r->pending_reads[0].active);
    MACRO_ASSERT_EQ_INT(r->pending_reads[0].acks, 0);
    raft_destroy(r);
}

MACRO_TEST(raft_remove_self_clears_leader_id) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    // 1. Legally become the leader
    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(r->leader_id, 1);

    // 2. Leader proposes its own removal
    uint64_t rm_node = 1;
    raft_entry_t e = { .type = ENTRY_CONF_REMOVE, .data = (uint8_t*)&rm_node, .data_len = sizeof(uint64_t) };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step_local(r, &prop);

    // 3. Peer acknowledges it, allowing the leader to commit it
    raft_msg_t ack = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_step_remote(r, &ack);
    raft_advance_all_for_tests_only(r);

    // 4. The leader processes its own removal, steps down, and clears identity
    MACRO_ASSERT_EQ_INT(r->leader_id, 0);
    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_FOLLOWER);

    raft_destroy(r);
}

MACRO_TEST(raft_remove_self_sets_removed_and_learner_self) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    uint64_t rm_node = 1;
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_CONF_REMOVE, .data = (uint8_t*)&rm_node, .data_len = sizeof(uint64_t) };

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = &e, .num_entries = 1, .commit = 1 };

    raft_step_remote(r, &app);
    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_TRUE(r->removed);
    MACRO_ASSERT_TRUE(r->is_learner_self);
    raft_destroy(r);
}

MACRO_TEST(raft_add_self_after_removed_restores_as_learner_only) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    // Remove self
    uint64_t rm_node = 1;
    raft_entry_t e1 = { .term = 1, .index = 1, .type = ENTRY_CONF_REMOVE, .data = (uint8_t*)&rm_node, .data_len = sizeof(uint64_t) };
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = &e1, .num_entries = 1, .commit = 1 };
    raft_step_remote(r, &app1);
    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_TRUE(r->removed);

    // Add self back
    uint64_t add_node = 1;
    raft_entry_t e2 = { .term = 1, .index = 2, .type = ENTRY_CONF_ADD_LEARNER, .data = (uint8_t*)&add_node, .data_len = sizeof(uint64_t) };
    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 1, .log_term = 1,
                       .entries = &e2, .num_entries = 1, .commit = 2 };
    raft_step_remote(r, &app2);
    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_FALSE(r->removed);
    MACRO_ASSERT_TRUE(r->is_learner_self); // Must require explicit promotion to vote again
    raft_destroy(r);
}

MACRO_TEST(committed_unapplied_config_blocks_new_config_proposal) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    // Boot leader
    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    // Propose first config
    uint64_t n3 = 3;
    raft_entry_t e1 = { .type = ENTRY_CONF_ADD_LEARNER, .data = (uint8_t*)&n3, .data_len = 8 };
    raft_msg_t prop1 = { .type = MSG_PROPOSE, .entries = &e1, .num_entries = 1 };
    raft_step_local(r, &prop1);

    // Commit it, but do NOT apply it!
    raft_msg_t ack = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_step_remote(r, &ack);
    raft_advance(r, 2, 1); // Applied is still 1!

    // Propose second config
    uint64_t n4 = 4;
    raft_entry_t e2 = { .type = ENTRY_CONF_ADD_LEARNER, .data = (uint8_t*)&n4, .data_len = 8 };
    raft_msg_t prop2 = { .type = MSG_PROPOSE, .entries = &e2, .num_entries = 1 };
    raft_step_local(r, &prop2);

    // The core must drop the second config because the first is still pending application
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 2);

    raft_destroy(r);
}

MACRO_TEST(promote_learner_rejected_until_caught_up) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);
    raft_add_learner(r, 3); // Node 3 is a learner at index 0

    // Boot leader and commit 10 dummy entries
    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    for(int i=0; i<10; i++) {
        raft_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"x", .data_len = 1 };
        raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
        raft_step_local(r, &prop);
        raft_msg_t ack = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 2 + i };
        raft_step_remote(r, &ack);
    }
    raft_advance_all_for_tests_only(r);
    MACRO_ASSERT_EQ_INT(raft_commit_index(r), 11);

    // Try to promote Node 3 (whose match_index is 0)
    uint64_t n3 = 3;
    raft_entry_t prom = { .type = ENTRY_CONF_PROMOTE_LEARNER, .data = (uint8_t*)&n3, .data_len = 8 };
    raft_msg_t prop_prom = { .type = MSG_PROPOSE, .entries = &prom, .num_entries = 1 };
    raft_step_local(r, &prop_prom);

    // Proposal must be dropped because learner is mathematically unsafe to promote
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 11);

    raft_destroy(r);
}

MACRO_TEST(entry_conf_add_is_safely_mapped_to_learner) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    uint64_t new_node = 4;
    // We intentionally use the dangerous ENTRY_CONF_ADD type
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_CONF_ADD, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = &e, .num_entries = 1, .commit = 1 };
    raft_step_remote(r, &app);
    raft_advance_all_for_tests_only(r);

    uint64_t active_peers[16]; bool is_learner[16];
    size_t num = raft_peers_ext(r, active_peers, is_learner, 16);

    // Prove that the engine intercepted the dangerous config and safely added it as a LEARNER instead
    // The cluster contains: Self(1), Node(2), Node(3), and the new Learner(4) = 4 total nodes.
    MACRO_ASSERT_EQ_INT(num, 4);
    MACRO_ASSERT_EQ_INT(active_peers[2], 4);
    MACRO_ASSERT_TRUE(is_learner[2]);

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

    // NEW EDGE CASE TESTS
    MACRO_ADD(tests, raft_add_learner_existing_voter_does_not_demote);
    MACRO_ADD(tests, raft_add_learner_self_voter_does_not_demote);
    MACRO_ADD(tests, raft_add_learner_zero_node_id_rejected_or_fatal);
    MACRO_ADD(tests, raft_malformed_config_payload_sets_fatal_error);
    MACRO_ADD(tests, raft_promote_missing_learner_is_noop);
    MACRO_ADD(tests, raft_promote_existing_voter_is_noop);
    MACRO_ADD(tests, raft_promote_learner_resets_recent_active);
    MACRO_ADD(tests, raft_remove_peer_clears_trailing_slot_metadata);
    MACRO_ADD(tests, raft_remove_peer_moves_voted_for_me_slot);
    MACRO_ADD(tests, raft_membership_change_clears_pending_read_index_state);
    MACRO_ADD(tests, raft_remove_self_clears_leader_id);
    MACRO_ADD(tests, raft_remove_self_sets_removed_and_learner_self);
    MACRO_ADD(tests, raft_add_self_after_removed_restores_as_learner_only);

    MACRO_ADD(tests, committed_unapplied_config_blocks_new_config_proposal);
    MACRO_ADD(tests, promote_learner_rejected_until_caught_up);

    MACRO_ADD(tests, entry_conf_add_is_safely_mapped_to_learner);

    macro_run_all("raft_membership", tests, test_count);
    return 0;
}
