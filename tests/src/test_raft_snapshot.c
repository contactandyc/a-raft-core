// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define RAFT_TESTING 1
#include <stdio.h>
#include <string.h>
#include "raft_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(raft_fault_duplicate_snapshot_install_is_safe) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    uint8_t snap_data[] = "STATE";
    raft_msg_t snap1 = {
        .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2,
        .index = 100, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 5,
        .snapshot_done = true // Explicitly close the chunk stream
    };

    raft_step_remote(r, &snap1);
    raft_ready_t rd1 = raft_get_ready(r);
    MACRO_ASSERT_TRUE(rd1.install_snapshot);
    MACRO_ASSERT_EQ_INT(rd1.snapshot_index, 100);

    raft_snapshot_acked(r, true);
    raft_advance_all_for_tests_only(r);

    raft_msg_t snap2 = {
        .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2,
        .index = 100, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 5,
        .snapshot_done = true
    };
    raft_step_remote(r, &snap2);

    raft_ready_t rd2 = raft_get_ready(r);

    MACRO_ASSERT_FALSE(rd2.install_snapshot);
    MACRO_ASSERT_EQ_INT(raft_snapshot_index(r), 100);
    MACRO_ASSERT_FALSE(rd2.messages[0].reject);
    MACRO_ASSERT_EQ_INT(rd2.messages[0].index, 100);

    raft_destroy(r);
}

MACRO_TEST(snapshot_install_failure_leaves_state_intact) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };
    raft_step_remote(r, &app);
    raft_advance_all_for_tests_only(r);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);

    uint8_t snap_data[] = "STATE";
    raft_msg_t snap = {
        .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2,
        .index = 100, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 5,
        .snapshot_done = true
    };
    raft_step_remote(r, &snap);

    MACRO_ASSERT_EQ_INT(raft_snapshot_index(r), 0);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.install_snapshot);
    MACRO_ASSERT_EQ_INT(ready.snapshot_index, 100);

    raft_snapshot_acked(r, false);

    MACRO_ASSERT_EQ_INT(raft_snapshot_index(r), 0);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);

    ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);
    MACRO_ASSERT_EQ_INT(ready.messages[0].index, 1);

    raft_destroy(r);
}

MACRO_TEST(snapshot_ack_sent_only_after_acked_true) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    uint8_t snap_data[] = "STATE";
    raft_msg_t snap = {
        .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2,
        .index = 100, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 5,
        .snapshot_done = true
    };
    raft_step_remote(r, &snap);

    raft_ready_t ready1 = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready1.install_snapshot);
    MACRO_ASSERT_EQ_INT(ready1.num_messages, 0);

    raft_snapshot_acked(r, true);

    raft_ready_t ready2 = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready2.num_messages, 1);
    MACRO_ASSERT_FALSE(ready2.messages[0].reject);
    MACRO_ASSERT_EQ_INT(ready2.messages[0].index, 100);
    MACRO_ASSERT_EQ_INT(raft_snapshot_index(r), 100);

    raft_destroy(r);
}

MACRO_TEST(snapshot_install_preserves_valid_suffix) {
    uint64_t peers[] = {1, 2, 3};
    raft_t* r = raft_restore(1, peers, NULL, 3, 2, 0, 99, 99, 99, 2, NULL, 0);

    raft_entry_t entries[3];
    for(int i=0; i<3; i++) {
        entries[i].term = 2;
        entries[i].index = 100 + i;
        entries[i].type = ENTRY_NORMAL;
        entries[i].client_id = 0;
        entries[i].client_seq = 0;
        entries[i].data = (uint8_t*)"X";
        entries[i].data_len = 1;
    }

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 2, .index = 99, .log_term = 2, .entries = entries, .num_entries = 3, .commit = 99 };
    raft_step_remote(r, &app);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 102);

    uint8_t snap_data[] = "STATE";
    raft_msg_t snap = {
        .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2,
        .index = 100, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 5,
        .snapshot_done = true
    };

    raft_step_remote(r, &snap);
    raft_snapshot_acked(r, true);

    MACRO_ASSERT_EQ_INT(raft_snapshot_index(r), 100);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 102);

    raft_destroy(r);
}

MACRO_TEST(snapshot_message_contains_confstate) {
    uint64_t peers[] = {1, 2, 3};
    raft_t* r = raft_restore(1, peers, NULL, 3, 1, 0, 100, 100, 100, 1, NULL, 0);

    raft_msg_t hup = { .type = MSG_HUP };

    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 2, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 2, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    raft_msg_t rej = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 2, .reject = true, .index = 101, .conflict_index = 50 };
    raft_step_remote(r, &rej);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_TRUE(ready.num_messages > 0);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_INSTALL_SNAPSHOT);

    MACRO_ASSERT_EQ_INT(ready.messages[0].snapshot_num_peers, 3);
    MACRO_ASSERT_TRUE(ready.messages[0].snapshot_peers != NULL);
    MACRO_ASSERT_EQ_INT(ready.messages[0].snapshot_peers[0], 2);
    MACRO_ASSERT_EQ_INT(ready.messages[0].snapshot_peers[1], 3);
    MACRO_ASSERT_EQ_INT(ready.messages[0].snapshot_peers[2], 1);

    raft_destroy(r);
}

MACRO_TEST(lagging_follower_installs_snapshot_and_gets_updated_membership) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    uint64_t snapshot_peers[] = {1, 2, 3, 4};
    bool snapshot_learners[] = {false, false, false, true};

    uint8_t snap_data[] = "STATE";
    raft_msg_t snap = {
        .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2,
        .index = 100, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 5,
        .snapshot_peers = snapshot_peers, .snapshot_is_learner = snapshot_learners, .snapshot_num_peers = 4,
        .snapshot_done = true
    };

    raft_step_remote(r, &snap);

    raft_snapshot_acked(r, true);
    raft_advance_all_for_tests_only(r);

    uint64_t active_peers[16]; bool is_learner[16];
    size_t num = raft_peers_ext(r, active_peers, is_learner, 16);

    MACRO_ASSERT_EQ_INT(num, 4);

    bool found_node_4 = false;
    for (size_t i = 0; i < num; i++) {
        if (active_peers[i] == 4) {
            found_node_4 = true;
            MACRO_ASSERT_TRUE(is_learner[i]);
        }
    }
    MACRO_ASSERT_TRUE(found_node_4);

    raft_destroy(r);
}

MACRO_TEST(snapshot_below_last_applied_is_ignored) {
    uint64_t peers[] = {1, 2, 3};
    // Boot a follower already safely compacted and applied up to index 100
    raft_t* r = raft_restore(1, peers, NULL, 3, 2, 0, 100, 100, 100, 2, NULL, 0);

    MACRO_ASSERT_EQ_INT(raft_last_applied(r), 100);

    // Simulate leader sending a stale snapshot at index 80
    uint8_t snap_data[] = "OLD_STATE";
    raft_msg_t snap = {
        .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2,
        .index = 80, .log_term = 1, .snapshot_data = snap_data, .snapshot_len = 9,
        .snapshot_done = true
    };
    raft_step_remote(r, &snap);

    // The core MUST ignore the stale snapshot, but importantly, MUST NOT reject it.
    // Furthermore, it should fast-forward the leader by responding with its actual last_applied (100)!
    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_FALSE(ready.install_snapshot);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_FALSE(ready.messages[0].reject); // Acknowledge without applying!
    MACRO_ASSERT_TRUE(ready.messages[0].snapshot_done);
    MACRO_ASSERT_EQ_INT(ready.messages[0].index, 100); // FIX: Expect 100, not 80

    raft_destroy(r);
}

MACRO_TEST(leader_snapshot_message_uses_snapshot_persisted_confstate_not_live_topology) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    // Become Leader
    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    // Apply 10 dummy entries
    for (int i=0; i<10; i++) {
        raft_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
        raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
        raft_step_local(r, &p);
        raft_msg_t ack = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = false, .index = 2+i };
        raft_step_remote(r, &ack);
    }
    raft_advance_all_for_tests_only(r);

    // Take a snapshot at index 11 (Topology is {1, 2, 3})
    raft_compact_after_snapshot(r, 11, 1);

    // Add Node 4 to the LIVE topology
    raft_add_learner(r, 4);
    raft_promote_learner(r, 4);

    // Force the leader to send a snapshot to a lagging follower (Node 3 is stuck at 0)
    raft_msg_t req = { .type = MSG_APPEND_RES, .to = 1, .from = 3, .term = 1, .reject = true, .index = 0, .conflict_index = 0 };
    raft_step_remote(r, &req);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_INSTALL_SNAPSHOT);

    // ASSERTION: The message MUST contain the frozen topology {1, 2, 3}, NOT the live {1, 2, 3, 4}
    MACRO_ASSERT_EQ_INT(ready.messages[0].snapshot_num_peers, 3);

    bool found_4 = false;
    for (size_t i = 0; i < ready.messages[0].snapshot_num_peers; i++) {
        if (ready.messages[0].snapshot_peers[i] == 4) found_4 = true;
    }
    MACRO_ASSERT_FALSE(found_4);

    raft_destroy(r);
}

MACRO_TEST(snapshot_next_chunk_before_ack_is_rejected) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    // Send chunk 0
    uint8_t snap_data[] = "CHUNK1";
    raft_msg_t snap1 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 0, .snapshot_data = snap_data, .snapshot_len = 6, .snapshot_done = false };
    raft_step_remote(r, &snap1);

    // Attempt to push chunk 2 BEFORE clearing ready/acking chunk 1
    uint8_t snap_data2[] = "CHUNK2";
    raft_msg_t snap2 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 6, .snapshot_data = snap_data2, .snapshot_len = 6, .snapshot_done = false };
    raft_step_remote(r, &snap2);

    raft_ready_t ready = raft_get_ready(r);

    // Must reject the second chunk with conflict_index = 0
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);
    MACRO_ASSERT_EQ_INT(ready.messages[0].conflict_index, 0);

    raft_destroy(r);
}

MACRO_TEST(snapshot_chunk_malloc_failure_does_not_advance_expected_offset) {
    uint64_t peers[] = {2};
    raft_t* r = raft_create(1, peers, 1);

    // Create an impossibly large payload that will guarantee a malloc failure
    raft_msg_t snap1 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 0, .snapshot_data = (uint8_t*)"X", .snapshot_len = 0xFFFFFFFFFFFFFFFF, .snapshot_done = false };

    raft_step_remote(r, &snap1);

    // Core must hit fatal_error immediately upon allocation failure,
    // and critically, the offset must remain safely at 0.
    MACRO_ASSERT_TRUE(raft_has_fatal_error(r));
    MACRO_ASSERT_EQ_INT(r->expected_snapshot_offset, 0);

    raft_destroy(r);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, raft_fault_duplicate_snapshot_install_is_safe);
    MACRO_ADD(tests, snapshot_install_failure_leaves_state_intact);
    MACRO_ADD(tests, snapshot_ack_sent_only_after_acked_true);
    MACRO_ADD(tests, snapshot_install_preserves_valid_suffix);
    MACRO_ADD(tests, snapshot_message_contains_confstate);
    MACRO_ADD(tests, lagging_follower_installs_snapshot_and_gets_updated_membership);
    MACRO_ADD(tests, snapshot_below_last_applied_is_ignored);
    MACRO_ADD(tests, leader_snapshot_message_uses_snapshot_persisted_confstate_not_live_topology);
    MACRO_ADD(tests, snapshot_next_chunk_before_ack_is_rejected);
    MACRO_ADD(tests, snapshot_chunk_malloc_failure_does_not_advance_expected_offset);

    macro_run_all("raft_snapshot", tests, test_count);
    return 0;
}
