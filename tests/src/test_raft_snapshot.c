// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <string.h>
#include "a-raft-library/raft.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(raft_fault_duplicate_snapshot_install_is_safe) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    uint8_t snap_data[] = "STATE";
    raft_msg_t snap1 = {
        .type = MSG_INSTALL_SNAPSHOT, .from = 2, .term = 2,
        .index = 100, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 5
    };

    raft_step(r, &snap1);
    raft_ready_t rd1 = raft_get_ready(r);
    MACRO_ASSERT_TRUE(rd1.install_snapshot);
    MACRO_ASSERT_EQ_INT(rd1.snapshot_index, 100);

    raft_snapshot_acked(r, true);
    raft_advance_all(r);

    raft_msg_t snap2 = {
        .type = MSG_INSTALL_SNAPSHOT, .from = 2, .term = 2,
        .index = 100, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 5
    };
    raft_step(r, &snap2);

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

    raft_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };
    raft_step(r, &app);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 1);

    raft_advance_all(r);

    uint8_t snap_data[] = "STATE";
    raft_msg_t snap = {
        .type = MSG_INSTALL_SNAPSHOT, .from = 2, .term = 2,
        .index = 100, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 5
    };
    raft_step(r, &snap);

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
        .type = MSG_INSTALL_SNAPSHOT, .from = 2, .term = 2,
        .index = 100, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 5
    };
    raft_step(r, &snap);

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
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_restore(1, peers, NULL, 2, 2, 0, 99, 99, 99, 2, NULL, 0);

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

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 2, .index = 99, .log_term = 2, .entries = entries, .num_entries = 3, .commit = 99 };
    raft_step(r, &app);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 102);

    uint8_t snap_data[] = "STATE";
    raft_msg_t snap = {
        .type = MSG_INSTALL_SNAPSHOT, .from = 2, .term = 2,
        .index = 100, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 5
    };

    raft_step(r, &snap);
    raft_snapshot_acked(r, true);

    MACRO_ASSERT_EQ_INT(raft_snapshot_index(r), 100);
    MACRO_ASSERT_EQ_INT(raft_last_index(r), 102);

    raft_destroy(r);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, raft_fault_duplicate_snapshot_install_is_safe);
    MACRO_ADD(tests, snapshot_install_failure_leaves_state_intact);
    MACRO_ADD(tests, snapshot_ack_sent_only_after_acked_true);
    MACRO_ADD(tests, snapshot_install_preserves_valid_suffix);
    macro_run_all("raft_snapshot", tests, test_count);
    return 0;
}
