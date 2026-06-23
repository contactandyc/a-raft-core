// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <uv.h>
#include "a-raft-library/raft_server.h"
#include "raft_internal.h"
#include "the-macro-library/macro_test.h"

// Helper to process libuv background threads synchronously in our tests
static void wait_for_pump(uv_loop_t* loop) {
    // Run the loop until the async worker thread completes and pump_after_work fires
    for (int i = 0; i < 50; i++) {
        uv_run(loop, UV_RUN_NOWAIT);
        usleep(2000); // 2ms
    }
}

// ----------------------------------------------------------------------------
// REVIEWER TEST 10b: Disk-Before-Network Append Safety
// ----------------------------------------------------------------------------
MACRO_TEST(append_success_not_sent_before_wal_flush) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("rm -rf /tmp/raft_test_pump");
    system("mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    // Connect a dummy peer so we can trap outgoing network packets
    raft_server_connect(&srv, "127.0.0.1", 9000, 2);

    raft_node_t node;
    uint64_t peers[] = {2};
    raft_node_init(&node, &srv, 0, peers, 1, NULL, NULL, NULL, NULL, NULL, NULL);

    // Leader (Node 2) sends an AppendEntries
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };

    raft_step_remote(node.core, &app);
    raft_node_pump(&node);

    // ASSERTION: Before the async disk flush finishes, the outbound network queue MUST be empty.
    MACRO_ASSERT_EQ_INT(srv.known_peers[0]->out_queue_len, 0);

    // Let the libuv thread pool finish fsyncing to disk
    wait_for_pump(&loop);

    // ASSERTION: Only after the disk completes should the AppendResponse be dispatched.
    MACRO_ASSERT_TRUE(srv.known_peers[0]->out_queue_len > 0);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);
}

// ----------------------------------------------------------------------------
// REVIEWER TEST 10a: Disk-Before-Network Vote Safety
// ----------------------------------------------------------------------------
MACRO_TEST(vote_response_not_sent_before_hardstate_persisted) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("rm -rf /tmp/raft_test_pump");
    system("mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_server_connect(&srv, "127.0.0.1", 9000, 2);

    raft_node_t node;
    uint64_t peers[] = {2};
    raft_node_init(&node, &srv, 0, peers, 1, NULL, NULL, NULL, NULL, NULL, NULL);

    // Node 2 requests a vote
    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = 1, .from = 2, .term = 1, .index = 0, .log_term = 0 };
    raft_step_remote(node.core, &req);
    raft_node_pump(&node);

    // Must not send vote until meta_grp.dat is safely on disk
    MACRO_ASSERT_EQ_INT(srv.known_peers[0]->out_queue_len, 0);

    wait_for_pump(&loop);

    // Vote response dispatched
    MACRO_ASSERT_TRUE(srv.known_peers[0]->out_queue_len > 0);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);
}

// ----------------------------------------------------------------------------
// REVIEWER TEST 1: Stale Ready Struct Discarded Safely
// ----------------------------------------------------------------------------
MACRO_TEST(node_pump_snapshot_install_does_not_consume_stale_ready) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("rm -rf /tmp/raft_test_pump");
    system("mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_server_connect(&srv, "127.0.0.1", 9000, 2);

    raft_node_t node;
    uint64_t peers[] = {2};
    raft_node_init(&node, &srv, 0, peers, 1, NULL, NULL, NULL, NULL, NULL, NULL);

    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                        .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = true };

    raft_step_remote(node.core, &snap);
    raft_node_pump(&node);
    wait_for_pump(&loop);

    MACRO_ASSERT_FALSE(node.fatal_error);
    MACRO_ASSERT_EQ_INT(raft_last_index(node.core), 10);

    // Proof the snapshot ACK was successfully transmitted and not lost by raft_advance!
    MACRO_ASSERT_TRUE(srv.known_peers[0]->out_queue_len > 0);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);
}

// ----------------------------------------------------------------------------
// REVIEWER TEST 4: WAL Tail Truncation on Conflicting Suffix
// ----------------------------------------------------------------------------
MACRO_TEST(snapshot_discarded_suffix_does_not_resurrect_after_restart) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("rm -rf /tmp/raft_test_pump");
    system("mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node;
    uint64_t peers[] = {2};
    raft_node_init(&node, &srv, 0, peers, 1, NULL, NULL, NULL, NULL, NULL, NULL);

    // 1. Append entries 1 through 5 to the WAL
    for (int i=1; i<=5; i++) {
        raft_entry_t e = { .term = 1, .index = i, .type = ENTRY_NORMAL, .data = (uint8_t*)"x", .data_len = 1 };
        raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = i-1, .log_term = (i==1?0:1), .entries = &e, .num_entries = 1, .commit = 0 };
        raft_step_remote(node.core, &app);
        raft_node_pump(&node);
        wait_for_pump(&loop);
    }
    MACRO_ASSERT_EQ_INT(raft_last_index(node.core), 5);

    // 2. Install snapshot at index 3 with a higher term. This logically discards suffix 4..5.
    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 3, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = true };
    raft_step_remote(node.core, &snap);
    raft_node_pump(&node);
    wait_for_pump(&loop);

    MACRO_ASSERT_EQ_INT(raft_snapshot_index(node.core), 3);
    MACRO_ASSERT_EQ_INT(raft_last_index(node.core), 3);

    // Shut down completely
    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);

    // 3. Restart the node from disk
    uv_loop_t loop2;
    uv_loop_init(&loop2);
    raft_server_t srv2;
    raft_server_init(&srv2, &loop2, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node2;
    raft_node_init(&node2, &srv2, 0, peers, 1, NULL, NULL, NULL, NULL, NULL, NULL);

    // 4. Verify suffix 4..5 did not resurrect from the dead WAL!
    MACRO_ASSERT_EQ_INT(raft_snapshot_index(node2.core), 3);
    MACRO_ASSERT_EQ_INT(raft_last_index(node2.core), 3);

    raft_wal_close(&node2.wal);
    raft_destroy(node2.core);
    uv_loop_close(&loop2);
}

// ----------------------------------------------------------------------------
// REVIEWER TEST 2: Snapshot ConfState Survives Restarts
// ----------------------------------------------------------------------------
MACRO_TEST(snapshot_confstate_survives_node_restart) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("rm -rf /tmp/raft_test_pump");
    system("mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node;
    uint64_t peers[] = {2}; // Initial cluster is {1, 2}
    raft_node_init(&node, &srv, 0, peers, 1, NULL, NULL, NULL, NULL, NULL, NULL);

    // The snapshot dictates the cluster is actually {1, 2, 3, 4(learner)}
    uint64_t snap_peers[] = {1, 2, 3, 4};
    bool snap_lrn[] = {false, false, false, true};

    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                        .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = true,
                        .snapshot_peers = snap_peers, .snapshot_is_learner = snap_lrn, .snapshot_num_peers = 4 };

    raft_step_remote(node.core, &snap);
    raft_node_pump(&node);
    wait_for_pump(&loop);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);

    // Restart the node
    uv_loop_t loop2;
    uv_loop_init(&loop2);
    raft_server_t srv2;
    raft_server_init(&srv2, &loop2, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node2;
    raft_node_init(&node2, &srv2, 0, peers, 1, NULL, NULL, NULL, NULL, NULL, NULL);

    uint64_t act_peers[16]; bool is_lrn[16];
    size_t num = raft_peers_ext(node2.core, act_peers, is_lrn, 16);

    // Validate the snapshot's topology survived the reboot
    MACRO_ASSERT_EQ_INT(num, 4);

    bool found_4 = false;
    for(size_t i=0; i<num; i++) {
        if (act_peers[i] == 4) {
            found_4 = true;
            MACRO_ASSERT_TRUE(is_lrn[i]); // Must remain a learner!
        }
    }
    MACRO_ASSERT_TRUE(found_4);

    raft_wal_close(&node2.wal);
    raft_destroy(node2.core);
    uv_loop_close(&loop2);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, append_success_not_sent_before_wal_flush);
    MACRO_ADD(tests, vote_response_not_sent_before_hardstate_persisted);
    MACRO_ADD(tests, node_pump_snapshot_install_does_not_consume_stale_ready);
    MACRO_ADD(tests, snapshot_discarded_suffix_does_not_resurrect_after_restart);
    MACRO_ADD(tests, snapshot_confstate_survives_node_restart);

    macro_run_all("raft_pump", tests, test_count);
    return 0;
}
