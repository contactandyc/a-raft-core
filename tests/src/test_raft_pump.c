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

void raft_node_pump(raft_node_t* node);

#pragma pack(push, 1)
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t term;
    uint64_t voted_for;
    uint64_t commit_index;
    uint64_t last_applied;
    uint64_t snapshot_index;
    uint64_t snapshot_term;
    uint32_t num_peers;
} test_meta_header_t;
#pragma pack(pop)

static void wait_for_pump(uv_loop_t* loop) {
    for (int i = 0; i < 50; i++) {
        uv_run(loop, UV_RUN_NOWAIT);
        usleep(2000);
    }
}

static uint64_t read_disk_term(uint64_t group_id) {
    char meta_path[512];
    snprintf(meta_path, sizeof(meta_path), "/tmp/raft_test_pump/meta_grp%llu.dat", (unsigned long long)group_id);
    FILE* f = fopen(meta_path, "rb");
    if (!f) return 0;
    test_meta_header_t hdr;
    size_t r = fread(&hdr, sizeof(hdr), 1, f);
    fclose(f);
    if (r == 1 && hdr.magic == 0x4D455441) return hdr.term;
    return 0;
}

static uint64_t read_disk_snap_idx(uint64_t group_id) {
    char meta_path[512];
    snprintf(meta_path, sizeof(meta_path), "/tmp/raft_test_pump/meta_grp%llu.dat", (unsigned long long)group_id);
    FILE* f = fopen(meta_path, "rb");
    if (!f) return 0;
    test_meta_header_t hdr;
    size_t r = fread(&hdr, sizeof(hdr), 1, f);
    fclose(f);
    if (r == 1 && hdr.magic == 0x4D455441) return hdr.snapshot_index;
    return 0;
}

// ----------------------------------------------------------------------------
// DISK-BEFORE-NETWORK RIGOROUS TESTS (Issues 2 & 3)
// ----------------------------------------------------------------------------

MACRO_TEST(vote_response_not_sent_before_hardstate_persisted) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_server_connect(&srv, "127.0.0.1", 9000, 2);

    raft_node_t node;
    uint64_t peers[] = {1, 2}; // Issue 1: Full topology
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = 1, .from = 2, .term = 5, .index = 0, .log_term = 0 };
    raft_step_remote(node.core, &req);
    raft_node_pump(&node);

    // Issue 2: Prove network is empty AND disk has not yet updated
    MACRO_ASSERT_EQ_INT(srv.known_peers[0]->out_queue_len, 0);
    MACRO_ASSERT_EQ_INT(read_disk_term(0), 0); // Disk is still stale!

    wait_for_pump(&loop);

    // Issue 2: Prove disk is securely written BEFORE we check network queue
    MACRO_ASSERT_EQ_INT(read_disk_term(0), 5); // Disk is durable!
    MACRO_ASSERT_TRUE(srv.known_peers[0]->out_queue_len > 0);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);
}

MACRO_TEST(snapshot_success_ack_not_sent_until_new_meta_persisted) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_server_connect(&srv, "127.0.0.1", 9000, 2);

    raft_node_t node;
    uint64_t peers[] = {1, 2}; // Issue 1
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                        .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = true };

    raft_step_remote(node.core, &snap);
    raft_node_pump(&node);

    // Issue 3: Prove network is empty AND disk is stale
    MACRO_ASSERT_EQ_INT(srv.known_peers[0]->out_queue_len, 0);
    MACRO_ASSERT_EQ_INT(read_disk_snap_idx(0), 0);

    wait_for_pump(&loop);

    // Issue 3: Prove disk is safely written with snapshot metadata
    MACRO_ASSERT_EQ_INT(read_disk_snap_idx(0), 10);
    MACRO_ASSERT_TRUE(srv.known_peers[0]->out_queue_len > 0);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);
}

// ----------------------------------------------------------------------------
// SNAPSHOT CHUNKING SAFETY TESTS (Issue 4)
// ----------------------------------------------------------------------------

MACRO_TEST(snapshot_chunk_duplicate_offset_is_rejected_or_reacked) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node;
    uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    // 1. Send valid Chunk 1 (Offset 0)
    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap1 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 0, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = false };
    raft_step_remote(node.core, &snap1);

    // FIX: Simulate host consuming Chunk 1 so the core unlocks the next chunk!
    raft_node_pump(&node); wait_for_pump(&loop);

    // 2. Send valid Chunk 2 (Offset 4)
    raft_msg_t snap2 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 4, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = false };
    raft_step_remote(node.core, &snap2);

    // FIX: Simulate host consuming Chunk 2!
    raft_node_pump(&node); wait_for_pump(&loop);

    // 3. Attempt to send Duplicate Chunk 2 (Offset 4) again!
    raft_step_remote(node.core, &snap2);

    raft_ready_t ready = raft_get_ready(node.core);

    // The third packet MUST trigger a rejection hinting the correct expected offset (which is now 8)
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);
    MACRO_ASSERT_EQ_INT(ready.messages[0].conflict_index, 8);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);
}

MACRO_TEST(snapshot_chunk_gap_does_not_append_to_tmp_file) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node;
    uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    // Send valid Chunk 1 (Offset 0, Len 4)
    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap1 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 0, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = false };
    raft_step_remote(node.core, &snap1);
    raft_node_pump(&node); wait_for_pump(&loop);

    // Send invalid Chunk 3 (Offset 8) -- skipping Chunk 2 (Offset 4)
    raft_msg_t snap3 = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                         .snapshot_offset = 8, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = false };
    raft_step_remote(node.core, &snap3);

    raft_ready_t ready = raft_get_ready(node.core);

    // Must cleanly reject and tell leader to resend from Offset 4
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);
    MACRO_ASSERT_EQ_INT(ready.messages[0].conflict_index, 4);
    MACRO_ASSERT_FALSE(ready.install_snapshot); // Do NOT give garbage chunk to disk worker!

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);
}

// ----------------------------------------------------------------------------
// EXISTING ROBUST TESTS
// ----------------------------------------------------------------------------

MACRO_TEST(node_pump_snapshot_install_does_not_consume_stale_ready) {
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_server_connect(&srv, "127.0.0.1", 9000, 2);

    raft_node_t node;
    uint64_t peers[] = {1, 2}; // Issue 1
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 10, .log_term = 2,
                        .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = true };

    raft_step_remote(node.core, &snap);
    raft_node_pump(&node);
    wait_for_pump(&loop);

    MACRO_ASSERT_FALSE(node.fatal_error);
    MACRO_ASSERT_EQ_INT(raft_last_index(node.core), 10);
    MACRO_ASSERT_TRUE(srv.known_peers[0]->out_queue_len > 0);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);
}

MACRO_TEST(snapshot_discarded_suffix_does_not_resurrect_after_restart) {
    // ... [Content remains exactly the same, just update peers[] to {1, 2}] ...
    uv_loop_t loop;
    uv_loop_init(&loop);
    system("rm -rf /tmp/raft_test_pump; mkdir -p /tmp/raft_test_pump");

    raft_server_t srv;
    raft_server_init(&srv, &loop, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node;
    uint64_t peers[] = {1, 2};
    raft_node_init(&node, &srv, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    for (int i=1; i<=5; i++) {
        raft_entry_t e = { .term = 1, .index = i, .type = ENTRY_NORMAL, .data = (uint8_t*)"x", .data_len = 1 };
        raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 1, .index = i-1, .log_term = (i==1?0:1), .entries = &e, .num_entries = 1, .commit = 0 };
        raft_step_remote(node.core, &app);
        raft_node_pump(&node);
        wait_for_pump(&loop);
    }

    uint8_t snap_data[] = "SNAP";
    raft_msg_t snap = { .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2, .term = 2, .index = 3, .log_term = 2, .snapshot_data = snap_data, .snapshot_len = 4, .snapshot_done = true };
    raft_step_remote(node.core, &snap);
    raft_node_pump(&node);
    wait_for_pump(&loop);

    raft_wal_close(&node.wal);
    raft_destroy(node.core);
    uv_loop_close(&loop);

    uv_loop_t loop2;
    uv_loop_init(&loop2);
    raft_server_t srv2;
    raft_server_init(&srv2, &loop2, 1, 1, "/tmp/raft_test_pump");
    raft_node_t node2;
    raft_node_init(&node2, &srv2, 0, peers, 2, NULL, NULL, NULL, NULL, NULL, NULL);

    MACRO_ASSERT_EQ_INT(raft_snapshot_index(node2.core), 3);
    MACRO_ASSERT_EQ_INT(raft_last_index(node2.core), 3);

    raft_wal_close(&node2.wal);
    raft_destroy(node2.core);
    uv_loop_close(&loop2);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, vote_response_not_sent_before_hardstate_persisted);
    MACRO_ADD(tests, snapshot_success_ack_not_sent_until_new_meta_persisted);
    MACRO_ADD(tests, snapshot_chunk_duplicate_offset_is_rejected_or_reacked);
    MACRO_ADD(tests, snapshot_chunk_gap_does_not_append_to_tmp_file);
    MACRO_ADD(tests, node_pump_snapshot_install_does_not_consume_stale_ready);
    MACRO_ADD(tests, snapshot_discarded_suffix_does_not_resurrect_after_restart);

    macro_run_all("raft_pump", tests, test_count);
    return 0;
}
