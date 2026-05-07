// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>
#include "a-raft-library/raft_server.h"
#include "the-macro-library/macro_test.h"

static int passed = 0;
static int cluster_state = 0;
static uv_timer_t check_timer;
static raft_server_t servers[3];

// FIXED: Dynamically allocate the nodes to prevent libuv memory corruption!
static raft_node_t* nodes[3];

static void close_walk_cb(uv_handle_t* handle, void* arg) {
    (void)arg;
    if (!uv_is_closing(handle)) uv_close(handle, NULL);
}

// The multi-stage cluster verification engine
static void on_test_check(uv_timer_t* handle) {
    (void)handle;

    static int ticks = 0;
    ticks++;
    if (ticks > 100) { // 10 second hard timeout
        uv_stop(uv_default_loop());
        return;
    }

    uint64_t c1 = raft_core_commit_index(nodes[0]->core);
    uint64_t c2 = raft_core_commit_index(nodes[1]->core);
    uint64_t c3 = (cluster_state != 2) ? raft_core_commit_index(nodes[2]->core) : 0;

    raft_node_t* leader = NULL;
    for (int i = 0; i < 3; i++) {
        if (cluster_state != 2 && raft_core_state(nodes[i]->core) == RAFT_STATE_LEADER) {
            leader = nodes[i]; break;
        }
    }

    // STATE 0: Wait for Leader -> Propose Payload 1
    if (cluster_state == 0 && leader) {
        printf("\n[Stage 1] Leader elected! Proposing payload 1...\n");
        raft_node_propose(leader, (const uint8_t*)"PAYLOAD_1", 9);
        cluster_state = 1;
    }
    // STATE 1: Wait for Commit -> Assassinate Node 3 -> Propose Payload 2
    else if (cluster_state == 1) {
        if (c1 >= 2 && c2 >= 2 && c3 >= 2) {
            printf("[Stage 2] Payload 1 committed. Assassinating Node 3...\n");

            // Kill its networking, nuke its RAM core, and close its disk handles
            servers[2].network_isolated = true;

            // FIXED: Cleanly close the active libuv handles before destroying the node!
            uv_close((uv_handle_t*)&nodes[2]->election_timer, NULL);
            uv_close((uv_handle_t*)&nodes[2]->heartbeat_timer, NULL);

            awal_close(&nodes[2]->wal);
            raft_core_destroy(nodes[2]->core);

            // Propose new data while it is dead (Nodes 1 and 2 still hold quorum)
            raft_node_propose(leader, (const uint8_t*)"PAYLOAD_2", 9);
            cluster_state = 2;
        }
    }
    // STATE 2: Wait for Quorum Commit -> Resurrect Node 3
    else if (cluster_state == 2) {
        if (c1 >= 3 && c2 >= 3) {
            printf("[Stage 3] Payload 2 committed by survivors. Resurrecting Node 3 from Disk...\n");

            // FIXED: Allocate a completely fresh block of memory so we don't overwrite
            // the old libuv timers that are still asynchronous-closing in the background!
            nodes[2] = calloc(1, sizeof(raft_node_t));

            uint64_t peers3[] = {1, 2};
            raft_node_init(nodes[2], &servers[2], 0, peers3, 2);
            servers[2].network_isolated = false;

            // Reconnect TCP pipes
            raft_server_connect(&servers[2], "127.0.0.1", 18081, 1);
            raft_server_connect(&servers[2], "127.0.0.1", 18082, 2);
            cluster_state = 3;
        }
    }
    // STATE 3: Verify Seamless Catch-up
    else if (cluster_state == 3) {
        if (c3 >= 3) {
            printf("[Stage 4] Node 3 recovered its log, rejoined the mesh, and synced Payload 2! SUCCESS.\n");
            passed = 1;
            uv_stop(uv_default_loop());
        }
    }
}

MACRO_TEST(cluster_tcp_crash_recovery_and_resync) {
    uv_loop_t* loop = uv_default_loop();

    system("rm -rf /tmp/raft_test_node*");
    system("mkdir -p /tmp/raft_test_node1 /tmp/raft_test_node2 /tmp/raft_test_node3");

    MACRO_ASSERT_EQ_INT(raft_server_init(&servers[0], loop, 1, 1, "/tmp/raft_test_node1"), 0);
    MACRO_ASSERT_EQ_INT(raft_server_init(&servers[1], loop, 2, 1, "/tmp/raft_test_node2"), 0);
    MACRO_ASSERT_EQ_INT(raft_server_init(&servers[2], loop, 3, 1, "/tmp/raft_test_node3"), 0);

    MACRO_ASSERT_EQ_INT(raft_server_listen(&servers[0], "127.0.0.1", 18081), 0);
    MACRO_ASSERT_EQ_INT(raft_server_listen(&servers[1], "127.0.0.1", 18082), 0);
    MACRO_ASSERT_EQ_INT(raft_server_listen(&servers[2], "127.0.0.1", 18083), 0);

    nodes[0] = calloc(1, sizeof(raft_node_t));
    nodes[1] = calloc(1, sizeof(raft_node_t));
    nodes[2] = calloc(1, sizeof(raft_node_t));

    uint64_t peers1[] = {2, 3};
    uint64_t peers2[] = {1, 3};
    uint64_t peers3[] = {1, 2};
    raft_node_init(nodes[0], &servers[0], 0, peers1, 2);
    raft_node_init(nodes[1], &servers[1], 0, peers2, 2);
    raft_node_init(nodes[2], &servers[2], 0, peers3, 2);

    raft_server_connect(&servers[0], "127.0.0.1", 18082, 2);
    raft_server_connect(&servers[0], "127.0.0.1", 18083, 3);
    raft_server_connect(&servers[1], "127.0.0.1", 18083, 3);

    uv_timer_init(loop, &check_timer);
    uv_timer_start(&check_timer, on_test_check, 100, 100);

    uv_run(loop, UV_RUN_DEFAULT);

    uv_walk(loop, close_walk_cb, NULL);
    uv_run(loop, UV_RUN_DEFAULT);

    awal_close(&nodes[0]->wal); awal_close(&nodes[1]->wal); awal_close(&nodes[2]->wal);
    raft_core_destroy(nodes[0]->core); raft_core_destroy(nodes[1]->core); raft_core_destroy(nodes[2]->core);

    free(nodes[0]); free(nodes[1]); free(nodes[2]);

    MACRO_ASSERT_TRUE(passed);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, cluster_tcp_crash_recovery_and_resync);
    macro_run_all("raft_cluster", tests, test_count);
    return 0;
}
