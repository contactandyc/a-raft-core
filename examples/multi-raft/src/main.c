// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <uv.h>
#include "a-raft-library/araft.h"

int main() {
    printf("--- Starting Multi-Raft Single-Connection Demo (Dynamic & HW Optimized) ---\n");

    uv_loop_t* loop = uv_default_loop();

    // 1. Setup Physical Server A (Node 100)
    // Initialized for up to 5,000 Raft groups
    physical_server_t server_a;
    araft_server_init(&server_a, loop, 100, 5000, "meta_server_a.dat");
    araft_server_listen(&server_a, "127.0.0.1", 9001);

    // 2. Setup Physical Server B (Node 200)
    physical_server_t server_b;
    araft_server_init(&server_b, loop, 200, 5000, "meta_server_b.dat");
    araft_server_listen(&server_b, "127.0.0.1", 9002);

    // 3. Connect Server B to Server A
    araft_server_connect(&server_b, "127.0.0.1", 9001);

    // 4. Create Multi-Raft Groups
    araft_node_t shard_1_a, shard_1_b;
    araft_node_t shard_2_a, shard_2_b;

    araft_node_init(&shard_1_a, &server_a, 1, 2);
    araft_node_init(&shard_1_b, &server_b, 1, 2);

    araft_node_init(&shard_2_a, &server_a, 2, 2);
    araft_node_init(&shard_2_b, &server_b, 2, 2);

    // 5. Start the engines
    araft_node_start((araft_node_t*)&shard_1_a.election_timer);
    araft_node_start((araft_node_t*)&shard_1_b.election_timer);
    araft_node_start((araft_node_t*)&shard_2_a.election_timer);
    araft_node_start((araft_node_t*)&shard_2_b.election_timer);

    printf("Engines started. Waiting for elections to trigger...\n");

    return uv_run(loop, UV_RUN_DEFAULT);
}
