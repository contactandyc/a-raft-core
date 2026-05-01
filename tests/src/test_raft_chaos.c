// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "a-raft-library/raft_core.h"
#include "the-macro-library/macro_test.h"

#define NUM_NODES 5
#define MAX_INFLIGHT 10000
#define MAX_TICKS 5000
#define MAX_LOG_SIZE 2000

typedef struct {
    raft_msg_t msg;
    int deliver_at_tick;
    bool active;
} in_flight_msg_t;

typedef struct {
    uint64_t term;
    uint8_t payload[16];
    size_t payload_len;
} applied_entry_t;

typedef struct {
    raft_core_t* nodes[NUM_NODES];
    in_flight_msg_t network[MAX_INFLIGHT];
    int current_tick;

    // THE TRUTH TRACKER
    applied_entry_t state_machines[NUM_NODES][MAX_LOG_SIZE];
    uint64_t commit_indices[NUM_NODES];
    uint64_t leaders_in_term[10000]; // Maps term -> node_id
} chaos_harness_t;

static uint32_t seed = 123456789;
static uint32_t fast_rand() {
    seed ^= seed << 13;
    seed ^= seed >> 17;
    seed ^= seed << 5;
    return seed;
}

static void route_messages(chaos_harness_t* h) {
    for (int i = 0; i < NUM_NODES; i++) {
        // PHASE 4 SAFETY: Track split-brain leaders
        if (raft_core_state(h->nodes[i]) == RAFT_STATE_LEADER) {
            uint64_t term = raft_core_term(h->nodes[i]);
            if (h->leaders_in_term[term] == 0) {
                h->leaders_in_term[term] = i + 1;
            } else {
                // FATAL: Two leaders elected in the exact same term!
                MACRO_ASSERT_EQ_INT(h->leaders_in_term[term], i + 1);
            }
        }

        raft_ready_t ready = raft_core_get_ready(h->nodes[i]);

        // 1. Process Network Traffic
        for (size_t m = 0; m < ready.num_messages; m++) {
            raft_msg_t msg = ready.messages[m];
            if (fast_rand() % 100 < 5) continue; // 5% Packet Drop

            int delay = 1 + (fast_rand() % 10); // 1-10 tick latency
            for (int j = 0; j < MAX_INFLIGHT; j++) {
                if (!h->network[j].active) {
                    h->network[j].msg = msg;
                    h->network[j].deliver_at_tick = h->current_tick + delay;
                    h->network[j].active = true;

                    if (msg.num_entries > 0) {
                        h->network[j].msg.entries = malloc(msg.num_entries * sizeof(raft_entry_t));
                        for (size_t k = 0; k < msg.num_entries; k++) {
                            h->network[j].msg.entries[k] = msg.entries[k];
                            if (msg.entries[k].data_len > 0) {
                                h->network[j].msg.entries[k].data = malloc(msg.entries[k].data_len);
                                memcpy(h->network[j].msg.entries[k].data, msg.entries[k].data, msg.entries[k].data_len);
                            }
                        }
                    }
                    break;
                }
            }
        }

        // 2. Process Committed State
        if (ready.num_committed_entries > 0) {
            for (size_t c = 0; c < ready.num_committed_entries; c++) {
                uint64_t idx = ready.committed_entries[c].index;
                if (idx >= MAX_LOG_SIZE) continue;

                h->state_machines[i][idx].term = ready.committed_entries[c].term;
                h->state_machines[i][idx].payload_len = ready.committed_entries[c].data_len;
                if (ready.committed_entries[c].data_len > 0) {
                    memcpy(h->state_machines[i][idx].payload, ready.committed_entries[c].data, ready.committed_entries[c].data_len);
                }

                // Assert monotonic commits
                MACRO_ASSERT_TRUE(idx > h->commit_indices[i] || h->commit_indices[i] == 0);
                h->commit_indices[i] = idx;
            }
            raft_core_apply(h->nodes[i]);
        }
        raft_core_advance(h->nodes[i]);
    }
}

static void deliver_due_messages(chaos_harness_t* h) {
    for (int j = 0; j < MAX_INFLIGHT; j++) {
        if (h->network[j].active && h->current_tick >= h->network[j].deliver_at_tick) {
            raft_msg_t msg = h->network[j].msg;
            int target_idx = msg.to - 1;
            raft_core_step(h->nodes[target_idx], &msg);

            if (msg.num_entries > 0) {
                for (size_t k = 0; k < msg.num_entries; k++) {
                    if (msg.entries[k].data) free(msg.entries[k].data);
                }
                free(msg.entries);
            }
            h->network[j].active = false;
        }
    }
}

MACRO_TEST(raft_chaos_proves_strict_linearizability) {
    chaos_harness_t h = {0};

    for (int i = 0; i < NUM_NODES; i++) {
        uint64_t node_id = i + 1;
        uint64_t peers[NUM_NODES - 1];
        int p_idx = 0;
        for (int j = 0; j < NUM_NODES; j++) {
            if (j + 1 != node_id) peers[p_idx++] = j + 1;
        }
        h.nodes[i] = raft_core_create(node_id, peers, NUM_NODES - 1);
    }

    // Node 1 triggers election
    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(h.nodes[0], &hup);

    // THE CHAOS LOOP
    for (h.current_tick = 0; h.current_tick < MAX_TICKS; h.current_tick++) {
        route_messages(&h);
        deliver_due_messages(&h);

        if (h.current_tick % 5 == 0) {
            raft_msg_t tick = { .type = MSG_TICK };
            for (int i = 0; i < NUM_NODES; i++) raft_core_step(h.nodes[i], &tick);
        }

        if (fast_rand() % 100 < 10) {
            int target = fast_rand() % NUM_NODES;
            uint8_t payload[4] = "DATA";
            raft_entry_t e = { .type = ENTRY_NORMAL, .data = payload, .data_len = 4 };
            raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
            raft_core_step(h.nodes[target], &p);
        }

        if (fast_rand() % 500 == 0) {
            int target = fast_rand() % NUM_NODES;
            raft_core_step(h.nodes[target], &hup);
        }
    }

    // ========================================================================
    // PHASE 4 ASSERTION: GLOBAL LINEARIZABILITY PROOF
    // ========================================================================
    uint64_t max_commit = 0;
    for (int i = 0; i < NUM_NODES; i++) {
        if (h.commit_indices[i] > max_commit) max_commit = h.commit_indices[i];
    }
    MACRO_ASSERT_TRUE(max_commit > 0);

    printf("[INFO] Highest committed index across the chaotic cluster: %llu\n", max_commit);

    // Iterate through every single index that was committed
    for (uint64_t idx = 1; idx <= max_commit; idx++) {
        uint64_t expected_term = 0;
        uint8_t expected_payload[16] = {0};
        size_t expected_len = 0;
        bool found = false;

        for (int i = 0; i < NUM_NODES; i++) {
            if (h.commit_indices[i] >= idx) {
                // First node found provides the "ground truth" for this index
                if (!found) {
                    expected_term = h.state_machines[i][idx].term;
                    expected_len = h.state_machines[i][idx].payload_len;
                    if (expected_len > 0) memcpy(expected_payload, h.state_machines[i][idx].payload, expected_len);
                    found = true;
                } else {
                    // All other nodes MUST match the ground truth mathematically
                    MACRO_ASSERT_EQ_INT(h.state_machines[i][idx].term, expected_term);
                    MACRO_ASSERT_EQ_INT(h.state_machines[i][idx].payload_len, expected_len);
                    if (expected_len > 0) {
                        MACRO_ASSERT_TRUE(memcmp(h.state_machines[i][idx].payload, expected_payload, expected_len) == 0);
                    }
                }
            }
        }
    }

    for (int i = 0; i < NUM_NODES; i++) raft_core_destroy(h.nodes[i]);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, raft_chaos_proves_strict_linearizability);
    macro_run_all("raft_chaos", tests, test_count);
    return 0;
}
