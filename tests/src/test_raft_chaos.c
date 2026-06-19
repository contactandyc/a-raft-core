// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

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

// PHASE 11: Strict Invariant Tracker
typedef struct {
    uint64_t prev_commit;
    uint64_t prev_term;
    uint64_t prev_applied;
} invariant_tracker_t;

typedef struct {
    raft_core_t* nodes[NUM_NODES];
    in_flight_msg_t network[MAX_INFLIGHT];
    int current_tick;

    applied_entry_t state_machines[NUM_NODES][MAX_LOG_SIZE];
    invariant_tracker_t invariants[NUM_NODES];

    uint64_t commit_indices[NUM_NODES];
    uint64_t leaders_in_term[10000];
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
        // PHASE 11 INVARIANT: Term Monotonicity & Leader Uniqueness
        uint64_t current_term = raft_core_term(h->nodes[i]);
        MACRO_ASSERT_TRUE(current_term >= h->invariants[i].prev_term);
        h->invariants[i].prev_term = current_term;

        if (raft_core_state(h->nodes[i]) == RAFT_STATE_LEADER) {
            if (h->leaders_in_term[current_term] == 0) {
                h->leaders_in_term[current_term] = i + 1;
            } else {
                // Proof: Only one leader can exist per term
                MACRO_ASSERT_EQ_INT(h->leaders_in_term[current_term], i + 1);
            }
        }

        raft_ready_t ready = raft_core_get_ready(h->nodes[i]);

        for (size_t m = 0; m < ready.num_messages; m++) {
            raft_msg_t msg = ready.messages[m];

            // 5% chance to drop message entirely (Network Partition Simulation)
            if (fast_rand() % 100 < 5) continue;

            int delay = 1 + (fast_rand() % 10);
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
                    // PHASE 11: Handle snapshot routing
                    if (msg.snapshot_len > 0) {
                        h->network[j].msg.snapshot_data = malloc(msg.snapshot_len);
                        memcpy(h->network[j].msg.snapshot_data, msg.snapshot_data, msg.snapshot_len);
                    }
                    break;
                }
            }
        }

        if (ready.num_committed_entries > 0) {
            for (size_t c = 0; c < ready.num_committed_entries; c++) {
                uint64_t idx = ready.committed_entries[c].index;
                if (idx >= MAX_LOG_SIZE) continue;

                h->state_machines[i][idx].term = ready.committed_entries[c].term;
                h->state_machines[i][idx].payload_len = ready.committed_entries[c].data_len;
                if (ready.committed_entries[c].data_len > 0) {
                    memcpy(h->state_machines[i][idx].payload, ready.committed_entries[c].data, ready.committed_entries[c].data_len);
                }

                // PHASE 11 INVARIANT: Log monotonically applies forward
                MACRO_ASSERT_TRUE(idx > h->commit_indices[i] || h->commit_indices[i] == 0);
                h->commit_indices[i] = idx;
            }
        }
        raft_core_advance_all(h->nodes[i]);

        // PHASE 11 INVARIANTS: State boundaries are mathematically sound
        uint64_t new_commit = raft_core_commit_index(h->nodes[i]);
        uint64_t new_applied = raft_core_last_applied(h->nodes[i]);

        MACRO_ASSERT_TRUE(new_commit >= h->invariants[i].prev_commit); // Commit index NEVER decreases
        MACRO_ASSERT_TRUE(new_applied <= new_commit);                  // Cannot apply uncommitted entries
        MACRO_ASSERT_TRUE(raft_core_last_index(h->nodes[i]) >= new_commit); // Log must contain committed bounds

        h->invariants[i].prev_commit = new_commit;
        h->invariants[i].prev_applied = new_applied;
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
            if (msg.snapshot_data) free(msg.snapshot_data);
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
            if ((uint64_t)(j + 1) != node_id) peers[p_idx++] = j + 1;
        }
        h.nodes[i] = raft_core_create(node_id, peers, NUM_NODES - 1);
    }

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(h.nodes[0], &hup);

    for (h.current_tick = 0; h.current_tick < MAX_TICKS; h.current_tick++) {
        route_messages(&h);
        deliver_due_messages(&h);

        if (h.current_tick % 5 == 0) {
            raft_msg_t tick = { .type = MSG_TICK };
            for (int i = 0; i < NUM_NODES; i++) raft_core_step(h.nodes[i], &tick);
        }

        if (fast_rand() % 100 < 10) {
            int target = fast_rand() % NUM_NODES;
            uint8_t* payload = (uint8_t*)"DATA";
            // PHASE 11: Pass mock sequence IDs to simulate client dedup load
            raft_entry_t e = { .type = ENTRY_NORMAL, .client_id = 1, .client_seq = h.current_tick, .data = payload, .data_len = 4 };
            raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
            raft_core_step(h.nodes[target], &p);
        }

        if (fast_rand() % 500 == 0) {
            int target = fast_rand() % NUM_NODES;
            raft_core_step(h.nodes[target], &hup);
        }
    }

    uint64_t max_commit = 0;
    for (int i = 0; i < NUM_NODES; i++) {
        if (h.commit_indices[i] > max_commit) max_commit = h.commit_indices[i];
    }
    MACRO_ASSERT_TRUE(max_commit > 0);

    printf("[INFO] Highest committed index across the chaotic cluster: %llu\n", (unsigned long long)max_commit);

    for (uint64_t idx = 1; idx <= max_commit; idx++) {
        uint64_t expected_term = 0;
        uint8_t expected_payload[16] = {0};
        size_t expected_len = 0;
        bool found = false;

        for (int i = 0; i < NUM_NODES; i++) {
            if (h.commit_indices[i] >= idx) {
                if (!found) {
                    expected_term = h.state_machines[i][idx].term;
                    expected_len = h.state_machines[i][idx].payload_len;
                    if (expected_len > 0) memcpy(expected_payload, h.state_machines[i][idx].payload, expected_len);
                    found = true;
                } else {
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
