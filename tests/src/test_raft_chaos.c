// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define RAFT_TESTING 1
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "raft_internal.h"
#include "a-raft-library/raft_io.h"
#include "a-raft-library/raft_wal.h"
#include "the-macro-library/macro_test.h"

#define NUM_NODES 5
#define MAX_INFLIGHT 20000
#define MAX_TICKS 10000
#define MAX_LOG_SIZE 10000

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
    raft_t* core;
    raft_wal_t wal;
    bool is_up;
    char wal_path[128];

    uint64_t saved_term;
    uint64_t saved_vote;
    uint64_t saved_commit;
    uint64_t saved_applied;
    uint64_t saved_snap_idx;
    uint64_t saved_snap_term;

    // ReadIndex Tracker
    raft_read_state_t pending_reads[128];
    size_t num_pending_reads;
    uint64_t successful_reads;
    uint64_t max_read_index;

    applied_entry_t state_machine[MAX_LOG_SIZE];
    uint64_t highest_applied;
} chaos_node_t;

typedef struct {
    chaos_node_t nodes[NUM_NODES];
    in_flight_msg_t network[MAX_INFLIGHT];
    int current_tick;
} chaos_harness_t;

static uint32_t seed = 0x1337BEEF;
static uint32_t fast_rand() {
    seed ^= seed << 13;
    seed ^= seed >> 17;
    seed ^= seed << 5;
    return seed;
}

// ----------------------------------------------------------------------------
// NODE LIFECYCLE (CRASH & RESTART)
// ----------------------------------------------------------------------------

static void boot_node(chaos_harness_t* h, int idx) {
    chaos_node_t* n = &h->nodes[idx];

    raft_wal_init(&n->wal, n->wal_path, 16, 4);

    uint64_t peers[NUM_NODES];
    bool learners[NUM_NODES];
    for (int i = 0; i < NUM_NODES; i++) {
        peers[i] = i + 1;
        learners[i] = false;
    }

    n->core = raft_io_boot(&n->wal, idx + 1, peers, learners, NUM_NODES,
                           n->saved_term, n->saved_vote, n->saved_commit, n->saved_applied,
                           n->saved_snap_idx, n->saved_snap_term);

    MACRO_ASSERT_TRUE(n->core != NULL);
    n->is_up = true;
}

static void crash_node(chaos_harness_t* h, int idx) {
    chaos_node_t* n = &h->nodes[idx];
    if (!n->is_up) return;

    n->saved_term = raft_term(n->core);
    n->saved_vote = raft_voted_for(n->core);
    n->saved_commit = raft_commit_index(n->core);
    n->saved_applied = raft_last_applied(n->core);
    n->saved_snap_idx = raft_snapshot_index(n->core);
    n->saved_snap_term = raft_snapshot_term(n->core);

    raft_wal_close(&n->wal);
    raft_destroy(n->core);
    n->core = NULL;
    n->is_up = false;
    n->num_pending_reads = 0; // Wipes pending reads in memory on crash
}

// ----------------------------------------------------------------------------
// THE CHAOS EVENT LOOP
// ----------------------------------------------------------------------------

static void pump_node(chaos_harness_t* h, int idx) {
    chaos_node_t* n = &h->nodes[idx];
    if (!n->is_up || n->core->fatal_error) return;

    raft_ready_t ready = raft_get_ready(n->core);
    uint64_t actual_saved_idx = raft_last_index(n->core) - ready.num_entries_to_save;
    uint64_t actual_applied_idx = n->saved_applied;

    // Buffer new pending reads from the core
    for (size_t i = 0; i < ready.num_read_states; i++) {
        if (n->num_pending_reads < 128) {
            n->pending_reads[n->num_pending_reads++] = ready.read_states[i];
        }
    }

    // 1. Snapshot Installations
    if (ready.install_snapshot) {
        n->saved_snap_idx = ready.snapshot_index;
        n->saved_snap_term = ready.snapshot_term;
        n->saved_applied = ready.snapshot_index;
        n->saved_commit = ready.snapshot_index;

        raft_wal_purge_head(&n->wal, ready.snapshot_index);

        actual_applied_idx = ready.snapshot_index > actual_applied_idx ? ready.snapshot_index : actual_applied_idx;
        actual_saved_idx = ready.snapshot_index > actual_saved_idx ? ready.snapshot_index : actual_saved_idx;

        raft_snapshot_acked(n->core, true);

        uint64_t new_tail = raft_last_index(n->core);
        if (new_tail <= n->wal.max_disk_index) {
            raft_wal_truncate_tail(&n->wal, new_tail + 1);
        }

        if (ready.num_entries_to_save > 0 && ready.entries_to_save) free(ready.entries_to_save);
        if (ready.num_committed_entries > 0 && ready.committed_entries) free(ready.committed_entries);
        ready = raft_get_ready(n->core);
        actual_saved_idx = raft_last_index(n->core) - ready.num_entries_to_save;
    }

    // 2. Disk Persistence
    if (ready.num_entries_to_save > 0) {
        MACRO_ASSERT_TRUE(raft_io_save(&n->wal, &ready));
        actual_saved_idx = raft_last_index(n->core);
    }

    n->saved_term = raft_term(n->core);
    n->saved_vote = raft_voted_for(n->core);

    // 3. Network Routing
    for (size_t m = 0; m < ready.num_messages; m++) {
        raft_msg_t msg = ready.messages[m];

        if (fast_rand() % 100 < 5) continue;

        int delay = 1 + (fast_rand() % 20);

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
                if (msg.snapshot_len > 0) {
                    h->network[j].msg.snapshot_data = malloc(msg.snapshot_len);
                    memcpy(h->network[j].msg.snapshot_data, msg.snapshot_data, msg.snapshot_len);
                }
                break;
            }
        }
    }

    // 4. State Machine Application
    if (ready.num_committed_entries > 0) {
        for (size_t c = 0; c < ready.num_committed_entries; c++) {
            raft_entry_t* e = &ready.committed_entries[c];
            if (e->index <= actual_applied_idx) continue;
            if (e->index >= MAX_LOG_SIZE) continue;

            n->state_machine[e->index].term = e->term;
            n->state_machine[e->index].payload_len = e->data_len;
            if (e->data_len > 0) {
                memcpy(n->state_machine[e->index].payload, e->data, e->data_len);
            }

            actual_applied_idx = e->index;
            if (e->index > n->highest_applied) n->highest_applied = e->index;
        }
    }

    // 5. READ INDEX RESOLUTION (The Oracle Check)
    for (size_t i = 0; i < n->num_pending_reads; ) {
        if (n->pending_reads[i].index <= actual_applied_idx) {
            n->successful_reads++;
            if (n->pending_reads[i].index > n->max_read_index) {
                n->max_read_index = n->pending_reads[i].index;
            }
            // Clear it
            n->num_pending_reads--;
            if (i < n->num_pending_reads) n->pending_reads[i] = n->pending_reads[n->num_pending_reads];
        } else {
            i++;
        }
    }

    raft_advance(n->core, actual_saved_idx, actual_applied_idx);

    if (actual_applied_idx > n->saved_applied) n->saved_applied = actual_applied_idx;
    n->saved_commit = raft_commit_index(n->core);

    if (ready.num_entries_to_save > 0 && ready.entries_to_save) free(ready.entries_to_save);
    if (ready.num_committed_entries > 0 && ready.committed_entries) free(ready.committed_entries);
}

static void deliver_due_messages(chaos_harness_t* h) {
    for (int j = 0; j < MAX_INFLIGHT; j++) {
        if (h->network[j].active && h->current_tick >= h->network[j].deliver_at_tick) {
            raft_msg_t msg = h->network[j].msg;
            int target_idx = msg.to - 1;

            if (h->nodes[target_idx].is_up) {
                raft_step_remote(h->nodes[target_idx].core, &msg);
            }

            if (msg.num_entries > 0) {
                for (size_t k = 0; k < msg.num_entries; k++) if (msg.entries[k].data) free(msg.entries[k].data);
                free(msg.entries);
            }
            if (msg.snapshot_data) free(msg.snapshot_data);
            h->network[j].active = false;
        }
    }
}

// ----------------------------------------------------------------------------
// THE GRAND MATRIX
// ----------------------------------------------------------------------------

MACRO_TEST(raft_grand_chaos_matrix) {
    system("rm -rf /tmp/raft_chaos_nodes");
    system("mkdir -p /tmp/raft_chaos_nodes");

    chaos_harness_t h = {0};

    for (int i = 0; i < NUM_NODES; i++) {
        snprintf(h.nodes[i].wal_path, 128, "/tmp/raft_chaos_nodes/node_%d", i);
        boot_node(&h, i);
    }

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(h.nodes[0].core, &hup);

    for (h.current_tick = 0; h.current_tick < MAX_TICKS; h.current_tick++) {

        deliver_due_messages(&h);
        for (int i = 0; i < NUM_NODES; i++) pump_node(&h, i);

        if (h.current_tick % 10 == 0) {
            raft_msg_t tick = { .type = MSG_TICK };
            for (int i = 0; i < NUM_NODES; i++) if (h.nodes[i].is_up) raft_step_local(h.nodes[i].core, &tick);
        }

        // Propose Data OR ReadIndex
        if (fast_rand() % 100 < 30) {
            int target = fast_rand() % NUM_NODES;
            if (h.nodes[target].is_up && raft_state(h.nodes[target].core) == RAFT_STATE_LEADER) {

                if (fast_rand() % 100 < 20) {
                    // Inject a Linearizable Read Request
                    raft_msg_t p = { .type = MSG_READ_INDEX, .read_seq = h.current_tick };
                    raft_step_local(h.nodes[target].core, &p);
                } else {
                    // Inject a Write Request
                    uint8_t payload[4] = "DATA";
                    raft_entry_t e = { .type = ENTRY_NORMAL, .client_id = 1, .client_seq = h.current_tick, .data = payload, .data_len = 4 };
                    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
                    raft_step_local(h.nodes[target].core, &p);
                }
            }
        }

        // Crash/Reboot
        if (h.current_tick > 100 && fast_rand() % 1000 < 5) {
            int target = fast_rand() % NUM_NODES;
            if (h.nodes[target].is_up) crash_node(&h, target);
            else boot_node(&h, target);
        }

        // Compaction
        if (h.current_tick > 500 && fast_rand() % 1000 < 10) {
            int target = fast_rand() % NUM_NODES;
            if (h.nodes[target].is_up) {
                uint64_t applied = raft_last_applied(h.nodes[target].core);
                if (applied > h.nodes[target].saved_snap_idx + 10) {
                    uint64_t compact_idx = applied - 5;
                    uint64_t term = raft_log_term(h.nodes[target].core, compact_idx);

                    raft_compact_after_snapshot(h.nodes[target].core, compact_idx, term);
                    raft_wal_purge_head(&h.nodes[target].wal, compact_idx);

                    h.nodes[target].saved_snap_idx = compact_idx;
                    h.nodes[target].saved_snap_term = term;
                    h.nodes[target].saved_applied = raft_last_applied(h.nodes[target].core);
                    h.nodes[target].saved_commit = raft_commit_index(h.nodes[target].core);
                }
            }
        }

        // Timeout
        if (fast_rand() % 500 == 0) {
            int target = fast_rand() % NUM_NODES;
            if (h.nodes[target].is_up) raft_step_local(h.nodes[target].core, &hup);
        }
    }

    // Drain
    for (int i = 0; i < NUM_NODES; i++) if (!h.nodes[i].is_up) boot_node(&h, i);
    for (int wait = 0; wait < 1000; wait++) {
        h.current_tick++;
        deliver_due_messages(&h);
        for (int i = 0; i < NUM_NODES; i++) pump_node(&h, i);
        if (wait % 10 == 0) {
            raft_msg_t tick = { .type = MSG_TICK };
            for (int i = 0; i < NUM_NODES; i++) raft_step_local(h.nodes[i].core, &tick);
        }
    }

    // THE ORACLE: Verify Linearizability & Read Bounds
    uint64_t global_max_commit = 0;
    uint64_t total_successful_reads = 0;
    for (int i = 0; i < NUM_NODES; i++) {
        if (h.nodes[i].highest_applied > global_max_commit) global_max_commit = h.nodes[i].highest_applied;
        total_successful_reads += h.nodes[i].successful_reads;

        // ORACLE CHECK 1: Ensure no read returned data from the future or deadlocked
        MACRO_ASSERT_TRUE(h.nodes[i].max_read_index <= global_max_commit);
    }

    printf("[INFO] Highest applied index after chaos: %llu\n", (unsigned long long)global_max_commit);
    printf("[INFO] Total strict ReadIndex requests served under chaos: %llu\n", (unsigned long long)total_successful_reads);

    // Prove liveness: Raft did work and served reads successfully!
    MACRO_ASSERT_TRUE(global_max_commit > 0);
    MACRO_ASSERT_TRUE(total_successful_reads > 0);

    // ORACLE CHECK 2: Strict Write Linearizability
    for (uint64_t idx = 1; idx <= global_max_commit; idx++) {
        uint64_t expected_term = 0;
        uint8_t expected_payload[16] = {0};
        size_t expected_len = 0;
        bool found_truth = false;

        for (int i = 0; i < NUM_NODES; i++) {
            if (h.nodes[i].highest_applied >= idx && h.nodes[i].saved_snap_idx < idx) {
                if (!found_truth) {
                    expected_term = h.nodes[i].state_machine[idx].term;
                    expected_len = h.nodes[i].state_machine[idx].payload_len;
                    if (expected_len > 0) memcpy(expected_payload, h.nodes[i].state_machine[idx].payload, expected_len);
                    found_truth = true;
                } else {
                    MACRO_ASSERT_EQ_INT(h.nodes[i].state_machine[idx].term, expected_term);
                    MACRO_ASSERT_EQ_INT(h.nodes[i].state_machine[idx].payload_len, expected_len);
                    if (expected_len > 0) {
                        MACRO_ASSERT_TRUE(memcmp(h.nodes[i].state_machine[idx].payload, expected_payload, expected_len) == 0);
                    }
                }
            }
        }
    }

    for (int i = 0; i < NUM_NODES; i++) crash_node(&h, i);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, raft_grand_chaos_matrix);
    macro_run_all("raft_chaos", tests, test_count);
    return 0;
}
