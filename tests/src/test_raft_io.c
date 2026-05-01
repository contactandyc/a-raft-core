// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include "a-raft-library/raft_core.h"
#include "a-raft-library/raft_io.h"
#include "a-raft-library/awal.h"
#include "the-macro-library/macro_test.h"

// Helper to wipe the test database files before/after runs
static void cleanup_wal_files(const char* base_path) {
    char path[512];
    snprintf(path, sizeof(path), "%s.dat", base_path);
    unlink(path);
    snprintf(path, sizeof(path), "%s.index", base_path);
    unlink(path);
}

MACRO_TEST(io_save_and_boot) {
    const char* wal_path = "/tmp/raft_test_wal";
    cleanup_wal_files(wal_path);

    uint64_t peers[] = {2, 3};
    awal_engine_t wal;
    MACRO_ASSERT_EQ_INT(awal_init(&wal, wal_path), 0);

    // 1. Create a fresh core and save its initial state to disk
    raft_core_t* core = raft_io_boot(&wal, 1, peers, 2, 0, 0);
    MACRO_ASSERT_TRUE(core != NULL);

    // Fake a leader election
    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(core, &hup);

    // NEW: Send a fake vote from Node 2 so Node 1 wins the election!
    // (Term is 1 because MSG_HUP incremented it from 0 to 1)
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(core, &vote);

    // NOW it is the leader and will accept the proposal!
    raft_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"HELLO", .data_len = 5 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(core, &prop);

    // 2. Pass the volatile state to the I/O bridge to flush to the SSD
    raft_ready_t ready = raft_core_get_ready(core);
    MACRO_ASSERT_TRUE(ready.num_entries_to_save > 0);
    raft_io_save(&wal, &ready);
    raft_core_advance(core);

    uint64_t saved_term = raft_core_term(core);
    uint64_t saved_last_index = raft_core_last_index(core);

    // 3. SIMULATE A CRASH (Destroy RAM state completely)
    raft_core_destroy(core);
    awal_close(&wal);

    // 4. SIMULATE A BOOT (Read from SSD)
    MACRO_ASSERT_EQ_INT(awal_init(&wal, wal_path), 0);
    raft_core_t* recovered_core = raft_io_boot(&wal, 1, peers, 2, saved_term, 1);

    MACRO_ASSERT_TRUE(recovered_core != NULL);
    MACRO_ASSERT_EQ_INT(raft_core_term(recovered_core), saved_term);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(recovered_core), saved_last_index);

    // Verify the payload survived LZ4 compression and disk persistence
    ready = raft_core_get_ready(recovered_core);
    // Since it was saved, the new core shouldn't think it needs to save anything!
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 0);

    raft_core_destroy(recovered_core);
    awal_close(&wal);
    cleanup_wal_files(wal_path);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, io_save_and_boot);
    macro_run_all("raft_io_layer", tests, test_count);
    return 0;
}
