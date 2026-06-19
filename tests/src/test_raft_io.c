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
#include "a-raft-library/raft_wal.h"
#include "the-macro-library/macro_test.h"

static void cleanup_wal_files(const char* base_path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base_path);
    system(cmd);
}

MACRO_TEST(io_save_and_boot) {
    const char* wal_path = "/tmp/raft_test_wal";
    cleanup_wal_files(wal_path);

    uint64_t peers[] = {2, 3};
    bool learners[] = {false, false};
    raft_wal_t wal;

    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, wal_path, 16, 4), 0);

    // Add 0 for applied bounds
    raft_core_t* core = raft_io_boot(&wal, 1, peers, learners, 2, 0, 0, 0, 0, 0, 0);
    MACRO_ASSERT_TRUE(core != NULL);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(core, &hup);

    // PHASE 3: Satisfy the Pre-Vote phase first
    raft_msg_t pv_res = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(core, &pv_res);
    raft_core_advance_all(core);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(core, &vote);

    raft_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"HELLO", .data_len = 5 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(core, &prop);

    raft_ready_t ready = raft_core_get_ready(core);
    MACRO_ASSERT_TRUE(ready.num_entries_to_save > 0);

    raft_io_save(&wal, &ready);
    raft_core_advance_all(core);

    uint64_t saved_term = raft_core_term(core);
    uint64_t saved_last_index = raft_core_last_index(core);
    uint64_t saved_commit = raft_core_commit_index(core);

    raft_core_destroy(core);
    raft_wal_close(&wal);

    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, wal_path, 16, 4), 0);

    raft_core_t* recovered_core = raft_io_boot(&wal, 1, peers, learners, 2, saved_term, 1, saved_commit, 0, 0, 0);

    MACRO_ASSERT_TRUE(recovered_core != NULL);
    MACRO_ASSERT_EQ_INT(raft_core_term(recovered_core), saved_term);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(recovered_core), saved_last_index);

    ready = raft_core_get_ready(recovered_core);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 0);

    raft_core_destroy(recovered_core);
    raft_wal_close(&wal);
    cleanup_wal_files(wal_path);
}

// PHASE 2: Ensure we don't permanently brick a node if we purged historical segments
MACRO_TEST(io_boot_with_purged_wal) {
    const char* wal_path = "/tmp/raft_test_wal_purged";
    cleanup_wal_files(wal_path);

    raft_wal_t wal;
    // 1MB segments force heavy rotation
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, wal_path, 1, 2), 0);

    uint8_t dummy[1024] = {0};
    for (uint64_t i = 1; i <= 2000; i++) {
        raft_wal_append(&wal, 1, i, 0, 0, 0, dummy, 1024);
    }
    raft_wal_flush_batch(&wal);

    // Purge everything up to index 1000
    raft_wal_purge_head(&wal, 1000);
    raft_wal_close(&wal);

    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, wal_path, 1, 2), 0);

    uint64_t peers[] = {2, 3};
    bool learners[] = {false, false};
    raft_core_t* core = raft_io_boot(&wal, 1, peers, learners, 2, 1, 0, 1000, 1000, 0, 0);
    MACRO_ASSERT_TRUE(core != NULL);

    // The WAL safely anchored itself at the first valid, un-purged index!
    MACRO_ASSERT_TRUE(raft_core_last_index(core) == 2000);

    raft_core_destroy(core);
    raft_wal_close(&wal);
    cleanup_wal_files(wal_path);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, io_save_and_boot);
    MACRO_ADD(tests, io_boot_with_purged_wal);
    macro_run_all("raft_io_layer", tests, test_count);
    return 0;
}
