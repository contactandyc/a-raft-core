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
#include "a-raft-library/raft_wal.h" // <-- CHANGED
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
    raft_wal_t wal; // <-- CHANGED

    // Boot the WAL with 16MB segments and 4 standby files
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, wal_path, 16, 4), 0); // <-- CHANGED

    raft_core_t* core = raft_io_boot(&wal, 1, peers, 2, 0, 0, 0);
    MACRO_ASSERT_TRUE(core != NULL);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(core, &hup);

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
    raft_wal_close(&wal); // <-- CHANGED

    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, wal_path, 16, 4), 0); // <-- CHANGED

    raft_core_t* recovered_core = raft_io_boot(&wal, 1, peers, 2, saved_term, 1, saved_commit);

    MACRO_ASSERT_TRUE(recovered_core != NULL);
    MACRO_ASSERT_EQ_INT(raft_core_term(recovered_core), saved_term);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(recovered_core), saved_last_index);

    ready = raft_core_get_ready(recovered_core);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 0);

    raft_core_destroy(recovered_core);
    raft_wal_close(&wal); // <-- CHANGED
    cleanup_wal_files(wal_path);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, io_save_and_boot);
    macro_run_all("raft_io_layer", tests, test_count);
    return 0;
}
