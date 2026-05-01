// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "a-raft-library/awal.h"
#include "the-macro-library/macro_test.h"

static void cleanup_wal(const char* base_path) {
    char path[512];
    snprintf(path, sizeof(path), "%s.dat", base_path);
    unlink(path);
    snprintf(path, sizeof(path), "%s.index", base_path);
    unlink(path);
}

MACRO_TEST(awal_rejects_oversized_payload) {
    const char* path = "/tmp/awal_test_oversize";
    cleanup_wal(path);

    awal_engine_t wal;
    MACRO_ASSERT_EQ_INT(awal_init(&wal, path), 0);

    // Create a payload that is 1 byte too large
    uint32_t bad_len = AWAL_PAYLOAD_MAX + 1;
    uint8_t* bad_payload = calloc(bad_len, 1);

    int res = awal_append(&wal, 1, 1, 0, bad_payload, bad_len);
    MACRO_ASSERT_EQ_INT(res, -1); // Must reject!

    free(bad_payload);
    awal_close(&wal);
    cleanup_wal(path);
}

MACRO_TEST(awal_truncation_math_correctness) {
    const char* path = "/tmp/awal_test_trunc";
    cleanup_wal(path);

    awal_engine_t wal;
    MACRO_ASSERT_EQ_INT(awal_init(&wal, path), 0);

    // Append 5 entries
    uint8_t data[] = "data";
    for (uint64_t i = 1; i <= 5; i++) {
        awal_append(&wal, 1, i, 0, data, 4);
    }
    awal_flush_batch(&wal);

    MACRO_ASSERT_EQ_INT(wal.max_disk_index, 5);
    MACRO_ASSERT_EQ_INT(wal.ring.disk_tail, 5); // 5 elements written

    // Truncate from index 3 (Removes 3, 4, 5)
    awal_truncate(&wal, 3);

    // Old bug check: max_disk_index should be 2, disk_tail should be 2.
    MACRO_ASSERT_EQ_INT(wal.max_disk_index, 2);
    MACRO_ASSERT_EQ_INT(wal.ring.disk_tail, 2);

    awal_close(&wal);
    cleanup_wal(path);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, awal_rejects_oversized_payload);
    MACRO_ADD(tests, awal_truncation_math_correctness);
    macro_run_all("awal_engine", tests, test_count);
    return 0;
}
