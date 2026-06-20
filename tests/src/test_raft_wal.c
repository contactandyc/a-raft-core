// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include "a-raft-library/raft_wal.h"
#include "the-macro-library/macro_test.h"

static void cleanup_wal(const char* base_path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base_path);
    system(cmd);
}

MACRO_TEST(raft_wal_truncation_tail) {
    const char* path = "/tmp/raft_test_wal_tail";
    cleanup_wal(path);

    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, path, 16, 2), 0);

    uint8_t data[] = "data";
    for (uint64_t i = 1; i <= 5; i++) {
        raft_wal_append(&wal, 1, i, 0, 0, 0, data, 4);
    }
    raft_wal_flush_batch(&wal);

    MACRO_ASSERT_EQ_INT(wal.max_disk_index, 5);

    raft_wal_truncate_tail(&wal, 3);
    MACRO_ASSERT_EQ_INT(wal.max_disk_index, 2);

    raft_wal_close(&wal);
    cleanup_wal(path);
}

MACRO_TEST(raft_wal_segment_rotation_and_purge) {
    const char* path = "/tmp/raft_test_wal_rotation";
    cleanup_wal(path);

    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, path, 1, 2), 0);

    uint8_t dummy[80] = {0};
    for (uint64_t i = 1; i <= 20000; i++) {
        raft_wal_append(&wal, 1, i, 0, 0, 0, dummy, 80);
    }
    raft_wal_flush_batch(&wal);

    MACRO_ASSERT_TRUE(wal.current_seg_id > 1);

    raft_wal_purge_head(&wal, 15000);

    MACRO_ASSERT_TRUE(wal.oldest_seg_id > 1);
    MACRO_ASSERT_TRUE(wal.standby_count > 0);

    raft_wal_close(&wal);
    cleanup_wal(path);
}

// PHASE 15: Extreme Disk Corruption Validation
MACRO_TEST(raft_wal_rejects_corrupted_crc) {
    const char* path = "/tmp/raft_test_wal_crc";
    cleanup_wal(path);

    raft_wal_t wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&wal, path, 1, 2), 0);

    uint8_t data[] = "SAFE_DATA";
    raft_wal_append(&wal, 1, 1, 0, 0, 0, data, 9);
    raft_wal_append(&wal, 1, 2, 0, 0, 0, data, 9);
    raft_wal_flush_batch(&wal);
    raft_wal_close(&wal);

    // Simulate bit rot: Open file and scramble a payload byte on index 2
    char seg_path[512];
    snprintf(seg_path, 512, "%s/0000000001.wal", path);
    int fd = open(seg_path, O_RDWR);
    MACRO_ASSERT_TRUE(fd >= 0);
    uint8_t poison = 0xFF;
    pwrite(fd, &poison, 1, 80); // Corrupts index 2 payload
    close(fd);

    raft_wal_t corrupted_wal;
    MACRO_ASSERT_EQ_INT(raft_wal_init(&corrupted_wal, path, 1, 2), 0);

    // Bootloader detects the bad CRC and forcefully truncates the timeline at index 1!
    MACRO_ASSERT_EQ_INT(corrupted_wal.max_disk_index, 1);

    raft_wal_close(&corrupted_wal);
    cleanup_wal(path);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, raft_wal_truncation_tail);
    MACRO_ADD(tests, raft_wal_segment_rotation_and_purge);
    MACRO_ADD(tests, raft_wal_rejects_corrupted_crc);
    macro_run_all("raft_wal_engine", tests, test_count);
    return 0;
}
