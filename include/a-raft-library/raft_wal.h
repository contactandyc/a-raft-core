// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef RAFT_WAL_H
#define RAFT_WAL_H

#include <stdint.h>
#include <stdbool.h>

#define RAFT_WAL_MAGIC 0x52414654 // "RAFT"
#define RAFT_WAL_SEG_HEADER_SIZE 20
#define RAFT_WAL_FRAME_HEADER_SIZE 25 // 4(crc) + 4(len) + 8(term) + 8(index) + 1(type)

// O(1) Lookup Table Entry
typedef struct {
    uint64_t seg_id;
    uint32_t offset;
} raft_wal_loc_t;

typedef struct {
    char base_dir[512];
    uint64_t segment_size_bytes;

    // Pool Management
    uint64_t current_seg_id;
    uint64_t oldest_seg_id;
    char** standby_paths;
    uint32_t standby_count;
    uint32_t max_standby;

    // Active Write State
    int active_fd;
    uint64_t file_offset;
    uint64_t max_disk_index;

    // Fast Read Cache
    int read_fd;
    uint64_t read_seg_id;

    // O(1) Index Lookup
    raft_wal_loc_t* offsets;
    uint64_t offsets_cap;

    // RAM Batching (Group Commit)
    uint8_t* batch_buf;
    uint32_t batch_len;
    uint32_t batch_cap;

} raft_wal_t;

// Boot the WAL, rebuild the offset map, and detect torn writes
int  raft_wal_init(raft_wal_t* wal, const char* dir, uint64_t segment_size_mb, uint32_t max_standby);

// Buffer a write in RAM (Automatically flushes and rotates files if segment fills)
int  raft_wal_append(raft_wal_t* wal, uint64_t term, uint64_t index, uint8_t type, const uint8_t* payload, uint32_t len);

// Force the OS to write the RAM buffer to the physical NVMe
int  raft_wal_flush_batch(raft_wal_t* wal);

// Read a specific index from disk O(1)
int  raft_wal_read_entry(raft_wal_t* wal, uint64_t target_index, uint64_t* out_term, uint8_t* out_type, uint8_t** out_payload, uint32_t* out_len);

// Slice invalid history off the END of the timeline (Leader Conflict)
int  raft_wal_truncate_tail(raft_wal_t* wal, uint64_t truncate_from_index);

// Recycle old history off the BEGINNING of the timeline (Garbage Collection/Snapshot)
void raft_wal_purge_head(raft_wal_t* wal, uint64_t safe_checkpoint_index);

void raft_wal_close(raft_wal_t* wal);

#endif // RAFT_WAL_H
