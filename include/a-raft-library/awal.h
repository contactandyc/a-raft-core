// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef AWAL_H
#define AWAL_H

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>

#define AWAL_RING_CAPACITY 1048576
#define AWAL_RING_MASK (AWAL_RING_CAPACITY - 1)
#define AWAL_ENTRY_SIZE 512
#define AWAL_PAYLOAD_MAX 488

typedef struct {
    uint64_t term;
    uint64_t index;
    uint32_t data_len;
    _Atomic uint32_t ready;
    uint8_t  data[AWAL_PAYLOAD_MAX];
} __attribute__((aligned(AWAL_ENTRY_SIZE))) awal_entry_t;

typedef struct {
    __attribute__((aligned(64))) atomic_uint_fast64_t head;
    __attribute__((aligned(64))) uint64_t disk_tail;
    __attribute__((aligned(64))) atomic_uint_fast64_t commit_index;
    __attribute__((aligned(64))) uint64_t demux_tail;

    awal_entry_t* entries;
} awal_ring_t;

typedef struct {
    awal_ring_t ring;

    int dat_fd;
    uint64_t dat_offset;
    uint64_t max_disk_index; // NEW: Expose to reconstruct last_log_index on boot

    int idx_fd;
    uint64_t* index_ram_array;
    uint32_t index_count;

    int read_fd;
    uint64_t demux_expected_raft_index;
    uint64_t demux_file_offset;
    uint8_t* demux_buf;
    uint32_t demux_buf_len;
    uint32_t demux_buf_pos;

    uint64_t ram_head_start_seq;
} awal_engine_t;

int  awal_init(awal_engine_t* engine, const char* base_filepath);
void awal_append(awal_engine_t* engine, uint64_t term, uint64_t index, const uint8_t* payload, uint32_t len);
int  awal_flush_batch(awal_engine_t* engine);
int  awal_demux_step(awal_engine_t* engine, uint64_t* out_term, uint64_t* out_index, uint8_t** out_payload, uint32_t* out_len);

// O(1) Random Access for Leader Catch-Up
int  awal_read_entry(awal_engine_t* engine, uint64_t target_index, uint64_t* out_term, uint8_t** out_payload, uint32_t* out_len);

// NEW: Truncate log on follower conflict
int  awal_truncate(awal_engine_t* engine, uint64_t truncate_from_index);

void awal_close(awal_engine_t* engine);

#endif // AWAL_H