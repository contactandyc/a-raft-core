// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define _GNU_SOURCE
#include "a-raft-library/awal.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#ifdef __APPLE__
#include <sys/types.h>
#endif

extern int LZ4_compress_default(const char* src, char* dst, int srcSize, int dstCapacity);
extern int LZ4_decompress_safe(const char* src, char* dst, int compressedSize, int dstCapacity);
extern int LZ4_compressBound(int inputSize);

int awal_init(awal_engine_t* engine, const char* base_filepath) {
    memset(engine, 0, sizeof(awal_engine_t));

    size_t ring_bytes = AWAL_RING_CAPACITY * sizeof(awal_entry_t);
    if (posix_memalign((void**)&engine->ring.entries, 4096, ring_bytes) != 0) return -1;
    memset(engine->ring.entries, 0, ring_bytes);

    char dat_path[512], idx_path[512];
    snprintf(dat_path, sizeof(dat_path), "%s.dat", base_filepath);
    snprintf(idx_path, sizeof(idx_path), "%s.index", base_filepath);

    // FIXED: Open/Create dat_fd FIRST before read_fd attempts to open it. Dropped O_DIRECT.
#ifdef __linux__
    engine->dat_fd = open(dat_path, O_RDWR | O_CREAT, 0644);
#elif defined(__APPLE__)
    engine->dat_fd = open(dat_path, O_RDWR | O_CREAT, 0644);
    fcntl(engine->dat_fd, F_NOCACHE, 1);
#endif

    engine->read_fd = open(dat_path, O_RDONLY);
    engine->idx_fd = open(idx_path, O_RDWR | O_CREAT, 0644);

    if (engine->dat_fd < 0 || engine->idx_fd < 0 || engine->read_fd < 0) return -1;

    size_t map_size = 1024 * 1024;
    off_t current_size = lseek(engine->idx_fd, 0, SEEK_END);

    if (current_size < (off_t)map_size) {
#ifdef __APPLE__
        fstore_t store = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, map_size};
        fcntl(engine->idx_fd, F_PREALLOCATE, &store);
        ftruncate(engine->idx_fd, map_size);
#else
        posix_fallocate(engine->idx_fd, 0, map_size);
#endif
    }

    // FIXED: Removed duplicate mmap memory leak
    engine->index_ram_array = (uint64_t*)mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, engine->idx_fd, 0);
    if (engine->index_ram_array == MAP_FAILED) return -1;

    engine->index_count = 0;
    while (engine->index_count < (map_size / 16)) {
        if (engine->index_ram_array[engine->index_count * 2] == 0) break;
        engine->index_count++;
    }

    uint64_t catchup_offset = 0;
    if (engine->index_count > 0) {
        catchup_offset = engine->index_ram_array[(engine->index_count - 1) * 2 + 1];
    }

    engine->dat_offset = catchup_offset;
    uint32_t block_header[2];
    uint64_t max_index_found = 0;

    // Recover Disk State
    while (pread(engine->dat_fd, block_header, 8, engine->dat_offset) == 8) {
        uint32_t uncomp_len = block_header[0];
        uint32_t comp_len = block_header[1];

        // FIXED: Corruption Check to prevent massive mallocs
        if (uncomp_len > 1024 * 1024 * 50 || comp_len > 1024 * 1024 * 10) break;

        uint8_t* comp_buf = malloc(comp_len);
        pread(engine->dat_fd, comp_buf, comp_len, engine->dat_offset + 8);

        uint8_t* uncomp_buf = malloc(uncomp_len);
        if (LZ4_decompress_safe((const char*)comp_buf, (char*)uncomp_buf, comp_len, uncomp_len) >= 0) {
            uint32_t pos = 0;
            while (pos < uncomp_len) {
                uint64_t idx;
                uint32_t len;
                memcpy(&idx, uncomp_buf + pos + 8, 8);
                memcpy(&len, uncomp_buf + pos + 16, 4);
                pos += 20 + len;

                max_index_found = idx;
                engine->ring.disk_tail++;
                engine->ring.head++;
            }
        }
        free(comp_buf);
        free(uncomp_buf);
        engine->dat_offset += 8 + comp_len;
    }

    engine->max_disk_index = max_index_found;
    engine->ring.commit_index = engine->ring.disk_tail;
    engine->ram_head_start_seq = engine->ring.disk_tail;
    engine->ring.demux_tail = 0;

    // FIXED: Properly set demux expectations for State Machine
    engine->demux_expected_raft_index = max_index_found > 0 ? max_index_found + 1 : 1;

    return 0;
}

void awal_append(awal_engine_t* engine, uint64_t term, uint64_t index, const uint8_t* payload, uint32_t len) {
    if (len > AWAL_PAYLOAD_MAX) len = AWAL_PAYLOAD_MAX;

    uint64_t my_seq = atomic_fetch_add_explicit(&engine->ring.head, 1, memory_order_relaxed);
    awal_entry_t* slot = &engine->ring.entries[my_seq & AWAL_RING_MASK];

    slot->term = term;
    slot->index = index;
    slot->data_len = len;
    if (len > 0) memcpy(slot->data, payload, len);

    // FIXED: Ready is the target index. Acts as safety lock.
    atomic_store_explicit(&slot->ready, index, memory_order_release);
}

int awal_flush_batch(awal_engine_t* engine) {
    uint64_t current_tail = engine->ring.disk_tail;
    uint64_t head = atomic_load_explicit(&engine->ring.head, memory_order_acquire);
    uint64_t batch_size = 0;

    while (batch_size < AWAL_RING_CAPACITY) {
        uint64_t target_seq = current_tail + batch_size;
        if (target_seq >= head) break; // Don't flush un-appended items

        awal_entry_t* slot = &engine->ring.entries[target_seq & AWAL_RING_MASK];
        if (atomic_load_explicit(&slot->ready, memory_order_acquire) != slot->index) break; // Incomplete write

        batch_size++;
    }

    if (batch_size == 0) return 0;

    uint8_t* uncomp_buf = malloc(batch_size * AWAL_ENTRY_SIZE);
    uint32_t uncomp_len = 0;
    uint64_t first_index = 0;
    uint64_t last_index = 0;

    for (uint64_t i = 0; i < batch_size; i++) {
        awal_entry_t* slot = &engine->ring.entries[(current_tail + i) & AWAL_RING_MASK];
        if (i == 0) first_index = slot->index;
        last_index = slot->index;

        memcpy(uncomp_buf + uncomp_len, &slot->term, 8); uncomp_len += 8;
        memcpy(uncomp_buf + uncomp_len, &slot->index, 8); uncomp_len += 8;
        memcpy(uncomp_buf + uncomp_len, &slot->data_len, 4); uncomp_len += 4;

        if (slot->data_len > 0) {
            memcpy(uncomp_buf + uncomp_len, slot->data, slot->data_len);
            uncomp_len += slot->data_len;
        }
    }

    uint32_t max_comp_len = LZ4_compressBound(uncomp_len);
    uint8_t* comp_buf = malloc(max_comp_len);
    int comp_len = LZ4_compress_default((const char*)uncomp_buf, (char*)comp_buf, uncomp_len, max_comp_len);

    uint32_t header[2] = { uncomp_len, (uint32_t)comp_len };
    pwrite(engine->dat_fd, header, 8, engine->dat_offset);
    pwrite(engine->dat_fd, comp_buf, comp_len, engine->dat_offset + 8);

#ifdef __APPLE__
    fcntl(engine->dat_fd, F_FULLFSYNC, 0);
#else
    fdatasync(engine->dat_fd);
#endif

    uint32_t c = engine->index_count;
    engine->index_ram_array[c * 2] = first_index;
    engine->index_ram_array[(c * 2) + 1] = engine->dat_offset;
    engine->index_count++;
    msync(engine->index_ram_array, 1024 * 1024, MS_SYNC); // FIXED: Force Index persistence

    engine->dat_offset += 8 + comp_len;
    engine->max_disk_index = last_index;
    free(uncomp_buf);
    free(comp_buf);

    engine->ring.disk_tail += batch_size;
    atomic_store_explicit(&engine->ring.commit_index, engine->ring.disk_tail, memory_order_release);

    return batch_size;
}

// NEW: Follower Log Truncation to enforce Safety properties
int awal_truncate(awal_engine_t* engine, uint64_t truncate_from_index) {
    uint64_t head = atomic_load(&engine->ring.head);
    uint64_t tail = engine->ring.disk_tail;

    // 1. Truncate RAM ring (Unflushed Entries)
    uint64_t new_head = head;
    for (uint64_t seq = tail; seq < head; seq++) {
        awal_entry_t* slot = &engine->ring.entries[seq & AWAL_RING_MASK];
        if (slot->index >= truncate_from_index) {
            new_head = seq;
            break;
        }
    }
    atomic_store(&engine->ring.head, new_head);

    // 2. Truncate Disk (Flushed Entries)
    if (truncate_from_index <= engine->max_disk_index) {
        int64_t block_idx = -1;
        for (uint32_t i = 0; i < engine->index_count; i++) {
            if (engine->index_ram_array[i * 2] >= truncate_from_index) break;
            block_idx = i;
        }

        if (block_idx >= 0) {
            uint64_t offset = engine->index_ram_array[(block_idx * 2) + 1];
            uint32_t header[2];
            pread(engine->dat_fd, header, 8, offset);

            uint8_t* comp_buf = malloc(header[1]);
            pread(engine->dat_fd, comp_buf, header[1], offset + 8);
            uint8_t* uncomp_buf = malloc(header[0]);
            LZ4_decompress_safe((const char*)comp_buf, (char*)uncomp_buf, header[1], header[0]);

            uint32_t pos = 0;
            uint32_t keep_len = 0;
            uint64_t last_valid_idx = 0;

            // Salvage entries prior to the truncate index
            while (pos < header[0]) {
                uint64_t idx;
                uint32_t len;
                memcpy(&idx, uncomp_buf + pos + 8, 8);
                memcpy(&len, uncomp_buf + pos + 16, 4);
                if (idx >= truncate_from_index) break;
                pos += 20 + len;
                keep_len = pos;
                last_valid_idx = idx;
            }

            if (keep_len > 0) {
                uint32_t max_comp_len = LZ4_compressBound(keep_len);
                uint8_t* new_comp_buf = malloc(max_comp_len);
                int new_comp_len = LZ4_compress_default((const char*)uncomp_buf, (char*)new_comp_buf, keep_len, max_comp_len);

                uint32_t new_header[2] = { keep_len, (uint32_t)new_comp_len };
                pwrite(engine->dat_fd, new_header, 8, offset);
                pwrite(engine->dat_fd, new_comp_buf, new_comp_len, offset + 8);
                free(new_comp_buf);

                engine->dat_offset = offset + 8 + new_comp_len;
                engine->index_count = block_idx + 1;
                engine->max_disk_index = last_valid_idx;
                engine->ring.disk_tail -= (engine->max_disk_index - last_valid_idx); // Adjust tail offset conceptually
            } else {
                engine->dat_offset = offset;
                engine->index_count = block_idx;
                engine->max_disk_index = (block_idx > 0) ? truncate_from_index - 1 : 0;
            }

            free(comp_buf);
            free(uncomp_buf);
        } else {
            // Complete Disk Wipe
            engine->dat_offset = 0;
            engine->index_count = 0;
            engine->max_disk_index = 0;
            engine->ring.disk_tail = 0;
            engine->ring.commit_index = 0;
        }

        ftruncate(engine->dat_fd, engine->dat_offset);
        memset(engine->index_ram_array + (engine->index_count * 2), 0, (1024 * 1024) - (engine->index_count * 16));
        msync(engine->index_ram_array, 1024 * 1024, MS_SYNC);
    }
    return 0;
}

static uint64_t find_offset_in_index(awal_engine_t* engine, uint64_t target_index) {
    if (engine->index_count == 0) return 0;

    int64_t low = 0;
    int64_t high = engine->index_count - 1;
    uint64_t best_offset = 0;

    while (low <= high) {
        int64_t mid = low + (high - low) / 2;
        uint64_t idx = engine->index_ram_array[mid * 2];
        uint64_t offset = engine->index_ram_array[(mid * 2) + 1];

        if (idx == target_index) return offset;

        if (idx < target_index) {
            best_offset = offset;
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }
    return best_offset;
}

int awal_read_entry(awal_engine_t* engine, uint64_t target_index, uint64_t* out_term, uint8_t** out_payload, uint32_t* out_len) {
    uint64_t head = atomic_load_explicit(&engine->ring.head, memory_order_acquire);
    uint64_t tail = engine->ring.disk_tail;

    // 1. Fast Path: Scan RAM backwards. FIXED: Allows reading flushed items still in the ring buffer bounds
    for (uint64_t i = 0; i < AWAL_RING_CAPACITY; i++) {
        if (head <= i) break;
        uint64_t seq = head - 1 - i;
        if (seq < tail && (tail - seq) >= AWAL_RING_CAPACITY) break; // Overwritten

        awal_entry_t* slot = &engine->ring.entries[seq & AWAL_RING_MASK];
        if (atomic_load_explicit(&slot->ready, memory_order_acquire) == slot->index && slot->index == target_index) {
            *out_term = slot->term;
            *out_len = slot->data_len;
            *out_payload = NULL;
            if (*out_len > 0) {
                *out_payload = malloc(*out_len);
                memcpy(*out_payload, slot->data, *out_len);
            }
            return 1;
        }
    }

    // 2. Slow Path: Read from Compressed Disk
    uint64_t offset = find_offset_in_index(engine, target_index);
    uint32_t header[2];
    if (pread(engine->read_fd, header, 8, offset) != 8) return 0;

    uint8_t* comp_buf = malloc(header[1]);
    pread(engine->read_fd, comp_buf, header[1], offset + 8);

    uint8_t* uncomp_buf = malloc(header[0]);
    LZ4_decompress_safe((const char*)comp_buf, (char*)uncomp_buf, header[1], header[0]);
    free(comp_buf);

    uint32_t pos = 0;
    while (pos < header[0]) {
        uint64_t term, idx;
        uint32_t dlen;
        memcpy(&term, uncomp_buf + pos, 8);
        memcpy(&idx, uncomp_buf + pos + 8, 8);
        memcpy(&dlen, uncomp_buf + pos + 16, 4);

        if (idx == target_index) {
            *out_term = term;
            *out_len = dlen;
            *out_payload = NULL;
            if (dlen > 0) {
                *out_payload = malloc(dlen);
                memcpy(*out_payload, uncomp_buf + pos + 20, dlen);
            }
            free(uncomp_buf);
            return 1;
        }
        pos += 20 + dlen;
    }
    free(uncomp_buf);
    return 0;
}

int awal_demux_step(awal_engine_t* engine, uint64_t* out_term, uint64_t* out_index, uint8_t** out_payload, uint32_t* out_len) {
    // FIXED: Changed tail-recursion to while(true) to prevent Stack Overflow
    while (true) {
        uint64_t current_commit = atomic_load_explicit(&engine->ring.commit_index, memory_order_acquire);
        uint64_t my_seq = engine->ring.demux_tail;

        if (my_seq >= current_commit) return 0;

        uint64_t current_head = atomic_load_explicit(&engine->ring.head, memory_order_acquire);
        bool in_disk_mode = (engine->demux_buf_pos < engine->demux_buf_len) ||
                            (my_seq < engine->ram_head_start_seq) ||
                            ((current_head - my_seq) >= AWAL_RING_CAPACITY);

        if (!in_disk_mode) {
            awal_entry_t* slot = &engine->ring.entries[my_seq & AWAL_RING_MASK];
            *out_term = slot->term;
            *out_index = slot->index;
            *out_len = slot->data_len;
            *out_payload = NULL;

            if (*out_len > 0) {
                *out_payload = malloc(*out_len);
                memcpy(*out_payload, slot->data, *out_len);
            }

            engine->demux_expected_raft_index = slot->index + 1;
            engine->ring.demux_tail++;
            return 1;
        }

        if (engine->demux_buf_pos >= engine->demux_buf_len) {
            if (engine->demux_buf) {
                free(engine->demux_buf);
                engine->demux_buf = NULL;
            }

            if (engine->demux_file_offset == 0) {
                engine->demux_file_offset = find_offset_in_index(engine, engine->demux_expected_raft_index);
            }

            uint32_t header[2];
            if (pread(engine->read_fd, header, 8, engine->demux_file_offset) != 8) return 0;

            uint8_t* comp_buf = malloc(header[1]);
            pread(engine->read_fd, comp_buf, header[1], engine->demux_file_offset + 8);

            engine->demux_buf_len = header[0];
            engine->demux_buf = malloc(engine->demux_buf_len);

            LZ4_decompress_safe((const char*)comp_buf, (char*)engine->demux_buf, header[1], header[0]);

            free(comp_buf);
            engine->demux_buf_pos = 0;
            engine->demux_file_offset += 8 + header[1];
        }

        while (engine->demux_buf_pos < engine->demux_buf_len) {
            uint64_t term, idx;
            uint32_t dlen;
            uint8_t* base = engine->demux_buf + engine->demux_buf_pos;

            memcpy(&term, base, 8);
            memcpy(&idx, base + 8, 8);
            memcpy(&dlen, base + 16, 4);

            engine->demux_buf_pos += 20 + dlen;

            if (idx < engine->demux_expected_raft_index) continue;

            *out_term = term;
            *out_index = idx;
            *out_len = dlen;
            *out_payload = NULL;

            if (dlen > 0) {
                *out_payload = malloc(dlen);
                memcpy(*out_payload, base + 20, dlen);
            }

            engine->demux_expected_raft_index = idx + 1;
            engine->ring.demux_tail++;
            return 1;
        }
    }
}

void awal_close(awal_engine_t* engine) {
    munmap(engine->index_ram_array, 1024 * 1024);
    close(engine->dat_fd);
    close(engine->idx_fd);
    close(engine->read_fd);
    free(engine->ring.entries);
    if (engine->demux_buf) free(engine->demux_buf);
}
