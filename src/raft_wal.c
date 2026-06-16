// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define _GNU_SOURCE
#include "a-raft-library/raft_wal.h"
#include "a-memory-library/aml_alloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <time.h>

// -----------------------------------------------------------------------------
// INTERNAL UTILITIES
// -----------------------------------------------------------------------------

static uint32_t crc32(const void *buf, size_t size) {
    const uint8_t *p = buf;
    uint32_t crc = ~0U;
    while (size--) {
        crc ^= *p++;
        for (int i = 0; i < 8; i++) crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
    }
    return ~crc;
}

static int preallocate_file(int fd, uint64_t size) {
#ifdef __APPLE__
    fstore_t store = {F_ALLOCATECONTIG, F_PEOFPOSMODE, 0, size, 0};
    fcntl(fd, F_PREALLOCATE, &store);
    return ftruncate(fd, size);
#else
    return posix_fallocate(fd, 0, size);
#endif
}

static void get_segment_path(raft_wal_t* wal, uint64_t seg_id, char* out_path) {
    snprintf(out_path, 1024, "%s/%010llu.wal", wal->base_dir, seg_id);
}

static void ensure_offset_capacity(raft_wal_t* wal, uint64_t index) {
    if (index >= wal->offsets_cap) {
        uint64_t new_cap = wal->offsets_cap == 0 ? 1024 : wal->offsets_cap * 2;
        while (index >= new_cap) new_cap *= 2;

        raft_wal_loc_t* new_offsets = aml_calloc(new_cap, sizeof(raft_wal_loc_t));
        if (wal->offsets) {
            memcpy(new_offsets, wal->offsets, wal->offsets_cap * sizeof(raft_wal_loc_t));
            aml_free(wal->offsets);
        }
        wal->offsets = new_offsets;
        wal->offsets_cap = new_cap;
    }
}

// -----------------------------------------------------------------------------
// LIFECYCLE & RECOVERY
// -----------------------------------------------------------------------------

int raft_wal_init(raft_wal_t* wal, const char* dir, uint64_t segment_size_mb, uint32_t max_standby) {
    memset(wal, 0, sizeof(raft_wal_t));
    strncpy(wal->base_dir, dir, sizeof(wal->base_dir) - 1);
    wal->segment_size_bytes = segment_size_mb * 1024 * 1024;
    wal->max_standby = max_standby;
    wal->standby_paths = aml_malloc(sizeof(char*) * max_standby);
    wal->batch_cap = 65536;
    wal->batch_buf = aml_malloc(wal->batch_cap);
    wal->read_fd = -1;

    mkdir(dir, 0755);

    uint64_t min_seg = UINT64_MAX, max_seg = 0;
    DIR *dp = opendir(dir);
    struct dirent *ep;

    // 1. Scan Directory
    if (dp) {
        while ((ep = readdir(dp))) {
            if (strncmp(ep->d_name, "standby_", 8) == 0) {
                if (wal->standby_count < wal->max_standby) {
                    wal->standby_paths[wal->standby_count++] = aml_strdupf("%s/%s", dir, ep->d_name);
                } else {
                    char excess[1024]; snprintf(excess, 1024, "%s/%s", dir, ep->d_name);
                    unlink(excess);
                }
            } else if (strstr(ep->d_name, ".wal")) {
                uint64_t seg_id;
                if (sscanf(ep->d_name, "%llu.wal", &seg_id) == 1) {
                    if (seg_id < min_seg) min_seg = seg_id;
                    if (seg_id > max_seg) max_seg = seg_id;
                }
            }
        }
        closedir(dp);
    }

    // 2. Format Fresh
    if (max_seg == 0) {
        wal->oldest_seg_id = 1;
        wal->current_seg_id = 1;
        wal->file_offset = RAFT_WAL_SEG_HEADER_SIZE;

        char path[1024]; get_segment_path(wal, 1, path);
        wal->active_fd = open(path, O_RDWR | O_CREAT, 0644);
        preallocate_file(wal->active_fd, wal->segment_size_bytes);

        uint32_t magic = RAFT_WAL_MAGIC;
        uint64_t start_idx = 1;
        pwrite(wal->active_fd, &magic, 4, 0);
        pwrite(wal->active_fd, &wal->current_seg_id, 8, 4);
        pwrite(wal->active_fd, &start_idx, 8, 12);
        return 0;
    }

    // 3. Recover Existing Timeline
    wal->oldest_seg_id = min_seg;
    wal->current_seg_id = max_seg;

    for (uint64_t seg = min_seg; seg <= max_seg; seg++) {
        char path[1024]; get_segment_path(wal, seg, path);
        int fd = open(path, O_RDWR);
        if (fd < 0) continue;

        uint64_t offset = RAFT_WAL_SEG_HEADER_SIZE;
        bool torn_write = false;

        while (offset < wal->segment_size_bytes) {
            uint8_t header[RAFT_WAL_FRAME_HEADER_SIZE];
            if (pread(fd, header, RAFT_WAL_FRAME_HEADER_SIZE, offset) != RAFT_WAL_FRAME_HEADER_SIZE) break;

            uint32_t crc, len; uint64_t term, index; uint8_t type;
            memcpy(&crc, header, 4); memcpy(&len, header + 4, 4);
            memcpy(&term, header + 8, 8); memcpy(&index, header + 16, 8);
            type = header[24];
            (void)type;

            if (len == 0 && index == 0) break; // End of written data

            uint8_t* payload = len > 0 ? aml_malloc(len) : NULL;
            if (len > 0 && pread(fd, payload, len, offset + RAFT_WAL_FRAME_HEADER_SIZE) != len) {
                torn_write = true;
                aml_free(payload);
                break;
            }

            if (crc != crc32(payload, len)) {
                torn_write = true;
                if (payload) aml_free(payload);
                break;
            }
            if (payload) aml_free(payload);

            ensure_offset_capacity(wal, index);
            wal->offsets[index].seg_id = seg;
            wal->offsets[index].offset = offset;
            wal->max_disk_index = index;

            offset += RAFT_WAL_FRAME_HEADER_SIZE + len;
        }

        if (torn_write || seg == max_seg) {
            wal->current_seg_id = seg;
            wal->active_fd = fd;
            wal->file_offset = offset;
            ftruncate(fd, offset); // Slice off dead bytes

            // Delete any segments that existed after the tear point
            for (uint64_t bad_seg = seg + 1; bad_seg <= max_seg; bad_seg++) {
                char bad_path[1024]; get_segment_path(wal, bad_seg, bad_path);
                unlink(bad_path);
            }
            break;
        }
        close(fd);
    }
    return 0;
}

// -----------------------------------------------------------------------------
// APPEND & ROTATE
// -----------------------------------------------------------------------------

static void raft_wal_rotate(raft_wal_t* wal, uint64_t next_seq) {
    close(wal->active_fd);
    wal->current_seg_id++;

    char new_path[1024]; get_segment_path(wal, wal->current_seg_id, new_path);

    if (wal->standby_count > 0) {
        wal->standby_count--;
        char* standby_path = wal->standby_paths[wal->standby_count];
        rename(standby_path, new_path);
        aml_free(standby_path);
        wal->active_fd = open(new_path, O_RDWR);
    } else {
        wal->active_fd = open(new_path, O_RDWR | O_CREAT, 0644);
        preallocate_file(wal->active_fd, wal->segment_size_bytes);
    }

    uint32_t magic = RAFT_WAL_MAGIC;
    pwrite(wal->active_fd, &magic, 4, 0);
    pwrite(wal->active_fd, &wal->current_seg_id, 8, 4);
    pwrite(wal->active_fd, &next_seq, 8, 12);

    wal->file_offset = RAFT_WAL_SEG_HEADER_SIZE;
}

int raft_wal_append(raft_wal_t* wal, uint64_t term, uint64_t index, uint8_t type, const uint8_t* payload, uint32_t len) {
    uint32_t total_size = RAFT_WAL_FRAME_HEADER_SIZE + len;

    // Flush & Rotate if this frame crosses the segment boundary
    if (wal->file_offset + wal->batch_len + total_size > wal->segment_size_bytes) {
        raft_wal_flush_batch(wal);
        raft_wal_rotate(wal, index);
    }

    if (wal->batch_len + total_size > wal->batch_cap) {
        wal->batch_cap = (wal->batch_len + total_size) * 2;
        wal->batch_buf = aml_realloc(wal->batch_buf, wal->batch_cap);
    }

    uint8_t* ptr = wal->batch_buf + wal->batch_len;
    uint32_t crc = crc32(payload, len);

    memcpy(ptr, &crc, 4); memcpy(ptr + 4, &len, 4);
    memcpy(ptr + 8, &term, 8); memcpy(ptr + 16, &index, 8);
    ptr[24] = type;
    if (len > 0) memcpy(ptr + RAFT_WAL_FRAME_HEADER_SIZE, payload, len);

    ensure_offset_capacity(wal, index);
    wal->offsets[index].seg_id = wal->current_seg_id;
    wal->offsets[index].offset = wal->file_offset + wal->batch_len;

    wal->max_disk_index = index;
    wal->batch_len += total_size;
    return 0;
}

int raft_wal_flush_batch(raft_wal_t* wal) {
    if (wal->batch_len == 0) return 0;
    if (pwrite(wal->active_fd, wal->batch_buf, wal->batch_len, wal->file_offset) != wal->batch_len) return -1;
    wal->file_offset += wal->batch_len;
    wal->batch_len = 0;
#ifdef __APPLE__
    fcntl(wal->active_fd, F_FULLFSYNC, 0);
#else
    fdatasync(wal->active_fd);
#endif
    return 0;
}

// -----------------------------------------------------------------------------
// READ CACHE
// -----------------------------------------------------------------------------

int raft_wal_read_entry(raft_wal_t* wal, uint64_t target_index, uint64_t* out_term, uint8_t* out_type, uint8_t** out_payload, uint32_t* out_len) {
    if (target_index == 0 || target_index > wal->max_disk_index) return 0;

    raft_wal_loc_t loc = wal->offsets[target_index];
    if (loc.seg_id == 0) return 0; // Truncated or purged

    if (wal->read_seg_id != loc.seg_id) {
        if (wal->read_fd >= 0) close(wal->read_fd);
        char path[1024]; get_segment_path(wal, loc.seg_id, path);
        wal->read_fd = open(path, O_RDONLY);
        wal->read_seg_id = loc.seg_id;
    }

    uint8_t header[RAFT_WAL_FRAME_HEADER_SIZE];
    if (pread(wal->read_fd, header, RAFT_WAL_FRAME_HEADER_SIZE, loc.offset) != RAFT_WAL_FRAME_HEADER_SIZE) return 0;

    uint32_t len;
    memcpy(&len, header + 4, 4);
    memcpy(out_term, header + 8, 8);
    *out_type = header[24];
    *out_len = len;

    if (len > 0) {
        *out_payload = aml_malloc(len);
        pread(wal->read_fd, *out_payload, len, loc.offset + RAFT_WAL_FRAME_HEADER_SIZE);
    } else {
        *out_payload = NULL;
    }
    return 1;
}

// -----------------------------------------------------------------------------
// TAIL TRUNCATION (Raft Conflicts)
// -----------------------------------------------------------------------------

int raft_wal_truncate_tail(raft_wal_t* wal, uint64_t truncate_from_index) {
    if (truncate_from_index > wal->max_disk_index) return 0;

    raft_wal_flush_batch(wal); // Flush RAM first
    raft_wal_loc_t loc = wal->offsets[truncate_from_index];

    // 1. Revert to the segment where the conflict begins
    if (wal->current_seg_id != loc.seg_id) {
        close(wal->active_fd);

        // Throw away newer segments into the Standby Pool
        for (uint64_t bad_seg = wal->current_seg_id; bad_seg > loc.seg_id; bad_seg--) {
            char bad_path[1024]; get_segment_path(wal, bad_seg, bad_path);
            if (wal->standby_count < wal->max_standby) {
                char* standby_path = aml_strdupf("%s/standby_%llu_%u.wal", wal->base_dir, (unsigned long long)time(NULL), wal->standby_count);
                rename(bad_path, standby_path);
                wal->standby_paths[wal->standby_count++] = standby_path;
            } else {
                unlink(bad_path);
            }
        }

        char path[1024]; get_segment_path(wal, loc.seg_id, path);
        wal->active_fd = open(path, O_RDWR);
        wal->current_seg_id = loc.seg_id;
    }

    // 2. Physically slice the file
    ftruncate(wal->active_fd, loc.offset);
#ifdef __APPLE__
    fcntl(wal->active_fd, F_FULLFSYNC, 0);
#else
    fdatasync(wal->active_fd);
#endif

    wal->file_offset = loc.offset;
    wal->max_disk_index = truncate_from_index - 1;
    return 0;
}

// -----------------------------------------------------------------------------
// HEAD PURGING (Garbage Collection)
// -----------------------------------------------------------------------------

void raft_wal_purge_head(raft_wal_t* wal, uint64_t safe_checkpoint_index) {
    while (wal->oldest_seg_id < wal->current_seg_id) {
        char path[1024]; get_segment_path(wal, wal->oldest_seg_id, path);

        // Peek at the NEXT segment's header
        char next_path[1024]; get_segment_path(wal, wal->oldest_seg_id + 1, next_path);
        int next_fd = open(next_path, O_RDONLY);
        if (next_fd < 0) break;

        uint64_t next_start_idx;
        pread(next_fd, &next_start_idx, 8, 12);
        close(next_fd);

        // If the next segment starts at index 5000, and our checkpoint is 6000,
        // then the entire oldest segment is completely obsolete!
        if (next_start_idx <= safe_checkpoint_index) {
            if (wal->standby_count < wal->max_standby) {
                char* standby_path = aml_strdupf("%s/standby_%llu_%u.wal", wal->base_dir, (unsigned long long)time(NULL), wal->standby_count);
                rename(path, standby_path);
                wal->standby_paths[wal->standby_count++] = standby_path;
            } else {
                unlink(path);
            }

            // Clean up our O(1) lookup map slightly to prevent using deleted references
            for (uint64_t i = 0; i < next_start_idx; i++) {
                if (i < wal->offsets_cap) wal->offsets[i].seg_id = 0;
            }

            wal->oldest_seg_id++;
        } else {
            break;
        }
    }
}

void raft_wal_close(raft_wal_t* wal) {
    raft_wal_flush_batch(wal);
    if (wal->active_fd >= 0) close(wal->active_fd);
    if (wal->read_fd >= 0) close(wal->read_fd);

    for (uint32_t i = 0; i < wal->standby_count; i++) aml_free(wal->standby_paths[i]);
    aml_free(wal->standby_paths);
    if (wal->offsets) aml_free(wal->offsets);
    if (wal->batch_buf) aml_free(wal->batch_buf);
}
