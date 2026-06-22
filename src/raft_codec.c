// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "raft_internal.h"
#include <string.h>
#include <stdlib.h>

#ifndef RAFT_MAX_MSG_ENTRIES
#define RAFT_MAX_MSG_ENTRIES 100000
#endif

// ============================================================================
// PORTABLE, ALIGNMENT-SAFE PRIMITIVES
// ============================================================================

static void write_u8(uint8_t** p, uint8_t v) {
    (*p)[0] = v;
    *p += 1;
}

static void write_u32(uint8_t** p, uint32_t v) {
    uint8_t* b = *p;
    b[0] = (uint8_t)(v >> 24); b[1] = (uint8_t)(v >> 16);
    b[2] = (uint8_t)(v >> 8);  b[3] = (uint8_t)(v);
    *p += 4;
}

static void write_u64(uint8_t** p, uint64_t v) {
    uint8_t* b = *p;
    b[0] = (uint8_t)(v >> 56); b[1] = (uint8_t)(v >> 48);
    b[2] = (uint8_t)(v >> 40); b[3] = (uint8_t)(v >> 32);
    b[4] = (uint8_t)(v >> 24); b[5] = (uint8_t)(v >> 16);
    b[6] = (uint8_t)(v >> 8);  b[7] = (uint8_t)(v);
    *p += 8;
}

static bool read_u8(const uint8_t** p, size_t* rem, uint8_t* out) {
    if (*rem < 1) return false;
    *out = (*p)[0];
    *p += 1; *rem -= 1;
    return true;
}

static bool read_u32(const uint8_t** p, size_t* rem, uint32_t* out) {
    if (*rem < 4) return false;
    const uint8_t* b = *p;
    *out = ((uint32_t)b[0] << 24) | ((uint32_t)b[1] << 16) |
           ((uint32_t)b[2] << 8)  |  (uint32_t)b[3];
    *p += 4; *rem -= 4;
    return true;
}

static bool read_u64(const uint8_t** p, size_t* rem, uint64_t* out) {
    if (*rem < 8) return false;
    const uint8_t* b = *p;
    *out = ((uint64_t)b[0] << 56) | ((uint64_t)b[1] << 48) |
           ((uint64_t)b[2] << 40) | ((uint64_t)b[3] << 32) |
           ((uint64_t)b[4] << 24) | ((uint64_t)b[5] << 16) |
           ((uint64_t)b[6] << 8)  |  (uint64_t)b[7];
    *p += 8; *rem -= 8;
    return true;
}

static bool read_bytes(const uint8_t** p, size_t* rem, uint8_t** out_data, size_t len) {
    if (len == 0) {
        *out_data = NULL;
        return true;
    }
    if (*rem < len) return false;

    *out_data = malloc(len);
    if (!*out_data) return false;

    memcpy(*out_data, *p, len);
    *p += len; *rem -= len;
    return true;
}

// ============================================================================
// PROTOCOL VALIDATION & SAFETY
// ============================================================================

static bool is_valid_msg_type(uint8_t t) {
    return t <= MSG_READ_INDEX_RES;
}

static bool is_valid_entry_type(uint8_t t) {
    return t == ENTRY_NORMAL ||
           t == ENTRY_CONF_ADD ||
           t == ENTRY_CONF_REMOVE ||
           t == ENTRY_CONF_ADD_LEARNER ||
           t == ENTRY_CONF_PROMOTE_LEARNER;
}

static bool add_size(size_t* total, size_t add) {
    if (add > SIZE_MAX - *total) return false;
    *total += add;
    return true;
}

void raft_msg_free_payloads(raft_msg_t* msg) {
    if (!msg) return;
    if (msg->entries) {
        for (size_t i = 0; i < msg->num_entries; i++) {
            if (msg->entries[i].data) free(msg->entries[i].data);
        }
        free(msg->entries);
    }
    if (msg->snapshot_data) free(msg->snapshot_data);
    if (msg->snapshot_peers) free(msg->snapshot_peers);
    if (msg->snapshot_is_learner) free(msg->snapshot_is_learner);

    memset(msg, 0, sizeof(raft_msg_t));
}

bool raft_msg_encoded_size_checked(const raft_msg_t* msg, size_t* out) {
    size_t sz = 86; // Base fixed header size

    if (!msg || !out) return false;
    if (msg->num_entries > RAFT_MAX_MSG_ENTRIES) return false;
    if (msg->snapshot_num_peers > MAX_PEERS) return false;
    if (msg->num_entries > UINT32_MAX) return false;
    if (msg->snapshot_len > UINT32_MAX) return false;
    if (msg->snapshot_num_peers > UINT32_MAX) return false;

    // Reject structurally invalid messages
    if (msg->num_entries > 0 && !msg->entries) return false;
    if (msg->snapshot_len > 0 && !msg->snapshot_data) return false;
    if (msg->snapshot_num_peers > 0 && (!msg->snapshot_peers || !msg->snapshot_is_learner)) return false;

    for (size_t i = 0; i < msg->num_entries; i++) {
        if (msg->entries[i].data_len > UINT32_MAX) return false;
        if (msg->entries[i].data_len > 0 && !msg->entries[i].data) return false;

        if (!add_size(&sz, 37)) return false; // 37 bytes per entry header
        if (!add_size(&sz, msg->entries[i].data_len)) return false;
    }

    if (!add_size(&sz, msg->snapshot_len)) return false;

    if (msg->snapshot_num_peers > SIZE_MAX / 9) return false;
    if (!add_size(&sz, msg->snapshot_num_peers * 9)) return false; // 8 bytes ID + 1 byte bool

    if (sz > RAFT_MAX_FRAME_SIZE) return false;

    *out = sz;
    return true;
}

size_t raft_msg_encoded_size(const raft_msg_t* msg) {
    size_t out = 0;
    raft_msg_encoded_size_checked(msg, &out);
    return out;
}

// ============================================================================
// PUBLIC CODEC API
// ============================================================================

bool raft_msg_encode(const raft_msg_t* msg, uint8_t* buf, size_t cap) {
    size_t expected_size;
    if (!raft_msg_encoded_size_checked(msg, &expected_size)) return false;
    if (expected_size > cap) return false;

    uint8_t* p = buf;

    write_u8(&p, (uint8_t)msg->type);
    write_u64(&p, msg->to);
    write_u64(&p, msg->from);
    write_u64(&p, msg->term);
    write_u64(&p, msg->log_term);
    write_u64(&p, msg->index);
    write_u64(&p, msg->commit);
    write_u64(&p, msg->conflict_term);
    write_u64(&p, msg->conflict_index);
    write_u64(&p, msg->read_seq);
    write_u8(&p, msg->reject ? 1 : 0);

    write_u32(&p, (uint32_t)msg->num_entries);
    write_u32(&p, (uint32_t)msg->snapshot_len);
    write_u32(&p, (uint32_t)msg->snapshot_num_peers);

    for (size_t i = 0; i < msg->num_entries; i++) {
        write_u64(&p, msg->entries[i].term);
        write_u64(&p, msg->entries[i].index);
        write_u8(&p, (uint8_t)msg->entries[i].type);
        write_u64(&p, msg->entries[i].client_id);
        write_u64(&p, msg->entries[i].client_seq);
        write_u32(&p, (uint32_t)msg->entries[i].data_len);

        if (msg->entries[i].data_len > 0) {
            memcpy(p, msg->entries[i].data, msg->entries[i].data_len);
            p += msg->entries[i].data_len;
        }
    }

    if (msg->snapshot_len > 0) {
        memcpy(p, msg->snapshot_data, msg->snapshot_len);
        p += msg->snapshot_len;
    }

    for (size_t i = 0; i < msg->snapshot_num_peers; i++) {
        write_u64(&p, msg->snapshot_peers[i]);
        write_u8(&p, msg->snapshot_is_learner[i] ? 1 : 0);
    }

    return true;
}

bool raft_msg_decode(const uint8_t* buf, size_t len, raft_msg_t* out_msg) {
    if (!out_msg) return false;
    memset(out_msg, 0, sizeof(raft_msg_t));
    if (!buf || len > RAFT_MAX_FRAME_SIZE || len < 86) return false;

    const uint8_t* p = buf;
    size_t rem = len;

    uint8_t type_val, reject_val;
    uint32_t num_entries, snap_len, num_peers;

    if (!read_u8(&p, &rem, &type_val) ||
        !read_u64(&p, &rem, &out_msg->to) ||
        !read_u64(&p, &rem, &out_msg->from) ||
        !read_u64(&p, &rem, &out_msg->term) ||
        !read_u64(&p, &rem, &out_msg->log_term) ||
        !read_u64(&p, &rem, &out_msg->index) ||
        !read_u64(&p, &rem, &out_msg->commit) ||
        !read_u64(&p, &rem, &out_msg->conflict_term) ||
        !read_u64(&p, &rem, &out_msg->conflict_index) ||
        !read_u64(&p, &rem, &out_msg->read_seq) ||
        !read_u8(&p, &rem, &reject_val) ||
        !read_u32(&p, &rem, &num_entries) ||
        !read_u32(&p, &rem, &snap_len) ||
        !read_u32(&p, &rem, &num_peers)) {
        goto decode_fail;
    }

    if (!is_valid_msg_type(type_val)) goto decode_fail;
    if (reject_val > 1) goto decode_fail;
    if (num_entries > RAFT_MAX_MSG_ENTRIES) goto decode_fail;
    if (num_peers > MAX_PEERS) goto decode_fail;

    out_msg->type = (msg_type_t)type_val;
    out_msg->reject = (reject_val != 0);
    out_msg->num_entries = num_entries;
    out_msg->snapshot_len = snap_len;
    out_msg->snapshot_num_peers = num_peers;

    if (num_entries > 0) {
        if (rem / 37 < num_entries) goto decode_fail;

        out_msg->entries = calloc(num_entries, sizeof(raft_entry_t));
        if (!out_msg->entries) goto decode_fail;

        for (size_t i = 0; i < num_entries; i++) {
            uint8_t etype;
            uint32_t dlen;
            if (!read_u64(&p, &rem, &out_msg->entries[i].term) ||
                !read_u64(&p, &rem, &out_msg->entries[i].index) ||
                !read_u8(&p, &rem, &etype) ||
                !read_u64(&p, &rem, &out_msg->entries[i].client_id) ||
                !read_u64(&p, &rem, &out_msg->entries[i].client_seq) ||
                !read_u32(&p, &rem, &dlen)) {
                goto decode_fail;
            }
            if (!is_valid_entry_type(etype)) goto decode_fail;

            out_msg->entries[i].type = (entry_type_t)etype;
            out_msg->entries[i].data_len = dlen;

            if (!read_bytes(&p, &rem, &out_msg->entries[i].data, dlen)) {
                goto decode_fail;
            }
        }
    }

    if (snap_len > 0) {
        if (!read_bytes(&p, &rem, &out_msg->snapshot_data, snap_len)) goto decode_fail;
    }

    if (num_peers > 0) {
        if (rem / 9 < num_peers) goto decode_fail;

        out_msg->snapshot_peers = calloc(num_peers, sizeof(uint64_t));
        out_msg->snapshot_is_learner = calloc(num_peers, sizeof(bool));
        if (!out_msg->snapshot_peers || !out_msg->snapshot_is_learner) goto decode_fail;

        for (size_t i = 0; i < num_peers; i++) {
            uint8_t lrn;
            if (!read_u64(&p, &rem, &out_msg->snapshot_peers[i]) ||
                !read_u8(&p, &rem, &lrn)) {
                goto decode_fail;
            }
            if (lrn > 1) goto decode_fail;
            out_msg->snapshot_is_learner[i] = (lrn != 0);
        }
    }

    // STRICT TAIL VALIDATION: Reject any packet with unparsed trailing bytes
    if (rem != 0) goto decode_fail;

    return true;

decode_fail:
    raft_msg_free_payloads(out_msg);
    return false;
}
