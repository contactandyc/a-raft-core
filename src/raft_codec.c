// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-raft-library/raft_codec.h"
#include <stdlib.h>
#include <string.h>

int raft_codec_serialize_msg(raft_msg_t* m, uint8_t** out_buf, uint32_t* out_len) {
    if (m->num_entries > RAFT_MAX_MSG_ENTRIES) return -1;

    uint64_t len = 86;

    for (size_t i = 0; i < m->num_entries; i++) {
        uint64_t entry_size = 8 + 8 + 1 + 8 + 8 + 4 + (uint64_t)m->entries[i].data_len;
        if (len > RAFT_MAX_FRAME_SIZE - entry_size) return -1;
        len += entry_size;
    }

    if (m->snapshot_len > 0) {
        if (len > RAFT_MAX_FRAME_SIZE - m->snapshot_len) return -1;
        len += m->snapshot_len;
    }

    uint8_t* buf = malloc((size_t)len);
    if (!buf) return -1;

    uint32_t pos = 0;
    buf[pos++] = m->type;
    memcpy(buf+pos, &m->to, 8); pos += 8;
    memcpy(buf+pos, &m->from, 8); pos += 8;
    memcpy(buf+pos, &m->term, 8); pos += 8;
    memcpy(buf+pos, &m->log_term, 8); pos += 8;
    memcpy(buf+pos, &m->index, 8); pos += 8;
    memcpy(buf+pos, &m->commit, 8); pos += 8;

    memcpy(buf+pos, &m->conflict_term, 8); pos += 8;
    memcpy(buf+pos, &m->conflict_index, 8); pos += 8;

    memcpy(buf+pos, &m->read_seq, 8); pos += 8;

    buf[pos++] = m->reject ? 1 : 0;

    uint64_t num_e = m->num_entries;
    memcpy(buf+pos, &num_e, 8); pos += 8;

    for (size_t i = 0; i < num_e; i++) {
        memcpy(buf+pos, &m->entries[i].term, 8); pos += 8;
        memcpy(buf+pos, &m->entries[i].index, 8); pos += 8;
        buf[pos++] = m->entries[i].type;

        memcpy(buf+pos, &m->entries[i].client_id, 8); pos += 8;
        memcpy(buf+pos, &m->entries[i].client_seq, 8); pos += 8;

        uint32_t dlen = m->entries[i].data_len;
        memcpy(buf+pos, &dlen, 4); pos += 4;
        if (dlen > 0 && m->entries[i].data != NULL) {
            memcpy(buf+pos, m->entries[i].data, dlen); pos += dlen;
        }
    }

    uint32_t snap_len = (uint32_t)m->snapshot_len;
    memcpy(buf+pos, &snap_len, 4); pos += 4;
    if (snap_len > 0 && m->snapshot_data != NULL) {
        memcpy(buf+pos, m->snapshot_data, snap_len); pos += snap_len;
    }

    *out_buf = buf;
    *out_len = (uint32_t)len;
    return 0;
}

int raft_codec_deserialize_msg(const uint8_t* buf, uint32_t len, raft_msg_t* m) {
    memset(m, 0, sizeof(raft_msg_t));
    if (!buf || len < 86) return -1;

    uint32_t pos = 0;
    m->type = buf[pos++];
    memcpy(&m->to, buf+pos, 8); pos += 8;
    memcpy(&m->from, buf+pos, 8); pos += 8;
    memcpy(&m->term, buf+pos, 8); pos += 8;
    memcpy(&m->log_term, buf+pos, 8); pos += 8;
    memcpy(&m->index, buf+pos, 8); pos += 8;
    memcpy(&m->commit, buf+pos, 8); pos += 8;

    memcpy(&m->conflict_term, buf+pos, 8); pos += 8;
    memcpy(&m->conflict_index, buf+pos, 8); pos += 8;

    memcpy(&m->read_seq, buf+pos, 8); pos += 8;

    m->reject = buf[pos++] == 1;

    uint64_t num_e;
    memcpy(&num_e, buf+pos, 8); pos += 8;

    if (num_e > RAFT_MAX_MSG_ENTRIES) return -1;
    m->num_entries = num_e;

    if (num_e > 0) {
        // PHASE 15 FIX: Subtraction-form bounds check prevents integer overflow attacks
        if (num_e * 37 > len - pos) return -1;

        m->entries = calloc(num_e, sizeof(raft_entry_t));
        if (!m->entries) return -1;

        for (size_t i = 0; i < num_e; i++) {
            if (37 > len - pos) goto cleanup_error;

            memcpy(&m->entries[i].term, buf+pos, 8); pos += 8;
            memcpy(&m->entries[i].index, buf+pos, 8); pos += 8;
            m->entries[i].type = buf[pos++];

            memcpy(&m->entries[i].client_id, buf+pos, 8); pos += 8;
            memcpy(&m->entries[i].client_seq, buf+pos, 8); pos += 8;

            uint32_t dlen;
            memcpy(&dlen, buf+pos, 4); pos += 4;
            m->entries[i].data_len = dlen;

            if (dlen > 0) {
                // PHASE 15 FIX: Subtraction-form limits
                if (dlen > len - pos) goto cleanup_error;
                m->entries[i].data = malloc(dlen);
                if (!m->entries[i].data) goto cleanup_error;
                memcpy(m->entries[i].data, buf+pos, dlen); pos += dlen;
            }
        }
    }

    if (4 <= len - pos) {
        uint32_t slen;
        memcpy(&slen, buf+pos, 4); pos += 4;
        m->snapshot_len = slen;
        if (slen > 0) {
            // PHASE 15 FIX: Subtraction-form limits
            if (slen > len - pos) goto cleanup_error;
            m->snapshot_data = malloc(slen);
            if (!m->snapshot_data) goto cleanup_error;
            memcpy(m->snapshot_data, buf+pos, slen); pos += slen;
        }
    }

    return 0;

cleanup_error:
    raft_codec_free_msg_entries(m);
    return -1;
}

void raft_codec_free_msg_entries(raft_msg_t* m) {
    if (m->num_entries > 0 && m->entries) {
        for (size_t i = 0; i < m->num_entries; i++) {
            if (m->entries[i].data) free(m->entries[i].data);
        }
        free(m->entries);
        m->entries = NULL;
        m->num_entries = 0;
    }
    if (m->snapshot_data) {
        free(m->snapshot_data);
        m->snapshot_data = NULL;
        m->snapshot_len = 0;
    }
}
