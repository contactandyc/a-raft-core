// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "raft_internal.h"
#include <stdlib.h>
#include <string.h>

uint64_t raft_log_last_index(raft_t* r) {
    if (r->log_len > 0) return r->log[r->log_len - 1].index;
    return r->snapshot_index;
}

uint64_t raft_log_term(raft_t* r, uint64_t index) {
    if (index == r->snapshot_index) return r->snapshot_term;
    if (index < r->snapshot_index || index > raft_log_last_index(r)) return 0;
    return r->log[index - r->snapshot_index].term;
}

raft_entry_t* raft_log_get(raft_t* r, uint64_t index) {
    if (index <= r->snapshot_index || index > raft_log_last_index(r)) return NULL;
    return &r->log[index - r->snapshot_index];
}

bool raft_log_append(raft_t* r, uint64_t term, entry_type_t type, uint64_t cid, uint64_t cseq, const uint8_t* data, size_t data_len) {
    if (r->fatal_error) return false;

    if (data_len > 0 && !data) {
        r->fatal_error = true;
        return false;
    }

    if (UINT64_MAX - r->uncommitted_bytes < data_len) {
        r->fatal_error = true;
        return false;
    }

    if (r->log_len >= r->log_cap) {
        size_t new_cap = r->log_cap == 0 ? 16 : r->log_cap * 2;
        if (new_cap < r->log_cap || new_cap > (SIZE_MAX / sizeof(raft_entry_t))) {
            r->fatal_error = true;
            return false;
        }

        raft_entry_t* new_log = realloc(r->log, new_cap * sizeof(raft_entry_t));
        if (!new_log) {
            r->fatal_error = true;
            return false;
        }
        r->log = new_log;
        r->log_cap = new_cap;
    }

    uint8_t* payload = NULL;
    if (data_len > 0) {
        payload = malloc(data_len);
        if (!payload) {
            r->fatal_error = true;
            return false;
        }
        memcpy(payload, data, data_len);
    }

    uint64_t next_idx = raft_log_last_index(r) + 1;
    raft_entry_t* e = &r->log[r->log_len++];
    e->term = term;
    e->index = next_idx;
    e->type = type;
    e->client_id = cid;
    e->client_seq = cseq;
    e->data_len = data_len;
    e->data = payload;

    r->uncommitted_bytes += data_len;

    return true;
}

void raft_log_truncate(raft_t* r, uint64_t index) {
    if (index <= r->snapshot_index) return;
    size_t new_len = index - r->snapshot_index;
    for (size_t i = new_len; i < r->log_len; i++) {

        if (r->log[i].index > r->commit_index) {
            if (r->uncommitted_bytes >= r->log[i].data_len) {
                r->uncommitted_bytes -= r->log[i].data_len;
            } else {
                r->uncommitted_bytes = 0;
            }
        }

        if (r->log[i].data) free(r->log[i].data);
    }
    r->log_len = new_len;
    if (r->last_saved_index >= index) r->last_saved_index = index - 1;
}

uint64_t raft_uncommitted_bytes(raft_t* r) {
    return r->uncommitted_bytes;
}
