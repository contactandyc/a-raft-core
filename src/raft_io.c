// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-raft-library/raft_io.h"
#include "a-memory-library/aml_alloc.h"
#include <stdlib.h>

raft_core_t* raft_io_boot(raft_wal_t* wal, uint64_t node_id, uint64_t* init_peers, size_t num_peers, uint64_t saved_term, uint64_t saved_vote, uint64_t saved_commit, uint64_t saved_applied) {

    if (wal->max_disk_index == 0) {
        raft_entry_t dummy = { .term = 0, .index = 0, .type = ENTRY_NORMAL, .data = NULL, .data_len = 0 };
        return raft_core_restore(node_id, init_peers, num_peers, saved_term, saved_vote, saved_commit, saved_applied, &dummy, 1);
    }

    // PHASE 2 (Gap 5): Dynamically discover the first surviving index after a WAL purge
    uint64_t start_idx = 1;
    while (start_idx <= wal->max_disk_index && wal->offsets[start_idx].seg_id == 0) {
        start_idx++;
    }

    if (start_idx > wal->max_disk_index) {
        // Complete amnesia: WAL exists but all records were purged and a snapshot is missing
        raft_entry_t dummy = { .term = 0, .index = 0, .type = ENTRY_NORMAL, .data = NULL, .data_len = 0 };
        return raft_core_restore(node_id, init_peers, num_peers, saved_term, saved_vote, saved_commit, saved_applied, &dummy, 1);
    }

    size_t total_entries = wal->max_disk_index - start_idx + 1;
    raft_entry_t* entries = calloc(total_entries, sizeof(raft_entry_t));

    for (uint64_t i = start_idx; i <= wal->max_disk_index; i++) {
        uint64_t term; uint8_t type; uint8_t* payload = NULL; uint32_t len = 0;
        size_t arr_idx = i - start_idx;

        if (raft_wal_read_entry(wal, i, &term, &type, &payload, &len)) {
            entries[arr_idx].term = term;
            entries[arr_idx].index = i;
            entries[arr_idx].type = (entry_type_t)type;
            entries[arr_idx].data = payload;
            entries[arr_idx].data_len = len;
        } else {
            // Rollback memory if disk verification fails halfway through
            for(size_t j = 0; j < arr_idx; j++) {
                if (entries[j].data) aml_free(entries[j].data);
            }
            free(entries);
            return NULL;
        }
    }

    raft_core_t* core = raft_core_restore(node_id, init_peers, num_peers, saved_term, saved_vote, saved_commit, saved_applied, entries, total_entries);

    for (size_t i = 0; i < total_entries; i++) {
        if (entries[i].data) aml_free(entries[i].data);
    }
    free(entries);

    return core;
}

bool raft_io_save(raft_wal_t* wal, raft_ready_t* ready) {
    if (ready->num_entries_to_save == 0) return true;

    uint64_t first_idx = ready->entries_to_save[0].index;

    if (first_idx <= wal->max_disk_index) {
        raft_wal_truncate_tail(wal, first_idx);
    }

    for (size_t i = 0; i < ready->num_entries_to_save; i++) {
        raft_entry_t* e = &ready->entries_to_save[i];
        if (raft_wal_append(wal, e->term, e->index, (uint8_t)e->type, e->data, e->data_len) != 0) {
            return false;
        }
    }

    return raft_wal_flush_batch(wal) == 0;
}
