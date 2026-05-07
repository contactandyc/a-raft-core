// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-raft-library/raft_io.h"
#include <stdlib.h>

raft_core_t* raft_io_boot(awal_engine_t* wal, uint64_t node_id, uint64_t* init_peers, size_t num_peers, uint64_t saved_term, uint64_t saved_vote, uint64_t saved_commit) {

    // PHASE 5: If the WAL is empty, we MUST STILL restore the HardState using a dummy entry!
    if (wal->max_disk_index == 0) {
        raft_entry_t dummy = { .term = 0, .index = 0, .type = ENTRY_NORMAL, .data = NULL, .data_len = 0 };
        return raft_core_restore(node_id, init_peers, num_peers, saved_term, saved_vote, saved_commit, &dummy, 1);
    }

    size_t total_entries = wal->max_disk_index + 1;
    raft_entry_t* entries = calloc(total_entries, sizeof(raft_entry_t));

    entries[0] = (raft_entry_t){ .term = 0, .index = 0, .type = ENTRY_NORMAL, .data = NULL, .data_len = 0 };

    for (uint64_t i = 1; i <= wal->max_disk_index; i++) {
        uint64_t term; uint8_t type; uint8_t* payload = NULL; uint32_t len = 0;

        if (awal_read_entry(wal, i, &term, &type, &payload, &len)) {
            entries[i].term = term;
            entries[i].index = i;
            entries[i].type = (entry_type_t)type;
            entries[i].data = payload;
            entries[i].data_len = len;
        } else {
            free(entries);
            return NULL;
        }
    }

    raft_core_t* core = raft_core_restore(node_id, init_peers, num_peers, saved_term, saved_vote, saved_commit, entries, total_entries);

    for (size_t i = 1; i <= wal->max_disk_index; i++) {
        if (entries[i].data) free(entries[i].data);
    }
    free(entries);

    return core;
}

bool raft_io_save(awal_engine_t* wal, raft_ready_t* ready) {
    if (ready->num_entries_to_save == 0) return true;

    uint64_t first_idx = ready->entries_to_save[0].index;

    if (first_idx <= wal->max_disk_index) {
        awal_truncate(wal, first_idx);
    }

    for (size_t i = 0; i < ready->num_entries_to_save; i++) {
        raft_entry_t* e = &ready->entries_to_save[i];
        if (awal_append(wal, e->term, e->index, (uint8_t)e->type, e->data, e->data_len) != 0) {
            return false;
        }
    }

    return awal_flush_batch(wal) >= 0;
}
