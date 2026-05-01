// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef RAFT_IO_H
#define RAFT_IO_H

#include "a-raft-library/raft_core.h"
#include "a-raft-library/awal.h"

// Wakes up a node, parsing the WAL and feeding it safely into raft_core_restore
raft_core_t* raft_io_boot(awal_engine_t* wal, uint64_t node_id, uint64_t* init_peers, size_t num_peers, uint64_t saved_term, uint64_t saved_vote);

// Reads entries_to_save, streams them to the LZ4 WAL, and performs rollback truncations automatically
bool raft_io_save(awal_engine_t* wal, raft_ready_t* ready);

#endif // RAFT_IO_H
