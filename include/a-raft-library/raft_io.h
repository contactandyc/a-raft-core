// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef RAFT_IO_H
#define RAFT_IO_H

#include "a-raft-library/raft_core.h"
#include "a-raft-library/raft_wal.h"

// PHASE 2: Added saved_applied
raft_core_t* raft_io_boot(raft_wal_t* wal, uint64_t node_id, uint64_t* init_peers, size_t num_peers, uint64_t saved_term, uint64_t saved_vote, uint64_t saved_commit, uint64_t saved_applied);

bool raft_io_save(raft_wal_t* wal, raft_ready_t* ready);

#endif // RAFT_IO_H
