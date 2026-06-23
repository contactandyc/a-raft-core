// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#ifndef RAFT_CODEC_H
#define RAFT_CODEC_H

#include <stdint.h>
#include <stdbool.h>
#include "a-raft-library/raft.h"

// Prevent billion-entry attacks
#define RAFT_MAX_MSG_ENTRIES 100000

// Returns 0 on success, -1 if the payload is malformed or out of bounds
int  raft_codec_serialize_msg(raft_msg_t* m, uint8_t** out_buf, uint32_t* out_len);
int  raft_codec_deserialize_msg(const uint8_t* buf, uint32_t len, raft_msg_t* m);
void raft_codec_free_msg_entries(raft_msg_t* m);

#endif // RAFT_CODEC_H
