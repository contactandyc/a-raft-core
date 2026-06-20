// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <string.h>
#include "a-raft-library/raft.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(raft_read_index_success_on_heartbeat_quorum) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &vote);
    raft_advance_all(r);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step(r, &p);
    raft_advance_all(r);
    raft_msg_t ack2 = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_step(r, &ack2);
    raft_advance_all(r);

    raft_msg_t ri = { .type = MSG_READ_INDEX, .read_seq = 12345 };
    raft_step(r, &ri);
    raft_advance_all(r);

    raft_msg_t ack3 = { .type = MSG_APPEND_RES, .from = 3, .term = 1, .reject = false, .index = 2, .read_seq = 1 };
    raft_step(r, &ack3);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 1);
    MACRO_ASSERT_EQ_INT(ready.read_states[0].read_seq, 12345);
    MACRO_ASSERT_EQ_INT(ready.read_states[0].index, 2);

    raft_destroy(r);
}

MACRO_TEST(raft_fault_stale_read_index_response_ignored) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_step(r, &vote);

    raft_step(r, &hup);
    raft_msg_t pv2 = { .type = MSG_PRE_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_step(r, &pv2);
    raft_msg_t vote2 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_step(r, &vote2);
    raft_advance_all(r);

    raft_msg_t stale_ack = {
        .type = MSG_APPEND_RES, .from = 2, .term = 1,
        .reject = false, .index = 2, .read_seq = 55
    };
    raft_step(r, &stale_ack);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 0);

    raft_destroy(r);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, raft_read_index_success_on_heartbeat_quorum);
    MACRO_ADD(tests, raft_fault_stale_read_index_response_ignored);
    macro_run_all("raft_read_index", tests, test_count);
    return 0;
}
