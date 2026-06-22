// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#define RAFT_TESTING 1
#include <stdio.h>
#include <string.h>
#include "raft_internal.h"
#include "the-macro-library/macro_test.h"

// Helper function to safely boot a leader and commit a current-term entry
// to mathematically unlock the ReadIndex mechanism.
static void boot_leader_and_unlock_reads(raft_t* r) {
    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);

    // Win election
    for (size_t i = 1; i < r->num_peers; i++) {
        raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = r->peers[i], .term = 1, .reject = false };
        raft_step_remote(r, &pv);
    }
    for (size_t i = 1; i < r->num_peers; i++) {
        raft_msg_t v = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = r->peers[i], .term = 1, .reject = false };
        raft_step_remote(r, &v);
    }
    raft_advance_all_for_tests_only(r);

    // Commit an entry in the current term to unlock ReadIndex
    raft_entry_t e = { .term = 1, .index = 2, .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_step_local(r, &prop);
    raft_advance_all_for_tests_only(r);

    for (size_t i = 1; i < r->num_peers; i++) {
        raft_msg_t ack = { .type = MSG_APPEND_RES, .to = 1, .from = r->peers[i], .term = 1, .reject = false, .index = 2 };
        raft_step_remote(r, &ack);
    }
    raft_advance_all_for_tests_only(r);
}

MACRO_TEST(read_index_single_node_completes_immediately) {
    uint64_t peers[] = {1};
    raft_t* r = raft_create(1, peers, 0); // 0 remote peers

    boot_leader_and_unlock_reads(r);

    raft_msg_t ri = { .type = MSG_READ_INDEX, .read_seq = 100 };
    raft_step_local(r, &ri);

    // Because quorum is 1, it should instantly bypass the queue and populate read_states
    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 1);
    MACRO_ASSERT_EQ_INT(ready.read_states[0].read_seq, 100);
    MACRO_ASSERT_EQ_INT(ready.read_states[0].index, 2);

    raft_destroy(r);
}

MACRO_TEST(read_index_rejected_if_leader_has_no_current_term_commit) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_step_local(r, &hup);
    raft_msg_t pv = { .type = MSG_PRE_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &pv);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .to = 1, .from = 2, .term = 1, .reject = false };
    raft_step_remote(r, &vote);
    raft_advance_all_for_tests_only(r);

    MACRO_ASSERT_TRUE(raft_state(r) == RAFT_STATE_LEADER);

    // We intentionally skip committing the no-op entry here!

    raft_msg_t ri = { .type = MSG_READ_INDEX, .read_seq = 100 };
    raft_step_local(r, &ri);

    // Read should be cleanly ignored to prevent a stale read
    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 0);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 0);

    raft_destroy(r);
}

MACRO_TEST(read_index_rejected_if_self_is_learner) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);
    boot_leader_and_unlock_reads(r);

    r->is_learner_self = true; // Demote leader to learner dynamically

    raft_msg_t ri = { .type = MSG_READ_INDEX, .read_seq = 100 };
    raft_step_local(r, &ri);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 0);

    raft_destroy(r);
}

MACRO_TEST(read_index_rejected_if_self_is_removed) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);
    boot_leader_and_unlock_reads(r);

    r->removed = true; // Logically remove leader

    raft_msg_t ri = { .type = MSG_READ_INDEX, .read_seq = 100 };
    raft_step_local(r, &ri);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 0);

    raft_destroy(r);
}

MACRO_TEST(read_index_seq_overflow_sets_fatal_error) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);
    boot_leader_and_unlock_reads(r);

    r->current_read_seq = UINT64_MAX; // Push sequence to absolute mathematical limit

    raft_msg_t ri = { .type = MSG_READ_INDEX, .read_seq = 100 };
    raft_step_local(r, &ri);

    // The engine must refuse to wrap around to 0
    MACRO_ASSERT_TRUE(raft_has_fatal_error(r));

    raft_destroy(r);
}

MACRO_TEST(read_index_pending_queue_full_rejects_or_drops_observably) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);
    boot_leader_and_unlock_reads(r);

    // Fill the internal pending read queue
    for (int i = 0; i < 200; i++) {
        raft_msg_t ri = { .type = MSG_READ_INDEX, .read_seq = i };
        raft_step_local(r, &ri);
    }

    // Must not segfault or overflow. The reads exceeding MAX_PENDING_READS are safely dropped.
    MACRO_ASSERT_FALSE(raft_has_fatal_error(r));

    raft_destroy(r);
}

MACRO_TEST(read_index_response_without_pending_request_ignored) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    r->state = RAFT_STATE_FOLLOWER;
    r->leader_id = 2;
    r->current_term = 1;

    // Follower receives a response to a read it never asked for
    raft_msg_t res = { .type = MSG_READ_INDEX_RES, .to = 1, .from = 2, .term = 1, .read_seq = 500, .index = 5, .reject = false };
    raft_step_remote(r, &res);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 0); // Strict routing ignores it

    raft_destroy(r);
}

MACRO_TEST(read_index_response_from_non_leader_ignored) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    r->state = RAFT_STATE_FOLLOWER;
    r->leader_id = 2;
    r->current_term = 1;

    // Follower queues a local read, but gets answered by Node 3 (not the leader)
    r->pending_reads[0].active = true;
    r->pending_reads[0].client_ctx = 500;

    raft_msg_t res = { .type = MSG_READ_INDEX_RES, .to = 1, .from = 3, .term = 1, .read_seq = 500, .index = 5, .reject = false };
    raft_step_remote(r, &res);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 0);

    raft_destroy(r);
}

MACRO_TEST(read_index_response_wrong_term_ignored) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    r->state = RAFT_STATE_FOLLOWER;
    r->leader_id = 2;
    r->current_term = 2;

    r->pending_reads[0].active = true;
    r->pending_reads[0].client_ctx = 500;

    // Stale term response
    raft_msg_t res = { .type = MSG_READ_INDEX_RES, .to = 1, .from = 2, .term = 1, .read_seq = 500, .index = 5, .reject = false };
    raft_step_remote(r, &res);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 0);

    raft_destroy(r);
}

MACRO_TEST(read_index_response_rejected_ignored) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    r->state = RAFT_STATE_FOLLOWER;
    r->leader_id = 2;
    r->current_term = 1;

    r->pending_reads[0].active = true;
    r->pending_reads[0].client_ctx = 500;

    // Leader explicitly rejected the read
    raft_msg_t res = { .type = MSG_READ_INDEX_RES, .to = 1, .from = 2, .term = 1, .read_seq = 500, .index = 5, .reject = true };
    raft_step_remote(r, &res);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 0);

    raft_destroy(r);
}

MACRO_TEST(forwarded_read_index_queues_locally_on_response) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);

    r->state = RAFT_STATE_FOLLOWER;
    r->leader_id = 2;
    r->current_term = 1;

    // We manually simulate the follower forwarding the packet...
    r->pending_reads[0].active = true;
    r->pending_reads[0].client_ctx = 1234;

    // ...and now the valid response comes back.
    raft_msg_t res = { .type = MSG_READ_INDEX_RES, .to = 1, .from = 2, .term = 1, .read_seq = 1234, .index = 5, .reject = false };
    raft_step_remote(r, &res);

    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 1);
    MACRO_ASSERT_EQ_INT(ready.read_states[0].read_seq, 1234);
    MACRO_ASSERT_EQ_INT(ready.read_states[0].index, 5);

    // Ensure the pending slot was cleared for reuse
    MACRO_ASSERT_FALSE(r->pending_reads[0].active);

    raft_destroy(r);
}

MACRO_TEST(rejected_append_res_does_not_satisfy_readindex) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);
    boot_leader_and_unlock_reads(r);

    raft_msg_t ri = { .type = MSG_READ_INDEX, .read_seq = 100 };
    raft_step_local(r, &ri);

    // Node 2 rejects the heartbeat
    raft_msg_t rej = { .type = MSG_APPEND_RES, .to = 1, .from = 2, .term = 1, .reject = true, .index = 1, .conflict_index = 0 };
    raft_step_remote(r, &rej);

    // A rejection proves the node is alive (maintaining leader authority),
    // BUT it does NOT acknowledge the ReadIndex sequence! The read remains pending.
    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 0);

    raft_destroy(r);
}

MACRO_TEST(learner_append_res_does_not_count_toward_readindex_quorum) {
    uint64_t peers[] = {2, 3};
    raft_t* r = raft_create(1, peers, 2);
    raft_add_learner(r, 4); // Add node 4 as a learner
    boot_leader_and_unlock_reads(r);

    raft_msg_t ri = { .type = MSG_READ_INDEX, .read_seq = 100 };
    raft_step_local(r, &ri);
    raft_advance_all_for_tests_only(r); // clear outbound queue

    // The learner acks the heartbeat with the read_seq included
    raft_msg_t ack = { .type = MSG_APPEND_RES, .to = 1, .from = 4, .term = 1, .reject = false, .index = 2, .read_seq = 1 };
    raft_step_remote(r, &ack);

    // Learner acks mathematically cannot bridge quorum
    raft_ready_t ready = raft_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_read_states, 0);

    raft_destroy(r);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, read_index_single_node_completes_immediately);
    MACRO_ADD(tests, read_index_rejected_if_leader_has_no_current_term_commit);
    MACRO_ADD(tests, read_index_rejected_if_self_is_learner);
    MACRO_ADD(tests, read_index_rejected_if_self_is_removed);
    MACRO_ADD(tests, read_index_seq_overflow_sets_fatal_error);
    MACRO_ADD(tests, read_index_pending_queue_full_rejects_or_drops_observably);
    MACRO_ADD(tests, read_index_response_without_pending_request_ignored);
    MACRO_ADD(tests, read_index_response_from_non_leader_ignored);
    MACRO_ADD(tests, read_index_response_wrong_term_ignored);
    MACRO_ADD(tests, read_index_response_rejected_ignored);
    MACRO_ADD(tests, forwarded_read_index_queues_locally_on_response);
    MACRO_ADD(tests, rejected_append_res_does_not_satisfy_readindex);
    MACRO_ADD(tests, learner_append_res_does_not_count_toward_readindex_quorum);

    macro_run_all("raft_read_index", tests, test_count);
    return 0;
}
