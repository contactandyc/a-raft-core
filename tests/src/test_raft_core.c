// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <string.h>
#include "a-raft-library/raft_core.h"
#include "the-macro-library/macro_test.h"

// --- 1. BASIC ELECTION & LEADER STATE ---

MACRO_TEST(raft_initial_state) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 0);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 0); // Index 0 is dummy

    raft_core_destroy(r);
}

MACRO_TEST(raft_campaign_becomes_candidate) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_CANDIDATE);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 1);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 2);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_REQUEST_VOTE);

    raft_core_destroy(r);
}

MACRO_TEST(raft_win_election_and_noop) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_core_advance(r);

    // Peer 2 grants vote. (1 self + 1 peer = majority of 3)
    raft_msg_t vote1 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote1);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    raft_ready_t ready = raft_core_get_ready(r);
    // Leader MUST append a No-Op entry to its log immediately upon winning
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 2);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_ENTRIES);

    raft_core_destroy(r);
}

// --- 2. LOG MATCHING & TRUNCATION SAFETY ---

MACRO_TEST(raft_follower_rejects_log_gaps) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    uint8_t payload[] = "data";
    raft_entry_t entry = { .term = 1, .index = 5, .data = payload, .data_len = 4 };

    // Leader tries to append index 5, but Follower only has up to index 0
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 4, .log_term = 1, .entries = &entry, .num_entries = 1, .commit = 0 };

    raft_core_step(r, &app1);

    // Log should remain empty (except for dummy 0)
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 0);

    // Should respond with a rejection
    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_RES);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_truncates_on_conflict) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    // Append index 1 from Term 1
    raft_entry_t entry1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &entry1, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app1);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);
    raft_core_advance(r);

    // New Leader (Term 2) sends a conflicting index 1
    raft_entry_t conflict = { .term = 2, .index = 1, .data = (uint8_t*)"B", .data_len = 1 };
    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .from = 3, .term = 2,
                        .index = 0, .log_term = 0, .entries = &conflict, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app2);

    // Log length stays 1, but it must have been replaced (success response)
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);
    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == false);

    raft_core_destroy(r);
}

// --- 3. LEADER STEP-DOWN (TERM SUPREMACY) ---

MACRO_TEST(raft_leader_steps_down_on_higher_term) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    // Force leadership
    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);
    raft_core_advance(r);

    // Receive AppendEntries from a Leader in Term 2
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 3, .term = 2,
                       .index = 0, .log_term = 0, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_core_step(r, &app);

    // Must revert to follower
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 2);

    raft_core_destroy(r);
}

// --- 4. ELECTION SAFETY (LOG UP-TO-DATE CHECK) ---

MACRO_TEST(raft_voter_rejects_stale_candidate) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    // Give Node 1 a log entry in Term 2
    raft_entry_t entry = { .term = 2, .index = 1, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 2,
                       .index = 0, .log_term = 0, .entries = &entry, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app);
    raft_core_advance(r);

    // Node 3 campaigns in Term 3, but its log is empty (last_term = 0, last_index = 0)
    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .from = 3, .term = 3,
                       .index = 0, .log_term = 0 };
    raft_core_step(r, &req);

    // Node 1 should update its term to 3, but REJECT the vote because its own log is newer
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 3);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_REQUEST_VOTE_RES);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);

    raft_core_destroy(r);
}

// --- 5. THE FIGURE 8 ANOMALY (COMMIT SAFETY) ---

MACRO_TEST(raft_figure_8_anomaly_prevention) {
    uint64_t peers[] = {2, 3, 4, 5}; // 5 node cluster, majority = 3
    raft_core_t* r = raft_core_create(1, peers, 4);

    // 1. Force Node 1 to be Leader in Term 1
    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    for (int i = 2; i <= 3; i++) {
        raft_msg_t v = { .type = MSG_REQUEST_VOTE_RES, .from = i, .term = 1, .reject = false };
        raft_core_step(r, &v);
    }
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);
    raft_core_advance(r);

    // 2. Propose an entry. Log now has: [Idx 1, Term 1 (No-Op)], [Idx 2, Term 1 (Data)]
    uint8_t data[] = "data";
    raft_entry_t entry = { .data = data, .data_len = 4 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &entry, .num_entries = 1 };
    raft_core_step(r, &prop);

    // 3. Node 1 is deposed, becomes Leader again in Term 4.
    // (Simulate by forcing a high term and winning election again)
    raft_msg_t app_fake = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 3,
                            .index = 0, .log_term = 0, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_core_step(r, &app_fake); // Become follower in term 3

    raft_msg_t hup2 = { .type = MSG_HUP };
    raft_core_step(r, &hup2); // Candidate for Term 4
    for (int i = 2; i <= 3; i++) {
        raft_msg_t v = { .type = MSG_REQUEST_VOTE_RES, .from = i, .term = 4, .reject = false };
        raft_core_step(r, &v);
    }
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    // At this point, Log is:
    // Idx 1: Term 1 (No-Op)
    // Idx 2: Term 1 (Data)
    // Idx 3: Term 4 (No-Op)
    raft_core_advance(r);

    // 4. Node 2 acknowledges Idx 2 (Term 1 Data).
    // Leader (1) + Node 2 = 2 nodes. We need 3 for a majority.
    raft_msg_t ack1 = { .type = MSG_APPEND_RES, .from = 2, .term = 4, .reject = false, .index = 2 };
    raft_core_step(r, &ack1);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0); // No majority yet

    // 5. Node 3 acknowledges Idx 2 (Term 1 Data).
    // Now Leader (1) + Node 2 + Node 3 = 3 nodes (MAJORITY).
    raft_msg_t ack2 = { .type = MSG_APPEND_RES, .from = 3, .term = 4, .reject = false, .index = 2 };
    raft_core_step(r, &ack2);

    // CRITICAL RAFT RULE: The leader cannot commit Index 2 yet! It is from Term 1,
    // but the leader is in Term 4. Counting replicas for older terms is unsafe (Figure 8).
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0);

    // 6. Node 3 catches up fully and acknowledges Index 3 (Term 4 No-Op).
    raft_msg_t ack3 = { .type = MSG_APPEND_RES, .from = 3, .term = 4, .reject = false, .index = 3 };
    raft_core_step(r, &ack3);

    // FIXED: Node 2 also catches up and acknowledges Index 3.
    // Now Leader + Node 3 + Node 2 = 3 nodes having Index 3 (MAJORITY!)
    raft_msg_t ack4 = { .type = MSG_APPEND_RES, .from = 2, .term = 4, .reject = false, .index = 3 };
    raft_core_step(r, &ack4);

    // NOW the leader can commit Index 3 (which is from its current term),
    // which implicitly and safely commits Index 2 along with it.
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 3);

    raft_core_destroy(r);
}

// --- 6. LEADER RECOVERY / NEXT_INDEX BACKTRACKING ---

MACRO_TEST(raft_leader_backtracks_next_index_on_reject) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    // Force leadership
    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance(r);

    // Propose 2 client entries
    uint8_t data[] = "X";
    raft_entry_t entry = { .data = data, .data_len = 1 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &entry, .num_entries = 1 };
    raft_core_step(r, &prop); // Idx 2
    raft_core_step(r, &prop); // Idx 3
    raft_core_advance(r);

    // Node 2 has been offline. It rejects the AppendEntries because it's missing Index 1.
    raft_msg_t rej = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = true, .index = 3 };
    raft_core_step(r, &rej);

    // Leader should decrement next_index for Node 2 and send a new AppendEntries message
    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_ENTRIES);

    // The previous index sent was 3. After reject, it should decrement to try sending from index 2
    MACRO_ASSERT_EQ_INT(ready.messages[0].index, 2);

    raft_core_destroy(r);
}

// --- 7. NEW: REVIEWER REQUESTED TESTS ---

MACRO_TEST(raft_leader_ignores_lower_term_append) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    // Become Leader in Term 2
    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_core_step(r, &hup); // Term 2
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance(r);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    // Receive AppendEntries from Term 1
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 3, .term = 1 };
    raft_core_step(r, &app);

    // Assert we stay leader in term 2, and rejected it
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 2);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_RES);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);
    MACRO_ASSERT_EQ_INT(ready.messages[0].term, 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_candidate_ignores_stale_vote_response) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_core_step(r, &hup); // Term 2 Candidate
    raft_core_advance(r);

    // Vote response from term 1
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);

    // Should NOT become leader
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_CANDIDATE);

    raft_core_destroy(r);
}

MACRO_TEST(raft_candidate_does_not_double_count_vote) {
    uint64_t peers[] = {2, 3, 4}; // 4 nodes, maj = 3
    raft_core_t* r = raft_core_create(1, peers, 3);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_core_advance(r);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };

    raft_core_step(r, &vote);
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_CANDIDATE); // 2 votes (Self + 2), need 3

    raft_core_step(r, &vote); // Send duplicate
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_CANDIDATE); // Still Candidate!

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_rejects_second_candidate_same_term) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t req2 = { .type = MSG_REQUEST_VOTE, .from = 2, .term = 1, .index = 0, .log_term = 0 };
    raft_core_step(r, &req2);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == false);
    raft_core_advance(r);

    raft_msg_t req3 = { .type = MSG_REQUEST_VOTE, .from = 3, .term = 1, .index = 0, .log_term = 0 };
    raft_core_step(r, &req3);

    ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject == true);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_updates_commit_from_heartbeat) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    // Leader appends 2 entries
    raft_entry_t entry1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t entry2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };
    raft_entry_t batch[] = {entry1, entry2};

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = batch, .num_entries = 2, .commit = 0 };
    raft_core_step(r, &app);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0);
    raft_core_advance(r);

    // Heartbeat updates commit to 2
    raft_msg_t hb = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                      .index = 2, .log_term = 1, .entries = NULL, .num_entries = 0, .commit = 2 };
    raft_core_step(r, &hb);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 2);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_appends_multiple_entries) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };
    raft_entry_t e3 = { .term = 1, .index = 3, .data = (uint8_t*)"3", .data_len = 1 };
    raft_entry_t batch[] = {e1, e2, e3};

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = batch, .num_entries = 3, .commit = 0 };

    raft_core_step(r, &app);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 3); // Advanced by 3

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_backtracks_multiple_times) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance(r);

    // Propose 2 times. Log ends at Idx 3.
    raft_entry_t d = { .data = NULL, .data_len = 0 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &d, .num_entries = 1 };
    raft_core_step(r, &p); // 2
    raft_core_step(r, &p); // 3
    raft_core_advance(r);

    // First reject
    raft_msg_t r1 = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = true, .index = 3 };
    raft_core_step(r, &r1);
    raft_ready_t rd1 = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(rd1.messages[0].index, 2);
    raft_core_advance(r);

    // Second reject
    raft_msg_t r2 = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = true, .index = 2 };
    raft_core_step(r, &r2);
    raft_ready_t rd2 = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(rd2.messages[0].index, 1);
    raft_core_advance(r);

    // Third reject
    raft_msg_t r3 = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = true, .index = 1 };
    raft_core_step(r, &r3);
    raft_ready_t rd3 = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(rd3.messages[0].index, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_advance_clears_messages_and_committed_entries) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    // Hup generates request votes
    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(ready.num_messages > 0);

    raft_core_advance(r);
    ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 0); // Cleared!

    raft_core_destroy(r);
}

MACRO_TEST(raft_candidate_steps_down_on_same_term_append) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_CANDIDATE);

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_core_step(r, &app);

    // Assert fallback to follower
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);

    raft_core_destroy(r);
}

MACRO_TEST(raft_single_node_becomes_leader_and_commits_noop) {
    uint64_t* peers = NULL;
    raft_core_t* r = raft_core_create(1, peers, 0); // 0 peers

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);

    // Should commit its own No-Op immediately
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);

    raft_core_destroy(r);
}

// --- 8. NEW: ADVANCED EDGE CASES & IDEMPOTENCY ---

MACRO_TEST(raft_follower_conflict_replacement_multiple_entries) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    // Follower log: 1:"A", 2:"B", 3:"C" all term 1
    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_entry_t e3 = { .term = 1, .index = 3, .data = (uint8_t*)"C", .data_len = 1 };
    raft_entry_t batch1[] = { e1, e2, e3 };

    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = batch1, .num_entries = 3, .commit = 0 };
    raft_core_step(r, &app1);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 3);
    raft_core_advance(r);

    // Leader sends prev_index=1, prev_term=1. Entries 2:"X", 3:"Y" term 2
    raft_entry_t x2 = { .term = 2, .index = 2, .data = (uint8_t*)"X", .data_len = 1 };
    raft_entry_t x3 = { .term = 2, .index = 3, .data = (uint8_t*)"Y", .data_len = 1 };
    raft_entry_t batch2[] = { x2, x3 };

    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 2,
                        .index = 1, .log_term = 1, .entries = batch2, .num_entries = 2, .commit = 0 };
    raft_core_step(r, &app2);

    // Expected: truncates from 2, appends X, Y. last_index = 3.
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 3);
    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_EQ_INT(ready.messages[0].index, 3);
    MACRO_ASSERT_FALSE(ready.messages[0].reject);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_duplicate_append_is_idempotent) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };

    raft_core_step(r, &app);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);
    raft_core_advance(r);

    // Send exactly the same again
    raft_core_step(r, &app);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1); // Not duplicated

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_FALSE(ready.messages[0].reject);
    MACRO_ASSERT_EQ_INT(ready.messages[0].index, 1);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_ignores_propose) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };

    raft_core_step(r, &p);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 0);
    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_candidate_does_not_count_rejected_vote) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_core_advance(r);

    raft_msg_t rej = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = true };
    raft_core_step(r, &rej);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_CANDIDATE);

    raft_msg_t acc = { .type = MSG_REQUEST_VOTE_RES, .from = 3, .term = 1, .reject = false };
    raft_core_step(r, &acc);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_ignores_unknown_peer_append_res) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t v1 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &v1);
    raft_core_advance(r);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(r, &p); // Proposes index 2
    raft_core_advance(r);

    // Unknown peer 99 sends ACK
    raft_msg_t ack = { .type = MSG_APPEND_RES, .from = 99, .term = 1, .reject = false, .index = 2 };
    raft_core_step(r, &ack);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0); // No change

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_ignores_stale_append_res) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup); // Term 1
    raft_core_step(r, &hup); // Term 2
    raft_msg_t v1 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_core_step(r, &v1); // Leader term 2
    raft_core_advance(r);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(r, &p); // Proposes index 2 (term 2)
    raft_core_advance(r);

    // Stale ACK from term 1
    raft_msg_t ack = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_core_step(r, &ack);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0); // Ignored

    raft_core_destroy(r);
}

MACRO_TEST(raft_candidate_steps_down_on_higher_term_vote_res) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup); // Candidate term 1

    raft_msg_t v1 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_core_step(r, &v1);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_commit_never_decreases) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 1 };
    raft_core_step(r, &app);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);
    raft_core_advance(r);

    // Heartbeat with commit=0
    raft_msg_t hb = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                      .index = 1, .log_term = 1, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_core_step(r, &hb);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1); // Still 1

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_commit_clamped_to_last_index) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 99 };
    raft_core_step(r, &app);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1); // Clamped to 1, not 99

    raft_core_destroy(r);
}

MACRO_TEST(raft_apply_clears_committed_entries) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };
    raft_entry_t batch[] = { e1, e2 };

    raft_msg_t app = {
        .type = MSG_APPEND_ENTRIES,
        .from = 2, .term = 1, .index = 0, .log_term = 0,
        .entries = batch, .num_entries = 2, .commit = 2
    };

    raft_core_step(r, &app);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 2);

    // NEW API CONTRACT: We must explicitly consume them!
    raft_core_apply(r);
    raft_core_advance(r);

    ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 0);

    raft_core_destroy(r);
}

// --- 9. NEW: DEFENSIVE PROGRAMMING & API CONTRACTS ---

MACRO_TEST(raft_ignores_messages_not_addressed_to_self) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = 3, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = NULL, .num_entries = 0, .commit = 0 };

    raft_core_step(r, &app);

    MACRO_ASSERT_EQ_INT(raft_core_term(r), 0);
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_ignores_hup) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 1);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1); // Only 1 no-op

    raft_core_advance(r);
    raft_core_step(r, &hup); // Should be ignored

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 1);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1); // Still 1

    raft_core_destroy(r);
}

MACRO_TEST(raft_candidate_steps_down_on_higher_term_request_vote) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_core_advance(r);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .from = 2, .term = 2, .index = 0, .log_term = 0 };
    raft_core_step(r, &req);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 2);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_REQUEST_VOTE_RES);
    MACRO_ASSERT_FALSE(ready.messages[0].reject); // Granted the vote

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_steps_down_on_higher_term_request_vote) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance(r);

    raft_msg_t req = { .type = MSG_REQUEST_VOTE, .from = 3, .term = 2, .index = raft_core_last_index(r), .log_term = 1 };
    raft_core_step(r, &req);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_steps_down_on_higher_term_append_res) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);

    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    raft_msg_t res = { .type = MSG_APPEND_RES, .from = 2, .term = 2, .reject = true };
    raft_core_step(r, &res);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_FOLLOWER);
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_rejects_append_entries_with_null_entries_and_count) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = NULL, .num_entries = 1, .commit = 0 }; // Malformed
    raft_core_step(r, &app);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 0);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_rejects_empty_proposal) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance(r);

    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = NULL, .num_entries = 0 }; // Malformed
    raft_core_step(r, &prop);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1); // No change

    raft_core_destroy(r);
}

MACRO_TEST(raft_ready_populates_entries_to_save) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    // Leader creation appends a no-op
    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 1); // The no-op

    raft_core_advance(r); // Simulates writing to disk

    ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 0); // Cleared

    raft_core_destroy(r);
}

MACRO_TEST(raft_committed_entry_content_is_correct) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"HELLO", .data_len = 5 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 1 };

    raft_core_step(r, &app);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 1);
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].index, 1);
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].term, 1);
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].data_len, 5);
    MACRO_ASSERT_TRUE(memcmp(ready.committed_entries[0].data, "HELLO", 5) == 0);

    raft_core_destroy(r);
}

// --- 10. NEW: TIGHTENED CONTRACTS & BATCHING ---

MACRO_TEST(raft_ready_populates_entries_to_save_on_follower_append) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };

    raft_core_step(r, &app);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 1);
    MACRO_ASSERT_EQ_INT(ready.entries_to_save[0].index, 1);
    MACRO_ASSERT_EQ_INT(ready.entries_to_save[0].term, 1);

    raft_core_advance(r);
    ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_ignores_zero_entry_proposal_even_with_entries_pointer) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance(r);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 0 }; // num_entries = 0

    raft_core_step(r, &p);
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1); // Only No-Op exists

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_appends_multiple_proposed_entries) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance(r);

    raft_entry_t e1 = { .data = (uint8_t*)"X", .data_len = 1 };
    raft_entry_t e2 = { .data = (uint8_t*)"Y", .data_len = 1 };
    raft_entry_t batch[] = { e1, e2 };

    raft_msg_t p = { .type = MSG_PROPOSE, .entries = batch, .num_entries = 2 };
    raft_core_step(r, &p);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 3); // No-Op + X + Y

    raft_core_destroy(r);
}

MACRO_TEST(raft_ready_returns_only_newly_committed_entries_after_apply) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };

    // Commit Index 1
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &e1, .num_entries = 1, .commit = 1 };
    raft_core_step(r, &app1);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 1);
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].index, 1);

    // NEW API CONTRACT: Consume the first batch
    raft_core_apply(r);
    raft_core_advance(r);

    // Commit Index 2
    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 1, .log_term = 1, .entries = &e2, .num_entries = 1, .commit = 2 };
    raft_core_step(r, &app2);

    ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_committed_entries, 1); // ONLY entry 2
    MACRO_ASSERT_EQ_INT(ready.committed_entries[0].index, 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_duplicate_append_with_higher_commit_advances_commit) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app1);
    raft_core_advance(r);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0);

    // Duplicate append, but now leader says it's committed
    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 1 };
    raft_core_step(r, &app2);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1); // Log didn't grow
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1); // But commit advanced

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_rejects_wrong_prev_log_term) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = &e1, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app1);
    raft_core_advance(r);

    // Leader sends PrevIndex = 1, but says PrevTerm = 2 (Mismatch!)
    raft_entry_t e2 = { .term = 2, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 2,
                        .index = 1, .log_term = 2, .entries = &e2, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app2);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1); // Log rejected
    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);

    raft_core_destroy(r);
}

MACRO_TEST(raft_append_reject_reports_last_index) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_entry_t batch[] = { e1, e2 };

    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = batch, .num_entries = 2, .commit = 0 };
    raft_core_step(r, &app1);
    raft_core_advance(r);

    // Gap in log (Leader sends Index 5)
    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 4, .log_term = 1, .entries = NULL, .num_entries = 0, .commit = 0 };
    raft_core_step(r, &app2);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);
    // Must tell leader we only have up to index 2
    MACRO_ASSERT_EQ_INT(ready.messages[0].index, 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_create_rejects_too_many_peers_and_self) {
    uint64_t valid_peers[] = {2, 3};
    uint64_t invalid_self_peers[] = {1, 2}; // Includes own ID (1)

    raft_core_t* r1 = raft_core_create(1, invalid_self_peers, 2);
    MACRO_ASSERT_TRUE(r1 == NULL);

    uint64_t too_many_peers[100] = {0};
    raft_core_t* r2 = raft_core_create(1, too_many_peers, 100);
    MACRO_ASSERT_TRUE(r2 == NULL);
}

MACRO_TEST(raft_candidate_ignores_unknown_peer_vote_response) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_core_advance(r);

    // Vote from unknown node 99
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 99, .term = 1, .reject = false };
    raft_core_step(r, &vote);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_CANDIDATE); // Still Candidate

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_heartbeat_clamps_commit_to_prev_index) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"1", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"2", .data_len = 1 };
    raft_entry_t batch[] = { e1, e2 };

    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                        .index = 0, .log_term = 0, .entries = batch, .num_entries = 2, .commit = 0 };
    raft_core_step(r, &app1);
    raft_core_advance(r);

    // Heartbeat says commit is 5, but ONLY proves the leader has index 1.
    // The follower MUST clamp commit to 1, not its local max of 2.
    raft_msg_t hb = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                      .index = 1, .log_term = 1, .entries = NULL, .num_entries = 0, .commit = 5 };
    raft_core_step(r, &hb);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);

    raft_core_destroy(r);
}

// --- 11. NEW: FINAL COMPLETENESS BATCH ---

MACRO_TEST(raft_leader_backoff_floor) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance(r);

    // Send 3 rejections to force it to floor at index 1
    raft_msg_t rj = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = true, .index = 99 };
    raft_core_step(r, &rj); // Drops to 1
    raft_core_step(r, &rj); // Drops to 1 (Floors here)
    raft_core_step(r, &rj); // Still 1

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(ready.num_messages > 0);
    // Prev_index must safely floor at 0 (meaning next_index floored safely at 1)
    MACRO_ASSERT_EQ_INT(ready.messages[ready.num_messages - 1].index, 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_ignores_out_of_bounds_match_index) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance(r);

    // Leader log_len is 2 (Index 0 dummy + Index 1 No-Op).
    // Peer sends malicious/broken ack for index 999.
    raft_msg_t ack = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 999 };
    raft_core_step(r, &ack);

    // FIXED: The core safely drops malicious OOB success messages.
    // Therefore, the quorum is not reached, and commit_index stays 0.
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_mixed_conflict_batch) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    // Setup: Follower has A_t1, B_t1, C_t1
    raft_entry_t e1 = { .term = 1, .index = 1, .data = (uint8_t*)"A", .data_len = 1 };
    raft_entry_t e2 = { .term = 1, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_entry_t e3 = { .term = 1, .index = 3, .data = (uint8_t*)"C", .data_len = 1 };
    raft_entry_t b1[] = { e1, e2, e3 };
    raft_msg_t a1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 0, .log_term = 0, .entries = b1, .num_entries = 3 };
    raft_core_step(r, &a1);
    raft_core_advance(r);

    // Leader sends: B_t1, C_t1, D_t2. (B and C match identically).
    raft_entry_t nx2 = { .term = 1, .index = 2, .data = (uint8_t*)"B", .data_len = 1 };
    raft_entry_t nx3 = { .term = 1, .index = 3, .data = (uint8_t*)"C", .data_len = 1 };
    raft_entry_t nx4 = { .term = 2, .index = 4, .data = (uint8_t*)"D", .data_len = 1 };
    raft_entry_t b2[] = { nx2, nx3, nx4 };

    // Send from prev_idx = 1
    raft_msg_t a2 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 2, .index = 1, .log_term = 1, .entries = b2, .num_entries = 3 };
    raft_core_step(r, &a2);

    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 4);

    // The core should only issue D to be saved, because B and C matched and were not truncated
    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_entries_to_save, 1);
    MACRO_ASSERT_EQ_INT(ready.entries_to_save[0].index, 4);

    raft_core_destroy(r);
}

MACRO_TEST(raft_candidate_split_vote_recampaign) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup); // Term 1
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 1);

    // Simulating election timeout before quorum
    raft_core_step(r, &hup); // Term 2
    MACRO_ASSERT_EQ_INT(raft_core_term(r), 2);

    // Vote from peer should now apply to term 2 and win
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 2, .reject = false };
    raft_core_step(r, &vote);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_requires_peer_ack_to_commit_noop) {
    uint64_t peers[] = {2}; // 2 node cluster
    raft_core_t* r = raft_core_create(1, peers, 1);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);

    MACRO_ASSERT_TRUE(raft_core_state(r) == RAFT_STATE_LEADER);

    // Assert No-Op is appended but NOT committed yet
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0);

    raft_core_destroy(r);
}

// Minimal Multi-Node Integration Harness
MACRO_TEST(raft_multi_node_smoke_test) {
    uint64_t peers1[] = {2, 3};
    uint64_t peers2[] = {1, 3};
    uint64_t peers3[] = {1, 2};

    raft_core_t* nodes[3] = {
        raft_core_create(1, peers1, 2),
        raft_core_create(2, peers2, 2),
        raft_core_create(3, peers3, 2)
    };

    // Node 1 becomes leader
    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(nodes[0], &hup);

    // Very simple message router loop
    bool active = true;
    int cycles = 0;
    while (active && cycles < 10) {
        active = false;
        cycles++;

        for (int i = 0; i < 3; i++) {
            raft_ready_t rd = raft_core_get_ready(nodes[i]);
            for (size_t m = 0; m < rd.num_messages; m++) {
                active = true;
                int target = rd.messages[m].to - 1;
                raft_core_step(nodes[target], &rd.messages[m]);
            }
            raft_core_advance(nodes[i]);
        }
    }

    MACRO_ASSERT_TRUE(raft_core_state(nodes[0]) == RAFT_STATE_LEADER);
    MACRO_ASSERT_TRUE(raft_core_state(nodes[1]) == RAFT_STATE_FOLLOWER);

    // Propose to Leader
    raft_entry_t e = { .data = (uint8_t*)"Hello", .data_len = 5 };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(nodes[0], &p);

    // Route traffic for the proposal and Acks
    active = true;
    while (active) {
        active = false;
        for (int i = 0; i < 3; i++) {
            raft_ready_t rd = raft_core_get_ready(nodes[i]);
            for (size_t m = 0; m < rd.num_messages; m++) {
                active = true;
                int target = rd.messages[m].to - 1;
                raft_core_step(nodes[target], &rd.messages[m]);
            }
            raft_core_advance(nodes[i]);
        }
    }

    // FIXED: The Leader committed the data, but needs a heartbeat to broadcast the new commit index to followers.
    raft_msg_t tick = { .type = MSG_TICK };
    raft_core_step(nodes[0], &tick);

    // Route the heartbeats
    active = true;
    while (active) {
        active = false;
        for (int i = 0; i < 3; i++) {
            raft_ready_t rd = raft_core_get_ready(nodes[i]);
            for (size_t m = 0; m < rd.num_messages; m++) {
                active = true;
                int target = rd.messages[m].to - 1;
                raft_core_step(nodes[target], &rd.messages[m]);
            }
            raft_core_advance(nodes[i]);
        }
    }

    // Assert cluster fully synced and committed
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(nodes[0]), 2);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(nodes[1]), 2);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(nodes[2]), 2);

    raft_core_destroy(nodes[0]);
    raft_core_destroy(nodes[1]);
    raft_core_destroy(nodes[2]);
}

// --- 12. NEW: DYNAMIC CONFIGURATION CHANGES ---

MACRO_TEST(raft_conf_change_applies_only_on_commit) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    uint64_t new_node = 4;
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_CONF_ADD, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };

    // Uncommitted append
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 0, .log_term = 0,
                       .entries = &e, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app);

    // Verify peers have NOT changed yet
    uint64_t active_peers[16];
    size_t num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);

    // Leader commits the entry
    raft_msg_t hb = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 1, .log_term = 1,
                       .entries = NULL, .num_entries = 0, .commit = 1 };
    raft_core_step(r, &hb);

    // Explicit application cycle
    raft_core_apply(r);
    raft_core_advance(r);

    // Verify peer was finally added
    num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 3);
    MACRO_ASSERT_EQ_INT(active_peers[2], 4);

    raft_core_destroy(r);
}

MACRO_TEST(raft_conf_truncate_uncommitted_config_is_safe) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    uint64_t new_node = 4;
    raft_entry_t e = { .term = 1, .index = 1, .type = ENTRY_CONF_ADD, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1,
                       .index = 0, .log_term = 0, .entries = &e, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app);

    // Force conflict/truncation
    raft_entry_t conflict = { .term = 2, .index = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"X", .data_len = 1 };
    raft_msg_t overwrite = { .type = MSG_APPEND_ENTRIES, .from = 3, .term = 2,
                             .index = 0, .log_term = 0, .entries = &conflict, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &overwrite);

    // Since it was never committed, truncation is harmless and peers remain 2
    uint64_t active_peers[16];
    size_t num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);

    raft_core_destroy(r);
}

MACRO_TEST(raft_conf_add_node_applies_on_commit) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    // Initial check
    uint64_t active_peers[16];
    size_t num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);

    // Become leader
    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance(r);

    // Propose adding Node 4
    uint64_t new_node = 4;
    raft_entry_t e = { .type = ENTRY_CONF_ADD, .data = (uint8_t*)&new_node, .data_len = sizeof(uint64_t) };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(r, &p);

    // NEW API CONTRACT: Assert the peer is NOT added yet (Uncommitted)
    num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);

    // Node 2 ACKs the proposal (Index 2, because Index 1 is the leader No-Op)
    raft_msg_t ack = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_core_step(r, &ack);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 2);

    // Explicitly apply state
    raft_core_apply(r);
    raft_core_advance(r);

    // NOW the peer should be added!
    num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 3);
    MACRO_ASSERT_EQ_INT(active_peers[2], 4);

    raft_core_destroy(r);
}

MACRO_TEST(raft_conf_remove_node_applies_on_commit) {
    uint64_t peers[] = {2, 3, 4};
    raft_core_t* r = raft_core_create(1, peers, 3);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote1 = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_msg_t vote2 = { .type = MSG_REQUEST_VOTE_RES, .from = 3, .term = 1, .reject = false };
    raft_core_step(r, &vote1);
    raft_core_step(r, &vote2);
    raft_core_advance(r);

    // Propose removing Node 3
    uint64_t rm_node = 3;
    raft_entry_t e = { .type = ENTRY_CONF_REMOVE, .data = (uint8_t*)&rm_node, .data_len = sizeof(uint64_t) };
    raft_msg_t p = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(r, &p);

    // NEW API CONTRACT: Assert the peer is NOT removed yet (Uncommitted)
    uint64_t active_peers[16];
    size_t num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 3);

    // Simulate quorum ACKing the removal (Index 2)
    raft_msg_t ack1 = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 2 };
    raft_msg_t ack2 = { .type = MSG_APPEND_RES, .from = 4, .term = 1, .reject = false, .index = 2 };
    raft_core_step(r, &ack1);
    raft_core_step(r, &ack2);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 2);

    // Explicitly apply state
    raft_core_apply(r);
    raft_core_advance(r);

    // Verify it was safely shifted out after commit
    num = raft_core_peers(r, active_peers);
    MACRO_ASSERT_EQ_INT(num, 2);
    MACRO_ASSERT_EQ_INT(active_peers[0], 2);
    MACRO_ASSERT_EQ_INT(active_peers[1], 4); // 4 shifted down

    raft_core_destroy(r);
}


// --- 13. NEW: CONFIG SAFETY & HEARTBEATS ---

MACRO_TEST(raft_leader_ignores_append_res_beyond_log_end) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance(r);

    raft_entry_t e = { .type = ENTRY_NORMAL, .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(r, &prop);
    raft_core_advance(r);

    // Malicious/broken ACK for an index the leader never created
    raft_msg_t bogus = { .type = MSG_APPEND_RES, .from = 2, .term = 1, .reject = false, .index = 999 };
    raft_core_step(r, &bogus);

    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 0); // Denied

    raft_core_destroy(r);
}

MACRO_TEST(raft_create_rejects_duplicate_peers) {
    uint64_t peers[] = {2, 2};
    raft_core_t* r = raft_core_create(1, peers, 2);
    MACRO_ASSERT_TRUE(r == NULL);
}

MACRO_TEST(raft_create_rejects_null_peers_when_num_peers_nonzero) {
    raft_core_t* r = raft_core_create(1, NULL, 2);
    MACRO_ASSERT_TRUE(r == NULL);
}

MACRO_TEST(raft_follower_rejects_conflict_before_commit_index) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_entry_t e1 = { .term = 1, .type = ENTRY_NORMAL, .data = (uint8_t*)"A", .data_len = 1 };
    raft_msg_t app1 = { .type = MSG_APPEND_ENTRIES, .from = 2, .term = 1, .index = 0, .log_term = 0,
                        .entries = &e1, .num_entries = 1, .commit = 1 };
    raft_core_step(r, &app1);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);
    raft_core_advance(r);

    // Rogue leader tries to overwrite index 1
    raft_entry_t conflict = { .term = 2, .type = ENTRY_NORMAL, .data = (uint8_t*)"B", .data_len = 1 };
    raft_msg_t app2 = { .type = MSG_APPEND_ENTRIES, .from = 3, .term = 2, .index = 0, .log_term = 0,
                        .entries = &conflict, .num_entries = 1, .commit = 0 };
    raft_core_step(r, &app2);

    // Assert the committed log remains intact and the request was rejected
    MACRO_ASSERT_EQ_INT(raft_core_last_index(r), 1);
    MACRO_ASSERT_EQ_INT(raft_core_commit_index(r), 1);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_TRUE(ready.messages[0].reject);

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_tick_sends_heartbeat_to_all_peers) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance(r);

    raft_msg_t tick = { .type = MSG_TICK };
    raft_core_step(r, &tick);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 2);
    MACRO_ASSERT_TRUE(ready.messages[0].type == MSG_APPEND_ENTRIES);
    MACRO_ASSERT_TRUE(ready.messages[1].type == MSG_APPEND_ENTRIES);

    raft_core_destroy(r);
}

MACRO_TEST(raft_follower_tick_does_nothing) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t tick = { .type = MSG_TICK };
    raft_core_step(r, &tick);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 0); // Followers don't heartbeat

    raft_core_destroy(r);
}

MACRO_TEST(raft_leader_tick_sends_pending_entries_to_lagging_peer) {
    uint64_t peers[] = {2, 3};
    raft_core_t* r = raft_core_create(1, peers, 2);

    raft_msg_t hup = { .type = MSG_HUP };
    raft_core_step(r, &hup);
    raft_msg_t vote = { .type = MSG_REQUEST_VOTE_RES, .from = 2, .term = 1, .reject = false };
    raft_core_step(r, &vote);
    raft_core_advance(r);

    raft_entry_t e = { .data = (uint8_t*)"x", .data_len = 1 };
    raft_msg_t prop = { .type = MSG_PROPOSE, .entries = &e, .num_entries = 1 };
    raft_core_step(r, &prop);

    // Simulate network drop: Leader sends payload but it gets lost in the void
    raft_core_advance(r);

    // Tick triggers a heartbeat. Because Leader uses optimistic pipelining,
    // it assumes Node 3 received the payload, so it sends an empty heartbeat at the tip of the log.
    raft_msg_t tick = { .type = MSG_TICK };
    raft_core_step(r, &tick);

    raft_ready_t ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 2);

    raft_msg_t* to_node_3 = ready.messages[0].to == 3 ? &ready.messages[0] : &ready.messages[1];

    // Assert it sent a lightweight heartbeat, NOT a heavy payload resend!
    MACRO_ASSERT_EQ_INT(to_node_3->num_entries, 0);

    // Node 3 receives this heartbeat, realizes it is missing the data, and REJECTS it!
    uint64_t hb_prev_index = to_node_3->index;
    raft_core_advance(r); // clear queue

    raft_msg_t rej = { .type = MSG_APPEND_RES, .from = 3, .term = 1, .reject = true, .index = hb_prev_index };
    raft_core_step(r, &rej);

    // NOW the leader backtracks and explicitly resends the heavy payload!
    ready = raft_core_get_ready(r);
    MACRO_ASSERT_EQ_INT(ready.num_messages, 1);
    MACRO_ASSERT_TRUE(ready.messages[0].num_entries > 0); // Payload is attached!

    raft_core_destroy(r);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    // --- 1. BASIC ELECTION & LEADER STATE ---
    MACRO_ADD(tests, raft_initial_state);
    MACRO_ADD(tests, raft_campaign_becomes_candidate);
    MACRO_ADD(tests, raft_win_election_and_noop);

    // --- 2. LOG MATCHING & TRUNCATION SAFETY ---
    MACRO_ADD(tests, raft_follower_rejects_log_gaps);
    MACRO_ADD(tests, raft_follower_truncates_on_conflict);

    // --- 3. LEADER STEP-DOWN (TERM SUPREMACY) ---
    MACRO_ADD(tests, raft_leader_steps_down_on_higher_term);

    // --- 4. ELECTION SAFETY ---
    MACRO_ADD(tests, raft_voter_rejects_stale_candidate);

    // --- 5. COMMIT SAFETY ---
    MACRO_ADD(tests, raft_figure_8_anomaly_prevention);

    // --- 6. LEADER RECOVERY / BACKTRACKING ---
    MACRO_ADD(tests, raft_leader_backtracks_next_index_on_reject);

    // --- 7. REGRESSION & EDGE CASES ---
    MACRO_ADD(tests, raft_leader_ignores_lower_term_append);
    MACRO_ADD(tests, raft_candidate_ignores_stale_vote_response);
    MACRO_ADD(tests, raft_candidate_does_not_double_count_vote);
    MACRO_ADD(tests, raft_follower_rejects_second_candidate_same_term);
    MACRO_ADD(tests, raft_follower_updates_commit_from_heartbeat);
    MACRO_ADD(tests, raft_follower_appends_multiple_entries);
    MACRO_ADD(tests, raft_leader_backtracks_multiple_times);
    MACRO_ADD(tests, raft_advance_clears_messages_and_committed_entries);
    MACRO_ADD(tests, raft_candidate_steps_down_on_same_term_append);
    MACRO_ADD(tests, raft_single_node_becomes_leader_and_commits_noop);

    // --- 8. ADVANCED EDGE CASES & IDEMPOTENCY ---
    MACRO_ADD(tests, raft_follower_conflict_replacement_multiple_entries);
    MACRO_ADD(tests, raft_follower_duplicate_append_is_idempotent);
    MACRO_ADD(tests, raft_follower_ignores_propose);
    MACRO_ADD(tests, raft_candidate_does_not_count_rejected_vote);
    MACRO_ADD(tests, raft_leader_ignores_unknown_peer_append_res);
    MACRO_ADD(tests, raft_leader_ignores_stale_append_res);
    MACRO_ADD(tests, raft_candidate_steps_down_on_higher_term_vote_res);
    MACRO_ADD(tests, raft_follower_commit_never_decreases);
    MACRO_ADD(tests, raft_follower_commit_clamped_to_last_index);
    MACRO_ADD(tests, raft_apply_clears_committed_entries);

    // --- 9. DEFENSIVE PROGRAMMING & API CONTRACTS ---
    MACRO_ADD(tests, raft_ignores_messages_not_addressed_to_self);
    MACRO_ADD(tests, raft_leader_ignores_hup);
    MACRO_ADD(tests, raft_candidate_steps_down_on_higher_term_request_vote);
    MACRO_ADD(tests, raft_leader_steps_down_on_higher_term_request_vote);
    MACRO_ADD(tests, raft_leader_steps_down_on_higher_term_append_res);
    MACRO_ADD(tests, raft_rejects_append_entries_with_null_entries_and_count);
    MACRO_ADD(tests, raft_leader_rejects_empty_proposal);
    MACRO_ADD(tests, raft_ready_populates_entries_to_save);
    MACRO_ADD(tests, raft_committed_entry_content_is_correct);

    // --- 10. TIGHTENED CONTRACTS & BATCHING ---
    MACRO_ADD(tests, raft_ready_populates_entries_to_save_on_follower_append);
    MACRO_ADD(tests, raft_leader_ignores_zero_entry_proposal_even_with_entries_pointer);
    MACRO_ADD(tests, raft_leader_appends_multiple_proposed_entries);
    MACRO_ADD(tests, raft_ready_returns_only_newly_committed_entries_after_apply);
    MACRO_ADD(tests, raft_follower_duplicate_append_with_higher_commit_advances_commit);
    MACRO_ADD(tests, raft_follower_rejects_wrong_prev_log_term);
    MACRO_ADD(tests, raft_append_reject_reports_last_index);
    MACRO_ADD(tests, raft_create_rejects_too_many_peers_and_self);
    MACRO_ADD(tests, raft_candidate_ignores_unknown_peer_vote_response);
    MACRO_ADD(tests, raft_follower_heartbeat_clamps_commit_to_prev_index);

    // --- 11. FINAL COMPLETENESS BATCH ---
    MACRO_ADD(tests, raft_leader_backoff_floor);
    MACRO_ADD(tests, raft_leader_ignores_out_of_bounds_match_index);
    MACRO_ADD(tests, raft_follower_mixed_conflict_batch);
    MACRO_ADD(tests, raft_candidate_split_vote_recampaign);
    MACRO_ADD(tests, raft_leader_requires_peer_ack_to_commit_noop);
    MACRO_ADD(tests, raft_multi_node_smoke_test);

    // --- 12. DYNAMIC CONFIGURATION CHANGES ---
    MACRO_ADD(tests, raft_conf_add_node_applies_on_commit);
    MACRO_ADD(tests, raft_conf_remove_node_applies_on_commit);

    MACRO_ADD(tests, raft_conf_change_applies_only_on_commit);
    MACRO_ADD(tests, raft_conf_truncate_uncommitted_config_is_safe);

    // --- 13. CONFIG SAFETY & HEARTBEATS ---
    MACRO_ADD(tests, raft_leader_ignores_append_res_beyond_log_end);
    MACRO_ADD(tests, raft_create_rejects_duplicate_peers);
    MACRO_ADD(tests, raft_create_rejects_null_peers_when_num_peers_nonzero);
    MACRO_ADD(tests, raft_follower_rejects_conflict_before_commit_index);
    MACRO_ADD(tests, raft_leader_tick_sends_heartbeat_to_all_peers);
    MACRO_ADD(tests, raft_follower_tick_does_nothing);
    MACRO_ADD(tests, raft_leader_tick_sends_pending_entries_to_lagging_peer);

    macro_run_all("raft_core", tests, test_count);
    return 0;
}
