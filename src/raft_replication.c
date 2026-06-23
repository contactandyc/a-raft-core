// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "raft_internal.h"
#include <stdlib.h>
#include <string.h>

// ============================================================================
// CORE HELPERS & MATH
// ============================================================================

static int cmp_u64(const void* a, const void* b) {
    uint64_t ua = *(const uint64_t*)a;
    uint64_t ub = *(const uint64_t*)b;
    if (ua < ub) return -1;
    if (ua > ub) return 1;
    return 0;
}

static bool get_peer_index(raft_t* r, uint64_t peer_id, size_t* out_idx) {
    for (size_t i = 0; i < r->num_peers; i++) {
        if (r->peers[i] == peer_id) {
            *out_idx = i;
            return true;
        }
    }
    return false;
}

void raft_replication_advance_commit(raft_t* r, uint64_t new_commit) {
    if (new_commit <= r->commit_index) return;

    // Phase 2: O(1) Backpressure Tracking
    for (uint64_t i = r->commit_index + 1; i <= new_commit; i++) {
        raft_entry_t* e = raft_log_get(r, i);
        if (e) {
            if (r->uncommitted_bytes >= e->data_len) {
                r->uncommitted_bytes -= e->data_len;
            } else {
                r->uncommitted_bytes = 0;
            }
        }
    }

    r->commit_index = new_commit;
}

static void update_commit_index(raft_t* r) {
    uint64_t matches[MAX_PEERS + 1];
    size_t voters = 0;

    if (!r->is_learner_self) {
        matches[voters++] = raft_log_last_index(r);
    }

    for (size_t j = 0; j < r->num_peers; j++) {
        if (!r->is_learner[j]) {
            matches[voters++] = r->match_index[j];
        }
    }

    qsort(matches, voters, sizeof(uint64_t), cmp_u64);

    if (voters > 0) {
        size_t quorum = (voters / 2) + 1;
        uint64_t candidate = matches[voters - quorum];

        if (candidate > r->commit_index && raft_log_term(r, candidate) == r->current_term) {
            raft_replication_advance_commit(r, candidate);
        }
    }
}

static uint64_t find_conflict_backtrack_index(raft_t* r, uint64_t conflict_term, uint64_t conflict_index) {
    if (conflict_term == 0) return conflict_index;

    uint64_t idx = raft_log_last_index(r);
    while (idx >= r->snapshot_index) {
        if (raft_log_term(r, idx) == conflict_term) return idx + 1;
        if (idx == r->snapshot_index || idx == 0) break;
        idx--;
    }
    return conflict_index;
}

// ============================================================================
// OUTBOUND MESSAGING
// ============================================================================

static void send_append(raft_t* r, size_t peer_idx) {
    uint64_t peer_id = r->peers[peer_idx];
    uint64_t next = r->next_index[peer_idx];
    uint64_t last = raft_log_last_index(r);

    if (next <= r->snapshot_index) {
        raft_msg_t msg = { .type = MSG_INSTALL_SNAPSHOT, .to = peer_id, .term = r->current_term,
                           .index = r->snapshot_index, .log_term = r->snapshot_term };

        msg.snapshot_offset = r->snapshot_offset[peer_idx];
        msg.snapshot_done = false;

        msg.snapshot_peers = malloc(MAX_PEERS * sizeof(uint64_t));
        msg.snapshot_is_learner = malloc(MAX_PEERS * sizeof(bool));

        if (!msg.snapshot_peers || !msg.snapshot_is_learner) {
            free(msg.snapshot_peers);
            free(msg.snapshot_is_learner);
            r->fatal_error = true;
            return;
        }

        msg.snapshot_num_peers = r->snapshot_peers_count;
        for (size_t i = 0; i < r->snapshot_peers_count; i++) {
            msg.snapshot_peers[i] = r->snapshot_peers_cache[i];
            msg.snapshot_is_learner[i] = r->snapshot_learners_cache[i];
        }

        raft_send_msg(r, msg);
        return;
    }

    raft_msg_t msg = { .type = MSG_APPEND_ENTRIES, .to = peer_id, .term = r->current_term,
                       .index = next - 1, .log_term = raft_log_term(r, next - 1), .commit = r->commit_index,
                       .read_seq = r->current_read_seq };

    size_t num_entries = last >= next ? last - next + 1 : 0;
    if (num_entries > 500) num_entries = 500;

    size_t batch_bytes = 0;
    size_t actual_entries = 0;
    for (size_t i = 0; i < num_entries; i++) {
        raft_entry_t* src = raft_log_get(r, next + i);
        if (!src) break;

        // FIX 1: Use the macro and drop gracefully instead of triggering a fatal error
        if (src->data_len > RAFT_MAX_PAYLOAD_SIZE - batch_bytes) {
            if (actual_entries == 0) {
                return;
            }
            break;
        }
        batch_bytes += src->data_len;
        actual_entries++;
    }
    num_entries = actual_entries;

    msg.num_entries = num_entries;
    if (num_entries > 0) {
        msg.entries = calloc(num_entries, sizeof(raft_entry_t));
        if (!msg.entries) {
            r->fatal_error = true;
            return;
        }
    }

    for (size_t i = 0; i < num_entries; i++) {
        raft_entry_t* src = raft_log_get(r, next + i);
        msg.entries[i].term = src->term;
        msg.entries[i].index = src->index;
        msg.entries[i].type = src->type;
        msg.entries[i].client_id = src->client_id;
        msg.entries[i].client_seq = src->client_seq;
        msg.entries[i].data_len = src->data_len;
        msg.entries[i].data = NULL;

        if (src->data_len > 0) {
            msg.entries[i].data = malloc(src->data_len);
            if (!msg.entries[i].data) {
                for (size_t j = 0; j < i; j++) free(msg.entries[j].data);
                free(msg.entries);
                r->fatal_error = true;
                return;
            }
            memcpy(msg.entries[i].data, src->data, src->data_len);
        }
    }

    r->next_index[peer_idx] = next + num_entries;
    raft_send_msg(r, msg);
}

void raft_replication_bcast_append(raft_t* r) {
    for (size_t i = 0; i < r->num_peers; i++) send_append(r, i);
}

// ============================================================================
// INBOUND HANDLERS
// ============================================================================

static void handle_tick(raft_t* r) {
    if (r->state == RAFT_STATE_LEADER) {
        raft_replication_bcast_append(r);
    }
}

static void handle_propose(raft_t* r, raft_msg_t* msg) {
    if (r->state != RAFT_STATE_LEADER || msg->num_entries == 0 || !msg->entries) return;

    uint64_t old_last_idx = raft_log_last_index(r);
    bool has_pending_config = false;

    // Phase 3: Check the entire unapplied tail for overlapping configurations
    for (uint64_t idx = r->last_applied + 1; idx <= old_last_idx; idx++) {
        raft_entry_t* e = raft_log_get(r, idx);
        if (e && e->type != ENTRY_NORMAL) {
            has_pending_config = true;
            break;
        }
    }

    bool appended = false;
    for (size_t i = 0; i < msg->num_entries; i++) {

        // FIX 2: Gracefully drop oversized proposals at the core level!
        if (msg->entries[i].data_len > RAFT_MAX_PAYLOAD_SIZE) {
            return;
        }

        if (msg->entries[i].type != ENTRY_NORMAL) {

            // Safety: Only one uncommitted/unapplied configuration change at a time
            if (has_pending_config) continue;

            // Phase 3: Secure Learner Promotion Guard
            if (msg->entries[i].type == ENTRY_CONF_PROMOTE_LEARNER && msg->entries[i].data_len == sizeof(uint64_t)) {
                uint64_t target_node;
                memcpy(&target_node, msg->entries[i].data, sizeof(uint64_t));

                size_t p_idx;
                if (get_peer_index(r, target_node, &p_idx)) {
                    if (r->match_index[p_idx] < r->commit_index) {
                        continue; // Drop unsafe promotion
                    }
                }
            }

            has_pending_config = true;
        }

        if (!raft_log_append(r, r->current_term, msg->entries[i].type, msg->entries[i].client_id, msg->entries[i].client_seq, msg->entries[i].data, msg->entries[i].data_len)) {
            break;
        }
        appended = true;
    }

    if (!appended) return;

    if (r->num_peers == 0) {
        raft_replication_advance_commit(r, raft_log_last_index(r));
        return;
    }

    for (size_t i = 0; i < r->num_peers; i++) {
        if (r->next_index[i] == old_last_idx + 1) {
            send_append(r, i);
        }
    }
}

static void handle_append_entries(raft_t* r, raft_msg_t* msg) {
    if (msg->num_entries > 0 && msg->entries == NULL) {
        raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = raft_log_last_index(r) };
        raft_send_msg(r, res);
        return;
    }

    r->state = RAFT_STATE_FOLLOWER;
    r->activity_accepted = true;
    r->leader_id = msg->from;

    raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = raft_log_last_index(r), .read_seq = 0 };
    uint64_t my_last_idx = raft_log_last_index(r);

    if (msg->index >= r->snapshot_index && msg->index <= my_last_idx && raft_log_term(r, msg->index) == msg->log_term) {
        res.reject = false;

        if (msg->num_entries > 0) {
            for (size_t i = 0; i < msg->num_entries; i++) {
                uint64_t new_idx = msg->index + 1 + i;

                if ((msg->entries[i].index != 0 && msg->entries[i].index != new_idx) ||
                    (msg->entries[i].term == 0 || msg->entries[i].term > msg->term) ||
                    (msg->entries[i].data_len > 0 && !msg->entries[i].data)) {
                    res.reject = true;
                    break;
                }

                my_last_idx = raft_log_last_index(r);

                if (new_idx <= my_last_idx && raft_log_term(r, new_idx) == msg->entries[i].term) continue;
                if (new_idx <= r->commit_index) {
                    res.reject = true;
                    break;
                }

                if (new_idx <= my_last_idx) raft_log_truncate(r, new_idx);

                if (!raft_log_append(r, msg->entries[i].term, msg->entries[i].type, msg->entries[i].client_id, msg->entries[i].client_seq, msg->entries[i].data, msg->entries[i].data_len)) {
                    res.reject = true;
                    break;
                }
            }
        }

        if (!res.reject) {
            res.index = msg->index + msg->num_entries;
            if (msg->commit > r->commit_index) {
                raft_replication_advance_commit(r, (msg->commit < res.index) ? msg->commit : res.index);
            }
            res.read_seq = msg->read_seq;
        }

    } else if (msg->index < r->snapshot_index) {
        res.reject = true;
        res.index = r->snapshot_index;
        res.conflict_term = 0;
        res.conflict_index = r->snapshot_index + 1;

    } else {
        res.reject = true;
        if (msg->index > my_last_idx) {
            res.conflict_index = my_last_idx + 1;
            res.conflict_term = 0;
        } else {
            res.conflict_term = raft_log_term(r, msg->index);
            uint64_t first_idx = msg->index;
            while (first_idx >= r->snapshot_index) {
                if (raft_log_term(r, first_idx) == res.conflict_term) {
                    while (first_idx > r->snapshot_index && raft_log_term(r, first_idx - 1) == res.conflict_term) {
                        first_idx--;
                    }
                    break;
                }
                if (first_idx == r->snapshot_index || first_idx == 0) break;
                first_idx--;
            }
            res.conflict_index = first_idx;
        }
    }

    raft_send_msg(r, res);
}

static void handle_append_response(raft_t* r, raft_msg_t* msg) {
    if (r->state != RAFT_STATE_LEADER || msg->term != r->current_term) return;

    size_t peer_idx;
    if (!get_peer_index(r, msg->from, &peer_idx)) return;

    r->recent_active[peer_idx] = true;

    if (r->next_index[peer_idx] <= r->snapshot_index) {
        // FIX 2: Honor rejected snapshot chunk conflict hints
        if (msg->reject) {
            r->snapshot_offset[peer_idx] = msg->conflict_index;
            send_append(r, peer_idx);
            return;
        }

        if (msg->snapshot_done) {
            r->match_index[peer_idx] = r->snapshot_index;
            r->next_index[peer_idx] = r->snapshot_index + 1;
            r->snapshot_offset[peer_idx] = 0;
            update_commit_index(r);
        } else {
            r->snapshot_offset[peer_idx] = msg->conflict_index;
        }
        send_append(r, peer_idx);
        return;
    }

    if (!msg->reject) {
        uint64_t last = raft_log_last_index(r);
        if (msg->index > last) return;

        raft_read_index_ack(r, peer_idx, msg->read_seq);

        uint64_t safe_idx = msg->index < last ? msg->index : last;
        if (safe_idx >= r->match_index[peer_idx]) {
            r->match_index[peer_idx] = safe_idx;
            r->next_index[peer_idx] = safe_idx + 1;
        }

        update_commit_index(r);

        if (r->next_index[peer_idx] <= raft_log_last_index(r)) {
            send_append(r, peer_idx);
        }

    } else {
        uint64_t backtrack = find_conflict_backtrack_index(r, msg->conflict_term, msg->conflict_index);
        uint64_t last = raft_log_last_index(r);

        if (backtrack < 1) backtrack = 1;
        if (backtrack > last + 1) backtrack = last + 1;

        r->next_index[peer_idx] = backtrack;
        send_append(r, peer_idx);
    }
}

// ============================================================================
// PUBLIC ROUTER
// ============================================================================

void raft_replication_step(raft_t* r, raft_msg_t* msg) {
    switch(msg->type) {
        case MSG_TICK:
            handle_tick(r);
            break;
        case MSG_PROPOSE:
            handle_propose(r, msg);
            break;
        case MSG_APPEND_ENTRIES:
            handle_append_entries(r, msg);
            break;
        case MSG_APPEND_RES:
            handle_append_response(r, msg);
            break;
        default:
            break;
    }
}
