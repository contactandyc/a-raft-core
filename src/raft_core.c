// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "raft_internal.h"
#include <stdlib.h>
#include <string.h>

void raft_send_msg(raft_t* r, raft_msg_t msg) {
    if (r->msg_queue_len >= r->msg_queue_cap) {
        size_t new_cap = r->msg_queue_cap == 0 ? 16 : r->msg_queue_cap * 2;
        raft_msg_t* new_q = realloc(r->msg_queue, new_cap * sizeof(raft_msg_t));
        if (!new_q) {
            if (msg.entries) {
                for (size_t i = 0; i < msg.num_entries; i++) {
                    if (msg.entries[i].data) free(msg.entries[i].data);
                }
                free(msg.entries);
            }
            if (msg.snapshot_data) free(msg.snapshot_data);
            return;
        }
        r->msg_queue = new_q;
        r->msg_queue_cap = new_cap;
    }
    msg.from = r->id;
    r->msg_queue[r->msg_queue_len++] = msg;
}

raft_t* raft_create(uint64_t id, uint64_t* peers, size_t num_peers) {
    if (num_peers > MAX_PEERS || (num_peers > 0 && !peers)) return NULL;

    for (size_t i = 0; i < num_peers; i++) {
        if (peers[i] == id) return NULL;
        for (size_t j = i + 1; j < num_peers; j++) {
            if (peers[i] == peers[j]) return NULL;
        }
    }

    raft_t* r = calloc(1, sizeof(raft_t));
    if (!r) return NULL;

    r->id = id;
    r->state = RAFT_STATE_FOLLOWER;
    r->num_peers = num_peers;

    for (size_t i = 0; i < num_peers; i++) {
        r->peers[i] = peers[i];
        r->is_learner[i] = false;
        r->next_index[i] = 1;
    }

    r->log_cap = 16;
    r->log = calloc(r->log_cap, sizeof(raft_entry_t));
    r->log_len = 1;

    return r;
}

void raft_destroy(raft_t* r) {
    if (!r) return;
    for (size_t i = 0; i < r->log_len; i++) {
        if (r->log[i].data) free(r->log[i].data);
    }
    free(r->log);

    if (r->msg_queue) {
        for (size_t i = 0; i < r->msg_queue_len; i++) {
            if (r->msg_queue[i].entries) {
                for (size_t j = 0; j < r->msg_queue[i].num_entries; j++) {
                    if (r->msg_queue[i].entries[j].data) free(r->msg_queue[i].entries[j].data);
                }
                free(r->msg_queue[i].entries);
            }
            if (r->msg_queue[i].snapshot_data) free(r->msg_queue[i].snapshot_data);
        }
        free(r->msg_queue);
    }
    if (r->read_states) free(r->read_states);
    if (r->pending_snapshot_data) free(r->pending_snapshot_data);
    free(r);
}

raft_t* raft_restore(uint64_t id, uint64_t* peers, bool* is_learners, size_t num_peers,
                               uint64_t term, uint64_t voted_for, uint64_t commit_index, uint64_t applied_index,
                               uint64_t snapshot_index, uint64_t snapshot_term,
                               raft_entry_t* entries, size_t num_entries) {
    if (num_peers > MAX_PEERS) return NULL;
    if (num_entries > 0 && entries == NULL) return NULL;

    for (size_t i = 1; i < num_entries; i++) {
        if (entries[i].index <= entries[i-1].index) return NULL;
        if (entries[i].term < entries[i-1].term) return NULL;
    }

    raft_t* r = calloc(1, sizeof(raft_t));
    if (!r) return NULL;

    r->id = id;
    r->state = RAFT_STATE_FOLLOWER;
    r->num_peers = 0;
    bool found_self = false;

    for (size_t i = 0; i < num_peers; i++) {
        if (peers[i] == id) {
            r->is_learner_self = is_learners ? is_learners[i] : false;
            found_self = true;
        } else {
            if (r->num_peers < MAX_PEERS) {
                r->peers[r->num_peers] = peers[i];
                r->is_learner[r->num_peers] = is_learners ? is_learners[i] : false;
                r->next_index[r->num_peers] = snapshot_index + 1;
                r->num_peers++;
            }
        }
    }

    if (!found_self && num_peers > 0) {
        r->removed = true;
        r->is_learner_self = true;
        r->state = RAFT_STATE_FOLLOWER;
    }

    r->current_term = term;
    r->voted_for = voted_for;
    r->snapshot_index = snapshot_index;
    r->snapshot_term = snapshot_term;

    r->log_cap = num_entries > 16 ? num_entries * 2 : 16;
    r->log = calloc(r->log_cap, sizeof(raft_entry_t));
    if (!r->log) { raft_destroy(r); return NULL; }

    r->log[0].index = r->snapshot_index;
    r->log[0].term = r->snapshot_term;
    r->log[0].type = ENTRY_NORMAL;
    r->log[0].client_id = 0;
    r->log[0].client_seq = 0;
    r->log[0].data = NULL;
    r->log[0].data_len = 0;
    r->log_len = 1;

    for (size_t i = 0; i < num_entries; i++) {
        if (entries[i].index <= r->snapshot_index) continue;

        raft_entry_t* e = &r->log[r->log_len++];
        e->term = entries[i].term;
        e->index = entries[i].index;
        e->type = entries[i].type;
        e->client_id = entries[i].client_id;
        e->client_seq = entries[i].client_seq;
        e->data_len = entries[i].data_len;
        e->data = entries[i].data_len > 0 ? malloc(entries[i].data_len) : NULL;
        if (e->data_len > 0 && e->data) memcpy(e->data, entries[i].data, e->data_len);
    }

    r->last_saved_index = raft_log_last_index(r);
    r->commit_index = commit_index;
    r->last_applied = applied_index;

    return r;
}

void raft_step(raft_t* r, raft_msg_t* msg) {
    if (msg->to != 0 && msg->to != r->id) return;

    if (msg->from != 0) {
        for (size_t i = 0; i < r->num_peers; i++) {
            if (r->peers[i] == msg->from) {
                r->recent_active[i] = true;
                break;
            }
        }
    }

    if (msg->term >= r->current_term && (msg->type == MSG_APPEND_ENTRIES || msg->type == MSG_INSTALL_SNAPSHOT)) {
        r->leader_id = msg->from;
    }

    if (msg->term > 0 && msg->term < r->current_term) {
        if (msg->type == MSG_APPEND_ENTRIES) {
            raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = raft_log_last_index(r) };
            raft_send_msg(r, res);
        } else if (msg->type == MSG_REQUEST_VOTE) {
            raft_msg_t res = { .type = MSG_REQUEST_VOTE_RES, .to = msg->from, .term = r->current_term, .reject = true };
            raft_send_msg(r, res);
        } else if (msg->type == MSG_PRE_VOTE) {
            raft_msg_t res = { .type = MSG_PRE_VOTE_RES, .to = msg->from, .term = r->current_term, .reject = true };
            raft_send_msg(r, res);
        }
        return;
    }

    if (msg->term > r->current_term) {
        if (msg->type != MSG_PRE_VOTE && msg->type != MSG_PRE_VOTE_RES) {
            r->current_term = msg->term;
            r->voted_for = 0;
            r->state = RAFT_STATE_FOLLOWER;
        }
    }

    // THE CLEAN ROUTER
    switch(msg->type) {
        case MSG_HUP:
        case MSG_PRE_VOTE:
        case MSG_PRE_VOTE_RES:
        case MSG_REQUEST_VOTE:
        case MSG_REQUEST_VOTE_RES:
        case MSG_CHECK_QUORUM:
            raft_election_step(r, msg);
            break;
        case MSG_TICK:
        case MSG_PROPOSE:
        case MSG_APPEND_ENTRIES:
        case MSG_APPEND_RES:
            raft_replication_step(r, msg);
            break;
        case MSG_INSTALL_SNAPSHOT:
            raft_snapshot_step(r, msg);
            break;
        case MSG_READ_INDEX:
        case MSG_READ_INDEX_RES:
            raft_read_index_step(r, msg);
            break;
    }
}

raft_ready_t raft_get_ready(raft_t* r) {
    raft_ready_t ready;
    memset(&ready, 0, sizeof(ready));

    ready.messages = r->msg_queue;
    ready.num_messages = r->msg_queue_len;

    uint64_t save_from = r->last_saved_index;
    if (save_from < r->snapshot_index) save_from = r->snapshot_index;

    uint64_t last_idx = raft_log_last_index(r);
    if (last_idx > save_from) {
        size_t count = last_idx - save_from;
        ready.entries_to_save = malloc(count * sizeof(raft_entry_t));
        if (ready.entries_to_save) {
            for (size_t i = 0; i < count; i++) {
                raft_entry_t* src = raft_log_get(r, save_from + 1 + i);
                if (src) ready.entries_to_save[i] = *src;
            }
            ready.num_entries_to_save = count;
        }
    }

    uint64_t apply_from = r->last_applied;
    if (apply_from < r->snapshot_index) apply_from = r->snapshot_index;

    if (r->commit_index > apply_from) {
        size_t count = r->commit_index - apply_from;
        ready.committed_entries = malloc(count * sizeof(raft_entry_t));
        if (ready.committed_entries) {
            for (size_t i = 0; i < count; i++) {
                raft_entry_t* src = raft_log_get(r, apply_from + 1 + i);
                if (src) ready.committed_entries[i] = *src;
            }
            ready.num_committed_entries = count;
        }
    }

    ready.read_states = r->read_states;
    ready.num_read_states = r->num_read_states;

    ready.install_snapshot = r->pending_snapshot;
    ready.snapshot_index = r->pending_snapshot_msg_index;
    ready.snapshot_term = r->pending_snapshot_msg_term;
    ready.snapshot_data = r->pending_snapshot_data;
    ready.snapshot_len = r->pending_snapshot_len;

    return ready;
}

void raft_advance(raft_t* r, uint64_t saved_index, uint64_t applied_index) {
    if (r->pending_snapshot) {
        if (r->last_saved_index < r->snapshot_index) r->last_saved_index = r->snapshot_index;
        if (r->commit_index < r->snapshot_index) r->commit_index = r->snapshot_index;
        if (r->last_applied < r->snapshot_index) r->last_applied = r->snapshot_index;

        if (r->pending_snapshot_data) free(r->pending_snapshot_data);
        r->pending_snapshot_data = NULL;
        r->pending_snapshot_len = 0;
        r->pending_snapshot = false;
    }

    if (saved_index > r->last_saved_index) r->last_saved_index = saved_index;

    for(uint64_t i = r->last_applied + 1; i <= applied_index; i++) {
        raft_entry_t* e = raft_log_get(r, i);
        if (e && (e->type == ENTRY_CONF_ADD || e->type == ENTRY_CONF_ADD_LEARNER || e->type == ENTRY_CONF_REMOVE || e->type == ENTRY_CONF_PROMOTE_LEARNER)) {
            raft_membership_apply_config(r, i);
        }
    }

    if (applied_index > r->last_applied) r->last_applied = applied_index;

    if (r->msg_queue) {
        for (size_t i = 0; i < r->msg_queue_len; i++) {
            if (r->msg_queue[i].entries) {
                for (size_t j = 0; j < r->msg_queue[i].num_entries; j++) {
                    if (r->msg_queue[i].entries[j].data) free(r->msg_queue[i].entries[j].data);
                }
                free(r->msg_queue[i].entries);
                r->msg_queue[i].entries = NULL;
            }
            if (r->msg_queue[i].snapshot_data) {
                free(r->msg_queue[i].snapshot_data);
                r->msg_queue[i].snapshot_data = NULL;
            }
        }
    }
    r->msg_queue_len = 0;
    r->num_read_states = 0;
    r->activity_accepted = false;
}

void raft_advance_all(raft_t* r) {
    raft_ready_t ready = raft_get_ready(r);
    if (ready.num_entries_to_save > 0) free(ready.entries_to_save);
    if (ready.num_committed_entries > 0) free(ready.committed_entries);
    raft_advance(r, raft_log_last_index(r), raft_commit_index(r));
}

void raft_compact(raft_t* r, uint64_t compact_index) {
    if (compact_index <= r->snapshot_index || compact_index > r->last_applied) return;

    r->snapshot_term = raft_log_term(r, compact_index);
    size_t keep_len = r->log_len - (compact_index - r->snapshot_index);

    for (uint64_t i = r->snapshot_index + 1; i <= compact_index; i++) {
        raft_entry_t* e = raft_log_get(r, i);
        if (e && e->data) free(e->data);
    }

    memmove(&r->log[1], &r->log[compact_index - r->snapshot_index + 1], (keep_len - 1) * sizeof(raft_entry_t));
    r->log_len = keep_len;
    r->snapshot_index = compact_index;

    r->log[0].index = compact_index;
    r->log[0].term = r->snapshot_term;
    r->log[0].type = ENTRY_NORMAL;
    r->log[0].client_id = 0;
    r->log[0].client_seq = 0;
    r->log[0].data = NULL;
    r->log[0].data_len = 0;
}

raft_state_t raft_state(raft_t* r) { return r->state; }
uint64_t raft_term(raft_t* r) { return r->current_term; }
uint64_t raft_voted_for(raft_t* r) { return r->voted_for; }
uint64_t raft_commit_index(raft_t* r) { return r->commit_index; }
uint64_t raft_last_index(raft_t* r) { return raft_log_last_index(r); }
uint64_t raft_last_applied(raft_t* r) { return r->last_applied; }
bool raft_activity_accepted(raft_t* r) { return r->activity_accepted; }

uint64_t raft_leader_id(raft_t* r) { return r->leader_id; }
uint64_t raft_snapshot_index(raft_t* r) { return r->snapshot_index; }
uint64_t raft_snapshot_term(raft_t* r) { return r->snapshot_term; }

size_t raft_peers(raft_t* r, uint64_t* out_peers) {
    if (out_peers) {
        for (size_t i = 0; i < r->num_peers; i++) out_peers[i] = r->peers[i];
    }
    return r->num_peers;
}

size_t raft_peers_ext(raft_t* r, uint64_t* out_peers, bool* out_is_learners) {
    size_t count = 0;
    for (size_t i = 0; i < r->num_peers; i++) {
        if (out_peers) out_peers[count] = r->peers[i];
        if (out_is_learners) out_is_learners[count] = r->is_learner[i];
        count++;
    }

    if (!r->removed) {
        if (out_peers) out_peers[count] = r->id;
        if (out_is_learners) out_is_learners[count] = r->is_learner_self;
        count++;
    }

    return count;
}
