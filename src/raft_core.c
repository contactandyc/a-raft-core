// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-raft-library/raft_core.h"
#include <stdlib.h>
#include <string.h>

#define MAX_PEERS 16
#define RAFT_MAX_APPEND_BATCH 2048 // Prevents network/codec deadlocks

struct raft_core_s {
    uint64_t id;

    uint64_t init_peers[MAX_PEERS];
    size_t   num_init_peers;

    uint64_t peers[MAX_PEERS];
    size_t   num_peers;

    raft_state_t state;
    uint64_t current_term;
    uint64_t voted_for;

    uint64_t snapshot_index;
    uint64_t snapshot_term;

    raft_entry_t* log;
    size_t log_len;
    size_t log_cap;

    uint64_t commit_index;
    uint64_t last_applied;
    uint64_t last_saved_index;

    uint64_t next_index[MAX_PEERS];
    uint64_t match_index[MAX_PEERS];

    uint32_t votes_received;
    bool     voted_for_me[MAX_PEERS];

    raft_msg_t* msgs;
    size_t num_msgs;
    size_t msg_cap;

    bool activity_accepted;
};

static void send_msg(raft_core_t* r, raft_msg_t m) {
    if (r->num_msgs >= r->msg_cap) {
        r->msg_cap = r->msg_cap == 0 ? 1024 : r->msg_cap * 2;
        r->msgs = realloc(r->msgs, r->msg_cap * sizeof(raft_msg_t));
        if (!r->msgs) abort();
    }
    m.from = r->id;
    r->msgs[r->num_msgs++] = m;
}

uint64_t raft_core_last_index(raft_core_t* r) {
    return r->snapshot_index + r->log_len - 1;
}

static raft_entry_t* log_at(raft_core_t* r, uint64_t absolute_index) {
    if (absolute_index < r->snapshot_index) return NULL;
    size_t arr_idx = absolute_index - r->snapshot_index;
    if (arr_idx >= r->log_len) return NULL;
    return &r->log[arr_idx];
}

static uint64_t log_term(raft_core_t* r, uint64_t idx) {
    if (idx == r->snapshot_index) return r->snapshot_term;
    raft_entry_t* e = log_at(r, idx);
    return e ? e->term : 0;
}

static void apply_conf_change(raft_core_t* r, entry_type_t type, const uint8_t* data, size_t len) {
    if (len != sizeof(uint64_t)) return;
    uint64_t pid;
    memcpy(&pid, data, sizeof(uint64_t));

    if (type == ENTRY_CONF_ADD) {
        if (pid == r->id) return;
        for (size_t i = 0; i < r->num_peers; i++) if (r->peers[i] == pid) return;

        if (r->num_peers < MAX_PEERS) {
            r->peers[r->num_peers] = pid;
            r->next_index[r->num_peers] = raft_core_last_index(r) + 1;
            r->match_index[r->num_peers] = 0;
            r->num_peers++;
        }
    } else if (type == ENTRY_CONF_REMOVE) {
        for (size_t i = 0; i < r->num_peers; i++) {
            if (r->peers[i] == pid) {
                for (size_t j = i; j < r->num_peers - 1; j++) {
                    r->peers[j] = r->peers[j+1];
                    r->next_index[j] = r->next_index[j+1];
                    r->match_index[j] = r->match_index[j+1];
                }
                r->num_peers--;
                break;
            }
        }
    }
}

static void log_append(raft_core_t* r, uint64_t term, entry_type_t type, const uint8_t* data, size_t len) {
    if (r->log_len >= r->log_cap) {
        r->log_cap = r->log_cap == 0 ? 128 : r->log_cap * 2;
        r->log = realloc(r->log, r->log_cap * sizeof(raft_entry_t));
        if (!r->log) abort();
    }
    raft_entry_t* e = &r->log[r->log_len];
    e->index = r->snapshot_index + r->log_len;
    e->term = term;
    e->type = type;
    e->data_len = len;

    if (len > 0 && data != NULL) {
        e->data = malloc(len);
        if (!e->data) abort();
        memcpy(e->data, data, len);
    } else {
        e->data = NULL;
    }
    r->log_len++;
}

static void log_truncate(raft_core_t* r, uint64_t absolute_index) {
    if (absolute_index <= r->snapshot_index) return;
    size_t arr_idx = absolute_index - r->snapshot_index;

    for (size_t i = arr_idx; i < r->log_len; i++) {
        if (r->log[i].data) free(r->log[i].data);
    }
    r->log_len = arr_idx;
    if (absolute_index > 0 && r->last_saved_index >= absolute_index) {
        r->last_saved_index = absolute_index - 1;
    }
}

void raft_core_compact(raft_core_t* r, uint64_t compact_index) {
    if (compact_index <= r->snapshot_index) return;
    if (compact_index > r->commit_index) return;

    uint64_t new_snapshot_term = log_term(r, compact_index);
    size_t arr_idx = compact_index - r->snapshot_index;

    for (size_t i = 0; i < arr_idx; i++) {
        if (r->log[i].data) free(r->log[i].data);
    }

    size_t remaining = r->log_len - arr_idx;
    memmove(r->log, &r->log[arr_idx], remaining * sizeof(raft_entry_t));
    r->log_len = remaining;

    r->snapshot_index = compact_index;
    r->snapshot_term = new_snapshot_term;

    r->log[0].data = NULL;
    r->log[0].data_len = 0;
    r->log[0].type = ENTRY_NORMAL;
    r->log[0].index = compact_index;
    r->log[0].term = new_snapshot_term;
}

raft_core_t* raft_core_create(uint64_t id, uint64_t* peers, size_t num_peers) {
    if (num_peers > 0 && peers == NULL) return NULL;
    if (num_peers > MAX_PEERS) return NULL;
    for (size_t i = 0; i < num_peers; i++) {
        if (peers[i] == id) return NULL;
        for (size_t j = i + 1; j < num_peers; j++) {
            if (peers[i] == peers[j]) return NULL;
        }
    }

    raft_core_t* r = calloc(1, sizeof(raft_core_t));
    if (!r) abort();

    r->id = id;
    r->num_init_peers = num_peers;
    r->num_peers = num_peers;
    for (size_t i = 0; i < num_peers; i++) {
        r->init_peers[i] = peers[i];
        r->peers[i] = peers[i];
    }

    r->state = RAFT_STATE_FOLLOWER;
    r->snapshot_index = 0;
    r->snapshot_term = 0;
    log_append(r, 0, ENTRY_NORMAL, NULL, 0);

    return r;
}

void raft_core_destroy(raft_core_t* r) {
    if (!r) return;
    for (size_t i = 0; i < r->log_len; i++) {
        if (r->log[i].data) free(r->log[i].data);
    }
    free(r->log);
    if (r->msgs) free(r->msgs);
    free(r);
}

raft_ready_t raft_core_get_ready(raft_core_t* r) {
    raft_ready_t ready = {0};
    ready.messages = r->msgs;
    ready.num_messages = r->num_msgs;

    if (r->commit_index > r->last_applied) {
        uint64_t first_app_idx = r->last_applied + 1;
        if (first_app_idx >= r->snapshot_index) {
            size_t arr_idx = first_app_idx - r->snapshot_index;
            ready.committed_entries = &r->log[arr_idx];
            ready.num_committed_entries = r->commit_index - r->last_applied;
        }
    }

    uint64_t last_idx = raft_core_last_index(r);
    if (last_idx > r->last_saved_index) {
        uint64_t first_save_idx = r->last_saved_index + 1;
        if (first_save_idx >= r->snapshot_index) {
            size_t arr_idx = first_save_idx - r->snapshot_index;
            ready.entries_to_save = &r->log[arr_idx];
            ready.num_entries_to_save = last_idx - r->last_saved_index;
        }
    }

    return ready;
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t va = *(const uint64_t *)a;
    uint64_t vb = *(const uint64_t *)b;
    return (va > vb) - (va < vb);
}

static void send_append(raft_core_t* r, size_t peer_idx) {
    uint64_t prev_idx = r->next_index[peer_idx] - 1;

    if (prev_idx < r->snapshot_index) {
        raft_msg_t snap = {
            .type = MSG_INSTALL_SNAPSHOT,
            .to = r->peers[peer_idx],
            .term = r->current_term,
            .index = r->snapshot_index,
            .log_term = r->snapshot_term,
            .commit = r->commit_index
        };
        send_msg(r, snap);
        return;
    }

    uint64_t num_entries = raft_core_last_index(r) - prev_idx;

    if (num_entries > RAFT_MAX_APPEND_BATCH) {
        num_entries = RAFT_MAX_APPEND_BATCH;
    }

    r->next_index[peer_idx] = prev_idx + 1 + num_entries;

    size_t arr_idx = (prev_idx + 1) - r->snapshot_index;
    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = r->peers[peer_idx], .term = r->current_term,
                       .index = prev_idx, .log_term = log_term(r, prev_idx),
                       .commit = r->commit_index,
                       .entries = num_entries > 0 ? &r->log[arr_idx] : NULL,
                       .num_entries = num_entries };
    send_msg(r, app);
}

static void bcast_append(raft_core_t* r) {
    for (size_t i = 0; i < r->num_peers; i++) {
        send_append(r, i);
    }
}

static void become_leader(raft_core_t* r) {
    r->state = RAFT_STATE_LEADER;
    log_append(r, r->current_term, ENTRY_NORMAL, NULL, 0);

    uint64_t last_idx = raft_core_last_index(r);
    if (r->num_peers == 0) {
        r->commit_index = last_idx;
        return;
    }
    for (size_t i = 0; i < r->num_peers; i++) {
        r->next_index[i] = last_idx + 1;
        r->match_index[i] = 0;
    }
    bcast_append(r);
}

void raft_core_step(raft_core_t* r, raft_msg_t* msg) {
    if (msg->to != 0 && msg->to != r->id) return;

    if (msg->term > 0 && msg->term < r->current_term) {
        if (msg->type == MSG_APPEND_ENTRIES) {
            raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = raft_core_last_index(r) };
            send_msg(r, res);
        } else if (msg->type == MSG_REQUEST_VOTE) {
            raft_msg_t res = { .type = MSG_REQUEST_VOTE_RES, .to = msg->from, .term = r->current_term, .reject = true };
            send_msg(r, res);
        }
        return;
    }

    if (msg->term > r->current_term) {
        r->current_term = msg->term;
        r->voted_for = 0;
        r->state = RAFT_STATE_FOLLOWER;
    }

    if (msg->type == MSG_TICK) {
        if (r->state == RAFT_STATE_LEADER) bcast_append(r);
        return;
    }

    if (msg->type == MSG_HUP) {
        if (r->state == RAFT_STATE_LEADER) return;
        r->state = RAFT_STATE_CANDIDATE;
        r->current_term++;
        r->voted_for = r->id;
        r->votes_received = 1;
        memset(r->voted_for_me, 0, sizeof(r->voted_for_me));

        if (r->num_peers == 0) {
            become_leader(r);
            return;
        }

        uint64_t last_idx = raft_core_last_index(r);
        for (size_t i = 0; i < r->num_peers; i++) {
            raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = r->peers[i], .term = r->current_term,
                               .index = last_idx, .log_term = log_term(r, last_idx) };
            send_msg(r, req);
        }
    }
    else if (msg->type == MSG_REQUEST_VOTE) {
        raft_msg_t res = { .type = MSG_REQUEST_VOTE_RES, .to = msg->from, .term = r->current_term, .reject = true };

        uint64_t my_last_idx = raft_core_last_index(r);
        uint64_t my_last_term = log_term(r, my_last_idx);
        bool log_ok = (msg->log_term > my_last_term) || (msg->log_term == my_last_term && msg->index >= my_last_idx);

        if (msg->term == r->current_term && (r->voted_for == 0 || r->voted_for == msg->from) && log_ok) {
            r->voted_for = msg->from;
            res.reject = false;
            r->activity_accepted = true;
        }
        send_msg(r, res);
    }
    else if (msg->type == MSG_REQUEST_VOTE_RES && r->state == RAFT_STATE_CANDIDATE && !msg->reject) {
        for (size_t i = 0; i < r->num_peers; i++) {
            if (r->peers[i] == msg->from && !r->voted_for_me[i]) {
                r->voted_for_me[i] = true;
                r->votes_received++;
                if (r->votes_received >= (r->num_peers + 1) / 2 + 1) {
                    become_leader(r);
                }
                break;
            }
        }
    }
    else if (msg->type == MSG_PROPOSE && r->state == RAFT_STATE_LEADER) {
        if (msg->num_entries == 0 || msg->entries == NULL) return;
        uint64_t old_last_idx = raft_core_last_index(r);

        for (size_t i = 0; i < msg->num_entries; i++) {
            log_append(r, r->current_term, msg->entries[i].type, msg->entries[i].data, msg->entries[i].data_len);
        }
        if (r->num_peers == 0) {
            r->commit_index = raft_core_last_index(r);
            return;
        }
        for (size_t i = 0; i < r->num_peers; i++) {
            if (r->next_index[i] == old_last_idx + 1) {
                send_append(r, i);
            }
        }
    }
    else if (msg->type == MSG_APPEND_ENTRIES) {
        if (msg->num_entries > 0 && msg->entries == NULL) {
            raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = raft_core_last_index(r) };
            send_msg(r, res);
            return;
        }

        raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = raft_core_last_index(r) };

        if (msg->term >= r->current_term) {
            r->state = RAFT_STATE_FOLLOWER;
            r->activity_accepted = true;

            uint64_t my_last_idx = raft_core_last_index(r);

            if (msg->index >= r->snapshot_index && msg->index <= my_last_idx && log_term(r, msg->index) == msg->log_term) {
                res.reject = false;

                if (msg->num_entries > 0) {
                    for (size_t i = 0; i < msg->num_entries; i++) {
                        uint64_t new_idx = msg->index + 1 + i;
                        my_last_idx = raft_core_last_index(r);

                        if (new_idx <= my_last_idx && log_term(r, new_idx) == msg->entries[i].term) continue;
                        if (new_idx <= r->commit_index) {
                            res.reject = true;
                            break;
                        }
                        if (new_idx <= my_last_idx) log_truncate(r, new_idx);
                        log_append(r, msg->entries[i].term, msg->entries[i].type, msg->entries[i].data, msg->entries[i].data_len);
                    }
                }

                if (!res.reject) {
                    res.index = msg->index + msg->num_entries;
                    if (msg->commit > r->commit_index) {
                        r->commit_index = (msg->commit < res.index) ? msg->commit : res.index;
                    }
                }
            } else if (msg->index < r->snapshot_index) {
                res.reject = true;
                res.index = r->snapshot_index;
            }
        }
        send_msg(r, res);
    }
    else if (msg->type == MSG_INSTALL_SNAPSHOT) {
        raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = raft_core_last_index(r) };

        if (msg->term >= r->current_term) {
            r->state = RAFT_STATE_FOLLOWER;
            r->activity_accepted = true;

            if (msg->index > r->snapshot_index) {
                for (size_t i = 0; i < r->log_len; i++) {
                    if (r->log[i].data) free(r->log[i].data);
                }

                r->log_len = 1;
                r->snapshot_index = msg->index;
                r->snapshot_term = msg->log_term;

                r->log[0].index = msg->index;
                r->log[0].term = msg->log_term;
                r->log[0].data = NULL;
                r->log[0].data_len = 0;

                if (r->commit_index < msg->index) r->commit_index = msg->index;
                if (r->last_applied < msg->index) r->last_applied = msg->index;
            }
            res.reject = false;
            res.index = msg->index;
        }
        send_msg(r, res);
    }
    else if (msg->type == MSG_APPEND_RES && r->state == RAFT_STATE_LEADER) {
        if (msg->term != r->current_term) return;

        uint64_t my_last_idx = raft_core_last_index(r);
        if (!msg->reject && msg->index > my_last_idx) return;

        for (size_t i = 0; i < r->num_peers; i++) {
            if (r->peers[i] == msg->from) {
                if (msg->reject) {
                    // FAST BACKTRACKING (Gap 9): Jump next_index to the exact index the follower said it rejected
                    uint64_t new_next = msg->index;
                    if (new_next == 0) new_next = 1;

                    // Only jump backward. If the pipelining pushed next_index way ahead, safely reset it.
                    if (new_next < r->next_index[i]) {
                        r->next_index[i] = new_next;
                    } else {
                        // Fallback if the follower didn't provide a useful hint
                        r->next_index[i] = (r->next_index[i] > 1) ? r->next_index[i] - 1 : 1;
                    }
                    send_append(r, i);
                } else {                    uint64_t safe_idx = msg->index < my_last_idx ? msg->index : my_last_idx;
                    if (safe_idx >= r->match_index[i]) {
                        r->match_index[i] = safe_idx;
                        r->next_index[i] = safe_idx + 1;
                    }

                    uint64_t matches[MAX_PEERS + 1];
                    matches[0] = my_last_idx;
                    for (size_t j = 0; j < r->num_peers; j++) matches[j+1] = r->match_index[j];

                    qsort(matches, r->num_peers + 1, sizeof(uint64_t), cmp_u64);

                    // PHASE 1: Explicit Quorum Math prevents even-node cluster bug
                    size_t total = r->num_peers + 1;
                    size_t quorum = total / 2 + 1;
                    uint64_t candidate = matches[total - quorum];

                    if (candidate > r->commit_index && log_term(r, candidate) == r->current_term) {
                        r->commit_index = candidate;
                    }

                    if (r->next_index[i] <= my_last_idx) {
                        send_append(r, i);
                    }
                }
                break;
            }
        }
    }
}

raft_state_t raft_core_state(raft_core_t* r) { return r->state; }
uint64_t raft_core_term(raft_core_t* r) { return r->current_term; }
uint64_t raft_core_commit_index(raft_core_t* r) { return r->commit_index; }
uint64_t raft_core_voted_for(raft_core_t* r) { return r->voted_for; }
bool raft_core_activity_accepted(raft_core_t* r) { return r->activity_accepted; }

size_t raft_core_peers(raft_core_t* r, uint64_t* out_peers) {
    if (out_peers) {
        for (size_t i = 0; i < r->num_peers; i++) out_peers[i] = r->peers[i];
    }
    return r->num_peers;
}

// PHASE 1: Decoupling State Machine Apply
// We no longer automatically iterate config changes during standard boot or blind loops.
// Internal config application is strictly bounded by the host applications explicit acknowledgment.
void raft_core_advance(raft_core_t* r, uint64_t saved_index, uint64_t applied_index) {
    if (saved_index > r->last_saved_index) r->last_saved_index = saved_index;

    while (r->last_applied < applied_index) {
        uint64_t next = r->last_applied + 1;
        raft_entry_t* e = log_at(r, next);

        if (e && (e->type == ENTRY_CONF_ADD || e->type == ENTRY_CONF_REMOVE)) {
            apply_conf_change(r, e->type, e->data, e->data_len);
        }
        r->last_applied = next;
    }

    r->num_msgs = 0;
    r->activity_accepted = false;
}

void raft_core_advance_all(raft_core_t* r) {
    raft_core_advance(r, raft_core_last_index(r), raft_core_commit_index(r));
}

uint64_t raft_core_last_applied(raft_core_t* r) { return r->last_applied; }

raft_core_t* raft_core_restore(uint64_t id, uint64_t* peers, size_t num_peers,
                               uint64_t term, uint64_t voted_for, uint64_t commit_index, uint64_t applied_index,
                               raft_entry_t* entries, size_t num_entries) {
    if (num_entries == 0 || entries == NULL) return NULL;

    for (size_t i = 1; i < num_entries; i++) {
        if (entries[i].index <= entries[i-1].index) return NULL;
        if (entries[i].term < entries[i-1].term) return NULL;
    }

    raft_core_t* r = raft_core_create(id, peers, num_peers);
    if (!r) return NULL;

    r->current_term = term;
    r->voted_for = voted_for;

    r->snapshot_index = entries[0].index;
    r->snapshot_term = entries[0].term;

    if (r->log[0].data) free(r->log[0].data);
    r->log_len = 0;

    for (size_t i = 0; i < num_entries; i++) {
        log_append(r, entries[i].term, entries[i].type, entries[i].data, entries[i].data_len);
    }

    r->last_saved_index = raft_core_last_index(r);
    r->commit_index = commit_index;

    // PHASE 2: Lock the application boundary so the host isn't forced to double-execute
    r->last_applied = applied_index;

    return r;
}
