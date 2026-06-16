// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-raft-library/raft_core.h"
#include <stdlib.h>
#include <string.h>

#define MAX_PEERS 16
#define RAFT_MAX_APPEND_BATCH 2048

struct raft_core_s {
    uint64_t id;

    uint64_t init_peers[MAX_PEERS];
    size_t   num_init_peers;

    uint64_t peers[MAX_PEERS];
    size_t   num_peers;

    raft_state_t state;
    uint64_t current_term;
    uint64_t voted_for;

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

    bool activity_accepted; // PHASE 5: Track safe timeout resets
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

static uint64_t log_term(raft_core_t* r, uint64_t idx) {
    if (idx >= r->log_len) return 0;
    return r->log[idx].term;
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
            r->next_index[r->num_peers] = r->log_len;
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
    e->index = r->log_len;
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

static void log_truncate(raft_core_t* r, uint64_t index) {
    for (size_t i = index; i < r->log_len; i++) {
        if (r->log[i].data) free(r->log[i].data);
    }
    r->log_len = index;
    if (index > 0 && r->last_saved_index >= index) {
        r->last_saved_index = index - 1;
    }
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
        ready.committed_entries = &r->log[r->last_applied + 1];
        ready.num_committed_entries = r->commit_index - r->last_applied;
    }

    if (r->log_len - 1 > r->last_saved_index) {
        ready.entries_to_save = &r->log[r->last_saved_index + 1];
        ready.num_entries_to_save = (r->log_len - 1) - r->last_saved_index;
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
    uint64_t num_entries = r->log_len - (prev_idx + 1);

    // Clamp batch size to prevent max-frame network deadlocks
    if (num_entries > RAFT_MAX_APPEND_BATCH) {
        num_entries = RAFT_MAX_APPEND_BATCH;
    }

    // Optimistically advance next_index so we can pipeline payloads
    r->next_index[peer_idx] = prev_idx + 1 + num_entries;

    raft_msg_t app = { .type = MSG_APPEND_ENTRIES, .to = r->peers[peer_idx], .term = r->current_term,
                       .index = prev_idx, .log_term = log_term(r, prev_idx),
                       .commit = r->commit_index,
                       .entries = num_entries > 0 ? &r->log[prev_idx + 1] : NULL,
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

    if (r->num_peers == 0) {
        r->commit_index = r->log_len - 1;
        return;
    }
    for (size_t i = 0; i < r->num_peers; i++) {
        r->next_index[i] = r->log_len;
        r->match_index[i] = 0;
    }
    bcast_append(r);
}

void raft_core_step(raft_core_t* r, raft_msg_t* msg) {
    if (msg->to != 0 && msg->to != r->id) return;

    if (msg->term > 0 && msg->term < r->current_term) {
        if (msg->type == MSG_APPEND_ENTRIES) {
            raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = r->log_len - 1 };
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

        uint64_t last_idx = r->log_len - 1;
        for (size_t i = 0; i < r->num_peers; i++) {
            raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = r->peers[i], .term = r->current_term,
                               .index = last_idx, .log_term = log_term(r, last_idx) };
            send_msg(r, req);
        }
    }
    else if (msg->type == MSG_REQUEST_VOTE) {
        raft_msg_t res = { .type = MSG_REQUEST_VOTE_RES, .to = msg->from, .term = r->current_term, .reject = true };

        uint64_t my_last_idx = r->log_len - 1;
        uint64_t my_last_term = log_term(r, my_last_idx);
        bool log_ok = (msg->log_term > my_last_term) || (msg->log_term == my_last_term && msg->index >= my_last_idx);

        if (msg->term == r->current_term && (r->voted_for == 0 || r->voted_for == msg->from) && log_ok) {
            r->voted_for = msg->from;
            res.reject = false;
            r->activity_accepted = true; // PHASE 5: We granted a vote, reset timer safely
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
        uint64_t old_log_len = r->log_len;
        for (size_t i = 0; i < msg->num_entries; i++) {
            log_append(r, r->current_term, msg->entries[i].type, msg->entries[i].data, msg->entries[i].data_len);
        }
        if (r->num_peers == 0) {
            r->commit_index = r->log_len - 1;
            return;
        }
        for (size_t i = 0; i < r->num_peers; i++) {
            if (r->next_index[i] == old_log_len) {
                // Was fully caught up before we appended. Send the new paginated batch!
                send_append(r, i);
            }
        }
    }
    else if (msg->type == MSG_APPEND_ENTRIES) {
        if (msg->num_entries > 0 && msg->entries == NULL) {
            raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = r->log_len - 1 };
            send_msg(r, res);
            return;
        }

        raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = r->log_len - 1 };

        if (msg->term >= r->current_term) {
            r->state = RAFT_STATE_FOLLOWER;
            r->activity_accepted = true; // PHASE 5: Recognized valid leader

            if (msg->index < r->log_len && log_term(r, msg->index) == msg->log_term) {
                res.reject = false;

                if (msg->num_entries > 0) {
                    for (size_t i = 0; i < msg->num_entries; i++) {
                        uint64_t new_idx = msg->index + 1 + i;
                        if (new_idx < r->log_len && log_term(r, new_idx) == msg->entries[i].term) continue;
                        if (new_idx <= r->commit_index) {
                            res.reject = true;
                            break;
                        }
                        if (new_idx < r->log_len) log_truncate(r, new_idx);
                        log_append(r, msg->entries[i].term, msg->entries[i].type, msg->entries[i].data, msg->entries[i].data_len);
                    }
                }

                if (!res.reject) {
                    res.index = msg->index + msg->num_entries;
                    if (msg->commit > r->commit_index) {
                        r->commit_index = (msg->commit < res.index) ? msg->commit : res.index;
                    }
                }
            }
        }
        send_msg(r, res);
    }
    else if (msg->type == MSG_APPEND_RES && r->state == RAFT_STATE_LEADER) {
        if (msg->term != r->current_term) return;
        if (!msg->reject && msg->index >= r->log_len) return;

        for (size_t i = 0; i < r->num_peers; i++) {
            if (r->peers[i] == msg->from) {
                if (msg->reject) {
                    // Backtrack and resend the paginated chunk
                    r->next_index[i] = (r->next_index[i] > 1) ? r->next_index[i] - 1 : 1;
                    send_append(r, i);
                } else {
                    uint64_t safe_idx = msg->index < r->log_len ? msg->index : r->log_len - 1;
                    if (safe_idx >= r->match_index[i]) {
                        r->match_index[i] = safe_idx;
                        r->next_index[i] = safe_idx + 1;
                    }

                    uint64_t matches[MAX_PEERS + 1];
                    matches[0] = r->log_len - 1;
                    for (size_t j = 0; j < r->num_peers; j++) matches[j+1] = r->match_index[j];

                    qsort(matches, r->num_peers + 1, sizeof(uint64_t), cmp_u64);
                    uint64_t median = matches[(r->num_peers + 1) / 2];

                    if (median > r->commit_index && log_term(r, median) == r->current_term) {
                        r->commit_index = median;
                    }

                    // FAST CATCH-UP: If the follower is still behind, pipeline the next batch immediately!
                    if (r->next_index[i] < r->log_len) {
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
uint64_t raft_core_last_index(raft_core_t* r) { return r->log_len - 1; }
uint64_t raft_core_voted_for(raft_core_t* r) { return r->voted_for; }
bool raft_core_activity_accepted(raft_core_t* r) { return r->activity_accepted; }

size_t raft_core_peers(raft_core_t* r, uint64_t* out_peers) {
    if (out_peers) {
        for (size_t i = 0; i < r->num_peers; i++) out_peers[i] = r->peers[i];
    }
    return r->num_peers;
}

void raft_core_apply(raft_core_t* r) {
    while (r->last_applied < r->commit_index) {
        uint64_t next = r->last_applied + 1;
        if (r->log[next].type == ENTRY_CONF_ADD || r->log[next].type == ENTRY_CONF_REMOVE) {
            apply_conf_change(r, r->log[next].type, r->log[next].data, r->log[next].data_len);
        }
        r->last_applied = next;
    }
}

// PHASE 5: Explicit Advancement
void raft_core_advance(raft_core_t* r, uint64_t saved_index, uint64_t applied_index) {
    if (saved_index > r->last_saved_index) r->last_saved_index = saved_index;
    if (applied_index > r->last_applied) r->last_applied = applied_index;
    r->num_msgs = 0;
    r->activity_accepted = false;
}

void raft_core_advance_all(raft_core_t* r) {
    // Automatically advance to the absolute limits of the current state
    raft_core_advance(r, raft_core_last_index(r), raft_core_commit_index(r));
}

// PHASE 5: Config Replay on Boot
raft_core_t* raft_core_restore(uint64_t id, uint64_t* peers, size_t num_peers,
                               uint64_t term, uint64_t voted_for, uint64_t commit_index,
                               raft_entry_t* entries, size_t num_entries) {
    if (num_entries == 0 || entries == NULL) return NULL;
    if (entries[0].index != 0) return NULL;

    for (size_t i = 1; i < num_entries; i++) {
        if (entries[i].index != i) return NULL;
        if (entries[i].term < entries[i-1].term) return NULL;
    }

    raft_core_t* r = raft_core_create(id, peers, num_peers);
    if (!r) return NULL;

    r->current_term = term;
    r->voted_for = voted_for;

    if (r->log[0].data) free(r->log[0].data);
    r->log_len = 0;

    for (size_t i = 0; i < num_entries; i++) {
        log_append(r, entries[i].term, entries[i].type, entries[i].data, entries[i].data_len);
    }

    r->last_saved_index = r->log_len - 1;
    r->commit_index = commit_index;

    // Explicitly replay committed configuration changes!
    raft_core_apply(r);

    return r;
}
