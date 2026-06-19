// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "a-raft-library/raft_core.h"
#include <stdlib.h>
#include <string.h>

#define MAX_PEERS 64
#define MAX_PENDING_READS 128

typedef struct {
    uint64_t read_seq;
    uint64_t client_ctx;
    uint64_t index;
    uint64_t from;
    size_t acks;
    bool acked_by[MAX_PEERS];
    bool active;
} pending_read_t;

struct raft_core_s {
    uint64_t id;
    raft_state_t state;

    uint64_t current_term;
    uint64_t voted_for;

    raft_entry_t* log;
    size_t log_cap;
    size_t log_len;
    uint64_t snapshot_index;
    uint64_t snapshot_term;

    uint64_t commit_index;
    uint64_t last_applied;

    uint64_t peers[MAX_PEERS];
    size_t num_peers;

    uint64_t next_index[MAX_PEERS];
    uint64_t match_index[MAX_PEERS];

    size_t votes_received;
    bool voted_for_me[MAX_PEERS];

    raft_msg_t* msg_queue;
    size_t msg_queue_cap;
    size_t msg_queue_len;

    uint64_t last_saved_index;
    bool activity_accepted;

    bool recent_active[MAX_PEERS];
    bool is_learner[MAX_PEERS];
    bool is_learner_self;
    bool removed;

    pending_read_t pending_reads[MAX_PENDING_READS];
    uint64_t current_read_seq;
    uint64_t peer_read_seq[MAX_PEERS];

    raft_read_state_t* read_states;
    size_t read_states_cap;
    size_t num_read_states;

    uint64_t term_start_index;
    uint64_t leader_id;

    bool pending_snapshot;
    uint8_t* pending_snapshot_data;
    size_t pending_snapshot_len;
};

static void send_msg(raft_core_t* r, raft_msg_t msg) {
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

static uint64_t raft_core_last_index_internal(raft_core_t* r) {
    if (r->log_len > 0) return r->log[r->log_len - 1].index;
    return r->snapshot_index;
}

static uint64_t log_term(raft_core_t* r, uint64_t index) {
    if (index == r->snapshot_index) return r->snapshot_term;
    if (index < r->snapshot_index || index > raft_core_last_index_internal(r)) return 0;
    return r->log[index - r->snapshot_index].term;
}

static raft_entry_t* log_get(raft_core_t* r, uint64_t index) {
    if (index <= r->snapshot_index || index > raft_core_last_index_internal(r)) return NULL;
    return &r->log[index - r->snapshot_index];
}

static void log_append(raft_core_t* r, uint64_t term, entry_type_t type, uint64_t cid, uint64_t cseq, const uint8_t* data, size_t data_len) {
    if (r->log_len >= r->log_cap) {
        size_t new_cap = r->log_cap == 0 ? 16 : r->log_cap * 2;
        raft_entry_t* new_log = realloc(r->log, new_cap * sizeof(raft_entry_t));
        if (!new_log) return;
        r->log = new_log;
        r->log_cap = new_cap;
    }
    uint64_t next_idx = raft_core_last_index_internal(r) + 1;
    raft_entry_t* e = &r->log[r->log_len++];
    e->term = term;
    e->index = next_idx;
    e->type = type;
    e->client_id = cid;
    e->client_seq = cseq;
    e->data_len = data_len;
    e->data = data_len > 0 ? malloc(data_len) : NULL;
    if (data_len > 0 && e->data) memcpy(e->data, data, data_len);
}

static void log_truncate(raft_core_t* r, uint64_t index) {
    if (index <= r->snapshot_index) return;
    size_t new_len = index - r->snapshot_index;
    for (size_t i = new_len; i < r->log_len; i++) {
        if (r->log[i].data) free(r->log[i].data);
    }
    r->log_len = new_len;
    if (r->last_saved_index >= index) r->last_saved_index = index - 1;
}

static void send_append(raft_core_t* r, size_t peer_idx) {
    uint64_t peer_id = r->peers[peer_idx];
    uint64_t next = r->next_index[peer_idx];
    uint64_t last = raft_core_last_index_internal(r);

    if (next <= r->snapshot_index) return;

    raft_msg_t msg = { .type = MSG_APPEND_ENTRIES, .to = peer_id, .term = r->current_term,
                       .index = next - 1, .log_term = log_term(r, next - 1), .commit = r->commit_index,
                       .read_seq = r->current_read_seq };

    size_t num_entries = last >= next ? last - next + 1 : 0;
    if (num_entries > 500) num_entries = 500; // Hard cap on object count

    // PHASE 10: Byte-Bounded Batching (Cap payload frame size strictly to 1MB)
    size_t batch_bytes = 0;
    size_t actual_entries = 0;
    for (size_t i = 0; i < num_entries; i++) {
        raft_entry_t* src = log_get(r, next + i);
        if (!src) break;
        if (batch_bytes + src->data_len > 1048576) {
            if (actual_entries == 0) actual_entries = 1; // Guarantee progress on massive entries
            break;
        }
        batch_bytes += src->data_len;
        actual_entries++;
    }
    num_entries = actual_entries;

    msg.num_entries = num_entries;
    msg.entries = num_entries > 0 ? malloc(num_entries * sizeof(raft_entry_t)) : NULL;

    for (size_t i = 0; i < num_entries; i++) {
        raft_entry_t* src = log_get(r, next + i);
        msg.entries[i].term = src->term;
        msg.entries[i].index = src->index;
        msg.entries[i].type = src->type;
        msg.entries[i].client_id = src->client_id;
        msg.entries[i].client_seq = src->client_seq;
        msg.entries[i].data_len = src->data_len;
        msg.entries[i].data = src->data_len > 0 ? malloc(src->data_len) : NULL;
        if (src->data_len > 0 && msg.entries[i].data) memcpy(msg.entries[i].data, src->data, src->data_len);
    }

    r->next_index[peer_idx] = next + num_entries;
    send_msg(r, msg);
}

static void bcast_append(raft_core_t* r) {
    for (size_t i = 0; i < r->num_peers; i++) send_append(r, i);
}

static void advance_commit_index(raft_core_t* r, uint64_t new_commit) {
    if (new_commit <= r->commit_index) return;
    r->commit_index = new_commit;
}

static void become_leader(raft_core_t* r) {
    r->state = RAFT_STATE_LEADER;
    r->activity_accepted = true;
    r->leader_id = r->id;

    uint8_t dummy = 0;
    log_append(r, r->current_term, ENTRY_NORMAL, 0, 0, &dummy, 0);
    r->term_start_index = raft_core_last_index_internal(r);

    for (size_t i = 0; i < r->num_peers; i++) {
        r->next_index[i] = r->term_start_index + 1;
        r->match_index[i] = 0;
    }

    size_t total_voters = r->is_learner_self ? 0 : 1;
    for (size_t i = 0; i < r->num_peers; i++) if (!r->is_learner[i]) total_voters++;

    if (total_voters <= 1 && !r->is_learner_self) advance_commit_index(r, r->term_start_index);

    bcast_append(r);
}

static int cmp_u64(const void* a, const void* b) {
    uint64_t ua = *(const uint64_t*)a;
    uint64_t ub = *(const uint64_t*)b;
    if (ua < ub) return -1;
    if (ua > ub) return 1;
    return 0;
}

raft_core_t* raft_core_create(uint64_t id, uint64_t* peers, size_t num_peers) {
    if (num_peers > MAX_PEERS || (num_peers > 0 && !peers)) return NULL;
    for (size_t i = 0; i < num_peers; i++) {
        if (peers[i] == id) return NULL;
        for (size_t j = i + 1; j < num_peers; j++) {
            if (peers[i] == peers[j]) return NULL;
        }
    }

    raft_core_t* r = calloc(1, sizeof(raft_core_t));
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

void raft_core_destroy(raft_core_t* r) {
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

raft_core_t* raft_core_restore(uint64_t id, uint64_t* peers, bool* is_learners, size_t num_peers,
                               uint64_t term, uint64_t voted_for, uint64_t commit_index, uint64_t applied_index,
                               uint64_t snapshot_index, uint64_t snapshot_term,
                               raft_entry_t* entries, size_t num_entries) {
    if (num_peers > MAX_PEERS) return NULL;
    if (num_entries > 0 && entries == NULL) return NULL;

    for (size_t i = 1; i < num_entries; i++) {
        if (entries[i].index <= entries[i-1].index) return NULL;
        if (entries[i].term < entries[i-1].term) return NULL;
    }

    raft_core_t* r = raft_core_create(id, NULL, 0);
    if (!r) return NULL;

    r->num_peers = num_peers;
    for (size_t i = 0; i < num_peers; i++) {
        r->peers[i] = peers[i];
        r->is_learner[i] = is_learners ? is_learners[i] : false;
        r->next_index[i] = snapshot_index + 1;
        if (peers[i] == id) r->is_learner_self = r->is_learner[i];
    }

    r->current_term = term;
    r->voted_for = voted_for;
    r->snapshot_index = snapshot_index;
    r->snapshot_term = snapshot_term;

    if (r->log[0].data) free(r->log[0].data);
    r->log_len = 0;

    r->log_cap = num_entries > 16 ? num_entries * 2 : 16;
    raft_entry_t* new_log = realloc(r->log, r->log_cap * sizeof(raft_entry_t));
    if (!new_log) { raft_core_destroy(r); return NULL; }
    r->log = new_log;

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

    r->last_saved_index = raft_core_last_index_internal(r);
    r->commit_index = commit_index;
    r->last_applied = applied_index;

    return r;
}

static void apply_config_change(raft_core_t* r, uint64_t index) {
    size_t arr_idx = index - r->snapshot_index;
    if (arr_idx >= r->log_len) return;

    uint8_t type = r->log[arr_idx].type;
    uint8_t* data = r->log[arr_idx].data;
    uint32_t data_len = r->log[arr_idx].data_len;

    if (type == ENTRY_CONF_ADD || type == ENTRY_CONF_ADD_LEARNER) {
        if (data_len == sizeof(uint64_t)) {
            uint64_t node_id = *(uint64_t*)data;
            if (node_id == r->id) r->is_learner_self = (type == ENTRY_CONF_ADD_LEARNER);

            bool found = false;
            for (size_t j = 0; j < r->num_peers; j++) {
                if (r->peers[j] == node_id) {
                    r->is_learner[j] = (type == ENTRY_CONF_ADD_LEARNER);
                    found = true;
                    break;
                }
            }
            if (!found && node_id != r->id && r->num_peers < MAX_PEERS) {
                r->peers[r->num_peers] = node_id;
                r->is_learner[r->num_peers] = (type == ENTRY_CONF_ADD_LEARNER);
                r->next_index[r->num_peers] = raft_core_last_index_internal(r) + 1;
                r->match_index[r->num_peers] = 0;
                r->recent_active[r->num_peers] = true;
                r->num_peers++;
            }
        }
    } else if (type == ENTRY_CONF_REMOVE) {
        if (data_len == sizeof(uint64_t)) {
            uint64_t node_id = *(uint64_t*)data;
            if (node_id == r->id) {
                if (r->state == RAFT_STATE_LEADER) r->state = RAFT_STATE_FOLLOWER;
                r->is_learner_self = true;
                r->removed = true;
            } else {
                for (size_t i = 0; i < r->num_peers; i++) {
                    if (r->peers[i] == node_id) {
                        r->num_peers--;
                        r->peers[i] = r->peers[r->num_peers];
                        r->is_learner[i] = r->is_learner[r->num_peers];
                        r->next_index[i] = r->next_index[r->num_peers];
                        r->match_index[i] = r->match_index[r->num_peers];
                        r->recent_active[i] = r->recent_active[r->num_peers];
                        break;
                    }
                }
            }
        }
    }
}

void raft_core_step(raft_core_t* r, raft_msg_t* msg) {
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
            raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = raft_core_last_index_internal(r) };
            send_msg(r, res);
        } else if (msg->type == MSG_REQUEST_VOTE) {
            raft_msg_t res = { .type = MSG_REQUEST_VOTE_RES, .to = msg->from, .term = r->current_term, .reject = true };
            send_msg(r, res);
        } else if (msg->type == MSG_PRE_VOTE) {
            raft_msg_t res = { .type = MSG_PRE_VOTE_RES, .to = msg->from, .term = r->current_term, .reject = true };
            send_msg(r, res);
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

    if (msg->type == MSG_TICK) {
        if (r->state == RAFT_STATE_LEADER) bcast_append(r);
        return;
    }

    if (msg->type == MSG_CHECK_QUORUM) {
        if (r->state == RAFT_STATE_LEADER) {
            size_t active = 1;
            size_t total = r->is_learner_self ? 0 : 1;
            for (size_t i = 0; i < r->num_peers; i++) {
                if (!r->is_learner[i]) {
                    total++;
                    if (r->recent_active[i]) active++;
                }
                r->recent_active[i] = false;
            }
            if (active < (total / 2 + 1)) {
                r->state = RAFT_STATE_FOLLOWER;
            }
        }
        return;
    }

    if (msg->type == MSG_READ_INDEX) {
        if (r->state != RAFT_STATE_LEADER) return;
        if (r->commit_index < r->term_start_index || log_term(r, r->commit_index) != r->current_term) return;

        r->current_read_seq++;
        bool slot_found = false;
        for (int pr = 0; pr < MAX_PENDING_READS; pr++) {
            if (!r->pending_reads[pr].active) {
                r->pending_reads[pr].active = true;
                r->pending_reads[pr].read_seq = r->current_read_seq;
                r->pending_reads[pr].client_ctx = msg->read_seq;
                r->pending_reads[pr].index = r->commit_index;
                r->pending_reads[pr].from = msg->from != 0 ? msg->from : r->id;
                r->pending_reads[pr].acks = 1;
                memset(r->pending_reads[pr].acked_by, 0, sizeof(r->pending_reads[pr].acked_by));
                slot_found = true;
                break;
            }
        }

        if (slot_found) {
            size_t total_voters = r->is_learner_self ? 0 : 1;
            for(size_t v = 0; v < r->num_peers; v++) if (!r->is_learner[v]) total_voters++;

            if (total_voters <= 1) {
                for (int pr = 0; pr < MAX_PENDING_READS; pr++) {
                    if (r->pending_reads[pr].active && r->pending_reads[pr].read_seq == r->current_read_seq) {
                        if (r->pending_reads[pr].from == r->id) {
                            if (r->num_read_states >= r->read_states_cap) {
                                r->read_states_cap = r->read_states_cap == 0 ? 16 : r->read_states_cap * 2;
                                raft_read_state_t* new_rs = realloc(r->read_states, r->read_states_cap * sizeof(raft_read_state_t));
                                if (new_rs) r->read_states = new_rs;
                            }
                            if (r->read_states) {
                                r->read_states[r->num_read_states].index = r->pending_reads[pr].index;
                                r->read_states[r->num_read_states].read_seq = r->pending_reads[pr].client_ctx;
                                r->num_read_states++;
                            }
                        } else {
                            raft_msg_t res = { .type = MSG_READ_INDEX_RES, .to = r->pending_reads[pr].from,
                                               .read_seq = r->pending_reads[pr].client_ctx, .index = r->pending_reads[pr].index, .reject = false };
                            send_msg(r, res);
                        }
                        r->pending_reads[pr].active = false;
                    }
                }
            } else {
                bcast_append(r);
            }
        }
        return;
    }
    else if (msg->type == MSG_READ_INDEX_RES) {
        if (r->num_read_states >= r->read_states_cap) {
            r->read_states_cap = r->read_states_cap == 0 ? 16 : r->read_states_cap * 2;
            raft_read_state_t* new_rs = realloc(r->read_states, r->read_states_cap * sizeof(raft_read_state_t));
            if (new_rs) r->read_states = new_rs;
        }
        if (r->read_states) {
            r->read_states[r->num_read_states].index = msg->index;
            r->read_states[r->num_read_states].read_seq = msg->read_seq;
            r->num_read_states++;
        }
        return;
    }

    if (msg->type == MSG_HUP) {
        if (r->state == RAFT_STATE_LEADER || r->is_learner_self || r->removed) return;

        r->state = RAFT_STATE_PRE_CANDIDATE;
        r->votes_received = 1;
        memset(r->voted_for_me, 0, sizeof(r->voted_for_me));

        size_t total_voters = 1;
        for (size_t i = 0; i < r->num_peers; i++) if (!r->is_learner[i]) total_voters++;

        if (total_voters == 1) {
            r->state = RAFT_STATE_CANDIDATE;
            r->current_term++;
            r->voted_for = r->id;
            become_leader(r);
            return;
        }

        uint64_t last_idx = raft_core_last_index_internal(r);
        for (size_t i = 0; i < r->num_peers; i++) {
            if (r->is_learner[i]) continue;
            raft_msg_t req = { .type = MSG_PRE_VOTE, .to = r->peers[i], .term = r->current_term + 1,
                               .index = last_idx, .log_term = log_term(r, last_idx) };
            send_msg(r, req);
        }
    }
    else if (msg->type == MSG_PRE_VOTE) {
        raft_msg_t res = { .type = MSG_PRE_VOTE_RES, .to = msg->from, .term = r->current_term, .reject = true };
        uint64_t my_last_idx = raft_core_last_index_internal(r);
        uint64_t my_last_term = log_term(r, my_last_idx);
        bool log_ok = (msg->log_term > my_last_term) || (msg->log_term == my_last_term && msg->index >= my_last_idx);

        if (msg->term > r->current_term && log_ok && !r->is_learner_self) {
            res.reject = false;
            res.term = msg->term;
        }
        send_msg(r, res);
    }
    else if (msg->type == MSG_PRE_VOTE_RES && r->state == RAFT_STATE_PRE_CANDIDATE) {
        if (msg->reject && msg->term > r->current_term) {
            r->current_term = msg->term;
            r->voted_for = 0;
            r->state = RAFT_STATE_FOLLOWER;
        } else if (!msg->reject && msg->term == r->current_term + 1) {
            for (size_t i = 0; i < r->num_peers; i++) {
                if (r->peers[i] == msg->from && !r->voted_for_me[i] && !r->is_learner[i]) {
                    r->voted_for_me[i] = true;
                    r->votes_received++;

                    size_t total_voters = 1;
                    for (size_t j = 0; j < r->num_peers; j++) if (!r->is_learner[j]) total_voters++;

                    if (r->votes_received >= (total_voters / 2) + 1) {
                        r->state = RAFT_STATE_CANDIDATE;
                        r->current_term++;
                        r->voted_for = r->id;
                        r->votes_received = 1;
                        memset(r->voted_for_me, 0, sizeof(r->voted_for_me));

                        uint64_t last_idx = raft_core_last_index_internal(r);
                        for (size_t j = 0; j < r->num_peers; j++) {
                            if (r->is_learner[j]) continue;
                            raft_msg_t req = { .type = MSG_REQUEST_VOTE, .to = r->peers[j], .term = r->current_term,
                                               .index = last_idx, .log_term = log_term(r, last_idx) };
                            send_msg(r, req);
                        }
                    }
                    break;
                }
            }
        }
    }
    else if (msg->type == MSG_REQUEST_VOTE) {
        raft_msg_t res = { .type = MSG_REQUEST_VOTE_RES, .to = msg->from, .term = r->current_term, .reject = true };

        uint64_t my_last_idx = raft_core_last_index_internal(r);
        uint64_t my_last_term = log_term(r, my_last_idx);
        bool log_ok = (msg->log_term > my_last_term) || (msg->log_term == my_last_term && msg->index >= my_last_idx);

        if (msg->term == r->current_term && (r->voted_for == 0 || r->voted_for == msg->from) && log_ok && !r->is_learner_self) {
            r->voted_for = msg->from;
            res.reject = false;
            r->activity_accepted = true;
        }
        send_msg(r, res);
    }
    else if (msg->type == MSG_REQUEST_VOTE_RES && r->state == RAFT_STATE_CANDIDATE && !msg->reject) {
        for (size_t i = 0; i < r->num_peers; i++) {
            if (r->peers[i] == msg->from && !r->voted_for_me[i] && !r->is_learner[i]) {
                r->voted_for_me[i] = true;
                r->votes_received++;

                size_t total_voters = 1;
                for (size_t j = 0; j < r->num_peers; j++) if (!r->is_learner[j]) total_voters++;

                if (r->votes_received >= (total_voters / 2) + 1) {
                    become_leader(r);
                }
                break;
            }
        }
    }
    else if (msg->type == MSG_PROPOSE && r->state == RAFT_STATE_LEADER) {
        if (msg->num_entries == 0 || msg->entries == NULL) return;

        uint64_t old_last_idx = raft_core_last_index_internal(r);
        bool has_pending_config = false;

        for (uint64_t idx = r->commit_index + 1; idx <= old_last_idx; idx++) {
            raft_entry_t* e = log_get(r, idx);
            if (e && e->type != ENTRY_NORMAL) {
                has_pending_config = true;
                break;
            }
        }

        bool appended = false;
        for (size_t i = 0; i < msg->num_entries; i++) {
            if (msg->entries[i].type != ENTRY_NORMAL) {
                if (has_pending_config) continue;
                has_pending_config = true;
            }
            log_append(r, r->current_term, msg->entries[i].type, msg->entries[i].client_id, msg->entries[i].client_seq, msg->entries[i].data, msg->entries[i].data_len);
            appended = true;
        }

        if (!appended) return;

        if (r->num_peers == 0) {
            advance_commit_index(r, raft_core_last_index_internal(r));
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
            raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = raft_core_last_index_internal(r) };
            send_msg(r, res);
            return;
        }

        raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = raft_core_last_index_internal(r), .read_seq = 0 };

        if (msg->term >= r->current_term) {
            r->state = RAFT_STATE_FOLLOWER;
            r->activity_accepted = true;

            uint64_t my_last_idx = raft_core_last_index_internal(r);

            if (msg->index >= r->snapshot_index && msg->index <= my_last_idx && log_term(r, msg->index) == msg->log_term) {
                res.reject = false;

                if (msg->num_entries > 0) {
                    for (size_t i = 0; i < msg->num_entries; i++) {
                        uint64_t new_idx = msg->index + 1 + i;
                        my_last_idx = raft_core_last_index_internal(r);

                        if (new_idx <= my_last_idx && log_term(r, new_idx) == msg->entries[i].term) continue;
                        if (new_idx <= r->commit_index) {
                            res.reject = true;
                            break;
                        }
                        if (new_idx <= my_last_idx) log_truncate(r, new_idx);

                        log_append(r, msg->entries[i].term, msg->entries[i].type, msg->entries[i].client_id, msg->entries[i].client_seq, msg->entries[i].data, msg->entries[i].data_len);
                    }
                }

                if (!res.reject) {
                    res.index = msg->index + msg->num_entries;
                    if (msg->commit > r->commit_index) {
                        advance_commit_index(r, (msg->commit < res.index) ? msg->commit : res.index);
                    }
                    res.read_seq = msg->read_seq;
                }
            } else if (msg->index < r->snapshot_index) {
                res.reject = true;
                res.index = r->snapshot_index;

                // PHASE 10: Deep Conflict Hint triggers InstallSnapshot instantly
                res.conflict_term = 0;
                res.conflict_index = r->snapshot_index + 1;
            } else {
                res.reject = true;
                if (msg->index > my_last_idx) {
                    res.conflict_index = my_last_idx + 1;
                    res.conflict_term = 0;
                } else {
                    res.conflict_term = log_term(r, msg->index);
                    uint64_t first_idx = msg->index;
                    while (first_idx >= r->snapshot_index) {
                        if (log_term(r, first_idx) == res.conflict_term) {
                            while (first_idx > r->snapshot_index && log_term(r, first_idx - 1) == res.conflict_term) {
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
        }
        send_msg(r, res);
    }
    else if (msg->type == MSG_INSTALL_SNAPSHOT) {
        raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = raft_core_last_index_internal(r) };

        if (msg->term >= r->current_term) {
            r->state = RAFT_STATE_FOLLOWER;
            r->activity_accepted = true;

            if (msg->index > r->snapshot_index) {
                bool suffix_match = false;
                uint64_t my_last_idx = raft_core_last_index_internal(r);
                if (msg->index <= my_last_idx && log_term(r, msg->index) == msg->log_term) {
                    suffix_match = true;
                }

                if (suffix_match) {
                    size_t keep_len = my_last_idx - msg->index + 1;
                    for (uint64_t i = r->snapshot_index + 1; i <= msg->index; i++) {
                        raft_entry_t* e = log_get(r, i);
                        if (e && e->data) free(e->data);
                    }
                    memmove(&r->log[1], &r->log[msg->index - r->snapshot_index + 1], (keep_len - 1) * sizeof(raft_entry_t));
                    r->log_len = keep_len;
                } else {
                    for (size_t i = 0; i < r->log_len; i++) {
                        if (r->log[i].data) free(r->log[i].data);
                    }
                    r->log_len = 1;
                }

                r->snapshot_index = msg->index;
                r->snapshot_term = msg->log_term;
                r->log[0].index = msg->index;
                r->log[0].term = msg->log_term;
                r->log[0].data = NULL;
                r->log[0].data_len = 0;

                r->pending_snapshot = true;
                if (r->pending_snapshot_data) free(r->pending_snapshot_data);
                r->pending_snapshot_data = msg->snapshot_len > 0 ? malloc(msg->snapshot_len) : NULL;
                if (msg->snapshot_len > 0 && r->pending_snapshot_data) {
                    memcpy(r->pending_snapshot_data, msg->snapshot_data, msg->snapshot_len);
                }
                r->pending_snapshot_len = msg->snapshot_len;

                // PHASE 11 FIX: Only ACK if the snapshot was actually installed!
                res.reject = false;
                res.index = msg->index;
            } else {
                // PHASE 11 FIX: Explicitly reject redundant snapshots
                res.reject = true;
                res.index = r->snapshot_index;
            }
        }
        send_msg(r, res);
    }
    else if (msg->type == MSG_APPEND_RES && r->state == RAFT_STATE_LEADER) {
        if (msg->term != r->current_term) return;

        uint64_t my_last_idx = raft_core_last_index_internal(r);

        for (size_t i = 0; i < r->num_peers; i++) {
            if (r->peers[i] == msg->from) {

                if (msg->read_seq > r->peer_read_seq[i]) r->peer_read_seq[i] = msg->read_seq;

                for (int pr = 0; pr < MAX_PENDING_READS; pr++) {
                    if (r->pending_reads[pr].active && !r->pending_reads[pr].acked_by[i]) {
                        if (r->peer_read_seq[i] >= r->pending_reads[pr].read_seq) {
                            r->pending_reads[pr].acked_by[i] = true;
                            if (!r->is_learner[i]) r->pending_reads[pr].acks++;

                            size_t voters = r->is_learner_self ? 0 : 1;
                            for (size_t j = 0; j < r->num_peers; j++) if (!r->is_learner[j]) voters++;

                            if (r->pending_reads[pr].acks >= (voters / 2) + 1) {
                                if (r->pending_reads[pr].from == r->id) {
                                    if (r->num_read_states >= r->read_states_cap) {
                                        r->read_states_cap = r->read_states_cap == 0 ? 16 : r->read_states_cap * 2;
                                        raft_read_state_t* new_rs = realloc(r->read_states, r->read_states_cap * sizeof(raft_read_state_t));
                                        if (new_rs) r->read_states = new_rs;
                                    }
                                    if (r->read_states) {
                                        r->read_states[r->num_read_states].index = r->pending_reads[pr].index;
                                        r->read_states[r->num_read_states].read_seq = r->pending_reads[pr].client_ctx;
                                        r->num_read_states++;
                                    }
                                } else {
                                    raft_msg_t rd_res = { .type = MSG_READ_INDEX_RES, .to = r->pending_reads[pr].from,
                                                          .read_seq = r->pending_reads[pr].client_ctx, .index = r->pending_reads[pr].index, .reject = false };
                                    send_msg(r, rd_res);
                                }
                                r->pending_reads[pr].active = false;
                            }
                        }
                    }
                }

                if (!msg->reject && msg->index > my_last_idx) return;

                if (msg->reject) {
                    if (msg->conflict_term == 0) {
                        r->next_index[i] = msg->conflict_index;
                    } else {
                        uint64_t last_idx = 0;
                        bool found = false;

                        uint64_t idx = my_last_idx;
                        while (idx >= r->snapshot_index) {
                            if (log_term(r, idx) == msg->conflict_term) {
                                last_idx = idx;
                                found = true;
                                break;
                            }
                            if (idx == r->snapshot_index || idx == 0) break;
                            idx--;
                        }

                        if (found) {
                            r->next_index[i] = last_idx + 1;
                        } else {
                            r->next_index[i] = msg->conflict_index;
                        }
                    }
                    if (r->next_index[i] < 1) r->next_index[i] = 1;
                    send_append(r, i);
                } else {
                    uint64_t safe_idx = msg->index < my_last_idx ? msg->index : my_last_idx;
                    if (safe_idx >= r->match_index[i]) {
                        r->match_index[i] = safe_idx;
                        r->next_index[i] = safe_idx + 1;
                    }

                    uint64_t matches[MAX_PEERS + 1];
                    size_t voters = 0;
                    if (!r->is_learner_self) matches[voters++] = my_last_idx;
                    for (size_t j = 0; j < r->num_peers; j++) {
                        if (!r->is_learner[j]) matches[voters++] = r->match_index[j];
                    }

                    qsort(matches, voters, sizeof(uint64_t), cmp_u64);

                    if (voters > 0) {
                        size_t quorum = (voters / 2) + 1;
                        uint64_t candidate = matches[voters - quorum];

                        if (candidate > r->commit_index && log_term(r, candidate) == r->current_term) {
                            advance_commit_index(r, candidate);
                        }
                    }

                    if (r->next_index[i] <= my_last_idx && r->state == RAFT_STATE_LEADER) {
                        send_append(r, i);
                    }
                }
                break;
            }
        }
    }
}

raft_ready_t raft_core_get_ready(raft_core_t* r) {
    raft_ready_t ready;
    memset(&ready, 0, sizeof(ready));

    ready.messages = r->msg_queue;
    ready.num_messages = r->msg_queue_len;

    // 1. Clamp the save boundary to prevent fetching purged prefixes
    uint64_t save_from = r->last_saved_index;
    if (save_from < r->snapshot_index) {
        save_from = r->snapshot_index;
    }

    uint64_t last_idx = raft_core_last_index_internal(r);
    if (last_idx > save_from) {
        size_t count = last_idx - save_from;
        ready.entries_to_save = malloc(count * sizeof(raft_entry_t));
        if (ready.entries_to_save) {
            for (size_t i = 0; i < count; i++) {
                raft_entry_t* src = log_get(r, save_from + 1 + i);
                if (src) ready.entries_to_save[i] = *src;
            }
            ready.num_entries_to_save = count;
        }
    }

    // 2. Clamp the apply boundary to prevent fetching purged prefixes
    uint64_t apply_from = r->last_applied;
    if (apply_from < r->snapshot_index) {
        apply_from = r->snapshot_index;
    }

    if (r->commit_index > apply_from) {
        size_t count = r->commit_index - apply_from;
        ready.committed_entries = malloc(count * sizeof(raft_entry_t));
        if (ready.committed_entries) {
            for (size_t i = 0; i < count; i++) {
                raft_entry_t* src = log_get(r, apply_from + 1 + i);
                if (src) ready.committed_entries[i] = *src;
            }
            ready.num_committed_entries = count;
        }
    }

    ready.read_states = r->read_states;
    ready.num_read_states = r->num_read_states;

    ready.install_snapshot = r->pending_snapshot;
    ready.snapshot_index = r->snapshot_index;
    ready.snapshot_term = r->snapshot_term;
    ready.snapshot_data = r->pending_snapshot_data;
    ready.snapshot_len = r->pending_snapshot_len;

    return ready;
}

void raft_core_advance(raft_core_t* r, uint64_t saved_index, uint64_t applied_index) {
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
        raft_entry_t* e = log_get(r, i);
        if (e && (e->type == ENTRY_CONF_ADD || e->type == ENTRY_CONF_ADD_LEARNER || e->type == ENTRY_CONF_REMOVE)) {
            apply_config_change(r, i);
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

void raft_core_advance_all(raft_core_t* r) {
    raft_ready_t ready = raft_core_get_ready(r);
    if (ready.num_entries_to_save > 0) free(ready.entries_to_save);
    if (ready.num_committed_entries > 0) free(ready.committed_entries);
    raft_core_advance(r, raft_core_last_index_internal(r), raft_core_commit_index(r));
}

void raft_core_compact(raft_core_t* r, uint64_t compact_index) {
    if (compact_index <= r->snapshot_index || compact_index > r->last_applied) return;

    r->snapshot_term = log_term(r, compact_index);
    size_t keep_len = r->log_len - (compact_index - r->snapshot_index);

    for (uint64_t i = r->snapshot_index + 1; i <= compact_index; i++) {
        raft_entry_t* e = log_get(r, i);
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

raft_state_t raft_core_state(raft_core_t* r) { return r->state; }
uint64_t raft_core_term(raft_core_t* r) { return r->current_term; }
uint64_t raft_core_voted_for(raft_core_t* r) { return r->voted_for; }
uint64_t raft_core_commit_index(raft_core_t* r) { return r->commit_index; }
uint64_t raft_core_last_index(raft_core_t* r) { return raft_core_last_index_internal(r); }
uint64_t raft_core_last_applied(raft_core_t* r) { return r->last_applied; }
bool raft_core_activity_accepted(raft_core_t* r) { return r->activity_accepted; }

uint64_t raft_core_leader_id(raft_core_t* r) { return r->leader_id; }
uint64_t raft_core_snapshot_index(raft_core_t* r) { return r->snapshot_index; }
uint64_t raft_core_snapshot_term(raft_core_t* r) { return r->snapshot_term; }

size_t raft_core_peers(raft_core_t* r, uint64_t* out_peers) {
    if (out_peers) {
        for (size_t i = 0; i < r->num_peers; i++) out_peers[i] = r->peers[i];
    }
    return r->num_peers;
}

size_t raft_core_peers_ext(raft_core_t* r, uint64_t* out_peers, bool* out_is_learners) {
    for (size_t i = 0; i < r->num_peers; i++) {
        if (out_peers) out_peers[i] = r->peers[i];
        if (out_is_learners) out_is_learners[i] = r->is_learner[i];
    }
    return r->num_peers;
}

void raft_core_add_learner(raft_core_t* r, uint64_t peer_id) {
    for (size_t i = 0; i < r->num_peers; i++) {
        if (r->peers[i] == peer_id) {
            r->is_learner[i] = true;
            return;
        }
    }
    if (r->num_peers < MAX_PEERS) {
        r->peers[r->num_peers] = peer_id;
        r->is_learner[r->num_peers] = true;
        r->next_index[r->num_peers] = raft_core_last_index_internal(r) + 1;
        r->match_index[r->num_peers] = 0;
        r->recent_active[r->num_peers] = false;
        r->peer_read_seq[r->num_peers] = 0;
        r->num_peers++;
    }
}

void raft_core_promote_learner(raft_core_t* r, uint64_t peer_id) {
    for (size_t i = 0; i < r->num_peers; i++) {
        if (r->peers[i] == peer_id) {
            r->is_learner[i] = false;
            return;
        }
    }
}

// PHASE 10: Holistic backpressure byte calculation
uint64_t raft_core_uncommitted_bytes(raft_core_t* r) {
    uint64_t bytes = 0;
    uint64_t last = raft_core_last_index_internal(r);
    for (uint64_t i = r->commit_index + 1; i <= last; i++) {
        raft_entry_t* e = log_get(r, i);
        if (e) bytes += e->data_len;
    }
    return bytes;
}
