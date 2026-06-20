// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include "raft_internal.h"
#include <stdlib.h>
#include <string.h>

static int cmp_u64(const void* a, const void* b) {
    uint64_t ua = *(const uint64_t*)a;
    uint64_t ub = *(const uint64_t*)b;
    if (ua < ub) return -1;
    if (ua > ub) return 1;
    return 0;
}

void raft_replication_advance_commit(raft_t* r, uint64_t new_commit) {
    if (new_commit <= r->commit_index) return;
    r->commit_index = new_commit;
}

static void send_append(raft_t* r, size_t peer_idx) {
    uint64_t peer_id = r->peers[peer_idx];
    uint64_t next = r->next_index[peer_idx];
    uint64_t last = raft_log_last_index(r);

    if (next <= r->snapshot_index) {
        raft_msg_t msg = { .type = MSG_INSTALL_SNAPSHOT, .to = peer_id, .term = r->current_term,
                           .index = r->snapshot_index, .log_term = r->snapshot_term };
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
        if (batch_bytes + src->data_len > 1048576) {
            if (actual_entries == 0) actual_entries = 1;
            break;
        }
        batch_bytes += src->data_len;
        actual_entries++;
    }
    num_entries = actual_entries;

    msg.num_entries = num_entries;
    msg.entries = num_entries > 0 ? malloc(num_entries * sizeof(raft_entry_t)) : NULL;

    for (size_t i = 0; i < num_entries; i++) {
        raft_entry_t* src = raft_log_get(r, next + i);
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
    raft_send_msg(r, msg);
}

void raft_replication_bcast_append(raft_t* r) {
    for (size_t i = 0; i < r->num_peers; i++) send_append(r, i);
}

void raft_replication_step(raft_t* r, raft_msg_t* msg) {
    if (msg->type == MSG_TICK) {
        if (r->state == RAFT_STATE_LEADER) raft_replication_bcast_append(r);
        return;
    }
    else if (msg->type == MSG_PROPOSE && r->state == RAFT_STATE_LEADER) {
        if (msg->num_entries == 0 || msg->entries == NULL) return;

        uint64_t old_last_idx = raft_log_last_index(r);
        bool has_pending_config = false;

        for (uint64_t idx = r->commit_index + 1; idx <= old_last_idx; idx++) {
            raft_entry_t* e = raft_log_get(r, idx);
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
            raft_log_append(r, r->current_term, msg->entries[i].type, msg->entries[i].client_id, msg->entries[i].client_seq, msg->entries[i].data, msg->entries[i].data_len);
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
    else if (msg->type == MSG_APPEND_ENTRIES) {
        if (msg->num_entries > 0 && msg->entries == NULL) {
            raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = raft_log_last_index(r) };
            raft_send_msg(r, res);
            return;
        }

        raft_msg_t res = { .type = MSG_APPEND_RES, .to = msg->from, .term = r->current_term, .reject = true, .index = raft_log_last_index(r), .read_seq = 0 };

        if (msg->term >= r->current_term) {
            r->state = RAFT_STATE_FOLLOWER;
            r->activity_accepted = true;

            uint64_t my_last_idx = raft_log_last_index(r);

            if (msg->index >= r->snapshot_index && msg->index <= my_last_idx && raft_log_term(r, msg->index) == msg->log_term) {
                res.reject = false;

                if (msg->num_entries > 0) {
                    for (size_t i = 0; i < msg->num_entries; i++) {
                        uint64_t new_idx = msg->index + 1 + i;
                        my_last_idx = raft_log_last_index(r);

                        if (new_idx <= my_last_idx && raft_log_term(r, new_idx) == msg->entries[i].term) continue;
                        if (new_idx <= r->commit_index) {
                            res.reject = true;
                            break;
                        }
                        if (new_idx <= my_last_idx) raft_log_truncate(r, new_idx);

                        raft_log_append(r, msg->entries[i].term, msg->entries[i].type, msg->entries[i].client_id, msg->entries[i].client_seq, msg->entries[i].data, msg->entries[i].data_len);
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
        }
        raft_send_msg(r, res);
    }
    else if (msg->type == MSG_APPEND_RES && r->state == RAFT_STATE_LEADER) {
        if (msg->term != r->current_term) return;

        uint64_t my_last_idx = raft_log_last_index(r);

        for (size_t i = 0; i < r->num_peers; i++) {
            if (r->peers[i] == msg->from) {

                if (!msg->reject) {
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
                                        raft_send_msg(r, rd_res);
                                    }
                                    r->pending_reads[pr].active = false;
                                }
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
                            if (raft_log_term(r, idx) == msg->conflict_term) {
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

                        if (candidate > r->commit_index && raft_log_term(r, candidate) == r->current_term) {
                            raft_replication_advance_commit(r, candidate);
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
