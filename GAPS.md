Based on the comprehensive Raft reference document provided, your C implementation is an excellent, structurally sound foundation. It perfectly captures the decoupling of the append/commit/apply paths (via the `raft_ready_t` / event pump pattern), correctly implements the notoriously tricky "Figure 8 anomaly" (waiting for a current-term entry to reach a majority), and ensures durable, atomic persistence of HardState.

However, cross-referencing your codebase against the production realities and edge conditions described in the document reveals several **critical gaps, potential deadlocks, and missing performance optimizations**.

Here is a detailed breakdown of the gaps in your library, mapped directly to the conditions in your document:

---

### 1. Critical Liveness & Memory Deadlocks

**Gap 1: The "Max Frame" Catch-Up Deadlock (Doc Section 5.14 / Condition 9)**

* **The Document:** "Chunk large entries... large entries can cause head-of-line blocking."
* **The Code:** In `raft_core.c` (`bcast_append`), the leader attempts to send **all** missing entries to a follower at once: `.num_entries = r->log_len - (prev_idx + 1)`.
* **The Impact:** Your `raft_codec.h` enforces a strict `RAFT_MAX_MSG_ENTRIES` of 10,000. If a follower falls behind by 10,001 entries, `raft_core` will request a message with 10,001 entries. `raft_codec_serialize_msg` will return `-1`, the `raft_node_pump` will drop it, and the leader will retry endlessly on the next `MSG_TICK`. The follower will never catch up.
* **The Fix:** You must paginate `AppendEntries`. Clamp `.num_entries` to a safe batch limit in the core.

To fix **Gap 1: The "Max Frame" Catch-Up Deadlock**, we need to paginate the `AppendEntries` messages. Right now, if a follower falls too far behind, the leader tries to send the entire missing log in a single massive frame, which gets rejected by the network codec and stalls the follower forever.

We can fix this by introducing a strict pagination limit and a central `send_append` helper function. Additionally, by sending the *next* page immediately upon receiving a success response (`MSG_APPEND_RES`), the follower will catch up at max network speed instead of waiting for the heartbeat timer.

Here are the specific updates to make to `src/raft_core.c`:

### 1. Define the Pagination Limit

At the top of `src/raft_core.c`, add the max batch limit just below the max peers definition:

```c
#define MAX_PEERS 16
#define RAFT_MAX_APPEND_BATCH 2048 // Prevents codec buffer deadlocks (Gap 1)

```

### 2. Add the `send_append` Helper

Add this new centralized sending function right before `bcast_append()`. This replaces the duplicated message construction logic and automatically paginates oversized logs:

```c
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

```

### 3. Simplify `bcast_append`

Refactor `bcast_append` to use your new helper. Find the existing `bcast_append` and replace it entirely with:

```c
static void bcast_append(raft_core_t* r) {
    for (size_t i = 0; i < r->num_peers; i++) {
        send_append(r, i);
    }
}

```

### 4. Update the `MSG_PROPOSE` Route

Inside `raft_core_step()`, locate the `MSG_PROPOSE` handler block. Replace the inner peer loop with the new pagination-safe helper:

```c
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

```

### 5. Update the `MSG_APPEND_RES` Route (Rejection & Pipelining)

Inside `raft_core_step()`, locate the `MSG_APPEND_RES` handler where the leader processes ACKs. Replace the `if (msg->reject) { ... } else { ... }` block with this safely paginated version that includes aggressive pipelining for lagging followers:

```c
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

```

### Why this works:

1. **No Network Drop Deadlocks:** `RAFT_MAX_APPEND_BATCH` guarantees we never exceed the codec's hard limit, meaning `raft_node_pump` will successfully route the payloads every time.
2. **Reduced Code Duplication:** Constructing the complicated `MSG_APPEND_ENTRIES` object is now centralized.
3. **Pipelined Catch-Up:** By adding `if (r->next_index[i] < r->log_len) send_append(r, i);` to the success block, a lagging follower will seamlessly chew through gigabytes of paginated backlog back-to-back at full network speed, rather than waiting 50ms for the next `MSG_TICK` to get the next chunk.

**Gap 2: In-Memory Log Exhaustion & Missing Snapshots (Doc Conditions 40 & 41)**

* **The Document:** "Periodically create snapshots... discard log entries." / "Leader sends an `InstallSnapshot` message."
* **The Code:** Your disk layer (`raft_wal.c`) has `raft_wal_purge_head()` to gracefully clean up NVMe. However, **the brain (`raft_core.c`) never compacts its RAM.** The `r->log` array only ever grows via `realloc`. Furthermore, `msg_type_t` lacks an `InstallSnapshot` equivalent.
* **The Impact:** A long-running server will eventually suffer an Out-Of-Memory (OOM) crash. Additionally, if a follower goes offline and falls so far behind that the leader has already purged the missing entries from disk, the follower is permanently bricked because the leader cannot send a snapshot.

**Gap 3: The Purged Log Boot Failure (Doc Condition 24 & 36)**

* **The Document:** "On restart, it replays committed log entries."
* **The Code:** In `raft_io_boot` inside `raft_io.c`, you hardcode the recovery loop to start at index 1: `for (uint64_t i = 1; i <= wal->max_disk_index; i++)`.
* **The Impact:** If the WAL has successfully garbage collected (purged) indexes 1 through 5,000, `raft_wal_read_entry` for index 1 will return false, the bootloader will hit `return NULL`, and your node will completely fail to boot on restart. `raft_io_boot` must discover the actual starting index on disk.

---

### 2. High-Throughput & Performance Gaps

**Gap 4: O(N) Network Backtracking (Doc Condition 19 / Section 5.5)**

* **The Document:** "Use conflict hints... The leader can then jump back by term rather than decrementing one index at a time."
* **The Code:** When a follower rejects an `AppendEntries`, the leader backs up exactly one index: `r->next_index[i] = (r->next_index[i] > 1) ? r->next_index[i] - 1 : 1;`
* **The Impact:** If a follower reboots with a conflicting log 50,000 entries deep, the leader and follower will exchange 50,000 separate failed RPCs over the network just to find the matching index.
* **The Fix:** Add `conflict_term` and `conflict_index` to `raft_msg_t`. The follower should return these upon rejection so the leader can skip backward instantly.

**Gap 5: Lack of Backpressure (Doc Section 5.15)**

* **The Document:** "The leader needs backpressure when clients submit commands faster than the cluster can replicate."
* **The Code:** `raft_node_propose()` accepts requests unconditionally and queues them in memory.
* **The Impact:** If a client writes at 100,000 requests per second, but the disk/network can only handle 10,000 rps, the in-flight pipeline will bloat infinitely, leading to massive recovery delays or OOM crashes.

---

### 3. Safety & Leadership Edge Cases

**Gap 6: Missing Pre-Vote Phase (Doc Section 5.8 / Condition 33)**

* **The Document:** "Use pre-vote to avoid disruptive elections by isolated nodes."
* **The Code:** When `MSG_HUP` fires in `raft_core_step()`, the node instantly executes `r->current_term++` and becomes a candidate.
* **The Impact:** If a follower is temporarily partitioned from the network, it will hit its election timeout repeatedly, incrementing its term to an artificially massive number. When the partition heals, it will bombard the legitimate leader with this massive term, forcing the leader to step down and causing a cluster-wide outage.

**Gap 7: Leader Isolation / Missing Check-Quorum (Doc Condition 31)**

* **The Document:** "Leader partitioned away from majority... majority side elects a new leader."
* **The Code:** Your leader only steps down if it receives an RPC with a higher term.
* **The Impact:** If the leader is placed into a minority partition, it will never see a higher term. It will remain in `RAFT_STATE_LEADER` indefinitely. If clients send it read requests, it will serve stale data. The leader must track heartbeat ACKs and voluntarily step down if it loses contact with the majority.

**Gap 8: Missing Safe Read Paths / ReadIndex (Doc Condition 30 & Section 5.9)**

* **The Document:** "Linearizable reads must reflect the latest committed write... Use ReadIndex protocol."
* **The Code:** There is no read protocol.
* **The Impact:** To do a linearizable read right now, a client would have to propose an empty payload via `raft_node_propose()` and wait for it to commit to disk. Implementing the `ReadIndex` protocol allows the leader to prove it holds a quorum via in-memory heartbeats without hitting the disk.

---

### 4. Configuration & Membership Gaps

**Gap 9: Leader Fails to Step Down on Self-Removal (Doc Condition 47)**

* **The Document:** "Leader is removed from the configuration... leader steps down once the new configuration is committed."
* **The Code:** In `apply_conf_change` for `ENTRY_CONF_REMOVE`, the code iterates over `r->peers` to remove a node. However, `r->id` (the node's own ID) is purposefully excluded from `r->peers`.
* **The Impact:** If the cluster removes the current leader, the leader's `apply_conf_change` will do nothing. The leader will never step down, continuing to send heartbeats and acting as a zombie leader.

**Gap 10: Unsafe Concurrent Configuration Changes (Doc Condition 49)**

* **The Document:** "Avoid large unsafe changes." (Single-server changes require strict serialization).
* **The Code:** A client can rapidly pipeline `ENTRY_CONF_ADD` multiple times before the first one commits.
* **The Impact:** Having multiple uncommitted configuration changes in the log simultaneously can cause overlapping quorums that allow two different leaders to be elected at once. You must reject a new configuration proposal if there is already an uncommitted config change pending.

**Gap 11: Missing Non-Voting Learners (Doc Condition 48 & Section 5.11)**

* **The Document:** "New server has no log... it should catch up before being allowed to affect quorum decisions."
* **The Code:** Nodes added via `ENTRY_CONF_ADD` immediately become voting members in `apply_conf_change`.
* **The Impact:** If you add a completely empty node to a 3-node cluster, the quorum size instantly becomes 3 (meaning 3 nodes must ACK). Because the new node is busy downloading historical logs, the cluster will stall and lose write availability until the new node finishes catching up. Adding a `LEARNER` state solves this.
