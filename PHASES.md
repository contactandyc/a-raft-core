### **Phase 1 Summary: Immediate Safety & Core Correctness**

#### 1. Mathematical Quorum Correction

* **The Bug:** The core previously calculated the commit index using a simple median (`matches[(num_peers + 1) / 2]`). In even-sized clusters (e.g., 4 nodes), this mathematically selected the second-highest index, effectively committing data that only existed on a minority of nodes (2 out of 4).
* **The Fix:** We implemented explicit quorum math (`total - quorum`), guaranteeing that the leader strictly validates a majority overlap before ever advancing the commit index.

#### 2. TCP Buffer Scaling & Codec Deadlock Prevention

* **The Bug:** The network codec permitted up to 10 MB payload frames, but the peer connection struct hardcoded a static 64 KB receive buffer. A large, valid Raft frame would overflow the buffer, force a disconnect, and permanently stall lagging followers.
* **The Fix:** We converted the `peer_connection_t` buffer into a dynamically scaling heap allocation. It now correctly doubles its capacity on demand, safely capped by `RAFT_MAX_FRAME_SIZE` to prevent malicious memory-exhaustion attacks.

#### 3. State Machine Decoupling

* **The Bug:** The core assumed that any committed configuration change or log entry should be instantly applied to the internal state. During a boot sequence, `raft_io_boot` would advance `last_applied` artificially before the host application had actually ingested the data.
* **The Fix:** We removed `raft_core_apply` from internal loops and the bootloader. Application progress is now strictly decoupled and driven by the host via `raft_core_advance()`. Raft will no longer lie about what the host database has successfully processed.

#### 4. Fast Conflict Backtracking *(Partially addressed in Phase 1, fully resolved in Phase 3)*

* **The Bug:** When a follower rejected an `AppendEntries` message due to a log mismatch, the leader would naively step its `next_index` backward by exactly 1 and retry. A follower behind by 5,000 entries would require 5,000 failed network roundtrips to sync.
* **The Fix:** We implemented fast backtracking. The leader now reads the `msg->index` from the follower's rejection and jumps its `next_index` directly to that point.

#### 5. Memory Safety & Libuv Crash Resolution

* **The Bugs:** The test suite exposed two critical memory panics. First, the bootloader allocated memory using `aml_malloc` but attempted to free it with standard `free()`. Second, the cluster simulation test forcefully destroyed libuv timers (`uv_close`) mid-tick, causing use-after-free panics.
* **The Fixes:** We swapped the bootloader to use `aml_free` strictly, and we updated the test harness to safely pause timers (`uv_timer_stop`) rather than destroying asynchronous handles out from under the event loop.

#### 6. Hermetic Build Isolation

* **The Bug:** The scaffolding engine successfully pulled the isolated dependencies, but the host machine's system-level `pkg-config` leaked into the build, forcing CMake to link against stale libraries outside the workspace.
* **The Fix:** We injected strict overrides for `PKG_CONFIG_PATH` and `CMAKE_PREFIX_PATH` into all Jinja2 build templates, forcing CMake to prioritize the local `repos/install` sandbox exclusively.

---

### **Phase 2 Summary: Disk I/O & Boot Integrity**

#### 7. Purged WAL Boot Failure (Gap 5)

* **The Bug:** The recovery bootloader (`raft_io_boot`) hardcoded its disk read loop to always start at `index 1`. If the node had successfully compacted its memory and purged older WAL segments (e.g., indexes 1 through 1000 were deleted), the bootloader would immediately fail to find index 1, abort, and permanently brick the node on restart.
* **The Fix:** We implemented dynamic index discovery. The bootloader now scans the O(1) offset map to find the first surviving, un-purged index on disk. It safely anchors the restoration process at this dynamic index, allowing nodes to reboot seamlessly regardless of how much historical data has been garbage-collected.

#### 8. Full-Frame WAL Checksums (Gap 6)

* **The Bug:** The WAL frame checksum (`crc32`) was previously computed exclusively over the payload data. The 21-byte frame metadata (term, index, type, payload length) was written to disk completely unprotected. A flipped bit in a frame's term or index would silently corrupt the timeline.
* **The Fix:** We separated the CRC hashing logic into a chainable `crc32_update` function. The `raft_wal_append` function now calculates a continuous cryptographic checksum over both the metadata header and the payload, ensuring the entire block is tamper-proof.

#### 9. Read-Time CRC Validation (Gap 7)

* **The Bug:** While the bootloader verified checksums on startup, runtime disk reads via `raft_wal_read_entry` blindly trusted the bytes returned by the OS without validating them. If disk rot occurred while the node was active, it could accidentally transmit corrupted historical data to a lagging follower.
* **The Fix:** We added strict read-time verification. `raft_wal_read_entry` now re-computes the full-frame CRC for every fetched entry and compares it to the header's stored CRC. Corrupted entries are safely discarded and reported as missing, forcing the Raft protocol to handle it naturally rather than propagating bad data.

#### 10. Comprehensive HardState Persistence (Gap 8)

* **The Bug:** The node safely persisted the `term` and `voted_for` values atomically to its `.dat` meta-file, but it neglected to track `last_applied`. If a node crashed after applying transactions to the host database but before a new snapshot was taken, it would reboot with `last_applied = 0` and forcefully double-execute historical commands.
* **The Fix:** We expanded the atomic HardState footprint. The `save_hardstate` function now natively persists the `applied_index` boundary. The bootloader reads this value and locks `r->last_applied` inside the core during restoration, strictly preserving exactly-once application semantics across hard power loss.

---

### **Phase 3 Summary: Network Liveness & Performance**

#### 11. O(1) Conflict Hinting (Gap 9)

* **The Bug:** Relying purely on the `msg->index` field for backtracking could still result in excess network roundtrips if entire terms of uncommitted logs conflicted across nodes.
* **The Fix:** We expanded the binary codec and `raft_msg_t` struct to support 16 bytes of explicit `conflict_term` and `conflict_index` hints. When a follower rejects an append, it actively guides the leader to the exact divergence point, establishing true O(1) network recovery regardless of the log length discrepancy.

#### 12. Pre-Vote Phase (Gap 10)

* **The Bug:** An isolated follower would constantly trigger its election timeout, incrementing its term to an artificially massive number. When the network partition healed, this massive term would force the stable leader to step down, causing a disruptive, cluster-wide outage.
* **The Fix:** We introduced the `RAFT_STATE_PRE_CANDIDATE` state alongside `MSG_PRE_VOTE` and `MSG_PRE_VOTE_RES` messages. A node must now poll the cluster and prove it can win an election *before* it is allowed to officially increment its term and disrupt the active leader.

#### 13. Reconnect Manager & Outbound Queuing (Gap 11)

* **The Bug:** `router_send_rpc` dropped messages entirely if the TCP socket was momentarily disconnected. While Raft can eventually recover from dropped messages via heartbeats, relying on this incidental timing makes cluster recovery slow and non-deterministic.
* **The Fix:** We introduced the `known_peer_t` struct to act as a persistent network anchor. If a socket drops, the Reconnect Manager automatically spins up a background libuv timer to aggressively heal the pipe. Concurrently, an outbound heap queue natively buffers up to 5MB of network traffic, instantly draining the payloads to the peer the moment the connection is organically restored.

---

### **Phase 4 Summary: Advanced Protocol Features**

#### 12. Partial Membership Changes (Learners & Stepdowns)

* **The Bug:** The cluster lacked joint consensus and non-voting ingestion capabilities. If a new, empty node was added, the global quorum size instantly increased, causing a complete drop in write availability until the new node finished downloading the entire historical log. Furthermore, if a leader was removed from the cluster, it didn't know how to step down.
* **The Fix:** We implemented `ENTRY_CONF_ADD_LEARNER` and dynamic topology parsing. New nodes now join as `LEARNER`s, securely syncing the state machine without participating in elections or stalling the commit quorum. Once caught up, they can be seamlessly promoted. We also added self-removal logic so a leader will gracefully step down to a follower if it is voted out of the cluster.

#### 13. Strict Linearizable Reads (ReadIndex)

* **The Bug:** To ensure a read was not served by a stale, isolated leader, clients were previously forced to propose a dummy "write" to the physical disk. This choked read throughput by tying it directly to disk IOPS.
* **The Fix:** We implemented the `ReadIndex` protocol. The leader now buffers incoming read requests, snapshots its current `commit_index`, and relies on its lightweight memory heartbeats to confirm it still holds a quorum. Once the quorum ACKs the heartbeat, the leader echoes the client's `read_seq` context back to the application layer, guaranteeing a linearizable, disk-free read.

#### 14. Check-Quorum (Stale Leader Guard)

* **The Bug:** If a network partition separated the leader from the majority of the cluster, the isolated leader would never see a higher term and would remain in `RAFT_STATE_LEADER` forever. While it couldn't commit *new* writes, it could potentially serve stale, outdated reads to any clients trapped in its partition.
* **The Fix:** We implemented an active Check-Quorum heartbeat guard. The leader now tracks a `recent_active` array for all network traffic. Instead of ignoring its own election timer, the leader now uses it to periodically execute a `MSG_CHECK_QUORUM` routine. If it realizes it hasn't heard from a majority of the cluster within the election window, it autonomously steps down, actively protecting clients from stale reads.

---

### **Phase 5 Summary: API, Limits, and Polish**

#### 15. Backpressure Mechanisms (Gap 15)

* **The Bug:** The core previously accepted unbounded payloads via `raft_node_propose`. Without limits, a fast client could push data into the leader's memory exponentially faster than the physical disks could sync it, inevitably leading to an Out-Of-Memory (OOM) crash under heavy load.
* **The Fix:** We implemented a strict in-flight queue depth check. If the difference between the leader's `last_index` and `commit_index` exceeds a safe threshold (e.g., 2,000 entries), the engine actively rejects new proposals and returns `RAFT_ERR_QUEUE_FULL`, natively enforcing backpressure onto the client application.

#### 16. Client Retry Deduplication (Gap 16)

* **The Bug:** If a leader successfully committed a write to disk but crashed immediately before replying to the client, the client would naturally retry the exact same write against the newly elected leader. Without idempotency tracking, this would cause the Raft state machine to double-execute the transaction.
* **The Fix:** We expanded the binary codec to include `client_id` and `client_seq` metadata. The Raft core now maintains an $O(1)$ session tracker array. If it receives a proposal with a `client_seq` less than or equal to the highest sequence already processed for that `client_id`, it silently drops the duplicate, guaranteeing Exactly-Once execution semantics.

#### 17. Follower Redirects (Gap 17)

* **The Bug:** If a client blindly sent a proposal to a Follower, the node would silently drop it on the floor, forcing the client to eventually timeout and guess another node.
* **The Fix:** We implemented a passive leader tracking mechanism (`raft_core_leader_id()`). Now, `raft_node_propose` intercepts misrouted requests immediately, returns `RAFT_ERR_NOT_LEADER`, and populates an `out_leader_id` pointer. This allows the host application to instantly issue an HTTP 307 Redirect (or equivalent gRPC reroute) to gracefully guide the client to the active leader.

---

### **Phase 6 Summary: Triage & Memory Safety**

#### 18. CPU Livelock on Conflict Resolution

* **The Bug:** When a follower rejected an `AppendEntries` payload, the leader scanned backward through its log using an unsigned 64-bit integer (`uint64_t idx`). If the node had not yet created a snapshot (`snapshot_index == 0`), the loop condition `idx >= 0` evaluated to unconditionally true. Upon hitting zero, the decrement wrapped the index to `UINT64_MAX`, creating an infinite loop that permanently locked the CPU thread.
* **The Fix:** We implemented safe underflow boundaries. The backward-scanning loop now explicitly breaks if `idx == 0` or `idx == r->snapshot_index`, cleanly yielding control and preventing the livelock.

#### 19. Heap Buffer Overflow in Router

* **The Bug:** The `router_send_rpc` function attempted to handle large frames by checking if the outbound queue needed more space and applying a single multiplier (`kp->out_queue_cap *= 2`). If a massive batch or snapshot arrived, doubling the buffer once was insufficient, causing the subsequent `memcpy` to write out of bounds and corrupt the heap.
* **The Fix:** We replaced the single multiplier with a bounded `while` loop that dynamically scales the queue capacity until it securely fits the incoming frame size, capped safely by `RAFT_MAX_FRAME_SIZE`.

#### 20. Unbounded Memory Leaks

* **The Bug:** The core suffered from three major leaks: (1) `raft_core_advance` freed the outer message arrays but abandoned dynamically allocated `entries[j].data` payloads. (2) Unrouted network frames allocated in the router were leaked if no matching peer was found. (3) The `raft_node_pump` failed to free the dynamically allocated `ready.entries_to_save` arrays after processing.
* **The Fix:** We established strict lifecycle ownership. Unrouted frames are now tracked and freed, `raft_core_advance` executes a deep-free on all nested payloads, and the pump cycle guarantees a cleanup block is executed before yielding.

---

### **Phase 7 Summary: Apply & Persistence Integrity**

#### 21. Volatile Deduplication Amnesia

* **The Bug:** Client sequence deduplication was tracked using an $O(1)$ array stored in volatile memory inside the leader's proposal handler. If the leader crashed, the new leader booted with an empty array. Retried client commands were treated as brand new, breaking exactly-once execution semantics and duplicating transactions on the state machine.
* **The Fix:** We moved deduplication out of the consensus core. `client_seq` metadata is now passed directly through the replicated log. The host state machine maintains a durable, crash-safe session table and inherently deduplicates commands during the apply phase.

#### 22. Phantom Application Progress

* **The Bug:** The core blindly advanced `last_applied` to match the `commit_index` internally, regardless of whether the physical state machine had finished processing the data. If the application layer crashed while ingesting a large batch, Raft would incorrectly believe the data was applied.
* **The Fix:** We implemented a strict `raft_apply_fn` synchronous callback. The core is no longer permitted to advance `last_applied` until the host application explicitly confirms successful execution.

#### 23. Incomplete Boot Metadata

* **The Bug:** Restarting a node with a compacted WAL synthesized a `snapshot_term = 0` and lost its dynamic membership context, as the `.meta` file only saved `term` and `voted_for`. This caused the recovered node to be violently rejected by healthy peers.
* **The Fix:** We expanded the atomic `.meta` footprint to explicitly include `snapshot_index`, `snapshot_term`, and active membership arrays. Bootloaders now perfectly rehydrate their context directly from disk.

---

### **Phase 8 Summary: Snapshot & Recovery Correctness**

#### 24. Destructive Snapshot Installation

* **The Bug:** When a follower received `MSG_INSTALL_SNAPSHOT`, the handler blindly executed `r->log_len = 1;`, wiping the entire log memory. This illegally destroyed valid, uncommitted log suffixes that existed ahead of the snapshot, breaking the leader's ability to commit those entries.
* **The Fix:** We rewrote snapshot installation to enforce strict suffix preservation. The core now only drops the covered prefix, gracefully preserving log entries if they match the snapshot boundary or exist strictly after it.

#### 25. Null-Pointer Dereference Post-Snapshot

* **The Bug:** Snapshot installation correctly advanced `snapshot_index`, but failed to synchronize `last_saved_index`. In the next pump cycle, `raft_core_get_ready` attempted to fetch logs between the lagging saved index and the new boundary, pulling `NULL` pointers and immediately segfaulting.
* **The Fix:** We enforced strict pointer synchronization. `last_saved_index`, `commit_index`, and `last_applied` are now safely bounded and advanced `>= snapshot_index` atomically after physical state installation.

#### 26. Blind Compacted Reboots

* **The Bug:** The recovery bootloader attempted to synthesize its snapshot parameters from the oldest surviving WAL file. If the WAL was completely purged up to the snapshot point, this logic failed.
* **The Fix:** The bootloader no longer guesses. It explicitly reads the true `snapshot_index` and `snapshot_term` from the fortified `.meta` file, ensuring seamless reboots even with zero historical WAL files on disk.

---

### **Phase 9 Summary: Advanced Protocol Semantics**

#### 27. Unsafe Overlapping Configurations

* **The Bug:** While `LEARNER` staging was implemented, there was no protection against the host application proposing multiple topology changes simultaneously. Overlapping configuration changes could mathematically split the cluster brain.
* **The Fix:** We implemented a pending-config guard. The core now actively rejects new topology proposals if an uncommitted `ENTRY_CONF_*` entry currently exists anywhere between `commit_index + 1` and `last_index`.

#### 28. Premature Configuration Mutation

* **The Bug:** Topology arrays (peers/learners) were mutated instantly when an `ENTRY_CONF_*` log entry was appended. This meant uncommitted configuration data was dictating quorum math, which violates Raft safety properties.
* **The Fix:** Topology array mutation was centralized. Configuration entries are proposed normally, but internal peer arrays are exclusively updated during the apply phase, strictly after the entry is globally committed.

#### 29. Loose ReadIndex Validation

* **The Bug:** Followers echoed `read_seq` context on append *rejections*, and the leader accepted read ACKs without validating terms. This allowed an old, deposed leader to falsely fulfill a linearizable read.
* **The Fix:** We hardened the `ReadIndex` pipeline. Followers only echo `read_seq` upon a successful append. The leader strictly validates `MSG_READ_INDEX_RES` against its current term and actively pending request barriers.

---

### **Phase 10 Summary: Scale, Hardening, and Conformance**

#### 30. Unbounded Payload Batching

* **The Bug:** The core limited `AppendEntries` solely by an entry count threshold (500). If a client proposed 500 massive payloads, it resulted in single RPC frames exceeding several megabytes, choking network throughput.
* **The Fix:** We implemented byte-bounded batching. `AppendEntries` is now capped by both entry count and maximum encoded frame size, ensuring consistent, high-velocity network pipelining.

#### 31. Deep Conflict Infinite Loops

* **The Bug:** If a follower rejected an append at an index older than its `snapshot_index`, it returned a generic rejection without a useful `conflict_index`. The leader defaulted to `1`, retried, and was rejected again, causing an infinite network ping-pong.
* **The Fix:** Followers now accurately report `r->snapshot_index + 1` for deep conflicts, instantly signaling the leader to switch from `AppendEntries` to `InstallSnapshot` routing.

#### 32. Storage Sync Ordering & Conformance

* **The Bug:** The disk engine was committing data but lacked structured POSIX sync ordering, making it susceptible to corrupted boundaries during a hard power loss.
* **The Fix:** We enforced a strict `fsync` hierarchy (WAL -> Meta -> Snapshot). We then validated the entire architecture against a massive conformance suite, proving resilience against simulated partition delays, leader crashes during replication, torn WAL headers, and dynamic membership reboots.
