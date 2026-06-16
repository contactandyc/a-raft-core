Here is the restructured and consolidated roadmap.

I have removed the two major gaps we just resolved (**Network Pagination/Deadlocks** and **Core Snapshot/Memory Compaction**) and reorganized the remaining items into logical, actionable phases. I've also incorporated the nuances regarding Check-Quorum and decoupled the internal Raft state from the application state machine.

---

### **Phase 1: Immediate Safety & Core Correctness**

*These are mathematical or state-machine footguns that could cause data loss or cluster corruption under normal operation.*

* **1. Even-Sized Cluster Majority Bug:** The leader computes the commit candidate as a median of match indexes. In a 4-node cluster (quorum = 3), the median selects the 2nd index, committing entries replicated to only a minority. *Fix: Implement explicit quorum math.*
* **2. State Machine "Apply" Decoupling:** `raft_node_pump` artificially marks entries as applied without waiting for a real application callback. Raft must not advance `last_applied` until the host database actually acknowledges the write.
* **3. Config vs. Application Replay:** On boot, `raft_core_restore` blindly calls `raft_core_apply`, which advances `last_applied`. This is safe for internal config changes but dangerous for normal commands, which the host database might have already applied (or might need to apply at its own pace).
* **4. TCP Buffer / Codec Mismatch:** The codec allows up to 10 MB frames, but `peer_connection_t` uses a rigid 64 KB buffer. If a valid Raft frame exceeds 64 KB, the connection will drop and silently fail to recover.

### **Phase 2: Disk I/O & Boot Integrity**

*These gaps involve how the system writes to and recovers from the physical disk, ensuring it survives hard power loss.*

* **5. Purged WAL Boot Failure:** Because we implemented `raft_core_compact` (Gap 2), the WAL will eventually purge old segments. However, `raft_io_boot` currently hardcodes recovery to start at `index 1`. If `index 1` is purged, the node will fail to boot entirely.
* **6. Incomplete WAL Frame Checksums:** The WAL frame header contains CRC, length, term, index, and type, but the CRC is computed *only* over the payload. A flipped bit in the `term` or `index` metadata will corrupt the timeline silently.
* **7. Missing CRC Re-Check on Read:** `raft_wal_read_entry` blindly trusts data off the disk without re-validating the CRC.
* **8. Incomplete HardState Persistence:** The node saves `term` and `voted_for` atomically, but the `commit_index` and `last_applied` bounds are not strictly persisted alongside the snapshots, risking amnesia on crash.

### **Phase 3: Network Liveness & Performance**

*These optimize how nodes communicate and prevent edge-case cluster outages.*

* **9. O(N) Conflict Backtracking:** When a follower rejects an append, the leader decrements `next_index` by exactly 1. If a follower is out of sync by 5,000 entries, it requires 5,000 failed network round-trips to find the matching index. *Fix: Implement term/index conflict hints.*
* **10. Missing Pre-Vote Phase:** An isolated follower will spam election timeouts, incrementing its term to an artificially massive number. When the network heals, it will force the stable leader to step down, causing a cluster outage.
* **11. No Outbound Retry Queue:** `router_send_rpc` drops messages entirely if the TCP socket isn't immediately ready. Raft tolerates this, but a reconnect manager ensures faster, deterministic catch-up.

### **Phase 4: Advanced Protocol Features**

*These are required for zero-downtime operations and strict read safety.*

* **12. Partial Membership Changes:** The code lacks joint consensus, leader self-removal stepdown, and `LEARNER` roles. Adding an empty node currently drops write availability because quorum sizes increase before the new node has downloaded the history.
* **13. Missing Safe Read Protocol (ReadIndex):** To do a linearizable read right now, clients must propose a dummy write. The implementation needs `ReadIndex` so the leader can verify its authority via memory heartbeats without hitting the disk.
* **14. Missing Check-Quorum (Stale Leader Guard):** A leader in a minority partition will never see a higher term, so it remains `RAFT_STATE_LEADER` forever. While it cannot commit writes (write-safe), it *can* serve stale reads to clients unless it tracks heartbeats and voluntarily steps down.

### **Phase 5: API, Limits, and Polish**

*These are necessary to expose the library to end-users safely.*

* **15. No Backpressure Mechanisms:** `raft_node_propose` accepts unbounded payloads. There are no limits on in-flight append bytes or client queues, risking OOM if clients push data faster than the disks can sync.
* **16. Client Retry Deduplication:** If the leader crashes after committing a write but before responding to the client, the client will retry. Without a sequence/ID deduplicator, the write executes twice.
* **17. Unhandled Follower Proposals:** `raft_node_propose` silently drops proposals if called on a follower. It needs to return a structured redirect pointing the client to the active leader.
