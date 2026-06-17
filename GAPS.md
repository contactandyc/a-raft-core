### Phase 1 Summary: Immediate Safety & Core Correctness

#### 1. Mathematical Quorum Correction

* **The Bug:** The core previously calculated the commit index using a simple median (`matches[(num_peers + 1) / 2]`). In even-sized clusters (e.g., 4 nodes), this mathematically selected the second-highest index, effectively committing data that only existed on a minority of nodes (2 out of 4).
* **The Fix:** We implemented explicit quorum math (`total - quorum`), guaranteeing that the leader strictly validates a majority overlap before ever advancing the commit index.

#### 2. TCP Buffer Scaling & Codec Deadlock Prevention

* **The Bug:** The network codec permitted up to 10 MB payload frames, but the peer connection struct hardcoded a static 64 KB receive buffer. A large, valid Raft frame would overflow the buffer, force a disconnect, and permanently stall lagging followers.
* **The Fix:** We converted the `peer_connection_t` buffer into a dynamically scaling heap allocation. It now correctly doubles its capacity on demand, safely capped by `RAFT_MAX_FRAME_SIZE` to prevent malicious memory-exhaustion attacks.

#### 3. State Machine Decoupling

* **The Bug:** The core assumed that any committed configuration change or log entry should be instantly applied to the internal state. During a boot sequence, `raft_io_boot` would advance `last_applied` artificially before the host application had actually ingested the data.
* **The Fix:** We removed `raft_core_apply` from internal loops and the bootloader. Application progress is now strictly decoupled and driven by the host via `raft_core_advance()`. Raft will no longer lie about what the host database has successfully processed.

#### 4. Fast Conflict Backtracking

* **The Bug:** When a follower rejected an `AppendEntries` message due to a log mismatch, the leader would naively step its `next_index` backward by exactly 1 and retry. A follower behind by 5,000 entries would require 5,000 failed network roundtrips to sync.
* **The Fix:** We implemented fast backtracking. The leader now reads the `msg->index` from the follower's rejection and jumps its `next_index` directly to that point, resolving conflicts in $O(1)$ network trips.

#### 5. Memory Safety & Libuv Crash Resolution

* **The Bugs:** The test suite exposed two critical memory panics. First, the bootloader allocated memory using `aml_malloc` but attempted to free it with standard `free()`. Second, the cluster simulation test forcefully destroyed libuv timers (`uv_close`) mid-tick, causing use-after-free panics.
* **The Fixes:** We swapped the bootloader to use `aml_free` strictly, and we updated the test harness to safely pause timers (`uv_timer_stop`) rather than destroying asynchronous handles out from under the event loop.

#### 6. Hermetic Build Isolation

* **The Bug:** The scaffolding engine successfully pulled the isolated dependencies, but the host machine's system-level `pkg-config` leaked into the build, forcing CMake to link against stale libraries outside the workspace.
* **The Fix:** We injected strict overrides for `PKG_CONFIG_PATH` and `CMAKE_PREFIX_PATH` into all Jinja2 build templates, forcing CMake to prioritize the local `repos/install` sandbox exclusively.

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
