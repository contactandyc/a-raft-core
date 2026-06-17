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

### **Phase 2 Summary: Disk I/O & Boot Integrity**

#### 1. Purged WAL Boot Failure (Gap 5)

* **The Bug:** The recovery bootloader (`raft_io_boot`) hardcoded its disk read loop to always start at `index 1`. If the node had successfully compacted its memory and purged older WAL segments (e.g., indexes 1 through 1000 were deleted), the bootloader would immediately fail to find index 1, abort, and permanently brick the node on restart.
* **The Fix:** We implemented dynamic index discovery. The bootloader now scans the O(1) offset map to find the first surviving, un-purged index on disk. It safely anchors the restoration process at this dynamic index, allowing nodes to reboot seamlessly regardless of how much historical data has been garbage-collected.

#### 2. Full-Frame WAL Checksums (Gap 6)

* **The Bug:** The WAL frame checksum (`crc32`) was previously computed exclusively over the payload data. The 21-byte frame metadata (term, index, type, payload length) was written to disk completely unprotected. A flipped bit in a frame's term or index would silently corrupt the timeline.
* **The Fix:** We separated the CRC hashing logic into a chainable `crc32_update` function. The `raft_wal_append` function now calculates a continuous cryptographic checksum over both the metadata header and the payload, ensuring the entire block is tamper-proof.

#### 3. Read-Time CRC Validation (Gap 7)

* **The Bug:** While the bootloader verified checksums on startup, runtime disk reads via `raft_wal_read_entry` blindly trusted the bytes returned by the OS without validating them. If disk rot occurred while the node was active, it could accidentally transmit corrupted historical data to a lagging follower.
* **The Fix:** We added strict read-time verification. `raft_wal_read_entry` now re-computes the full-frame CRC for every fetched entry and compares it to the header's stored CRC. Corrupted entries are safely discarded and reported as missing, forcing the Raft protocol to handle it naturally rather than propagating bad data.

#### 4. Comprehensive HardState Persistence (Gap 8)

* **The Bug:** The node safely persisted the `term` and `voted_for` values atomically to its `.dat` meta-file, but it neglected to track `last_applied`. If a node crashed after applying transactions to the host database but before a new snapshot was taken, it would reboot with `last_applied = 0` and forcefully double-execute historical commands.
* **The Fix:** We expanded the atomic HardState footprint. The `save_hardstate` function now natively persists the `applied_index` boundary. The bootloader reads this value and locks `r->last_applied` inside the core during restoration, strictly preserving exactly-once application semantics across hard power loss.

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
