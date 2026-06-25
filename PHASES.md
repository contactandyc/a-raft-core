My goal is to be building a **smaller, stricter, more verifiable C Raft stack** with better embeddability, deterministic failure testing, and operational guardrails. etcd’s own advantage is maturity: its raft library is stable, widely used, deterministic, and deliberately leaves network/disk I/O to integrators. Your advantage can be: **one integrated, low-latency, audited C distribution with storage, transport, tests, and formal artifacts included**.

## Overall goals

1. **Embeddability:** C ABI, no Go runtime, no GC, small surface area.
2. **Tail latency:** libuv event loop plus bounded disk workers and no garbage collector.
3. **Whole-stack determinism:** core + WAL + codec + transport fault injection.
4. **Operational self-checking:** online WAL verification, snapshot verification, startup invariants.
5. **Auditability:** smaller codebase, model specs, invariant assertions, sanitizer-clean CI.
6. **Resource predictability:** strict payload limits, bounded queues, sliding WAL offset windows, O(1) uncommitted-byte accounting. Your current code already tracks uncommitted bytes in the log path and deducts them on commit/truncation, which is the right direction for predictable backpressure.

That is a credible niche.

---

## 1. Split the library into five explicit layers

Right now you are building a combined Raft library/server. That is useful, but to compete with etcd’s clean architecture, split it into layers with hard contracts:

```text
libaraft_core
  Pure deterministic Raft state machine.
  No malloc failure surprises, no disk, no sockets, no timers except logical ticks.

libaraft_storage
  WAL, hardstate, snapshot metadata, checksums, recovery, compaction.

libaraft_transport
  Codec, framing, peer connections, backpressure, authentication hooks.

libaraft_node
  Async pump: converts Ready -> durable writes -> network sends -> apply callbacks.

libaraft_testkit
  Deterministic simulator, fault injection, network partitions, disk crash testing.
```

etcd’s own README emphasizes a minimal Raft core where network and disk I/O are left to the user, with deterministic state-machine behavior from message input to output messages/log entries/state changes. ([GitHub][1]) You can keep your integrated server, but make it **optional**. The best architecture is:

```text
serious users can embed only core + their own storage/transport
most users can use your batteries-included node/server
```

That gives you etcd-style rigor and Consul-style convenience.

---

## 2. Add a deterministic simulator before adding features

The fastest way to “buy years of testing” is to make bugs cheap to generate. Build a simulator that runs your core in a single process with fake clocks, fake disks, fake networks, and randomized schedules.

It should generate:

```text
drop message
duplicate message
reorder message
delay message
partition cluster
heal partition
crash node before fsync
crash node after fsync before send
restart from disk
install snapshot mid-flight
truncate WAL tail
corrupt WAL frame
change membership
promote learner
remove leader
issue ReadIndex
compact at random applied index
```

Then assert global invariants every step:

```text
No two applied entries differ at same index.
No committed entry disappears after restart.
At most one leader per term.
Future leaders contain committed entries.
lastApplied <= commitIndex <= lastIndex.
snapshotIndex <= lastApplied.
Every persisted commit is recoverable.
No learner counts toward quorum.
No read is served before its ReadIndex is applied.
```

This is where you can beat etcd in confidence-per-line-of-code. etcd has deep production exposure; you need **exhaustive simulated adversity**.

---

## 3. Write a TLA+ or PlusCal model of your exact variant

Do not model generic Raft. Model **your Raft**:

```text
pre-vote
check-quorum
learners
single-server config changes
apply-time membership
ReadIndex
snapshot install
snapshot ConfState
current-term commit rule
```

Then map each implementation invariant to a model invariant. The goal is not academic decoration. It is to make future refactors safer. Add comments in code like:

```c
// TLA invariant: AppliedLogAgreement
// TLA invariant: OneConfigChangeInFlight
// TLA invariant: CurrentTermCommitOnly
```

This makes the code auditable in a way most embedded Raft libraries are not.

---

## 4. Make storage self-verifying like Consul’s LogStore verification

Consul’s Raft configuration now includes online LogStore verification: the leader periodically writes checkpoint log messages with checksums of entries, and followers recompute and report verification results; HashiCorp documents this as low-overhead and safe in production. ([HashiCorp Developer][2])

You should implement the same idea, but stricter:

```text
ENTRY_VERIFY_CHECKPOINT {
    start_index
    end_index
    crc64_or_xxh3_128_of_terms_indexes_types_payloads
    snapshot_index
    snapshot_term
}
```

Followers verify asynchronously from WAL, not memory. If verification fails:

```text
mark node unhealthy
stop accepting Raft traffic
require operator wipe/rejoin or automatic snapshot repair
```

This would be a major “better than etcd core” differentiator because etcd/raft itself does not own storage. You own storage, so use that advantage.

---

## 5. Make snapshots cryptographically bound to metadata

Your snapshot contract guard is a good baseline, but a production-grade version should not merely check that `snap_grpX.dat` exists. It should bind bytes to metadata:

```c
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t group_id;
    uint64_t snapshot_index;
    uint64_t snapshot_term;
    uint64_t payload_len;
    uint64_t conf_state_hash;
    uint8_t  payload_hash[32];
} raft_snapshot_header_t;
```

Then require:

```text
raft_node_compact(index) only succeeds if:
  snapshot file exists
  header.index == index
  header.term == raft_log_term(index)
  ConfState hash matches core ConfState at index
  payload checksum verifies
  file fsync + directory fsync completed
```

Consul’s docs emphasize snapshots for restoring Raft-index state and durable data directories; your opportunity is to make the snapshot artifact itself self-describing and self-verifying. ([GitHub][3])

---

## 6. Add etcd-style replication flow control and optimistic pipelining

Your current leader replication is conservative. etcd explicitly lists optimistic pipelining, replication flow control, message batching, log-entry batching, and parallel leader disk writes as optional enhancements. ([GitHub][1]) Its config also exposes `MaxInflightMsgs` to bound in-flight append messages and `MaxUncommittedEntriesSize` to bound uncommitted log bytes. ([Go Packages][4])

Add per-peer progress state:

```c
typedef enum {
    PROBE,
    REPLICATE,
    SNAPSHOT
} raft_progress_state_t;

typedef struct {
    uint64_t match_index;
    uint64_t next_index;
    uint64_t inflight_first;
    uint64_t inflight_last;
    size_t   inflight_msgs;
    size_t   inflight_bytes;
    raft_progress_state_t state;
    bool paused;
} raft_progress_t;
```

Behavior:

```text
PROBE: send one append, wait for response.
REPLICATE: optimistically advance next_index and keep N messages in flight.
SNAPSHOT: send snapshot chunks, block normal appends until done.
```

Expose knobs:

```c
max_inflight_msgs
max_inflight_bytes_per_peer
max_append_batch_bytes
max_append_batch_entries
max_uncommitted_bytes
```

This is one place you can match etcd directly and potentially beat it for tail-latency predictability.

---

## 7. Add lease reads only as an optional, explicitly unsafe-without-clock-bound mode

Your ReadIndex-safe path is the right default. etcd’s docs distinguish `ReadOnlySafe`, which communicates with quorum, from lease-based reads, which depend on clock assumptions and can be affected by clock drift. ([Go Packages][4])

Add:

```c
RAFT_READ_SAFE       // current quorum ReadIndex
RAFT_READ_LEASED     // requires monotonic clock + configured max drift
RAFT_READ_STALE      // follower local read for explicitly stale clients
```

But make `RAFT_READ_SAFE` the default forever. To be better than etcd here, expose the risk in the API:

```c
raft_node_read_index_safe(...)
raft_node_read_lease(..., max_clock_drift_ns)
raft_node_read_stale(...)
```

No ambiguous “read” function.

---

## 8. Add leadership transfer

etcd lists leadership transfer as a feature. ([GitHub][1]) You should add it after pipelining.

API:

```c
int raft_node_transfer_leader(raft_node_t* node, uint64_t target_id);
```

Rules:

```text
target must be voter
target must not be learner
target match_index >= leader last_index
leader sends TimeoutNow
leader stops accepting new proposals during transfer window
transfer times out safely
```

This is important operationally: it enables graceful maintenance without forcing random elections.

---

## 9. Make membership operations first-class APIs

Do not ask users to manually encode config entries. Provide:

```c
int raft_node_add_learner(raft_node_t*, uint64_t node_id);
int raft_node_promote_learner(raft_node_t*, uint64_t node_id);
int raft_node_remove_member(raft_node_t*, uint64_t node_id);
```

Each should return precise errors:

```c
RAFT_ERR_CONFIG_IN_FLIGHT
RAFT_ERR_NOT_LEADER
RAFT_ERR_NOT_CAUGHT_UP
RAFT_ERR_WOULD_LOSE_QUORUM
RAFT_ERR_UNKNOWN_NODE
RAFT_ERR_ALREADY_MEMBER
```

etcd has a strict reconfiguration philosophy; its docs warn against reconfiguration that can cause quorum loss. ([etcd][5]) You can make this safer than many libraries by refusing dangerous operations by construction.

---

## 10. Build a “correct-by-default” operating profile

Production users should not need to tune 30 flags. Ship profiles:

```c
RAFT_PROFILE_LOCAL_SSD_LOW_LATENCY
RAFT_PROFILE_CLOUD_VM_BALANCED
RAFT_PROFILE_WAN_SAFE
RAFT_PROFILE_TEST_DETERMINISTIC
```

Each profile sets:

```text
heartbeat interval
election timeout range
max inflight messages
max append bytes
snapshot chunk bytes
max uncommitted bytes
WAL segment size
fsync policy
ReadIndex mode
check-quorum
pre-vote
```

Consul exposes raft logstore, snapshot threshold, snapshot interval, WAL segment size, and verification parameters in config. ([HashiCorp Developer][2]) Your advantage can be fewer knobs with safer presets.

---

## 11. Add observability as a first-class API, not printf logs

Expose a metrics snapshot:

```c
typedef struct {
    uint64_t current_term;
    uint64_t commit_index;
    uint64_t last_applied;
    uint64_t last_index;
    uint64_t snapshot_index;
    uint64_t leader_id;

    uint64_t proposals_accepted;
    uint64_t proposals_dropped;
    uint64_t append_sent;
    uint64_t append_rejected;
    uint64_t snapshots_sent;
    uint64_t snapshots_installed;

    uint64_t wal_fsync_count;
    uint64_t wal_fsync_ns_p50;
    uint64_t wal_fsync_ns_p99;
    uint64_t apply_ns_p99;
    uint64_t read_index_ns_p99;

    uint64_t outbound_queue_bytes;
    uint64_t uncommitted_bytes;
} raft_node_metrics_t;
```

And per-peer status:

```c
match_index
next_index
inflight_msgs
inflight_bytes
state: probe/replicate/snapshot
recent_active
is_learner
snapshot_offset
```

etcd exposes detailed status/logging through its ecosystem; you can make the embedded C API easier to monitor.

---

## 12. Add fault-injection hooks behind `#ifdef RAFT_TESTING`

Do not rely on `chmod` tests. Add deterministic failure points:

```c
RAFT_FAILPOINT("wal.pwrite.after_n_bytes")
RAFT_FAILPOINT("wal.fsync.fail")
RAFT_FAILPOINT("meta.rename.fail")
RAFT_FAILPOINT("snapshot.pwrite.short")
RAFT_FAILPOINT("uv.write.fail")
RAFT_FAILPOINT("malloc.fail_after_n")
RAFT_FAILPOINT("network.drop.append")
RAFT_FAILPOINT("clock.freeze")
```

Then tests become:

```c
raft_failpoint_enable("wal.fsync.fail", 1);
...
MACRO_ASSERT_TRUE(node.fatal_error);
```

This will produce far more confidence than ordinary unit tests.

---

## 13. Add a C ABI stability layer

If you want adoption, make the API boring and stable.

Use opaque handles:

```c
typedef struct raft_node raft_node_t;
typedef struct raft_config raft_config_t;
```

Return structured errors:

```c
typedef enum {
    RAFT_OK = 0,
    RAFT_ERR_NOT_LEADER,
    RAFT_ERR_NOMEM,
    RAFT_ERR_IO,
    RAFT_ERR_CORRUPT,
    RAFT_ERR_INVALID_ARG,
    RAFT_ERR_CONFIG_IN_FLIGHT,
    RAFT_ERR_QUORUM_LOSS,
    RAFT_ERR_OVERSIZED_ENTRY
} raft_err_t;
```

Expose error strings:

```c
const char* raft_strerror(raft_err_t);
```

Version every durable format:

```text
WAL segment version
WAL frame version
hardstate version
snapshot version
network protocol version
```

HashiCorp Raft explicitly notes protocol versioning and compatibility in its library history; this is a real production concern, not an afterthought. ([Go Packages][6])

---

## 14. Keep the integrated server, but make it optional

Your integrated libuv server is a strength for users who want a complete stack. But the API should allow:

```text
core only
core + WAL
core + WAL + codec
full server
```

Suggested package split:

```text
include/a-raft-library/raft_core.h
include/a-raft-library/raft_storage.h
include/a-raft-library/raft_transport.h
include/a-raft-library/raft_node.h
include/a-raft-library/raft_server.h
include/a-raft-library/raft_testkit.h
```

This lets an embedded database use its own network layer while still trusting your core/WAL.

---

## 15. Add a compatibility and conformance suite

Build a standalone binary:

```bash
araft-conformance
```

It should run:

```text
Raft paper Figure 8 scenarios
Raft dissertation membership scenarios
snapshot overlap/stale/chunk reorder cases
WAL torn-write matrix
crash-before/after-fsync matrix
ReadIndex partition cases
learner promotion under lag
remove leader
restore from every durable boundary
```

Then publish:

```text
number of simulated schedules run
number of crash points explored
seed corpus
coverage report
sanitizer report
TLA model hash
```

This is how you partially compensate for not having years of production.

---

## My recommended “beat etcd” roadmap

### Phase 1: Finish correctness hardening

Do these before adding performance features:

```text
Finish sliding-window WAL offset map.
Reject oversized follower AppendEntries.
Fix snapshot MAX_REMOTE_PEERS validation.
Add snapshot metadata/header/checksum.
Add fault-injection hooks.
Add deterministic simulator.
```

Your tests already cover boot from a purged WAL, but the WAL rebasing work needs explicit tests for offset-base advancement, map shrinking, reading purged entries, and booting with rebased offsets.

### Phase 2: Match etcd features

```text
Optimistic append pipelining.
Per-peer inflight windows.
Leadership transfer.
Follower ReadIndex forwarding.
Proposal forwarding toggle.
Strict membership APIs.
Per-peer status API.
```

### Phase 3: Beat etcd operationally

```text
Online WAL verification checkpoints.
Snapshot metadata verification.
Built-in fault injection.
Built-in deterministic simulator.
Stable C ABI.
Prometheus/OpenTelemetry metrics bridge.
Zero-copy append batching API.
io_uring backend on Linux, libuv fallback elsewhere.
```

### Phase 4: Prove it

```text
TLA+/PlusCal model.
Sanitizer-clean CI: ASan, UBSan, TSan where applicable.
libFuzzer/AFL harnesses for codec/WAL/snapshot parser.
Deterministic simulation nightly with millions of seeds.
Crash-recovery matrix tests.
Jepsen-style black-box cluster tests.
```

## Final take

The way to make this better than etcd is not to clone etcd feature-for-feature. It is to become the **most auditable, embeddable, failure-tested C Raft stack** available.

Your differentiator should be:

```text
etcd-grade Ready architecture
+
Consul-style integrated storage/server option
+
C-level latency predictability
+
built-in deterministic simulation
+
self-verifying WAL/snapshot artifacts
+
strict resource bounds everywhere
```
