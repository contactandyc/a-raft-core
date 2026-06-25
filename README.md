# a-raft-core

`a-raft-core` is a mathematically pure, 100% deterministic C implementation of the Raft consensus algorithm.

Built on the **Ready Pattern** (popularized by `etcd`), this library contains absolutely zero I/O. It has no concept of disks, sockets, or thread pools. It is a mathematical black box that consumes structs, mutates its internal state, and produces an array of commands for the host application to execute.

## Key Features
* **Zero I/O:** Completely decoupled from networking and disk storage.
* **Deterministic:** Safe to run in simulation environments.
* **Bounded Memory:** Strict pre-allocation strategies prevent mid-consensus OOM panics.
* **Learner Nodes:** Native support for non-voting replica nodes for safe topology transitions.
* **ReadIndex:** True linearizable reads without invoking the disk.

## The Ready Pattern
Instead of firing callbacks that block the state machine, `a-raft-core` returns a `raft_ready_t` struct containing exactly what the host application needs to do next:

1. Send the `messages` array over the network.
2. Write the `entries_to_save` array to the Write-Ahead Log.
3. Apply the `committed_entries` array to your state machine.

```c
raft_ready_t ready = raft_get_ready(core);
// ... handle I/O ...
raft_advance(core, highest_saved_index, actual_applied_index);
```
