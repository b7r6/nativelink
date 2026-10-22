# H: Scheduler State Ownership — Design Proposal

> **Status: proposal.** Nothing here is implemented. Level 1 of the three levels below is described in [Locks, and Who Owns the State](../contributors/08-locks-and-state-ownership.md) and is a contributor-sized task today. Levels 2 and 3 are the subject of this appendix, and level 3 presumes a decision about high availability that has not been made.

## The argument

The scheduler's correctness problems share a root cause, and it is not any individual lock. It is that the scheduler's state has no owner. Four trait implementations — `ClientStateManager`, `WorkerStateManager`, `MatchingEngineStateManager`, `WorkerScheduler` — mutate a queue and a worker registry from whatever task happens to call them, coordinating through locks, optimistic-concurrency retries, and generation counters. Every bug family in [chapter 6](../contributors/06-concurrency-and-correctness.md) is a different way for that coordination to be subtly wrong, and the code that exists to make it right is itself substantial:

| Mechanism | Where | Exists because |
| --------- | ----- | -------------- |
| `MAX_UPDATE_RETRIES`, `version_conflict_backoff` | `simple_scheduler_state_manager.rs:96,106` | two writers can race a versioned record |
| retry loops at `:384`, `:816`, `:985` | same | the same race, three call sites |
| "Action already assigned" rejection | `simple_scheduler_state_manager.rs:1289` | a dispatch can arrive twice |
| re-check-after-lock, dispatch generations, pass generations | `api_worker_scheduler.rs:938` | an observation can go stale between lock drops |
| the placement-requirement revalidation (N3) | `simple_scheduler_state_manager.rs` | a passing CAS can still be a wrong decision |

None of that code is wrong. All of it is the cost of not having a single writer. The proposal is to pay for a single writer instead, and delete the rest.

## Level 2: one owner task

Make one task per scheduler process the sole owner of the queue and the worker registry. The four trait implementations become message senders with oneshot replies; the matcher pass becomes a message like any other. No shared mutable state, therefore no locks, therefore none of Family 1, 4, or 7 by construction.

The seam already exists. The service crate reaches the scheduler through those four traits from one place, so the change is behind an interface that does not move. The core it replaces:

| File | Lines |
| ---- | ----: |
| `api_worker_scheduler.rs` | 3,025 |
| `simple_scheduler_state_manager.rs` | 1,826 |
| `simple_scheduler.rs` | 1,791 |
| `store_awaited_action_db.rs` | 1,360 |
| `memory_awaited_action_db.rs` | 1,085 |
| **total** | **9,087** |

Much of that evaporates rather than moves. The retry machinery, the version-conflict backoff, the already-assigned rejection, and every re-check-after-lock exist only to tolerate concurrent writers. The matcher itself transfers nearly verbatim: it is already written as a pass over a listing that makes decisions, which is exactly what an owner task does.

The test story improves more than the production story. The mid-pass interleaving tests — `room_opening_mid_pass_keeps_the_listing_order_across_shapes` at `nativelink-scheduler/tests/simple_scheduler_test.rs:3851` is the clearest example — are racy by construction, because they must arrange a real interleaving between real tasks to test what they test. Against an owner task they become what they always wanted to be: a deterministic sequence of messages with an asserted outcome.

**What level 2 costs.** One task is one core for the scheduler's state transitions. The matcher pass is already effectively serialized by the locks it takes, so this is close to a no-op in practice, but it does mean a scheduler cannot scale its decision-making across cores. For NativeLink's workload — thousands of actions, tens of workers, decisions in microseconds — that is not the bottleneck. If it ever becomes one, the answer is partitioning the action space, not re-introducing shared state.

## Level 3: the owner is the replication unit

Level 2 makes the scheduler a deterministic state machine driven by an ordered sequence of events. That is also the precondition for replication, which is why the two should be designed together even if only level 2 is built.

Model the scheduler as a state machine over an ordered event log: `Submit`, `WorkerRegister`, `WorkerKeepalive`, `WorkerReport`, `Result`, `Tick`, `Assign`. One owner per replica applies the log to resident state. The leader additionally runs the matcher and appends its decisions to the log. Failover becomes "the lease moves; the new leader resumes from its applied index", with hot state rather than a cold rebuild from Redis.

This also delivers the property that would have caught #3001 in CI: a core that is replayable from a log can be replayed *in a test*, deterministically, against an arbitrary event schedule. Today the scheduler's tests approximate that from the outside, with a current-thread runtime and a mocked clock; level 3 would make it structural, and make an arbitrary event schedule a first-class test input rather than something each test arranges by hand.

### Mapping onto openraft

[`databendlabs/openraft`](https://github.com/databendlabs/openraft), pinned at **0.9.25** (`release-0.9`; 0.10 is alpha and the API is unstable before 1.0). Apache-2.0 or MIT. In production under Databend, CnosDB, RobustMQ and Hiqlite. The example to crib is `examples/raft-kv-memstore`; `examples/raft-kv-rocksdb` is the durable shape.

| openraft | NativeLink scheduler |
| -------- | -------------------- |
| `RaftTypeConfig` | `NodeId` is a scheduler instance; `D` is the event enum; `R` is the reply (operation id, assignment outcome) |
| `RaftStateMachine::apply` | the owner. Applies events in log order to the queue `BTreeMap`s and the worker registry, driven by one task, in order — that *is* the single-writer guarantee. `MemoryAwaitedActionDb` minus its lock goes here |
| `RaftLogStorage`, `RaftLogReader`, `RaftSnapshotBuilder` | the log: in-memory for fast restart, RocksDB or Redis streams for durable. A snapshot is the applied state, so a new leader starts hot |
| `RaftNetworkFactory`, `RaftNetwork` | append-entries and vote over tonic, which the binary already serves |
| `AsyncRuntime` (`TokioRuntime`) | the existing runtime; the state machine never touches it |
| `Raft::client_write` | what `Execute`, the worker API and the sweep call instead of taking a lock |
| `Raft::ensure_linearizable` plus a local read | `WaitExecution`, admin listings, the matcher's view of the queue |
| `Raft::add_learner`, `change_membership` | explicit membership, instead of "anything that can reach Redis is a scheduler". A one-member cluster is the non-HA deployment, same binary |

### Where the model pushes back

**Matcher decisions are state transitions, so they go through the log.** The leader proposes `Assign(action, worker)`; it applies when committed; the worker is told after commit. That adds one commit round trip to the dispatch path — sub-millisecond on a LAN, nothing at all on a one-member cluster. Reservation holds and parked placements stop being shared state and become leader-local heuristics that *feed* proposals.

**Workers and clients still connect to one process.** On a lease change they reconnect. [#2925](https://github.com/TraceMachina/nativelink/pull/2925)'s running-operations-on-keepalive is the reconciliation mechanism, and `WaitExecution` by operation id is how clients resume — which REAPI already requires of them, so this is not new protocol surface.

**What today's code becomes.** `MemoryAwaitedActionDb` is applied state. `StoreAwaitedActionDb`'s versioned CAS writes disappear as a concurrency mechanism and come back as log transport, if Redis stays in the picture at all. `api_worker_scheduler`'s 29 lock sites become "worker events in, assignment events out". The trait surface survives as the client side of the owner.

## Decide this first

The three levels are not a roadmap to walk end to end. Level 1 is unambiguously worth doing and costs days. Level 2 is worth doing on its own merits — it deletes more code than it adds and makes the scheduler's tests deterministic — and it does not commit anyone to Raft.

Level 3 requires a decision nobody has made: **what high availability is supposed to mean here.**

- "The scheduler restarts and the build continues" is a single leader — a one-member cluster, or a lease — and level 3 delivers it directly.
- "Two schedulers share the load" is a different system. It needs partitioning on top: shard the action space, one log per shard.

The current Redis-backed model quietly promises the second and delivers neither cleanly: any process that can reach Redis is a scheduler, coordination is by optimistic concurrency on individual records, and there is no membership, no leader, and no bound on how stale a participant's view may be. Choosing between those two meanings is the prerequisite for level 3, and it is a product decision, not an engineering one.
