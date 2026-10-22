# H: Scheduler State Ownership — Design Proposal

> **Status: proposal.** Nothing here is implemented. Level 1 of the three levels below is described in [Locks, and Who Owns the State](../contributors/08-locks-and-state-ownership.md) and is a contributor-sized task today. Levels 2 and 3 are the subject of this appendix, and level 3 presumes a decision about high availability that has not been made.

> **Superseded in part.** Draft 0.3 of the architecture reduction plan (October 22, 2026) takes this appendix as architectural motivation and supersedes four of its claims by name: the physical reading of I1, the equivalence between an assignment record and a worker notification in I3, the unconditional bound in I4, and the suggestion that a Raft log could be backed by Redis. All four are corrected below. The plan's own registry — AUTH, EFFECT, RES, TERM, LOOKUP, PUB, GC, REC, CFG, CTRL, LIVE — is the executable version of the table in this appendix, and where the two disagree the plan wins. "Decide this first", at the bottom, has since been decided: Configuration C is the shape of the plan's M4a and Configuration D is M4b.

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

## The invariants

Before asking what high availability should mean, name what a scheduler owes its clients. These nine are the properties the rest of this appendix is measured against. They are not aspirational: each one is either enforced somewhere in the tree today, enforced conditionally, or not enforced at all, and the next two sections say which.

| | Invariant | Statement |
|---|---|---|
| **I1** | At-most-one authoritative attempt | Each execution has at most one attempt authorised to produce its result. |
| **I2** | Terminal stability | An operation that reached a terminal state never leaves it. |
| **I3** | No ghost assignment | Every dispatch a worker can act on originates in committed state and carries a stable identity. Success is never inferred from having enqueued the message. |
| **I4** | Bounded failure detection | *Under stated assumptions* — reachable quorum, eventual delivery, advancing timers, responsive storage — a worker that dies has its actions requeued within a bounded, configured time. |
| **I5** | Capacity accounting | Reservations on a worker never exceed its slots, and completion refunds exactly what dispatch charged. |
| **I6** | Placement validity | An action dispatched to a worker satisfies that worker's platform properties *as evaluated at dispatch*, not as evaluated when it was queued. |
| **I7** | Client resumability | A client that disconnects can resume by operation id and observe the operation's current state. |
| **I8** | Submission durability | An accepted operation survives a scheduler restart. |
| **I9** | Progress | A queued action with a capable, idle, reachable worker is eventually dispatched. |

Three notes on reading these, and the first two are corrections to an earlier draft of this appendix.

**I1 is about authority, not physics.** An earlier version of this table said "an action is executing on at most one worker at a time", which is not a property any scheduler can enforce. Revoking an attempt's authority does not establish that its process stopped; a replacement attempt may run while the old one is still physically alive, and no clock proves otherwise. What is enforceable is that exactly one attempt is *authorised to produce the result*, which is what the plan calls AUTH 1. The consolation for cacheable hermetic actions still applies — duplicate execution writes identical content-addressed blobs, so the physical overlap costs CPU rather than correctness — but it is a property of the workload, not a licence, and it does not extend to result selection or capacity accounting. That is why I3 and I5 are separate.

**I3 is about committed intent, not delivery.** The earlier wording — "one that worker was actually told to run" — asks for knowledge the scheduler cannot have. The achievable property is that the intent was committed before it was sent and carries an identity stable across retries, so a duplicate or delayed delivery is harmless. The current tree already concedes the distinction: `PendingActionInfoData.accepted` exists because dispatching and being acknowledged are different facts (`worker.rs:68`).

**I4 is conditional, and it is the invariant that distinguishes the configurations below.** Everything else is roughly equal across them; failure detection is where they diverge sharply. But no bound on it holds unconditionally — safety never depends on a clock proving a remote process stopped, and liveness is owed only under reachable quorum, eventual delivery and advancing timers. A bound stated without its assumptions is a bound that will be wrong during a partition.

## The configurations that exist today

Both are measured against the inventory in [What the Scheduler Stores](../contributors/09-what-the-scheduler-stores.md), which is the premise this whole appendix rests on: the shared record is twelve fields, and its entire knowledge of the worker fleet is one nullable string, `worker_id: Option<WorkerId>`. Everything else a scheduler knows about a worker — its liveness, its reservations, what it was told and when it acknowledged — lives in one process's memory and is not `Serialize`.

Exactly two, selected by one config field — `experimental_backend` on `SimpleSpec` (`nativelink-config/src/schedulers.rs`). The word *experimental* is load-bearing and should be read as written.

### Configuration A: one scheduler, memory backend

```json5
{ "simple": { "experimental_backend": "memory" } }   // also the default when omitted
```

All state is `MemoryAwaitedActionDb` — a `BTreeMap` and a `HashMap` behind one mutex — plus the per-process worker registry. One process, one writer by virtue of that lock, every worker connected here, every client served here.

**Holds: I1–I7 and I9.** Not because the code is careful (though it is) but because there is exactly one of everything. The mutex serialises writers, the registry knows every worker, and `worker_timeout_s` (default 10s) is a real failure detector over workers this process is talking to directly.

**Fails: I8, totally.** A restart loses the queue. Clients receive a stream error and must resubmit; in-flight work on workers is orphaned until those workers reconnect and reconcile. This is the only invariant Configuration A misses, and it is the entire reason anyone reaches for Configuration B.

### Configuration B: N schedulers, Redis backend

```json5
{ "simple": { "experimental_backend": { "redis": { "redis_store": "SCHEDULER_REDIS_STORE" } } } }
```

Shipped as `nativelink-config/examples/worker_with_redis_scheduler.json5`. The awaited-action records move to Redis as versioned keys, so the *queue* is shared and survives restart. What does **not** move is the thing that matters most:

> **The queue is shared. The worker registry is not.**
>
> "A worker only appears in the registry of the instance holding its `connect_worker` stream" — `nativelink-scheduler/src/worker_registry.rs:32`.

That one sentence is the whole shape of Configuration B, and it is worth being blunt about what follows from it. N schedulers sharing Redis are **not** N interchangeable replicas. They are one shared queue in front of **N disjoint worker pools**, with no failure detector spanning them. There is no leader, no lease, no quorum, and no membership — `git grep -niE 'leader|election|lease|quorum|membership'` over `nativelink-scheduler/src` returns nothing but lock releases and `Ordering::Release`. Any process that can reach Redis is a scheduler, and no process knows how many peers exist or whether any of them is alive.

The code is scrupulous about this; the registry models it in the type:

```rust
// worker_registry.rs:36 — WorkerLiveness
Alive,    // registered here, heartbeat within the timeout
Stale,    // registered here, not heard from within the timeout
Unknown,  // not registered here at all — "not mine to judge", NOT "dead"
```

**What still holds.** I1, I2 and I3 survive on optimistic concurrency: records carry a version, assignment is a compare-and-set, and a losing replica gets `Aborted` and retries (`simple_scheduler_state_manager.rs:96,106` and the retry loops at `:384`, `:816`, `:985`). I5 holds *per replica* and trivially so, because a replica can only dispatch to workers it owns. I6 holds because assignment revalidates placement-relevant requirements rather than merely checking that the record is assignable — that fix is the N3 bug in [chapter 6](../contributors/06-concurrency-and-correctness.md), family 4. I7 holds through the shared store plus a subscription manager, with the caveat below. I8 is the point of the configuration and it holds.

**Where I4 goes.** A replica that sees an action assigned to a worker it does not know returns `Unknown`, and `Unknown` must not mean dead — a peer's healthy worker looks exactly like an abandoned one from here. So `worker_timeout_s` cannot apply, and the fallback is:

```rust
// simple_scheduler_state_manager.rs:942
WorkerLiveness::Unknown => {
    if self.max_executing_timeout > Duration::ZERO { silent_for(self.max_executing_timeout) }
    else { silent_for(ORPHANED_ACTION_TIMEOUT) }   // = 1 hour (worker_registry.rs:28)
}
```

`max_action_executing_timeout_s` has no runtime default — `#[serde(default)]` leaves it 0, and 0 means disabled. **So in the shipped default, when a scheduler replica dies, every action its workers were executing stays `Executing` for one hour before anything reaps it.** I4's bound is one hour, per action, and it is one hour precisely because the honest alternative is worse: reaping earlier would kill live work on healthy peers' workers. The comment at `worker_registry.rs:24-27` says exactly this — "heartbeats never reach the shared state, so a healthy hour-long action and an abandoned one look identical from here, and acting early costs live work."

Setting `max_action_executing_timeout_s` tightens the bound at the cost of capping how long any action may legitimately run. That is the trade-off the configuration forces, and there is no setting that gets both.

It is worth noting what the configuration *does* get right, because it shows the gap is mechanical rather than conceptual. The one piece of fleet state that is shared — `fc_<scheduler_id>`, each scheduler's worker *shapes* — is republished every five seconds under a fifteen-second TTL, and a peer that dies drops out of the index on its own. A working TTL-based membership protocol is already running in this configuration. It carries shapes and not identities, so it can say that nothing in the fleet can run a given platform property set, but not that worker `w-7` is gone. The distance from fifteen seconds to one hour is the distance between those two facts.

**Where I9 goes.** Redis searches are eventually consistent with respect to the notification that triggered them, so a re-queued operation can be invisible to the matching pass its own notification woke. `fallback_match_interval_s` (default 5s) exists for that: an unconditional matching pass on a timer, described in its own doc comment as "a safety net for missed notifications and for scheduler backends with eventually consistent searches." I9 therefore holds with a latency bound of about five seconds rather than promptly, and holds *only because* that timer is on. Similarly, the subscription manager is explicitly allowed to be unreliable, with polling as the fallback (`store_awaited_action_db.rs:103-104`) — which is what keeps I7 true.

### The matrix

| | A: one scheduler, memory | B: N schedulers, Redis |
|---|---|---|
| I1 at-most-one executor | by single writer | by versioned CAS |
| I2 terminal stability | yes | yes |
| I3 no ghost assignment | yes | yes |
| I4 bounded failure detection | **`worker_timeout_s`, default 10s** | **1 hour** for a peer's workers, or `max_action_executing_timeout_s` if set |
| I5 capacity accounting | yes | per replica |
| I6 placement validity | yes | yes |
| I7 client resumability | yes | via store + subscription, polling fallback |
| I8 submission durability | **no** | yes |
| I9 progress | prompt | ~5s, and only with `fallback_match_interval_s` on |

Read across the I4 row and the I8 row together, because that is the actual choice on offer today: **a ten-second failure detector and no durability, or durability and a one-hour failure detector.** Neither is high availability. Configuration A is a single point of failure that fails fast and cleanly. Configuration B survives a restart and degrades, on the death of a replica, into an hour of actions that no surviving process is willing to touch.

Which is why the question below is not academic, and why answering it is a prerequisite for level 3 rather than an output of it.

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
| `RaftLogStorage`, `RaftLogReader`, `RaftSnapshotBuilder` | the log: in-memory for fast restart, an embedded durable journal otherwise. A snapshot is the applied state, so a new leader starts hot. **Not Redis** — an earlier draft offered Redis streams here, and the reduction plan removes Redis permanently, journal included. Note also that in 0.9.25 returning from append is not itself the durable acknowledgement; `LogFlushed` is, and the chosen journal has to be qualified against that |
| `RaftNetworkFactory`, `RaftNetwork` | append-entries and vote over tonic, which the binary already serves |
| `AsyncRuntime` (`TokioRuntime`) | the existing runtime; the state machine never touches it |
| `Raft::client_write` | what `Execute`, the worker API and the sweep call instead of taking a lock |
| `Raft::ensure_linearizable` plus a local read | `WaitExecution`, admin listings, the matcher's view of the queue |
| `Raft::add_learner`, `change_membership` | explicit membership, instead of "anything that can reach Redis is a scheduler". A one-member cluster is the non-HA deployment, same binary |

### Where the model pushes back

**Matcher decisions are state transitions, so they go through the log.** The leader proposes `Assign(action, worker)`; it applies when committed; the worker is told after commit. Reservation holds and parked placements stop being shared state and become leader-local heuristics that *feed* proposals. What that costs per deployment shape is the next section.

**What today's code becomes.** `MemoryAwaitedActionDb` is applied state. `StoreAwaitedActionDb`'s versioned CAS writes disappear as a concurrency mechanism and do not come back: the log replaces them, and Redis leaves entirely. `api_worker_scheduler`'s 29 lock sites become "worker events in, assignment events out". The trait surface survives as the client side of the owner.

## The configurations level 3 would offer

Level 3 does not add high availability as a feature; it makes the single owner the unit of replication, and the deployment shapes fall out of the membership you configure. Three shapes, one binary, and the distinction between them is the cluster size and whether the action space is partitioned.

### Configuration C: one-member cluster

A single scheduler whose owner applies a durable log before acknowledging. `change_membership` has one node; no network, no election, no commit round trip beyond the local append.

This is Configuration A **plus I8**, and that is the entire point. The thing Configuration B is reached for — "my scheduler restarted and the build should continue" — is delivered here with a ten-second failure detector intact, because there is exactly one registry and it knows every worker. Recovery is "replay the log from the last snapshot", and because a snapshot *is* the applied state, the restarted process starts hot rather than rebuilding from Redis.

What it does not survive is the machine. A one-member cluster is a single point of failure; it is a single point of failure that no longer loses work.

### Configuration D: multi-member, one leader

Three or five members, one leader by Raft election, the leader runs the matcher. Followers apply the committed log and hold identical resident state.

The invariant this buys that nothing else does is **I4 restored across the whole fleet, under I4's stated assumptions**. Failure detection stops depending on which process a worker happens to be connected to, because the registry is replicated state rather than per-process state. A worker's heartbeats are log entries; every member sees them; any member can judge any worker. `Unknown` stops existing as a liveness verdict, and with it the one-hour ceiling.

The qualifier matters. What the replicated registry removes is the *structural* reason the bound was an hour — that no process could distinguish a peer's healthy worker from an abandoned one. It does not remove the need for a quorum to be reachable and timers to be advancing before any bound applies at all. During a partition the minority has no bound because it has no authority, which is the correct answer and not a loophole.

The costs are real and should be stated:

- **One commit round trip on the dispatch path.** The leader proposes `Assign(action, worker)` and tells the worker only after commit. Sub-millisecond on a LAN; zero on Configuration C.
- **Workers and clients still connect to one process.** Raft replicates the scheduler's state, not its gRPC sessions. On a leader change, workers reconnect and reconcile ([#2925](https://github.com/TraceMachina/nativelink/pull/2925)'s running-operations-on-keepalive is that mechanism), and clients resume with `WaitExecution` by operation id — which REAPI already requires of them, so this is not new protocol surface. **I7 is the invariant that makes failover tolerable, and it is the one REAPI hands us for free.**
- **A minority partition stops scheduling.** This is the correct behaviour and worth saying out loud, because Configuration B does the opposite: a partitioned replica in Configuration B keeps dispatching to its own workers from a queue it can no longer safely reason about. Raft trades that availability for authority that is never ambiguous: in the minority there is nothing to be wrong about. The invariants are still conditional — they are simply conditioned on something checkable (a quorum) rather than on a guess about a peer.

### Configuration E: sharded

Partition the action space, one log and one leader group per shard. This is the only shape that scales decision-making horizontally, and it is strictly more machinery than D: a shard map, a routing layer, and a rebalancing story.

Nothing in NativeLink needs it today. The matcher's pass is already serialised by the locks it takes, and at the fleet sizes in question — thousands of actions, tens of workers, decisions in microseconds — the scheduler is not the bottleneck. Sharding is the answer if that ever changes, and naming it here is mostly to keep it from being confused with D, because "two schedulers sharing load" describes E while almost everyone who says it means D.

### The matrix, extended

| | A: memory | B: N + Redis | C: 1-member | D: multi-member | E: sharded |
|---|---|---|---|---|---|
| I4 failure detection | 10s | **1 hour** (peers) | 10s | 10s, fleet-wide | 10s, per shard |
| I8 durability | **no** | yes | yes | yes | yes |
| I9 progress | prompt | ~5s | prompt | prompt | prompt |
| Survives process restart | no | yes | yes | yes | yes |
| Survives machine loss | no | partially, badly | no | yes | yes |
| Scales decisions | — | no (shared queue) | — | no | yes |
| Partition behaviour | — | keeps dispatching, unsafely | — | minority stops | minority stops per shard |
| Dispatch latency cost | none | Redis round trips | log append | one commit | one commit |

## Decide this first

The three levels are not a roadmap to walk end to end.

**Level 1 is unambiguously worth doing** and costs days. It removes family 7 for the state DB and does not commit anyone to anything.

**Level 2 is worth doing on its own merits.** It deletes more code than it adds, makes the scheduler's interleaving tests deterministic, and is a strictly local change behind a trait seam the service crate already uses from one place. It does not commit anyone to Raft. If nothing else in this appendix is ever built, level 2 still pays.

**Level 3 requires a decision nobody has made**, and the decision is not "should we have HA" — it is which of these sentences is the requirement:

1. *"A scheduler process can restart and the build continues."* → **Configuration C.** One member, a durable log, no election, no round trip. This is what most people mean, it is the cheapest thing on this list that is honestly HA-adjacent, and it is strictly better than Configuration B at the thing Configuration B is for.
2. *"A scheduler machine can die and the build continues."* → **Configuration D.** Accept the commit round trip and that a minority partition stops scheduling.
3. *"Scheduling throughput scales with more schedulers."* → **Configuration E**, and first produce the measurement showing the matcher is the bottleneck, because nothing in the current fleet suggests it is.

Those are three different systems. The current Redis-backed model quietly promises the third, is reached for to get the first, and delivers the second badly — any process that can reach Redis is a scheduler, coordination is optimistic concurrency on individual records, there is no membership and no leader, and the price of that honesty about peers is the one-hour ceiling on I4.

So the question to put to whoever owns the product, in one sentence: **when a scheduler dies, what is the longest a running action may sit unreaped — and are you willing to pay a commit round trip per dispatch to make that number small?** Everything above follows from the answer, and nothing above should be built before it.

### The answer, as of Draft 0.3

That question has been answered, and the answer is *both, in order*. The reduction plan takes sentence 1 and sentence 2 as sequential qualification stages of one implementation rather than as alternative deployments: M4a delivers singleton-durable authority — Configuration C — and M4b qualifies the identical code path with replicated authority, which is Configuration D. Sentence 3 is not in the program. "Desired node count" is the only remaining scheduler topology choice, and it drives one membership reconciler rather than selecting an algorithm.

Two things in the plan are sharper than this appendix managed, and both are worth carrying back.

**The singleton is not a special case.** This appendix treated Configuration C as the cheap option and D as the upgrade. The plan forbids a singleton bypass outright: volatile and durable authority implement the same interface and the same logical rules, differing only in a declared recovery envelope, and the one-member deployment uses the same proposal, commit, apply and effect path as a five-member one. That is a stronger statement than "same binary", which is all this appendix claimed, and it is what makes M4b a qualification rather than a port.

**Failure detection was the wrong thing to lead with.** This appendix organised everything around I4, because I4 is where the shipped configurations diverge most visibly. The plan organises around *effects*: every externally actionable intent originates in committed state with a stable identity, and uncertain delivery is retried without inferring success. Read that way, the one-hour ceiling is a symptom rather than the disease — it is what you get when liveness is inferred from a timestamp instead of from an owned, committed intent. The plan's instruction not to use timestamps as fencing tokens is the general form of the bug this appendix found one instance of.
