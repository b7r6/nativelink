# Replicated scheduler state: design

> Status: draft, 2026-10-13. Builds on `spike/openraft-awaited-db` and its
> `CROSSED-OFF.md` (including the Astra premortem errata). That document argues
> *that* a log removes the storage choreography; this one specifies *what the
> log contains, who proposes each entry, and how committed state turns into
> effects on workers*. Paths are relative to the repo root.

## Status (2026-10-13)

**Built:** P1 (durable single voter, selectable as
`experimental_backend: { raft: { node_id, data_dir, snapshot_every } }`)
and the wire half of P2 (`attempt` on every worker message, worker dedup,
kill tombstones). Not built yet: the `Offered`/`Cancelling` stages and the
effect dispatcher (rest of P2), multiple voters (P3).

**P2, scheduler side (all backends):** done: a silent worker is not offered
work; losing a silent worker costs `worker_losses`, not the action's
retries; a revoked attempt is killed at once (the sweep stays as backstop);
an assignment that loses a version conflict is not retried (a retried stale
choice dispatched attempt 3 to a worker lost two requeues earlier). The client-visible
`Offered` stage (§4.1) is opt-in, `report_executing_on_accept` (default
off: no extra write, no change in what clients see, stored records
byte-identical): an assigned action shows as queued until its worker
accepts it. Internally the stage stays `Executing`, so the matcher never
offers it twice. Scheduler suite after P2: 257/257 on memory and on raft,
0 of 10 flaky full runs;
`restart-chaos.sh raft`: 0 findings, 66/66 oracle comparisons equivalent,
1307 operations completed.

**Where this differs from the plan below, and why:**

- *Log encoding is JSON, not bincode* (no new dependency for P1). Framed
  with a CRC, fsynced in groups (§7).
- *Group commit happens above openraft.* Openraft hands storage one entry
  per client write, so proposals are batched into `Command::Batch` by a
  proposer task; the log writer additionally groups whatever is queued.
  Durable add+update: 914 ops/s from one sequential writer (one ~1ms sync
  each), 28,320 ops/s from 64 concurrent writers.
- *`UpdateAwaitedAction` carries the caller's whole record*, validated in
  apply, so counters added on main (`worker_losses`, `escalations`,
  `dispatch_attempt`) and escalated `action_info` survive.
- *Client facts stay local, as §3 says, and that needed two rules the plan
  did not spell out:* a client with a live subscriber is attached and never
  evicted (retention counts from when it stops waiting); and a restarted
  replica gives every restored client a fresh keepalive, because the
  keepalives it held died with the process.
- *`ReplicaRestarted`* (P1 form of `ReplicaLost`): on startup the replica
  requeues everything executing, uncharged, since its worker streams died
  with the old process.
- *The per-operation `Heartbeat` command is still present* (from the
  spike); nothing in the scheduler proposes it. It goes in P2.

**Evidence:**

| Check | Result |
|---|---|
| Scheduler suite, memory backend | 255/255 |
| Scheduler suite, raft backend (`NATIVELINK_TEST_SCHEDULER_BACKEND=raft`) | 255/255 after the P2 matcher fix (was 251/252) |
| Worker / service / config / util suites | 582/582 |
| Restart durability: log only, snapshot + tail, torn final write | pass; each fails with log replay disabled |
| Randomized model test, 200 seeds × 300 commands, nested batches | invariants I1–I9 hold; found and fixed an orphaned operation (I3) and replica-dependent output order (I9) |
| rechaos `restart-chaos.sh raft` (10 × `kill -9` in 120s, seeded) | 0 findings, 0 lost operations, 61/61 oracle comparisons equivalent, 1263 stress operations completed; the memory backend under the same run completes 1281 and loses 110 acknowledged operations |
| rechaos `run.sh raft` (chaos monkey + stress) | no scheduler findings; CAS short-write findings identical to the memory backend |

**Found along the way, not raft-specific:**

- *Both backends evicted an operation while its client was still waiting*
  once the action outlived `retain_completed_for_s`
  (`client_retention_test`); fixed for both.
- *`client_action_timeout_s` must exceed twice the 10s keepalive*: a
  waiting client's keepalive goes out after more than 10s on a 10s sleep,
  so every 10–20s. The server only warns below 10.
- *After a worker-silence timeout the matcher can hand the action straight
  back to the silent worker* (both backends); the raft backend exposes it
  as the one failing scheduler test. The single-owner `WorkerLeave` of §3
  is the fix.
- *CAS: a short ByteStream write is stored under the full digest* (rechaos
  monkey, both backends); see the store-side fix PR #35.

## 0. Thesis, corrected once more

The premortem said: **the log is necessary, not sufficient**, apply must
validate transitions. That holds, and the hardened spike does it. This design
adds the second half:

> **Every fact has exactly one decider, and every effect is an obligation in
> the log that is discharged by another fact in the log.**

Most of the races in the open fix PRs come from one of two shapes:

1. **Two deciders for one fact.** Several schedulers each judge worker liveness
   with their own clock against a timestamp another node wrote (#39, #40, #42,
   #57, #2900). Two schedulers both try to assign the same action (version CAS,
   `LostRace`).
2. **An effect with no identity.** A kill, a dispatch or a result names only
   `(worker_id, operation_id)`. A requeued action keeps its `operation_id`, so
   a late message from attempt *n* is indistinguishable from attempt *n+1*
   (#43, #45/N2, #44/N1, late `ExecuteAccepted` → `kill_unknown_operation`).

A Raft log removes shape 1 by construction, if we are disciplined about which
node proposes which command. Shape 2 needs an **attempt identity on the
wire**, which no log can supply by itself.

## 1. Scope

**In:** `AwaitedActionDb` state (operations, dedup, the sorted queues), worker
sessions, client attachments, dispatch and kill obligations, timeouts.

**Out, deliberately:**

- **Blob storage (CAS/AC, `FastSlowStore`, …).** CAS blobs are named by their
  digest and never change, so duplicate writes converge. `in_flight_slow_writes`
  is a per-process optimization for `has()`, not a correctness mechanism.
  Putting blob traffic behind a quorum buys nothing and costs every upload a
  network round trip. The store-layer fix PRs (#35, #36, #49, #51, #53–56,
  #58–60, #65) are I/O correctness fixes and land on their own merits.
- **The Redis backend.** It stays as is. Raft is a third
  `ExperimentalSimpleSchedulerBackend` variant, not a replacement.

## 2. Topology

```
            clients (Execute / WaitExecution)        workers (ConnectWorker)
                 │            │                          │          │
           ┌─────▼────┐ ┌─────▼────┐               ┌─────▼────┐ ┌───▼──────┐
           │ replica 1│ │ replica 2│ ...           │ replica 2│ │ replica 3│
           │ (leader) │ │          │               │          │ │          │
           └────┬─────┘ └────┬─────┘               └────┬─────┘ └────┬─────┘
                └────────────┴──── raft log (gRPC) ─────┴────────────┘
```

- **N voters (1, 3 or 5), static membership from config.** One voter is a valid
  deployment: the memory backend, plus durability and a real effect protocol.
- **Clients and workers connect to any replica.** Each replica owns the gRPC
  streams it holds. Stream ownership is the one thing that cannot be
  replicated, and the design is built around that.
- **Every replica serves reads from its local applied state.** It may be
  behind, but it is monotonic per subscriber. The spike's revision-guarded
  `Watchers::publish` already enforces this.
- **Proposals go to the leader.** A follower forwards with
  `RaftService.Forward(Command) -> CommandResponse`. openraft returns
  `ForwardToLeader` with the leader's address.

## 3. Deciders: who proposes what

This table is the core of the design. Every command has exactly one kind of
proposer, and no apply step reads a clock or anything outside the command.

| Command | Proposer | Decided from |
|---|---|---|
| `AddAction` | replica holding the client stream | the request |
| `ClientAttach` / `ClientDetach` | replica holding the client stream | the stream opening or closing |
| `WorkerJoin` / `WorkerLeave` | replica holding the worker stream | stream opened; stream closed, `GoingAway`, or **that replica's own** keepalive timeout |
| `Offer` | replica holding the worker stream (it runs matching for its own workers) | local applied state plus local worker capacity |
| `Accepted` / `Declined` / `Finished` / `Progress` | replica holding the worker stream | the worker's message, carrying `attempt` |
| `RequestCancel` | any replica | client cancel, or last client detached |
| `ReplicaLost(node)` | leader | raft's own view of a voter: not reachable for longer than `replica_loss_timeout` |
| `Tick(now)` | leader | leader clock, made **monotonic in apply** (§6) |

Consequences:

- **No node ever compares its clock to a timestamp another node wrote.** Worker
  liveness is decided by the replica that receives the keepalives, against its
  own receive times, as today. But it is now the *only* decider, and its
  decision is a log fact (`WorkerLeave`). This removes the class of #39, #40,
  #42 and #57. The `Unknown` liveness and 1h `ORPHANED_ACTION_TIMEOUT` for
  workers on other schedulers go away too.
- **Keepalives never enter the log.** The spike's per-operation `Heartbeat`
  command is dropped: per-op × per-tick log traffic does not scale, and it is
  not needed once liveness has a single owner. Worker load (`free_memory_kb`)
  stays local to the owning replica, which is the only one that matches for
  that worker.
- **Matching is local and arbitrated by apply.** Each replica runs the matching
  loop *only for workers it holds* and proposes `Offer{op, expected_revision,
  worker, …}`. Two replicas can offer the same queued op; apply accepts the
  first and rejects the second with `RevisionConflict`. That is today's
  `LostRace`, but arbitrated in one place with no retry/backoff sleep. A
  rejected offer costs one log entry and nothing is sent to any worker.
- **A dead replica takes its streams with it.** `ReplicaLost(node)` makes every
  session and client attachment owned by `node` leave in one apply: owned ops
  are requeued as worker losses, and orphaned client-less ops follow §5.3.
  Workers reconnect to a surviving replica under a new `worker_id` (they already
  mint a new id per connection, `worker_api_server.rs:210`).

## 4. State machine

### 4.1 Operation stages

The client-visible REAPI stage stays `Queued / Executing / Completed`. Internally
the replicated state adds one stage and an attempt number:

```
              Offer(attempt=n+1)          Accepted(n+1)
  Queued ───────────────────────▶ Offered ─────────────▶ Executing
    ▲                               │  │                    │  │
    │   Declined(n+1) / offer        │  │                    │  │ Finished(n+1)
    │   deadline / session left      │  │                    │  ▼
    └────────────────────────────────┘  │                    │ Completed
    ▲                                   │ RequestCancel      │
    │  session left / exec deadline     ▼                    │ RequestCancel
    └─────────────────────────── Cancelling(n+1) ◀───────────┘
                                        │ Finished(n+1) / session left / kill deadline
                                        ▼
                                    Completed (cancelled)
```

- `Offered` is shown to clients as `Queued`: the action is not running until a
  worker says so. This also fixes today's lie, where an op shows `Executing` the
  moment `try_send` succeeds.
- **`attempt` is minted in apply** (`operation_attempt[op] += 1` on `Offer`) and
  is the identity for everything after it. Every later command for the op names
  the attempt, and apply rejects a mismatch with `StaleAttempt`. The `attempts`
  retry counter (charged on failure) and `worker_losses` (charged on session
  loss) stay as separate budgets, with `attempt` as a plain sequence number.
- `Cancelling` is a real stage. The cancel is committed, but the worker has not
  confirmed it is dead yet. Completion happens when the worker reports
  `Finished(n)` for that attempt, its session leaves, or the kill deadline
  passes, in which case the worker is evicted, as `unacknowledged_kill_timeout_s`
  does today.

### 4.2 Validation in apply (extends the hardened spike)

`Offer` is accepted only if:

- the revision matches, and
- the op is `Queued`, and
- the worker session is current, and
- **the worker's properties satisfy the op's placement requirements** (N3,
  #46). `WorkerJoin` therefore carries the worker's static properties.
- **the same session is not already executing this op** (N2, #45). With
  per-attempt identity, N2 becomes a plain apply check rather than a
  scheduler-side guard.

`Accepted(n)`, `Declined(n)` and `Finished(n)` are accepted only if `n` is the
current attempt *and* the proposing session owns it. A late `ExecuteAccepted`
from attempt `n-1` is rejected in apply, and the owning replica answers it with
a kill for `(op, n-1)` (§5.2), which replaces today's
`kill_unknown_operation` special case.

## 5. Effects

Committing a fact does not make a worker do anything. Effects are
**obligations** derived from applied state and executed by a per-replica
**dispatcher**:

```
pending effects on replica R =
    { StartExecute(op, n) | op Offered(n) to a session owned by R }
  ∪ { Kill(op, n)         | op Cancelling(n) on a session owned by R }
```

The dispatcher reconciles that set against what R has sent:

- **At least once.** On restart, or on reconnect of the session's stream, the
  set is recomputed from applied state and resent. Workers deduplicate by
  `(op, attempt)` (§5.1), so resending is harmless.
- **Discharged only by log facts.** `Accepted` / `Declined` / `Finished` /
  `WorkerLeave` / `ReplicaLost`, never by "the send succeeded". Today's split,
  where the send succeeds but the scheduler still doesn't know (channel full,
  `PendingActionInfoData.accepted:false`), becomes ordinary pending state.
- **Bounded by log-time deadlines.** `Offer` records
  `offer_deadline = log_now + dispatch_ack_timeout`. A `Tick` past it requeues
  the op uncharged (today's "unacknowledged" decline). `RequestCancel` records
  `kill_deadline = log_now + unacknowledged_kill_timeout`. A `Tick` past it
  makes the session leave (eviction). Both use log time, so every replica
  computes the same outcome.

The dispatcher sends nothing apply did not decide, and apply sends nothing at
all. That keeps apply pure (the spike's determinism claim) and makes the
sending side restartable.

### 5.1 Wire protocol changes (`worker_api.proto`)

Add `uint64 attempt` to:

| Message | Field |
|---|---|
| `StartExecute` | **9**. *Do not reuse 7*: it was `digest_function` (removed in #899). Both are varints, so an old binary would decode an attempt as a digest function. |
| `KillOperationRequest` | 2 |
| `ExecuteAccepted` | 2 |
| `ExecuteDeclined` | next free |
| `ExecuteResult` | next free |
| `ExecuteComplete` | next free |

Compatibility: `attempt = 0` means "legacy peer". A legacy worker never echoes
an attempt. The owning replica fills in the current attempt for its session when
it proposes, which is safe because a legacy worker's `worker_id` is unique per
connection and apply forbids a same-session redispatch (§4.2). A legacy
scheduler ignores the field.

### 5.2 Worker side

- **Dedup:** `running_actions` is keyed by `(op, attempt)`. A duplicate
  `StartExecute` for a live `(op, attempt)` is re-acked, not refused with
  `AlreadyExists`.
- **Kill tombstones (N1, #44):** a `Kill(op, n)` for an attempt that is not
  registered yet is recorded in a small tombstone set (bounded, TTL equal to
  the dispatch ack timeout). `create_and_add_action` checks it before inserting
  and aborts. Today the kill is logged and dropped
  (`running_actions_manager.rs:4903`).
- **Reconnect:** unchanged for now (`kill_all()`, new id). A later phase can let
  a worker re-announce `(op, attempt)` pairs on reconnect so long actions
  survive a replica failover. The protocol above already supports it: a
  `Resume(op, n)` from a new session is just another fact for apply to validate.

### 5.3 Client attachment

`ClientAttach(op, replica)` and `ClientDetach` replace the client keepalive
timestamp and its retry-on-version-conflict loop
(`store_awaited_action_db.rs:334–369`). An op with zero attachments for longer
than `client_action_timeout` (log time, checked on `Tick`) gets a
`RequestCancel`. This replaces both `sweep_abandoned_queued_actions` and the
cancel-as-a-side-effect-of-a-read in `apply_filter_predicate`
(`simple_scheduler_state_manager.rs:794–860`).

## 6. Time

- **Log time** = the `now` in the most recent applied `Tick`, `AddAction` or
  `Offer`, **clamped to never decrease**: `log_now = max(log_now, entry.now)`.
  A new leader with a slower clock cannot move time backwards (#2900 class).
  A new leader with a *faster* clock can jump time forward, so a deadline can
  fire early by up to the clock skew between leaders. That is acceptable for
  timeouts measured in tens of seconds. The skew is reported as a metric, and a
  leader whose clock is behind `log_now` logs it.
- The leader proposes `Tick` only while some deadline is pending (one entry per
  `tick_interval`, default 1s), so an idle cluster writes nothing.
- Deadlines live in a `BTreeMap<(u128 deadline, OperationId), Kind>` index, so
  `Tick` is O(expired), not the spike's O(all ops) scan in `Expire`.
- Action execution timeouts (`Action.timeout` + grace, `timeout_cause`) become
  an `exec_deadline` set at `Accepted`, enforced on `Tick`.

## 7. Durability and snapshots

- **Log storage:** append-only segment files in `data_dir`, with batched
  `fdatasync` per `append()` call. openraft batches entries, so one sync covers
  many commands. No embedded database is needed; if one is wanted later,
  `redb` fits `RaftLogStorage` cleanly.
- **Snapshots:** the spike's `PortableState` (map keys as pair sequences),
  encoded with `bincode` rather than JSON for size, and built every
  `snapshot_every` entries (default 50k).
- **Retention:** completed ops stay for `retain_completed` (for
  `WaitExecution` after completion), then a `Tick` purges them. Without this,
  state grows without bound.
- **Determinism checks:** every snapshot records `hash(PortableState)` at its
  log index. In tests, every replica compares hashes at the same index. In
  production, a mismatch is logged loudly.

### 7.1 Rough sizing

Per action: `AddAction`, `Offer`, `Accepted` and `Finished` make about 4 entries,
plus `ClientAttach`/`Detach`. At 1k actions/s that is about 5k entries/s, which
openraft batches into a few hundred appends/s. `AddAction` carries `ActionInfo`
(hundreds of bytes); `Finished` carries the `ActionResult` *digest*, not the
result. Steady-state log bandwidth is a few MB/s, well within one gRPC stream
per follower.

## 8. Configuration

```json5
schedulers: {
  main: {
    simple: {
      experimental_backend: {
        raft: {
          node_id: 1,
          data_dir: "/var/lib/nativelink/raft",
          peers: [
            { node_id: 1, address: "grpc://sched-0:50071" },
            { node_id: 2, address: "grpc://sched-1:50071" },
            { node_id: 3, address: "grpc://sched-2:50071" },
          ],
          // optional: tick_interval_ms, snapshot_every, retain_completed_s,
          // replica_loss_timeout_s
        },
      },
    },
  },
}
```

The only wiring point is `default_scheduler_factory.rs:162–232`: add an arm
that builds `RaftAwaitedActionDb`. `shares_state()` returns `true`, and
`exchange_fleet_capabilities` reads the fleet from the replicated sessions
instead of from TTL records.

## 9. What this deletes

| Today | Fate | Fix PRs it makes unnecessary |
|---|---|---|
| `AwaitedAction.version` CAS + `Aborted` retry/backoff | `expected_revision` checked in apply | #39, #42 |
| client keepalive retry sleep (100ms) | `ClientAttach` / `ClientDetach` | #39, #42 |
| `check_liveness` cross-node clock comparison, `Unknown` liveness, 1h orphan timeout | single-owner `WorkerLeave` | #40, #57, #2900 |
| `sweep_abandoned_queued_actions`, cancel-on-read | §5.3 | #37 |
| `try_subscribe` two searches + 20ms sleep | dedup in `AddAction` apply | #50 |
| `fleet_generation` | replicated sessions | — |
| same-id re-registration orphan healing | `WorkerJoin` supersession | #38 |
| `kill_revoked_operations` polling sweep (5s) | `Cancelling` obligation, sent immediately | #43 |
| `kill_unknown_operation` on late ack | `StaleAttempt` rejection → `Kill(op, n-1)` | — |
| N2 same-worker redispatch guard | apply check (§4.2) | #45 |

**Still needed, carried forward:** #44 (N1, now done as worker-side
tombstones), #46 (N3, moved into `Offer` validation), #41, #52 (local
scheduler-loop fixes), #47 (Redis backend only), and every store PR.

## 10. Phases

Each phase is shippable and keeps the Redis and memory backends untouched.

**P1 — Durable single voter, drop-in.**
Segment-file log, bincode snapshots, `Tick` with monotonic log time, the
deadline index, the factory arm. Run the **existing scheduler test suite**
parametrized over `memory | raft(1)`. *Exit:* suite green, plus a kill -9 /
restart test that keeps queued and completed ops.

**P2 — Effect protocol.**
`Offered` / `Cancelling` stages, `attempt` on the wire (fields above), the
dispatcher, worker dedup and tombstones, `ClientAttach`. Remove the per-op
`Heartbeat` command. *Exit:* regression tests for N1, N2, N3 and the late-ack
case named after their PRs, green on `raft(1)`, and the kill sweep deleted on
that backend.

**P3 — Multiple voters.**
tonic `RaftService` (AppendEntries, Vote, InstallSnapshot streaming, Forward),
`ReplicaLost`, and static membership. Test under deterministic simulation with
[`turmoil`](https://docs.rs/turmoil): partition the leader, kill a replica
holding workers, skew clocks across leaders, and assert determinism hashes
match plus no double execution of `(op, attempt)`. *Exit:* those properties
hold over 10k seeded runs.

**P4 — Delete.**
Once raft is the recommended backend, remove the §9 machinery from the code
paths shared with it.

## 11. Open questions

1. **Is HA the requirement, or just correctness?** P1–P2 already fix the races
   with one durable voter. P3 is only worth its operational cost (quorum,
   disks, a new failure mode) if scheduler failover is a goal. Decide before P3.
2. **Failover without killing long actions.** §5.2 leaves reconnect as
   `kill_all()`. Re-announcing `(op, attempt)` is cheap protocol-wise but needs
   worker work. Is it P3 or later?
3. **Client streams on a lost replica.** Clients reconnect via `WaitExecution`
   with the operation name. Confirm that every client we care about (Bazel,
   Buck2, reclient) does this rather than re-`Execute`, or rely on `AddAction`
   dedup for cacheable actions and accept a re-run for `do_not_cache` ones.
4. **Where matching runs.** Per-replica matching for owned workers (above) keeps
   dispatch local but lets replicas contend for the same queue head under load.
   The alternative is leader-only matching that forwards `StartExecute` to the
   owning replica: no contention, but an extra hop and the leader becomes the
   matching bottleneck. Measure rejected `Offer`s in P3 before choosing.
5. **Migration from Redis.** No state migration is planned. Switching backends
   means draining: stop admitting, let queued work finish, switch. Is that
   acceptable for current users?
