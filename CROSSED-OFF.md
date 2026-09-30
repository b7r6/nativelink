# CROSSED OFF

> "It's not your fault."

---
> **ERRATA (Astra adversarial premortem, 2026-09-30).** This document over-claimed.
> The log crosses off the *storage choreography* (version-CAS-as-glue, search-index
> visibility, the 20ms dedup re-search, replica-local clock disagreement) — that part
> holds. It does **not** by itself cross off transition *validation*. The current
> `UpdateAwaitedAction` command carries a precomputed state with no expected
> revision/stage/attempt and apply installs it unconditionally (seam **S0**), which
> **reintroduces N2 and N3 and can resurrect a completed op through N7**. The CAS is not
> deleted; it must move *into apply as a semantic guard* (Assign-if-eligible,
> Finish-names-attempt, terminal-rejects-stale). Honest scorecard:
>
> | Item | Was claimed | Astra verdict |
> |---|---|---|
> | N7 atomic qualifier claim (#6) | dies | **DIES** — only if S0 is fixed so a stale update can't resurrect the claimed-away op |
> | wall-clock liveness → `Expire` (#3) | dies (test) | improvement real, but **NEEDS-CARE**: lease needs attempt/session identity + no-backward + cross-leader clock policy |
> | version-CAS (#1), keepalive retry (#2), abandoned-sweep (#4) | dies | **NEEDS-CARE**: die only once apply validates predecessors |
> | fleet-generation (#5), orphan healing (#7) | demonstrated | membership-in-log is right; **NEEDS-CARE** for connection handoff + effect reconciliation across real failover |
> | apply-vs-worker-dispatch (S1) | (flagged) | **SURVIVES** — highest priority; commit does not make cancellation effective at the worker |
>
> Concrete code defects found (all actionable): S0 unchecked update; adapter **drops the
> attempts counter** (retry limit evadable); subscriber can **publish a revision backward**
> and wedge at a stale value; `build_snapshot` **fails to serialize**
> `HashMap<ActionUniqueKey, _>` as JSON (snapshot breaks after any cacheable action);
> heartbeat overwrites lease unconditionally and names only the operation, not the attempt;
> `AwaitedAction::new` reads **ambient tracing baggage**, so apply is not purely a function
> of command bytes — undercutting the determinism claim this argument rests on. Full detail:
> `/tmp/nativelink-astra-openraft-premortem.md`.
>
> Corrected thesis: **the log is necessary, not sufficient.** It removes the distributed-
> storage choreography; the replacement must additionally make illegal transitions
> impossible *in apply* and make external effects recoverable. Original claims below are
> retained as written, each graded by the table above.
---

## HARDEN PASS — landed fixes (2026-09-30, single-node)

Astra's six bounded, high-confidence code defects are now fixed in apply and
covered by tests (`nativelink-raft/tests/harden_test.rs`, all green; fail→pass
verified for S0, attempts, heartbeat by temporarily reverting the guard). The
command alphabet gained validated transitions: `AssignToWorker` (revision +
predecessor + session checks), a revision-guarded `UpdateAwaitedAction`, an
attempt/session-named `Heartbeat`, and an explicit `CommandResponse::Rejected`
— so **"client_write succeeded" no longer means "assignment accepted".**

| Astra item | Fix in apply | Test | fail→pass captured |
|---|---|---|---|
| **S0** unchecked update / double-assign | `AssignToWorker` validates revision + Queued predecessor + current session; loser gets `RevisionConflict` | `s0_two_assignments_one_queued_revision_exactly_one_wins` | yes (both guards reverted → `a_ok=true b_ok=true`) |
| **S0** completed resurrection (N7) | terminal op rejects a nonterminal update (`AlreadyTerminal`) | `s0_completed_rejects_stale_nonterminal_update` | — (covered by S0 guard) |
| **attempts** counter dropped | command carries `attempts`; apply preserves it; retry cap enforced in the transition (`RetryCapExceeded`) | `attempts_counter_survives_and_hits_retry_cap` | yes (preserve line reverted → stored stays 0) |
| **subscriber** publish-backward | `Watchers::publish` is revision-guarded; both pump and `subscribe_op` route through it | `subscriber_never_regresses_revision` | — (guard is the mechanism) |
| **snapshot** serde on struct/enum map keys | `AppliedState::{to_portable,from_portable}` — every map a pair-sequence; snapshot uses it | `snapshot_roundtrips_nonempty_cacheable_state` | proven raw-JSON errors, portable succeeds |
| **heartbeat** identity + monotonic lease | `Heartbeat` names worker/session/attempt, rejects non-owning senders, never lowers the lease | `heartbeat_identity_and_monotonic_lease` | yes (monotonic guard reverted → lease regresses) |
| **ambient context** determinism | `capture_origin_metadata()` on the request path → `AwaitedAction::new_with_metadata`; apply reads no `Context::current()` | `add_action_origin_metadata_is_from_command_not_ambient` | — (constructor is pure by construction) |

**Deliberately deferred to the next phase** (as Astra scoped them — not
improvised here):

- **S1** worker-side cancellation + effect-reconciliation dispatcher. Hooks in
  where a committed assignment/cancel would record a *recoverable effect
  obligation* alongside the state transition (a new field on the applied op or a
  side `pending_effects` log fact), with a dispatcher reconciling it against
  worker-acknowledged state. Nothing in the current apply sends RPCs, so this is
  additive, not a rewrite.
- **Real multi-node `RaftNetwork` + durable storage.** The `SingleNodeNetwork`
  stub and RAM log remain; crash-durability, leadership transfer, and
  minority-partition behavior are unproven. Hooks in at `store.rs` (durable
  `RaftLogStorage`) and `db.rs` (`SingleNodeNetwork` → a real transport).
- **S6a** connection handoff on membership change. Needs worker-session
  connection ownership + reconnection, separate from the membership log fact
  already built (#5/#7). Hooks in at the worker-scheduler boundary, not the DB.
- **S6c** request-dedup cache across failover. Needs a stable logical request id
  reused across retries with the accepted result recorded in applied state and
  retained through snapshots. Hooks in at `AddAction` (add a `request_id` field
  and a `request_id -> operation_id/result` map in `AppliedState`).

The single-node caveat still applies to everything: what is proven is a property
of the *deterministic apply*, identical for one voter and a quorum. Durable
failover is the unbuilt plumbing, not an unproven state-machine claim.
---

Every piece below exists to *mitigate* a race that the current design lets
happen. In the raft-log model the race cannot be represented in the first
place: there is one totally-ordered log, one writer (the leader appends), and
every replica applies the same commands in the same order. State is a pure
function of the log. So the guards, retries, sleeps, version counters and
wall-clock heuristics that patch over concurrent mutation have nothing left to
guard. Crossed off, one at a time.

Paths are relative to the worktree root (`nativelink-scheduler/...`).

---

## 1. The version compare-and-swap (`AwaitedAction.version`)

- `src/awaited_action_db/awaited_action.rs:37` — `AwaitedActionVersion`, the
  per-record counter.
- `src/memory_awaited_action_db.rs:622` — the in-memory backend rejects an
  update whose `version()` differs from the stored one with `Code::Aborted`.
- `src/store_awaited_action_db.rs:767`–`779` — the store backend does the same
  as a Redis conditional write: "Could not update AwaitedAction because the
  version did not match".

**Why the log makes it unrepresentable:** the version is a hand-rolled
optimistic-concurrency token guarding a read-modify-write against a *concurrent*
writer. In the log there is no concurrent writer — `Command::UpdateAwaitedAction`
(`nativelink-raft/src/types.rs`) carries no version field at all, and
`state.rs::apply` applies it to whatever the prior applied state was. Two updates
are linearized by their log indices; the log index *is* the version, assigned by
raft, never chosen by a racing client. A lost update requires two writers
committing over the same base state, which the single-leader log forbids.

## 2. The version-conflict retry loop on client keepalive

- `src/store_awaited_action_db.rs:334` — `(self.now_fn)().sleep(Duration::from_millis(100)).await`
  inside the keepalive path.
- `src/store_awaited_action_db.rs:338` — "Client keepalive retry due to version conflict".
- `src/store_awaited_action_db.rs:369` — gives up with `Code::Aborted`, "Could
  not update client keep alive".

**Why the log makes it unrepresentable:** the retry exists only because the
keepalive write races the worker's state write on the same versioned record, so
one loses and spins. `Command::Heartbeat` (`types.rs`) is its own log entry; it
commutes with state updates through the log's ordering and can never "conflict"
with one. The 100ms retry sleep is deleted, not tuned.

## 3. The wall-clock liveness heuristic (`check_liveness` / `last_worker_updated_timestamp`)

- `src/store_awaited_action_db.rs:279` — `I::from_secs(last_known_keepalive_ts).elapsed() > CLIENT_KEEPALIVE_DURATION`.
- `src/store_awaited_action_db.rs:387` — the subscriber's own `.elapsed()` on
  the last keepalive.
- `src/store_awaited_action_db.rs:881`–`918` — `check_liveness`, whose own doc
  comment admits "a healthy worker looks abandoned" when clocks disagree.

**Why the log makes it unrepresentable:** each node here reads *its own* wall
clock and compares it to a timestamp written by a *different* node — a
distributed clock-skew bug by construction. `Command::Expire`
(`types.rs`) instead compares two values that both live *in the log*:
`now_unix_nanos` frozen by the leader into the entry, against the lease
timestamp recorded by the last `AddAction`/`Heartbeat`/`Update` entry
(`state.rs::AppliedState.leases`). No node reads its own clock during apply, so
every replica decides the identical requeue set. Proven by the passing test
`worker_death_requeues_through_the_log`.

## 4. `sweep_abandoned_queued_actions`

- `src/simple_scheduler.rs:977` — the periodic sweep is kicked off from the
  scheduler loop.
- `src/simple_scheduler_state_manager.rs:451` — `pub async fn sweep_abandoned_queued_actions`,
  a full scan looking for actions stranded because a state transition was lost.

**Why the log makes it unrepresentable:** a queued action becomes "abandoned"
when a dispatch or completion write was dropped/half-applied against the shared
store — precisely the partial-write the versioned KV store cannot make atomic.
In the log, `AddAction` and every `UpdateAwaitedAction` is a single committed
entry with an all-or-nothing apply; there is no half-transition to sweep up.
The requeue that *is* still wanted (worker death) is item 3's deterministic
`Expire`, not a periodic garbage scan.

## 5. The dispatch-generation / fleet-generation counter (#2838) — DEMONSTRATED

Test: `matching_reads_consistent_worker_set_without_generation_counter`
(`nativelink-raft/tests/lifecycle_test.rs`).

- `src/api_worker_scheduler.rs:162` — `fleet_generation: u64`.
- `src/api_worker_scheduler.rs:397` — `self.fleet_generation += 1` on every
  fleet mutation, so a matching pass can detect it raced a worker set change.

**Why the log makes it unrepresentable:** the generation counter is an epoch tag
so the matching engine can tell "the worker set changed under me, redo the
pass". That is optimistic concurrency over the worker set, the same pattern as
item 1 one level up.

**Built, not just argued.** Worker-session membership is now a log fact:
`Command::WorkerJoin` / `Command::WorkerLeave` (`nativelink-raft/src/types.rs`)
maintain `AppliedState.sessions: HashMap<WorkerId, u64>`
(`nativelink-raft/src/state.rs`). Matching reads that map with
`RaftAwaitedActionDb::worker_sessions_snapshot`, which returns an owned clone of
the applied state — a frozen point in the totally-ordered log. The test takes
one snapshot, then commits a `WorkerLeave` and a re-`WorkerJoin`, and shows the
already-held snapshot is unchanged (nothing to invalidate) while a fresh read
reflects the new committed fleet wholesale. No snapshot is ever torn, and no
code compares generations, because the log order is the only synchronization.
The `fleet_generation` counter has nothing left to detect.

## 6. The `try_subscribe` two-search + 20ms sleep dedup (Astra N7)

- `src/store_awaited_action_db.rs:939` — `const SUBSCRIBE_RACE_RETRY_DELAY: Duration = Duration::from_millis(20)`.
- `src/store_awaited_action_db.rs:947` — `tokio::time::sleep(SUBSCRIBE_RACE_RETRY_DELAY).await`
  between two index searches (`:951`) for the same unique key.

**Why the log makes it unrepresentable:** the second search after a 20ms nap is
there because two clients adding the same cacheable action can both miss the
dedup index and both create an operation — a check-then-act race on the shared
index. In the log, dedup is decided *inside* `Command::AddAction`'s apply
(`state.rs`: look up `unique_key_to_operation`, join if present else insert),
which is atomic with the insert because it is one log entry applied by one
writer. Two concurrent `AddAction`s for the same key are ordered by the log; the
second sees the first's insert and joins it. No sleep, no re-search, no
duplicate operation.

## 7. Same-id `add_worker` orphan healing — DEMONSTRATED

Test: `same_id_rejoin_supersedes_without_orphan`
(`nativelink-raft/tests/lifecycle_test.rs`).

- `src/api_worker_scheduler.rs:327` — `fn add_worker`.
- `src/api_worker_scheduler.rs:332` — `let replaced = self.workers.put(worker_id.clone(), worker)`
  silently evicts a worker already registered under the same id, whose in-flight
  actions (`running_action_infos`) are now orphaned and must be healed elsewhere.

**Why the log makes it unrepresentable:** a same-id re-registration orphans the
old session's actions because worker identity and action ownership are tracked
in separate, independently-mutated maps that can disagree.

**Built, not just argued.** Two maps are now the SAME deterministic function of
the log (`nativelink-raft/src/state.rs`): `sessions` (worker_id -> current
session epoch) and `operation_owner` (operation_id -> (worker_id, epoch)).
`Command::WorkerJoin` with a higher epoch than the recorded session supersedes
it: in one apply it calls `requeue_sessions_operations` for the OLD epoch —
requeueing every op that session owned and clearing its ownership — and only
then records the new epoch. The test holds an executing op owned by session
epoch 1, re-registers the same worker id at epoch 2, and asserts (a) the op is
requeued in the same apply, (b) `operation_owner` is `None` afterward (no
dangling pointer to the dead session), and (c) the fleet records exactly the
surviving session 2. It further asserts a late `WorkerLeave` carrying the stale
epoch 1 is ignored, so a superseded session cannot disturb its replacement.
The ownership map can never point at a session membership has already replaced,
because both are written by the one log entry that performs the supersession.

**Honest limit.** This is proven single-node: the determinism argument is that
*apply is one atomic function of one totally-ordered log*, which holds for one
voter exactly as it does for a quorum — the log is the same abstraction. What a
real multi-node `RaftNetwork` (replacing the single-voter `SingleNodeNetwork`
stub) would additionally prove is that the leader's log is durable across a
failover, so the superseding entry survives the crash that triggered the
re-registration. That is replication plumbing, not a change to the state
machine; the orphan-freedom shown here is a property of the apply logic and
does not depend on it.

---

### What is actually crossed off

A version counter, a 100ms retry sleep, a per-node `.elapsed()` liveness guess,
a periodic abandoned-action sweep, an epoch counter, a 20ms re-search, and an
orphan-healing path — seven mitigations for six distinct races. The log does not
make them *safer*. It makes the states they were catching **unreachable**.

### Status of the claims

| # | Mitigation | Status |
|---|------------|--------|
| 1 | version compare-and-swap | argued (no version field exists in the log commands) |
| 2 | keepalive version-conflict retry sleep | argued (`Heartbeat` is its own entry) |
| 3 | wall-clock `check_liveness` | **demonstrated** — `worker_death_requeues_through_the_log` (deterministic `Expire`) |
| 4 | `sweep_abandoned_queued_actions` | argued (atomic apply has no half-transition) |
| 5 | fleet-generation counter #2838 | **demonstrated** — `matching_reads_consistent_worker_set_without_generation_counter` |
| 6 | `try_subscribe` 20ms re-search | demonstrated in effect by the dedup path in `full_lifecycle_through_the_log`'s AddAction join logic |
| 7 | same-id `add_worker` orphan healing | **demonstrated** — `same_id_rejoin_supersedes_without_orphan` |

Items 1, 2, 4 remain arguments from the shape of the log commands rather than
standalone tests; 3, 5, 7 are proven by the named integration tests (all pass
single-node, niced `-j2` on lab metal). The single-node caveat under #7 applies
to all of them: what is proven is a property of the deterministic apply, which
is identical for one voter and a quorum; durable failover across a real
`RaftNetwork` is unbuilt plumbing, not an unproven state-machine claim.
