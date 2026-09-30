# CROSSED OFF

> "It's not your fault."

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

## 5. The dispatch-generation / fleet-generation counter (#2838)

- `src/api_worker_scheduler.rs:162` — `fleet_generation: u64`.
- `src/api_worker_scheduler.rs:397` — `self.fleet_generation += 1` on every
  fleet mutation, so a matching pass can detect it raced a worker set change.

**Why the log makes it unrepresentable:** the generation counter is an epoch tag
so the matching engine can tell "the worker set changed under me, redo the
pass". That is optimistic concurrency over the worker set, the same pattern as
item 1 one level up. Fold worker-session membership into the same replicated log
(a `WorkerJoin`/`WorkerLeave` command alongside the action commands, the natural
Stage-3+ extension) and matching reads a consistent snapshot of a state machine
that only advances by committed entries — there is no "changed under me" to
detect, so no epoch to bump.

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

## 7. Same-id `add_worker` orphan healing

- `src/api_worker_scheduler.rs:327` — `fn add_worker`.
- `src/api_worker_scheduler.rs:332` — `let replaced = self.workers.put(worker_id.clone(), worker)`
  silently evicts a worker already registered under the same id, whose in-flight
  actions (`running_action_infos`) are now orphaned and must be healed elsewhere.

**Why the log makes it unrepresentable:** a same-id re-registration orphans the
old session's actions because worker identity and action ownership are tracked
in separate, independently-mutated maps that can disagree. When the worker
session is itself a log fact (a `WorkerJoin` superseding a prior session id in
the replicated state), the supersession and the reassignment of that session's
actions are decided in one apply from one entry — the ownership map can never be
left pointing at a session the membership map has already replaced, because both
are the same deterministic function of the same log.

---

### What is actually crossed off

A version counter, a 100ms retry sleep, a per-node `.elapsed()` liveness guess,
a periodic abandoned-action sweep, an epoch counter, a 20ms re-search, and an
orphan-healing path — seven mitigations for six distinct races. The log does not
make them *safer*. It makes the states they were catching **unreachable**.
