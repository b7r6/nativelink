# The Hard Parts: Concurrency, Cancellation, and Consistency

NativeLink is a distributed system that spends most of its time pretending to be a library. You call `store.update(...).await` and it feels like a function. It is not a function. Behind that `.await` is a worker that may be partitioned, a leader that may change, a process that may outlive the decision to kill it, a wall clock that two machines disagree about, and a cache whose "yes, I have that" was true a microsecond ago and is a lie now. **Almost every correctness bug in this codebase lives in that gap between the function-call feeling and the distributed-system reality.**

This chapter is the field guide to that gap. It is organized by *failure family*, because the bugs are not random — they cluster into a small number of shapes, and once you can see the shape you stop writing the bug. Every pattern here is drawn from a real defect with a real fix; the exhaustive index is the stability-pass bug catalog (`nativelink-bug-catalog.md`) and PRs #2831–#2853. If you internalize one chapter of this book before touching the scheduler, the worker, or any store, make it this one.

The unifying lesson, stated once so you can forget everything else: **an operation that spans an `.await`, a clock, a shared record, or a physical effect is not atomic, not current, not exclusive, and not done — unless you did specific work to make it so.**

## The async model, briefly

NativeLink is Tokio all the way down. Two facts about that model generate most of what follows:

1. **A future can be dropped at any `.await` point.** When a future is dropped mid-flight — a `tokio::time::timeout` elapses, a client disconnects, a `select!` branch loses — execution *stops* at the last suspended `.await`. Nothing after it runs. Code that acquired something before that await and planned to release it after does not get to release it, unless the release is in a `Drop`.
2. **Between any two `.await` points, the world moves.** Another task ran. The map you read is stale. The worker you picked disconnected. The record you loaded was bumped. An `.await` is a yield to a hostile universe.

The test harness leans on a *current-thread* runtime and a `MockClock` precisely so these interleavings become deterministic and reproducible (see `nativelink-macro`'s `nativelink_test`, and the tooling section below). In production they are not deterministic; they are adversarial.

---

## Family 1 — Cancellation and drop-safety

**The shape:** you acquire something (a lock count, a pause, a reservation, a slot), you `.await`, you plan to release it. A drop between the acquire and the release strands it forever.

**The canonical bug.** `ExistenceCacheStore` pauses its removal callbacks while an upload is in flight, so an eviction fired mid-upload doesn't delete the entry you're about to insert. The pause was a plain increment/decrement around an `.await` on the inner store. The worker wraps output uploads in a `tokio::time::timeout` that *drops the upload future* on elapse (this is `W1` in the catalog, and it is normal production behavior). Drop the future between increment and decrement, and `resume` never runs: the pause count is stuck ≥1 forever, every future invalidation is queued and never drained, and the cache reports evicted blobs as present until the process restarts. Refcounting made it *worse* — nothing could bring the count back to zero (catalog C20/N6, PR #2842).

The fix is an RAII guard whose `Drop` releases the count. But there is a second, sharper lesson here, because the *first* fix was still wrong: the guard's `Drop` decremented the count but **discarded the pending-removal vector that the release returns**, so in the evict-*before*-cancel ordering the queued invalidations were dropped on the floor anyway. A green regression test hid it because the test evicted *after* the drop, so nothing was ever queued at drop time. An adversarial source read caught it; the real fix makes `Drop` actually *drain* the queued work (asynchronously, via `background_spawn!`, because `Drop` can't `.await`).

**The rules:**
- If you `.await` while holding a resource, the release must live in a `Drop`, not in the line after the await.
- A `Drop` that "releases" must do the *whole* release — including any deferred work the release produces. Decrementing a counter is not the same as finishing the cleanup the counter was gating.
- `Drop` cannot `.await`. If cleanup is async, spawn it (`background_spawn!`) and make the observable state converge promptly; document that it converges *after* `drop()` returns, not within it.
- Write the regression in the ordering that actually strands the resource (acquire → queue work → **drop**), not the ordering that happens to pass.

---

## Family 2 — Multi-step invariants across an `.await` (TOCTOU)

**The shape:** you check a condition, you `.await`, you act on the condition. Between the check and the act, the condition changed. Classic time-of-check-to-time-of-use, but the "time" is an await point, so it slips past review.

**Canonical bugs.**
- **Kill before registration (N1, PR #2849).** The worker acknowledges a dispatch, then *asynchronously* fetches the action, waits for cleanup, and makes the input directory — several `.await`s — before inserting the operation into its `running_actions` map. A `KillOperationRequest` that arrives in that window looks the operation up, finds nothing, logs an error, and drops the kill. The action then registers and runs, unkilled. The check ("is it in the map?") and the use ("so there's nothing to kill") straddle the entire async startup.
- **Eviction under the heartbeat snapshot (keepalive TOCTOU, PR #2835).** The sweep selects an apparently-expired worker, releases the lock, a heartbeat arrives, and the sweep evicts anyway using its stale observation.

**The rules:**
- An invariant that spans an `.await` is not an invariant. Either re-validate the premise *after* the await (re-read the record, re-check liveness under the acting lock), or hold a guard/lock across the whole check-act so the premise can't move.
- If an event (a kill, a cancellation) can arrive before the thing it targets exists, leave a **tombstone** the later registration consults — don't rely on "it's not here, so there's nothing to do." (N1's fix is exactly a `pending_kills` set checked under the same lock that guards the insert.)

---

## Family 3 — The wall clock is not a fact

**The shape:** a decision keyed on "enough time has passed" or "this timestamp identifies this thing." Clocks drift between machines, advance in jumps, and have finite resolution. Any of those turns the decision into a race.

**Canonical bugs.**
- **Second-resolution identity collision (R1 / PR #2838).** The scheduler identified a dispatch by `dispatched_at = duration_since(UNIX_EPOCH).as_secs()`. Two dispatches within the same second share an identity, so a stale revocation's `Kill` targets the *replacement* dispatch. The fix replaces the timestamp identity with a monotonic, scheduler-wide **dispatch generation** counter allocated under the lock.
- **Failed write advances the clock (PR #2837).** A client-keepalive write *fails*, but the subscriber advances its local "last success" timestamp anyway, faking liveness for a write that never landed.
- **Liveness from a local clock vs a remote timestamp.** The store-backed awaited-action DB judged worker liveness by comparing *its own* `.elapsed()` against a timestamp written by a *different* node — a distributed clock-skew bug by construction. Its own doc comment admitted "a healthy worker looks abandoned."

**The rules:**
- A timestamp is not an identity. Identify things with monotonic counters, UUIDs, or epochs — never with "when it happened," especially not at coarse resolution.
- Advance a success marker only *after* the success is observed, never on the attempt.
- "Enough time has passed" is a failure *suspicion*, not a fact. It belongs to a failure detector with explicit assumptions, and the verdict should be committed as an event before anyone acts on it — not read ad hoc from `SystemTime::now()` inside business logic. (The architecture RFC develops this at length; the practical rule for everyday code is: keep wall-clock reads out of decision logic, or make the value an explicit, recorded input.)

---

## Family 4 — Concurrent mutation / lost update

**The shape:** two writers read a shared record, each computes an update from what it read, each writes. The second clobbers the first. Both "succeeded."

**Canonical bugs.**
- **Keepalive clobber (PR #2834).** A worker's full-record write restores an older client-keepalive timestamp that a concurrent subscription had advanced. Fixed with a max-merge of the timestamp — the accumulated evidence is preserved regardless of write order.
- **Same-worker redispatch double-charge (N2, PR #2850).** A worker still holds operation X's reservation when X times out and is requeued; spare capacity lets X redispatch to the same worker; `run_action` unconditionally overwrites the operation-keyed reservation entry and deducts a second time, but completion refunds only the surviving entry. Permanent capacity leak. Fixed by rejecting a redispatch of an operation the worker already holds, before any deduction.
- **Stale placement after escalation (N3, PR #2851).** A matcher parks a 4 GiB placement snapshot; a peer escalates the action to 8 GiB and requeues; the stale snapshot dispatches on a 4 GiB worker. The assignment CAS succeeds — the record *is* currently Queued — but it never checked that the requirements the worker was *chosen for* still match. The fix validates the placement-relevant requirements at assignment, not just that the record is assignable.

**The rules:**
- A read-modify-write on a shared record needs optimistic-concurrency control (a version/revision checked at write) or a lock scoped to the conflict.
- Validation must check the *premise of the decision*, not merely that *a* write can land. "The record is still Queued" is not "the record still has the requirements I planned around." (This is the single subtlest point in the scheduler: a passing CAS can still be a wrong decision — see the architecture RFC's "S0" discussion.)
- When order shouldn't matter, make the merge order-independent (max/union) instead of last-writer-wins.

---

## Family 5 — Logical decision vs physical effect

**The shape:** you commit a decision in the ledger and assume the physical world obeyed. It didn't, or not yet.

A committed `Kill` does not stop a running Unix process; a logical revocation is not physical quiescence; "the scheduler reassigned it" does not mean the old worker stopped computing. The old attempt can still be writing outputs, holding resources, and — if you're not careful — getting its results accepted against the *new* attempt.

**The rules:**
- Distinguish "the ledger says stopped" from "the process is confirmed dead." Only the latter justifies reclaiming a physical exclusion, and confirming it needs a worker-side quiescence signal, not a timeout.
- **Fence results by identity, not by hope.** Every attempt carries an epoch/session; accept a result only if it matches the *current* attempt. A stale attempt that finishes late is fine — its result is rejected by the fence — as long as you never key acceptance on the operation alone.
- For most of NativeLink's workload the saving grace is the CAS: duplicate execution of a *cacheable, hermetic* action writes the same content-addressed blobs, so overlap is wasted CPU, not corruption. That is a property of the workload, not a license to ignore fencing on the result-selection and resource-accounting paths. (The whole Lane B architecture in the RFC is about exactly where this freebie does and does not apply.)

---

## Family 6 — Store and cache coherence

**The shape:** cache invalidation is a distributed-ordering problem even inside a single process, because reads, writes, evictions, and subscriptions interleave.

**Canonical bugs.**
- **Overlapping upload pauses / read resurrection (N4/N5, folded into PR #2842).** One upload's completion drains another's queued invalidations; an in-flight read re-inserts a positive entry after the blob was evicted. Both leave a false "present."
- **Subscription lost on reconnect (PR #2839).** A Redis addition subscribes on the old connection before registering the pattern in the tracked set; reconnect snapshots the stale set and the new connection silently misses the pattern.
- **Torn / mixed reads (PR #2841; residual C10/W4).** A chunked read spans multiple round-trips with no snapshot; a key replaced mid-read (even by an equal-length value) returns a blob stitched from two values as `Ok`. Length checks don't catch equal-length replacement — that residual is RFC-track, pinned by a reproduction test.
- **Batch eviction mass-delete (PR #2840).** A `has()` over N keys cascaded into evicting all N resident blobs because the eviction batch decided against a cached, never-decremented count.

**The rules:**
- Publication and invalidation must be ordered *per key*. "Pause all invalidation, do work, resume all" is a global sledgehammer that creates the overlap bugs above; prefer per-key generations.
- A read that returns bytes is not a license to assert residency — the bytes may be from a value that was evicted or replaced after the read began.
- Multi-round-trip reads against a mutable backend are not atomic. If the value can change under you, you need snapshot-consistency (atomic read) or value-versioning — a length check is not identity.
- Content-addressed keys (the CAS) are immune to the *replacement* class, because the key *is* the hash; mutable-value paths (the AC, scheduler rows, arbitrary keys) are not.

## Family 7 — A lock acquisition that stops being polled

**The shape:** a future starts acquiring an async lock, something keeps that future alive without polling it, and the lock's bookkeeping is now wrong forever. No deadlock cycle, no lock held too long, no `.await` under a guard. The lock is *free*, every task is waiting for it, and nothing moves again.

This family is different from the six above in a way worth stating plainly. Those are bugs about state you can see: a counter, a record, a timestamp, an entry in a map. This one is a bug about the *liveness contract of a future*, and the contract is nowhere in the type system. Rust will not warn you. Clippy's `await_holding_lock` does not fire, because nobody holds a lock across an await. The code reads correctly line by line, and the only way to see it is to know the invariant:

> **Every future that has started acquiring an async lock must keep being polled until it holds the lock.**

An async mutex is not a flag you test. Acquiring is a multi-step protocol: register as a waiter in an intrusive list, return `Pending`, get woken on release, then take the lock. A future that completes step one and never reaches the last step has made a *reservation* nobody will ever collect. Whether that merely wastes a wakeup or wedges the system for good depends on the lock's fairness policy — and the fair policies are the dangerous ones.

### The canonical bug: a listing, a prefetch, and a mutex

Upstream `main` after `4edf8224ea` ([#3001](https://github.com/TraceMachina/nativelink/pull/3001), "Read the listed records of a filter pass 64 at a time") stops scheduling within about fifteen seconds of ordinary remote-execution load — four runs out of four. CAS, AC and ByteStream in the same process keep serving normally. Only the scheduler is dead. The fix is [#3013](https://github.com/TraceMachina/nativelink/pull/3013); the mechanism is four layers, each defensible on its own:

1. **A lock hidden inside a stream.** `MemoryAwaitedActionDb::get_range_of_actions()` returns a `ChunkedStream` whose chunk-fetch closure does `self.inner.lock().await` on the DB's `async_lock::Mutex`. "Iterating the listing" secretly means "acquiring the scheduler's state lock, at some later poll, from inside whatever combinator happens to wrap me."
2. **A combinator that keeps a half-acquired future alive without polling it.** #3001 added `.map(read_listed).buffered(64)`. `Buffered` polls upstream to fill its buffer; the chunk fetch returned `Pending` because the lock was busy; `Buffered` handed a ready row downstream and only polled upstream again when asked for another row. The lock-acquire future is now parked in the buffer — registered as a waiter, polled at the consumer's convenience.
3. **The consumer takes the same lock on the same task.** The matcher dispatches the row it was just handed, which runs `inner_update_operation` → `get_by_operation_id` → `inner.lock().await` (`memory_awaited_action_db.rs:1010`). One task, two outstanding acquisitions of one mutex, and only one of them can ever be polled from here on.
4. **Fairness turns a lost wakeup into a permanent reservation.** `async-lock` hands a release to the oldest listener, and once a waiter has waited longer than 500 µs it enters "starved" mode: every other acquirer's fast path is refused until the starved waiter takes the lock (`async-lock-3.4.1/src/mutex.rs:581`, and the "Somebody is starved" branches at `:551` and `:569`). The starved waiter is the parked one. It will never be polled, so it will never take the lock, so nobody else may have it.

Step 4 is why this is a wedge and not a hiccup. A lock with barging semantics would have let the next acquirer through and the symptom would have been a rare stall. The fairness that exists to prevent starvation is what converts a dropped wakeup into a deadlock with no cycle in it.

Note what the fix was *not*. The first attempt swapped `buffered` for `chunks` + `join_all` and broke two of upstream's matcher tests (`room_opening_mid_pass…` and `lost_parked_assignment…`), because the matcher genuinely depends on rows being handed on as they are read. Streaming, prefetch, and a lock inside the stream cannot all three hold. The lock is the one to remove: #3013 reads the listing whole under one hold, *in the `async fn`, before the stream exists*, so the stream owns no lock. The chunk fetch already read the entire range in one call, so the snapshot is exactly what the stream used to yield.

### This tree is one combinator away

Do not read the above as a bug in some other version. Layer 1 is in the code you have:

```rust
// nativelink-scheduler/src/memory_awaited_action_db.rs:1021
std::future::ready(Ok(ChunkedStream::new(
    start,
    end,
    move |start, end, mut output| async move {
        let inner = self.inner.lock().await;   // ← the lock lives in the stream
```

Both listings do it — `get_all_awaited_actions` at `:990` and `get_range_of_actions` at `:1025`. What keeps this tree alive is only layer 2's absence: the consumer in `simple_scheduler_state_manager.rs:1620` wraps the stream in `filter_map` and `try_filter_map`, which are lazy and one-at-a-time, and the matcher drives it with a plain `while let Some(…) = stream.next().await` (`simple_scheduler.rs:917`). Nothing buffers, so nothing parks a half-acquired lock.

That is a property of the current consumer, not of the DB's interface — and the DB's interface is what makes it a trap, because the consumer cannot see the lock. Any future change that adds prefetch, concurrency, or buffering to a listing pass re-creates #3001 exactly. `buffered`, `buffer_unordered`, `chunks`, `ready_chunks`, a `FuturesUnordered` held across an await, `Peekable::peek` — any of them. The guard rail is to fix the producer: a stream handed out across a trait boundary must own no lock.

### The ways this happens

- **Buffering and prefetch combinators**: `buffered`, `buffer_unordered`, `chunks`, `ready_chunks`, `FuturesUnordered`/`FuturesOrdered` kept across an await, `Peekable::peek`.
- **`select!` arms that lose.** The branch that was not chosen keeps its future alive, mid-acquisition, until the next loop iteration polls it again — or forever, if the loop takes the same lock and blocks.
- **A lock hidden behind a trait method that returns a stream or a future.** The caller has no way to know, which is exactly how #3001 got reviewed and merged.
- **One task acquiring one lock through two paths.** Usually a helper that locks, called from inside a scope that already holds or is acquiring.
- **Fair lock implementations generally.** `async_lock`'s starvation mode is one; tokio's semaphore assigns permits FIFO to waiters that may be parked, which has the same failure mode.

A work-stealing runtime makes all of this worse without causing any of it. NativeLink builds one `new_multi_thread()` runtime with no per-core confinement: that supplies the contention which trips the fairness path, migrates the matcher's hot state between cores on every yield, and makes the interleaving unreproducible in a test.

### Recognising one in the field

- **Scheduler-only death.** CAS, AC and ByteStream keep answering; `Execute` streams hang. Bazel sits at a constant `N running` or `0 running` with a frozen action counter.
- **"Timed out with no update from its worker" for actions that finished.** Their results are already in the AC, and the worker's own log shows `upload_results … completed`. The scheduler stopped consuming updates, not the worker.
- **At debug level** (`nativelink_scheduler=debug`): `inner_update_operation START` from two different tasks with no matching `SUCCESS`, and the state DB silent after its last `Handling action:` line.
- **The process ignores `SIGTERM`** once wedged. A shell script that sends `TERM` and waits will hang; systemd escalates, `kill` does not.

### Why no unit test reproduces it

On a current-thread runtime the lock is never contended at the instant the prefetch polls it, so the bug cannot exist. On a multi-threaded runtime a stress test is confounded by the executor: non-yielding writers freeze the timer, yielding writers never contend hard enough. The regression test that ships with #3013 therefore pins the *semantics the fix relies on* — a row added after the listing was requested is not in it — and not the race. The reproduction is a fleet running a real build, which is a statement about this family in general: a liveness bug in the lock protocol is not reachable from the unit-test harness, and finding it is a soak-and-bisect job.

**The rules:**
- A stream or future handed across a trait boundary must own no lock. Take what you need under one hold in the `async fn`, before the stream exists, and let the stream yield from a snapshot.
- Never park a future that is acquiring a lock. If you buffer, prefetch, or `select!` over futures, know for certain that none of them locks.
- One task must not have two outstanding acquisitions of one lock. If a helper locks, the caller cannot be mid-acquisition of the same lock.
- For state that is purely in-memory, prefer `parking_lot::Mutex` and forbid awaits under it. A lock that cannot be held across an await cannot be parked mid-acquisition, which deletes this entire family for that lock.
- When a wedge has no lock-order cycle and no guard held across an await, stop looking for the cycle and start looking for a parked acquisition.

---

## The tools that catch these

**The deterministic test harness.** `nativelink_test` (in `nativelink-macro`) puts each test on a *current-thread* runtime, and `nativelink-util`'s `instant_wrapper.rs` swaps the clock for `mock_instant`'s `MockClock`, which a test advances in lockstep. Together those make an interleaving reproducible: the scheduler's interleaving tests (`nativelink-scheduler/tests/simple_scheduler_test.rs` and its neighbours) arrange an exact order of events and assert the outcome. This is the tool that catches families 1 through 6, and the reason to reach for it first.

Two honest limits. It is per-test hand-arranged rather than schedule-generated: each test encodes one interleaving its author thought of, so the harness finds regressions in known races and does not search for unknown ones. And a current-thread runtime cannot reach family 7 at all — the lock is never contended at the instant a prefetch polls it. A deterministic simulation harness that decodes arbitrary event schedules from fuzz input, and lifts invariants like RESULT-ONCE and NO-GHOST-ASSIGNMENT into properties checked across all of them, is the natural next tool; the fork has prototyped one (PR #2831), and the only fuzz target in this tree is `cas_config` (`nativelink-test/fuzz/fuzz_targets/`). Treat scheduler fuzzing as something to build, not something to run.

**Fails-without-the-fix discipline.** A regression test that passes before your change is not a regression test for your change. Prove it: revert the fix (or neuter the guard), watch the test fail, restore, watch it pass. And — the C20 lesson — make sure it fails in the *ordering that actually triggers the bug*, not a cousin ordering. A green test that exercises the wrong interleaving is worse than no test, because it buys false confidence.

**The clippy aspect and the sanitizers.** CI runs clippy at deny-level and asan/tsan builds that `cargo test` does not. They catch a different class — but they will not catch a logic race. Only a test in the right interleaving, or a harness that searches interleavings for you, will.

---

## The smell test

Before you send a diff that touches the scheduler, the worker, or any store, read it once asking:

1. **Does this `.await` between acquiring and releasing something?** → the release must be drop-safe, and the `Drop` must do the *whole* release.
2. **Does this check a condition, `.await`, then act on it?** → re-validate after the await, or hold a guard across it; leave a tombstone if the target may not exist yet.
3. **Does a decision read a wall clock or use a timestamp as an identity?** → use a monotonic counter / epoch; advance success markers only after success.
4. **Does this read-modify-write a shared record?** → version/CAS it, and validate the *premise*, not just that a write lands.
5. **Does this assume a committed decision had a physical effect?** → fence by identity; don't assume a timeout stopped the work.
6. **Does this publish to or invalidate a cache?** → order per key; don't trust a read as proof of residency.
7. **Does this hand out a stream or future that locks inside, or buffer one that might?** → a stream across a trait boundary owns no lock; nothing that is acquiring a lock may be parked unpolled.

Seven questions. The first six would have caught every bug in the stability pass; the seventh is #3001, which none of them would have caught. The codebase is not hostile — it is a genuinely careful system — but it operates in a hostile regime, and the care has to go exactly here.
