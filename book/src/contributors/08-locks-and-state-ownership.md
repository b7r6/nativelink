# Locks, and Who Owns the State

The previous chapter ends with a family of bug — a lock acquisition that stops being polled — whose fix is not a patch but a policy. This chapter is the policy question, asked against the code as it stands: how much shared mutable state does NativeLink actually have, guarded by how many different kinds of lock, in what order, and who decided?

The answers, in short: 69 async lock acquisitions across nine files of production code, seven lock flavours from four crates, no written lock order, and no lint. None of that is carelessness. It is what a careful codebase looks like when locking grows one reasonable decision at a time over several years, and nobody has yet had cause to write the rule down. The cause arrived with [#3001](https://github.com/TraceMachina/nativelink/pull/3001).

## The census

Every count below is reproducible against the tree this book ships with:

```bash
# acquisitions per crate
git grep -ohE '\.(lock|read|write)\(\)\s*\.await' -- 'nativelink-scheduler/src/**/*.rs' | wc -l
# which files, and how many each
git grep -cE '\.(lock|read|write)\(\)\s*\.await' -- '*/src/**/*.rs'
```

| Crate | Acquisitions | Production | In mocks under `src/` | Files |
| ----- | -----------: | ---------: | --------------------: | ----: |
| `nativelink-scheduler` | 45 | 39 | 6 (`mock_scheduler.rs`) | 5 |
| `nativelink-store` | 38 | 21 | 17 (`gcs_client/mocks.rs`) | 5 |
| `nativelink-worker` | 9 | 9 | 0 | 1 |
| `nativelink-util` | 0 | 0 | 0 | 0 |
| `nativelink-service` | 0 | 0 | 0 | 0 |

Two readings of that table matter more than the totals.

**The service layer and the worker's execution path hold no async locks at all.** `nativelink-service` is zero across every file: the gRPC handlers are stateless per request, and they are the easiest code in the repository to reason about for exactly that reason. `nativelink-worker`'s nine are all in `directory_cache.rs` — the execution path proper (`running_actions_manager.rs`) carries none. The concurrency risk in NativeLink is not spread thin; it is concentrated in the scheduler and in a handful of stores.

**The hot spots, by file:**

| File | Acquisitions | Flavour |
| ---- | -----------: | ------- |
| `nativelink-scheduler/src/api_worker_scheduler.rs` | 29 | `async_lock::Mutex` |
| `nativelink-worker/src/directory_cache.rs` | 9 | `tokio::sync::{Mutex, RwLock}` |
| `nativelink-store/src/redis_store.rs` | 8 | `tokio::sync::{Mutex, RwLock}`, `parking_lot`, `async_lock::Mutex` |
| `nativelink-store/src/filesystem_store.rs` | 6 | `async_lock::{Mutex, RwLock}`, `parking_lot::Mutex`, `std::sync::Mutex` |
| `nativelink-store/src/ontap_s3_existence_cache_store.rs` | 6 | `tokio::sync::RwLock` |
| `nativelink-scheduler/src/worker_registry.rs` | 5 | `async_lock::RwLock` |
| `nativelink-scheduler/src/memory_awaited_action_db.rs` | 4 | `async_lock::Mutex` |
| `nativelink-scheduler/src/simple_scheduler_state_manager.rs` | 1 | `async_lock::Mutex` |

One file holds 29 of the scheduler's 39 production acquisitions. `api_worker_scheduler.rs` is 3,025 lines guarding one `Mutex<ApiWorkerSchedulerImpl>` (`:2045`) that every worker-API connection, every matcher pass, and the liveness sweep all contend for. If you are going to read one file in this repository with concurrency in mind, read that one.

## Seven flavours, and what each one costs

```
async_lock::Mutex      async_lock::RwLock
tokio::sync::Mutex     tokio::sync::RwLock     tokio::sync::OwnedMutexGuard
parking_lot::Mutex     parking_lot::RwLock
std::sync::Mutex
```

Four crates' worth of locking primitives coexist, sometimes within one file — `redis_store.rs` and `filesystem_store.rs` each use three. The choice is not cosmetic:

- **`async_lock::Mutex`** is fair, and its fairness is implemented by a starvation mode: a waiter that has waited more than 500 µs makes every other acquirer's fast path fail until it is served (`async-lock-3.4.1/src/mutex.rs:581`). That is the right behaviour for throughput under contention and the wrong behaviour when a waiter is parked, which is the whole of Family 7. **Both of the scheduler's two central locks are this flavour** — `api_worker_scheduler.rs:25` and `memory_awaited_action_db.rs:21` — so both are exposed.
- **`tokio::sync::Mutex`** assigns permits FIFO from a semaphore, with the same consequence for a parked waiter by a different mechanism.
- **`parking_lot::Mutex`** cannot be held across an `.await` at all: the guard is not `Send`, so the compiler refuses. That constraint is a feature. A lock you cannot hold across an await is a lock you cannot park mid-acquisition, which means Family 7 cannot happen to it. For state whose critical section is pure in-memory work, this is the correct default and the compiler enforces it for free.
- **`std::sync::Mutex`** behaves likewise but without `parking_lot`'s poisoning-free API and faster uncontended path.

There is a rule implied by that list which nobody has written down: **in-memory state should use a blocking mutex; only state whose critical section genuinely must await should use an async one.** The scheduler's state DB is a `BTreeMap` and a `HashMap` behind an `async_lock::Mutex`, and the work under the lock is pure computation. It does not need to be an async lock. Making it `parking_lot::Mutex` would have made #3001 a compile error.

## The order nobody wrote down

The matcher pass crosses both scheduler locks on one task, in a fixed order, on every action it considers:

1. `simple_scheduler.rs:917` drives the listing stream, which locks the state DB inside the stream (`memory_awaited_action_db.rs:1025`);
2. for each row, `find_worker_for_action_observed` locks the worker registry (`api_worker_scheduler.rs:2045`);
3. the dispatch then locks the state DB again to record the assignment (`memory_awaited_action_db.rs:1010`).

That order is correct, consistently followed, and documented nowhere. `git grep -niE 'lock order|lock ordering|lock hierarchy'` across the whole repository returns exactly one hit — a local comment in `directory_cache.rs:480` about two mutexes for one digest. There is no module doc, no `ARCHITECTURE.md` section, and no lint:

```bash
$ git grep -n 'await_holding_lock' -- '*.rs' '*.toml'
$ # (no output)
```

`clippy::await_holding_lock` is not enabled in `clippy.toml` or in the workspace lints at `Cargo.toml:143`, which is defensible — the codebase deliberately holds async guards across awaits in about 35 places — but it means the one mechanical check that speaks to this class is off, and nothing replaces it.

How many sites would it flag? 53 of the 69 production acquisitions bind the guard to a name rather than using it as a temporary, so the guard lives to the end of its scope; a hand audit of those puts the number that then cross a later `.await` at roughly 35. Thirteen are in `api_worker_scheduler.rs`, where the registry mutex is held across calls that reach into the state manager.

## The next instance, already in the tree

Chapter 6 shows that the in-memory DB is one combinator away from #3001. The Redis-backed DB is closer than that. `StoreAwaitedActionDb`'s listing is a cursor stream that holds a connection guard taken from `redis_store.rs:233`'s `tokio::sync::RwLock<(C, Uuid)>`; ordinary writes take the same lock for read; reconnect takes it for write (`reconnect_lock`, `:241`). Same shape as #3001 — a lock living inside a stream handed across a trait boundary — with a different lock flavour and a network round trip inside the critical section.

A fleet backed by the in-memory DB cannot test this. A Redis-backed deployment can, and should, before anyone adds prefetch to a listing pass again.

## Level 1: the discipline that removes the class

Three levels of fix exist for what this chapter describes. Levels 2 and 3 are structural and belong in a design document — see [Appendix H](../appendix/scheduler-ownership.md). Level 1 is a few days' work, stays compatible with upstream churn, and a contributor can do it today:

1. **Make the state DB's lock blocking.** Its critical sections are in-memory map operations. `parking_lot::Mutex` makes "held across an await" a compile error rather than a review question.
2. **Turn the lint on where it can be on.** `#![deny(clippy::await_holding_lock)]` per module, starting with the modules that can satisfy it, rather than workspace-wide against 35 existing sites.
3. **Make "a stream owns no lock" a rule with a home.** It belongs in the `AwaitedActionDb` trait's doc comment, where the next person to implement or wrap a listing will read it — not in a commit message.
4. **Collapse to one async-lock crate.** Seven flavours from four crates is six more than the number of fairness policies anyone is reasoning about.
5. **Write the lock order down.** Three locks, one order, one module doc. It took one paragraph above; it has never been in the repository.

What level 1 does not fix: `api_worker_scheduler`'s 29 acquisitions of a mutex held across calls that reach the state manager. That is the next wedge, and no amount of discipline makes a 3,000-line critical section safe. Which is the argument for level 2, and the subject of the appendix.

Before the appendix, though, read [What the Scheduler Stores](./09-what-the-scheduler-stores.md). Every lock in this chapter guards something, and that chapter says what — which of it is written down, which of it exists only in one process's memory, and which structures hold the same fact twice and are kept equal by hand. The argument for a single owner is much easier to have once the inventory is on the table.
