# Review — fix for nl-watch-store burst drops

Companion to [`2026-07-17-nl-watch-store-silently-drops-paths-under-burst.md`](./2026-07-17-nl-watch-store-silently-drops-paths-under-burst.md).

- **Reviewer:** b7r6
- **Date:** 2026-07-17
- **Fix under review:** `2e2b7ea8` — *nl-watch-store: stop silently dropping store paths under commit bursts*
- **Verdict:** Correct — addresses all three root causes. Compiles clean, rustfmt-clean. Two follow-ups owed before "done" (a burst regression test; a clippy decision). Neither blocks correctness.

## What I verified

- `cargo check -p nativelink-nix-client --bin nl-watch-store` → `Finished`, no errors.
- `cargo fmt -p nativelink-nix-client -- --check` → clean.
- Manual read of the `select!` requeue logic and the `FAN_Q_OVERFLOW` branch placement.

## The three changes map onto proposed fixes 1–3

### 1. `watch.rs` `emit()` — backpressure, not silent drop ✅

`try_send(...)` + `drop` → `send().await`. This is the core fix: `try_send` fails on a
**full** channel (not only a closed one), which silently discarded the event. `send().await`
now waits for capacity; `Err` means the receiver is closed (shutdown) and is logged. This
alone removes the drop that made burst-tail paths vanish.

### 2. `nl_watch_store.rs` — decoupled, non-blocking revalidation ✅

The inline `8 × 250 ms` sleep-retry that throttled the single consumer is gone. A `NotFound`
path is requeued onto a deadline-ordered `VecDeque<PendingValidation>` and revalidated via a
`select!` timer (`sleep_until`) that races event arrival, so the channel keeps draining while
paths wait to become valid. The DB lookup correctly stays on the consumer task (the connection
is `!Sync`, so it can't move into the spawned push). The 2 s total window
(`MAX_VALIDATE_ATTEMPTS × VALIDATE_RETRY`) preserves the original semantics.

### 3. `watch.rs` `fanotify_init` — `FAN_UNLIMITED_QUEUE` + overflow detection ✅

Flag added (`CAP_SYS_ADMIN` already held), and `parse_fanotify` now detects `FAN_Q_OVERFLOW`
and logs loudly. Branch placement is correct: the synthetic overflow event passes the header
sanity guard (valid `vers`, `event_len == METADATA_SIZE`), is caught before the name-parse, and
`buf` advances by `event_len` before `continue`.

## Findings

### A. Clippy is not clean — pre-existing fork debt, not caused by the fix

The workspace sets `std-instead-of-core = "deny"` (`Cargo.toml:196`), and `cargo clippy -p
nativelink-nix-client` fails with ~20 violations. **None are on lines the fix introduced** —
they are pre-existing (`watch.rs:29,30,274,298,331,372` = imports / `str::from_utf8` /
`read_unaligned`; plus `client.rs`, `nar.rs`, `store.rs`, `metrics.rs`, `lib.rs`). The
fork-added `nativelink-nix-client` crate has never been clippy-clean against its own workspace
gate; it passes `nix build` because that runs `cargo build`, not clippy.

The fix **does** add three more same-kind usages, consistent with the file's existing
`use std::…` style:

- `nl_watch_store.rs:19` — `use std::collections::VecDeque`
- `nl_watch_store.rs:21` — `use std::time::Duration`
- `nl_watch_store.rs:133` — `std::future::pending()`

So the fix changes clippy's failure *count*, not its *kind*. If the project intends to enforce
`std-instead-of-core`, the crate needs a crate-wide `core`/`alloc` pass — a separate cleanup,
not this patch's responsibility. If it does not, the gate is knowingly not run on the fork
crates and this is moot. **A decision is owed either way.**

### B. Residual (benign) coupling — consumer can still stall on the push permit

The consumer still calls `semaphore.acquire_owned().await` inline before spawning each push.
Under sustained push saturation (all `--concurrency` permits busy) the consumer blocks there,
the channel fills, and backpressure now propagates to the **kernel** fanotify queue — which,
with `FAN_UNLIMITED_QUEUE`, grows in memory rather than dropping. This is strictly better than
the old behavior (no drops), but it is not *fully* decoupled: an extreme, sustained burst
trades a bounded drop for unbounded kernel-queue / memory growth. Acceptable as-is; documented
so it is a known tradeoff, not a surprise.

### C. Deque-ordering micro-wrinkle — no correctness impact

Requeued items get `ready_at = now + VALIDATE_RETRY` and are `push_back`ed. If the spread of
pending deadlines ever exceeded `VALIDATE_RETRY` (250 ms), the deque could go slightly out of
deadline order and revalidate a path a little late — but never drop it. Within a real commit
burst the spread is sub-millisecond, so it stays ordered in practice. Not worth complicating
with a `BinaryHeap` unless profiling says otherwise.

### D. Still owed — a burst regression test

This bug lived in an untested `fanotify → channel → push` path, and nothing in the fix
reproduces a commit burst, so the regression can silently return. The resolution defers the
reconciliation sweep (proposed fix 4) to `PRODUCTION-READINESS.md` — the right backstop — but a
burst-repro integration test is the actual guard against *this* class of failure. Recommended
shape: commit N paths (several large, committed last) faster than the consumer drains, then
assert every one is present in the target cache.

## Bottom line

Functionally sound; the silent-drop paths are eliminated by construction. Two follow-ups before
calling it done: (1) a burst regression test (Finding D), and (2) a clippy decision for the
fork crate (Finding A). Findings B and C are documented tradeoffs, not defects.
