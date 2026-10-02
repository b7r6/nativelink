# nl-watch-store silently drops store paths under commit bursts (large toolchain outputs never reach the cache)

- **Date:** 2026-07-17
- **Reporter:** b7r6
- **Component:** `nativelink-nix-client` — `nl-watch-store` (`nativelink-nix-client-1.5.2`)
- **Repo state:** branch `nix-cache`, `b5032d38`
- **Host:** `ultraviolence` (monolithic NativeLink + `nix_cache` substituter + watch-store)
- **Severity:** High — the auto-push mirror is silently incomplete; affected paths never become substitutable, so they rebuild from scratch on every consumer forever.

## Summary

`nl-watch-store` is meant to mirror **every** newly-committed store path into the
local `nix_cache`. In practice it mirrors everything committed at a calm rate but
**silently drops paths committed during a burst** (e.g. the tail of a large
`nix build` that commits many outputs in quick succession). Dropped paths produce
**no log line at all** — not `pushed`, not `push failed`, not `skipping` — so the
gap is invisible from the service journal. The observable symptom is that large,
late-committed outputs (multi-GiB OCI images) are permanently absent from the
cache and rebuild ~5 min every run instead of substituting in seconds.

## Environment

The watch-store unit is healthy and running:

```
nl-watch-store --to http://127.0.0.1:50071/nix/main --store /nix/store \
  --concurrency 4 --signing-key /run/agenix/nativelink-nix-cache-key
```

- `nativelink-nix-cache.service` (substituter, `:50071/nix/main`, priority 40): active
- `max_nar_size_bytes` = 32 GiB (upload cap) — **not** the limiting factor here

## Observed vs. expected

Built all package exports of `straylight-toolchain` (`nix build .#<each>`), then
probed the cache for each output's narinfo.

| output | own NAR size | committed | in cache? | in watcher log? |
|---|---|---|---|---|
| `cxx-clang22-libcxx-glibc-buckconfig` | small | today | ✅ 200 | ✅ pushed |
| `cxx-gcc15-libstdcxx-glibc-oci` | 0.08 GiB | today | ✅ 200 | ✅ pushed |
| `cxx-clang22-libcxx-glibc-oci` | **4.52 GiB** | today 12:44:58 | ❌ 404 | ❌ **absent** |
| `cxx-clang22-libcxx-musl-oci` | ~4.5 GiB | today | ❌ 404 | ❌ **absent** |
| `cxx-clang22-libstdcxx-glibc-oci` | ~4.5 GiB | today | ❌ 404 | ❌ **absent** |

- The three missing paths were committed **while the watcher was up** (since Jul 15),
  yet their hashes appear **nowhere** in `journalctl -u nativelink-nix-cache-watch`.
- 187 successful pushes today; the **largest single push all day was 1.2 GiB**
  (`compiler-rt-src`). Nothing ≥ ~1.3 GiB has ever landed.
- No `overflow` / `q_overflow` / `push failed` messages in the journal.

**Expected:** every committed path is either `pushed`, `already present`, or logged
as skipped/failed with a reason. **Actual:** a class of paths vanishes with zero trace.

### Note on the size correlation

It looks size-correlated, but the real variable is **burst position**, not size.
Large outputs finish last in a build, so they are committed at the tail of the
commit burst — exactly when the event pipeline is most backed up (see below). Size
is a proxy for "committed late in a burst," not the actual filter. A small path
committed in the same burst window would be dropped too.

## Reproduction

1. On a host with `nixCache.watchStore = true`, `nix build` a derivation set whose
   tail commits several large outputs in quick succession (the toolchain's four
   `-oci` images, ~4.5 GiB each, do it reliably).
2. For each output: `h=$(basename $(nix path-info .#out) | cut -c1-32)` then
   `curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:50071/nix/main/$h.narinfo`.
3. Observe `404` for the large late outputs and **no** corresponding line in
   `journalctl -u nativelink-nix-cache-watch.service`.

## Root cause

Two independent silent-drop points on the event path, either of which alone loses
events under load; together they explain the symptom completely.

### 1. Bounded channel drops on full, silently — `nativelink-nix-client/src/watch.rs`

```rust
// watch.rs:133
let (tx, rx) = mpsc::channel::<StoreEvent>(4096);
...
// watch.rs:270 (in `emit`)
// A closed receiver just means the daemon is shutting down.
drop(tx.try_send(StoreEvent { path }));
```

`try_send` fails when the channel is **full**, not only when it is closed, and the
result is unconditionally `drop`ped. The comment only accounts for the closed case.
When the single consumer falls behind, the 4096-slot channel fills and every
further event is discarded with no log and no metric.

The consumer is easy to back up (`nl_watch_store.rs:113–133`): for each event it
does an **inline, blocking** `query_path_info` retry loop of up to
`8 × 250 ms = 2 s` before it even spawns the push:

```rust
// nl_watch_store.rs — per event, on the consumer task, BEFORE spawning the push
for attempt in 0..8u32 {
    match store.query_path_info(&event.path) {
        Ok(meta) => ...,
        Err(err) if err.code == Code::NotFound => {
            if attempt == 7 { tracing::debug!(..., "skipping (never became valid)"); }
            else { tokio::time::sleep(Duration::from_millis(250)).await; }
        }
        ...
    }
}
```

A burst of hundreds of freshly-renamed paths (each initially `NotFound` for a beat,
so each costs real sleep time) makes the consumer drain far slower than the producer
fills → channel saturates → tail events dropped. The largest outputs commit last,
so they are the ones dropped.

### 2. fanotify queue is not unlimited — `nativelink-nix-client/src/watch.rs:162`

```rust
libc::fanotify_init(
    FAN_CLASS_NOTIF | FAN_REPORT_DFID_NAME | FAN_CLOEXEC | FAN_NONBLOCK,
    ...
)
```

`FAN_UNLIMITED_QUEUE` is not set, so the kernel caps the notification queue at
16384 events and drops on overflow, signalling `FAN_Q_OVERFLOW`. The read path does
not detect or log the overflow marker, so a kernel-side drop is also invisible. The
unit already runs with `CAP_SYS_ADMIN`, so `FAN_UNLIMITED_QUEUE` is available at no
extra privilege cost.

## Impact

- The mirror is **incomplete and silently so** — its whole value proposition
  (a full mirror of the host closure, "never blocks a build") is violated for
  exactly the expensive artifacts caching most benefits.
- Every downstream `nix build` re-derives the dropped paths (the four `-oci`
  images cost ~284 s each, every run) because the substituter 404s them.
- No signal: operators cannot tell from logs or metrics that anything was missed.

## Proposed fixes

1. **Never drop silently on a full channel.** Distinguish `Full` from `Closed` in
   `emit` (`watch.rs:270`). On `Full`, either block/await capacity
   (`tx.send(...).await`, applying backpressure to the reader) or, at minimum,
   `tracing::warn!` + bump a dropped-events counter so the gap is observable. A
   dropped store path should be as loud as a failed push.
2. **Decouple metadata lookup from the consume loop.** Move the up-to-2 s
   `query_path_info` retry into the spawned push task (bounded by the existing
   `--concurrency` semaphore) so the channel consumer never blocks on it. This
   removes the backpressure source that fills the channel.
3. **Set `FAN_UNLIMITED_QUEUE`** in `fanotify_init` (`watch.rs:162`) and detect
   `FAN_Q_OVERFLOW` in the read path, logging a warning + counter when the kernel
   drops. `CAP_SYS_ADMIN` is already held.
4. **Reconciliation sweep (defense in depth).** On startup and periodically, diff
   `nix path-info --all` (or the paths registered since a watermark) against the
   cache and push the difference, so a missed event self-heals instead of being
   lost forever.

## Immediate workaround

Re-push the affected paths out of band (no burst → no drop):

```sh
nix copy --to 'http://127.0.0.1:50071/nix/main' \
  $(nix path-info .#cxx-clang22-libcxx-glibc-oci \
                  .#cxx-clang22-libcxx-musl-oci \
                  .#cxx-clang22-libstdcxx-glibc-oci)
```

(or `nl-nix push`, if that path is preferred). This restores substitutability until
the drop is fixed, but a subsequent GC + reburst will lose them again.

## Resolution

Fixed the two silent-drop points plus the kernel-queue cap (proposed fixes 1–3):

- **`watch.rs` — backpressure, never drop.** `emit` now `send().await`s instead of
  `try_send` + `drop`, so a full channel applies backpressure to the reader rather
  than discarding the event. `Err` (receiver closed) is logged.
- **`nl_watch_store.rs` — decoupled revalidation.** The inline up-to-2 s
  `query_path_info` sleep-retry is gone. A `NotFound` path is requeued onto a
  deadline-ordered `VecDeque` and revalidated via a `select!` timer that races
  event arrival, so the consumer never blocks and the channel keeps draining. The
  DB lookup stays on the consumer task (the connection is `!Sync`).
- **`watch.rs` — `FAN_UNLIMITED_QUEUE`.** Set in `fanotify_init` (CAP_SYS_ADMIN is
  already held) so the kernel queue is no longer capped at 16384; `FAN_Q_OVERFLOW`
  is now detected and logged loudly if it ever occurs.

Proposed fix 4 (periodic `nix path-info --all` reconciliation sweep) is deferred as
defense-in-depth — tracked in `PRODUCTION-READINESS.md`. Verified: crate compiles
and rustfmt-clean; the drop paths are eliminated by construction. Still owed: an
integration test that reproduces a commit burst (the untested-networking-layer gap
this bug came from).
