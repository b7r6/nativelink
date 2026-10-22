# The Nix Cache Client

The [Nix Substituter Facade](./nix-substituter.md) is the server: a `nix_cache` service that speaks the Nix HTTP binary-cache protocol out of store composition. Stock `nix copy --to` and ordinary substitution already talk to it with nothing but a `substituters` entry. This chapter is about the *client* the fork ships alongside it — the `nativelink-nix-client` crate and its two executables — for the jobs stock `nix` does not do as well:

- **`nl-nix`** — a `nix copy`-style client that pushes and pulls store paths over the same wire protocol, streaming zstd end to end and emitting the same OTLP metrics the server does.
- **`nl-watch-store`** — a daemon that watches the local store with `fanotify` and auto-pushes every newly-committed path to a cache the instant it lands.

Both are co-designed with the facade: they speak its exact routes, share its `nativelink-nix` protocol primitives (nixbase32, narinfo rendering, the signing fingerprint, canonical NAR names), and report through the same `nativelink-util` telemetry so a collector's ClickHouse or Prometheus exporter sees client throughput next to server throughput with no extra wiring.

One design decision runs through both: **pure Rust, no `nix` subprocess.** The client serializes a store path to a NAR itself — byte-identical to `nix-store --dump`, verified against real store paths — and reads the local store's `db.sqlite` directly for the reference graph and path metadata. It never shells out to `nix` and needs no running `nix-daemon` for reads, so it drops cleanly into a container, a CI step, or a systemd unit.

## Two Executables

| Binary | Role | Privilege |
|---|---|---|
| `nl-nix` | On-demand `push` / `pull` / `reconcile` / `info` against a cache URL | none |
| `nl-watch-store` | Long-running daemon: auto-push on store commit | `CAP_SYS_ADMIN` for `fanotify` (or `--inotify`) |

Both take `--to <URL>` (or `NL_NIX_CACHE`) and `--token <TOKEN>` (or `NL_NIX_TOKEN`), read the store named by `--store` (default `/nix/store`), and sign pushed narinfo with any number of `--signing-key <FILE>` Ed25519 secret keys.

## The Base URL Includes the Mount Prefix

The facade mounts its HTTP router at a URL prefix, not the listener root — `/nix/<instance_name>` by default (see [the facade's "Deploying Alongside Remote Execution"](./nix-substituter.md#deploying-alongside-remote-execution)). So a `nix_cache` instance named `main` on port `50071` serves `/nix/main/nix-cache-info`, and the client's `--to` must include that prefix exactly as `nix copy --to` and a `substituters` entry do:

```bash
# Correct: the base URL carries the /nix/<instance> mount prefix.
nl-nix --to http://cache.example.com:50071/nix/main info
# StoreDir: /nix/store
# WantMassQuery: 1
# Priority: 40

# Wrong: pointing at the listener root gets a clean 404.
nl-nix --to http://cache.example.com:50071 info
# Error: GET http://cache.example.com:50071/nix-cache-info returned 404 Not Found
```

Every request the client makes — the `/nix-cache-info` probe, the `GET`/`PUT` on `{hash}.narinfo`, the `GET`/`PUT` on `nar/<name>` — is relative to that base, so a missing prefix fails uniformly rather than subtly.

## `nl-nix`: push, pull, info, reconcile

Global options precede the subcommand (`nl-nix [OPTIONS] --to <URL> <push|pull|info|reconcile>`):

| Option | Default | Meaning |
|---|---|---|
| `--to <URL>` | — (required, or `NL_NIX_CACHE`) | Cache base URL, mount prefix included |
| `--store <DIR>` | `/nix/store` | Local store directory to read |
| `--token <TOKEN>` | — (or `NL_NIX_TOKEN`) | Bearer token; sent as Basic *password* with an empty user for netrc parity |
| `--signing-key <FILE>` | — (repeatable) | `nix key generate-secret` Ed25519 secret key; one `Sig` line per key |
| `--compression-level <N>` | `3` | zstd level for uploads (matches the server default) |
| `--no-compress` | off | Upload the raw NAR instead of zstd |
| `--jobs <N>` | `8` | Maximum concurrent path transfers |

### push

`nl-nix push <paths…> [--recursive]` uploads each path in one streaming pass co-designed with the facade's ingest:

1. **Validated dedup probe.** A `GET {hash}.narinfo` skips a path only when the record parses and passes the same structural checks as admission — reported as `skipped (present)`. A malformed historical record is treated as absent and overwritten.
2. **Stream once, hash twice.** The path is serialized to a NAR and the bytes are teed through SHA-256 for the `NarHash` while feeding a zstd encoder; the compressed bytes are teed through a second SHA-256 for the `FileHash` and spooled to a temp file. Dump, NarHash, compression, and FileHash all happen in a single pass — the NAR is never materialized uncompressed on disk.
3. **`PUT nar/{FileHash}.nar.zst`** (or `nar/{nixbase32(NarHash)}-{NarSize}.nar` with `--no-compress`), then a **signed `PUT {hash}.narinfo`** describing it. The narinfo's fingerprint covers the store path, `NarHash`, `NarSize`, and references — never the URL or compression — so the server can re-render it around its canonical NAR while keeping the signatures ([Protocol Discipline](./nix-substituter.md#protocol-discipline)).

`--recursive` first walks each path's reference closure from `db.sqlite` and pushes **references-first** — a path is always uploaded after everything it depends on, so the cache is never momentarily inconsistent (a present narinfo whose references 404). The temp spool file is deleted on drop, success or failure.

```bash
# Push one path and its whole closure, signing on the way out.
nl-nix --to http://cache.example.com:50071/nix/main \
       --signing-key /etc/nativelink/cache.key \
       push --recursive ./result

# pushed /nix/store/…-glibc-2.40
# skipped (present) /nix/store/…-bash-5.2
# pushed /nix/store/…-my-app
# done: 2 uploaded, 1 already present, of 3 paths (18446744 wire bytes)
```

### pull

`nl-nix pull <hashes…> --out <DIR>` fetches each path by its 32-character store-path hash, decompresses the NAR through the codec its narinfo names, **verifies the restored bytes against the signed `NarHash`/`NarSize`**, and restores the tree into `<DIR>/<hash>`. It deliberately does **not** register the result in the local store — it is an inspection/mirroring tool, not `nix copy --from`. Signature verification against a trusted-key set exists in the library (`CacheClient::pull_path` takes trusted keys) but the CLI does not yet expose a `--trusted-key` flag, so today the CLI verifies NAR *integrity* against the signed hash, not the narinfo *signature* against a public key.

```bash
nl-nix --to http://cache.example.com:50071/nix/main \
       pull a1b2c3… --out /tmp/inspect
# pulled a1b2c3… (5242880 NAR bytes) -> /tmp/inspect/a1b2c3…
```

### info

`nl-nix info` prints the cache's `/nix-cache-info` verbatim — the fastest way to confirm the base URL, token, and mount prefix are right before a large push.

### reconcile

`nl-nix reconcile` takes a stable, path-sorted snapshot of every row in the
local Nix `ValidPaths` database and pushes it with bounded concurrency. Unlike
the event watcher, this includes paths committed before daemon startup, missed
events, and every output of a multi-output derivation. The validated dedup probe
also makes reconciliation self-heal poisoned historical narinfos.

The NixOS module runs reconciliation two minutes after boot and hourly
thereafter while `nixCache.watchStore` is enabled. `nixCache.reconcileInterval`
changes the timer cadence.

## `nl-watch-store`: the auto-push daemon

`nl-watch-store` turns "a path was just built" into "a path is in the cache" with no cron, no post-build hook, and no `nix copy` step. It watches the store directory and, for each newly-committed top-level store path, reads its metadata and pushes it — deduped, signed, and zstd-streamed, exactly as `nl-nix push` would.

**The watch is `fanotify` first.** A directory mark on the store with `FAN_REPORT_DFID_NAME` (Linux ≥ 5.9) catches the atomic `rename()` that commits a path (`FAN_MOVED_TO`) and directory creation (`FAN_CREATE`), and reports the parent directory plus the entry name so the path resolves without racing a half-built tree. `fanotify` needs `CAP_SYS_ADMIN` — a non-issue for a root systemd unit, and strictly better than `inotify` for a privileged watcher. On an unprivileged host or an older kernel, `--inotify` selects the `inotify` fallback, and the daemon also downgrades automatically if `fanotify_init` returns `EPERM`. Either backend only emits an event once the entry is a fully-committed store path (it must pass `StorePath::from_full_path`), never a temporary build directory.

Metadata is read from `db.sqlite` on the daemon's main task — the reader's SQLite connection is `!Sync`, so it can't cross a spawn — and only the `Send` push future is spawned, under a semaphore that bounds it to `--concurrency` (default 8) in-flight uploads. `SIGINT` and `SIGTERM` stop accepting new events and let in-flight pushes drain.

| Option | Default | Meaning |
|---|---|---|
| `--to <URL>` | — (or `NL_NIX_CACHE`) | Cache to push to, mount prefix included |
| `--store <DIR>` | `/nix/store` | Store directory to watch and read |
| `--token <TOKEN>` | — (or `NL_NIX_TOKEN`) | Bearer write token |
| `--signing-key <FILE>` | — (repeatable) | Ed25519 secret key(s) to sign pushed narinfo |
| `--compression-level <N>` | `3` | zstd level for uploads |
| `--concurrency <N>` | `8` | Maximum concurrent path pushes |
| `--inotify` | off | Use the `inotify` fallback instead of `fanotify` |

A systemd unit for a build host that mirrors everything it realizes into a private cache:

```ini
[Unit]
Description=Auto-push new Nix store paths to the NativeLink cache
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/bin/nl-watch-store --signing-key /etc/nativelink/cache.key
Environment=NL_NIX_CACHE=http://cache.example.com:50071/nix/main
Environment=NL_NIX_TOKEN=%d/nix-cache-token
Environment=NL_OTEL_ENDPOINT=http://otel-collector:4317
# fanotify needs CAP_SYS_ADMIN; grant just that, drop the rest.
AmbientCapabilities=CAP_SYS_ADMIN
CapabilityBoundingSet=CAP_SYS_ADMIN
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

## Reading the Local Store

Both binaries read `<store>/../var/nix/db/db.sqlite` **read-only** through `rusqlite`. `ValidPaths` supplies each path's `NarHash`, `NarSize`, `deriver`, `sigs`, and `ca`; `Refs` supplies the reference edges the closure walk follows. The reader opens with an `immutable=1` URI when the WAL sidecar files aren't writable, so it coexists with a running `nix-daemon` without taking a lock or perturbing the database. Store-path hashes are handled whether the database stores them base16 (`sha256:<64 hex>`) or nixbase32, and every path is validated on construction — a `StorePath` can never carry a traversal segment or a malformed hash into a URL or a query.

## Metrics

The client and daemon emit OpenTelemetry metrics over OTLP through the same `nativelink-util::telemetry` path the server uses; `init_tracing()` runs at startup, best-effort, so a missing collector drops metrics rather than failing the command. Point `NL_OTEL_ENDPOINT` at the collector exactly as for the server ([Observability](../part7/observability.md#the-two-environment-knobs-that-matter)). The instrument scope (meter name) is `nl-nix` or `nl-watch-store`; the metric names are `nl.nix.*` and reach Prometheus through the same three transforms every other NativeLink metric does — dots to underscores, a collector-added `nativelink_` prefix, and a `_total` suffix on counters:

| Instrument (`nl.nix.*`) | Type | Unit | Prometheus series |
|---|---|---|---|
| `nl.nix.paths_pushed` | Counter | paths | `nativelink_nl_nix_paths_pushed_total` |
| `nl.nix.paths_deduped` | Counter | paths | `nativelink_nl_nix_paths_deduped_total` |
| `nl.nix.push_errors` | Counter | pushes | `nativelink_nl_nix_push_errors_total` |
| `nl.nix.nar_bytes` | Counter | bytes | `nativelink_nl_nix_nar_bytes_total` |
| `nl.nix.wire_bytes` | Counter | bytes | `nativelink_nl_nix_wire_bytes_total` |
| `nl.nix.push_seconds` | Histogram | seconds | `nativelink_nl_nix_push_seconds_bucket` |
| `nl.nix.paths_pulled` | Counter | paths | `nativelink_nl_nix_paths_pulled_total` |
| `nl.nix.pull_errors` | Counter | pulls | `nativelink_nl_nix_pull_errors_total` |
| `nl.nix.pulled_bytes` | Counter | bytes | `nativelink_nl_nix_pulled_bytes_total` |

The compression ratio a push actually achieves is `nl.nix.wire_bytes / nl.nix.nar_bytes`; the dedup hit rate on a `nl-watch-store` fleet is `nl.nix.paths_deduped / (nl.nix.paths_pushed + nl.nix.paths_deduped)`.

## Code Map

| File | Purpose |
|---|---|
| `nativelink-nix-client/src/bin/nl_nix.rs` | The `nl-nix` CLI: `push`/`pull`/`info`, closure walk, bounded-concurrency transfers |
| `nativelink-nix-client/src/bin/nl_watch_store.rs` | The `nl-watch-store` daemon: watch → read metadata → spawn push, with drain |
| `nativelink-nix-client/src/client.rs` | `CacheClient`: dedup probe, dump→hash→zstd→spool push, verifying pull |
| `nativelink-nix-client/src/nar.rs` | Pure-Rust streaming NAR `dump_path`/`restore_path` (byte-identical to `nix-store --dump`) |
| `nativelink-nix-client/src/store.rs` | `NixStore`: read-only `db.sqlite` reader, `StorePath`/`PathMeta` |
| `nativelink-nix-client/src/watch.rs` | `watch_store`: `fanotify` primary, `inotify` fallback, `StoreEvent` stream |
| `nativelink-nix-client/src/metrics.rs` | The `nl.nix.*` OTLP instruments |
| `nativelink-nix-client/src/config.rs` | `CacheConfig`, the `Auth` enum |

## Cross-References

- [The Nix Substituter Facade](./nix-substituter.md) — the server these tools talk to: routes, ingest, signing, compression, read-through.
- [Observability](../part7/observability.md) — the OTLP pipeline the `nl.nix.*` metrics ride, and how OTLP names become Prometheus names.
- [Local Remote Execution with Nix](../part8/lre-nix.md) — the toolchain closures most worth pushing.
