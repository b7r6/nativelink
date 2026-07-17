# Production Readiness — straylight-nativelink

The outstanding work to get *this fork's own surface* to production grade.

Base NativeLink — CAS, AC, scheduling, workers, the store algebra — is upstream
TraceMachina work that already carries real load; it is not in scope here. This
list is about the additions this fork made: the `nix_cache` substituter, the
`cas_witness` proxy, the `nativelink-oci` bridge, the `nativelink-nix-client`
tools, and the config/provisioning around them. Items the book flags that are
actually inherited upstream/REAPI characteristics are collected at the bottom as
context, not as our backlog.

**Sources.** `[book]` — a limitation the guide states plainly. `[code]` — found
by a sweep of the fork crates. `[session]` — surfaced while operating the fork
(see the two fixed bugs noted under Tier 2).

**Severity.** **P0** correctness/security blocker · **P1** blocks real use ·
**P2** robustness/coverage · **P3** observability/ops · **P4** roadmap.

---

## Tier 0 — Correctness / security blockers (P0)

- [ ] **OCI bridge: verify downloaded blob digests.** `fetch_blob()` returns
      registry bytes as-is; the requested `sha256:` is never recomputed, so a
      buggy/compromised/MITM registry can substitute content silently.
      `nativelink-oci/src/registry.rs:322-355`. `[code]`
- [ ] **OCI bridge: cap blob download + projection memory.** `resp.bytes()` has
      no `Content-Length`/size cap, and the projected filesystem tree is built in RAM
      with capacity taken from the **attacker-controlled tar header** — trivial
      OOM/DoS. `registry.rs:348`, `projection.rs:144,184-190`. `[code]`
- [ ] **Make strict digest-function enforcement the production default.** An
      unset digest function falls back to SHA256 with a one-time warning; a
      BLAKE3 client that omits the field gets SHA256-hashed output trees — a
      quiet cross-hashing correctness hole. `require_explicit_digest_function`
      defaults to false for upstream compatibility. `[book]`
- [ ] **`cas_witness`: add request authentication.** No auth today — anyone who
      can reach the listener drives fetches and obtains signed attestations.
      Private-network-only until fixed. `cas_witness.rs`; stated in
      `part10/cas-witness.md` "Current Limitations". `[book]` `[session]`

## Tier 1 — Functional gaps blocking real use (P1)

- [ ] **OCI: implement zstd layer decompression.** `decompress_zstd()` is a stub
      that returns `Err("zstd decompression not yet implemented")`; any image
      with a zstd layer fails the whole import. `oci_client.rs:356-363`. `[code]`
- [ ] **OCI: manifest-list / multi-arch resolution.** The `Accept` header
      requests only single image manifests; a multi-arch tag (an index) isn't
      selected. `registry.rs` (Accept ~`:278`, note `:130`). `[code]` `[book]`
- [ ] **OCI: private-registry credentials.** Real auth is Docker Hub only;
      ghcr.io/ECR/quay require anonymous access today. `[book]`
- [ ] **OCI + nix-client: retry + timeout on registry/cache HTTP.** Both are
      single-shot `reqwest` calls with no retry or explicit timeout; a transient
      blip is a hard failure. `registry.rs:256,322`, `client.rs`. `[code]`
- [ ] **nix-client: `pull --trusted-key` signature verification.** `nl-nix pull`
      verifies NAR integrity against the signed hash but does not verify the
      narinfo *signature* against a trusted public key (the library supports it;
      the CLI has no flag). `part10/nix-cache-client.md`. `[book]` `[session]`
- [ ] **nix-client: native flake-aware "build + push everything" (retire the
      shell wrapper).** The `push-flake` app (`nix run …#push-flake`) is a shell
      wrapper around enumerate-outputs → `nix build` → `nl-nix push --recursive`.
      Promote it to a first-class `nl-nix` capability (e.g. `nl-nix push --flake
      <ref>` or a `push-flake` subcommand) so "cache this whole flake" needs no
      external orchestration — nl-nix already speaks the push protocol; teach it
      to resolve a flake's outputs for the current system and push their closures
      (build via the Nix CLI or `nix-eval`/store APIs). `flake.nix`
      (`apps.push-flake`). `[session]`
- [ ] **`cas_witness`: re-hash on cache hit before re-attesting.** A hit re-emits a
      receipt after a `has()` existence probe, not a re-read/re-hash — the
      binding is the one captured at first fetch. `cas-witness.md`. `[book]`

## Tier 2 — Test coverage & robustness (P2) — highest ROI

> **Three** production-breaking bugs shipped **this session** from the same gap:
> `nl-watch-store` emitting **full-path** narinfo `References`/`Deriver` (the
> server 400'd ~64% of a real toolchain build round), a **frozen-snapshot**
> read-only DB open (the watch daemon saw no new paths at all), and **silent
> event drops under commit bursts** (large late-committed paths vanished with no
> log). All three were fixed (`43ec1bc0`, `2de39afa`, `2e2b7ea8`) — and all three
> shipped because only the ref-less, pure-parsing, calm-rate paths were tested.

- [ ] **Integration suite for the nix-client networking layer.** `client.rs`,
      `watch.rs`, and both binaries have no `#[cfg(test)]`. Cover: push a path
      **with references** and assert basename `References`/`Deriver`; pull +
      verify; `nl-watch-store` end-to-end against a live `nix_cache`; DB liveness
      under concurrent `nix-daemon` commits. `nativelink-nix-client/src/`. `[code]`
- [ ] **Tests for OCI `registry.rs` / `lib.rs`** — the network + entrypoint
      surface is untested (`oci_client.rs`/`projection.rs` do have tests). `[code]`
- [ ] **Exercise the slow-store-write-failure path.** Does `cas_witness` still
      *serve* a fetched body when the CAS/R2 write fails (best-effort), or does
      it error the FOD? Same for `nix_cache` under an unreachable slow tier.
      Untested. `[session]`
- [ ] **Reconcile the fanotify→inotify fallback.** The book says the daemon
      auto-downgrades on `EPERM`; the code selects the backend from a
      caller-passed `bool` (`watch_store(.., use_inotify)`). Make it match. `[code]`
- [ ] **Burst regression test for `nl-watch-store`.** The burst-drop fix
      (`2e2b7ea8`) has no test that reproduces a commit burst, so the regression
      can silently return. Commit N paths (several large, committed last) faster
      than the consumer drains, then assert every one is present in the target
      cache. `watch.rs`, `bin/nl_watch_store.rs`. `[session]` (burst-drop review
      finding D, `bugs/2026-07-17-…burst.review.md`)
- [ ] **Fully decouple the push-permit from the watch consumer.** The consumer
      still `acquire_owned().await`s the concurrency semaphore inline before
      spawning each push; under sustained push saturation the channel fills and
      backpressure now propagates to the (unbounded, `FAN_UNLIMITED_QUEUE`)
      kernel fanotify queue — a bounded drop traded for unbounded memory growth.
      Move the acquire into the spawned task. `bin/nl_watch_store.rs`. `[session]`
      (review finding B — a documented tradeoff today, not a defect)
- [ ] **Decide the clippy gate for `nativelink-nix-client`.** The workspace sets
      `std-instead-of-core = "deny"` (`Cargo.toml:196`) but the crate has never
      been clippy-clean against it (~20 pre-existing violations across
      `watch.rs`/`client.rs`/`nar.rs`/`store.rs`/`metrics.rs`/`lib.rs`; `nix
      build` runs `cargo build`, not clippy). Either do a crate-wide `core`/`alloc`
      pass and run clippy in CI, or record that the fork crates are exempt.
      `[session]` (review finding A)

## Tier 3 — Observability the fork should close (P3)

- [ ] **`cache_metrics` wrapper: emit `delete`/`evict`/`size`/`entries`.** These
      instruments are declared but never populated, so the eviction-rate alert
      never fires and the size/entry dashboards are dead. `part7/observability.md`.
      `[book]`
- [ ] **Dashboards + alerts for the `nl.nix.*` client metrics.** They reach the
      OTLP pipeline but nothing charts push/pull throughput, dedup rate, or
      failures yet. `[session]`

## Tier 4 — Config validation & provisioning (P3)

- [ ] **Grow `nativelink --check`** to the checks the Nix-provisioning appendix
      names but `--check` does not yet do: store-type-matches-usage (a
      `cas_fast_slow_store` really is a `fast_slow`), `instance_name`
      reconciliation across CAS/AC/execution/capabilities/bytestream,
      worker↔scheduler platform-property alignment, shard-weight agreement, and
      **OCI `digest_function` == the execution service's**. Today these fail at
      boot or silently, not in CI. `appendix/nix-provisioning.md`. `[book]`
- [ ] **Fold the "Nix Provisioning" proposal into reality.** The appendix is
      still marked *Proposal*; the Dhall-typed config + NixOS module it describes
      is now actually built and running (see the sibling `nixos-config`). Update
      the appendix to document what shipped. `[book]` `[session]`

## Tier 5 — Roadmap / explicitly future (P4)

- [ ] `nix_cache` builds-as-actions phase (realisation as an ordinary REv2 action).
- [ ] OCI: streaming layer import (drop whole-image buffering), manifest→REAPI
      digest mapping cache, worker toolchain pre-staging, conformance tool
      (`nativelink-toolchain-check`).
- [ ] `cas_witness`: MPC-TLS notarization path (the reserved-`null` `notary` field).

---

## Inherited from upstream / REAPI — context, not our backlog

These are real limitations the book documents, but they are upstream NativeLink
or REAPI design, not the fork's surface. Work around them, or contribute
upstream. The three that bite hardest here and would make good upstream PRs are
marked ★.

- ★ **Digest-function default** is SHA256-silent-fallback (see Tier 0 #3 — the
  fork already added the opt-in fix; contributing the safer default upstream is the ask).
- ★ **No `graceful_shutdown_timeout`** — worker drain is unbounded, so k8s
  `terminationGracePeriodSeconds` must be `>= max_action_timeout_s`.
- ★ **Multi-value platform properties collapse to the last value** (`HashMap`
  insert), so `values: ["rust","cpp"]` silently keeps only one.
- No `InvalidateActionResult` RPC (stale AC entries can't be invalidated in place).
- `fast_slow` never re-checks the slow store; `dedup.has()` doesn't verify chunks.
- `SimpleScheduler` is in-memory by default; HA needs the *experimental* Redis backend.
- No gRPC health service (HTTP `/status` only; worker pods expose no listener).
- 64 KiB batch cap; no transport compression advertised; no streaming stdout/stderr.
- Namespace isolation is not a security boundary; container startup overhead makes
  the per-action container path unfit for sub-second actions.
- Worker resource queries run once at connect; mTLS is presence-of-`client_ca_file`,
  not a toggle; multi-region failover and worker drain are DIY (admin HTTP route,
  no CLI).
