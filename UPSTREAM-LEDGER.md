# The Upstream Ledger

Every commit this fork carries over `TraceMachina/nativelink`, classified:
**PR** (upstreamable, with its wave), **LOCAL** (the fork's own product surface
or environment), or **SUPERSEDED** (upstream solved it another way). Maintained
as part of the merge runbook; re-audit at every upstream sync.

Baseline at last audit: upstream/main `4278d4b3`, fork `nix-cache` 76 ahead
(2026-08-04, merge `7c342778`). The +7 sync (scheduler 1.6 patches, worker
disconnect fix, Redis retryability #2657, FT.AGGREGATE expiry, bazel-retry,
docs regen) superseded nothing we carry.

## PR waves

Ordered by isolation and value; each wave is one reviewable PR.

| wave | commits | subject | notes |
| ---- | ------- | ------- | ----- |
| 1 | `5016e7d9` | grpc_store: cap WriteRequest chunk size (fix >4 MiB blob upload) | **OPENED: TraceMachina/nativelink#2659** (2026-08-03) — ported to upstream shape (both `update()` and `update_compressed()`), with a draining-mock regression test that reproduces the exact production error against the uncapped client. |
| 2 | `4341e365` | store: dead shard-ring peer fails fast instead of wedging every RPC | **NEW (2026-08-03), strong candidate.** Production-observed: a dead/restarting `grpc_store` peer in a shard ring hung every CAS RPC forever (`ConnectionManager` reconnects in the background while `connection()` requests queue unbounded; `rpc_timeout_s` only covered writes). Fix = per-endpoint connect health gating (fast `Unavailable` when the whole endpoint set is unreachable) + `rpc_timeout_s` applied to unary RPCs and read-stream establishment. Ships with an in-process two-shard lab repro (`shard_ring_dead_peer_test`) that hangs against the unfixed code. Do not open yet. |
| 3 | `8b35998f` + `9432960a` | s3_store: 64 MiB multipart parts (>5 GiB write-through ceiling) | Bug report + fix pair; squash for the PR. |
| 4 | `b2994285` | store: zero-length underflow panic; completeness backend error propagation | Two small correctness fixes; may split on review feedback. |
| 5 | `478bdde2` | buf_channel: widen in-flight buffer 2 → 32 (write-stall resets) | Perf/correctness; needs a motivating benchmark in the PR text. |
| 6 | `5de7c286` + `7318d924` (transport half) | error: transport Unknown → retryable Unavailable, anchored match | Retry-semantics change; expect design discussion. Pitch strengthened 2026-08-04: upstream's own #2657 reclassified transient Redis failures as retryable `Unavailable` on exactly the "Bazel treats non-retryable as permanent" argument — same thesis, adjacent (Redis vs tonic transport) surface, no overlap with our diff. |
| 7 | `2f27ca68` | fast_slow_store: opt-in write-back for the slow tier | Feature (config-gated, default off). Our #35 payoff. |
| 8 | `8fe8f2ed` | metrics: Prometheus /metrics endpoint | Feature; check upstream's own metrics plans first. |
| 9 | `6512f60e` | nativelink `--check`: offline config validation | Small feature, high operator value. |
| 10 | `ee7b5b32` | sha2: enable `asm` (hardware SHA-256) | Trivial; may fold into any earlier wave. |

`7318d924`'s multipart-memory-bounding half belongs to wave 3's territory;
split at PR time.

## LOCAL — the fork's product surface

- **Nix binary-cache facade** (`nativelink-nix`, `nix_cache` service+config,
  `nativelink-nix-client`: `nl-nix`, `nl-watch-store`): `9ff1e966` `ee48e5a5`
  `ecc40584` `f2b72777` `e3ccd4c8` `4d7dc333` `0e5c97bc` `6bf3b0d7` `4b5c8767`
  `5f7f3fa7` `81dfc2fa` `df1e18c6` `934f07e0` `80bb395a` `961bca4f` `aaf37fcc`
  `a1839d64` `2de39afa` `43ec1bc0` `2e2b7ea8` `f3e2d7a8` `7316b864` `ea803264`
  `87e1b2d1` — the CAS-is-the-spine thesis; not offered upstream (for now).
- **OCI→REAPI bridge** (`nativelink-oci`): `2a4cd2ed` `a8822ee3` `810bb8f7`
  `48f3c51e` `ea7b0700` — candidate for a future feature offer once PROD-3
  lands the Distribution API; too big for the current waves.
- **fetch proxy**: `08a15f69`.
- **The book + fork docs**: `4d314f27` (book half) `0ec9a370` `5fcf05ce`
  `f5a847de` `a4a55bf6` `e8dadc32` `009172b6` `5731eb9e` `95db40bf` `178b56a7`
  `3206aeaa` `67f1b1f8` `f5edb52c`.
- **Environment** (flake pins, vendored overlays, lre regen, lint config):
  `a245baf8` `8f1c1344` `0ee5b507` `8e07a545` `b0c9523b` `f0e6d31c` `5290d358`
  `b5032d38` `21389a08` `863598fd`.
- **Merge**: `6af1439e` (v1.6.3 rejoin).

## SUPERSEDED / PARTIAL

- `ee7b9296` (digest_hasher strict ByteStream checks): upstream landed its own
  mismatch telemetry in v1.6.3; the merge composed both. Our strictness knob
  may still be worth offering — revisit after wave 6.
- `4d314f27` (BLAKE3 digest-function defaulting half): verify against current
  upstream default handling before offering; book half stays LOCAL.
- `fe931b61` (clippy sweep): point-in-time; superseded by upstream's own lint
  churn. Nothing to offer.
- `cc69bb54` (grpc_store: end write stream after `finish_write`): SUPERSEDED —
  current upstream terminates the stream in `WriteRequestStreamWrapper`
  (`write_finished` guard in `poll_next`), proven by running our draining-mock
  test against upstream with the inner guard removed: it passes. Our inner
  guard stays as local defense-in-depth; the *test* went upstream with wave 1
  (#2659) as `write_stream_terminates_after_finish_write`.
