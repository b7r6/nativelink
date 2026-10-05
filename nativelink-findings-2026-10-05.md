# NativeLink findings catalog — state of 2026-10-05

All branches live on `b7r6/nativelink` (GitHub). Integration branches, both green under `bazel test //...`:

- `integration/all-fixes-2026-10-05` — all 33 fix branches merged onto OSS main @4ef4c34e34 (122/122 tests, no build errors)
- `straylight/on-main-2026-10-05` — migrate-1.7.3 (straylight product + 18 ported fixes) rebased onto the same main
- `integration/straylight-plus-all-fixes-2026-10-05` — straylight surface + all fixes (131/131 tests, no build errors)
- `straylight/{main,nix-cache,nix-cache-s3-firefix,migrate-1.7.3,upstream-main}` — git.s4.gl mirror (forge offline backup)
- `meta/findings-catalog-2026-10-05` — this catalog + UPSTREAM-LEDGER.md

Branch-combination notes: redis lock pinned back to 1.0.0 (upstream sentinel replays); fast-first `has()` contract kept
(straylight write-back semantics) over the stack's slow-first test variants; WORKER-MEM superseded by upstream #2873.

Upstream (TraceMachina/nativelink) by b7r6: 34 closed, 3 merged (#2874 #2878 #2879), 4 open CI PRs (#2877 #2880 #2886 #2897).

| id | title | subsystem | sev | status | fork branch | all-fixes | stray+ | former OSS PR |
|---|---|---|---|---|---|---|---|---|
| CI-WARM | ci(nix): key Bazel-Dev lane to hermetic toolchain identity (fixes 0 cache hits) | ci/nix | n/a | MERGED upstream (#2874, ci/bazel-dev-warming) | `ci/bazel-dev-warming@0273fade` | Y | Y | TraceMachina#2874 |
| WORKER-MEM | cgroup reclaimable page cache counted as used -> false memory pressure -> workers pause (THE P0 root cause; stays-fixed vs restart) | worker/capacity.rs | P1 | SUPERSEDED upstream (#2873 merged the newer iteration with dirty+writeback subtraction); fork branch historical | `b7r6/worker-free-memory-reclaimable-cache@12e69b38` | Y | Y | - |
| N13 | Short/torn/ABORTED ByteStream uploads committed as complete + dishonest QueryWriteStatus | service/bytestream + store/filesystem | P1 | held on fork (verified; integrated+green 2026-10-05) | `b7r6/cas-integrity-unified@92013ba764` | Y | Y | withdrawn (was 1.7.3 PR cand) |
| N14 | Cancelled upload strands FindMissingBlobs waiters to client deadline (build-disruptor) | store/fast-slow in-flight-write map | MEDIUM | held on fork (verified; integrated+green 2026-10-05) | `b7r6/fastslow-inflight-waiter-fresh-upload@a2a99518` | Y | Y | - |
| SCHED-D | Lost/undecodable QUEUED records counted-never-deleted -> queue grows forever, scheduler count stays accurate | scheduler/redis awaited-action-db | MEDIUM | held on fork (verified; integrated+green 2026-10-05) | `b7r6/scheduler-redis-deletes-lost-queued-records@ad2e2c61` | Y | Y | - |
| FLEET-R2 | Cold large-tree BatchReadBlobs 30s per-blob timeout under R2 writeback backfill | store/fast-slow + r2 slow tier | MED-HIGH | cataloged (robustness item | ` non-blocking)` | - | - | - |
| B01 | Same-id worker re-registration strands old incarnation's running ops (no requeue) | scheduler/worker-registry | P1 | held on fork (verified; integrated+green 2026-10-05) | `fix/worker-replacement-evicts` | Y | Y | was #2833 |
| B02 | Client keep-alive stamp lost racing a worker's versioned state write (lost-update) | scheduler/awaited-action-db | P1 | held on fork (verified; integrated+green 2026-10-05) | `fix/keepalive-versioned-writes` | Y | Y | was #2834 |
| B03 | Worker liveness not re-checked under eviction lock (keepalive TOCTOU) | scheduler/worker-scheduler | P1 | held on fork (verified; integrated+green 2026-10-05) | `fix/eviction-rechecks-heartbeat` | Y | Y | was #2835 |
| B04 | Matching-engine task not gated on construction (startup race) | scheduler/matching-engine | P1 | held on fork (verified; integrated+green 2026-10-05) | `fix/matching-engine-startup` | Y | Y | was #2836 |
| B05 | Failed client-keepalive write still advances local timestamp (fake liveness) | scheduler/awaited-action-db | P1 | held on fork (verified; integrated+green 2026-10-05) | `fix/store-keepalive-retry` | Y | Y | was #2837 |
| B06 | Dispatch identity not re-validated before killing a revoked op (TOCTOU) | scheduler/worker-scheduler | P1 | held on fork (verified; integrated+green 2026-10-05) | `fix/kill-revoked-revalidates` | Y | Y | was #2838 |
| B07 | Kill received before action registration completes is dropped (N1) | worker/running-actions | P1 | held on fork (verified; integrated+green 2026-10-05) | `fix/n1-kill-before-registration` | Y | Y | was #2849 |
| B08 | Same-worker re-dispatch double-charges resource budget, leaks capacity (N2) | scheduler/worker | P1 | held on fork (verified; integrated+green 2026-10-05) | `fix/n2-same-worker-redispatch` | Y | Y | was #2850 |
| B09 | Stale placement dispatches escalated action with old smaller reservation (N3) | scheduler/state-manager | P1 | held on fork (verified; integrated+green 2026-10-05) | `fix/n3-placement-version` | Y | Y | was #2851 |
| B10 | Redis pub/sub subscriptions silently lost on reconnect/transient init error | store/redis | P1 | held on fork (verified; integrated+green 2026-10-05) | `fix/redis-subscription-lifecycle` | Y | Y | was #2839 |
| B11 | Batch size/existence query mass-evicts every resident entry (has() cascade) | store/evicting-map | P1 | held on fork (verified; integrated+green 2026-10-05) | `fix/evicting-map-batch-eviction` | Y | Y | was #2840 |
| B12 | get_part returns truncated/torn blobs as success; has() not atomic | store/redis | P1 | held on fork (verified; integrated+green 2026-10-05) | `fix/redis-read-integrity` | Y | Y | was #2841 |
| B13 | Non-refcounted remove-callback pause + get_part resurrection (closes N4+N5) | store/existence-cache | P1 | held on fork (verified; integrated+green 2026-10-05) | `fix/existence-cache-pause` | Y | Y | was #2842 |
| B14 | try_subscribe returns completed record over live successor -> recreated op (N7) | scheduler/awaited-action-db | P2 | held on fork (verified; integrated+green 2026-10-05) | `fix/awaited-action-prefer-live` | Y | Y | was #2853 |
| B15 | N4/N5 interleavings (reconciled into #2842) | store/existence-cache | P1/P2 | reconciled into B13/#2842 (no separate PR) | `-` | - | - | - |
| B16 | #2842 pause drop-guard discarded pending-removal vector -> invalidation lost forever | store/existence-cache | P2 | held on fork (verified; integrated+green 2026-10-05) | `fix/existence-cache-pause` | Y | Y | was #2842 |
| D1 | DedupStore::get_part serves truncated/torn chunk as Ok (or panics) | store/dedup-store | P1 | held on fork (verified; integrated+green 2026-10-05) | `pr/worker-cleanup-guard` | Y | Y | was #2852 |
| N8 | CacheLookupScheduler coalescing drop-guard clobbers a concurrent same-key entry | scheduler/cache-lookup | P2 | held on fork (verified; integrated+green 2026-10-05) | `b7r6/cachelookup-scopeguard-stale-remove` | Y | Y | was #2860 |
| N9 | GrpcStore::get_part bounded-read resume over-reads (read_limit not decremented) | store/grpc-store | P2 | held on fork (verified; integrated+green 2026-10-05) | `b7r6/grpc-read-limit-resume` | Y | Y | was #2861 |
| N10 | S3/R2/ONTAP/GCS get_part HTTP inclusive-range off-by-one over-read (+1 byte) | store/s3+r2+ontap+gcs | P2 | held on fork (verified; integrated+green 2026-10-05) | `b7r6/s3-range-off-by-one-overread` | Y | Y | was #2868 |
| N11 | ExperimentalMongoStore::update commits truncated blob on mid-stream Err | store/mongo-experimental | P2 | held on fork (verified; integrated+green 2026-10-05) | `b7r6/mongo-update-truncated-on-stream-error` | Y | Y | was #2869 |
| N12 | ExperimentalMongoStore::update_data version-CAS conflict returns Err not retryable Ok(None) | store/mongo-experimental | P2 | held on fork (verified; integrated+green 2026-10-05) | `b7r6/mongo-update-data-upsert-defeats-version-cas` | Y | Y | - |
| TL1 | Keepalive stamped after inner.lock (held across await) -> false Stale eviction under load | scheduler/api-worker-scheduler | P1 | held on fork (verified; integrated+green 2026-10-05) | `b7r6/tl1-keepalive-liveness-starvation` | Y | Y | was #2867 |
| W1 | max_upload_timeout is a cooperative soft-deadline, not a hard cancel (fd/task leak) | worker/upload-timeout | P2 | confirmed; RFC/structural (no point fix) | `-` | - | - | - |
| W2 | upload_file trusts has()==Some as proof of complete content (TOCTOU, amplified by W1) | worker/upload-file | P2 | confirmed; RFC/structural (contingent) | `-` | - | - | - |
| W3 | wait_for_cleanup stale-dir removal TOCTOU | worker/running-actions-cleanup | - | non-finding (already fixed upstream) | `-` | - | - | - |
| W4 | Redis chunked get_part tears on same-/shorter-length replacement at a chunk boundary | store/redis | P2 | confirmed + reachable; RFC/structural (reproduction pins it) | `fork#5 aff1449c` | - | - | - |
| E1 | S3 multipart part size 5MiB floor -> excessive parts; bound multipart memory (64MiB) | store/s3 | P2 | held on fork (verified; integrated+green 2026-10-05) | `pr/s3-multipart-64mib` | Y | Y | was #2813 |
| E2 | Fail fast on a dead shard-ring peer instead of wedging every RPC | store/shard | P2 | held on fork (verified; integrated+green 2026-10-05) | `pr/shard-ring-dead-peer` | Y | Y | was #2814 |
| E3 | CompletenessCheckingStore propagate backend errors instead of serving as misses | store/completeness | P2 | held on fork (verified; integrated+green 2026-10-05) | `pr/completeness-error-propagation` | Y | Y | was #2815 |
| E4 | Map transport-originated Unknown to retryable Unavailable | transport | P2 | held on fork (verified; integrated+green 2026-10-05) | `pr/transport-unknown-retryable` | Y | Y | was #2816 |
| E5 | Add --check: offline config validation of store/scheduler references | config | P3 | held on fork (verified; integrated+green 2026-10-05) | `pr/config-check-flag` | Y | Y | was #2817 |
| E6 | Split gRPC write requests over the 4MiB message limit | store/grpc-store | P2 | held on fork (verified; integrated+green 2026-10-05) | `fix/grpc-write-chunk-limit` | Y | Y | was #2659 |
| FUZZER | Deterministic simulation fuzzing for SimpleScheduler (DST harness) | scheduler/test-harness | tool | held on fork (verified; integrated+green 2026-10-05) | `pr/scheduler-race-fuzzer` | Y | Y | was #2831 |
| DOCS | NativeLink Operator & Contributor Guide (mdbook) | docs | - | held on fork (verified; integrated+green 2026-10-05) | `docs/operator-contributor-guide` | Y | Y | was #2854 |
| RECHAOS | Black-box REAPI adversarial fuzz + determinism toolkit (public) | tooling | tool | PUBLIC (github.com/b7r6/rechaos) | `-` | - | - | - |
