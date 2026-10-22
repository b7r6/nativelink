# Categorical implementation source and removal map

Inspection date: 2026-10-22. Clean starting checkout:
`283ff776264fd4f7afe672d209995df088fc347d`, subject
`ci: defer the main.yaml integration-tests OCI lane off per-PR`.
Working directory: `/home/b7r6/src/tracemachina/nativelink/nativelink-b7r6-0x03`.
Origin is `git@github.com:b7r6/nativelink.git`; implementation branch observed
during this run is `categorical-elimination`. No preexisting tracked or untracked
changes were reported by the initial `git status --short`.

The plan's public baseline `a7bb376e6f66284eb37a77b22d2f258c03ba9a7f` was initially
absent, then fetched by the root implementation task. The inspected scheduler
diff to starting HEAD changes `memory_awaited_action_db.rs` listing from eager
collection under one lock to lazy `ChunkedStream` reads, and removes
`the_listing_is_read_whole_when_it_is_requested` from
`simple_scheduler_state_manager_test.rs`. The removed test documents a suspended
prefetch/lock-wait deadlock history; retain that history as a regression input,
without treating either old listing implementation as the new model's oracle.
No other source drift in the reviewed scheduler/storage/config/worker/service
scope was reported by the root comparison. Line numbers below describe the
inspected checkout and may move during implementation. Paths are relative to
the repository root.

Read root `AGENTS.md`; its source-of-truth, generated-doc, test, and contribution
rules apply. This design directory is not generated docs. Do not hand-edit the
generated config reference or root llms corpora when implementation lands.

## Mutation, protocol, and ownership seams

| Responsibility | Existing source | Required handoff |
| --- | --- | --- |
| Scheduler construction | `nativelink-config/src/schedulers.rs:30`; `nativelink-scheduler/src/default_scheduler_factory.rs` | One canonical scheduler, no backend or wrapper selection |
| Scheduler state | `simple_scheduler.rs`, `simple_scheduler_state_manager.rs`, `memory_awaited_action_db.rs`, `store_awaited_action_db.rs`, `awaited_action_db/` in scheduler `src` | Reducer owns transitions; remove both legacy state implementations after cutover |
| Worker authority/capacity | `api_worker_scheduler.rs`, `worker_registry.rs`, `worker.rs` in scheduler `src` | Attempt/incarnation/session/effect identities and explicit uncertainty |
| Matching/unsatisfiable | `worker_capability_index.rs`, `match_outcome.rs`, `unsatisfiable_tracker.rs` in scheduler `src` | Preserve diagnosable eligibility and bounded matching work |
| Property types | `nativelink-scheduler/src/platform_property_manager.rs:79`; `nativelink-util/src/platform_properties.rs:113` | Fixed typed resources/exact requirements; report removal of dynamic missing-property matching and custom type rules |
| Request cacheability | `nativelink-util/src/action_messages.rs:344`; `nativelink-service/src/execution_server.rs:271` | Separate lookup bypass from immutable `Action.do_not_cache` |
| Scope and client compatibility | `execution_server.rs:191`, `:254`; `action_messages.rs:257` | Preserve Buck2 scope only for inflight sharing; preserve command-platform fallback and operation-name resolution |
| Worker publication bypass | `nativelink-worker/src/local_worker.rs:732`; `running_actions_manager.rs` cache/history helpers | Remove pre-acceptance AC/history publication and mutable terminal-message templates |
| Direct AC updates | `nativelink-service/src/ac_server.rs:121` | Route through accepted publication; existing guard is read-only instance config, not worker/client identity authorization |
| Listeners/trust | `nativelink-config/src/cas_server.rs:735`; `src/bin/nativelink.rs` service mounting and mTLS construction | Fixed worker/client roles and verified exclusion from every writable AC ingress |
| Worker materialization | `local_worker.rs:1058`; `running_actions_manager.rs:871` | Replace required FastSlow composition with fixed filesystem workspace/gateway path, preserving hardlinks and pins |
| Worker lifetime/capture | `nativelink-config/src/cas_server.rs:1358`, `:1827`; `nativelink-worker/src/local_worker.rs:522` | Preserve single-use incarnation fact; qualify file-capture lifecycle separately |
| Physical factory | `nativelink-store/src/default_store_factory.rs:85` | Replace recursive construction with fixed physical/transport roles |
| Filesystem generations | `filesystem_store.rs:1124`, `:1882`, `:1904`, `:1954`, `:2091` in store `src` | Preserve generation paths; add a store incarnation that is never reused, exact-generation retirement and qualified durable publication |
| Azure writes/cleanup | `azure_blob_store.rs:133`, `:197`, `:415`, `:767`, `:1095` in store `src` | Replace digest-only generation identity and detached cleanup; distinguish corruption and absence |
| Azure fixtures | `nativelink-store/tests/azure_blob_store_test.rs`; `azurite_store_test.rs:61` | Reuse mock/emulator coverage; add generation/fault suite and actual provider qualification |
| Config entry | `nativelink-config/src/cas_server.rs:1775` | Separate canonical schema; reject any legacy top-level stores/schedulers occurrence before effects |

## Scheduler dispositions

All five variants at `nativelink-config/src/schedulers.rs:30` are covered.

| Legacy variant | Canonical disposition |
| --- | --- |
| `Simple` | Replaced by committed reducer/OpenRaft path |
| Grpc | Independent scheduler federation retired; internal forwarding serves the same authority |
| CacheLookup | Fixed request stage under the lookup/cacheability matrix |
| PropertyModifier | Wrapper removed; only fixed validated normalization remains |
| HistoricalResource | Wrapper/class selectors removed; accepted history feeds typed estimates bounded by requirements and safety |

## Store dispositions

All eighteen variants at `nativelink-config/src/stores.rs:52` are covered.

| Legacy variant | Canonical disposition and migration consequence |
| --- | --- |
| CacheMetrics | Fixed instrumentation; remove wrapper |
| Memory | Bounded caches/tests only; memory-only authoritative CAS needs explicit destination |
| ExperimentalCloudObjectStore | Azure first; AWS/S3, GCS, ONTAP, R2, OCI require later qualification or migration |
| OntapS3ExistenceCache | Remove wrapper; no implicit ONTAP support |
| Verify | Mandatory boundary verification; remove wrapper |
| CompletenessChecking | Fixed publication/serving closure validation |
| Compression | Fixed versioned encoding; legacy physical encoding needs explicit import/migration |
| Dedup | Chunk/index layout retired initially; identical whole-blob reuse retained |
| ExistenceCache | Bounded derived optimization, never proof of durability or protection |
| FastSlow | Fixed gateway read cache and worker materialization roles |
| Shard | Arbitrary placement retired; converter enumerates every old destination |
| Filesystem | Qualify singleton authority placement plus worker/cache roles |
| RefStore | Remove arbitrary aliases; retain explicit endpoint and namespace identity |
| SizePartitioning | Arbitrary size placement retired; show both lower/upper destinations |
| Grpc | Remove recursive store; retain authenticated worker-to-gateway CAS transport |
| RedisStore | Remove implementation, client, helpers, dependency and operational closure |
| Noop | Tests only; never acknowledge-and-discard production storage |
| ExperimentalMongo | Unsupported target layout; explicit migration |

## Deletion and migration closure

Redis reaches `nativelink-error/Cargo.toml:26` and
`nativelink-error/src/lib.rs:329`, scheduler/store Cargo and Bazel dependencies,
root Cargo metadata, lockfiles, `nativelink-redis-tester`, store Redis utilities,
legacy scheduler DB/index/notification code, integration fixtures, deployment
examples, and supported operating docs. M9 audits the entire shipped graph;
removing a config variant or making connections conditional is insufficient.
The offline legacy parser may recognize Redis data, but has no Redis client.

Minimum converter fixtures: shard, size partition including Redis/S3, dedup,
compression, grpc aliases, scheduler Redis state, memory-only CAS, mixed
CAS/authority aliases, removed history templates, custom property rules,
Buck2 isolation, single-use workers, and a worker-reachable writable AC route.
Show namespace/collector ownership, physical encoding, endpoints, prefixes,
credential locations, trust/network boundaries, and data relocation. Never
guess a destination or claim that a converted file proves live fencing.

Initial qualification manifest, not current support claims:

| Stage | Authority and storage placement | Additional release gate |
| --- | --- | --- |
| M4a | Singleton durable authority; fixed worker/gateway roles; isolated temporary mutable AC sink | New worker cohort, publication ownership, converter, listener exclusion |
| M4b | Same implementation with replicated authority | Elections, partitions, membership, restart and ambiguous effects |
| M5/M6 | Qualified singleton filesystem or Azure shared namespace; immutable publications | Actual physical-generation/acknowledgment/retention contract |
| M9 | Finite tested combinations of qualified durable/volatile authority and qualified placement | Full fault/upgrade/cutover campaigns; no Redis or recursive runtime topology |

Unrelated local filesystems do not become shared storage because the scheduler
has three members. Regional gateways remain unqualified until latency/failure
tests pass. Memory-only authoritative CAS, arbitrary upstream chains, independent
scheduler federation, and the other providers above are explicit reductions.

## Validation and documentation handoffs

Repository commands include `cargo test -p nativelink-scheduler`, focused Bazel
targets, and `bazel test //...` for final broad qualification. Dependency changes
also require locked resolution, actual feature output
(`cargo tree --locked -e features -i openraft@0.9.25`), and repository Bazel
dependency regeneration. Exact commands/results are separate evidence, not
asserted by this source inspection. Tools discovered in PATH were Nix-provided
Cargo/Rust 1.97.1, Bazel, and Bun 1.3.13; no version or dependency qualification
is inferred from their path names.

Behavior changes must update platform-properties, architecture, server/service,
security-hardening, production-config, store backend, and CLI/env docs where
applicable. Config comments regenerate config reference; metrics source/call
sites regenerate metrics reference. Changed URLs need redirects. Follow the
root `AGENTS.md` docs gates and AI-assisted contribution disclosure before any
future PR. No deployment, merge, or PR is implied by these design records.
