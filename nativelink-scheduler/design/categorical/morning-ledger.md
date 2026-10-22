# Morning ledger

2026-10-22. Questions and external dependencies are collected here so independent
implementation can continue without interrupting the user. This is not a request
for approval of routine engineering choices. User authorization superseded the
draft's M1 stop; qualification, production-format, deployment, and merge boundaries
remain distinct.

## Implementation checkpoint

The branch contains an experimental committed-event scheduler, an independently
implemented oracle and replay/shrinking harness, a real OpenRaft/redb journal,
three-member fault tests, and an identity-bearing worker recipient using real
subprocesses. Production factories and listeners still construct the legacy
runtime. No production cutover, deletion of Redis, deployment, or merge has
occurred.

The scheduler now preserves original REAPI request/result payloads, resource
charges, coalesced operation interests, fixed priority/FIFO order, bounded
starvation holds, and a typed three-attempt memory-retry policy. The retry policy
keeps accepted minima and every old reservation immutable. A worker's memory
classification and physical quiescence still need qualification at the eventual
coordinator boundary; exit status 137 is not that evidence.

The worker recipient retains duplicate/cancel/reconnect receipts and never calls
the legacy AC/history publisher. Linux PID-namespace closure is observed through
an exact `pidfd` receipt; directory cleanup, transport loss, and a process-group
kill without isolation do not refund uncertain capacity. A combined durable
authority/subprocess test exercises lost results, coordinator restart, replay,
accepted publication, and refused completion under termination uncertainty.
These are test adapters, not a mounted canonical REAPI service.

Finite coarse-retention comparison and synthetic journal measurements are in
[budget.md](budget.md) and [retention-contract.md](retention-contract.md).
[incident-coverage.md](incident-coverage.md) records scheduler campaigns and
counterexamples. Historical measurement and mutation receipts identify their
exact source hashes; later source changes do not inherit those performance
claims. Workspace-wide verification is recorded separately as it completes.

The October 22 checkpoint passed `cargo check --locked --offline --all
--profile smol -j 24` and the corresponding full Cargo test run: 1,546 passed,
zero failed, five ignored. All 135 Bazel test targets passed; a subsequent
three-target run covered the final snapshot-restore and fixture corrections.
[Workspace receipts](evidence/workspace-checkpoint/manifest.json) retain the
commands, compressed logs, and source hashes. An existing filesystem test used
shared fixed `/tmp` paths; it now uses the repository's unique temporary-path
helper after those paths collided with directories owned by another user.

The [first 20-mutant run](evidence/checkpoint-mutations-before-repair/mutations.json)
caught 19 injected faults. Removing the immovable CPU/slot check from memory
growth survived because the fixture's first growth fit both ceilings. The
fixture now exercises a second failure at the valid ceiling, requiring a
terminal result while uncertain work still owns the other worker's CPU and
slots. The [repaired run](evidence/checkpoint-mutations-repaired/mutations.json)
caught all 20 faults. Both baselines and all mutant logs are retained. These
receipts qualify their named assertions, not arbitrary faults or production
workload capacity.

The next slice implements [aggregate byte credits](byte-headroom.md), including
prepaid terminal copies for coalesced operations and bounded journal membership
and snapshot metadata. A real maximum-payload fixture recovers a snapshot larger
than 40 MiB. The [24-mutant run](evidence/byte-credit-mutations/mutations.json)
caught every selected fault. Its initial attempt stopped on an obsolete source
anchor; the repaired runner validates all anchors before compiling, and never
counts compiler rejection as a detected semantic fault.

[Worker progress](worker-progress.md) now observes actual transferred bytes,
completed work, and the owned running child. Real child and 2 MiB input fixtures
renew committed progress leases while replay and transport chatter cannot.
Execution deadlines remain fixed. The observer's 4096-identity limit can expire
healthy large actions; optimized cache/mount paths and supported workload bounds
remain unqualified. A wrapper initially overflowed a worker test's stack; boxing
the large inner future fixed it without changing thread stack limits. The full
96-test manager suite and four worker/authority bridge tests pass.

[Volatile recovery](volatile-recovery.md) uses the same journal transaction and
validation paths with a RAM backend. Five storage tests and eight cluster tests
cover fresh-learner recovery and refusal after quorum loss. Peer domain checks
remain fixture transport logic. Review found and fixed an error-response path
that skipped the final domain check; all three RPC types now check both success
and error responses. This does not implement production identity allocation or
authorize a same-domain disaster reset.

The [format activation design](format-activation.md) identifies a prerequisite:
the old compatible reader must already understand the base control vocabulary,
including an activation proposal that reached disk without committing. Executable
version alone cannot select a new writer format. Current prototype versions do
not have that compatibility contract.

The later workspace Cargo run passed 1,580 tests, with zero failures and five
ignored tests, across 143 result summaries. It preceded the final response-fence
fixture; the separate eight-test cluster run and strict Clippy cover that change.
The corresponding workspace check also passed. All 136 Bazel targets now pass.
The expanded generated-history target exceeded its old five-minute timeout in
the build without compiler optimization; its declared timeout is now longer, with all cases and
assertions retained. Redundant measurement serialization was removed only after
the harness had already validated that state. [Second-checkpoint receipts](evidence/second-checkpoint/manifest.json)
retain the timeout, final passes, focused follow-ups, and source hashes.

The offline [migration inventory](../../../../nativelink-config/examples/migration_inventory/README.md)
reports legacy construction paths and blocking decisions without opening stores
or choosing destinations. It has 12 passing Cargo/Bazel tests and bounded input,
traversal, and graph work. Cargo and Bazel CLI outputs match and leave the input
unchanged. Every report refuses deployment; this is an inventory aid, not the
final migration converter or removal of the Redis dependency.

Remaining engineering includes finite recipient-ledger
compaction, replacement-incarnation fencing, feature activation and compatible
writers, canonical ingress/listeners/effect drivers,
generation-aware physical stores, actual typed closure protection, sustained
collection, migration/cutover, and deletion of the legacy construction paths.
The experimental snapshots explicitly reject earlier prototype versions; that
is not an upgrade or rollback protocol. None of M0 through M9 is declared fully
qualified by this checkpoint.

## Evidence currently missing

| Item                                                              | Why it matters                                                                                                                            | Work that continues without it                                         |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Original Lane B harness location/revision and hosts               | Required unchanged historical baseline; its reported rate is not a capacity target                                                        | Independent model, harness, synthetic experiment with its own identity |
| Runnable rechaos deployment prerequisites                         | Pinned soak interface and Lean artifacts inspected; available fault selection excludes Execute/WaitExecution and does not prove ownership | Native deterministic scenarios and real protocol tests                 |
| Representative workload distributions                             | Closure tails, hot roots, direct updates, progress traffic and supported load envelope gate format freeze                                 | Labeled bounded synthetic measurements                         |
| Azure qualification account/topology and provider lifecycle facts | Emulator tests cannot certify actual deletion, late-write, durability or throttling assumptions                                           | Mock/Azurite adapters, fault fixtures, generation protocol             |
| Actual rollout listener/network/credential topology               | Must demonstrate worker exclusion from every writable AC route and namespace alias separation                                             | Local mTLS/listener tests and blocking migration fixtures              |
| Legacy data/placement and desired migration destinations          | Converter cannot choose destinations or grant collector ownership                                                                         | Offline structural parser/diff and cold-start manifests                |
| Required serving/reconnect windows and workload deadlines         | Timing/space calibration and supported guarantees                                                                                         | Conservative logical-time model and explicit unqualified constants     |

Search provenance: repository text and filenames, plus accessible `/home/b7r6`
filenames, did not locate a Lane B harness or directly named rechaos checkout.
Root subsequently located `/home/b7r6/src/hypermodern/hypermodern-nativelink-0x01`.
Its README describes `nix/soak-rig.nix`, vendored `nix/soak/` workload/collector,
and a pinned rechaos dependency. README lines 183–189 explicitly say
Execute/WaitExecution are not fault targets in the referenced `validFault`;
do not claim this supplies worker/consensus fault coverage. The pinned rechaos
source was subsequently inspected: its nine valid fault families cover CAS, AC,
and ByteStream, while the Lean model concerns fault selection, probability, and
replay. It does not establish execution ownership. No live soak fleet was
contacted. No Lane B benchmark was reconstructed
and no historical result was relabeled as a local measurement.

Subsequent process-list inspection exposed a separate active legacy soak under
`/home/b7r6/soak/nl-oss-head-2026-10-22`. Only existing archived findings and script
text were read; no scripts, fleet controls, Redis queries, or network requests
were executed. [Observation receipts](evidence/legacy-soak-observation/manifest.json)
preserve the short source notes and byte hashes. The reports include Redis index
and subscription prefix failures, a scheduler-state stall, and loss of a completed
worker candidate after coordinator death. The summary records a 3,663-second
recovery, but its restart timestamp conflicts with the narrative notes and the
archived raw logs end before the final retry. These are incident reports from
several legacy binaries, not a Lane B identification, a new benchmark, or evidence
that this branch repaired the running fleet. Mounted reconnect, automatic
candidate resubmission, timer reconstruction, and scheduling wakeup still need
real runtime fixtures.

The plan's pinned public commit was initially absent, then fetched by root.
[source-map.md](source-map.md) records the observed listing/test drift against
the actual starting tree.

## Decisions made provisionally

- Use redb for the durable journal candidate behind exactly OpenRaft 0.9.25
  split storage interfaces; root owns exact dependency resolution and evidence.
  Durable commit must precede successful flush notification. Qualify crash,
  snapshot, and activation ordering before claiming durability.
- Keep typed resource requirements and strict exact properties. Unknown legacy
  type/rewriting rules block migration; no operator algorithm selectors return.
  [resource-mapping.md](resource-mapping.md) records whole-core `cpu_count`,
  KiB `memory_kb`, mandatory one-slot reservations, and migration blockers.
- Preserve Buck2 scope as inflight compatibility, excluding it from the AC key;
  preserve `single_use` as an incarnation lifetime fact. Existing capture
  integration needs a separate lifecycle regression inventory.
- Evaluate sealed-cohort collection with temporarily fenced new protection
  acquisition and live-root renewal without mark restarts. It is a candidate
  with explicit admission/space/latency costs, not a frozen production format.
- Start new AC view/sink namespaces cold unless finite imports validate complete
  roots and ownership. Unknown old writes stay isolated; old retained results
  cannot silently lose their obligations.

## Counterexamples to preserve as fixtures

1. Azure cleanup observes no committed block list; another upload commits to
   the same digest name; cleanup deletes that name. Current cleanup's fixed
   sleep is not generation fencing.
2. A filesystem catalogue restarts empty and reuses a generation number while
   an old delete is outstanding. A store incarnation that is never reused must prevent aliasing.
3. A worker publishes before scheduler acceptance, is revoked, and its stale
   completion changes AC/history. M4a removes this bypass.
4. A accepted sink write is uncertain; B is accepted/written; A finishes late.
   M4 permits the sink result with stable operation terminals; M6 must preserve
   the authoritative latest head.
5. A cache hit grants protection, then its head is removed before output read.
   Previously granted serving coverage survives head replacement.
6. Hot renewals keep restarting global marking, so cold unreachable objects
   never become collectible under otherwise supported load.
7. An expired root in a stale index renews without closure validation after one
   of its required generations retired. Reacquisition cannot revive it.
8. A new incarnation advertises full capacity while old processes still run.
   Replacement identity alone cannot refund their charged resources.

## Reporting discipline

Distinguish source inspection, implementation, local model evidence, synthetic
measurement, actual adapter qualification, and deployed boundary validation.
Missing telemetry blocks unknown-workload claims and format freeze, not useful
independent work. No document here marks M0, M1, or any later gate passed. Update
the root-owned evidence with exact commands and actual coverage before reporting
a completed gate; retain the smallest unresolved counterexample and next patch
boundary. No production service has been changed by this documentation task.
