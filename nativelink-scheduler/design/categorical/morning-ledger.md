# Morning ledger

Started 2026-10-22; updated through 2026-10-23. Questions and external dependencies
are collected here so independent implementation can continue without interrupting
the user. This is not a request
for approval of routine engineering choices. User authorization superseded the
draft's M1 stop; qualification, production-format, deployment, and merge boundaries
remain distinct.

Latest full integration at `4876ddd48c`: Cargo passed 2,007 tests with zero failures
and eleven ignored cases; all 166 Bazel targets passed, including the two manual
canonical-config targets. Of those targets, 134 executed and 32 were cached.
The integrated listener binary also passed all four explicit local mTLS cases.
[Workspace receipts](evidence/authority-construction-workspace/manifest.json)
record the exact commands, 475 unchanged source/build inputs and binary hashes.
Later standalone package changes passed their separate manual targets: nineteen
parser/converter cases and thirteen namespace ownership cases. Their
[integration receipt](evidence/canonical-ownership-integration/manifest.json)
retains an initial Bazel rule error and the corrected build. Final formatted
fixtures also pass both Cargo packages and all three manual Bazel targets;
[final receipts](evidence/canonical-ownership-final/manifest.json) separate that
source boundary. These additions do not inherit the earlier full-workspace totals.
The preceding integration passed all 22 selected Bazel targets across the service crate,
publication candidate/comparisons, physical fixture, and worker publication.
[Integration receipts](evidence/construction-integration/manifest.json) retain
the exact source and an initial library lint failure. Authority-only construction retains no legacy publication
handle. Its 163-case Cargo regression and all 17 worker Bazel targets pass;
[construction receipts](evidence/authority-construction/manifest.json) separate
the source boundaries. The preceding full workspace checkpoint recorded
1,979 full-workspace Cargo passes, then 13 passes for the
additive terminal comparison, and all 163 integrated Bazel targets passed. The
[receipts](evidence/publication-terminal-workspace/manifest.json) separate the
source checkpoints and ten ignored Cargo cases. Production factories remain
legacy and every categorical gate remains open. Start with
[next work and missing facts](#next-work-and-facts-for-the-morning).

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

A later recovery review found that worker acceptance consumed the only dispatch
identity record. Pure-state snapshot version 5 now retains that identity with
its attempt, including after acceptance and coordinator restart. The public
ingress watermark also survives terminal retirement, so a serialized leader
can allocate without reusing retired operation handles. Neither read method grants
writer authority; ingress still needs a read barrier and commit correlation.
Core tests and the independent oracle compare the new identity association.

The [physical filesystem experiment](physical-filesystem.md) adds exact
incarnation/generation effects and a durable finite retention bridge. Ten
physical tests, three bridge tests, and all four affected Bazel targets pass.
Three semantic mutations were detected. The [local listener experiment](listener-boundary.md)
uses the actual binary and temporary certificates: the isolated topology passes,
while shared trust, a worker-accessible AC route, and aliased object/sink stores
are each detected as unsafe. This is local topology evidence, not deployed
boundary qualification. [Recovery and physical receipts](evidence/physical-recovery-checkpoint/manifest.json)
retain these focused checks; the earlier full workspace result remains scoped
to its own checkpoint.

The cluster suite now passes ten tests on Cargo and Bazel, including volatile
recipient loss during snapshot transfer and leader loss after joint membership
commits. Both compositions preserve pending authority; the old durable leader
also converges after restart. [Composed recovery receipts](evidence/composed-recovery/manifest.json)
record the exact fixture boundary and its initial cutpoint correction.

The [format activation experiment](format-activation.md) now selects compatible
writers from committed feature state and refuses unsupported committed recovery.
Ten new tests cover real quorum activation, mixed pending authority, committed
suffixes, snapshot cutpoints, and reconstruction of the trusted admission
fixture. That checkpoint used immutable reader profiles in one executable;
the later separately compiled Base fixture qualifies the declared experimental
binary boundary.

The [automatic coordinator](automatic-coordinator.md) drives actual Execution and
worker RPCs, matching, deadlines, reconciliation, and accepted-only publication.
Five wire tests include completion during coordinator loss, reconnect with the
same recipient, stable terminals and later cache hits, cancel before dispatch,
report identity checks, bounded streams and oversized-result projection. The
[typed output closure](typed-output-closure.md) validates exact physical objects,
embedded Tree records, opaque leaves, and bounded DAG traversal in fourteen
tests. A complete verified manifest is still evidence, not a protection grant.
[Integrated receipts](evidence/activation-runtime-closure/manifest.json) retain
the commands, source identities, and outcomes for this combined checkpoint.

Four mutations of the automatic RPC adapter fail its named wire assertions.
The full workspace run also exposed two corrections: Bazel Clippy requires the
default boxed BLAKE3 constructor, and filesystem tests that inspect a global
file-descriptor budget need exclusion from other tests using that budget.
The fixture now uses shared guards for ordinary tests and exclusive guards for
global-budget checks, preserving concurrency within each upload history.
Failed checks remain in the receipts alongside the corrected runs. Cargo reports
1,627 passes, zero failures and five ignored tests across the workspace; Bazel
passes all 141 targets. The later filesystem corrections also pass their focused
Cargo checks.

The next focused checkpoint adds nine RPC histories for active observation
renewal, including paused-reader transport backpressure and recovery after a
committed clock advance precedes expiry. Seven separately compiled frozen Base
cases pass against the pinned envelope-3/core-5 baseline. The typed retention
bridge passes eleven tests and composes complete manifests with durable cohort
protection and separate serving roots. Six compiled semantic mutations of that
bridge fail their assertions; a later cross-namespace grant test also failed
before the namespace binding was repaired. [Focused receipts](evidence/renewal-retention-frozen/manifest.json)
retain these results and the exact source boundaries. The bridge is a bounded
singleton test wrapper, and transport renewal does not prove that a remote
application consumed a message.

The next checkpoint adds [durable receipt retirement](receipt-authority.md) and
its [automatic RPC delivery](receipt-retirement.md). The authority records one
immutable pending certificate per worker incarnation, preserving every uncertain
attempt and the existing finalization budget. Exact acknowledgment permits
bounded recipient compaction without refunding authority resources. Independent
comparison covers six real-State histories; its separate finite model explores
739 states and 6520 transitions. Snapshot review preserves four malformed
certificate cases and twelve coordinated zero-ID corruptions as regressions.

Integrated Cargo checks pass 64 library, 15 format, seven reference, and six
differential tests. Eighteen selected receipt and ingress mutations compile and
fail their named assertions, with restored baselines passing. Actual RPC checks
cross issuance, delivery, and acknowledgment restart cutpoints, then complete
16 processes beside an old active attempt with a four-receipt limit. The longer
48-completion run has separate source provenance. These are bounded fixtures,
not workload capacity measurements.

The first integrated Bazel run exposed a completed worker candidate waiting
behind obsolete inventory snapshots while the coordinator continued committing.
The service now coalesces complete snapshots before journal processing and avoids
redundant accepted/progress/capacity commits. Standalone reports seal preceding
snapshots and preserve ordering barriers. Causal tests cover a blocked consumer
with 256 incoming inventories and updates arriving during a commit. The unchanged
16-completion Bazel history now passes in 33.3 seconds; its earlier failed
167-second run remains in the evidence. A separate format-suite timeout under
heavy host load passes serially with its original timeout. [Checkpoint receipts](evidence/receipt-retirement/manifest.json)
record these distinctions and source hashes.

Three actual RPC [publication histories](publication-ordering.md) also pass:
accepted A finishes after B and may overwrite the temporary sink, a revoked
candidate creates no publication, and a direct client AC update races with an
accepted worker effect. Original terminal Operations remain stable. The controlled
sink models an external ambiguous request; this is not M6 ordered-head behavior
or accepted-history relocation.

The committed receipt writer `b2d92025` passes seven frozen Base cases with
feature 2 alone and seven with features 1 and 2 together. The Base revision and
decoder-removal overlay remain unchanged. Before activation, the old executable
recovers exact state and can perform a quorum read; after activation it refuses
authority participation before reading a vote. This qualifies the experimental
envelope-3/core-5 compatibility contract, not released deployments.

The [replicated RPC fixture](replicated-rpc.md) now runs the same coordinator over
three durable journals. A minority cannot return a committed operation handle;
the successor preserves an accepted terminal and exact pending publication, and
the same live worker reconnects with its full completion candidate. Both selected
adapter mutations compile and fail; the restored run and integrated Bazel target
pass. Routing is explicit fixture forwarding, not fleet leader discovery.

The [Azure physical fixture](physical-azure.md) exercises the existing SDK against
Azurite 3.37 with immutable generation names and conditional requests. Six compiled
mutations fail, and the explicit integrated Bazel emulator run passes. It covers
lost upload acknowledgment, verified ranges, incarnation isolation, stale `ETag` values,
and missing-object versus missing-container errors. An emulator DELETE with a
wildcard absence condition proved unsafe; the adapter issues DELETE only with an
observed `ETag`. A test-only runner override fixes Bazel's relative crate path.
Ordinary suites intentionally ignore emulator qualification. These results do
not qualify a cloud account, distributed ownership, or retention authority.
[Integration receipts](evidence/replicated-azure-frozen/manifest.json) preserve the
source hashes, prerequisite failures, and successful runs.

Workspace verification of `552aaa89` passes 1,690 Cargo tests with zero failures
and eight ignored tests, across 154 result summaries. The broad Bazel run passes
146 of 148 targets. One format fixture's 50 ms quorum read fails transiently and
the worker-manager suite exceeds its unchanged short timeout. Focused reruns with
eight test threads and one concurrent test target pass both, in 3.8 and 25.7
seconds respectively. This is combined coverage from the broad run and focused
reruns, not a single 148-target pass. [Workspace receipts](evidence/receipt-workspace/manifest.json)
retain both failures and the passing commands.

An independently written [accepted-history reference](../../tests/accepted_history/README.md)
now specifies fixed publication policy, exact diagnostic bytes, qualified logical
namespace, prepaid metadata and effect identities, and pending retirement pins.
Fourteen tests pass under Cargo and Bazel; eight compiled oracle mutations fail,
and the restored baseline passes. The final contract preserves zero-valued caller
identities while requiring nonzero allocated IDs and exact authority epoch.
[Reference receipts](evidence/accepted-history-reference/manifest.json) identify
the frozen source. Actual reducer comparison, journal activation, and scheduler
effect delivery remain separate work.

Remaining engineering includes canonical integration of recipient compaction,
replacement-incarnation fencing, deployed format admission, canonical
construction and authenticated listeners, qualified physical stores and their
replicated authority integration, sustained collection, migration/cutover, and
deletion of the legacy construction paths. Experimental envelope 3 rejects earlier prototypes; its
activation protocol does not migrate released deployments. None of M0 through
M9 is declared fully qualified by this checkpoint.

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
that this branch repaired the running fleet. The later automatic coordinator
fixtures exercise reconnect, candidate resubmission, timer reconstruction, and
scheduling wakeup over actual local RPCs; production construction and the active
fleet still use the legacy runtime.

The plan's pinned public commit was initially absent, then fetched by root.
[source-map.md](source-map.md) records the observed listing/test drift against
the actual starting tree.

The existing typed-retention composition now runs against the actual Azurite
SDK fixture through a verified-read and physical-effect seam. Its catalogue,
checkpoint, grant checks, and independent retention oracle are shared with the
filesystem fixture. The provider run records 33 SDK requests and three
conditional deletes; a successful delete with a lost acknowledgement remains
pending across reopen until absence reconciliation commits its completion.
Six compiled causal faults are detected and the restored baseline passes.
Integrated Bazel passes the provider case and both unchanged filesystem suites.
[Receipts](evidence/azure-typed-retention/manifest.json) retain failures and
source hashes. This remains a singleton test composition, without production
construction, cloud-account qualification, or a completed M5 gate.

The experimental accepted-history core now records one fixed diagnostic recipe
at accepted worker completion, including noncacheable and final failed results.
Its metadata and identity budget is prepaid; pending delivery pins retirement,
and acknowledgement cannot alter the accepted terminal. An independent model
and real-State adapter cover the contract, with eight compiled core faults
caught. Feature-3 journal activation adds proposal ordering and staged preflight;
independent review exposed two concurrency races that are now regression cases.
Nine compiled format faults are detected. Integrated RPC delivery now checks both digest algorithms through a verifying
CAS, exact retry after lost acknowledgement, failure, stale exclusion, and cache
hits. Six compiled delivery faults are detected. Ordinary storage effects and
history share four slots with rotating selection. The frozen Base reader now passes seven boundaries each against the original
direct writer, the new history writer, and the writer activating all three
features. Base sources and its binary hash remain unchanged. Production factories and
worker history configuration are unchanged, so this does not close M4a.2. [Design and receipts](accepted-history.md) record exact scope.

An independent finite M4-to-M6 activation reference is now frozen with fifteen
cases and twelve detected semantic faults. It requires explicit disposition of
retained old intents, original acceptance provenance, and verified protection
for every result/history root in its worker-accepted terminal universe. A later
[scope audit](publication-activation-scope.md) found that its provenance type
cannot represent retained cache-hit terminals. Those operations and their output
roots remain an activation blocker; the frozen files and evidence are unchanged. It separately records opaque old-domain
exposure: an empty outbox cannot prove that forgotten duplicate requests stopped.
Missing closure evidence refuses activation; a quarantine label does not waive
protection. [Reference contract](../../tests/publication_activation/README.md)
and [receipts](evidence/publication-activation-reference/manifest.json) state
its scenario-owned evidence assumptions. No candidate activation implementation
or production M6 switch follows from this reference alone.

The shared typed-retention composition now also covers BLAKE3 through actual
Azure SDK requests, with SHA256 objects coexisting in its catalogue. Preparation
selects the result algorithm and rejects a required edge from another algorithm.
The BLAKE3 provider history records 37 requests and three conditional deletes;
three compiled algorithm faults are detected. Unknown restored algorithms still
refuse. [Receipts](evidence/azure-blake3-retention/manifest.json) also preserve a
flaky checkpoint test whose random identity byte was sometimes already zero;
its corruption now always changes that byte.

The accepted-history checkpoint passed the full Cargo workspace: 1,745 tests,
zero failures, nine ignored. After the activation reference, BLAKE3 retention,
and real-process history additions, all 153 Bazel targets pass. The first two
Bazel attempts retain their failures: two suite timeouts, a Rust-only argument
sent to a protobuf shell test, and the unchanged-byte corruption fixture. A
later build caught a formatting difference before running tests. The maximum
snapshot case now has a separate moderate-timeout target, and the worker suites
have a moderate allowance; their assertions and internal deadlines are unchanged.
[Workspace receipts](evidence/history-workspace/manifest.json) identify each
source boundary and keep all attempts. Provider tests remain separate explicit
runs; an ignored provider case is never counted as provider qualification.

The [durable history budget](history-budget.md) now has 24 passing fixed cases
with both algorithms, widths 4/16, and valid response payloads up to 32 KiB stdout.
Default admission falls from 64 to 56 executions after feature 3. A full base
authority refuses activation after a committed Abort without changing scheduler
state. All cases preserve exact terminals through snapshots and reopen, retain
pending-history pins, and drain after exact synthetic acknowledgements. These
are measured encodings and shared-host timings, without provider or workload
capacity claims.

The independent [publication/head reference](../../tests/publication_head/README.md)
is frozen with 18 passing cases and 16 detected compiled faults. It covers
atomic acceptance obligations, original terminals, current-worker authorization,
ordered heads, request-policy distinctions, and retained serving coverage. The
[response audit](publication-head-response-scope.md) limits its opaque response
model: concrete cached ExecuteResponse projection needs separate expected bytes.
The later [publication composition](publication-composition.md) now combines real
State and Coarse retention. Its independently authored comparison checks accepted
order, stable terminals, cache projection, request policy, live coverage, and
recovery under an explicit common lease alphabet. The comparison exposed a
wrong-action staging admission that consumed another execution's reserved slot.
That and a late-predecessor variant are fixed before credit consumption. Complete
payload and cache-projection limits now match the actual scheduler's encoding.
Twenty-two focused candidate cases pass; finite capacity witnesses fit below
2 MiB, without a general future-byte reserve claim.

[Physical composition](publication-physical.md) now stages real filesystem
generations before writes, verifies exact reads through typed closure preparation,
and preserves old operation and serving reads after head replacement. Eight new
SHA256/BLAKE3 scenarios and the imported retention regression pass in Cargo and
Bazel. These variants qualify physical object algorithms; the original fixture
still uses a synthetic scheduler request and does not establish production ingress.

The activation-v2 successor retains separate operation/execution terminals and
unknown provenance. A valid transition exceeded its 512 KiB snapshot cap; an
additive guarded wrapper now refuses such transitions atomically, with original
frozen evidence preserved. The [actual-State inventory](../../tests/activation_inventory/README.md)
retains exact source snapshots and payloads, checks captured principal and complete
namespace identity, and projects independently prepared terminal/history roots.
Sixteen Cargo/Bazel cases and twelve detected compiled faults cover this adapter.
It does not perform durable activation, convert unfinished work, or infer that
old requests stopped.

Real BLAKE3 process histories also exposed a missing action-scoped digest context
in the authority worker. Verified inputs launched, but result upload inherited
SHA256 and failed verification. The worker now scopes its action lifecycle to the
request algorithm. Both algorithms pass noncacheable-success and nonzero-exit
process histories, serially and concurrently, plus the integrated worker/RPC
regressions. [Receipts](evidence/worker-history-blake3/manifest.json) retain the
failing process run and exact correction. Production factories remain unchanged.

The [publication workspace checkpoint](evidence/publication-workspace/manifest.json)
passes the complete Cargo workspace with 1,866 tests, zero failures and ten ignored
cases, and all 160 Bazel targets. All 431 recorded source and build files remain
unchanged across those runs. Independent review then found that the inventory
and new real-request adapter must also validate each joined operation's original
request. That correction and a real-to-synthetic snapshot-mode refusal were
qualified at the following checkpoint; these workspace totals exclude those later changes.

The [real-request correction](publication-real-request.md) now passes its
integrated Bazel checks: 30 candidate, nine physical, six independent-comparison,
18 corrected inventory, and 11 request cases. Every joined operation retains its
own captured request and trusted principal binding; removing real bindings cannot
downgrade captured requests into synthetic mode. The original failing snapshots
and independent review are preserved with the separate correction receipts.

The [actual-worker publication fixture](publication-worker.md) now connects
both SHA256 and BLAKE3 namespace processes to real staged filesystem writes,
typed preparation, and publication in the original scheduler State. All seven
worker bridge cases pass in Cargo and Bazel, with no skipped process case.
Independent expected edges, exact response bytes, joined operation reads, cache
hits and checkpoint reopen are checked. The worker's temporary upload CAS stays
outside the collector namespace and its report remains unacknowledged; this is
not yet the complete canonical gateway or M5 acknowledgment path.

The independent publication comparison now also admits captured SHA256/BLAKE3
requests into the real constructor. All eight cases pass in Cargo and Bazel;
strict Clippy passes after an explicit Timestamp default spelling correction.
The [additive receipts](evidence/publication-real-differential/manifest.json)
retain both source boundaries. The frozen oracle is unchanged, and physical
readiness remains an explicit fixture assumption in this comparison.

An additive [revoked-worker case](evidence/publication-worker-late/manifest.json)
now prepares real outputs before revocation, then proves unchanged publication
refusal for both algorithms. The old reservation remains charged until the
identity-checked quiescent report supplies Cleanup. A fresh attempt owns a
distinct reservation; old stage/write records survive that retry and checkpoint
reopen. All eight worker bridge cases pass in Cargo and Bazel with no skipped
process case. No second process or worker acknowledgment is implied.

The [direct-client grant extension](evidence/publication-direct-client/manifest.json)
adds explicit trusted permission for the complete registered action binding,
without creating a worker attempt. Revocation blocks future staging and
acceptance while preserving earlier accepted heads and serving protection.
Review reproduced six impossible retained-position collisions across both digest
algorithms; the correction enforces one recorded event per retained commit
position. Five compiled faulty variants are detected. The integrated candidate,
comparison, physical and worker suites pass with 38, eight, nine and eight cases.
Private snapshot v3 remains separate from durable accepted-history feature 3.

The separately frozen [terminal-outcome reference](terminal-outcome-reference.md)
now covers accepted responses with `result=None`, memory retry versus final
failure, per-operation cancellation, explicit expiry, and retained storage
obligations. It reuses the unchanged independent M1 and retention oracles.
Twenty-two new cases and seven inherited cases pass in Cargo and root Bazel;
all twelve compiled faulty variants are detected. Frozen Rust hashes and the
original evidence archive remain preserved in the
[integration receipt](evidence/terminal-outcome-reference/manifest.json).
Full pre-commit exposed an entry-point-only formatter check; a separately recorded
formatting delta now passes explicit child checks, full hooks and repeated root
Bazel. Two integrated Rust hashes changed, with original mutation evidence intact.
At that checkpoint, comparison against the successor candidate was still pending. Graph-bearing
resource failures, active history, physical collection, durable publication
formats and production cutover remain outside this checkpoint.

The separate [terminal restore audit](terminal-outcome-restore-audit.md) found
eight compiled counterexamples against the original frozen oracle. The corrected
private v2 reader checks local memory evidence, actual attempt count, immutable
grant growth and report ordering; v1 snapshots are refused without invented
history. Cargo and root Bazel pass the 29-case original target and 44-case audit
target, comprising 44 unique cases because the audit embeds all original cases.
All 22 compiled faulty variants are detected. The
[corrective receipt](evidence/terminal-outcome-restore-audit/manifest.json)
preserves the original eight failures and original 29/12 qualification separately.
Both sides froze before the new comparison; full historical worker eligibility
and production activation remain outside this correction.

The [private-v4 terminal candidate](publication-terminal.md) now accepts exact
no-graph Complete and MemoryFailure reports while leaving retry classification
and physical refunds to actual State. Cancellation, fenced expiry and exhaustion
close unused logical credit while retaining stages, writes and uncertain charges.
Review reproduced and fixed locally contradictory restore evidence, older-format
downgrades, and duplicate write observations that invalidated preparation.
The source was frozen before reading the new terminal oracle. Cargo passes
54 candidate, eight existing comparison and nine physical cases; integrated
Bazel also passes all eight worker cases. The later
[candidate mutation campaign](evidence/publication-terminal-mutations/manifest.json)
compiled ten variants and detected nine. Removing only the snapshot byte-length
refusal survived all 54 cases, so aggregate-overflow coverage remains explicitly
missing. All source hashes were restored before the 54/8/9 baseline passed again.
The corrected terminal-oracle comparison and broad workspace refresh have
separate receipts below. Production role construction and every categorical
gate remain open.

The [terminal comparison](terminal-outcome-differential.md) now checks the frozen
candidate against the corrected independent reference. Five new paired histories
exercise both digest algorithms; eight inherited component cases also run in the
13-case target. All pass in author and integrated Cargo/Bazel runs. All three compiled
faulty variants fail unchanged assertions, and all 25 engine/support hashes were
restored. Every command compares scheduler state, each original operation request,
report provenance and retained storage relations, then restores both engines
before the suffix continues. Native commit counts and refusal categories retain
explicit limits; the adapter does not invent oracle graph semantics. The
[separate receipts](evidence/publication-terminal-differential/manifest.json)
preserve the first observer-serialization and replacement-quarantine fixture
failures, the corrected source, and independent review.

The [final workspace receipts](evidence/publication-terminal-workspace/manifest.json)
record 1,979 Cargo passes, zero failures and ten ignored cases at commit
`a6cf4c1213`, with all 493 source/build files unchanged during that run. The new
comparison was then added and passed its separate 13-case Cargo run. All 163
Bazel test targets passed on that integrated state, with 52 executed and 111
cached; all 497 recorded source/build files stayed unchanged. The only code/build
delta between those checkpoints is the new four-file test harness and its Bazel
target. Those totals are executed test cases/targets, not unique proofs or
provider qualification; inherited tests and ignored provider cases stay explicit.

[Authority worker construction](authority-construction.md) now accepts only
execution/CAS inputs and holds no legacy AC/history publisher. Accidental calls
to the old publication method refuse before changing the result. Six actual
process histories cover both hash algorithms and three cache policies, accepted
history through restart, and delayed accepted AC effects. The 163-case Cargo
worker regression passed; after a logging-only lint correction, all 17 worker
Bazel targets passed. Existing legacy-enabled fixtures and production factory
behavior remain intact. Fixed listener roles and strict canonical configuration
are the next construction boundaries; this does not close M4a.1 or M4a.2 in the
product.

[Operations RPCs](authority-operations.md) now read, wait for, and cancel retained
operation handles through the committed authority. One caller's cancellation
preserves another caller's coalesced execution and accepted AC result. Exact
terminals survive restart. Wait durations include the initial commit; review
caught and corrected that missing bound. Full authority RPC Cargo passes 17
cases, service units pass six, and focused Bazel targets pass. The fixed listener
mount is integrated separately; production construction remains unchanged.

The [snapshot byte bound](publication-snapshot-bound.md) now accounts for at most
2,022,804 bytes under the current finite schema, below 2 MiB by 74,348 bytes.
The missing premise was outer restore: it accepted valid child retention states
outside its own exposed transitions. Restore now rejects those states. Four new
cases reproduced prior acceptance; all 58 candidate cases pass. The bound
explains the surviving guard mutation within this reviewed domain, without
lowering caps or claiming a new mutation kill. It is not a production memory or
future retry-capacity proof. [Author receipts](evidence/publication-snapshot-bound/manifest.json)
preserve the reproduced failures and per-field calculation.

The [fixed listeners](authority-listener-construction.md) now mount complete
client transfer routes, Capabilities, and committed Operations alongside
Execution and AC. Worker routes expose only the authority Worker API, CAS and
ByteStream. The same immediate store handle cannot occupy both object and sink
roles. All 22 selected integration targets pass, including the full service
crate and an additional client-only Operations case. The library-wide lint run
also caught a wildcard import omitted from the earlier focused target coverage;
explicit imports fix it.

The [fixed-listener mTLS qualification](authority-listener-tls.md) now exercises
both hash algorithms with isolated trust and deliberately shared trust. Worker
credentials fail on the isolated client AC route while authorized client writes,
worker transfers, accepted publication and later cache hits succeed. The shared
trust control admits the worker write and is classified unsafe. All four cases
pass again against the integrated Bazel binary. This covers every client listener
in the one-client-listener fixture, not deployed ingress or credential placement.

The [canonical schema and offline converter](../../../../tools/canonical-config/README.md)
now form a standalone package without runtime or Redis dependencies. Seventeen
Cargo tests and both manual Bazel targets pass. The parser refuses legacy keys,
duplicates, interpolation, unknown fields and bounded-input violations. The
converter accepts only its narrow direct-filesystem source shape and requires
explicit ownership, encoding, placement, trust and fencing declarations. Unknown
or unsupported facts emit blockers with no candidate; even complete proposals
remain unqualified for deployment. [Author receipts](evidence/canonical-config/manifest.json)
retain actual legacy-schema acceptance, source/dependency hashes and successful
and refused CLI syscall observations. No runtime consumes this format yet.

The [deadline driver repair](authority-deadline-driver.md) fixes two reproduced
failures. A restored due-interest sweep now commits before the first queued
observation; a retained running deadline no longer masks upload progress expiry.
The final fixtures each fail when their specific fix is omitted, then pass the
repaired driver. Actual worker RPCs retain the resource charge through revocation
until matching cleanup; their manual cleanup is protocol evidence, not physical
process termination. [Author receipts](evidence/authority-deadline-driver/manifest.json)
preserve both failures, independent omission controls, final passes and exact
validation overlays. No reducer, request projection, format or clock policy changed.

The [parser adversarial campaign](evidence/canonical-parser-adversarial/manifest.json)
ran 2,253 frozen cases in 4,506 fresh processes using both the main stack and
a 2 MiB thread stack. No case crashed, timed out or accepted a legacy key.
The parser implementation stayed unchanged; two added regressions check escaped
key equivalence and precise integer identities. The pinned reader's conservative
integer limits are documented. Eighteen private binary visibility warnings were
also removed; the package now passes nineteen tests and strict lint checks.

The isolated [filesystem ownership library](../../../../tools/canonical-filesystem/README.md)
checks four existing roots before cold initialization, holds cooperative directory
locks, writes role/domain records relative to pinned descriptors, and refuses
partial or conflicting records. Thirteen cases pass on Cargo and Bazel. An
[independent fourteen-case review](evidence/canonical-filesystem-review/manifest.json)
also reproduced two limits: directory inode reuse can admit a copied old record,
and all records can remain readable after a final sync fails. The API and docs
now distinguish observed identities from durable physical generations and readable
records from historical initialization success. These are runtime blockers, not
qualification of copied namespaces or power-loss recovery. A path-based store
still cannot safely consume this guard; no runtime calls it.

The [clock proposal](authority-clock-proposal.md) records another construction
prerequisite. A persisted monotonic base and fresh origin can conservatively
exclude downtime, but integer rounding and stale samples can expire requested
durations early. Fresh sampling, deadline comparison, overflow and bootstrap
compatibility need explicit qualification. The proposal changes no runtime clock.

The [prepared memory-report candidate](publication-prepared-memory.md) now applies
actual MemoryFailure to a prepared result graph. Retry preserves staging and write
obligations; final failure protects only the operations receiving that result and
never changes the AC head. Its 72-case target and strict Clippy pass. The
[candidate receipt](evidence/publication-prepared-memory-candidate/manifest.json)
records the source and test freeze before the new independent reference was read.
All four affected integrated Bazel targets executed and passed; their
[separate receipt](evidence/publication-prepared-memory-integration/manifest.json)
pins 141 unchanged scheduler source/build inputs.
The [independent byte review](prepared-memory-bound-review.md) confirms the
2,026,948-byte generated snapshot bound under the unchanged 2 MiB cap.
Independent comparison and compiled faults remain separate qualification work;
the private candidate adds no production routing or accepted-history activation.

## Next work and facts for the morning

1. Maintain the finite snapshot bound when fields, caps, or exposed transitions
   change. The current bound and repaired restore premise explain the surviving
   guard mutation. Future byte headroom and retry-stage capacity still need their
   own argument.
2. Resume the plan's M4a.1 through M4a.4 product sequence: canonical role
   construction, removal of legacy worker publication/history bypasses, fixed
   listener isolation, and strict configuration refusal plus the offline
   converter. The parser, converter and local mTLS fixture are implemented;
   physical namespace preflight, startup refusal ordering and the fixed clock
   still need combined construction. Existing fixtures qualify parts of these behaviors; production
   factories still select the legacy runtime. Unknown migration destinations must
   remain explicit blocking output from the converter.
3. Measure combined execution, publication, lease and retirement demand, and
   finish the canonical worker/gateway acknowledgment path. Local composition and
   synthetic budget work can proceed without production telemetry. Later M6
   work includes graph-bearing resource failures, protected accepted history,
   durable publication formats and activation suffixes; sustained collection and
   safe deletion of old runtime choices remain later gates.

Production qualification needs representative workload distributions and the
Lane B baseline; intended listener/trust and credential boundaries; ownership of
object, journal and legacy sink namespaces; filesystem placement and Azure
lifecycle assumptions; existing data/migration destinations; and supported
serving, reconnect and deadline guarantees. These facts are saved for the morning.
They do not block all independent implementation, and no missing fact is replaced
by a guessed production default.

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
boundary. Production factories and listeners still construct the legacy runtime;
this checkpoint does not establish production cutover.
