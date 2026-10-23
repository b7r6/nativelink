# Morning ledger

Started 2026-10-22; updated through 2026-10-23. Questions and external dependencies
are collected here so independent implementation can continue without interrupting
the user. This is not a request
for approval of routine engineering choices. User authorization superseded the
draft's M1 stop; qualification, production-format, deployment, and merge boundaries
remain distinct.

Latest focused checkpoints add exact fixed upload identity, an inert canonical
journal owner, fixed listener
digest defaults, ByteStream read
producer completion, synchronized namespace commitments, bounded fixed CAS and AC
admission, digest context for
captured requests and replayed effects, private canonical journal binding,
membership and persisted-limit preflight, and memory-breach reporting.
Their receipts separate independent source review, original failures, author
fault controls and root integration. Production construction, combined admission
and the complete canonical clock contract remain open.

Latest full Bazel checkpoint at `ef57a7deae`: all 172 targets passed in the
reduced-concurrency run, including six explicitly selected manual canonical
targets. 172 executed and 0 were cached; the run took 1324.52
seconds with 1,332 unchanged tracked inputs. The first run passed 171 targets
and failed one worker fixture before action startup: the singleton's five-second
leader wait expired. The unchanged-source rerun used one test process and one
Rust test thread at a time. No timeout was extended. Both runs and the startup
availability residual are retained in the
[receipt](evidence/canonical-admission-checkpoint/manifest.json); passing under
reduced concurrency does not prove load independence. The
[previous checkpoint](evidence/canonical-construction-checkpoint/manifest.json)
at `e6978139cf` remains a separate source boundary. Later changes do not inherit
these full-run totals.

The later focused checkpoint at `d1c304281b` passed all eight selected targets
and the server build: 303 Rust test passes, one normally ignored TLS case,
61.204 seconds and 1,337 unchanged tracked inputs. All eight targets executed;
none used cached test results. Namespace source matches all ten final author
inputs; bounded-read source matches eight of nine, with the BUILD list retaining
both the existing timeout fixture and the new read-lifetime fixture. The
[combined receipt](evidence/authority-bounded-read-lifetime/manifest.json)
preserves the root original-source failure and exact integration logs. These
counts overlap earlier checks; no fresh explicit TLS run is claimed here.

The later digest/ByteStream checkpoint at `f5e589bb37` passes nine selected
targets and the server build. Its first run at `091bef4395` executed all nine
targets successfully (176 Rust passes and one normally ignored TLS case), but
the independent Bazel Clippy aspect rejected a test diagnostic on stdout.
Changing that marker to stderr completed the gate: one test target executed,
eight results were cached, and all nine passed. Both runs had 1,340 unchanged
tracked inputs, with only that diagnostic line differing between runs. Strict
Cargo Clippy for the service library and tests also passed at `f5e589bb37`.
The [integration receipt](evidence/fixed-listener-digest-default/root-integration.json)
separates those checks from the author's original failures and twelve compiled
fault controls. These are focused counts, not a new full-workspace checkpoint.

The canonical-owner checkpoint at `595ece120c` passes nine selected targets and
the server build. At its preceding `2947999e96` source, all eight compiled targets
executed and passed (298 Rust tests and two ignored cases), while the external
API test failed compilation because its Bazel rule omitted the tracing macro
dependency. Adding that one dependency completed the gate: the API test ran,
eight results were cached, and the logs total 299 passes and two ignored cases.
Each run retained 1,346 unchanged tracked inputs. An independent execution of
the real sync matrix against the root Bazel binary passed three successful opens
and six injected sync refusals, with nine separate setup invocations. The
[root receipt](evidence/canonical-journal-owner/root-integration.json) binds those
observations to the exact binary and source, separately from author qualification.
The [sealed author packet](evidence/canonical-journal-owner/author/README.md)
retains eleven detected compiled faults, one positive and fourteen negative API
compiler controls, the author sync matrix and independent reviews. Its restored
source passes 116 scheduler unit tests, one API test, 47 filesystem tests and
three strict Clippy configurations. All nine archives and 1,134 mapped files
were independently checked after copying the packet into this checkout.

The fixed upload-identity checkpoint at `a6bcfb158a` passed all seven selected
Bazel targets and the server build: 169 Rust test passes, one normally ignored
TLS case and 80.271 seconds. All seven targets executed with no cached test
results. The run retained 1,349 unchanged tracked inputs, verified against the
commit. Thirteen of seventeen author inputs match exactly, including runtime and
identity fixtures; the root retains earlier owner dependency edges and its
stderr diagnostic marker in the other four. The
[root receipt](evidence/authority-bytestream-identity/root-integration.json)
preserves logs, all seven executed binaries and independent source reviews.
Original failures and compiled mutation qualification remain separate author
evidence. These counts do not extend the earlier full-workspace checkpoint.

The latest full Cargo workspace run remains `4876ddd48c`: 2,007 tests passed with
zero failures and eleven ignored cases. That checkpoint's 166 Bazel targets also
passed, including the two manual
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
durations early. The [first Running sampling repair](authority-running-clock-sampling.md)
now removes the reproduced stale starting sample: a report at tick 10 previously
derived deadline 5 from stored time 0, and now derives deadline 15. Already-due
expiry wins before the grant, repeated Running cannot extend it, and expiry does
not release charge. Six focused cases pass; actual journal positions confirm
one extra proposal only when the clock advances. Both
[integrated Bazel targets](evidence/authority-running-clock-integration/manifest.json)
executed and passed, covering thirteen service unit and twelve authority
RPC cases with 231 unchanged source/build inputs. The RPC target filtered six
cases assigned to separate targets; the earlier count of eighteen here was
incorrect. Those separate targets pass in the later grant/journal integration
below. Fixed units, conservative
duration comparisons, overflow and bootstrap compatibility remain open; this
repair uses the existing injected clock.

The [filesystem capability prerequisite](../../../../tools/canonical-filesystem/README.md)
now pins the fixed CAS/sink layout and retains all ownership locks through
opaque file handles and delayed blocking I/O. CAS publication cannot replace an
existing slot; sink replacement preserves already-open readers. Twenty-five
Cargo cases, strict Clippy and the
[integrated Bazel target](evidence/canonical-filesystem-capabilities-integration/manifest.json)
pass. The [author receipt](evidence/canonical-filesystem-capabilities/manifest.json)
also preserves failures at both final parent-directory syncs: artifacts and
locks remain, while readable reopen does not prove historical sync success.
The API checks filenames and descriptor identity, not content hashes or persistent
physical generations. Existing path-based store and journal constructors still
need separate capability adapters; no runtime consumes this package.

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
Independent comparison remains separate qualification work;
the private candidate adds no production routing or accepted-history activation.
The subsequent [eight compiled candidate faults](prepared-memory-mutations.md)
were all detected by their named assertions. The unmodified and restored source
each passed all 72 tests. This result does not inherit or complete the independent
reference comparison.

The new independent reference was frozen before reading the candidate. A
subsequent [executable audit](evidence/prepared-memory-reference-audit/manifest.json)
found five mismatches: preparation lease bounds, stage-local write IDs, repeated
abandonment, duplicate admission, and atomic cache-miss resolution. The original
candidate passed all five corresponding regressions; the original reference
failed all five intended assertions. The corrected reference uses an explicitly
new private replay format. Full focused suites now pass 77 candidate and 29
reference cases with strict Clippy; both
[integrated Bazel targets](evidence/prepared-memory-audit-integration/manifest.json)
also executed and passed. The candidate and underlying scheduler implementations
are unchanged. All six separately compiled
[reference faults](evidence/prepared-memory-reference-mutations/manifest.json)
were detected, with 29-case baseline and restored passes. These fault checks and
the collaborative reference correction remain distinct qualification steps.

The subsequent [command-by-command comparison](prepared-memory-differential.md)
passes 34 histories across both digest algorithms, including all five audit
regressions. Eighteen cases pass before and after seven detected compiled faults;
strict Clippy, hooks and an independent adapter review also pass. The new target
executed successfully in Bazel with 151 unchanged scheduler source/build inputs.
Every command prefix is restored and compared, including rejected commands and
duplicates. The bounded domain has one execution, up to three attempts and a
preinstalled physical catalogue; it does not qualify global physical-capacity
admission. Neither engine changed for this comparison, and the original
reference's failures remain preserved.

The [immutable worker grant repair](authority-resource-grant.md) closes an actual
execution mismatch: a second attempt charged for 256 MiB still enforced the
original request's 128 MiB property. The exact typed grant now reaches the
sampler independently of the original Action, Command, platform and environment.
Real namespace processes pass with both digest algorithms: the 128 MiB grant is
enforced and the 256 MiB grant lets the same allocation complete. Checked upward
rounding and configured headroom preserve the intended ceiling; legacy disk
diagnostics remain intact. Author qualification passes 22 recipient and 97
manager tests, five compiled fault probes and independent review. This does not
activate RPC memory classification, automatic retry or a hard OS memory cap.
[Author receipts](evidence/authority-resource-grant/manifest.json) preserve both
the original failure and a caught disk-diagnostic regression.

The [ByteStream composition repair](bytestream-digest-context.md) preserves the
resource name's digest selection in completed status lookups and deferred read
futures. Real HTTP/2 tests reproduced both an incorrect cross-algorithm status
hit and a BLAKE3 read returning `NotFound` after a successful upload. All 36
ByteStream tests pass after the repair, including both wire encodings and a
backend pause before namespace selection. Removing the compressed-read context
alone reproduces a failure. This qualifies request context through the service,
not physical-store durability.

The [descriptor-backed journal](descriptor-journal.md) now keeps all four owned
root locks inside its redb backend through database and transaction lifetimes.
It refuses missing or empty reopen attempts, retains artifacts after ambiguous
initialization, and preserves the previous journal validation. The portable
constructor has no Linux filesystem dependency. Actual syscall-failure and
cancellation witnesses pass; three selected compiled faults are detected.
The [combined integration](evidence/grant-journal-integration/manifest.json)
built the server and passed 80 scheduler unit, 22 worker recipient, 97 manager,
and all 18 RPC cases across three targets. The journal bridge passed seven
cases; the explicit redb target passed 35 and the default filesystem target 25.
The receipt preserves two initial test-build failures and the correction that
enabled redb on the Bazel test itself. No production constructor changed.

## Storage, clock and memory reporting prerequisites

The [canonical store bridge](../../../../tools/canonical-store/README.md) now
implements the real store interface through descriptor capabilities. CAS verifies
the complete digest and size; the temporary AC sink preserves opaque values.
File sync, publication and all directory syncs precede success. Completed-upload
queries and duplicates verify and sync existing content before acknowledging it.
Root Bazel passed 17 store, 25 default filesystem and 35 redb filesystem cases.
The final standalone binary passed fifteen actual syscall scenarios: three
baselines and twelve injected file/directory sync failures. An independent held-read
case checks four-operation admission and every role lock across cancellation;
its compiled permit-release fault was detected on the preserved earlier source.
[Receipts](evidence/canonical-store/manifest.json) distinguish that prefix from
the final repository-lint repair and its repeated syscall qualification.
Abandoned and duplicate temporary files still accumulate: lifetime disk bounds,
cleanup, GC, worker materialization and production wiring remain absent.

The [checked clock prerequisite](authority-fallible-clock.md) adds a fallible
monotonic-seconds source and propagates observation failures through coordinator
shutdown. Construction awaits its first observation and recovery sweep. Once
polled, startup retains authority independently of caller cancellation until it
can finish or shut down. Real-journal cases preserve the full scheduler snapshot
and charges across errors; cancellation retains redb custody until shutdown.
The restored integration built the server and passed 51 cases across seven
targets, with one existing ignored listener case. Strict Cargo Clippy passed;
four compiled faults were detected. [Exact receipts](evidence/fallible-authority-clock/manifest.json)
retain source boundaries, initial lint failures and the separate bootstrap audit.
Inclusive expiry comparisons, deadline headroom and persistent time/proposal
binding remain open. A checked clock alone cannot reinterpret an old journal.

The [local memory-breach receipt](authority-memory-breach.md) requires the exact
typed grant, a sampled ceiling breach, a live-child probe, the first selected stop
decision and a completed kill procedure. It survives upload failure independently
of a finished result. Actual namespace tests exercise R and 2R under both digest
algorithms while preserving stdout, stderr and output bytes. A supervised SIGKILL
near the ceiling triggers heuristic telemetry with no receipt; the original
PID-1 self-signal fixture failed and was corrected before qualification. All 175
worker cases passed in Cargo and again in root Bazel, alongside twelve base RPC
cases and a server build. Five compiled faults were detected, including fabricated
heuristic evidence and receipt loss on upload failure.
[Receipts](evidence/authority-memory-breach/manifest.json) preserve the tests and
independent reviews. Wire versioning and coordinator MemoryFailure classification
are the next step; receipt presence alone proves neither closure nor acceptance.

The descriptor journal now reaches the real Singleton constructor through
`open_from_factory`. After first poll, an owned preparation task retains the
journal through startup and any required Raft shutdown. Sending the result is not
handoff: a private acknowledgment distinguishes accepted ownership from an
unobserved result. Four portable cases and four descriptor cases cover successful
handoff, held factory and parent sync, cancellation, an unacknowledged result,
renamed roots, all four directory locks and refusal to adopt an uninitialized
existing journal. Cargo passed all 84 scheduler units, strict Clippy and two
compiled negative controls; root Bazel executed those 84 units and all eleven
manual bridge cases. The [receipt](evidence/descriptor-singleton/manifest.json)
archives 34 files, all independently checked, with three exact integrated Rust
source hashes. Its baseline document differs from the receipt commit only by the
added qualification paragraph. This constructor does not certify recovered
membership, limit compatibility, canonical bootstrap or admission history.

Execution expiry now waits strictly past its rounded deadline. Three real-journal
boundary tests first failed against the inclusive comparison, then passed with
the repair. Exact committed-event assertions distinguish execution expiry from
independently due progress; equality after restart retains authorization, and
expiry retains the reservation until cleanup. Two compiled faults caught a
progress-path bypass and an unintended delay to progress expiry. The restored
source passed 54 cases across seven executed Bazel targets plus a server build,
with 546 unchanged source inputs. The
[receipt](evidence/authority-quantized-deadline/manifest.json) preserves those
boundaries and the independent review. These synthetic projected-duration
fixtures do not qualify every Action duration, finite counter headroom or a
persistently bound canonical clock.

Every experimental fixed listener route now has explicit protobuf decode and
encode ceilings, with 64 KiB ByteStream read chunks. Actual HTTP/2 tests cover
all nine service/role bindings, the exact control-message boundary, both digest
algorithms with objects larger than a message, malformed requests, disabled
compression and outgoing size refusal. Three compiled faults were detected and
the exact source restored. Cargo and strict Clippy passed; root Bazel executed
six listener and twenty canonical-store tests and built the server. The separately
invoked local mTLS fixture passed all four cases with fresh disposable identities,
including its shared-trust negative control. The
[receipt](evidence/authority-listener-wire-limits/manifest.json) distinguishes
these checks and records 548 unchanged integrated inputs. Outgoing size checks
occur after encoding; CAS batch accumulation, GetTree traversal and service-wide
admission remain open. The generic capabilities advertisement still does not
describe the canonical backend's finite object ceiling.

Journal reopen now checks the supplied five-field limit envelope against both
restored applied state and every saved snapshot before registering admission or
starting Raft. Matching custom limits, including disabled admission, remain
admissible. The compiled old implementation failed four new unit cases and the
descriptor bridge case. Removing either guard was independently detected: the
first applied-state fault selection was masked by the remaining snapshot guard,
so the retained no-snapshot singleton fixture supplies its separate oracle.
The restored source passed 89 unit and 12 bridge cases; the subsequent integration
passed nine Bazel targets (eight executed, one cached) and built the server with
548 unchanged inputs. The [receipt](evidence/persisted-authority-limits/manifest.json)
preserves the initial surviving selection. This is constructor compatibility,
not enforcement on live snapshot installation or a cluster-wide resize policy.

The experimental worker protocol now requires version 2 and explicit memory
evidence before registration or reconnect inventory. A matching original
reservation, successful output capture, quiescence and cleanup allow the existing
memory retry policy to run without changing the Action or Command. Actual process
fixtures cover growth and final resource failure with retained output/history,
both digests, capacity changes, stale/replayed reports, restart and malformed
evidence. Six compiled faults were detected. Root integration executed all ten
selected Bazel targets and built the server with 553 unchanged inputs; the fresh
listener binary also passed the four-case local mTLS fixture. The
[receipt](evidence/authority-memory-wire/manifest.json) separates author Cargo,
independent reviews, corrected fixture prerequisites and root integration.
Oversized worker reports still become an ordinary bounded size error and lose
the full graph and automatic memory retry. This explicit gap remains open;
bounded marked reports received by the coordinator cannot silently become
ordinary completions when their committed representation is too large.

Singleton construction now checks recovered membership before admission and
before Raft can replay or mutate the journal. It projects the committed replay
interval by index, refuses pending membership changes and requires exactly the
local voter with no retained learners. Saved snapshots must be compatible with
the recovered frontier; older snapshots from a committed three-to-one history
remain admissible. The old implementation failed seven new recovery cases and
the descriptor retry case; the fixed implementation passed 98 scheduler units
and twelve descriptor cases. Five compiled faults were detected, and the restored
integration passed eleven Bazel targets with 554 unchanged inputs. The
[receipt](evidence/singleton-membership-preflight/manifest.json) retains the
initial format failure and the guard removal masked by final-position checking.
This is constructor admission: public experimental Raft handles can still change
membership afterward. Synthetic histories do not certify an actual migration,
historical quorum decisions or every internal index in an older snapshot.

Fixed authority CAS handlers now admit at most 64 entries and 64 KiB per batch,
checking combined actual and declared sizes before storage calls. Reads require
the selected digest, exact length, genuine EOF and a successful producer.
GetTree completes a bounded traversal before pagination, counts repeated and
missing children before enqueue, and binds continuation to the instance, digest,
root and observed traversal. Direct gRPC forwarding cannot bypass these checks.
All nine new RPC fixtures failed against the old handlers. The fixed integration
passed eight Bazel targets (seven executed, one cached), with 150 logged passes,
one ignored TLS case, a server build and 559 unchanged inputs. A fresh run of the
ignored local mTLS fixture passed all four scenarios. The
[receipt](evidence/authority-cas-admission/manifest.json) preserves author tests,
twelve detected compiled faults, the initial compilation failure and the later
combined-total fixture. Raw byte limits do not establish a decoded heap bound;
arbitrary backends can allocate or start blocking work before collector refusal.
AC decoding, ByteStream registry growth and global concurrency remain open.

Captured Action timeout fixtures now cover omission, explicit zero, fractional
seconds and the maximum finite duration. They preserve exact request bytes and
the derived timeout through snapshot restore, replay of committed Submit/Match
entries not yet applied, and actual singleton restart. Short finite durations retain the strict
driver boundary. A diagnostic starts admission at a high committed tick and
shows that a finite duration can reach `u64::MAX` without arithmetic overflow;
there is no later representable strict-expiry tick. Independent progress and
interest expiry can still revoke authorization. This does not demonstrate
indefinite execution or select a canonical lifetime policy. Six compiled faults
were detected; root ran both affected Bazel targets with 38 passes, a server
build and 560 unchanged inputs. The
[receipt](evidence/authority-captured-timeouts/manifest.json) retains the initial
incorrect Bazel label and its correction. This patch changes tests and documents
the missing headroom contract; it changes no runtime timeout projection.

The private journal path now binds one fixed canonical profile and a nonzero
namespace commitment in required metadata, applied state and saved snapshots.
Generic constructors and admission recovery refuse bound or partially bound
histories. New private journals atomically record `preparing` before the parent
sync callback; existing opens require `sealed`. No runtime code writes that seal
yet. Live reads and writes check required metadata, and snapshot imports compare
the immutable contract before mutation. Generic serialization remains unchanged.
Nine storage fixtures and five detected compiled faults qualify this boundary;
root integration executed eleven Bazel targets with 260 passes, a server build
and 562 unchanged inputs. The
[integration receipt](evidence/canonical-journal-binding/root-integration.json)
preserves the independent review and earlier clock/constructor audit. Synthetic
commitments, seals and parent callback failures do not prove owned bootstrap,
actual sync failures or temporal provenance. Live generic snapshot installation
still does not compare imported Limits with the constructor's requested Limits;
the future canonical interface must qualify or prohibit that path.

The constructor audit also separates logical accepted-history namespace from the
physical CAS identity, and authority-member incarnation from worker incarnation.
Neither pair can be substituted. Reopen must validate persisted limits,
membership, supported format/admission history and the canonical clock contract;
readable ownership records do not supply those facts. Descriptor-to-Singleton
construction and explicit listener wire ceilings now have separate implementations.
Fixed CAS batch/traversal limits and fixed AC raw/encoded value bounds now
have separate implementations. Combined service admission still needs its own
bounds before a service memory claim is justified.

Captured Action/Command reads and committed cache lookup/publication effects now
select digest context explicitly. Three actual HTTP/2 fixtures cover SHA256 and
BLAKE3, including publication replay after a singleton restart; a fourth direct
Execution call proves that omitted digest selection ignores an ambient BLAKE3
context. The author retained two compiled original-runtime runs failing all four
fixtures, five detected compiled context-removal faults, and restored regression
results. Root independently reproduced three original failures and passed the
four final fixtures plus server build with 565 unchanged inputs. Its later AC
integration also passed all 24 canonical-store cases. The
[receipt](evidence/authority-storage-hash-context/manifest.json) retains all author
binaries, source reviews and root evidence. Replay retained live filesystem
capabilities; it does not prove cold reconstruction, process quiescence or power-loss
recovery. The separate cache-endpoint process-default audit remains open: legacy
global defaults and ByteStream forwarding still need consistent fixed-role handling.

Fixed client AC now admits at most 1 MiB of raw stored value and encoded
ActionResult, rejects oversized updates before storage, requires channel EOF and
producer success, and applies a cooperative 30-second deadline. It keeps opaque
Action keys and mutable M4 publication semantics. Fixed handlers bypass direct
GrpcStore forwarding and disable inline output requests; the legacy constructor
is unchanged. Twelve compiled root fixtures failed against the original behavior,
and the canonical response fixture separately exposed late encoder rejection.
Two earlier fixture compile failures and one mistaken target label remain labeled
as non-executions. The author detected eight compiled faults, passed strict lint
and regression checks, and exercised four explicit mTLS cases. Root passed all
ten selected targets with 175 test passes, one normally ignored TLS fixture,
server build and 566 unchanged inputs; a fresh explicit TLS run passed its four
cases. The [receipt](evidence/authority-ac-admission/manifest.json) keeps these
source boundaries distinct. Complete range delivery remains a backend premise;
upstream decoding, spare allocation, global concurrency and physical IO are not
bounded by this handler. Cancellation cannot undo an uncertain remote write.

## Current namespace synchronization and bounded-read completion

The filesystem ownership capability now derives a versioned SHA256 commitment
from the fixed manifest identities and all four role device/inode pairs. It
validates all four exact namespace records before any synchronization, retains
their descriptors through eight file/parent syncs, and checks pinned-root
attachment before and after. The opaque non-Clone token retains all four locks.
Path names and explicitly excluded runtime facts do not affect the commitment;
that exclusion does not authorize changing those facts. Existing hardlinked
record acceptance is preserved.

Final author source passed 45 all-feature and 35 default-feature tests, strict
Clippy and repository hooks. Ten actual syscall cases cover success, each of
the eight targeted sync failures, and late invalid input with zero syncs. Six
compiled fault controls were detected on earlier source with identical runtime
and selected oracle bodies; the final textual golden fixture conversion has
separate source and binary boundaries. Root independently checked framing and
then integrated all ten final inputs unchanged. The
[author receipt](evidence/canonical-namespace-resynchronization/manifest.json)
and [root supplement](evidence/canonical-namespace-resynchronization/root-integration.json)
retain those distinctions, including eleven executable artifacts. This token
establishes current validation, synchronization and cooperative lock custody;
it does not recover a historical acknowledgment or grant journal, serving,
physical-generation or worker authority.

The fixed authority's bounded Action/Command reader now gives its producer
future ownership of the channel writer. A producer that returns successfully
without EOF closes the channel and produces an error instead of leaving the
collector waiting forever. Producer errors still propagate even after EOF;
excess bytes still cancel the producer. The runtime change adds six lines and
removes one, without adding a timeout or changing the byte limit or digest
context.

Five fixtures cover missing EOF, producer errors before and after EOF, empty
and exact-limit success, excess-byte cancellation, and a producer that remains
pending after EOF. Both author and root original-source runs passed four cases
and failed the missing-EOF case at its first empty iteration; the later partial
iteration was not reached. Author fixed/restored suites passed 28 service,
15 listener and 24 canonical-store tests, with one normally ignored TLS case;
three compiled behavioral faults were detected. The
[receipt](evidence/authority-bounded-read-lifetime/manifest.json) preserves exact
sources, binaries, review and root integration. Arbitrary blocking backends,
false EOF, whole-value completeness and physical IO remain separate premises.

## ByteStream final status and fixed digest defaults

The common uncompressed ByteStream facade now waits for both channel EOF and
successful store-producer completion before reporting successful trailers. The
five tests were written first: original source passed two controls and failed
three regressions, including a real HTTP/2 late error after delivered data.
Repaired source passes all five; three independently compiled faults are
detected at the intended error, pending-producer and HTTP/2 oracles. The first
broader six-target run passed 140 Rust tests and failed four during the fixture's
five-second singleton leader wait, before requests began. The unchanged-source
serialized rerun executed all six targets successfully: 144 passes, one ignored
TLS case, and the server build. All 1,339 tracked inputs match the same commit
within and between runs. The
[receipt](evidence/bytestream-read-completion/manifest.json) preserves both runs,
the compile-rejected first spelling of one mutant, independent source review and
five distinct executables. Waiting cannot retract bytes already delivered,
preempt a blocking backend or independently prove whole-value completeness.

Fixed listeners now resolve omitted digest functions to SHA256 independently of
the process-wide legacy default. AC and CAS bind that selection before their
operation contexts; ByteStream also retains its local transfer facade for
GrpcStore backends. An isolated process first sets the legacy default to BLAKE3,
then exercises fifteen selected HTTP/2 phases. Original source fails thirteen
affected phases and passes the legacy and Execute controls. Corrected source
passes all fifteen in one test plus the 24 existing canonical-store tests.
Twelve compiled faults are detected, each with passing legacy and Execute
controls; the restored suite passes. The
[author receipt](evidence/fixed-listener-digest-default/manifest.json) retains
the initial test oracle that exceeded fixed CAS admission and its correction,
strict Cargo checks and independent review. Its stdout-marker source boundary
is separate from the root stderr-marker integration. Nineteen distinct author
and root executables are retained by the
[root supplement](evidence/fixed-listener-digest-default/root-integration.json).

Neither repair changes production role construction. Aggregate transfer
admission and the complete canonical clock remain open.
Legacy-name GrpcStore upstream servers still require refusal or adaptation before
canonical construction; the qualified upstream uses modern resource names.

The subsequent [upload-identity repair](authority-bytestream-identity.md) keys
fixed uploads by exact raw UUID, resolved algorithm, hash and size within each
instance. Active exact collisions refuse; absent nonzero resumes cannot insert
idle progress. Single-message writes retain the tracked guard until backend
completion. Each consumed nonempty later resource name must equal the first
within a Write RPC. Existing parser admission and legacy construction remain.
The registry still has no aggregate quota; this closes the identity slice without
establishing a total transfer-memory bound.

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
   still need combined construction. The private compatibility binding must be
   derived from retained ownership and sealed only after the required barriers.
   Existing fixtures qualify parts of these behaviors; production
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
