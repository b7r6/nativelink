# Atomic publication composition candidate

This is a bounded test composition of the existing `authority::State` and
`retention::Coarse`. It is not a production constructor, M6 activation, durable
Raft boundary, provider adapter, or replacement implementation of either reducer.
The original graph candidate was implemented without reading the separately
frozen publication-head reference reducer or its tests.

Every public mutation runs on an outer clone. `Publish` checks current prepared
physical identities and fences, passes the captured response to real
`State::Complete`, installs operation roots and transfers staging through real
`Coarse`, updates accepted provenance and the latest commit head, then swaps the
whole state once. Its temporary M4 `PublishResult` is consumed inside that clone;
no external effect driver can observe it. Production bridge removal remains a
separate activation gate. Accepted-history activation is refused on restore.

Direct publishers have a distinct client identity and no synthetic attempt.
Only canonical successful cacheable direct updates are accepted; broader direct
update policy remains unspecified. Worker eligibility comes from the actual
scheduler request and captured response. The synthetic constructor keeps opaque action labels. The real constructor
binds each label to one complete registered ActionKey, captured selected platform,
cache policy and trusted principal/config context.

Typed closure and physical readiness inputs are explicitly trusted fixture
observations. Their completeness is an environmental assumption, not a proof
inferred from a worker claim. The candidate verifies exact result bytes, complete
member equality, catalogue revision, current generation readiness and retirement
fences. Each full namespace and incarnation identity has a distinct persisted
Coarse handle. Trusted initial readiness is replayed through a short internal
Coarse staging/write/release transaction; no provider I/O is claimed.

Finite caps cover 8 stages/accepted records, 16 operations, 64 generations,
8-member closures, 16 serving grants and bounded payloads. New execution
admission reserves a future stage, eight member/write slots and an acceptance
root; joined operations reserve their own result-root row. Unrelated catalogue
uploads and direct stages cannot consume those credits. Replacement attempts
reserve another candidate before their real State match becomes visible, while
prior write obligations remain retained. Root/write capacities prepay all
bootstrap, staging, accepted, operation and serving rows. Identity
rows are retained, so infinite churn eventually refuses admission. This does not
establish production sizing, reclamation progress or M7 collection.

Snapshots preserve both real engines plus outer metadata in one canonical,
bounded encoding. Restoration validates cross-component identity, exact terminal
bytes, provenance, protection roots and commit-order head selection. The derived
index is disposable and rebuilt from current protected authoritative heads.

The original graph-publication alphabet requires a captured ExecuteResponse with
an ActionResult. The explicit private-v4 extension below adds no-graph terminal
outcomes, cancellation and expiry. Graph-bearing ResourceFailed outcomes and
active feature-3 history remain concrete promotion blockers. The fixture does
not synthesize an empty ActionResult or route a memory failure through Complete.
Fresh protection after an expired head,
provider retirement/deletion, production admission sizing, and persistent feature
migration also remain separate qualification work. Physical preparation and live
read barriers are trusted test inputs. Operation result deadlines and prepared
lease intervals must still permit live protection at acceptance.

Cache hits construct and capture a fresh ExecuteResponse with cached_result=true
from the verified immutable ActionResult. They preserve the original accepted
worker response byte for byte and never select via the disposable index.

The actual scheduler prepays its own complete JSON result payloads and lifecycle
rows. Stage checks that full payload ceiling, including the distinct cached-result
projection for an eligible head, before spending execution credit. Worker stages
must name the actual execution's qualified action; retained stages do not require
a currently authorized attempt on restore.

Every accepted outer mutation also serializes its canonical snapshot before the
swap and refuses snapshots above the unchanged 2 MiB ceiling. This is a bound on
the proposed state. The additive
[snapshot byte ledger](../../design/categorical/publication-snapshot-bound.md)
bounds every valid state of the current finite alphabet at 2,022,804 bytes,
including outer copies and nested child encoding. Restore restricts retention to
the outer alphabet's Allocating and Closing phases with no capture or mark
records. FinishClose and Mark remain excluded. This bound does not qualify
production sizing or reserve additional retry-stage rows.

The finite capacity histories fill 8 stages, 16 operations, 64 generations,
8 workers, all 128 write rows, and (for successful results) all 112 root rows and
16 serving grants. They use 256-byte qualified action strings with JSON escaping,
4094-byte property maps, near-limit complete result payloads, wide numeric IDs,
repeated scheduler progress, and all 64 cohort fence transitions. Every exposed
state snapshots and restores canonically. The largest measured snapshots after
payload qualification are 1,082,981 bytes for success and 1,039,398 bytes for error;
these witnesses are not a worst-case proof for all histories. No aggregate
snapshot overflow was reproduced. Advance changes the two clocks only; private
v4 also requires explicit scheduler events for cancellation and expiry.

A worker Stage previews actual Complete on a discarded scheduler clone before
spending the current execution's single stage credit. A late predecessor cannot
poison that credit. Stable receipt identity and action bindings also validate on
restore, while old staged writes survive later revocation. Publish repeats the
actual authority check at acceptance. This stricter Stage admission follows from
the candidate's per-execution row credit; it does not claim equivalence to the
reference's entire Stage alphabet with a global stage budget. Preview cloning and
snapshot serialization costs are test-only; production performance is unqualified.

Real direct updates require an explicit bounded authorization record for the exact
pre-registered ActionBinding. The fixture API takes trusted context, retains all
four ActionKey fields, and never treats the grant ID as an incoming credential.
It constructs a canonical successful ActionResult projection with the registered
SHA256 or BLAKE3 algorithm. The caller still supplies trusted typed closure and
physical observations. No RPC authentication, production UpdateActionResult
adapter or fake worker attempt is introduced.

Authorization IDs cannot be reused. Revocation prevents new stages and acceptance
of already prepared updates, while an accepted head and existing serving roots
remain valid. Restore checks the retained full binding and the ordered grant,
staging, acceptance and revocation positions. Revoked grants remain bounded rows;
the fixture refuses new grants after eight distinct IDs. Worker and client heads
follow accepted commit order without changing the original worker response.

Private test snapshot version 1 preserves the synthetic format. Version 2 is the
real-request format without direct grants; version 3 requires a nonempty direct
authorization ledger, including revoked records. A version number alone grants
nothing. Version 3 here is unrelated to durable accepted-history feature 3, which
remains disabled and refused on restore. Optional fields are omitted from older
formats; no production persistent migration is claimed.

The additive [real-request constructor](../../design/categorical/publication-real-request.md)
retains the captured REAPI request and full trusted action binding without
changing these frozen synthetic scenarios. Each joined operation is checked
through its own original request. Real-mode direct publication now requires an explicit trusted grant as described
above; provider readiness remains separately qualified.


## No-graph terminal successor

An explicit `enable_terminal_outcomes` transition selects private snapshot
version 4. Older private formats omit the new section and the false `closed`
credit field. Version 4 is unrelated to persistent feature activation; accepted
history remains disabled. This is still a finite test composition.

`report_no_graph` accepts a bounded report identity, the exact captured response,
qualified action, epoch, execution, attempt, worker and incarnation. Both Complete
and MemoryFailure require a validated response with no ActionResult. The actual
State transition decides whether a memory failure retries or terminates, and
selects the exact resource-failure reason. The outer layer records that observed
decision, its commit position and logical time. A retry is a classified report,
not a terminal acceptance. Reusing an identical report identity is a no-op;
conflicting reuse refuses without mutation.

Terminal acceptances have no publication ID, CAS object, graph closure or root.
Only operations that the scheduler actually terminates with the accepted value
receive its additive association. An independently canceled handle keeps its
terminal. The accepted worker origin remains immutable after later revocation.
Current scheduler authority is checked at report acceptance, not reauthorized
when reading an already accepted record.

Cancellation, interest renewal and fenced expiry use actual State events. Unused
logical stage/member credit closes whenever the execution becomes terminal,
including through worker loss or replacement. Physical reservations, cancellation
intents, stage guards and write/outcome rows remain until their existing explicit
transitions settle them. A final memory report requires its original charged
vector and quiescence; cancellation or exhaustion alone cannot refund capacity.

The successor admits at most three classified reports per execution, 24 reports
and eight terminal acceptances overall. Operation associations fit the existing
16-operation bound. Admission already reserves these finite row counts through
the execution and operation caps. The snapshot byte ledger covers the complete
outer encoding; additional retry-stage reserves remain unqualified. The complete
proposed snapshot must fit 2 MiB before
any transaction becomes visible. Ignored events preserve authoritative snapshot
bytes even when the disposable index has been lost.

The candidate author used the frozen terminal contract and actual State/Coarse
APIs without reading the new terminal reference reducer or its tests. Separately
written candidate scenarios were captured before the implementation was made
available to their author. Qualification receipts distinguish the initial test
accessor/coalescing corrections, candidate review fixes, restored baseline checks
and compiled mutations. Neither these scenarios nor the separate reference
qualify graph-bearing ResourceFailed preparation, history protection, production
RPC routing, collectors or durable activation.

## Prepared memory successor

The additive [private-v5 candidate](prepared_memory.rs) accepts a prepared graph
with an explicit MemoryFailure classification. Its
[contract and limits](../../design/categorical/publication-prepared-memory.md)
keep the exact response and closure in the existing stage and let actual State
decide retry or ResourceFailed at acceptance. The no-graph alphabet above remains
unchanged. Accepted history, production routing and durable formats remain
outside this experiment.
