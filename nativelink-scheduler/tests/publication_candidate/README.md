# Atomic publication composition candidate

This is a bounded test composition of the existing `authority::State` and
`retention::Coarse`. It is not a production constructor, M6 activation, durable
Raft boundary, provider adapter, or replacement implementation of either reducer.
The publication-head reference implementation and its tests remain unread.

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

The common alphabet requires a captured ExecuteResponse with an ActionResult.
Error responses without an ActionResult, final MemoryFailure/ResourceFailed outcomes,
last-interest cancellation/expiry, and active feature-3 history remain concrete
promotion blockers. The fixture does not synthesize an empty ActionResult or
route a memory failure through Complete. Fresh protection after an expired head,
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
the proposed state, not a reservation for every future outer byte. The scheduler's
credit does not cover the extra outer copies of results, closures, operation
payloads, nested child encoding, or Coarse capture records. A complete residual
byte reserve for those fields remains unqualified.

The finite capacity histories fill 8 stages, 16 operations, 64 generations,
8 workers, all 128 write rows, and (for successful results) all 112 root rows and
16 serving grants. They use 256-byte qualified action strings with JSON escaping,
4094-byte property maps, near-limit complete result payloads, wide numeric IDs,
repeated scheduler progress, and all 64 cohort capture transitions. Every exposed
state snapshots and restores canonically. The largest measured snapshots after
payload qualification are 1,082,981 bytes for success and 1,039,398 bytes for error;
these witnesses are not a worst-case proof for all histories. No aggregate
snapshot overflow was reproduced. Advance changes the two clocks only; it does
not synthesize excluded last-interest expiry or cancellation transitions.

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
