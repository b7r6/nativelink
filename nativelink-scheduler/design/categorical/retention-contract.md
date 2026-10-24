# Bounded sealed-cohort retention experiment

Status: independently checked finite executable experiment; no production representation,
provider, typed-closure, or M0 qualification is implied. The neutral protocol is
[`tests/retention/protocol.rs`](../../tests/retention/protocol.rs). The coarse
candidate and independent per-generation oracle implement it separately.

## Model boundary

A physical key is `(store incarnation, generation)`. A logical object may have
several physical keys. Neither key components nor cohort/root/write/delete IDs
are recycled in this finite experiment. Roots have immutable kind and canonical
flat closure; a lease generation distinguishes successive acquisitions of the
same root. The flat closure is an abstraction of a verified typed manifest, not
evidence that an actual `Tree`, stdout, stderr, or result closure is complete.

All time is committed logical time. Coverage is live exactly when `until > now`.
This checks transition ordering; it does not establish election-safe clock
conversion, provider availability, or enforcement of real read deadlines.

The public `View` exposes canonical maps, current protection, complete captured
manifests, marking progress, and retained write/delete records. The oracle stores
protection as separate per-generation rows. It does not read or call the coarse
implementation. A candidate snapshot must restore its complete future behavior;
the two representations need not serialize identically.

## Bounds and initialization

`new(Limits)` returns an error unless every bound is positive and checked
`generations + writes <= delete_intents`. The checked sum must also fit below
`u64::MAX`, reserving numeric delete IDs. Initially time and fences are zero,
all relations are empty, and the next delete ID is one. Admission enforces hard
caps on cohorts, generations, roots, closure members, mark and retirement batch
members, write records, and delete records. Retained records count toward caps.

Each generation retires at most once and each write settles at most once. Thus
at most `generations + writes` delete intents can ever be created: one initial
retirement and at most one additional intent per successful late write. This
reserves finalization capacity before writes are admitted. An observed late
write cannot be rejected because unrelated work filled an effect queue.

All key vectors are nonempty, strictly sorted, and duplicate-free. A closure
has at most `closure` keys. Mark requests have at most `mark_batch` complete
captured manifests; each manifest still has the closure bound. Retirement
requests have at most `retire_batch` keys. Overflow rejects an event without
partial mutation. Rejecting an event never changes state.

The ledgers and identity tombstones are deliberately not compacted. Successful
finite campaigns establish finite-population progress under their stated bounds;
they do not establish admission or bounded metadata under infinite churn.

## Exact transition rules

`Applied` changes state; `Ignored` is a recognized harmless duplicate or no-op;
`Rejected` changes nothing. Unknown IDs reject unless a rule below says otherwise.

| Event | Rule |
| --- | --- |
| `AdvanceTime(now)` | Reject backward time; ignore equal time; advance otherwise. Logical expiry removes current coverage, never an immutable captured manifest. |
| `CreateCohort(id)` | Existing ID is ignored. Otherwise admit within cap in `Allocating`, fence zero. |
| `Allocate(cohort,key,logical_object)` | An exact existing identity/descriptor is ignored; reuse with a different descriptor rejects. A new key requires an allocating cohort and generation capacity. It starts unready, not retired, at revision zero, without a delete. |
| `Seal(cohort)` | Allocating becomes sealed and increments its fence. Already sealed is ignored; other phases reject. Population can never increase after this transition. |
| `BeginClose(cohort)` | Sealed becomes closing and increments its fence. Already closing is ignored; other phases reject. New acquisition and reacquisition immediately stop. |
| `FinishClose(cohort,fence)` | Require the current fence and closing phase with no live staging root touching the cohort. Capture every live root touching it in ascending root-ID order, including each complete immutable closure and lease generation; start cursor zero and empty marks; become closed. An already closed cohort at the same fence is ignored. |
| `Mark(cohort,fence,at,roots)` | Require closed/current fence and a nonempty bounded list exactly equal to the captured slice at `at`. A previously processed complete slice is ignored. Otherwise `at` must equal the cursor; add only this cohort's keys to marks and advance the cursor. Partial overlap, omission, alteration, wrong ordinal, or a stale fence rejects. Empty capture is complete without a mark event. |
| `Retire(cohort,fence,keys)` | Require closed/current fence, completed marking, and a canonical bounded list entirely belonging to that cohort. Every key must be unmarked. Already retired members need no new effect; if all are retired, ignore. Otherwise atomically retire the new members, make them unready, and allocate one exact-key delete per member at its current write revision. |
| `Reopen(cohort,fence)` | Require closed/current fence; return to sealed, increment fence, and clear capture, cursor, and marks. This may abandon an incomplete pass. All old preparation evidence is invalid. Retired generations and their obligations remain. |
| `Acquire(root,lease,kind,closure,until,fences)` | Require a canonical bounded closure. An already live root with identical kind/closure/lease and `until <= current until` is ignored; any other live-root acquisition rejects. A fresh root uses lease one and consumes root capacity; an expired root requires its immutable kind/closure and exactly the next lease generation. Require `now < until <= now + lease_ticks`, no retired key, and all keys ready except for staging roots. The fence map must exactly name touched cohorts at their current revisions, all allocating or sealed. Install coverage atomically. |
| `Renew(root,lease,until)` | Require the same live lease. An expiry no later than the current expiry is ignored. Otherwise require the lease bound; extend coverage without changing membership, fence, captured manifests, or mark progress. Staging extensions additionally require every touched cohort allocating or sealed. Public/serving/operation/input roots can renew while closed. |
| `Release(root,lease)` | Require the matching lease. Already expired is ignored; otherwise set expiry to now and remove only this root's current coverage. |
| `Transfer(staging,lease,publication,until)` | Require a live matching staging lease, a never-used publication ID, root capacity, a valid new expiry, ready members that have not retired, and no closed touched cohort. Atomically expire staging and install a publication at lease one over exactly the same closure. A closing cohort is allowed only for this exact already-protected transfer. Duplicated or otherwise invalid transfers reject. |
| `StartWrite(write,key,staging,lease)` | An existing write ID for the same key is ignored; reuse for another key rejects. Otherwise require write capacity, a generation that has not retired, and the same live staging lease containing the key. Record an unresolved provider write. |
| `WriteDone(write,success)` | A repeated identical terminal outcome is ignored; a contradictory one rejects. Set the known-quiescent outcome. Success increments the generation's write-observation revision. A live generation becomes ready. A retired generation stays unready and gets a fresh delete ID/revision, even if an older delete is still pending or already succeeded. Failure creates no bytes and no additional intent. |
| `DeleteDone(delete,success)` | A failure is ignored so the exact intent remains retryable. A repeated success is ignored. First success confirms that record; only if it is still the generation's current delete at the same write revision may it clear current delete and set confirmed revision. Older success never settles a newer orphan. Outstanding write records and retirement markers are never erased. |

Fence, lease, revision, time-bound, and ID increments use checked arithmetic.
Delete IDs are allocated consecutively in canonical retirement-key order.
Allocating a new delete clears the generation's confirmed revision; historical
confirmations remain attached to the older immutable delete records.
If the mathematical lease bound exceeds `u64`, the proposed lease rejects.

`WriteDone(false)` means the provider attempt is known terminal and quiescent;
a timeout or lost response is not such an observation and remains unresolved.
`DeleteDone` describes acknowledgment of an exact immutable locator. The model
does not assert that an arbitrary provider offers this identity or quiescence
contract. A new physical key is unaffected by every old-key delete regardless
of reuse of the logical object name.

## Safety and progress argument being checked

Before closing, every newly protected generation enters a root. Closing fences
new membership immediately. Existing staging guards have bounded expiry and
cannot extend it while closing; their only successful transfer retains the exact
membership. Consequently a fair advancing clock permits the close to finish.

At that point every live root touching the finite sealed population is captured.
Subsequent renewal changes expiry only. Root release or expiry cannot remove a
captured closure, and reacquisition is fenced. Exact bounded slices therefore
produce a finite conservative mark set containing every still-protected key.
Unmarked retirement cannot remove an oracle-protected generation. Expiration
during marking may delay reclamation for one additional pass; it does not force
the current pass to restart. A continuously renewed root may retain its own
closure forever, while unrelated unmarked objects remain reclaimable.

Publication transfer is one transition over identical members. Serving is a
separate root: releasing a publication root, used here to model head removal,
cannot revoke an already granted serving lease. This does not test actual AC
head selection or transport read authorization.

Retirement is irreversible. Late writes cannot restore readiness or protection.
A new delete identity after each observed late write prevents an old success
from falsely settling it. A successful delete with unresolved writes is not
quiescence; final orphan settlement additionally needs all writes known terminal.
No finite reclamation bound exists without that provider assumption.

## Checked finite envelope

The executable tests are
[`retention_test.rs`](../../tests/retention_test.rs). The fixed exploration starts
with one sealed cohort, two physical generations, one publication, and one
unresolved write. It enumerates every sequence through depth seven over fourteen
explicit event values, merging only identical complete canonical candidate
checkpoints. Exploration uses no state cap or partial-order reduction. Both models are
restored before every explored edge, and every reply, full observable view, and
per-generation protection invariant is checked.

The observed exploration has **508 states and 4,060 transitions**, with depth
counts `[1, 6, 14, 22, 36, 71, 140, 218]`. Eleven event families have successful
transitions in exploration; bootstrap and named fixtures cover all sixteen event
families and all five root classes. This is exhaustive for that alphabet and
depth, not for the complete protocol or arbitrary histories.

The sustained-renewal fixture advances 64 ticks while renewing two captured
publications. The first mark slice stays complete throughout, and an unrelated
generation is retired and its delete acknowledged. The finite fair-drain fixture
contains eight generations and four unresolved writes. It settles in **22 of 22
derived steps**: clock/seal/begin/finish, two retirement batches, eight initial
delete completions, and a write observation plus new delete completion for each
unresolved write. Arrivals stop, the clock and collector receive service, and
every provider write becomes known quiescent. Those are assumptions, not inferred
provider guarantees.

Other fixtures check grandfathered staging transfer without a protection gap, closing guard
expiry despite attempted new arrivals, serving after publication removal,
expired renewal versus fresh acquisition, changed lease identities, complete
cross-cohort manifests, stale and omitted mark evidence, bounded atomic batches,
physical reincarnation, both orderings of a late write versus old delete success,
capacity reserved to finalize every admitted write, and checkpoint
recovery with unresolved obligations. The candidate additionally rejects forged
and duplicate identities in its checkpoint fixture.

Run `cargo test --locked -p nativelink-scheduler --test retention_test
--profile=smol -- --nocapture`. Machine-readable exploration and fair-drain
receipts are written beneath `NATIVELINK_RETENTION_EVIDENCE_DIR`, defaulting to
`/tmp/nativelink-categorical-evidence`. The exploration receipt includes the
complete fixed alphabet and bootstrap, so the bound is directly reviewable.

## Conservative retention and resource bounds

For generation cap `G`, root cap `R`, closure cap `K`, and cohort cap `C`, current
root manifests contain at most `R*K` key references. Complete captured manifests
across concurrent passes contain at most `C*R*K`; marks contain at most `G`
distinct physical keys. A single mark command contains at most
`mark_batch*K` keys. With the fixture defaults these bounds are 128 live-root
references, 512 captured references, 16 marked keys, and 32 submitted mark keys.
These are counts, not measured bytes or apply latency.

A sealed cohort's pass processes at most `ceil(R/mark_batch)` accepted slices.
After closing starts, staging guards expire within at most `lease_ticks` logical
ticks unless they transfer sooner. Expiry during a pass can conservatively retain
at most that cohort's finite population for one additional pass. Continuing live
renewals can retain their own members indefinitely, without increasing the pass
population or resetting the cursor. New acquisitions may need fresh generations
in another cohort; the experiment admits at most `G` total generations and thus
does not hide copying behind an unbounded allocation space.

Actual payload sizes, retained bytes, copying amplification relative to a real
workload, and collector service latency are unmeasured. Unresolved provider writes
have no finite settlement time without eventual quiescence. Typed graph traversal,
manifest authenticity, real provider effects and timing, shared-authority costs,
durable compatibility, and compaction supporting sustained infinite churn remain
separate missing qualification gates. This checks the proposed refinement at a
small abstract boundary; it does not close M0 as a whole or freeze a format.
