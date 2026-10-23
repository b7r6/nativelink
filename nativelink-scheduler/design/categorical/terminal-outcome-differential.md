# Terminal outcome comparison

This test was authored after separate source freezes of the actual-State/Coarse
candidate and the independent M1/retention terminal reference. It changes neither
engine. The target is `//nativelink-scheduler:terminal_outcome_differential_test`.
Its five comparison tests also import eight pre-existing component tests; the
component tests are not counted as new differential histories.

The candidate freeze is `publication-terminal-tests/final-freeze/freeze.json`,
SHA-256 `5dbe52245602eb65721ea6b72961de2e0235a8d173e9497c3a5d60524665e72b`.
The corrected reference freeze is
`terminal-outcome-robustness-evidence/corrected-freeze/manifest.json`, SHA-256
`e79061b6240bd4d6219b644308c43cef937d7d87bc98a0dfb0a553dfd35ca1c0`.
The candidate terminal source hash is
`01794a74e045a6f5125e8df83ee868ca6505636a7845208cf651c8da5cb1a7df`;
the reference source hash is
`35909d9e1af651be089935a3821113c639ea667a913a4bbcc903d5128cb11c65`.
These identify distinct frozen implementations, not a shared reducer.

## Common command mapping

| Neutral input | Candidate mapping | Reference mapping |
| --- | --- | --- |
| Complete captured Request and operation ID | `admit_request` with registered full binding and trusted context | `Admit` with the same captured Request |
| Lifecycle scheduler event | `scheduler_event`, or `advance` for the joint clock | `Scheduler` with the identical event |
| No-graph report | Exact payload/classification plus explicit epoch, action label, execution and worker origin | Same report ID, payload, classification and attempt/worker/incarnation; the model resolves execution and epoch |
| Eight-member stage | Allocate eight fresh physical catalogue entries, then Stage, on an adapter-owned clone | One Stage command allocates, protects and starts the eight writes |
| Settled write observation | Resolve retained internal write ID to its external stage/write/key/object and call `observe` | `WriteDone` with the original internal write ID |
| First abandonment | `abandon` | `Abandon` |

The comparison observes the Stage macro at its complete command boundary. It does
not claim that the candidate's separate public allocation commands are one
production transaction. Failure of any component discards the adapter clone.
Every staged generation is new, the full eight-member credit is consumed, and
the stage's exact stable write identities remain observable after it closes.

Both digest algorithms use independently constructed Action, Command and
ExecuteRequest bytes with a selected platform, timeout, request metadata, queued
timestamp and trusted principal. Joined callers have distinct original metadata,
priority and queued timestamp. Every original operation Request is compared with
the independent reference's retained original and the candidate scheduler's
`operation_request` method after restore. Comparing only the representative
execution Request would miss this distinction.

Requests explicitly skip AC lookup. The candidate resolves its initial lookup
inside admission, while the reference exposes lookup as a separate event. This
test does not rewrite either scheduler's lookup state to force agreement.

The candidate has an initialization commit and separate allocation commits. The
reference starts at position zero and allocates within Stage. Each new report's
native position is first checked against its engine's actual acceptance commit.
Only then are retained positions mapped to the common command ordinal for later
comparison. Exact logical time, worker origin and terminal bytes are compared
without normalization. Native snapshots are compared with their own restored
bytes, never with the other engine's private format.

## Observations and recovery

After every neutral command, including refusal, ignored events and duplicate
reports, the adapter compares the complete scheduler view: attempts, original
charged reservations, operation terminals/versions/deadlines, worker state,
holds, effects and execution request/target. It separately compares all original
operation requests, qualified actions, report evidence, acceptance provenance,
operation links, logical credits and stage associations.

Retention views use the same numeric root/write identities and physical-key
mapping in this restricted fixture. Physical generation numbers are consecutive
allocation ordinals within one declared namespace and incarnation. Structured
map keys are serialized as ordered entry arrays; their fields are not omitted.
Roots, write outcomes, generation readiness/revisions, protections, deletion
state and logical clock are compared exactly. The reference pre-creates an empty
cohort; the candidate creates it upon first allocation. Their empty-cohort
representations and configured capacity limits are explicit representation
differences, not equal private snapshots. Tests stay within both engines' limits.

Both states are saved as snapshots and restored after every command. Each snapshot must
round-trip byte-for-byte, all observations are compared again, and the candidate's
disposable index is lost and rebuilt. Rejected, ignored and duplicate commands
must preserve each engine's complete canonical snapshot. These histories thus
exercise every prefix recovery cut of the declared input sequence.

Native commit cardinality is intentionally different and is not compared.
Refusal categories are collapsed to accepted versus refused at the common API;
exact error-category parity is not claimed. Both complete unchanged snapshots
are still required after refusal. Configured bounds differ, and this comparison
does not claim boundary agreement outside its explicitly shared finite inputs.

## Existing publication protection

One fixture creates a real candidate direct publication before enabling terminal
outcomes. The reference receives a trusted initial Publication seed for the same
physical member and lease. Their staging root, one completed write, released
staging guard and live Publication root have the same retained relations.

The candidate's exact head, graph acceptance and serving-grant maps are saved
locally and must remain unchanged across every terminal command. The reference
has no AC-head semantics; none are fabricated for comparison. The publication
root's exact closure and deadline are compared in both retention engines. Typed
closure evidence and write outcomes remain explicit fixture assumptions.
The reference models physical identities and retained relations, not object
hashes or protobuf graph traversal. Stages declare an empty ActionResult and seven
additional Blob members; content and typed-graph validation remain separately
qualified by the candidate's other fixtures.

## Histories and limits

Both SHA-256 and BLAKE3 run through exact no-graph Complete, joined cancellation,
late cancellation and duplicate/conflicting reports, with graph-bearing and
wrong-incarnation refusals. Memory histories cover MissingGrowthEvidence,
NoLargerEligibleWorker, and two genuine growth retries followed by
AttemptBudgetExhausted. Wrong original charge, missing quiescence and
graph-bearing memory reports refuse without mutation.
Growth from an original zero-memory grant uses explicit nonzero observed peak
evidence; nonzero original grants retain their original growth base.

Cancellation and timer histories exercise queued last cancellation, renewed
interest and stale/current expiry, progress-fenced attempt expiry, absolute
execution expiry, retries and final Exhausted. Worker revocation/replacement and
old cleanup histories check retained charges and successor ownership. Unknown
stage writes survive retry, cancellation, terminal acceptance, expiry and explicit
abandonment; late outcomes cannot create a new terminal or head.

This is a finite common-alphabet comparison. It excludes graph Publish and direct
update transitions after setup, graph-bearing ResourceFailed, active cache-hit
admission, seeded Operation/Serving roots, partial-member credit semantics,
arbitrary namespaces, collector/deletion transitions, throughput, and proof of
future byte or physical capacity. It uses positive fresh operation/report IDs;
duplicate admission and repeated abandonment have different API return semantics
and are not silently equated. No production activation or historical snapshot
authentication follows from these tests.
