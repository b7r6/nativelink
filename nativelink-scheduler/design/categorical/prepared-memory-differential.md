# Prepared memory differential comparison

The additive comparison exercises the frozen publication candidate against the
independently authored prepared-memory relation. It uses the audited version 2
reference, whose five disagreements with the original reference remain recorded
in the audit evidence. Neither engine is changed by this comparison.

The candidate source freeze is `1bccba3acb85e7153798ba2b29d264050584765c`.
The independent reference was frozen at
`8b0dc3f3bf41b4027e4d05d70f5414830d228bce`; its audited successor is
`d9be5a0d8ed56eb6be2dbe338f3daa483f0e34d6`. The adapter author read only the
neutral types and scope before that independent freeze. Reading either engine
for translation and observation began after the freeze.

Run the comparison with:

```sh
cargo test --locked --offline --profile smol -p nativelink-scheduler \
  --test prepared_memory_differential_test
bazel test //nativelink-scheduler:prepared_memory_differential_test
```

## Input boundary

The ten comparison tests contain 34 histories. Every history uses SHA-256 or
BLAKE3, one qualified action and execution, joined operation handles, one worker
incarnation, and at most three actual attempts. The adapter asserts execution
identity 1. The immutable scenario owns three eight-member graphs: an
ActionResult, stdout blob, Directory, Tree, and four output-file blobs. The
fixture computes their digest identities directly. Expected closure membership
comes from those typed scenario inputs, never candidate traversal output.

The physical catalogue starts with all 24 generations ready and unowned in one
namespace/incarnation pair. Candidate setup completes and releases a bootstrap
write/root for each generation before admission. The comparison accounts for
those retained records explicitly. This setup does not exercise execution-owned
physical allocation, physical-credit consumption, competing admissions, or
exhaustion of the global catalogue. Eight-member graphs exercise complete graph
and write obligations; they do not establish admission equivalence at capacity.
The candidate's unused physical credit is checked as eight while the execution
is open and zero when closed. Current and prior stage identities are checked
against the independent retained stage/attempt history.

Each operation is admitted through time 100. Acceptance may request time 80;
terminal graph acceptance must still prove operation coverage through 100.
Retry only needs its acceptance deadline. Report IDs are nonzero. Payloads,
context strings, and labels fit both engines' declared byte limits. The tests
remain within the independent model's 192-changing-event history bound.

Commands are admission, selected scheduler events, memory staging, write
observation, preparation, prepared acceptance, no-graph memory reporting, and
abandonment. Scheduler events used here are registration, matching, capacity
updates, cancellation, worker unavailability, and clock advancement. Ordinary
admission includes both skip-cache and resolved-miss requests. Raw scheduler
completion, memory-failure, lookup-result, and observe events are outside this
alphabet. Provider deletion, collector phases, and namespace fencing are also
outside it: a candidate namespace fence changes cohort phase, while the neutral
physical fact exposes a fence revision without that phase.

Neutral preparation-only fields are fixed: proof ID equals stage ID, complete
and verified are true, and protected keys equal typed membership. The adapter
asserts those restrictions and requires the result identity to equal the staged
scenario result. Incomplete evidence is exercised by removing typed
membership and its corresponding key; stale evidence uses an incorrect or
superseded catalogue revision. Stage payloads, attempt identity, and worker ID must equal their immutable
scenario values; the adapter asserts this domain without returning a synthetic
refusal. Incarnation, epoch, charge, and quiescence remain variable for the
explicit invalid-authority cases. Stage payloads come from their scenario;
this comparison does not vary ExecuteResponse diagnostics independently of a
scenario's graph.

## Observation boundary

After every command, including rejection and duplicate delivery, the adapter
compares the complete scheduler observation, original per-operation requests,
qualified action context and semantics, exact retained report/result payloads,
retry targets and final reasons, acceptance arguments, operation delivery links,
stage membership, preparation, outcomes, and report provenance. Failure results
must install no action-cache head or serving grant.

Retention comparison accounts for every root and write, including expired
bootstrap records and unresolved writes belonging to abandoned or expired
stages. It checks physical identity/readiness/retirement, successful-write
revisions, stage-local external write IDs, internal write uniqueness, full stage
closure, every live root owner/closure/deadline, and the complete protected-key
union. The one cohort stays allocating with revision zero and no collection or
delete records.

Both engines produce a snapshot and restore it after every command. Each restored
snapshot must reproduce exactly; the candidate's derived index is discarded and
rebuilt, then the observations are compared again. Consequently each history
continues through every prefix restore cut, including late writes and retries.
Refusals and duplicates must leave each engine's own snapshot byte-identical.

Native acceptance positions must equal the accepting engine's current commit.
Positions are then mapped to the shared command ordinal: the candidate has
bootstrap commits and the reference has its own replay cursor. Earlier position
mappings are retained across no-ops; position values are never erased or replaced
with a constant.

Native error enums have different domains. This comparison normalizes them to
refusal while retaining both native errors and the complete command/fact history
in a disagreement. It does not claim equality of error messages or refusal
subcategories. The candidate's void Observe and Prepare APIs report an identical
existing value as success; only those two APIs normalize unchanged success to an
explicit duplicate. Stage duplicate delivery remains conflict. Acceptance uses
its native explicit duplicate result and must replay before checking expiry,
current worker authority, or publication-ID reuse.

## Cases and limits

The histories cover both capacity-change directions at acceptance, all three
resource-failure reasons, zero peak evidence, retry through the third attempt,
joined callers and a cancelled sibling, both graph/no-graph orderings, proof
coverage failure without scheduler charge release, incomplete and stale proofs,
retry publication-ID reuse, stage-local write-ID reuse, and late true/false
receipts after acceptance, abandonment, expiry, and restore.

All five audit counterexamples are explicit cases: preparation beyond the lease
bound, reuse of a write ID in a later stage, repeated abandonment, duplicate
admission, and ordinary cache-miss admission. The tests also reject incorrect
original charge, missing quiescence, stale epoch/incarnation, and competing report
identity for the same attempt.

This is bounded differential evidence for the specified command language. It is
not a production activation, provider proof, unbounded equivalence theorem,
global capacity proof, or evidence that the original reference freeze had no
errors. Candidate and reference mutation checks and preserved first-run artifacts
are recorded separately with their exact source and command results.

## Executed qualification

All eighteen cases passed after the adapter review and again after restoring
the source from seven separately compiled faults. Each faulty executable ran
its exact named comparison case and failed a semantic assertion. The faults
change memory classification, current graph evidence, retry staging protection,
final failure reason, operation protection, duplicate acceptance, or peak-growth
evidence. The corrupt-snapshot validation fault is excluded: restoring valid
history prefixes does not test refusal of corrupt snapshots.

Strict Clippy and repository hooks passed. The independent adapter review and
the [author receipt](evidence/prepared-memory-differential/manifest.json) retain
the frozen source, first compiler/style diagnostics, literal fault replacements,
each compilation and test result, and the restored source checks. No semantic
disagreement occurred against the audited reference during this campaign.
The [integrated Bazel receipt](evidence/prepared-memory-differential-integration/manifest.json)
records a separate executed pass of the eighteen-case target and 151 unchanged
scheduler source/build inputs. These checks do not repeat the earlier full
workspace checkpoint.
