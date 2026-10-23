# Typed closure and durable retention composition experiment

The sibling `typed_physical_retention_test` composes the
[typed closure preparer](typed-output-closure.md), real filesystem objects, the
unchanged finite cohort candidate, and its independent retention oracle.
`tests/retention/typed_bridge.rs` is a test-only singleton wrapper. No production
authority, journal, worker, service, or factory selects it. These fixtures add
composition evidence; they do not complete the M6 publication gate.

## Preparation and acquisition

The wrapper owns one bounded catalogue, a monotonic catalogue revision, root
manifests, and one experimental head. A pending preparation captures the
catalogue revision and cohort fences before physical I/O. Only a completed
typed preparation can supply the flat closure used by `Acquire`. The wrapper
checks the captured revision and every exact catalogue binding, then submits
the closure and captured fences to the existing candidate and oracle. Those
independent implementations decide readiness, retirement, lease, and cohort
eligibility; the wrapper does not replace their checks with physical existence.

Each row preserves the full physical incarnation and generation. The retention
protocol's numeric incarnation is a namespace handle mapped to a persisted
128-bit physical incarnation, not a truncated physical identity. Bootstrap
allocates a fresh random physical incarnation. Equal numeric handles in two
fixtures cannot make a preparation from one namespace valid in the other.
Restoration validates the mapping, row consistency, immutable root closures,
and logical-object associations.

The global catalogue revision is deliberately conservative: even an unrelated
catalogue change invalidates a preparation. A cohort close/reopen invalidates
the captured fence without changing catalogue membership. Retirement is also
terminal when bytes remain readable. After retirement and reopening, a fresh
typed preparation can verify those remaining bytes, but `Acquire` rejects the
retired generation. Physical deletion instead makes fresh preparation fail.

## Durable reliance and ambiguity

A transaction clones the finite candidate, oracle, and wrapper metadata.
Each event is applied independently to the candidate and oracle; replies and
observable states must agree. The wrapper writes their combined checkpoint,
syncs the file, renames it, and syncs the parent directory before adopting the
state and issuing a grant. Head selection and publication-root acquisition
share that test checkpoint. A separate owner-file lock excludes a second local
wrapper. This is not a replicated authority or a production transaction format.

An injected failure before rename leaves the prior checkpoint authoritative.
A failure after rename is ambiguous: no success receipt or new head is exposed
by the failed wrapper, but reopening may recover the committed protection.
Failure after directory sync likewise can recover a grant whose acknowledgement
was lost. The failed wrapper is poisoned and requires reopening. These are
controlled I/O-boundary fixtures, not power-loss certification.

Serving creates a separate committed root naming the selected head's immutable
manifest. Removing or replacing that head releases its publication root but
leaves granted serving coverage intact. Reads check the grant's full namespace,
root, lease, deadline, and exact member descriptor; they do not consult the
current head. Equal root and lease numbers in another namespace do not authorize
a read, even when both namespaces contain identical object descriptors.
Tests perform real reads after checkpoint restoration and reject retirement
intents for every covered generation. Uncovered replacement outputs can be
retired and physically deleted while an older serving grant remains usable.

Expired coverage cannot use cheap renewal. Reacquisition requires complete new
preparation and current fences; it cannot revive retired keys. Replacing the
logical objects with fresh generations permits a new root while old retirement
markers remain intact.

## Bounds and qualifications

The wrapper uses the existing finite limits: 16 generations, 16 roots, eight
objects per closure, four cohorts, and bounded write/delete relations. Catalogue
and manifest vectors have corresponding caps and canonical unique ordering.
Checkpoint reads are capped at 1 MiB before JSON decoding. The envelope includes
version, length, and checksum; decoded state must satisfy the finite limits and
re-encode byte-for-byte. Unknown, duplicate, inconsistent, oversized, truncated,
or differently encoded checkpoints are rejected rather than normalized or repaired.

At most two preparation handles can exist per wrapper. A handle expires after
five authority ticks; expiry prevents further work or acceptance, even if the
caller retains it. Dropping the handle releases its slot. Caller retention can
therefore block new preparations while memory stays bounded. Typed decoding,
graph work, total object bytes, and per-message bytes retain the preparation caps.
Upload bytes are checked against the physical object cap before hashing.

The wrapper stores a bounded flat manifest whose completeness came from the
typed preparer. The independent retention oracle checks protection semantics,
not REAPI traversal. Fixtures separately assert the known complete physical
closure, including the result, Tree container, and file/stream blob, so omitting
an edge cannot be hidden by feeding the same incomplete vector to both models.

The experiment has no worker authorization, action-key protocol, durable
preparation-resume format, Raft read barrier, lease coalescing, root reclamation,
or Azure qualification. New serving-root acquisition follows the existing
candidate's cohort-phase restrictions. Capacity exhaustion and conservative
revision rejection remain explicit errors, not production throughput claims.

## Semantic mutation checks

`tools/typed_retention_mutations.py` changes only the new wrapper, one mutation
at a time, and restores it in a `finally` block. Each mutant must compile before
tests run. Only a compiled mutant producing failing test assertions counts as
a kill; compilation failures and process errors have separate classifications.
Baseline and restored-baseline runs must pass. Raw compile/test logs, hashes,
commands, and the classification summary are retained in the selected evidence
directory. The mutations target omitted closure members, catalogue revision,
full physical identity, preparation fences, persistence before acknowledgement,
and serving coverage after head removal.
