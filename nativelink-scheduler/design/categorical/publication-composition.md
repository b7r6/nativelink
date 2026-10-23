# Publication composition checkpoint

The [candidate](../../tests/publication_candidate/README.md) composes the actual
scheduler `State` with the existing `Coarse` retention engine. Publication checks
the current attempt, completes the real execution, transfers staging protection,
installs operation roots, and selects the latest eligible head inside one outer
transaction. Its temporary M4 mutable-sink intent is consumed within that
transaction. This is an experimental composition, not a production constructor,
journal format, feature activation, or removal of the legacy bridge.

The candidate and [head reference](../../tests/publication_head/README.md) were
frozen separately. The candidate author read the reference contract and types,
but did not read its reducer or tests. Their original sources and the candidate's
explicit restore-validation correction are retained in
[candidate receipts](evidence/publication-candidate/manifest.json). Canonical
snapshot corruption exposed missing preparation-fence and provenance relations;
the correction binds preparation intervals and fences, accepted terminal time,
and the final actual attempt. Initial mutation survivors and their later
isolated regressions remain in the evidence.

A [second correction](evidence/publication-capacity/manifest.json) prevents
invalid staging from consuming the only candidate slot reserved for an execution.
It checks the complete scheduler payload encoding, the distinct cached response,
the actual action and receipt identity, and current authority through a discarded
real `Complete` preview. Publication checks authority again. The independent
comparison reproduced the wrong-action case before this fix; a late predecessor
also cannot consume a replacement attempt's slot. Retained stages still survive
later revocation. Canonical snapshot admission bounds each proposed state, while
a general reserve for future outer metadata remains unqualified.

The [comparison adapter](../../tests/publication_differential_test.rs) owns its
protobuf fixtures and typed member sets. It translates execution and attempt
identities explicitly, drives actual registration and running transitions, and
compares accepted order rather than equating the engines' internal event counts.
Checks cover stable terminal bytes, full physical identities, latest eligible
heads, protected generations, serving grants, late write observations, refused
stale publication, index reconstruction, and snapshot recovery. Request cases
include ordinary hits, skip-lookup execution and coalescing, joined cancellation,
independent noncacheable work, nonzero exit, an explicit execution error, and a
direct update under a different qualified action key.

Two constraints define that comparison. The frozen reference carries source
response bytes opaquely; the adapter independently constructs the exact expected
cache-hit response with `cached_result` set, and checks original worker bytes
separately. See the [response scope audit](publication-head-response-scope.md).
Worker operation and publication deadlines are equal in this common alphabet:
the reference roots worker results until the publication deadline, while the
candidate supports a separate requested operation deadline. Lease boundaries
and recovery are compared under that explicit equality. Broader lease policies
are not established by these cases.

Both digest variants qualify immutable object hashing and physical identity.
The frozen comparison puts its opaque qualified action string into a fixed
SHA256 placeholder scheduler request. The separately qualified
[real-request extension](publication-real-request.md) preserves the original
REAPI payload and complete action binding in that same scheduler State. Its
explicit trusted principal/configuration context also checks each joined
operation's original request. An additive independent comparison now drives
that constructor with captured SHA256/BLAKE3 Action and Command bytes, while the
original synthetic histories remain unchanged. The new cases cover worker
publication, cache projection, skip-lookup/coalescing, noncacheable outcomes,
recovery and revocation. Reference action labels remain opaque; the real fixture
registers distinct immutable inputs under one explicit trusted context.
Physical readiness and complete typed closure are trusted fixture observations
in this adapter. No provider calls or materialization path are implied.

The [actual-worker extension](publication-worker.md) then connects captured
SHA256/BLAKE3 requests, real namespace processes, exact reports, staged filesystem
writes and independently expected typed edges in the same authority State.
Its temporary worker upload CAS remains outside collector ownership, and worker
acknowledgment is explicitly outside that experiment.

The [activation v2 reference](../../tests/publication_activation_v2/CONTRACT.md)
separates execution terminals, operation terminals, history associations, and
recorded versus unknown provenance. Its original frozen admission can accept a
728928-byte state despite a 524288-byte snapshot limit. The
[additive guard](../../tests/publication_activation_bounded.md) constructs and
serializes each candidate before exposing it, preserving the previous state on
refusal. Original and corrected evidence remain separate under
[v2 receipts](evidence/publication-activation-v2/manifest.json) and
[bounded-admission receipts](evidence/publication-activation-bounded/manifest.json).
Neither establishes a real M4-to-M6 activation boundary.

The [retained inventory adapter](../../tests/activation_inventory/README.md) now
reads actual State, retains its exact snapshot and captured bytes, and validates
that its projected public fields still match that source. The original sixteen cases
cover distinct operation terminals, accepted history, explicit identity
bindings, physical preparation, and snapshot/suffix cuts; twelve compiled faults
are detected. An additive eighteen-case correction also captures each operation's
original request and refuses a joined caller outside the trusted execution
context. Its three new compiled faults are detected. Result and history roles in one physical CAS cannot acquire
different namespace identities, and an external principal cannot contradict the
captured request. [Receipts](evidence/activation-inventory/manifest.json) retain
the original sources, failures and exact limits; the
[operation-binding correction](evidence/activation-inventory-operation-binding/manifest.json)
retains its own reproduced failure and receipts. This inventories retained owners;
it does not execute a durable activation or convert unfinished executions.

The explicit private-v4 [terminal successor](publication-terminal.md) adds
no-graph Complete and MemoryFailure reports, cancellation and fenced expiry to
the actual-State composition. Retry classification remains the scheduler's
decision; accepted no-graph outcomes retain exact origin and operation
associations without inventing a result object, root or head. Existing writes
and uncertain reservations survive those lifecycle transitions.

Remaining promotion work includes production request routing, active accepted
history, prepared graph-bearing resource failures, renewed protection, physical
collection, durable publication sections and suffix replay, and production role
construction. Conservative finite admission still needs the shared-authority
workload budget. No M6 gate is declared complete.
