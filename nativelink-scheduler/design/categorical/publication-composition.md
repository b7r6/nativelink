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
The composition currently puts the opaque qualified action string into a fixed
SHA256 placeholder scheduler request. This is not real BLAKE3 request
normalization or a proof that wire requests bind every qualified-key component.
Physical readiness and complete typed closure are trusted fixture observations
in this adapter. No provider calls or materialization path are implied.

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

Remaining promotion work includes actual request normalization, active accepted
history, terminals without an ActionResult, resource failures, cancellation and expiry,
renewed protection, physical collection, durable publication sections and suffix
replay, and production role construction. Conservative finite admission still
needs the shared-authority workload budget. No M6 gate is declared complete.
