# Real request binding for the publication candidate

This test-only extension admits a captured REAPI request into the same actual
scheduler state used for publication acceptance. It closes the synthetic
`ActionKey` admission gap in the publication candidate. It does not activate a
production journal, collector namespace, gateway, or RPC adapter.

## Explicit trusted context

Each real-mode authority accepts one explicit `TrustedContext`: a principal and
a semantic execution-configuration identity supplied by the trusted caller.
Request metadata and an action label cannot supply or replace either value.
`RequestPayload::principal()` exposes the principal captured by trusted ingress;
it introduces no authentication decision.

This restriction is necessary because the actual scheduler's compatibility
predicate does not carry an external configuration identity or principal for
every request. Accepting several such contexts into one state could coalesce
work across contexts even if the completed-cache index separated them. A foreign
context is rejected before admission, without rewriting `ActionKey` or
`execution_scope`. Production multi-context routing remains unqualified.

Each registered action label resolves to an `ActionBinding` containing the
trusted context, all four original `ActionKey` fields, the complete selected
platform, and `do_not_cache`. Action platform selection and command fallback
come from the captured request's canonical projection. Resource properties remain
in the binding even though the scheduler separately normalizes them into
resource quantities. Duplicate action keys under different labels are refused.

## Admission, dispatch, and publication

`admit_request` requires the complete original `RequestPayload`, validates its
canonical projection, and passes the original `Request` unchanged to the actual
scheduler. Stage, Publish, and restore use the same full request-to-binding
comparison. Worker candidates must use the binding's digest algorithm for their
result and every supplied closure member. Attempt, execution, worker, and
incarnation remain actual scheduler identities. Current Stage authorization and
Publish acceptance reuse the actual scheduler reducer.

The original synthetic constructor remains available for the previously frozen
comparison fixtures. Real-mode authorities refuse synthetic admission. The later
[direct-client extension](evidence/publication-direct-client/manifest.json)
requires an explicit trusted grant for a complete registered action binding;
its private permission ledger is not production RPC authentication. No arbitrary request label can impersonate a registered binding.

`execution_scope` remains an in-flight compatibility fact. It is absent from the
completed-cache binding. A successful cacheable request with skip-lookup set can
publish the same completed-cache head that an ordinary request later reads. A
`do_not_cache` request neither shares nor publishes a completed-cache head; its
accepted result still obtains operation protection.

## Qualification and limits

The focused cases exercise SHA-256 and BLAKE3 with every combination of
skip-lookup and `do_not_cache`. They check the original dispatch request, preserved
unknown protobuf bytes, actual scheduler attempt identities, atomic completion
and publication, a fresh cached response, completed reuse across different Buck2
invocations, and bypass of that completed head. Separate cases cover action and
command platform selection, trusted context refusal, all four `ActionKey`
fields, absent or inconsistent request payloads, algorithm mismatch, action-label
substitution, and cross-component snapshot corruption.

Real snapshots use private test format version 2, with explicit bindings.
Synthetic snapshots retain version 1. Restore validates the exact version,
canonical bytes, context, bindings, original requests, staged candidates,
accepted results, and operation relations. Every joined operation's retained
original request is checked independently of its execution's representative.
Synthetic mode requires absent request payloads, so removing real bindings and
changing the version cannot downgrade captured requests into synthetic mode. This is relational validation of a
trusted snapshot; it does not authenticate a coherently replaced history.

These cases use an explicitly trusted fixture observation for the result object.
They do not execute a process or prove filesystem readiness. The separate
physical-publication fixture proves owned-filesystem preparation for its known
closure. Combining actual worker output with that path is a further qualification
step. Candidate row credits and the current-snapshot byte bound also retain the
limits documented in the publication candidate README: they do not prepay every
future byte obligation or establish production throughput.
