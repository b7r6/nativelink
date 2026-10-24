# Finite publication acceptance and head reference

This independent reference covers the M6 `Publish` boundary, stable operation
results, fixed request matrix, and protected head selection. It imports no
production scheduler, provider, retention candidate, closure preparer, or earlier
oracle implementation. The normative inputs are `source-plan.md` and
`retention-contract.md` under `../../design/categorical/`.

## Contract

The scenario owns immutable result bytes and their independently known complete
typed physical closure. Qualified action keys are opaque: principal, instance,
digest algorithm/digest, platform properties, and execution configuration cannot
be discarded by interpreting a short digest as the entire key. A staging record
owns stable write IDs and a bounded provisional guard. It is invisible to AC.
Preparation is identity-bearing, exact, independently verified, and bound to
catalogue revision and retirement fences. Successful preparation does not
authorize a later acceptance after worker replacement or staging expiry.

One ordered `Publish` validates the current worker attempt/incarnation or explicit
client authority, current preparation, and complete protection. It atomically
records immutable accepted provenance, stable terminal responses for waiting
operations, result/operation roots, and the eligible latest-commit-order head,
then transfers staging coverage without a gap. Direct client provenance has no
worker attempt fields. Later revocation never revalidates accepted authority.
Closed worker incarnation numbers remain recorded and cannot be registered
again. Publication coverage must reach every waiting operation's requested
result deadline; restored operation coverage preserves that promise.
Old write observations and delayed view materialization cannot change a head or
terminal. All late write obligations remain recorded after staging expiry.

Ordinary cacheable requests may use a protected completed head or coalesce with
an actual unfinished execution. Cacheable `skip_cache_lookup` requests bypass
completed heads but may join unfinished work and remain publishable.
`Action.do_not_cache` requests always create distinct executions. Their terminal
results, and error/unsuccessful worker results, retain protected operation roots
without entering AC. Canceling one joined operation while another still waits
prevents that canceled operation from receiving the worker terminal association.
Last-operation cancellation and accepted-history production are outside scope.

Serving selection checks the authoritative head and current read barrier at one
ordered point; a captured old head cannot acquire a grant after replacement.
An adequate live result root permits a separate bounded serving grant. Head
replacement releases only its result root, retaining operation and serving roots.
Expired result protection requires complete fresh preparation or a miss; an
older head is never selected as fallback. The disposable index may lag or be
lost, but cannot choose a result. Rebuild projects the same authoritative live
head associations without inventing acceptance.

All time is committed logical time. Stage guards expire within ten ticks and
leases within 100 ticks. Finite caps cover operations, executions, stages,
accepted records, members, payloads, writes, and serving grants. A stage reserves
its acceptance/root storage within this fixed representation. Retained identity
rows are not compacted; admission eventually refuses under infinite churn.
This is not a production capacity, resource-budget, or M7 progress claim.

Publication and operation roots use explicit protected-generation sets. Physical
availability, namespace qualification, REAPI traversal, provider writes/deletes,
clock conversion, consensus/read-barrier implementation, root retirement,
production formats, and power-loss recovery are separate qualification gates.
Scenario facts represent authoritative verification, never untrusted RPC claims.
Snapshots are capped at 2 MiB, preserve the full scenario and authority state,
and require canonical encoding on restore. They check structural invariants,
not the cryptographic authenticity of arbitrary replacement authority facts.

The model reads the authoritative head map directly. Materialized index entries
are disposable acceleration data and are never an independent acceptance
source. Each `Serve` is a conservative ordered transition that records coverage;
the model does not claim an optimized production read-barrier or lease-reuse
implementation. Replies describe model transitions, not successful real RPCs.
Rejected and ignored events leave the full model unchanged. The supplied ordered
position must follow the last state-changing position; this finite transition
protocol does not prescribe how a Raft adapter accounts for duplicate log entries.

Direct-client fixtures cover only canonical successful cacheable ActionResult
updates. The plan's fixed success/error-free policy appears in the worker
publication section; it does not by itself settle all direct-update semantics.
The oracle's response bytes for a direct source describe the expected cached
operation encoding, not an UpdateActionResult request field. Actual RPC checks,
including Action and Command objects uploaded earlier, remain outside scope. Other
direct-result admission policies remain an explicit design residual; scenarios
outside this narrow direct-update alphabet are refused, not presented as an
implementation of unspecified protocol policy.

## Qualification

Eighteen scenarios pass with both digest families represented. They include
every cut through a 17-event admission/staging/acceptance/rebuild history and
every cut through six permutations of late observations, materialization, and
revocation after A/B acceptance. Rejections preserve the complete snapshot.

Sixteen semantic mutants compile and fail unchanged assertions. They target
stale attempts/incarnations, expired staging, stale fences, missing typed edges,
retired generations, missing operation protection, conflated lookup/cacheability,
noncacheable coalescing, canceled subscribers, late head selection, stale-index
serving, revoked serving grants, expired-index reconstruction, unauthorized
clients, and truncated physical identity. Baseline and restored source pass.
Compilation failures are not counted as semantic kills.

```sh
cargo test --locked --profile smol -j 8 -p nativelink-scheduler \
  --test publication_head_reference_test
bazel test --jobs=8 //nativelink-scheduler:publication_head_reference_test
```

The standalone Bazel target depends only on digest and serialization libraries.
Cargo also builds workspace dependencies; the test imports no production module.
