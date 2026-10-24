# Independent accepted-history contract

This is a test-only component specification for the fixed-policy history part
of M4a. It was written without reading the candidate authority transition
implementation, receipt-retirement implementation, or earlier authority and
retention oracles. It imports no NativeLink production crate. SHA-256 and
BLAKE3 are external cryptographic primitives; protobuf framing, transition
rules, lifecycle fixtures, and accounting below are independent.

The component owns admission reservations, accepted associations, publication
status, and their retention pins. It does not own scheduling, worker resource
accounting, AC policy, receipt retirement, provider selection, or namespace
qualification. `types.rs` is the complete public boundary of this oracle.

## Independent facts

`Facts` must come from a separate lifecycle oracle or handwritten fixtures.
Never derive them from candidate `ObservableState`, a candidate snapshot, a
candidate transition result, or production encoding helpers.

- Epoch, execution IDs, original attempt/worker/incarnation, authorization
  immediately before completion, and the exact immutable terminal response.
- The action digest and independently qualified fixed CAS namespace identity.
  The 32-byte namespace is a model fact. It is not evidence that a production
  namespace, credentials, store instance, or provider identity has been bound.
- Operations interested at accepted terminal completion. This is an accepted
  association, not the live-interest bit: lifecycle code may clear live
  interest when it makes an operation terminal. Handles canceled before
  acceptance are absent. Operation and attempt IDs belong to only one
  execution, as guaranteed by the independent lifecycle allocator.
- The global next effect ID, after any preceding AC effect; all other future
  effect-ID obligations; live/prepaid effect slots and metadata outside this
  component; global capacities; normal execution-retirement eligibility.

`Phase::WorkerTerminal` supplies the independently designated terminal worker
outcome to the acceptance event, together with pre-event authorization. It
also supplies immutable committed terminal facts to restore/publication.
`Phase::Open` includes retry-only failures. `OtherTerminal` includes cache
hits, direct-client updates, scheduler cancellation, and expiry. Lifecycle
facts and accounting are immutable inputs; the component never changes them.
Caller-supplied epoch, worker, and incarnation identities may be zero; the
qualified namespace may contain all zero bytes. Allocator-issued execution,
attempt, and effect IDs must be nonzero. The caller advances its independent
global cursor from `Outcome::Allocated`. One authority epoch is immutable for
this component, so restore requires equality to that fixed epoch. A fresh
authority cannot adopt another authority's accepted associations implicitly.
In production the lifecycle terminal transition and history acceptance must
commit atomically; this component does not by itself establish that property.

## Fixed acceptance policy and identity

Every accepted terminal worker completion creates exactly one history
association, including successful, noncacheable, failing, infrastructure-error,
and final `ResourceFailed` completions with exact response bytes. A memory
failure that causes a retry creates no association. Stale completions, cache
hits, direct-client publications, and scheduler-only terminal outcomes create
none. Cacheability and success flags do not select history policy.

Authorization is checked when the association is first accepted. Later
revocation, replacement, or lease expiry does not revoke already accepted
bytes. An exact duplicate completion leaves the existing association and
effect ID unchanged; a conflicting duplicate is rejected.

Identity is `(epoch, effect)`, using the shared effect allocator. AC allocation
may precede history allocation; its ordering and credit are supplied by the
independent lifecycle facts. Identical immutable objects may be shared by CAS,
but distinct accepted executions retain distinct associations and effect IDs.
The association preserves the original attempt, worker, and incarnation.

## Exact object recipe

The existing `HistoricalExecuteResponse` schema has action digest at field 1
and response at field 3. This model writes canonical protobuf framing directly
and never decodes or re-encodes the embedded response:

`object_bytes = recorded_prefix || immutable_accepted_response_bytes`

Action hashes are exactly 32 bytes rendered as 64 lowercase hexadecimal
characters. Action size is a nonnegative protobuf int64. A zero size is omitted
as a proto3 scalar default. A present empty response remains a present message.
The prefix maximum is 82 bytes: 66 for the Digest hash, at most 10 for its size,
2 for its enclosing field, and at most 4 for the response field and length.
The response maximum is 256 KiB. Tests exercise the exact 82-byte boundary.

Object hashing uses the action's SHA-256 or BLAKE3 algorithm over the prefix
and exact response. A separate SHA-256 fingerprint binds the response alone.
The fixed namespace, algorithm, object hash, and byte length are all recorded.
An optional preparation proposal is compared against the fully derived
recipe; it never supplies authoritative bytes, hashes, or destination.

The immutable response belongs to external facts as `Arc<[u8]>`. Association
metadata contains only the bounded prefix, hashes, provenance, object identity,
and status. No second full response is serialized or charged as metadata.
An implementation must preserve the existing immutable response until its
pending publication settles. This model does not retain action inputs or
outputs transitively through the historical response's digest references.

## Admission, activation, and retirement

Activation validates one independently qualified namespace and prepays one
history slot and one metadata row for every unfinished execution. Failure of
any capacity or effect-ID headroom check rejects activation atomically.
Already-terminal executions receive no reservation and cannot receive history
retroactively.
Later admissions prepay the same obligations. Non-worker terminal outcomes
release unused reservations. An accepted history obligation cannot be
released through that path.

Every reservation contributes one ID to the checked global future-obligation
sum, in addition to independently supplied non-history obligations, including
receipt retirement. Acceptance consumes its reservation and the next global
effect ID together. Outstanding pending publications consume effect slots.
`Stored` releases that effect slot, but its compact association remains until
normal execution retirement. Metadata accounting is independent of response
length: 2,048 bytes per reserved or accepted row and 1,024 bytes for the active
header. A maximal-decimal-width JSON row, including all 82 prefix octets and
32-byte hashes, fits this conservative oracle credit. This is not a proof of
the production snapshot, command frame, or journal's byte bounds.

Only an exact `(epoch, effect, namespace, algorithm, hash, length)` successful
publication observation changes Pending to Materialized. Failed and unknown
observations leave Pending unchanged. Wrong, duplicate, or stale observations
have no effect. Materialization means acknowledgment under the supplied store
contract; it makes no provider-durability, latest-head, generation, or serving
lease claim.

Pending history pins its execution, original attempt provenance, and existing
immutable response. Retirement is Busy until publication is materialized and
the independent normal-retirement predicate permits it. Retirement removes
the compact association. Late observations cannot recreate it. No permanent
retired-ID set is needed: external lifecycle IDs are unique and never reused.

## Recovery and qualification scope

The JSON snapshot is an oracle artifact, not a production format proposal.
Restore does not activate, backfill, pick a namespace, or repair malformed
state. It checks pending reservations against unfinished executions, all
association provenance and exact recipe bindings against independent terminal
facts, ID ranges, duplicate effect identities, disjoint reservations, and
resource headroom. Authorization is not rechecked for an accepted association.

The tests qualify this component contract, not a candidate implementation or
full NativeLink system. They do not demonstrate production namespace binding,
an atomic core/journal command, worker relocation, recipient-side I/O,
provider guarantees, process crash recovery, legacy-reader activation,
estimator consumption, or a composed receipt/history proof. The frozen
receipt-only reference remains unchanged; composition must supply the extra
history obligation explicitly. A later candidate adapter must keep these
facts independent and compare complete observations after each step.

Run the pure target with:

```bash
cargo test --locked --profile smol -j 8 -p nativelink-scheduler \
  --test accepted_history_reference_test
bazel test --jobs=8 //nativelink-scheduler:accepted_history_reference_test
```
