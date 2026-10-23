# M4-to-M6 activation-only reference

This independent finite reference covers the committed transition and retained
obligations. It has no publication engine, Raft implementation, provider driver,
production activation switch, or claim that M6 is complete. Its inputs are
scenario-owned facts; no candidate implementation supplied these expectations.
The normative sources are the M4-to-M6 activation section and immutable
publication/retention rules in `../../design/categorical/source-plan.md`, plus
`typed-output-closure.md` and `physical-filesystem.md` in that directory.

## Contract

The legacy inventory contains accepted terminal bytes and original acceptance
provenance, every retained mutable-sink effect, any observed request identities
(including unknown outcomes), retained history effects and exact immutable bytes,
and a complete finite set of retained result/history roots. Activation requires
an explicit disposition for exactly every old sink effect: settled, converted to
an explicit selected import, or retired. All original records survive unchanged.
The current M4 outbox does not authoritatively enumerate sends or retries. Each
effect therefore supports opaque exposure with unknown request count and
identities; an empty request map cannot imply that nothing was issued. Observed
request rows are additional evidence, not a required complete enumeration. Only
explicit complete effect-quiescence evidence permits a settled disposition.
A successful retry does not settle another unknown request. Retirement does not
prove a request stopped. Late observations and independently proven quiescence
remain representable after the legacy driver disappears. Existing late-outcome
and quiescence records are also preserved.

A separate opaque old-domain obligation covers requests whose effect was already
removed after another retry succeeded. Empty retained outbox does not clear this
obligation. Only explicit domain-quiescence evidence can settle it; no deleted
effect tombstones or invented request identities are required. Namespace
isolation keeps these unknown requests away from the new view and collector.
An observation without a previously identified request cannot be attached to a
made-up identity; the opaque effect/domain obligations retain that uncertainty.

The new view starts cold unless the committed activation explicitly names
imports. Imports require an authorized migration publisher, original accepted
provenance, eligibility for AC, and complete verified protection. Revocation of
the former worker never invalidates accepted authority. This bounded reference
allows at most one selected import per action at the activation boundary; future
publication ordering and direct updates belong to the later publication model.
Late old outcomes cannot select a head or modify any terminal response.

Both old and new logical namespaces require externally qualified physical
bindings. Equal namespaces or aliased physical destinations are rejected. The
new namespace must be independently established cold; issued old requests must
remain confined to the old destination. Guessed destinations and implicit
imports are rejected. This model never grants collection ownership over the old sink.
That conservative scope does not settle any unresolved request.

Every retained result and history root requires complete coverage before
activation succeeds. Missing evidence is a migration blocker: the transition is
refused and all original obligations remain. A quarantine label cannot waive an
obligation or turn refusal into a release claim. Pending history retains its
original effect identity, bytes, destination, request uncertainty, and accepted
origin. A pending acknowledgment can coexist with verified physical bytes; a
pending object that cannot yet be verified blocks this oracle's activation.

Typed completeness and physical verification are independently supplied facts,
not inferred from digest-shaped bytes or physical presence. The oracle compares
against the scenario's exact typed member set, checks the full physical
incarnation/generation and current catalogue/fences, requires ready generations,
and checks protection covering every required generation. Once acquired,
coverage remains an authority obligation; this model does not implement expiry
or retirement. Collection permission is bounded by all retained-root deadlines.
A later fence change does not retroactively revoke a valid acquisition.

## Deliberate limits and ambiguities

The plan leaves the production migration command schema, import ordering within
one commit, proof transport, and protection of history bytes not yet physically
materialized to later design. This oracle chooses explicit imports with unique
action keys, externally verified evidence, and refusal until every retained root
has protection. These are finite qualification choices, not operator selectors.
The old-effect inventory must be obtained at the same committed boundary as
activation by the eventual implementation; a caller-supplied partial inventory
cannot establish completeness in production. Here the scenario owns the entire
retained-intent universe and the command must account for every member. Opaque
domain exposure separately covers historical requests that universe cannot name.

The root inventory states exact required typed edges. Protobuf traversal,
provider durability, acquisition/retirement concurrency, consensus, operational
namespace qualification, collector execution, future head changes, and proof
cryptography are outside this transition reference. No provenance reference
implicitly retains an action's input graph. Bounds are fixture limits, not a
production capacity or performance claim.

Snapshots retain acquired obligations and original facts. Restore checks their
structural invariants and exact equality with the scenario's original inventory;
it does not authenticate an arbitrary replacement for trusted external evidence.

## Qualification

The 15 tests cover both digest algorithms, every subset of the retained effect
and root inventory in the fixture, explicit imports, uncertain old requests,
opaque exposure with empty inventories, history continuity, typed protection,
namespace aliases, collection deadlines, rejected transitions without mutation,
and snapshot replay at every cut in two late-outcome orders.

Twelve compiled semantic mutants are rejected by unchanged tests: omitted
dispositions, namespace aliasing, omitted typed edges, stale fences, retired
generations, unsupported settlement, empty-request and empty-outbox shortcuts,
worker revalidation, discarded prior observations, late head changes, and
truncated physical generation identity. Baseline and restored source pass.
Compiler failures do not count as mutation kills.

```sh
cargo test --locked --profile smol -j 8 -p nativelink-scheduler \
  --test publication_activation_reference_test
bazel test --jobs=8 //nativelink-scheduler:publication_activation_reference_test
```

The Bazel target depends only on the digest and serialization libraries. Cargo
also builds the workspace crate, but this test imports no production scheduler,
store, retention, publication, or protobuf implementation.
