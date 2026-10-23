# Activation v2 independent reference

This bounded successor adds cached and unrecorded execution origins, independent
operation terminals, and responses without an ActionResult to the frozen
activation v1 oracle. The exact obligations and source boundaries are in
[CONTRACT.md](CONTRACT.md). Neither v1 nor the publication-head reference is
modified by these files, and their original evidence keeps its original scope.

The scenario supplies separate execution and operation terminal maps. Every
retained value preserves its exact variant, payload and original metadata.
Recorded worker classification, recorded worker acceptance, cache lookup,
authority, and unrecorded provenance stay distinct. Accepted history preserves its
own original acceptance record. A logical terminal time is never converted to a
commit position. A canceled subscriber cannot inherit its execution's later
terminal or accepted-history identity.

Cold activation accepts unknown original provenance when all actual graph and
history obligations are protected. No-result terminals need no invented result
object. Every actual result owner needs an explicit root even when graph bytes
are shared with another owner. Explicit imports require recorded acceptance and
a new authorized migration publisher; this bounded alphabet does not authorize
imports from cache lookup or mere worker classification.

The v1 effect, request, domain-isolation, protection, and late-outcome machinery is
copied additively from the independently authored frozen reference. Original
uncertainty survives an empty request map or empty retained outbox. Every retained
sink intent has a committed disposition. Activation acquires complete protection
atomically; quarantine cannot waive a missing retained root. An old physical
namespace remains outside new collector ownership.

The target depends only on hash and serialization libraries. Cargo also compiles
the package library as its normal test prerequisite, but this oracle imports no
scheduler or publication implementation helpers. The separate Bazel target has
no scheduler implementation dependency.

```sh
cargo test --locked --profile smol -j 8 -p nativelink-scheduler \
  --test publication_activation_v2_reference_test
bazel test --jobs=8 \
  //nativelink-scheduler:publication_activation_v2_reference_test
```

The suite retains fifteen v1 scenarios with the new representation and adds
twelve focused scenarios. These cover SHA256/BLAKE3 cached and unknown origins,
independent cancellation, seven terminal shapes without a result, history independent
of terminal provenance, unauthorized imports, effect provenance without a commit
position, duplicate graph owners, an operation whose execution terminal is no
longer retained, altered operation snapshot facts, an empty captured response,
response integrity, and thirty mixed activation/suffix snapshot cuts.

Twenty-three semantic mutants compile and fail unchanged assertions; baseline
and restored source pass. They cover the inherited effect/protection obligations
and the new provenance, independent cancellation, owner-root inventory, response
integrity, history without ActionResult, and snapshot preservation rules. A
compiler rejection is never counted as an assertion kill.

The finite bounds are eight execution terminals, thirty-two operation terminals
and roots, sixteen effects, thirty-two members per root and enumerated requests,
4096 bytes per payload, and a 512 KiB snapshot. Overflow refuses the scenario;
this is not a production capacity or progress claim.

This model does not extract a real M4 checkpoint or decode protobufs. The input
inventory and result extraction must be independently complete and faithful.
An operation may outlive its execution terminal when it makes no recorded
forwarding claim. This bounded history alphabet requires its execution terminal
to remain represented. It is not evidence that production always retains that
execution. Retention deadlines are explicit proof facts; converting production
reconnect and retention clocks is outside this model. Response checksums detect
inconsistent captured facts, not forged authority. Scenario closures are expected
facts, not candidate-generated proofs. Provider qualification, production format
migration, and power-loss recovery remain separate gates.
