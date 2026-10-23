# Additive snapshot-bound audit of frozen activation v2

The frozen activation-v2 model can accept a transition whose resulting authority
state cannot produce a snapshot within its declared 512 KiB limit. This is a concrete
reference defect in the bounded replay contract; the passing semantic scenarios
did not reach the combined byte bound. It does not relax production retention or
permit dropping owners, graphs, or obligations to fit a snapshot.

A compiled probe uses thirty-two roots and twenty-nine operation owners sharing a
thirty-two-member result graph, while preserving the original worker/history
fixture and every required proof. Every declared count and per-payload bound is
respected. The initial model snapshots to 326248 bytes. Activate returns Applied,
but its resulting canonical state is 728928 bytes and snapshot returns Bounds
against the 524288-byte limit. Duplicated typed members in root declarations and
protection records, plus explicit protected-generation sets, cause the growth.
The frozen constructor similarly checks structure without checking total encoded
state size. The original source and artifact hashes remain unchanged.

The additive `publication_activation_bounded` module guards the frozen model's
constructor and every transition. It builds a candidate state, validates and
serializes that candidate under the existing bound, and installs it only after
snapshot succeeds. A refusal preserves the entire previous state. Restore still
uses the frozen model's canonical encoding, original-inventory anchor, structural
checks, and byte limit. This is an explicit admission change, not a larger limit
or removal of retained obligations.

Focused checks preserve the original counterexample, reject an oversized initial
inventory, compare ordinary activation/collection/recovery with the frozen model,
and construct an exactly full 524288-byte active snapshot whose later protection
refresh must refuse atomically. The first version of the final test had an eager
`then_some` arithmetic overflow in its fixture search; the failure log is retained
and the fixture was corrected to evaluate the subtraction only after the bound
check. No expected reference behavior was changed to hide that fixture error.

This guard provides bounded reference admission, not production capacity credit,
compact state representation, fair progress, or real snapshot-store durability.
Any extractor qualification using v2 must use this guard or equivalent atomic
byte admission and cite this audit alongside the original frozen evidence.
