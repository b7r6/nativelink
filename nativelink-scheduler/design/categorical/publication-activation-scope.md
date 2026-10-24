# Additive scope audit of the frozen activation reference

The frozen activation reference does not represent every terminal operation that
real M4 can retain. Its checked terminal universe is limited to worker-accepted
result terminals with recorded execution, attempt, worker, and incarnation
provenance. This is a qualification gap, not permission to omit other retained
operations during production activation.

The frozen `types.rs` gives every `Terminal` an `Origin` containing execution,
attempt, worker, and incarnation. `validate_legacy` rejects zero execution or
attempt identifiers. The README describes a complete accepted-terminal/root
inventory without stating that narrower source limitation. A retained terminal
produced by a cache lookup can have exact returned result bytes and a committed
lookup decision without any accepted worker attempt for that operation. The
frozen type cannot represent that fact faithfully. Inventing an attempt or
mislabeling a cache lookup as worker acceptance would violate provenance
preservation.

Consequently, the 15 passing scenarios and 12 compiled mutation kills qualify
the stated worker-result subset. They do not establish completeness of a real
M4-to-M6 terminal inventory or complete the activation gate. The separate opaque
legacy-domain exposure marker covers unknown old provider requests; it cannot
substitute for missing retained operation-result roots.

An additive successor contract must distinguish worker acceptance from cached
lookup provenance and preserve each terminal's exact original response and
retention obligations. A cache-hit operation needs its own protected result and
typed output closure even when it creates no accepted-worker publication. Where
the old path lacks an immutable result object, activation must materialize and
verify the exact retained bytes before claiming coverage, or remain blocked.
Import into the new AC view requires an explicit migration decision and verified
protection; a historical cache lookup alone cannot manufacture accepted-worker
authority. A full inventory audit must also classify scheduler-generated errors
or cancellation terminals that have no ActionResult, preserving those terminal
facts without inventing a result graph.

No frozen file or archive was changed by this audit. Original artifacts remain:

- Patch SHA256:
  `31126491ec5d46adabb0a5ce672f498f1e10bb912ab1c17e6aada66871e6211a`
- Evidence archive SHA256:
  `4ac412084c7b29c7e6cd12d9e5fb1ceaefd50ee38a360d1c806e070c3cabcbe7`
- Reference SHA256:
  `0d61e87111a9149ea318ae9ec3cfb783c3cecd7754a9882c309b2776eadd3722`
- Types SHA256:
  `f5010d9d7da8291d764201f398b5ab65f894d8f8317d0d9ab429819b12e46044`
- Tests SHA256:
  `b8f9b599df189bd149234f8dab852ba6823ad6f6417d42d2d9820089b6406ba5`

This audit relies on the frozen reference's public types and validation plus the
root agent's independently reported real-M4 cache-hit path. It does not read a
new activation candidate or change expected outputs to fit one.
