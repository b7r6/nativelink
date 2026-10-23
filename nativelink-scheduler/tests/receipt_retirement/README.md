# Independent receipt-retirement reference

`reference.rs` models the optional authority component from its agreed public
contract. It shares identity types, inert data structures, and published bounds
with the candidate. It does not import the production reducer, allocator,
accounting, state restore, or wire encoder. The model author did not read the
receipt candidate's transition implementation.

The input `Facts` is an independent scenario projection: current worker
incarnations, original attempt bindings and release facts, execution obligations,
and the three next-allocation cursors. It must never be populated from candidate
observations. Cancellation identities in these facts retain the scenario's
original command history even after the ordinary cancellation effect is consumed.

The modeled transitions are:

- Activation creates one dirty stream for each current worker. Repeating
  activation leaves existing streams intact.
- Issuance checks scope and the acknowledged certificate identity before checking
  for existing pending work. It captures every unreleased target-incarnation
  binding, allocates one effect identity, and freezes all three inclusive cuts.
  Rejected issuance changes neither metadata nor allocation cursors.
- A pending certificate is immutable. Later cancellation and confirmed release
  change lifecycle facts without rewriting that certificate. Its bindings pin
  their attempt records until matching acknowledgement or accepted incarnation
  replacement.
- Acknowledgement compares the full certificate and scope, records its floors,
  and releases only the certificate's pins. It cannot refund capacity, complete
  an execution, alter ordinary effects, or erase a release that happened while
  the certificate was pending.
- Accepted replacement creates a fresh stream for the new incarnation. It
  leaves the old incarnation's physical obligations unchanged. Reconnection
  within the same incarnation preserves the stream.

Only a real transition from an unreleased original reservation to a released
reservation invokes `released`. Duplicate cleanup must not manufacture dirty
work. Cleanup for an obsolete incarnation cannot dirty its replacement's stream.

Issuance preserves the independently specified effect-identity headroom. After
allocating the certificate, checked addition must still fit:

```text
next_effect + 1 + sum over nonterminal executions of
    (3 - attempts_started) * 2
    + (current_attempt exists ? 1 : 0)
    + (cacheable ? 1 : 0)
```

The certificate has at most 1024 bindings, at most 256 KiB of compact JSON,
and at most 64 KiB in its specified protobuf representation. The reference
computes protobuf field sizes directly from scalar varint widths and nested
message framing; it does not call the production encoder.

`differential.rs` provides a boundary trait for separately authored real-State
adapters. Its driver compares replies and complete receipt metadata, and checks
that every issue or acknowledgement preserves the complete ordinary observable
lifecycle state. Baseline updates use explicit scenario facts and expected
replies. The update closure receives no candidate observations. Checkpoint tests
restore the model and candidate separately.

Run the standalone reference scenarios with:

```sh
cargo test --profile smol -p nativelink-scheduler \
  --test receipt_retirement_reference_test -- --nocapture
```

The suite includes immutable pending bodies across later cancellation and
cleanup, exact acknowledgement correlation, incarnation replacement, count and
identity bounds, malformed independent snapshots, 48 newer completed attempts
around one old active attempt, and a bounded metadata exploration. The explorer
prints its actual state and transition counts. These model tests qualify the
reference; candidate equivalence requires the separate real-State adapter and
its reported differential results.
