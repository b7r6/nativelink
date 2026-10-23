# Durable receipt retirement authority

The authority component decides when a worker may forget command receipts.
It complements the [recipient protocol](receipt-retirement.md); it does not
change completion acceptance, establish physical cleanup, or release a resource
reservation. Production construction remains outside this experiment.

## Committed decisions

Feature 2 activates the component in the same journal transaction as format
activation. Before activation, state preserves the prior core-5 snapshot bytes. Active
state uses core 6. Feature 1 independently selects the direct scheduler encoding;
feature 2 works with either encoding. The journal rejects receipt commands before
activation, including commands carrying a forged base feature epoch. A follower
may append activation and subsequent commands before applying that suffix, so
validation follows the preceding log rather than only its applied format.

Every current worker has one stream for its process incarnation. `IssueReceipts`
allocates a stable effect identity, captures all three allocation cutoffs, and
retains every unreleased attempt belonging to that incarnation. Revoked attempts
with uncertain processes remain exceptions. The certificate is immutable while
pending; later cancellation or release cannot rewrite an already issued command.
Reconnection preserves it. An accepted new incarnation resets the recipient
stream while preserving the old incarnation's uncertain physical obligations.

An actual reservation release marks the current stream dirty. Acknowledgement
compares the complete pending certificate and incarnation, stores the cutoffs,
and removes only its metadata pins. It preserves any dirty work accumulated
while delivery was pending. A missing or changed certificate is not evidence of
receipt. Transport authentication must be established at the RPC boundary;
the adapter checks the current negotiated session, and the pure event is not an
authentication mechanism.

Pending bindings pin their attempt records, including records released after
issuance. Terminal retirement refuses to discard those records until exact
acknowledgement or accepted incarnation replacement. This preserves the evidence
needed to validate a restored pending certificate and to retry its exact body.

## Bounded state and refusal

Each worker has at most one pending certificate and one acknowledged cutoff
tuple, with at most 1024 retained bindings per certificate. The JSON certificate
is bounded by 256 KiB; its maximum-width protobuf encoding fits the separate
64 KiB wire limit. An oversized exception set defers issuance atomically without
forgetting uncertain work. Exhaustion cannot turn uncertainty into cleanup.

The [existing byte credits](byte-headroom.md) fund each stream header with its
worker rows and each pending binding with the attempt it pins. Maximum-width
fixtures check these paired bounds. Issuance also preserves the effect identities
already promised for finalizing admitted executions. It refuses before consuming
the allocator when that headroom is unavailable.

Restore checks certificate identity uniqueness, simultaneous allocation cuts,
exact attempt associations, cancellation order, complete uncertain exceptions,
and the paired byte limits. A cancellation cannot predate its dispatch or belong
to an authorized attempt. Allocator-issued row IDs must be nonzero. This last
rule does not change the separate caller-supplied epoch, worker, incarnation,
or session domains in the pure model.

## Independent comparison and counterexamples

The [independent reference](../../tests/receipt_retirement/README.md) shares only
public identity types, inert data structures, and declared bounds. Its author
did not inspect the candidate transition implementation. Scenario facts and
allocation cursors are supplied independently; they are never copied from
candidate observations. The reference computes protobuf length itself.

Seven reference tests include a bounded exploration of 739 metadata states and
6520 transitions. Six separate real-State scenarios compare complete receipt
metadata and replies, and verify that receipt events preserve the ordinary
lifecycle state. They cover an old active attempt beside 48 later completions,
immutable pending bodies, cancellation, cleanup, replacement, delayed proposals,
and snapshot plus suffix recovery. This is finite scenario evidence, not an
exhaustive equivalence proof for the complete scheduler.

Candidate tests add 128 completion/retirement cycles around one active attempt,
1026 uncertain exceptions and atomic deferral, maximum-width budgets, allocator
headroom, and malformed snapshot relations. Review reproduced four malformed
snapshot cases before adding the missing relation checks. A further regression
demonstrated
12 coordinated zero-ID corruptions across base and activated state before the
allocator-domain guard was added. These were restore-validation gaps; no valid
transition history was shown to produce those states.

The isolated mutation runner compiles each fault before executing its named
assertion. Eight faults exercise omitted uncertain exceptions, changed-body
acknowledgement, lost dirty work, missing retirement pins, spent finalization
headroom, stale incarnation metadata, and accidental resource refunds. Compiler
errors and process failures never count as semantic detection. Both the original
and restored source must pass the complete receipt unit suite.
