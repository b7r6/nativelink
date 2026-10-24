# Canonical temporal preflight

The inert `PreparedCanonicalOwner` audits temporal storage facts before its
current synchronization barrier returns an owner. Cold preparation still leaves
Preparing metadata. Sealed fixtures are synthetic; this change supplies no seal
writer, Raft startup, clock, proposal ingress or serving permission.

The private audit holds a `Journal.io` queue guard and database reference in a
blocking task. One read transaction checks the namespace-derived identity,
contract, exact phase, Base format and fixed limits. It validates the applied
state and retained snapshot independently, then streams every retained log row.
The existing log decoder, continuity and singleton membership rules remain in
force. Every retained Submit requires captured ingress facts, and every retained
AdvanceTime must fit the clock range, even when its event was covered, rejected
or ignored. These are stronger canonical storage admission rules than reducer
acceptance.

For every log index after the applied index, the audit stages format/membership
and each event in order, including the uncommitted suffix. Generic and temporal
state invariants run after each event within a batch. This work emits no effects,
persists no state, and advances no committed or applied frontier. Memory is
bounded by decoded records and bounded states; retained history length and
startup work have no total bound here.

The largest finite Action timeout in the seconds profile is 315576000001 ticks,
including upward rounding of fractional seconds. The maximum admitted clock is
18446743758133551613: `u64::MAX` less that duration and one strict-expiry tick.
Each request must match its captured Action projection. A finite running
deadline cannot equal `u64::MAX`; an omitted Action timeout retains that sentinel.

Attempt deadlines must permit a numeric anchor at or before the current tick:
10 ticks for Dispatching, 30 for Accepted, Preparing, Running or Uploading.
Finite running deadlines similarly permit subtraction of their Action timeout.
Interest deadlines use 300 ticks. A cooldown is either the initial zero sentinel
or permits subtraction of 300 ticks. Generic invariants retain the existing
enqueue, terminal, heartbeat, hold and phase checks. Uploading can legitimately
lack a running deadline when the worker skipped Running. Expired deadlines and
retained revoked attempts remain admissible.

A derived numeric anchor does not establish the provenance of an earlier clock
sample. Future deadlines may exceed the admitted clock cutoff. The reserved
arithmetic headroom does not promise that a future clock can reach expiry before
refusing its own next tick. Clock startup, lifetime policy, first-Running sample
freshness and actual expiry remain separate work.

The tests construct bounded synthetic states that satisfy the previous generic
invariants, then exercise actual owner opening. They compare authority metadata
and log bytes before and after admission or refusal. This does not assert that
redb repair or internal filesystem bytes never change. Cancellation fixtures pause
inside the read transaction and check namespace custody and the owned journal
queue. The batch fixture protects against final-state-only validation, but its
stateless request check and intermediate state check are redundant; it cannot
independently distinguish them.

The queue fixture alone clones the private Journal before starting a separate
audit. Releasing the original owner while that audit retains the database tests
the extra alias's custody. The public owner exposes no such alias or audit, so
this fixture is not evidence of an early public release acknowledgement.

## Qualification

The [author receipt](evidence/canonical-temporal-preflight/manifest.json)
preserves an original 33-test baseline: eleven pass and 22 fail at semantic
refusal assertions. Adding the audit and two custody controls passes all 35
focused tests. The restored regression passes 151 library tests and one external
API test, with one existing ignored test; strict all-target checks pass.

Sixteen compiled control executions cover fifteen distinct mutated sources and
executables. Twelve fail their intended semantic assertions; four selected
oracles survive because other checks still reject the fixture. In particular,
generic request identity and per-operation payload validation make the selected
execution-payload guard redundant. The batch checks also cannot be isolated by
their shared fixture. The initial interest mutation fails compilation on an unused
import and is excluded from detection counts. Its corrected source compiles.

All original, fixed, mutated and restored executables are retained in the
[executable supplement](evidence/canonical-temporal-preflight/executables-manifest.json).
Both restored executables match their initial qualified binaries. Independent
source and artifact reviews are distinct from root's actual Bazel execution.

The [combined root receipt](evidence/temporal-terminal-root-integration/manifest.json)
records twelve executed targets, 421 Rust passes, two ignored tests and the server
build alongside the upload repairs. Sixteen of seventeen selected author inputs
match; the external API test retains root's earlier Bazel tracing dependency.
No production constructor, clock or serving gate follows from these results.
