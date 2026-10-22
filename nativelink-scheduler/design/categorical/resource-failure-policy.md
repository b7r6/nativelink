# Resource failure and infeasibility: inventory and next policy proposal

Status: bounded source review and agreed pure-model memory policy, 2026-10-22.
The independent reviewer attacked the policy before candidate implementation.
Memory escalation is implemented only in the experimental reducer; production
worker classification, authenticated translation, and canonical REAPI projection
remain unqualified. Terminal infeasibility remains a proposal, not implemented.

## Observed behavior and incident coverage

The reviewed `escalation_test.rs`, `unsatisfiable_action_test.rs`, and
`unsatisfiable_tracker.rs` have no diff from the plan's pinned
`a7bb376e6f66284eb37a77b22d2f258c03ba9a7f` baseline. The test families establish:

- A memory kill can raise the next reservation, preserve the operation handle,
  stop at a ceiling or step budget, and retain a useful final failure.
- Legacy application failures, worker losses, and memory escalations use
  separate configurable budgets; defaults are 3, 10, and 8 respectively. A
  configured class ladder can replace multiplication and end by taking a
  worker's memory and CPU. Preserving those selectors is not a target goal.
- A busy compatible fleet is distinct from an incompatible fleet. An empty
  fleet has a separate optional timeout. Newly submitted work must not inherit
  immediate failure from an old already-expired property shape.
- Equivalent worker churn does not restart the old infeasibility clock; a
  capable worker appearing does. Stale/unknown peer census cannot justify
  failure. Saturation does not hide an incompatible queued action.
- Diagnostics distinguish a missing property, a value mismatch, insufficient
  numeric capacity, and a combination that no single worker offers. Client
  messages omit private worker property values. Timer wakes and bounded failure
  batches are part of progress, not only logging behavior.

Relevant implementation seams:

| Source | Meaning or limitation |
| --- | --- |
| `nativelink-scheduler/src/api_worker_scheduler.rs:1316`, `fleet_largest_worker` | Chooses largest declared memory among non-draining workers, without filtering by the failed request's exact properties, CPU, or other requirements. |
| Same file, `maybe_escalate` | Uses the last usage report's `KilledMemory` tag; takes its echoed reservation before the original scheduler value; grows by a percentage or ladder step; caps at the chosen worker; can also replace CPU at the last step. |
| Same file, `record_action_resource_usage` | Stores the latest report associated with worker and operation before processing completion. The new attempt protocol must fence this more precisely than an operation string. |
| `nativelink-scheduler/src/simple_scheduler_state_manager.rs`, `UpdateWithEscalation` | Mutates ActionInfo platform properties and requeues; tracks escalation count separately from other retries. This is not compatible with an immutable accepted request and original per-attempt charge without a new distinction. |
| `nativelink-scheduler/src/api_worker_scheduler.rs:1029`, `static_verdict` | Tests total worker declarations, including peer declarations. Busy/free admission state is a different question. Cached intrinsic verdicts are gated by live census freshness before failing work. |
| `nativelink-scheduler/src/unsatisfiable_tracker.rs` | Per-shape observation age plus each action's own original submission age; no inherited immediate failure for young actions. Tracks only observed queued intervals and bounded recheck backoff. |
| `nativelink-scheduler/src/simple_scheduler.rs:496`, `handle_unsatisfiable` | Rechecks fleet generation before making the result terminal; produces `FailedPrecondition`; bounds failures per matching pass. |
| `nativelink-config/src/schedulers.rs:227` | The old timeout is operator chosen relative to provisioning/restart time, not a proof that future workers cannot satisfy a request. |

The new authority's `MAX_ATTEMPTS = 3` is an explicit bounded experimental policy.
It need not reproduce the old three separate budgets. Reintroducing unbounded
"untried" dispatch attempts would exceed the current attempt/effect tombstone
and finalization bounds even if no execution retry counter moved.

## Counterexamples that the replacement must close

1. An x86 action fails at 4 GiB; the only 64 GiB worker is ARM, while the largest
   compatible x86 worker has 8 GiB. A global memory ceiling must not raise the
   action to a reservation only the incompatible worker can satisfy.
2. Worker usage echoes 1 GiB for an attempt charged 4 GiB. A second reservation
   computed from the echo can be smaller than the original requirement or
   target. The authoritative original charge is the growth base.
3. An older attempt reports a memory kill after its successor runs. The old
   report cannot change the successor's estimate, charge, retry count, result,
   or publication. A report for a valid operation string is insufficient.
4. A process is killed or loses transport, but quiescence is uncertain. Growing
   an estimate does not refund its old reservation. Cleanup must release the
   original attempt vector, never the new larger target.
5. The worker exits with 137 for an unrelated reason. Exit status, an error
   substring, or absent telemetry does not prove memory exhaustion.
6. A "whole memory and CPU" reservation still shares with actions declaring
   zero on both dimensions. It is not an exclusivity proof. A genuine exclusive
   grant needs an explicit slot/occupancy rule; this proposal makes no such
   claim and does not add the old whole-worker special case.
7. Autoscaling is temporarily at zero, a compatible worker is draining, or a
   stale capacity sample lowers available memory. None proves the action is
   intrinsically impossible. A finite wait policy can stop waiting without
   asserting that no future worker could execute it.
8. A single ready worker advertises enough memory and another enough CPU, but
   neither offers both plus the exact platform. Per-dimension maxima cannot
   establish that one real compatible allocation exists.

## Agreed fixed memory escalation

The narrow implementation retains the existing total three-attempt bound. A dispatch, decline/retry, worker loss, and memory retry all consume that
same finite attempt identity budget. This is a deliberate policy reduction
from the legacy separate budgets, reported by migration, and keeps existing
finalization headroom finite without silently multiplying the lifecycle.

Separate three facts:

- The accepted `Request` contains immutable explicit minima and exact platform
  requirements. Its payload, Action identity, and coalescing key never change.
- Each execution has a current typed `reservation_target`, initially equal to
  its accepted resource vector. Later accepted resource evidence may raise
  memory. CPU and slot requirements stay unchanged in this first policy.
- Each attempt's `ReservationView.charged` is the immutable vector chosen at
  placement. Dispatch carries this vector separately from the original
  request. A release always uses this original vector, including after a new
  target, a new incarnation, or a capacity change.

The reducer event is an explicit **current-attempt memory failure**
with worker/incarnation/attempt identity, bounded original failure response,
optional measured peak in bytes, and confirmed physical quiescence. The
classifier is an explicit resource outcome, never an inference from exit code
137. A conflicting echoed reservation is rejected, not used as another growth
base. Missing optional measurement is distinct from measured zero.

Authentication and typed classification are trusted adapter obligations, not
claims established by a plain reducer event. A normalized successful response
(`error = None`, `exit_code = 0`) contradicts memory failure and is rejected.

For the first implementation, uncertain termination does not qualify for this
event. Use the existing revocation/cleanup machinery and retain the old charge;
a later stale memory report cannot retroactively alter a successor. A future
pending-resource-failure state may preserve evidence until cleanup, but it
requires its own acceptance and retry-order contract.

On a valid memory-failure event:

1. Verify current attempt authority, exact charged-vector association, typed
   failure outcome, bounded response, and quiescence before changing anything.
   The accepted failure retires that attempt and releases its original charge
   once. Duplicate/stale reports cannot consume another attempt or grow again.
2. Find the largest **eligible declared memory ceiling**. Each candidate is one
   actual registered worker with all exact properties. Evaluate the state after
   releasing only this confirmed-quiescent attempt. Exclude draining and
   quarantined workers and any worker whose remaining original charges exceed
   any declared dimension. Checked-subtract immovable unreleased unauthorized
   charges from all declared dimensions. The existing CPU and slot target must
   fit the remaining CPU and slots on that same worker; the remaining memory is
   its candidate ceiling. Never assume uncertain processes disappear. Ignore
   transient available samples for growth only, and never combine dimensions
   across workers. Placement still enforces every current admission ceiling and
   all remaining charges before dispatch.
3. Let `R` be the just-failed authoritative memory charge. If `R > 0`, propose
   twice `R`, capped at that eligible ceiling. Compute the capped result without
   overflow, for example compare `R` to half the ceiling before multiplying.
   If `R == 0`, use a positive measured peak as the base and apply the same
   doubling/cap. With no positive base, do not invent a minimum from a class.
4. Retry only if the new target is strictly greater than `R`, dominates every
   explicit minimum, has an eligible declared ceiling, and another attempt
   remains. Record the chosen target in this committed transition; do not
   recompute it during dispatch retry or replay. Original queue age and live
   priority remain intact. Later compatible operation joins use the immutable
   request, not a historical estimate as a new coalescing identity.
5. Otherwise finish with the accepted failure and a bounded internal reason.
   Precedence is `AttemptBudgetExhausted`, then `MissingGrowthEvidence` (zero
   original charge and no positive peak), then `NoLargerEligibleWorker`. These
   are policy/execution failure reasons, not proof that the action can never run
   on any future fleet. Preserve the full original worker response unchanged.
   The reason is internal authority state; no current claim says clients receive
   it. A future canonical adapter must define and test its diagnostic projection
   without silently dropping the reason or overwriting original response bytes.

A memory failure produces no AC publication. This narrow change has no global
history or hint sink. Any future accepted-history publication must follow the
same current-attempt, failure, and quiescence validation. Current worker
telemetry's KiB quantities require checked conversion to bytes; raw reports
cannot mutate the reducer or write accepted history first.

Changing the model this way also changes invariants: an old reservation no
longer equals the execution's latest target. It must remain associated with its
own attempt and dominate the immutable minima. Every dispatch grant must equal
its associated original reservation. Holds and matching use the current target;
request payload validation and compatibility continue to use the original
request. Snapshot version and required reader capabilities must name the new
semantics explicitly. Pure state snapshots use version 3; version 2 is rejected
without a migration or activation claim.

The shared DTO contract is:

```text
Event::MemoryFailure {
  attempt, worker, incarnation, charged: Resources,
  peak_memory_bytes: Option<u64>, quiescent: bool, result: ResultPayload
}
ExecutionView.reservation_target: Resources
EffectKind::Dispatch.charged: Resources
TerminalResult::ResourceFailed { result: ResultPayload, reason: ResourceFailureReason }
ResourceFailureReason = AttemptBudgetExhausted | MissingGrowthEvidence | NoLargerEligibleWorker
```

The event variant asserts typed memory exhaustion. `quiescent` must be true;
`charged` must equal the associated original reservation. Current-attempt
authority is checked first, followed by all validation before any mutation.
Missing or zero measured peak cannot grow an original zero reservation. A
positive peak may seed capped doubling up to `u64::MAX`, but the committed new
memory must still strictly exceed the original charge. Dispatch and replay use
the recorded target without re-deriving it from later fleet observations.

## Terminal infeasibility is a different contract

The authority sees registered workers; that alone is not a closed description
of workers that may be provisioned later. Therefore the next memory-escalation
change should not add terminal "unsatisfiable" decisions based on absence,
heartbeat loss, draining, an available-memory sample, a hold, or uncertainty.
It can still return bounded deterministic diagnostic reasons for why no present
placement exists.

A later intrinsic infeasibility event needs a committed, bounded, versioned
**closed capability envelope**: the permitted future worker shapes and their
resource ceilings for that instance. This is an infrastructure fact with an
owner and revision, not a choice of matching policy. Every newly registered
worker must conform to the envelope until an authorized revision changes it.
Only then can the reducer prove that no permitted single shape satisfies the
immutable requirements (or a chosen reservation target). The proof must use
whole shapes, checked quantities, exact requirements, and the current revision;
stale proofs fail. The envelope cannot be inferred from a temporarily empty
fleet or from historical hints.

Without that ownership contract, a finite queue-wait deadline may produce a
wait-budget error, explicitly distinct from intrinsic incompatibility. Such a
deadline needs a declared service wait budget, a timer/leadership contract, and
REAPI status behavior. The source inventory cannot calibrate a provisioning
budget or justify a universal new number. It remains a qualification item,
not a reason to silently retain an unbounded runtime selector. Production
client retry/local-fallback behavior for the selected status also needs a
client integration fixture.

## Required next tests and remaining limits

Independent histories should cover incompatible-largest-worker selection,
resource echo mismatch, exact/minimum preservation, memory zero with and without
measured peak, capped doubling at integer boundaries, stale/duplicate failures,
quiescence refusal, original-vector cleanup after growth, preserved operation
handles/queue age/coalescing, successful completion after retry, exhaustion with
full response preservation, and snapshot-plus-suffix replay between growth and
placement. Preserve distinct diagnostics for waiting capacity, no registered
workers, and no currently compatible registered worker. Do not turn these
observations into global impossibility claims.

The recently added bounded counting writer prevents `State::payload_fits` from
allocating an oversized JSON copy merely to reject it. It does **not** reserve
aggregate snapshot bytes. Per-operation original requests, execution requests,
dispatch copies, result bytes, and nested journal JSON escaping all accumulate.
The journal's 64 MiB receive/metadata ceiling rejects excess safely, but is not
an admission proof that every admitted lifecycle can later snapshot/finalize.
A subsequent byte-accounting change must reserve worst-case finalization and
encoding headroom at admission, or choose a representation with a demonstrated
bound. Existing synthetic budget measurements predate these new fields and do
not qualify the changed state size or throughput.
