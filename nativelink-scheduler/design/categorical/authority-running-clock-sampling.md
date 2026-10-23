# First Running clock observation

The experimental coordinator samples its injected clock after receiving the
first eligible Running report and before committing that report's Running
transition. This removes an arbitrarily stale stored starting time when the
periodic timer is delayed. It does not select a clock unit or establish the
minimum elapsed duration proposed in [the clock design](authority-clock-proposal.md).

## Witness and correction

The regression uses a real singleton journal, a controlled clock and the private
`run_loop` timer seam. An initial observation barrier completes recovery at tick
0. The periodic timer's first tick is one hour away. After setting the injected
clock to 10, the fixture sends a manual Running report for a Preparing attempt
whose requested execution timeout is five ticks.

Before the correction, Running committed with stored `now = 0` and derived
deadline 5. An ordered clock sweep at 11 revoked the attempt, only one tick after
the report's clock observation. The reservation correctly remained charged.
The repaired path commits the observation 10 before Running, derives deadline
15, and keeps the attempt authorized at 11. The original compiled failing test,
source and log are retained in the evidence receipt.

The original witness used a one-minute delay and finished before that tick. The
final fixture uses one hour so a slow test host cannot make the periodic timer
part of this controlled schedule. The runtime driver retains its existing timer.

Manual reports establish coordinator ordering. This fixture does not execute a
child or measure launch time. It seeds a fixed experimental request through the
existing reducer; it does not replace immutable REAPI timeout projection or
establish new evidence about it.

## Ordering and refusal

The driver first checks the existing session, report framing, nonzero observation
sequence, worker and attempt identity, dispatch effect and live authorization.
Decline handling stays ahead of the new observation. Sampling applies only to a
Running report with no earlier Running deadline, an attempt in Dispatching,
Accepted or Preparing, and room for its next progress generation.

The existing `time()` operation samples the injected clock, clamps it against
committed time and commits any advance. It then completes its existing expiry
sweep before Running can proceed. Thus an already-due dispatch acknowledgment,
progress or operation-interest deadline wins over a newly arrived Running
report. The driver reloads the attempt after the sweep and stops if authorization
was revoked. It must not grant Running using its earlier copied attempt state.

For a Dispatching attempt that remains authorized, this observation also precedes
WorkerAccepted. The subsequent Running transition still fixes the first execution
deadline exactly once. Duplicate Running, meaningful later Running progress,
backward phase reports and reports for revoked attempts do not sample through
this new path. Invalid session, identity, framing and effect reports also do not
trigger it. These exclusions concern additional clock work; the existing outer
loop can still propose its usual matching check after input.

The original repair left later progress timing and the inclusive `deadline <= now`
expiry comparison unchanged. The subsequent
[execution boundary repair](authority-quantized-deadline.md) waits strictly past
the stored execution deadline while preserving independent progress expiry.
Expiry continues to revoke authorization and emit
cancellation. It supplies no quiescence evidence and cannot release the original
reservation. Only the existing matching cleanup observation can release charge.

## Proposal cost and remaining obligations

An eligible first Running report performs one clock read. If it exceeds committed
time, this adds one ordered `AdvanceTime` Raft proposal and its existing committed
state read barrier before the progress proposal. The sweep can also propose
already-due expiries or retirement for other retained work; that work follows the
same bounded state scan as the periodic timer. If time has not advanced and the
recovery sweep has completed, no extra clock proposal is issued. Repeated Running
reports do not pay this new cost. A focused journal-position check isolates the
same input path from the outer loop's Match and Observe proposals: first Running
at unchanged tick 0 appends one Progress proposal; first Running at tick 10
appends AdvanceTime and Progress. The broader sweep cost above follows the
source. This is not a throughput or latency benchmark.

The focused cases cover a stale initial sample, direct dispatch acknowledgment,
invalid and stale reports, repeated and advancing Running progress, backward
phase and clock observations, and dispatch/progress/interest expiry before the
first Running grant. They also preserve charge through expiry and release it only
after a matching synthetic cleanup report.

The executed invalid-report cases cover a stale session, zero observation
sequence, invalid or unspecified phase, wrong incarnation or attempt identity,
wrong dispatch effect and missing attempt identity. The encoded-size and epoch
guards are existing source checks; this fixture does not separately exercise
their malformed-frame and wrong-epoch inputs.

This sampling change did not define seconds, a canonical clock constructor,
checked clock exhaustion, journal clock-contract binding, downtime accounting,
or compatibility with old Running anchors. A separate
[checked clock source](authority-fallible-clock.md) and the execution boundary
repair now cover parts of those obligations. Persistent bootstrap binding,
finite deadline headroom and the complete policy-window contract remain open.
These fixtures do not establish the full execution-duration guarantee, physical
process termination or production readiness.
