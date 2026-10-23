# Proposed singleton authority clock

This is a design proposal for the complete canonical clock contract. The current
qualification interface accepts arbitrary experimental ticks. The
[deadline-driver repair](authority-deadline-driver.md) orders the recovery sweep
and fixes upload expiry; the subsequent
[execution boundary repair](authority-quantized-deadline.md) now proposes execution
expiry only past its stored deadline. No timeout projection, reducer event or
snapshot changes accompany these repairs. The
separate [first Running sampling repair](authority-running-clock-sampling.md)
orders observations from the existing injected clock; a fixed canonical clock
source is available as a [checked prerequisite](authority-fallible-clock.md),
without a persistent bootstrap binding.

## One clock and an explicit recovery policy

The canonical singleton would use one fixed unit: monotonic elapsed seconds.
After acquiring journal ownership and completing the recovery read barrier,
capture committed `State.now` as `base` and a fresh monotonic `origin`. During
this process lifetime, observe:

```text
observed = base + floor(monotonic_elapsed_since_origin / one_second)
```

Only committed `AdvanceTime` changes reducer time. No OS wall clock supplies
these observations. Backward or forward wall-clock adjustments therefore cannot
shorten a deadline. REAPI queued timestamps remain captured wall-clock metadata;
their validation and possible capture failure are separate from expiry.

A restart begins at the last committed tick with a fresh origin. Downtime,
uncommitted clock observations, and the preceding fractional second do not count
as elapsed authority time. A retained deadline is neither shifted nor renewed.
If it is already due under the selected comparison, the initial recovery sweep
proposes expiry before input. Otherwise its remaining ticks run from the new
origin. Repeated restarts can delay expiry indefinitely; this policy supplies no
wall-time upper bound across outages. It cannot establish that remote work
stopped, refund a reservation, or authorize storage reclamation.

Creating this clock requires a validated bootstrap binding between the journal
identity and its fixed time-unit contract. The current journal does not identify
the unit used by an arbitrary injected test clock. A canonical constructor must
not silently adopt an experimental journal and reinterpret its ticks as seconds.
The binding may follow a versioned canonical bootstrap contract; this proposal
does not add a clock-selection setting or change the current manifest schema.
Knowing that an old journal used seconds also does not prove its retained
Running starts had fresh samples. The bootstrap contract must bind the sampling
and proposal rules as well as the unit; adopting an older history needs separate
compatibility evidence or an explicitly fresh, isolated cutover.
Journal domain, principal, instance, CAS namespace, sink namespace, and exclusive
ownership need their own persistent binding and qualification. A fresh origin
proves none of those facts.

Counter arithmetic must be checked. Exhaustion must stop admission and expiry
processing through an explicit failure path, rather than wrap or manufacture an
observation of `u64::MAX`. Finite deadline additions also need enough headroom
for the maximum admitted duration, policy intervals, and any rounding allowance.
An omitted timeout intentionally uses the existing infinite sentinel; that case
must remain distinct from overflow. The experimental interface now propagates
fallible clock observations through coordinator shutdown and refuses startup on
an initial failure. Deadline headroom and a canonical bootstrap binding remain
design obligations.

## The duration invariant and its anchor

The proposed execution-timeout invariant is:

> An expiry attributed to the requested execution duration must not revoke an
> attempt before that duration has elapsed since its successful process launch.

This does not promise uninterrupted execution for the whole requested duration.
Cancellation, interest loss, worker replacement, and independently due progress
policy can revoke authorization earlier. Those reasons must remain explicit in
the contract and tests.

The worker's [launch receipt](../../../nativelink-worker/src/running_actions_manager.rs)
is sent after `command_builder.spawn()` succeeds and the child is owned. The
[authority recipient](../../../nativelink-worker/src/authority_worker.rs) permits
`Running` only after receiving that receipt. Thus a fresh authority clock sample
taken after receiving the first valid Running report is causally after launch.
It does not prove the instant of the program's first instruction or measure CPU
time. A promise measured from the later journal commit would be a different,
stronger contract and is not established by the proof below.

The [first Running sampling repair](authority-running-clock-sampling.md) now
takes a fresh injected-clock sample before the first eligible Running grant and
finishes already-due expiry proposals first. Previously this input could use
`State.now` from arbitrarily earlier, after delayed timer selection or journal
I/O. A one-tick allowance cannot repair that stale starting sample. The preserved
regression reports Running at injected tick 10 while committed time is 0, then
observes expiry at 11 against the old derived deadline 5. The repair removes this
stale-anchor case without selecting units or changing quantization policy.

A canonical duration guarantee requires a fresh monotonic observation committed
before the first Running grant is timestamped. The experimental repair supplies
the ordering for its injected clock. A checked monotonic source is now available;
its recovery binding remains pending. Sampling before waiting for an input is not sufficient.
The first Running deadline remains fixed through later progress and
reconciliation; reconnect cannot refresh the original execution duration.

## Quantization and the two implementation choices

For a positive requested duration `T`, the current immutable projection stores
`D = ceil(T)`. Explicit zero has a minimum derived value of one; an omitted
timeout has its own infinite sentinel. Let `S` be the fresh monotonic sample
after launch, expressed in seconds within the current clock lifetime. A Running
deadline based on that sample is `floor(S) + D`, relative to the same base.

An inclusive expiry can occur before `S + T`: with `S = 0.999` and `T = 1`, the
deadline is 1 and only 0.001 seconds need elapse. Rounding the duration upward
does not recover the fraction lost when rounding its starting time downward.

The earliest integer tick strictly beyond the stored deadline is safe:

```text
floor(S) + ceil(T) + 1 >= S + T
```

This proof requires the fresh sample. It concerns the monotonic elapsed source,
not a synchronized clock on the worker. Delayed sampling, transport, or expiry
commits only postpone execution-duration expiry once that causal ordering holds.
Discarding elapsed time on restart also postpones it. Fresh origins must never
add inferred downtime or round the restored base forward.

| Choice | Consequence | Prerequisites and costs |
| --- | --- | --- |
| Driver proposes execution expiry only when `now > running_deadline` | Supplies the extra boundary tick without changing the stored deadline or request projection. | Fresh starting sample; review every expiry proposal path, including recovery and progress expiry. The reducer still accepts its existing inclusive event contract. |
| Derive an extra tick in the captured timeout and retain inclusive expiry | Supplies the same conservative boundary for finite positive durations. | Fresh starting sample still required. Changes immutable request projections and needs explicit versioning or migration, including zero and omitted-timeout rules. |

The driver-only choice is now implemented in the experimental coordinator and
qualified by the separate boundary repair. The
[request validation](../../src/authority/request.rs) recomputes the projection
from retained Action bytes and compares `execution_timeout`. Changing that
projection in place would reject previously valid explicit-timeout requests on
restore. The worker must continue using the exact protobuf Action duration; a
coordinator rounding allowance must not extend its local execution timeout.

## Progress expiry cannot bypass the argument

The [reducer](../../src/authority/mod.rs) handles `ExpireAttempt` in Running
using `min(running_deadline, progress_deadline)`. Changing only the driver's
`ExpireExecution` comparison is therefore insufficient as a global proof about
all revocation proposals. The driver must never submit `ExpireAttempt` solely
because the execution deadline reached equality. If it submits one for an
independently due progress deadline, that earlier liveness-policy revocation
must be identified as such, rather than attributed to execution duration.

For example, an unrenewed progress window and a 30-second execution timeout can
both have deadline 30. A strict execution comparison can be followed by an
inclusive progress branch that revokes at tick 30. This is permitted only if the
contract explicitly allows that independently due coarse progress window. It
does not prove a 30-second minimum progress window.

Submit, progress, renewal, acknowledgment, and retention timestamps have the same
sampling and quantization questions when described as durations. Two options
keep those promises consistent: document coarse tick boundaries and avoid claims of
minimum elapsed windows, or use fresh samples and a conservative comparison
consistently for every window that promises a minimum duration. Neither option
requires changing the reducer's existing event acceptance rules. A canonical
contract must select and qualify the rule for each deadline class before
construction is exposed; a mixture justified only for execution expiry is not
enough.

## Qualification required before implementation is exposed

- Capture the exact source and proposal rule for each deadline class, including
  Running progress expiry, Uploading progress expiry, and initial recovery.
- Exercise samples just before and at a second boundary for integer and
  fractional durations, explicit zero, omitted timeout, and the maximum
  supported finite Action duration.
- Delay timer selection and journal I/O by several ticks before the first
  Running report. The fresh observation must prevent stale-time early expiry.
- Cross the execution/progress equality case with fresh progress, stale
  progress, and independently due liveness policy; record which event revokes.
- Reopen after a committed clock advance but before expiry, both at equality
  and strictly beyond the deadline. A new origin must preserve the chosen rule.
- Repeatedly restart between ticks and adjust OS wall time in both directions.
  The clock may delay authority expiry but must never invent elapsed time.
- Verify that reconnect and later progress preserve the first execution
  deadline, and that worker execution still uses the original Action duration.
- Exercise checked exhaustion, incompatible bootstrap time-unit bindings, and
  due expiry with unresolved cancellation. Time alone must never release charge.

These are requirements for the full clock contract. The deadline, first Running
sampling, checked source and execution boundary receipts cover bounded subsets;
they do not establish a persistently bound canonical clock or the complete
duration guarantees.
