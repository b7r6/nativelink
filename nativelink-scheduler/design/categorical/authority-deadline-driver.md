# Authority deadline proposals

The experimental coordinator sweeps restored deadlines before taking its first
input. A journal can end after `AdvanceTime` commits and before the corresponding
expiry proposals commit. Recovery must finish that sweep even when the local
clock returns the same or an earlier tick. The existing maximum with committed
`now` and one recovery sweep supply that behavior; ordering the sweep before the
input loop prevents a queued observation or worker report from overtaking it.

An execution deadline applies while an attempt is `Running`. An attempt retains
that deadline when progress moves it to `Uploading`, but upload liveness follows
its progress deadline. Previously the coordinator selected `ExpireExecution`
whenever the retained running deadline was due. The reducer correctly ignored
that proposal outside `Running`, so the selection masked the due upload progress
deadline. The driver now selects execution expiry only in the running phase and
otherwise considers `ExpireAttempt` with the current progress generation.

The deadline comparison remains `at <= now`. This change does not redefine a
tick, alter the captured action timeout, grant an extra tick, or introduce a
runtime clock source. It changes no reducer event, snapshot format, or oracle
transition. Expiry revokes authorization and emits cancellation; only a matching
cleanup observation can settle the retained physical reservation.

## Focused qualification

The recovery fixture commits an operation and `AdvanceTime(300)` through a real
singleton, shuts it down, and reopens its journal. It queues `Observe` before
entering the coordinator loop and supplies a private timer whose first tick is
60 seconds away. The first acknowledged observation must already contain the
committed interest expiry. Its deadline and generation stay unchanged, and the
clock cannot move backward. The timer parameter only controls this private test
schedule; the running driver still constructs its existing 20 millisecond timer.

The worker fixture sends actual HTTP/2 registration, execution, progress, and
cleanup messages. A manual recipient reports `Running`, then `Uploading`, with
execution deadline 5 and progress deadline 30. At tick 5 the upload stays
authorized; at tick 30 the coordinator must revoke it and deliver the committed
cancel command. An ordered capacity observation waits for the entire due-event
sweep, without relying on a sleep. The worker stream stays open, and the charge
stays unchanged until the matching cleanup fence arrives. A wrong cancel effect
cannot refund it. Draining keeps replacement attempts from obscuring that charge.

The manual recipient does not execute a process. Its synthetic cleanup qualifies
protocol fencing and accounting, not physical process termination. These checks
also do not qualify a production clock, downtime accounting, timeout rounding,
canonical bootstrap, or deployment ownership fencing.
