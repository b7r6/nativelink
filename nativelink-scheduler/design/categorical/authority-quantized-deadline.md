# Execution expiry past the rounded boundary

The experimental coordinator proposes `ExpireExecution` only when a Running
attempt's stored execution deadline is strictly less than the observed tick.
At equality it retains execution authorization unless another independent reason
revokes it. The recovery sweep uses the same comparison before service startup
returns. Request projection, retained Action bytes, reducer acceptance rules and
persisted formats are unchanged.

The first valid Running report already takes a fresh clock observation after the
worker's launch receipt. For a monotonic source whose sample is `S` seconds and
a finite positive requested duration `T`, the stored deadline is
`floor(S) + ceil(T)`, provided that addition has enough finite headroom. The
existing reducer saturates additions; this argument does not qualify overflow.
Inclusive expiry can be early: sampling at 10.999 seconds
with a five-second duration produces deadline 15, only 4.001 seconds later.
Waiting until the first integer tick past that deadline supplies the lost
fraction. The worker still uses the original protobuf duration. This driver
allowance does not extend its local execution timeout.

Independent progress expiry remains a coarse tick boundary. The driver proposes
`ExpireAttempt` only when the attempt's own current progress deadline is due,
never merely because execution reached equality. When both deadlines are 30,
unrenewed progress may revoke at 30. Renewing progress at 29 moves that boundary
to 59 while retaining execution deadline 30; execution then revokes at 31. This
does not promise a minimum elapsed progress window. Interest, acknowledgment,
retention and other policy boundaries are unchanged.

Three added real-journal tests inspect actual committed events, retained
deadlines and unreleased reservations. They cover projected execution durations
one, two and five; recovery after committed time reaches equality or passes it;
and the coincident execution/progress boundary with and without renewal. All
three compiled and failed against the prior inclusive comparison. The existing
invalid, stale and replayed Running report case now also checks equality before
expiry, while retaining its original no-reanchoring and cleanup assertions.
These fixtures inject ticks and projected durations; they do not measure elapsed
process execution or qualify every protobuf duration projection.

The [captured timeout fixtures](authority-captured-timeouts.md) additionally
exercise real Action captures, committed submission and grant replay, restoration
of zero and omitted timeouts, and the maximum finite projection. Their saturation
diagnostic records the remaining headroom gap without changing counter policy.

The [canonical clock proposal](authority-clock-proposal.md) still requires a
persistent binding for units, sampling and proposal rules, plus checked headroom
for finite deadlines. The checked monotonic source deliberately discards downtime
and uncommitted fractions on restart, so repeated restarts can defer expiry
indefinitely. An infinite sentinel is not finite deadline headroom. No part of
this change establishes physical quiescence, refunds a reservation on time alone,
activates a production constructor, or qualifies all policy windows.
