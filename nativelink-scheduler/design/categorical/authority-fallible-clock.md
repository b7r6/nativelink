# Checked authority clock observations

The experimental coordinator now accepts fallible clock observations. Failure
stops its loop through the existing shutdown path. It does not substitute zero,
the last tick or a saturated maximum, and cannot manufacture an expiry or release
a reservation. A failed observation before the first Running report prevents
both the acknowledgment and the Running transition.

Service construction awaits its initial clock observation and recovery sweep
before returning a service or either fixed listener role. Once the startup
future is polled, a separate owned preparation task retains the authority while
reads, proposals and shutdown are pending. If the caller cancels, preparation
finishes and shuts down any coordinator whose result can no longer be delivered.
Dropping a delivered but unobserved result drops the coordinator's stop sender;
the running task remains responsible for shutdown. This is not a guarantee for
an async constructor that is discarded without ever being polled.

`MonotonicClock::from_recovered_tick` captures a fresh monotonic origin. Its
observation is the supplied committed base plus whole elapsed seconds, using
checked arithmetic. A sample before that origin or an overflowing sum is an
error. An exactly representable maximum is a valid observation; admission must
still establish deadline headroom separately. Copying the clock retains its
origin rather than starting a new lifetime.

On reconstruction only the committed base survives. Downtime, uncommitted time
and a previous fractional second do not advance the new clock. Repeated restarts
can therefore postpone expiry indefinitely. No wall-clock reading supplies
elapsed authority time.

This is a prerequisite for the [canonical clock contract](authority-clock-proposal.md),
not its activation. A caller must bind the recovered journal to seconds and the
sampling and proposal rules before using this source. The constructor does not
validate that provenance. Deadline comparisons, request projections, reducer
events and persisted formats remain unchanged. The existing inclusive expiry
comparison does not establish a minimum elapsed duration after rounding a start
down to a whole second. Policy-window semantics, checked deadline headroom and
persistent bootstrap binding remain open.

Qualification uses deterministic fractional, restart and exhaustion boundaries
and real redb/Raft histories. The service cases check startup refusal with due
work retained, failure after a successful initial observation, first Running
refusal without an acknowledgment, and cancellation while initial observation
is held. They compare the complete scheduler snapshot and reopen the journal
after shutdown. The cancellation witness establishes redb custody; descriptor
role-lock lifetime is qualified separately by the filesystem and journal bridge.
