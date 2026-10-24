# Captured Action timeout boundaries

The timeout fixtures retain the existing request projection version. Explicit
zero projects to one tick, an omitted timeout projects to `u64::MAX`, and a
positive finite duration rounds upward to whole seconds. The largest accepted
finite projection is `315_576_000_001`: the maximum protobuf seconds value,
`315_576_000_000`, can carry a nonzero nanosecond component.

The request fixture covers zero, omission, one nanosecond, a fractional second
above one second, and both maximum integer and fractional durations, with SHA256
and BLAKE3 captures. It admits the real captured request, restores its snapshot,
and rejects a changed timeout projection both at submission and when every
duplicated projection in the snapshot is changed consistently. Captured Action
bytes remain the source of truth.

The service fixtures use real journal entries. A committed suffix awaiting
application contains the captured submission and matching event; reopening must replay it
and recover the exact dispatch request and resource reservation before any
worker acceptance. The original Action bytes include an unknown protobuf field.
Separate driver checks retain authorization at the rounded boundary for short
finite requests, revoke strictly afterward, and preserve the reservation until
cleanup. Zero, omitted and maximum timeouts also retain their original request
capture and derived deadline through reopen. These tests use manual reports and
controlled ticks; they do not run an action process or prove elapsed wall time.

The maximum duration also exposes an unresolved construction boundary. At
`now = u64::MAX - 315_576_000_001`, the current reducer accepts a finite maximum
request and stores `running_deadline = u64::MAX`, the same deadline value produced
by omission. This high tick precedes submission and matching, so the diagnostic
does not skip or renew an expired progress window. The captured requests still
distinguish those cases. No subsequent
integer tick can satisfy the driver's strict execution-expiry comparison.
The diagnostic preserves that behavior through reopen so that it cannot be
mistaken for a qualified headroom policy. It does not assert indefinite execution:
independent progress or interest expiry can still revoke authorization.

Canonical construction must decide and bind the checked headroom policy. A
startup-only check is insufficient because the first Running report and later
renewals can occur after time advances. A finite maximum request needs room for
its deadline and the following expiry tick. Changing the version-one projection
in place would instead invalidate existing captures on restore. This fixture
change chooses neither a new counter policy nor a migration rule.

Worker conversion has separate existing coverage in
`authority_timeout_preserves_zero_and_fractional_duration_and_rejects_over_max`.
That test observes the exact local sleep duration: explicit zero stays zero,
fractional nanoseconds remain exact, omission uses the configured worker maximum,
and an explicit timeout above that maximum refuses before launch. These local
semantics are separate from the coordinator's omitted-timeout sentinel.
