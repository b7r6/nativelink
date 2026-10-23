# Authority bounded-read producer lifetime

The authority's shared bounded-read helper owns its channel writer inside the
producer future and drops it when the store call completes. A store that returns
success without sending EOF now produces the channel's missing-EOF error. The
collector no longer waits for a writer retained outside the joined futures.

Successful reads still require both explicit channel EOF and successful producer
completion. EOF followed by a producer error remains an error. EOF followed by a
pending producer remains pending until the caller cancels it. An excess chunk
still fails immediately and cancels the producer without draining more bytes.

This changes no storage range, byte limit, digest context or effect protocol.
The helper still requests the first 65,537 bytes and accepts at most 64 KiB of
actual received data. It makes no metadata request. Its key can describe an
Action while the value contains an ActionResult; it does not compare the key's
declared size or hash to that opaque value.

## Counterexample and qualification

Five direct helper fixtures separate missing EOF, producer errors before and
after EOF, empty and exact-bound success, excess-byte cancellation, and EOF with
a still-pending producer. The unchanged implementation compiled with these
fixtures: four controls passed and the missing-EOF test timed out on its first,
empty-input case. That failure did not reach the second, partial-input case.
The fixed implementation must complete both missing-EOF cases with an error.

These fixtures call the actual helper through a controlled Store implementation;
they do not traverse HTTP/2. Excess-byte fixtures deliberately exceed the
requested range so the collector must inspect actual chunks. Cancellation means
the pending producer future is dropped. It does not prove that an arbitrary
store cancels detached tasks or blocking I/O.

The helper adds no deadline and cannot force a pending producer to finish. A
bounded range plus EOF depends on the store's complete-range delivery contract;
this change does not prove whole-object completeness for a producer that lies
about delivery, or impose physical I/O and memory bounds on arbitrary stores.
