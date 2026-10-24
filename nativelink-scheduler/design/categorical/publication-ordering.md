# Temporary publication sink ordering qualification

`nativelink-worker/tests/authority_publication_ordering_test.rs` exercises the
plan's weaker M4 publication guarantee through real HTTP/2 RPCs, a durable
singleton, and a controlled mutable sink. The worker client supplies synthetic
candidate results. The test does not execute worker processes or qualify a
deployed listener boundary.

The first history accepts result A and captures its committed publication effect
before allowing the write to finish. It restarts the coordinator, settles a retry
of A, replaces the worker incarnation, and accepts and writes result B for the
same action. Finally, the original A request finishes and overwrites B. Every
worker-originated sink write is checked against its captured committed effect,
action key, and exact result bytes. Both original terminal Operations remain
unchanged through WaitExecution before and after the late write.

The controlled sink runs each write in an owned task whose lifetime exceeds
the coordinator's request future. This models an external request with an
ambiguous outcome; cancellation of the local waiter cannot recall that request.
Dropping an unreleased test gate cancels the owned task. The restart closes and
reopens the actual singleton journal; it is not an operating-system crash or
a replicated failover experiment.

A second history revokes an offered attempt before receiving its candidate.
The late candidate creates no accepted result or publication effect. Its exact
quiescent report may still settle the old physical charge. An ordered worker
capacity message acts as a processing barrier before the negative assertions.

The third history mounts the existing ActionCache service against the same
temporary sink. An actual UpdateActionResult writes a direct client value while
an accepted worker write is held. GetActionResult observes the client value,
then the late worker value. The direct update leaves the entire authority view
unchanged and creates no synthetic operation, attempt, or worker effect. The
worker's accepted terminal remains stable. Client access here is authorized by
test construction; deployed trust exclusion is a separate qualification.

These three histories pass with the explicitly permitted mutable-sink ordering.
They establish neither latest accepted head selection nor output protection.
M6 must replace the sink and make the A/B history preserve the latest authority
head. Accepted-history publication and its provenance are also separate work.
