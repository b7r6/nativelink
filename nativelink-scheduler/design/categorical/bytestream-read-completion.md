# ByteStream read producer completion

An uncompressed ByteStream read now requires both channel EOF and successful
completion of the store producer before ending its response successfully. The
previous reader returned immediately at EOF. A producer could still be pending,
or return a storage error after sending EOF, while the client received successful
gRPC trailers. Waiting for the producer preserves that final error. The existing
NotFound message truncation also applies to errors observed at EOF.

This changes the common local transfer facade used by the public service and
fixed experimental listeners. It does not change the legacy direct GrpcStore
forwarding shortcut or the compressed reader. The fixed digest-default change
separately routes remote fixed-role reads through the local facade; its source
and evidence remain distinct.

Data may already have reached the client before a later producer failure. The
final gRPC status carries the failure; this change cannot retract delivered
bytes. Channel EOF is still a producer assertion about completeness. The reader
does not independently verify whole-value length, digest integrity, backend
durability or physical IO. Waiting introduces no timeout and cannot preempt a
blocking backend. Dropping the response cancels its owned producer future under
the existing store cancellation contract.

Five tests were authored before the runtime repair. The original reader passed
two control cases and failed three regression cases: a producer error after
EOF, a producer still pending after EOF, and a real HTTP/2 response that ended
successfully after delivering bytes while its producer was waiting at a gate.
The combined error fixture failed on its first empty, immediate-error iteration;
its later data and yield iterations were not reached in that original run.

The HTTP/2 fixture releases its producer gate only after the client receives the
data frame. It then requires the producer's Unavailable error in the stream's
final status. The cancellation fixture cancels one pending stream poll, confirms
the producer is still held by the stream, and then drops the stream to release
it. Positive cases cover empty, ordinary and larger framed payloads. The mock
store deliberately does not validate the digest's declared size; these cases
qualify response framing and completion, not CAS integrity.

Qualification receipts distinguish the initial successful five-case repair,
four test-only redundant-qualification corrections, independently compiled fault
controls, restored regression runs and source review. Production role selection,
aggregate transfer admission and active upload identity remain separate work.
The [receipt](evidence/bytestream-read-completion/manifest.json) retains the
original failures, all three compiled fault detections, five distinct executables,
and both broader regression runs. The initial run had four fixture leader-startup
timeouts; the unchanged source passed all six targets when serialized. No timeout
was extended, and the passing rerun does not establish load independence.
