# Experimental automatic RPC coordinator

`nativelink-service/src/authority_server.rs` is constructed only by local tests.
Production factories, configuration and binary service mounting are unchanged.
This is an integration experiment, not M4a release or cutover qualification.

The fixture mounts actual tonic Execution and AuthorityWorkerApi services. It
captures the original ExecuteRequest, request metadata, queued timestamp and
bounded raw Action/Command bytes before deriving scheduler fields. CAS bytes
retain unknown protobuf fields; tonic-decoded RPC messages retain only fields
known to the pinned schema. A serialized ingress loop allocates operation IDs
from the persisted watermark after quorum read barriers. Only committed replies
produce handles, observations or worker commands.

The loop wakes on input, effect outcomes and an injected monotonic clock. It
proposes matching and due attempt, execution and interest deadlines, then drains
the final committed outbox. Worker reports require the full process, execution,
attempt and original dispatch identity, a recognized phase and nonzero sequence.
The original dispatch identity survives acknowledgment and coordinator restart.
A revoked attempt may instead establish cleanup with an exact committed cancel
identity: this permits a tombstone created before the first dispatch arrives.
That alternative never authorizes progress or completion. Transport loss alone
does not revoke work or refund resources.

`authority_worker_transport::run_reconnecting` retains the same process recipient
and opens a new transport connection for each bounded retry. Sessions increase
without resetting receipts, candidate results, spent state or resource charges.

When feature 2 has been explicitly activated through the real journal, the
driver also issues and delivers committed receipt-retirement certificates and
commits exact current-session acknowledgments. Base constructors retain their
existing feature set. The [receipt-retirement qualification](receipt-retirement.md)
describes bounded receiving, the durable outbox and its restart cutpoints.
Protocol rejection cannot downgrade to the old worker protocol. Replacement
sessions close the previous server ingress task. Delivery bookkeeping retains
one session per unresolved effect, rather than an entry for every reconnect.

The experiment permits at most 64 concurrent capture handlers and live RPC
streams combined. It acquires a permit before capture or registration and holds
it through the stream. The ingress queue holds at most 64 messages; at most four
storage effects execute concurrently. Each Action/Command accumulator refuses
bytes beyond 64 KiB before extending its buffer. Invalid or missing identities
never reach completion application. Driver failures and ordinary shutdown both
close Raft explicitly before the journal can be reopened.

Complete worker inventories and capacity observations also coalesce after gRPC
decoding, before the coordinator queue. Repeated inventory and capacity messages
share one pending notification instead of forming a FIFO of stale copies.
The snapshot preserves every unresolved candidate, cleanup receipt and explicit
certificate acknowledgment. Taking it and clearing its notification are atomic;
observations arriving during a commit queue another turn. Standalone reports
seal the preceding snapshot, so later capacity or inventory cannot overtake
that report. Equal accepted/progress facts do not generate new journal events;
unchanged capacity still records a heartbeat when authority time advances.

Worker response bytes and the committed JSON representation have different size
costs. This fixture requires a core payload limit of at least 64 KiB, bounds its
worker responses to 64 KiB, and checks the complete serialized ResultPayload
against 64 KiB before committing completion. A valid quiescent candidate whose
serialized representation exceeds that limit becomes a fixed, bounded
ResourceExhausted ExecuteResponse. It is not cache eligible. Supported responses
retain all captured fields. Internal-error receipts use the same bounded status
projection. A smaller core limit refuses service construction and closes the
journal. Larger core limits do not enlarge this experimental RPC contract.

The local tests cover automatic placement, original raw CAS-byte preservation,
restart after WorkerAccepted removed the dispatch intent, completion while the
coordinator is absent, retained-candidate reconciliation, stable WaitExecution,
acceptance/publication restart, a subsequent cache hit, automatic execution
expiry and physical cleanup, interest expiry, retirement and operation IDs that are never reused. Additional wire histories exercise cancel-before-dispatch tombstones,
wrong dispatch/cancel identities, invalid phases, zero sequences, the live RPC
bound, and bounded terminal projection for byte-array and escaped-status growth.
Real process tests require namespaces; their explicit skip means that process
closure remains unqualified on a host lacking that capability.

`python3 tools/test-coordinator-mutations.py --output <evidence-directory>`
checks the named wire assertion against four isolated source faults: wrong
original dispatch identity, zero observation sequence, wrong cancellation
identity, and bypass of the complete committed-payload bound. The selected
wire test must pass unchanged and fail at runtime for each mutant; compiler
failures and unmatched filters do not count. These checks do not establish
production authentication, process closure, or capacity.

## Observation interest and bounded transport opportunities

Execute and WaitExecution observe one retained operation interest. Observe and
the first body item do not renew it. Later response-body polls may submit a
renewal every 100 experimental ticks against the 300-tick interest window. A
quiet operation emits the same Operation as a heartbeat. State changes otherwise
emit only when the projected Operation changes. The driver checks the current
clock and applies due expiry before renewal, then checks the persisted deadline
again. Concurrent observers share that deadline and cannot multiply renewals
within the cadence or add interests. Restart preserves the deadline and its
generation. Dropping a stream leaves the retained window; it does not cancel the
operation synchronously. One renewal already handed to the coordinator can
finish as the stream closes, but no detached task continues renewing it.
Startup rechecks due deadlines before the first input, even at an unchanged
persisted clock. This recovers a journal that committed AdvanceTime just before
expiry proposals were interrupted; it does not repeatedly propose rejected
retirement at a fixed time. The [deadline qualification](authority-deadline-driver.md)
also covers upload progress expiry after the running deadline has passed.

Each stream retains one previous Operation and one pending body item. The whole
encoded Operation, including its name, metadata and terminal response, is limited
to 128 KiB. The shared 64-stream/handler limit and 64-command queue also cover
renewal. Committed watch updates coalesce without a stream-local notification
queue. A closed coordinator/watch returns Unavailable before another renewal.

The observation is a transport delivery opportunity, not proof that the remote
application consumed a message. In the pinned tonic 0.14.6 encoder, a pending
source flushes the current batch; continuously ready messages accumulate until
the 32 KiB yield threshold, plus at most one bounded message and its five-byte
gRPC header. Hyper 1.7.0 checks h2 send capacity before polling that body, so it
can queue one such bounded chunk beyond the remaining peer-advertised credit.
These dependency facts are checked in `tonic/src/codec/{mod,encode}.rs` and
`hyper/src/proto/h2/mod.rs`; they require review when those dependencies change.
The server's receive-window settings cannot bound outbound credit granted by a
peer. A peer that continues reading or advertising credit can keep an interest
alive even if its application ignores the data. No stronger claim is made.

The paused-reader fixture fixes its stream receive credit at 512 bytes and
connection credit at 4096 bytes, disables adaptive windows, consumes one message,
then stops polling. Quiet heartbeat batches contain one message each. It checks
that the finite buffered messages exhaust credit and interest expires; its
receipt records message bytes, last deadline and renewal generation. This is a
qualification of that explicit client profile, not a universal bound from an
application's last read for arbitrary HTTP/2 clients. A separate actual RPC
fixture consumes quiet heartbeats for four interest windows and checks duplicate
streams, reconnect and restart. Precisely controlled body polling additionally
checks first-item behavior, shutdown, stale expiry generations and a clock jump
past the retained deadline. A durable clock-before-expiry cutpoint verifies
automatic expiry on restart when the observed clock equals the restored clock.

Important remaining limitations:

- The qualified observation envelope above is local and experimental. General
  client buffering profiles and publication serving windows remain unqualified.
- Clock ticks, bounded matching scans and storage retries are experimental. This
  loop has no production throughput, latency or clock-recovery qualification.
- The local principal and worker identities are fixture facts. This service does
  not authenticate a fleet or establish old-incarnation fencing.
- The temporary sink is an isolated mutable store. Publication is accepted-only;
  there is no physical exactly-once write or latest-accepted-head guarantee.
- Operations cancellation/listing, typed memory-failure policy projection,
  accepted-history relocation, migration conversion and canonical construction
  remain incomplete. The old production path remains present.
- The worker listener's absent AC route is a local control. It does not prove
  deployed credential exclusion across every writable route or combine the
  separate listener-boundary qualification with this coordinator.
