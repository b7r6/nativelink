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

Important remaining limitations:

- Observe does not renew interest. Even an attached client expires after the
  current interest interval. Renewal tied to actual bounded delivery and
  backpressure, long-lived observation and serving-window qualification remain
  future work. Dropping an observation does not synchronously cancel interest.
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
