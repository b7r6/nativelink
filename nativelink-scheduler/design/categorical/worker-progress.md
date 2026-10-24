# Worker progress observations

Status: implemented candidate, 2026-10-22. The experimental recipient and real
action manager now observe byte advancement, completed work, and owned-child
liveness. These services remain test wiring. This document does not qualify M2,
a production clock mapping, or all worker optimization paths.

## Protocol and leases

`AuthorityAttemptReport.phase_progress` is field 10, separate from
`observation_sequence`. The mandatory `MeaningfulProgress` capability is enum
value 5 in experimental protocol version 1. Negotiation rejects a peer missing
it. This is not a rolling-upgrade compatibility claim.

A report sequence orders the retained complete observation; it is not evidence
of useful work. Within one phase, only a strictly greater work counter can renew
a progress-stall lease. Reconnect, capacity traffic, duplicate dispatch, and
inventory publication retain the same counter. Old-phase observations cannot
advance a newer phase, and cancellation disables further renewal. A lost launch
receipt stays uncertain even if the local process observer can see the child.

Running liveness does not change the worker's exact execution timeout or the
authority's original `running_deadline`. The model still uses a provisional
30-logical-tick progress lease. The worker observes a running child and coalesces
changed progress at five-second intervals. Those settings have not been
qualified as a production logical-time/monotonic-time mapping.

## Bounded observation state

[The shared utility](../../../nativelink-util/src/work_progress.rs) owns one
registry per attempt. Its fixed-size key contains the phase, work kind, and a
SHA256 hash of a stable object identity. CAS operations use the object key;
hashing and materialization use the output or destination path. Separate kinds
distinguish a CAS read, cache population, write, hash, materialization, and the
owned process. No authority identity or resource charge lives in this utility.

Each entry retains its greatest byte offset and a completion bit. Only a larger
offset or the first successful completion advances the phase counter. Completion
closes that identity permanently: even an optimized completion with no byte
callbacks cannot later gain credit from a replayed prefix. Retrying a transfer,
rebuilding an evicted input, and duplicate callbacks cannot recount old work.
The counter counts meaningful observations, not bytes or elapsed time.

The cap is **4096 identities across all phases**, with no eviction. Keys contain
no retained variable-length path or object string. Checked counter exhaustion or
registry saturation emits a diagnostic and permanently disables renewal for
that attempt, including already known identities and later phases. This is a
bounded observation-state policy, not a process-RSS bound.

This cap has a concrete qualification cost: roughly 2048 distinct cold plain
input files consume the limit through population and materialization alone,
before Action, Command, and Directory observations. Warm files still consume
materialization identities. Large input/output sets and subsequent Running
renewal after exhaustion are not qualified. Raising the cap requires workload
and memory measurements; eviction is not a valid remedy.

A `watch` channel retains only the latest `PhaseProgress` value. The recipient
arms its five-second reporting delay only after an actual change; the delay
cannot manufacture progress. Phase, candidate-result, and uncertainty updates
continue through the existing retained-inventory publisher. Transport backpressure
coalesces complete inventories instead of accumulating individual byte events.

## Scope and shared operations

`create_and_add_authority_action_observed` installs the observer before loading
the initial Action. Implementations without this additive seam return an
unsupported error. The real manager retains it across preparation, execution,
and upload. Explicit task-local scopes carry it through nested helper futures;
legacy manager calls have no observer and retain their existing hashing and
transfer paths. A scope alone cannot observe a different task's work.

[FastSlowStore](../../../nativelink-store/src/fast_slow_store.rs) therefore keeps
one independent greatest-offset watch value beside each shared loader. Every
interested caller subscribes its own registry, including a follower of a legacy
caller. Bytes advance only after successful forwarding to the bounded cache and
caller pipelines. The retained offset survives replacement of a canceled loader
leader. Canceling a subscriber drops its observation future; it does not cancel
a different caller's required fill. A replacement replaying a prefix earns no
new credit for a surviving observer.

Cache population and generic byte consumption have separate work identities.
A byte already in flight can therefore settle through more than one bounded
pipeline after the upstream stops. Stall assertions must allow this finite
credit to drain. No periodic observation extends it indefinitely.

## Implemented observation sites

| Work | Observation and completion boundary |
| --- | --- |
| Initial Action, Command, and Directory reads | `consume_observed` records actual received bytes. Generic reads close the identity only at offset zero with the full digest size, or an unbounded read of a non-digest key. A partial range does not close the whole object. |
| Plain/executable input CAS fill | Every `populate_fast_store` subscriber receives shared forwarding offsets. Successful cache population closes its own identity; a repeated cache hit cannot reopen it. |
| Batched hardlinks | Each successful destination link reports completion from the blocking batch. Failed links do not count. |
| Inline input nodes | Successful symlink/metadata materialization reports completion; observed private-file copies report actual written chunks. |
| Output hashing | SHA256 and BLAKE3 observed file hashing use chunks. The observed BLAKE3 path bypasses its callback-free `mmap` optimization. Legacy hashing retains that optimization. |
| Whole-file output transfer | `slow_update_store_with_file` reports actual forwarded offsets, including FastSlowStore's optimized filesystem path. The manager closes publication only after the composed upload returns successfully, or a successful existence check. |
| Tree, Directory, stdout, and stderr publication | Observed in-memory uploads forward bounded chunks; logical publication owners record successful completion. Spilled stdout/stderr use observed hashing and whole-file transfer. A nested tier's acknowledgment cannot close the outer publication. |
| Running child | An actual `Child::try_wait() == Ok(None)` probe of the owned child advances liveness. Exit, probe failure, cancellation, or a recorded kill reason disables further probes. |

Source files:

- [Action manager](../../../nativelink-worker/src/running_actions_manager.rs)
- [Recipient](../../../nativelink-worker/src/authority_worker.rs) and
  [session transport](../../../nativelink-worker/src/authority_worker_transport.rs)
- [Transfer helpers](../../../nativelink-util/src/store_trait.rs)
- [Hashing](../../../nativelink-util/src/digest_hasher.rs)
- [Worker protocol](../../../nativelink-proto/com/github/trace_machina/nativelink/remote_execution/worker_api.proto)

Forwarded bytes establish work advancement, not durable CAS publication. After
all bytes have crossed the bounded pipeline, an unacknowledged commit stops
renewing. The authority still requires separate result acceptance and physical
process-domain quiescence before releasing reservations.

The initial Action load retains its independent `max_download_timeout` budget
before action-directory allocation. Preparation has its own download budget;
neither consumes the exact finite Action execution timeout. Observing partial
Action bytes now closes the earlier observation gap before action creation.

## Evidence and remaining qualification

Focused fixtures exercise:

- Two observers sharing a chunk-gated fill, with both observed and legacy
  leaders; leader cancellation; replacement replaying an identical prefix;
  survivor advancement only after new bytes.
- A partial successful read followed by the full object; repeated complete
  reads; completion before byte callbacks; registry exhaustion without eviction.
- A CAS that consumes an upload but withholds acknowledgment. Progress settles
  and remains unchanged until actual success.
- SHA256 and BLAKE3 intermediate hashing, duplicate hashing, and the optimized
  filesystem whole-file transfer with a withheld slow-tier acknowledgment.
- A real quiet child, retained progress across reconnect and duplicate dispatch,
  and termination at its original twelve-second execution deadline.
- Initial Action bytes observed before action creation, followed by an open
  stalled read after bounded pipeline credit drains.
- The separate committed-authority bridge: actual child progress renews the
  phase lease, replayed observations do not, and the original running deadline
  survives restart and remains unchanged.
- One real 2 MiB input blob, forwarded in gated chunks through the normal
  filesystem population path, remains incomplete while its observed bytes
  renew a committed authority lease beyond the original expiry. After forwarding
  stops and finite pipeline credit drains, reconnect inventories, duplicate
  dispatch, newer report sequences, and real recipient capacity observations
  leave the deadline unchanged. Expiry revokes authorization while retaining the
  charge; cancellation observes pre-execution quiescence. This fixture commits
  actual recipient observations directly; it does not inject faults into a
  network transport.

Exact commands and raw outputs belong in the evidence directory; a fixture name
alone does not establish a passing run. The bounded registry tests belong to
`nativelink-util` unit tests, transfer fixtures to `fast_slow_store_test`, initial
creation to `running_actions_manager_test`, and recipient/authority integration
to `authority_worker_test` and `authority_scheduler_test`.

Directory-cache builds and materialization, read-only input mounts, executable
variant construction, and generic streaming callers that bypass an observed
consumer still require explicit intermediate-work coverage. Background tasks do
not automatically inherit the observer scope. The normal worker file population
and protobuf-consumer paths are covered, but that does not qualify arbitrary
store compositions or every cache/mount optimization. Persistent-worker execution
is also outside the current owned-child observation contract.

Remaining gates include long multi-object output and captured-output spill
campaigns through the committed-authority bridge, stalled transfer with continued
network transport traffic, follower-only cancellation, optimized cache/mount
fixtures, and supported-load tests around the identity cap. These gaps must be
closed or explicitly excluded before claiming general long-transfer liveness.
