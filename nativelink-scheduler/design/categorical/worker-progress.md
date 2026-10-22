# Worker progress observations

Status: implementation proposal, 2026-10-22. Progress observation plumbing and
the protocol field described below are **pending**. This document records a
source inspection; it does not qualify M2 or change production services. The
existing candidate recipient reports phase changes, not sustained transfer
progress. The separately bounded initial Action fetch described below is already
implemented and tested.

## Required distinction

The [authority contract](contract.md) separates renewable progress-stall leases
from the nonrenewable execution deadline. A healthy long input preparation or
output upload currently outlives a phase-only report: the model's progress lease
is 30 logical ticks. Those ticks are provisional and are not a production
seconds mapping. Transport traffic, capacity reports, and reconnect inventories
cannot establish action progress.

Add a `phase_progress` unsigned integer to `AuthorityAttemptReport`, separate
from `observation_sequence`. The latter orders complete reports, including
cancellation and terminal evidence. It must never be substituted for progress.
Protocol tag 10 is currently available; the implementation must check it again
and regenerate the protobuf code. Require the new progress capability during
experimental negotiation before qualifying this behavior.

Within one attempt and phase, only a strictly greater progress value renews its
lease. A phase transition is a separate event; it may start the next phase's
counter at zero. An inventory retry or reconnect sends the retained value again.
Late Preparing observations cannot change an attempt already Uploading, and no
progress observation restores revoked authority or releases a reservation.
Running progress does not alter either the worker's execution timer or the
authority's already installed `running_deadline`.

## Observer and bounded reporting

The proposed worker API installs one observer before preparation through an
authority-only creation argument or a `RunningAction::observe_progress` method.
An implementation lacking the hook must return an explicit unsupported error.
The observer supplies a bounded `watch` channel carrying the latest
`PhaseProgress { phase, progress }`; it does not enqueue one message per byte.
The recipient consumes observations only for the matching current phase, updates
the retained report, and notifies its existing bounded inventory publisher.

Use checked counter arithmetic. Exhaustion closes progress reporting with a
diagnostic; it must not wrap or reset. Transport rate limiting may publish the
latest changed value at a fixed interval shorter than the qualified lease. A
timer alone never manufactures an advancement. The interval and real-time lease
mapping need qualification together; five seconds is a proposal, not a selected
production setting. Coalescing cannot discard the final candidate or physical
termination uncertainty.

Maintain a bounded per-attempt registry keyed by phase, work kind, and stable
object identity. Each entry retains the greatest observed byte offset and a
completion bit. Input identity can use the CAS digest, with a separate stable
path identity for materialization. Output hashing needs a stable output path
before its digest exists; completed CAS writes can use the digest. Every work
kind has an explicit meaning, so hashing and publication are not confused.

Only an increase in greatest offset or the first completion earns credit.
Repeated callbacks, replayed prefixes, retrying a successful Command fetch, and
staging already counted inputs again earn no additional credit. Registry entries
remain until attempt termination; evicting one and recounting it on retry would
allow a permanently failing transfer to renew forever. Choose and test a finite
entry/byte bound before implementation. Exhausting that bound must fail the
attempt or conservatively stop renewal with a diagnostic, never evict and mint
new credit. This registry is observation state, not a second reservation ledger.

## Concrete observation sites

| Work | Existing implementation | Proposed evidence |
| --- | --- | --- |
| Action load before action creation | `RunningActionsManagerImpl::create_action_info` | Actual CAS bytes or successful Action decoding, if an observer is added before creation. The existing local deadline alone provides no renewable progress evidence. |
| Command and Directory load | `fetch_command`, `collect_download_links` in the action manager | Successfully decoded object; partial transfer bytes require the store hook below. Repeated decoding on retry is deduplicated. |
| Plain/executable input files | `resolve_plain_files`, `resolve_exec_files`, `materialize_inline_nodes`, `materialize_links` | Successful transfer offset advancement and first successful materialization. Attempting an operation or entering a retry backoff is not progress. |
| Slow-to-fast CAS fill | `FastSlowStore::populate_and_maybe_stream` | Greatest received offset after forwarding each nonempty chunk successfully. Completion only after both tier operations settle successfully. |
| Output hashing | `DigestHasher::compute_from_reader` / `digest_for_file` | Newly hashed bytes for the stable output identity. The BLAKE3 `mmap` path currently exposes only completion; it needs an observed implementation or an explicit qualification gap. |
| Output file publication | `upload_file` and `slow_update_store_with_file` | Newly forwarded bytes and first successful CAS completion. A successful existence check can establish object completion, once. |
| Tree/Directory/stdout/stderr publication | `inner_upload_results`, `upload_captured_output` | Actual byte advancement or successful object completion, including spill files and small in-memory uploads. |
| Running process | The action manager's owned `child_process_guard` wait loop | A successful probe showing the owned child remains live and has not been reaped, while the attempt is Running and termination has not been requested. |

Source files:

- [Action manager](../../../nativelink-worker/src/running_actions_manager.rs)
- [Recipient](../../../nativelink-worker/src/authority_worker.rs) and
  [session transport](../../../nativelink-worker/src/authority_worker_transport.rs)
- [Fast/slow store](../../../nativelink-store/src/fast_slow_store.rs)
- [Whole-file transfer helper](../../../nativelink-util/src/store_trait.rs)
- [Hashing](../../../nativelink-util/src/digest_hasher.rs)
- [Worker protocol](../../../nativelink-proto/com/github/trace_machina/nativelink/remote_execution/worker_api.proto)

Current manager `Callbacks` expose only time and sleep. Aggregate store metrics
are shared by actions and cannot renew a particular attempt. Add explicit
observed variants while keeping existing legacy entry points; a task-local
callback alone is insufficient for shared downloads.

Forwarding bytes into a bounded pipeline is evidence of advancement, not proof
of durable CAS publication. A downstream stall can consume only the remaining
bounded pipeline credit. Once the bytes stop advancing, the lease must expire;
after all bytes are forwarded, an unacknowledged CAS commit must eventually
stall too. Completion credit requires successful settlement of the operation.

## Shared downloads and optimized paths

`FastSlowStore::get_loader` deduplicates concurrent fills through a shared
`OnceCell`. Only one caller performs the transfer; the other actions await that
same fill. A callback attached solely to the leader would falsely expire healthy
followers. Extend the shared loader with bounded latest progress state and allow
every interested caller to subscribe. A late subscriber observes the current
greatest offset, then subsequent increases. Cancellation detaches that observer
without canceling another action's required fill. Leader failure and replacement
must not reset the greatest offset retained by an attempt.

The ordinary read path, explicit `populate_fast_store`, and bypass-dedup path
must all reach the appropriate observation seam. Whole-file upload optimization
also bypasses generic byte-channel code: the fast/slow implementation calls
`slow_update_store_with_file` before moving a local file. Observing generic
`StoreDriver::update` alone misses this path.

Directory-cache hits/builds, read-only input mounts, relinking after eviction,
and BLAKE3 `mmap` hashing need separate inspection and fixtures. They cannot be
silently counted as complete at operation start. If an initial slice only
observes completed objects, it qualifies multi-object progress only: one healthy
blob, hash, or cache materialization lasting longer than a lease still has no
intermediate evidence. Full byte and shared-loader coverage is required before
claiming that long transfers generally stay live.

## Initial Action load and process liveness

The authority-only manager creation seam already bounds `create_action_info`
using `max_download_timeout`, before allocating an action directory. Command and
input-tree preparation has its own download budget; neither consumes the exact
finite Action execution timeout. These are separate local budgets, not one
shared end-to-end preparation interval. The existing paused-clock hanging-store
test establishes the initial Action-fetch bound.

The initial Action load happens before the `RunningAction` exists. Installing an
observer only after creation leaves this progress gap. A bounded local deadline
prevents an indefinite wait but does not keep a slow healthy Action load alive
past the authority's Accepted lease. Either pass the observer into authority
creation or qualify an explicit relation between the initial Action-load deadline
and the Accepted lease. No such real-time relation is currently qualified.

For Running, a periodic `Child::try_wait() == Ok(None)` probe within the owned
child wait loop is meaningful process liveness. Ownership of the child before
reaping prevents the numeric-PID reuse ambiguity of looking up a later `/proc/PID` or
sending a signal to a recycled PID. If the probe observes exit, process the exit
normally and stop renewal. A sleep or quiet compiler can remain live without
producing output. Stop observations after cancellation/termination begins and
retain existing process-domain quiescence requirements for releasing capacity.

Wrapper liveness proves only this defined Running liveness. It is not evidence
of input or output progress, successful execution, descendant quiescence, or
durable results. Persistent-worker execution remains separately unqualified by
the current authority launch seam. The execution deadline is installed once and
still fires even when every liveness probe succeeds.

## Required fault fixtures

1. Transfer one large input in gated chunks for longer than a progress lease.
   Genuine new offsets keep Preparing live. Stop the chunks while transport,
   capacity reports, and reconnects continue; the unchanged progress expires.
2. Repeatedly fail and replay the same prefix or re-fetch an already completed
   Command. Progress stops increasing until new work is accomplished. Exercise
   duplicate completion callbacks and registry exhaustion without eviction.
3. Let two attempts share one deduplicated fill. Both see advancement; cancel
   one subscriber, fail/replace the loader leader, and verify the survivor's
   observations and retained greatest offsets.
4. Upload multiple objects and a single large output, including stdout/stderr
   spill files. New bytes/objects keep Uploading live. A CAS that consumes all
   bytes but never acknowledges completion eventually stops renewing.
5. Gate hashing and optimized cache/mount materialization where supported.
   Document any path lacking intermediate observations; do not renew from its
   wrapper's continued existence.
6. Keep a real quiet child Running longer than a progress lease. Owned-child
   probes renew that lease, but its original finite execution deadline still
   terminates it. No later probe revives the revoked attempt or renews an
   Uploading/Preparing phase.
7. Reconnect and redeliver older/equal report sequences and progress values.
   Preserve the phase progress and original execution deadline across authority
   restart. A newer report sequence carrying unchanged progress is not renewal.
8. Delay initial Action creation in a real store fixture. Verify its independent
   local deadline, and separately test any future progress observer installed before creation.

Existing manager fixtures `FlakyStore`, `HangingStore`, and `GatedStore` provide
transient failures, stalled reads, and gated writes. A chunk-gated store is still
needed for real byte-level and shared-loader cases. Use a real child process for
Running observations and the committed authority adapter for lease/deadline
claims. Passing DTO or timer tests alone does not establish these behaviors.
