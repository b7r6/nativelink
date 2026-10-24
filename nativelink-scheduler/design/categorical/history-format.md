# Accepted-history activation qualification

Feature 3 enables the prospective accepted-history component through the
existing `Prepare`, `Activate`, and `Abort` control vocabulary. This is the
experimental journal's trusted in-process qualification path. It does not
establish a distributed admission service, physical provider incarnation,
production cutover, or rollback after activation.

## Identity and compatibility

The fixed logical namespace is SHA-256 over these concatenated bytes:

1. `nativelink.accepted-history.namespace.v1` followed by one zero byte.
2. The authority epoch as an unsigned 64-bit big-endian integer.
3. The authority namespace as an unsigned 64-bit big-endian integer.

`format::accepted_history_namespace` exposes this derivation for explicit sink
binding. Process incarnation, leader, permit, feature epoch, and storage provider
configuration are absent. A sink must check the logical namespace before I/O;
this hash does not attest which physical provider owns it.

`ReaderCapabilities::AcceptedHistory` supports features 1, 2, and 3. Its
serialized member evidence remains `ReaderEncoding::DirectScheduler`.
Preparation evidence carries no new field or enum variant. Actual admitted
executable profiles must support every requested feature, including learners
and both sides of joint membership.

Core snapshots remain version 5 before optional components and version 6 with
receipt retirement alone. Feature 3 selects version 7. Feature 1 independently
selects the outer direct scheduler representation. Restore and snapshot install
require exact feature/component parity and the namespace derived from the outer
authority domain. Existing terminal work gains no history records.

An unsupported reader may decode the frozen control vocabulary in an
uncommitted suffix. It cannot join an unresolved preparation requiring a newer
profile. Once the commit marker covers activation, even an older applied
snapshot cannot admit the old reader. This qualification does not turn an old
binary into a decoder for newly introduced application commands.

## Proposal and activation ordering

Every runtime mutation enters `Singleton::write_batch`, then
`Journal::propose_batch`. The automatic service coordinator already uses this
path for requests, worker reports, timers, receipts, and effect outcomes.
Replicated wrappers share the same journal and trusted registry. Deliberate
storage-boundary tests may bypass the proposal helper.

The registry's shared enqueue mutex serializes permit allocation, application
admission, and the actual OpenRaft API send. It is released before waiting for a
quorum, read barrier, or response. The pinned OpenRaft API sends to its queue
before returning the fire-and-forget receiver. Canceling a caller cannot remove
an already queued proposal.

Adding feature 3 freezes application proposals as soon as the permit is
allocated. A canceled activation leaves that permit pinned. Waiting normal
writers release the gate, allowing the current leader to resolve the control.
They do not commit substitute `Busy` results or stop the service for a routine
pause. Other feature preparations retain their previous admission behavior;
queueing any activation pauses old-epoch proposals until its decision.

Receipt activation relies on the existing core invariant that worker/hold/receipt
headers were already prepaid before the feature. Enabling it creates fresh
streams without allocating new identities or increasing byte credit. Thus a
valid intervening normal transition cannot introduce a new receipt-enablement
capacity refusal, including when receipt retirement follows accepted history.
Feature 1 changes no core state. Accepted history is the feature that introduces
additional prepayment and therefore requires the Prepare-to-decision freeze.

After Prepare applies, the helper clones the exact state and enables the target
components in deterministic order, receipt retirement before accepted history.
History activation validates every retained nonterminal action and prepays its
additional bounded metadata and effect capacity. It consumes no later result
or provider observation.

If preflight fails, the helper commits the existing Abort and reports refusal
only after that Abort applies. If the abort outcome is unknown, the permit and
freeze remain unresolved. A committed Activate is never reinterpreted as a
refusal or no-op. Successful activation installs the component and feature
metadata in one journal transaction.

The commit marker can resolve registry admission before state-machine Apply
finishes. A normal writer compares the applied feature set with the registry's
committed requirements and passes a real read barrier when they differ. It
cannot stamp an old feature epoch behind a committed activation.

## Recovery and defensive storage checks

`Journal::resolve_preparation` is the explicit current-leader handoff operation.
After a real read barrier, it preserves an already committed activation or
aborts the original unresolved permit. A later activation attempt uses a fresh
permit. Closed-journal recovery still requires every earlier process to be
externally stopped and fenced; this helper supplies no such fencing proof.
Aborting an already aborted permit returns its known outcome without appending
a duplicate control at a different log position.

The caller must establish that the earlier workflow was abandoned. This helper
cannot distinguish an abandoned preparation from another live caller's
preparation, so ordinary service startup must not invoke it unconditionally.
The fixtures perform this resolution explicitly during recovery and handoff.

When features are present in applied state, registry admission, or the incoming
or retained suffix, append validation reconstructs the contiguous candidate
suffix from the applied state, overlaying incoming entries on retained entries.
It stages preceding normal transitions before validating activation capacity. Each decoded entry,
batch, and state retains the existing byte and count bounds; replay streams the
retained suffix without collecting another history-sized list.

The freeze boundary is the log interval from Prepare through Activate or Abort.
An earlier queued normal entry remains valid even if registry admission has
already closed. A normal entry inside the frozen interval, an infeasible
activation, a wrong feature epoch, or a history command before activation fails
before any incoming entry is persisted. Prefix replacement and missing suffix
positions fail closed. These storage errors defend against bypass and malformed
input; normal runtime concurrency is resolved before proposal.

An entirely plain base history retains OpenRaft's generic storage contract,
including reconstruction of older log rows after state-machine application.
This fast path cannot omit a preparation in applied state or in the retained
suffix not yet applied, and registry admission also forces the checked path.

The format tests cover independent feature orders, unchanged bytes before activation, failed-prepay aborts, cancellation, application lag at the committed
boundary, staged-prefix rejection, snapshot namespace/component forgery,
uncommitted old-reader decoding, cold recovery, and successor abort. The
application-pause hook is a deterministic local qualification cutpoint. It
changes no persisted representation or authority decision.

## Concurrent helpers and engine boundaries

A decision is owned by its proposing member and leader term before the actual
Raft send. Another Abort from that owner waits outside the enqueue gate for the
original decision, including an Activate already in flight. It returns the
original committed Abort position instead of appending a second control. A
newer leader may resolve an abandoned earlier-term decision after its real read
barrier; a stale or conflicting term cannot acquire that ownership.

Preparation also compares the supplied applied metadata with the registry's
committed requirements under the registry lock. If a previous decision committed
after the helper's barrier, the helper releases the enqueue gate and repeats the
barrier before allocating a permit. A stale applied preparation after Abort is
handled the same way. This protects control proposals as well as normal work
from the commit-to-Apply interval.

The pinned OpenRaft 0.9.25 engine does not send already committed or purged old
rows back through the append callback after snapshot installation:
`src/engine/raft_state/log_state_reader.rs` treats positions below the committed
next index as present, `src/engine/handler/following_handler/mod.rs` appends only
the unseen suffix, and `src/storage/helper.rs` repairs a last-log boundary below
last-applied by purging to last-applied before startup. Tests install an activated
snapshot and append/apply its subsequent normal suffix. Arbitrary direct-storage
reconstruction of missing covered rows remains outside the feature-bearing
journal contract; the plain-base storage-suite path is preserved separately.

The registry regressions deterministically exercise concurrent decision
admission and stale applied metadata. The real-Raft preparation test suspends a
second helper after its successful barrier, commits the first activation, and
resumes the second during the Apply pause. Sixteen concurrent Abort helpers also
verify one decision position and a healthy subsequent ordinary write. The
prepare and Apply pauses change no persisted representation.
