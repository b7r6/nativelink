# Authority Operations RPCs

The experimental authority implements `GetOperation`, `CancelOperation`, and
`WaitOperation` through its existing committed coordinator. This is test wiring;
the production execution factory is unchanged.

Get and Wait use the same committed `Observe` and bounded delivery path as
`WaitExecution`. Cancel proposes `Event::Cancel` and acknowledges only after the
commit. It cancels the named operation interest. Another coalesced caller can
continue the execution; cancelling a terminal operation succeeds without changing
its accepted result. Unknown or retired operations return `NotFound`.

Wait validates the protobuf duration before admission. Its duration bounds the
initial committed observation as well as subsequent updates. If no initial state
can be confirmed in time, it returns `DeadlineExceeded`; otherwise expiration
returns the latest confirmed operation, which may still be pending. A zero
duration can therefore expire before the initial observation. Dropping a timed
out wait releases its concurrency permit. An already queued read-only Observe
may still commit; it cannot renew interest or change authorization.

Tonic's transport also enforces a shorter RPC deadline. The pinned 0.14.6 stack
maps its `TimeoutExpired` error to `Cancelled`; the wire test records that actual
status. Ending the wait does not cancel the operation. `ListOperations` and
`DeleteOperation` remain `Unimplemented`; retirement follows committed authority
policy.

## Qualification

Two additional wire histories cover distinct coalesced operation names, unknown
instances/epochs/IDs, malformed durations, timed pending waits, a shorter transport
deadline, duplicate cancellation, terminal delivery, and restart. One runs a real
namespace process through the authority-only action manager: cancelling one caller
preserves the other caller's authorized attempt and full reservation; the other
caller completes and its accepted result reaches the AC. Both exact terminal
responses survive restart and the process runs once. Cancelling the completed
operation cannot rewrite its accepted result.

A unit fixture retains the coordinator input queue without driving a commit. It
demonstrates that the initial observation cannot outlive the declared wait and
that its permit is returned. Independent review found this missing initial bound
before the final implementation was tested.

The complete worker authority RPC suite passes 17 cases; service unit tests pass
six. Focused Bazel targets pass with strict lint checks. A separate focused Cargo
run emitted the real-process marker without skipping namespace execution.
[Receipts](evidence/authority-operations/manifest.json) preserve source boundaries,
initial formatting/compiler failures, successful checks, and source review.

These histories do not qualify production authentication, every cancellation
delivery cutpoint, or the canonical configuration entry point. Existing worker
tests separately cover cancellation receipts and uncertain reservations. The
later fixed-listener integration mounts Operations on the client role and checks
that valid Get and Cancel requests return `Unimplemented` on the worker role.
