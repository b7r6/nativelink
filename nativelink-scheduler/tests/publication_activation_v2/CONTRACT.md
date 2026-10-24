# Additive activation v2 contract

This successor preserves the frozen v1 reference and its evidence unchanged.
Its separate scenario alphabet adds the retained terminal facts that v1 cannot
represent: cached terminals, unrecorded origins, independent operation
cancellation, and terminal responses without an ActionResult.

## Terminal inventory

The input has separate maps for execution terminals and operation terminals.
An operation retains its own ID, joined execution ID, exact terminal variant and
payload, version, interested flag, and interest deadline. An execution retains
its own ID, qualified action, exact terminal, and optional `terminal_at` logical
time. These maps must come from a complete retained checkpoint inventory; this
oracle does not claim that a hand-written subset proves inventory completeness.

Terminal variants are Completed, ResourceFailed with its exact reason, Cancelled,
and Exhausted. Completed and ResourceFailed retain the complete old ResultPayload,
including its optional captured ExecuteResponse. A captured response retains its
version, exact bytes, and SHA256. It can contain no ActionResult. A separately
scenario-owned optional result descriptor denotes the independently decoded
ActionResult bytes and expected complete typed closure. Absence means no result
graph, not an invented empty ActionResult. Cancelled and Exhausted carry no
ResultPayload. The oracle does not implement protobuf parsing.

Execution provenance is tagged Unrecorded, RecordedCacheLookup,
RecordedWorkerClassification, RecordedWorkerAcceptance, or RecordedAuthority.
Classification is not proof of accepted origin. RecordedWorkerAcceptance retains
its recorded worker identity and an actual acceptance record (an effect ID and/or
committed position where available). Historical `terminal_at` is a logical time,
never a fabricated commit position. The ordinary M4 terminal view and retained
PublishResult do not establish accepted worker identity by themselves.

Each operation has separately tagged Unrecorded, RecordedExecutionTerminal, or
RecordedIndependentCancellation provenance. RecordedExecutionTerminal requires
its exact terminal value to equal the execution terminal; membership in the
execution alone does not establish this tag. A canceled joined operation stays
canceled when the execution later completes. Its history identity cannot silently
be replaced by the execution's later worker acceptance.

Accepted-history associations belong to their recorded execution, preserve their
own original worker acceptance identity and exact diagnostic bytes, and do not
require an ActionResult. An independently recorded history origin can coexist
with an Unrecorded terminal origin. Missing history proves neither cache lookup
nor worker acceptance. A worker classification alone cannot fill absent history
acceptance fields.

## Atomic activation

Every retained operation result graph, execution result graph, and accepted
history object has an explicit typed root. Duplicate graph objects across roots
are allowed; omitting an owner is not. A terminal without ActionResult remains
byte-for-byte represented and creates no fictitious result root. Every real root
must obtain verified physical generation protection before activation succeeds.
Pending history without verified protected bytes blocks activation. Namespace
quarantine does not waive missing root coverage or turn a blocker into success.

Cold activation is the default and can succeed with Unrecorded terminal origins:
exact retained facts and protected graph obligations do not require inventing a
publication acceptance. Selected AC imports are narrower. They require an
eligible result, an authorized migration publisher, newly verified protection,
and recorded original acceptance provenance. This version allows imports of
RecordedWorkerAcceptance terminals only; additional cache import authority is a
separate contract, not an inferred old worker acceptance. Former worker revocation
does not revoke the recorded acceptance.

The v1 obligations still apply independently: every retained sink effect gets a
committed disposition; exact IDs, payloads, uncertainty and late observations
survive; an opaque effect remains opaque even with no enumerated requests; the
old domain can have unknown outstanding requests even with no retained effect.
Old and new destinations require independently qualified distinct physical
domains and confinement. Late outcomes cannot select a head or rewrite either
terminal map. New collection ownership excludes the old domain. Activation,
observations, and every suffix replay preserve these facts atomically.

## Independence and limits

Sources are the user's categorical plan, frozen v1 reference/types, public M4
terminal/view/effect/history DTO declarations, and root-reported State facts.
No new activation or publication candidate implementation was read. A bounded
State-field search also exposed snapshot byte-writer plumbing; a bounded public
ResponsePayload search exposed its deserializer header. No transition algorithm
or accepted-history implementation was used. Scenario-owned extracted result
bytes/typed closures are independent expected facts, not candidate-computed data.
This is a bounded oracle, not a real-checkpoint inventory adapter, protobuf parser,
provider verification, production format, or power-loss qualification.
