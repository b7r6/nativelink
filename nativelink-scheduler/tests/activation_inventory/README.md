# Retained authority inventory candidate

This test-only adapter reads actual `authority::State` and projects retained
terminal owners into the frozen publication activation v2 alphabet. It is not a
second scheduler, a durable activation reducer, or a production migration tool.
The v2 reference and its additive snapshot-byte admission guard are prerequisites.

`SourceInventory::capture` retains the original State snapshot, complete public
observable view, accepted-history records, and exact history object bytes. The
view includes original request payloads, operation versions, priorities, deadline
generations, outbox effects, and execution metadata. Projection restores the
snapshot and recaptures these fields before accepting an externally supplied
inventory. Projection failure does not discard or rewrite the source inventory.

The smaller v2 projection preserves each execution terminal and each operation's
own terminal. A canceled joined operation stays canceled even if its execution
later completes. Completed and resource-failure terminals retain their complete
captured ExecuteResponse, failure reason, and optional ActionResult. A response
without an ActionResult creates no invented result graph. An unsupported wire
encoding remains in the source inventory and causes projection refusal.

An accepted-history record supplies its original epoch, effect, execution,
attempt, worker, and incarnation. No commit number is inferred from a timestamp.
Without that record, execution origin remains unknown, including cache hits and
worker terminals from an older State. Operation origin remains unknown because
State does not retain a separate terminal-cause tag. Materialized history still
has opaque old-request exposure: an observed successful write does not prove that
all older retries stopped.

## Explicit external bindings

Action labels are opaque fixture handles mapped uniquely to the complete
ActionKey, trusted principal, execution configuration, and semantic platform
properties. Captured request principal must agree with the external binding when
the original request payload exists. Older requests without a payload require an
explicit trusted binding. Execution scope is excluded because it is an in-flight
compatibility rule, as are priority, cache flags, resource estimates and grants,
retry targets, and timing. The Action digest already includes immutable Action
bytes, including its timeout. The complete request remains in SourceInventory.

Namespace labels are opaque 16-byte v2 fixture handles mapped uniquely to
full physical namespace identities. Actual 32-byte CAS identities are never
truncated. Result and history roles in the same physical CAS use one namespace
label, reader, and catalogue; role names cannot manufacture isolation. The old
mutable sink has an explicitly supplied complete external destination identity,
not an identity inferred from an action's instance name.

Result graph preparation uses the production protocol-typed closure reader with
an explicitly supplied verified reader and physical catalogue. Blob, Directory,
Tree, and ActionResult roles carry complete physical incarnation and generation
identities. History binds exact original bytes to a separately verified physical
object. Missing or ambiguous bindings refuse projection. The fixture catalogue
and read interface are trusted preparation inputs. They do not prove retention,
collection ownership, a closed external send inventory, or deployment fencing.

Activation facts remain a separate input to the frozen model. Tests author the
expected typed closure from scenario bytes before comparing it with extraction;
then they provide separate catalogue readiness, fences, and protection facts.
No successful read is converted into a claim that collection is safe. The old
mutable domain and every projected old intent remain opaque, with empty request
maps meaning unknown rather than quiescent.

## Qualification and limits

The tests drive actual State through cache lookup, worker acceptance, independent
operation cancellation, worker errors without ActionResult, two final memory-failure
reasons, pending and materialized history, and snapshot/suffix recovery cuts.
They compare original retained bytes and independently authored owner/closure
facts before cold activation in the bounded v2 model. A shared physical CAS case
checks that result/history roles cannot split a namespace identity. Refusal cases
cover modified source inventory, missing bindings, unsupported response wire bytes,
and missing preparation or retired protection facts.

The adapter is bounded by the actual State snapshot and frozen model limits.
It inventories retained terminal owners only; unfinished executions remain in
the lossless source and are not converted into new execution state. Older raw
request/send inventories are unavailable and remain opaque. Additional admission
policy is needed before this can become a production converter. Durable
activation, durable publication heads, service wiring, and deployment cutover
remain outside this candidate.

The reference was frozen before this candidate read production transition code.
Candidate development used public State data types and events, request/response capture
APIs, accepted-history read methods, and the production physical-closure reader.
The memory-failure transition was inspected after a fixture with a nonzero grant
correctly retried instead of becoming final; the fixture was corrected to express
missing growth evidence. This candidate makes no new independent-reference claim.

## Original requests on joined operations

The lossless source now includes each operation's own original request, obtained
through the read-only State query. The snapshot correspondence check covers this
map as well. Projection checks that original request's captured principal and
complete qualified action against the execution's explicit authority binding.
A representative execution request cannot supply another caller's principal.

The experimental State compatibility predicate can join requests from different
captured principals when there is no invocation scope. External domain routing
belongs to the outer authority. A regression drives that valid State directly,
retains both original requests, and refuses the foreign operation's projection.
A positive case preserves different priority, queue time and lookup policy on
joined requests without changing their shared action identity. The original
16-case evidence archive remains unchanged; this correction has separate evidence.
