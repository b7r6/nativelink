# Independent no-graph terminal-outcome reference

This additive test authority relates genuine scheduler terminal decisions to
recorded worker provenance, operation ownership and existing retention promises.
It does not create an empty `ActionResult` or a publication when the captured
`ExecuteResponse` has no result. The separately frozen
`contract-v1.md` in the accompanying evidence was handed to the candidate author
before either successor implementation was shared.

The reducer imports the existing independent M1 relational oracle and independent
retention oracle unchanged. Only authority data types are imported from the scheduler
crate; production `State`, `Coarse`, publication reducers, request projection and
response validation methods are not used to decide expected transitions. Real
request captures and response envelopes in the cases are assembled independently
from protobuf messages, checksums and the frozen capture framing. Data-type readers are
shared vocabulary, so their existing deserialization validation still runs.

Before this work, its author reviewed the *previous* actual-State composition,
request-binding fixes and lifecycle source for the design. The new candidate
reducer and tests were not read. This is an independent implementation using
previously frozen lifecycle policy, not a claim of blind authorship or a new
independent implementation of M1. The candidate author received the contract only.

## Ownership and state

A new report preserves the exact response payload and explicit classification.
M1 decides whether `MemoryFailure` grows the target or becomes `ResourceFailed`,
including the original failure and exact policy reason. A terminal acceptance
records the original worker/attempt/incarnation, epoch, acceptance position and
logical time. That record survives later worker replacement. Retry classification
is retained separately and cannot become an accepted terminal or an AC head.
Stable report identity makes identical repetition a no-op and rejects conflicting
reuse. Both classifications require `result=None`; even an empty result or one
with an error needs the separately qualified prepared graph path.

Each operation's original captured request is read from the M1 relation, not the
execution's representative request. Binding includes the complete action key,
trusted principal/configuration and selected immutable platform properties before
resource extraction. Priority and queue time may differ between joined handles.
Cancellation and exhaustion have independent terminal links, with no invented
response or worker origin. Actual timer events enforce generations and identities;
advancing time alone cannot cancel work.

A terminal closes unused logical stage/member credit. It does not refund a revoked
physical reservation: M1 keeps that original charge and cancellation effect until
actual cleanup. Confirmed memory failure releases its exact original grant once.
Earlier stages and stable write outcomes survive retry, cancellation, finalization,
expiry and abandonment. A late write changes its existing storage observation only.

The retention oracle uses small fixture handles. A full namespace/incarnation/
generation identity has an exact one-to-one mapping to each handle; no truncation or role-based
physical namespace splitting is used. Initial publication, operation and serving
protections are explicit trusted scenario facts. Stage closures are supplied
canonical member sets; this reference allocates fresh staged generations and one
stable unknown write per member. It does not independently traverse a protobuf
result graph, perform provider I/O, import a previous graph publication, or collect
retired generations. The frozen retention oracle's collector tests remain separate.

## Bounds, recovery and scope

The private snapshot frame is `independent-no-graph-terminals-v1`. It is separate
from the frozen synthetic publication-v1 and real-request publication-v2 formats.
Every changing command commits an outer clone only after both oracle readers,
relationship invariants and the complete 2 MiB canonical snapshot admit it.
Ignored and duplicate events do not consume a new position. Recovery checks each
original operation binding and preserves exact bytes, report classification,
recorded origin, charges, stages, write outcomes and logical credit associations.

The limits permit at most eight executions/workers/stages/terminal acceptances, sixteen
operations, sixty-four generations, eight members per stage and three reports per
execution. Execution admission reserves report/terminal row capacity structurally
through those fixed bounds. It also reserves one stage and eight member slots;
retry matching needs additional capacity when a prior stage is retained. The
capacity test demonstrates atomic refusal instead of borrowing another admitted
execution's unused credit. Up to eight explicit initial protected-root groups,
128 stable writes and 32 roots fit the bounded retention fixture.

The aggregate snapshot gate is measured by a valid large-response history that
first admits terminals and then refuses the next terminal unchanged at the outer
byte ceiling. This is not a proof of prepaid future outer bytes, retry-stage
availability, production throughput, infinite-churn progress or eventual completion
under permanent capacity pressure. History feature 3, receipt retirement, execution
record retirement, graph-bearing `ResourceFailed`, direct-client publication,
production M3a/M6 formats and production activation are excluded.

The frozen publication-head reference requires an `ActionResult` and has an
acknowledged opaque-response-token limitation. Activation v2 inventories retained
terminal facts, but is not a lifecycle transition oracle; its separate byte guard
remains required. This new alphabet extends neither artifact silently.

## Qualification

The Cargo target currently passes 29 cases: 22 new composition cases and seven
unchanged embedded M1 cases. New cases cover SHA-256/BLAKE3 no-result completion;
all three memory-failure reasons; growth from nonzero grants and explicit peak
with zero grants; graph-bearing refusal; invalid authority/grant/quiescence;
joined and last-interest cancellation; renewal and explicit expiry; progress and
execution timeouts through exhaustion; worker-unavailability exhaustion; retained
unknown writes and unrelated roots; historical origin; per-operation principal
binding; recovery cuts; count/credit caps; duplicate/conflicting identities; and
the outer snapshot byte gate.

The mutation receipt, exact qualified source hashes, tool logs and any integrated
Bazel result accompany the frozen patch as separate evidence. Compilation failures
are not mutation kills. The seven inherited M1 cases are not counted as new
terminal-outcome cases. This target has not itself compared the successor candidate;
a later differential must freeze both sides first and map neutral observations
without copying expected decisions from the implementation.
