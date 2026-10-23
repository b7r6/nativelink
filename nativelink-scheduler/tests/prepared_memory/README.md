# Independent prepared memory-failure reference

This additive finite oracle composes prepared graph-bearing memory failures with
the unchanged independent M1 scheduler relation. The author read the source plan,
the frozen publication-head and no-graph terminal reference contracts, and the
candidate author's explicit public command/result contract. The new candidate
implementation and tests were not read. Existing reference sources and receipts
remain unchanged.

The neutral command and observation vocabulary is in [`types.rs`](types.rs).
Unchanged graph vocabulary and scenario validation come from the publication-head
reference. Unchanged report, classification and qualified-action vocabulary come
from the terminal reference. Only authority data types are shared with production;
production State, preparation, publication, retention, request projection and
snapshot implementations do not decide expected transitions.

## Command contract

The model starts with trusted principal/configuration facts, a bounded scenario
of exact worker response/result bytes and independently declared complete typed
closures, and optional unrelated serving protections. Each step records a command
and the physical facts observed for that command. Those facts model authoritative
verification and retirement fences, not untrusted worker assertions or provider I/O.

| Command | Contract |
| --- | --- |
| `Admit` | Capture each operation's original request; bind full action, principal, execution configuration and immutable selected platform. Reserve stage credit for a new execution. The common alphabet fixes operation retention to absolute tick 100. |
| `Scheduler` | Forward the listed lifecycle, capacity and timer controls to the unchanged M1 relation. Raw Complete and MemoryFailure are refused here. |
| `Stage` | Bind stage ID, scenario candidate, independent report ID, epoch, exact worker identity/classification/response, lease and stable write IDs. Validate a cloned memory-failure transition without retaining its retry decision. |
| `Observe` | Record a matching authoritative write receipt. Identical repetition changes nothing; conflicting identity/outcome refuses. Late outcomes remain valid after classification or abandonment. |
| `Prepare` | Verify exact result, typed closure, protected generations, current catalogue revision, namespace fences and physical readiness. Preparation remains separate from worker authority at acceptance. |
| `Accept` | Revalidate stage/proof/authority and apply MemoryFailure against current worker capacity. Record either RetryScheduled with its target or the exact ResourceFailed reason and original payload. |
| `NoGraph` | Accept existing Complete or MemoryFailure reports only when the captured ExecuteResponse has no result. Share report and attempt identity reservations with graph staging. |
| `Abandon` | Release the stage guard while retaining stage and write records. This does not establish that physical writes stopped. |

The candidate contract has no proof ID or caller-supplied complete/verified
flags. The reused neutral preparation vocabulary fixes its ID to the stage and
requires both flags true; those are oracle evidence checks, not additional
candidate API fields. The typed closure is independently supplied by the scenario.
This experiment does not independently walk all REAPI tree edges.

Report IDs are distinct from candidate, stage and publication IDs. Staging reserves
one globally shared report ID and one report per attempt. Reusing a stage ID
refuses, including identical staged replay. A different report ID for the same attempt, or conflicting reuse
across graph and no-graph paths, refuses atomically. At most three distinct reports
belong to one execution.

Acceptance arguments are `(stage, publication, until)`. A first acceptance needs a
nonzero publication ID absent from accepted publications and a valid lease even
when the decision retries. An exact accepted-command replay is Duplicate before
current authority checks; changed arguments conflict. A retry does not reserve its
publication argument globally. A later stage may use that ID, and replaying the
original retry remains Duplicate afterward.

## Ownership and outcomes

Only committed acceptance selects the memory policy. Capacity changes after
staging/preparation can change retry into terminal or terminal into retry. The
fixed M1 policy selects MissingGrowthEvidence, NoLargerEligibleWorker or
AttemptBudgetExhausted, releases the exact original charge once, and preserves
immutable client requirements. Neither the report's exit code nor text establishes
the explicit MemoryFailure classification.

Retry keeps the old stage, its provisional protection until expiry/abandonment,
stable writes and known/unknown outcomes. It creates no accepted terminal,
publication, operation root or AC head. A replacement match must reserve fresh
stage/member capacity; it cannot borrow another admitted execution's credit.

Terminal acceptance records exact response, worker provenance, position, time
and ResourceFailed reason. Each operation actually terminated by this acceptance
receives an operation root covering the complete physical closure through its
admitted retention deadline, fixed to tick 100 in this alphabet. That deadline
is distinct from the acceptance publication lease. Preparation must cover both
for a final outcome; a shorter proof can still support a retry. Failure of the
operation protection check rolls back classification and charge release.
Previously canceled handles remain canceled. The
transient publication root is released after transfer; the final observation has
only operation roots, no retained publication root and no AC head. Unrelated
serving roots remain intact. A no-graph final after a graph retry does not invent
a graph root or erase the earlier write obligations.

This lease observation is bounded by the fixed operation deadline. Admission at
or after tick 100 refuses. It does
not qualify serving an operation after that protection expires, renewing roots,
or reconciling production operation lifetime with storage retention.

## Recovery and finite limits

The private `independent-prepared-memory-history-v1` snapshot stores initial
scenario/context/seed facts and the complete sequence of changing commands with
their observed physical facts. Restore replays that sequence and requires exact
canonical encoding. It reconstructs earlier capacity decisions from their original
ordered observations, rather than applying today's capacity to an old report.
Ignored, duplicate and rejected commands are absent from the history.

This is an independent history-based recovery oracle, not a production snapshot
format or the candidate's structural snapshot validation. A coherent edit producing
another valid history can be accepted. No cryptographic history authentication is
claimed. Removing preparation, changing an original grant, invalid replay order,
truncation, trailing bytes and format downgrade are tested corruption classes;
they must not be generalized to all coordinated snapshot edits.

Bounds allow eight executions/workers/stages, sixteen operations, eight members
per stage, sixty-four materialized/reserved member slots, twenty-four report IDs,
three reports per execution, thirty-two roots, 192 changing events and a 2 MiB
snapshot. Stage guards last at most ten ticks and acceptance leases at most 100.
The aggregate snapshot gate can refuse a proposed event; future outer bytes and
unbounded retry-stage availability are not prepaid or qualified.

## Qualification scope

Cases cover both digest families; exact graph/response binding; live joined
operations; all three failure reasons; zero-grant peak evidence; capacity changes
in both directions; stage/no-graph ID conflicts; both mixed retry/final orders;
retry publication-ID reuse and duplicate replay after revocation; incomplete,
stale, retired and unavailable preparation; retained late/unknown writes;
stage-credit and event-count bounds; and every restore/suffix cut of a terminal
history with later capacity changes. Tests inject invalid command/proof facts and
corrupted replay histories. Compiled semantic-mutation evidence, when run, is
reported separately; rejected malformed inputs are not mutation kills.

```bash
cargo test --locked --offline --profile smol -p nativelink-scheduler --test prepared_memory_reference_test
cargo clippy --locked --offline --profile smol -p nativelink-scheduler --test prepared_memory_reference_test -- -D warnings
bazel test //nativelink-scheduler:prepared_memory_reference_test
```

The seven embedded M1 cases are inherited cases, not new prepared-memory cases.
History feature 3, direct client publication, collector progress, provider writes,
real RPCs, production format activation and deployment qualification remain
outside this alphabet. Neither the fixture nor a later differential closes those
gates.
