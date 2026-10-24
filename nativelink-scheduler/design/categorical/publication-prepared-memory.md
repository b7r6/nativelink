# Prepared memory reports in the finite publication candidate

The private-v5 candidate joins a prepared result graph to actual State's
MemoryFailure transition. It retains the existing execution, operation, physical,
stage, root, write and snapshot caps. It does not activate accepted-history
feature 3, route a real worker RPC, change a production format or complete M6.

## Commands and authority

`enable_prepared_memory()` enables the existing private-v4 terminal controls and
records a separate private-v5 activation position. The whole activation is
atomic, including refusal when the commit counter cannot represent both steps.
Earlier formats omit both the activation marker and optional stage metadata.

`stage_memory(stage, candidate, report, until, writes)` uses the existing
Candidate for the exact captured ExecuteResponse, canonical ActionResult bytes
and trusted complete typed closure. It requires a worker publisher. The report
adds a nonzero ID, epoch, original charged resource vector, optional memory peak
and quiescence. Action, execution, attempt, worker and incarnation come from the
candidate's existing qualified identity. The report does not copy its payload or
closure into another retained ledger.

Stage admission previews actual MemoryFailure on a discarded State clone before
spending stage credit. The preview checks current authority and report validity;
it retains no retry or terminal decision. Existing physical observations and
`prepare(stage, Evidence)` establish exact closure, catalogue revision, fences,
readiness and coverage. A present empty ActionResult still follows this graph
path. The old no-graph report path continues to require an absent ActionResult.

`accept_prepared_memory(stage, publication, until)` checks the prepared graph,
shared report identity, original grant and current authority on the outer commit
clone. It applies actual MemoryFailure against current capacity. Capacity changes
between stage and acceptance may change a prospective retry into a final failure
or the reverse. A zero original memory grant uses a positive peak when supplied.
The actual reducer chooses the exact final reason and releases only the original
reservation after the quiescent report passes its checks.

## Retry and terminal ownership

A retry records the decision's resource target, position and time. It creates no
terminal acceptance, publication, operation root or AC head. The original stage,
guard and outstanding writes retain their existing lifetimes. A replacement Match
still has to reserve another stage and its full member allowance before becoming
executable. Reaching the unchanged eight-stage cap can refuse that Match.

A final decision retains its ResourceFailed reason alongside the existing exact
response. It transfers the staging guard to a transient publication root,
installs roots for only the operations actually terminated with this result, and
releases the transient publication root. An independently canceled subscriber
keeps its original terminal and receives no result root. A previously selected AC
head remains selected. A memory failure never selects an AC head, even if a
future policy gives its diagnostic payload a different representation.

Acceptance and operation protection share one outer transaction. If any required
operation lifetime exceeds prepared coverage, refusal restores authority,
reservation, stage, roots and metadata together. Late writes settle their
original obligations and cannot change an accepted terminal or select a head.

## Identities and replay

The graph report ID is separate from stage and publication IDs. Stage admission
reserves it against both graph and no-graph reports, with one report identity per
attempt and at most three per execution, 24 overall. Acceptance repeats the shared
identity check. Abandonment releases the stage guard but retains the candidate,
report identity and attempt reservation. This alphabet has no report retirement command,
so abandoned identities can consume finite availability.

Acceptance records its publication and lease arguments even for a retry. An
exact replay returns Duplicate before checking current authority, expiry or
publication reuse. Changed arguments conflict. A retry does not reserve a
publication ID globally: a later terminal stage can use that ID, and the original
retry still replays as Duplicate. Re-preparation after classification refuses;
the accepted proof remains immutable. Stage abandonment and later write outcomes
do not remove the retained classification or graph.

## Restore and size boundary

Restore checks the shared graph/no-graph report identities, original reservation
chain, retry targets, locally checkable reason priority, acceptance positions and
times, exact operation terminals and retained graph/root ownership. It rejects
removal of required classification, feature-marker downgrades and inconsistent
final publication links. Current workers cannot reconstruct an earlier growth
ceiling, so restore does not recalculate past decisions from current capacity.

These are consistency checks on retained facts. They cannot distinguish every
altered byte from a different legal history: an unused nonzero report ID, unused
retry publication argument or policy-irrelevant peak can describe another valid
input. No append-only journal or cryptographic history proof is claimed.

The [refreshed snapshot ledger](publication-snapshot-bound.md) charges 518 added
JSON bytes per stage and no new response or closure copies. Its canonical state
bound is 2,026,948 bytes, leaving 70,204 bytes under the unchanged 2 MiB cap. All
eight graph classifications and all 24 no-graph rows are charged together even
though the shared identity cap prevents that combination. History protection and
history-object capacity remain separate work.

## Qualification boundary

Candidate scenarios cover both digest algorithms, capacity changes in both
directions, zero-grant peak growth, all three final reasons, mixed graph/no-graph
retry chains, canceled subscribers, prior AC heads, original pending writes,
atomic preparation and protection refusals, fixed stage capacity, duplicate
arguments, restoration after capacity changes and inconsistent snapshot fields.
Existing private-v1 through private-v4 scenarios remain in the same test target.

The candidate author had read the earlier head and no-graph reference models
during the gap audit. The new prepared-memory reference implementation and its
tests were kept unread through the candidate source and test freeze. The new
reference author received the command/result contract first. Independent
comparison and compiled mutation results require their separate qualification
receipts; candidate self-tests alone do not establish that comparison.
