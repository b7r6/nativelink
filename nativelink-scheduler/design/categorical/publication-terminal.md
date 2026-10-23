# No-graph terminal publication composition

The private terminal successor composes actual `authority::State` lifecycle
decisions with the existing publication and coarse-retention candidate. It adds
exact no-graph worker outcomes, cancellation and fenced expiry. It does not
activate production publication, accepted history or collection.

## Acceptance and retained ownership

An explicit conversion selects private snapshot version 4. The no-graph report
boundary requires a captured, validated ExecuteResponse with no ActionResult,
plus its qualified action, epoch, execution, attempt, worker and incarnation.
Complete and MemoryFailure use their respective actual scheduler events. Only
that transition decides retry versus terminal and the resource-failure reason.
The outer transaction records the exact response and observed disposition,
commit position and logical time before checking its complete snapshot bound.

Retry reports have stable identities but are not terminal acceptances. Identical
repetition is a no-op; conflicting reuse refuses unchanged. A terminal acceptance
has no invented CAS object, ActionResult, graph root or AC head. An operation is
associated only if the actual scheduler terminated that handle with the accepted
value. A previously canceled joined handle remains canceled. Later worker loss
does not revoke the provenance of an already accepted report.

Cancellation, interest renewal, interest expiry, attempt expiry and execution
timeout use actual scheduler events. Advancing time alone expires nothing.
Every terminal-producing route closes unused logical stage/member credit,
including worker loss and incarnation replacement. It preserves existing stages,
write outcomes, original reservations and cancellation intents. Cancellation or
exhaustion does not prove quiescence; Cleanup still owns the original refund.
MemoryFailure requires confirmed quiescence and the exact original charged vector.

Existing graph publications, operation roots and serving grants retain their
promises. Unknown writes remain represented across retry, cancellation, expiry
and terminal outcomes. A late write cannot create an accepted terminal or head.
Identical repeated write observations in private v4 preserve both snapshot bytes
and the catalogue revision used by preparation.

## Recovery checks

Restoration checks exact original requests, report origins, terminal values,
operation associations, released report reservations and retained write identity.
The original request, successive charged vectors and classified retry targets
must form one consistent chain. A missing retry record cannot silently erase the
evidence for memory growth. Immutable growth evidence, attempt ordinal and final
reason must agree. Report and graph acceptance positions must respect monotonic
logical time; retained positions cannot alias another recorded authority event.

These checks use retained facts. They do not recompute an old growth ceiling from
workers whose capacities may have changed, authenticate a snapshot against a
cryptographic event history, or prove every conceivable coordinated rewrite.
Removing the terminal section cannot make a final memory failure, grown target
or last-interest execution cancellation load under an older private alphabet
that could not produce it.

## Bounds and qualification boundary

The existing caps allow eight executions, eight workers, eight stages and
terminal acceptances, sixteen operations, sixty-four generations and eight
members per stage. Each execution can retain at most three classified reports.
Execution/operation admission reserves these finite row counts. The proposed
canonical snapshot must fit the unchanged 2 MiB ceiling. A complete reserve for
all future outer bytes and retry-stage capacity remains unqualified.

The [candidate checkpoint](evidence/publication-terminal/manifest.json) passes
54 candidate, eight existing comparison and nine physical cases in Cargo;
strict Clippy passes for those three targets. The integrated Bazel run also
passes all eight actual-worker cases. Sixteen new scenarios exercise the
terminal extension. Candidate mutation qualification and comparison against the
corrected terminal reference follow in separately recorded boundaries.

The candidate author and its separate scenario author used the frozen terminal
contract without reading the new terminal reference reducer or tests. The
scenario source was captured before candidate implementation access. Review
preserves the initially failing snapshots and test integration corrections;
qualification receipts distinguish those boundaries from later fixes and
compiled mutation campaigns. The independent reference has its own
[checkpoint and evidence](terminal-outcome-reference.md).

Graph-bearing ResourceFailed outcomes require a later prepared-outcome path.
Feature-3 history requires exact history objects and their own protection.
Canonical worker/gateway acknowledgment, production direct-client routing,
durable section activation, migration suffixes and sustained collection remain
separate work. No M0 through M9 gate is marked complete by this experiment.
