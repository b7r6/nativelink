# Shared-authority representation and measurement gate

Status: unqualified design candidate, updated 2026-10-23. Bounded scheduler/journal
experiments are recorded below; combined publication, retention, and provider
costs remain unqualified. Later [typed retention](typed-retention-bridge.md),
[Azure](physical-azure.md), and [physical publication](publication-physical.md)
fixtures add functional evidence, without a combined workload measurement.
This memo is not permission to freeze a production representation.
See [contract.md](contract.md) and root-owned
`evidence/` for implementation and measurement receipts as they become available.

## Coarse protection candidate

Use immutable publication/operation/staging/input roots with renewable leases.
Each root references an immutable typed manifest of exact physical generations.
The independent oracle stores explicit per-generation protection sets. Production
stores root leases and bounded cohort summaries; closure expansion and mark
construction run outside apply. Acceptance validates identity-bearing preparation
evidence against current cohort and generation fences.

An allocation cohort has a finite generation population once sealed. Physical
locators include a store incarnation and generation that are never reused. A collector owns
an explicit namespace; authority journals/checkpoints and the metadata needed
to recover root manifests stay outside that collector's reach.

Candidate cycle, chosen for its safety/progress argument:

1. Seal an allocation cohort; subsequent writes use a newer cohort. Bound its
   generation population and metadata size through admission.
2. Commit closure of **new** protection acquisition into that cohort. Existing
   staging/acquisition guards must finish or expire under recorded rules before
   the close completes. Reserve collector service so new guards cannot prevent
   closure forever. New publishers wait within bounded queues or materialize
   fresh generations; waiting does not count as accepted publication.
3. At the closed cohort fence, capture active roots and their immutable closures.
   Build marks incrementally outside apply. A still-live root renewal changes
   expiry, not membership, and does not restart marking. Expiration during the
   pass may conservatively retain objects until the next pass. Reacquiring an
   expired root is a new acquisition and remains fenced.
4. Submit bounded retirement batches with cohort/fence identity and mark
   preparation evidence. Apply checks the cohort is still closed at that fence,
   rejects protected/retired generations, and records generation-specific delete
   intents. Unresolved writes remain represented even after successful deletion.
5. Reopen acquisitions against the remaining live generations, or retire the
   cohort after all obligations settle. Failed external deletes are retried with
   the same identity. Late writes into retired generations never revive them;
   orphan reaping continues until provider quiescence is established.

This trades publication admission delay and possible extra copies for bounded
collection work. It is one implementation policy, not an operator-selected GC
mode. It does not claim that all required evidence fits a constant-size command.
Bound cohort summaries, preparation batches, guard lifetime, and concurrent
closed cohorts. If realistic closures make this policy impractical, revise the
candidate before freezing formats; do not remove fencing or add a fallback.

Protection starts with an explicit staging guard before relying on an object.
The prepared root includes every typed strong edge, not just files directly
named by ActionResult. Preparation carries manifest identity, generation
population, and cohort revisions. Committed root installation transfers staging
protection without a gap. A delayed prepared result is invalid after a relevant
fence changed. Decoding and traversing large manifests must have bounded work on the apply
path.

Before an AC hit, select the authoritative head after the pinned read barrier.
Reuse a live lease long enough for the serving interval, or commit a coalesced
extension. Removing the head cannot revoke already granted coverage. Expired
roots need complete reacquisition, which may wait behind a closed cohort or
return a miss. Active reads need coverage through completion or an enforced
deadline. The timing model must prevent early expiry across elections.

## Proof and progress obligations

| Obligation | Counterexample the harness must rule out |
| --- | --- |
| Protection refinement | Any generation protected by the per-object oracle cannot retire in production |
| Complete preparation | A missing tree child or stdout/stderr object passes publication validation |
| Atomic transfer | A generation is briefly unprotected between upload/staging and live publication |
| Closed-cohort fence | A new root commits after the collector's root snapshot without entering its marks |
| Renewal distinction | Expired root renewal revives a retired generation; live renewal forces endless restart |
| Exact deletion | A delayed delete reaches a recreated logical object in a new incarnation |
| Late writes | Successful deletion discards the only record of an outstanding provider write |
| Bounded apply | One large closure or retire batch monopolizes application or exhausts state memory |
| Sustained progress | New acquisitions indefinitely prevent cohort closure or collector service |
| Fair recovery | With arrivals stopped and prerequisites restored, pending work never reaches terminal/reclaimable state |

The sealed population makes a pass finite only if root enumeration, traversal,
retirement, and effect execution receive service. Reserve those resources under
load. Existing root renewal can keep its own reachable objects alive indefinitely;
it must not prevent reclamation of unrelated unreachable objects. Measure space
amplification from expired roots retained for a pass, copied generations, upload
grace, conservative timing, and unresolved provider writes. No finite reclaim
bound exists without a provider quiescence assumption for arbitrarily late writes.

## Measurement ledger

### First scheduler/journal experiment

The [machine-readable manifest](evidence/synthetic-authority/manifest.json)
records exact commands, source hashes, dependency lock hash, hardware, profile,
raw sample hashes, and compressed observations. The workload uses the actual
OpenRaft 0.9.25 singleton and redb Immediate transactions. It submits cache
misses, dispatches, worker phase observations, completions, publication-handoff
acknowledgments, and terminal retirement. Each cycle snapshots charged attempts
with pending effects; restart compares complete recovered state. Handoff
acknowledgments are synthetic: no provider or AC sink receives these effects.

The initial runs await every `client_write` reply. Width is concurrent retained
work, **not outstanding proposal concurrency**. These rates describe serial
round-trip service, not saturated consensus throughput or a supported production
load. The shared host was not isolated; unrelated Bazel/rustc activity was
observed during the width-64 run and is retained in the evidence.

| Retained width | Request padding | Commands | Observed commands/s | Write p50 | Write p99 |
| --- | --- | --- | --- | --- | --- |
| 1 | 1 KiB | 221 | 46.56 | 16.50 ms | 57.46 ms |
| 16 | 1 KiB | 806 | 71.02 | 13.27 ms | 26.33 ms |
| 64 | 1 KiB | 1,924 | 28.32 | 26.84 ms | 130.08 ms |
| 16 | 16 KiB | 484 | 26.22 | 29.03 ms | 339.36 ms |

The tiny-state width-1 run measured median pure application at 617 nanoseconds and
snapshot/validation around 24 microseconds, against approximately 5.50 ms for an Immediate
redb replacement and 6.31 ms for a raw 1 KiB file write/sync probe. The width-64
peak core snapshot was 323,619 bytes; its median snapshot/restore costs were about
1.81/2.08 ms. These small samples point to durable round trips dominating the
serial small-state path, with full-state serialization becoming material as
state grows. They do not establish a throughput ceiling. The following
experiments distinguish outstanding proposals from application batching.

Read-barrier samples here are for a singleton: they measure the actual
`ensure_linearizable` path but contain no network quorum traffic. Snapshot
samples are sparse (one per cycle), so their high percentiles are descriptive
observations, not estimated service-level guarantees. Logical command bytes,
allocated database bytes, and actual filesystem write amplification are distinct;
the file size is not a measurement of bytes written to disk.

The prototype persists a complete scheduler state per applied batch. Production
format freeze remains blocked on combined publication/lease/retirement cost,
refinement through actual typed closures and providers, sustained collection at
representative load, bounded closure preparation, realistic workload
distributions, and recovery under load. The finite abstract refinement below
does not establish these implementation or scale properties.
This experiment neither supplies the missing Lane B baseline nor qualifies M0.

### Concurrent proposals and measured durable transactions

The [concurrency manifest](evidence/concurrent-authority/manifest.json) preserves
the next experiment separately. Balanced causal rounds submit actual
`client_write_ff` requests without awaiting their replies, then await every
response. Ingress order preserves monotonic operation admission; raw event
replay follows actual returned log positions. Each proposal still contains one
logical event. The outstanding count measures response handles before they are awaited, not
an instrumented count inside the Raft queue.

| Mode | Retained width | Events / entries | Outstanding handles | Events/s | Reply p50 | Reply p99 |
| --- | --- | --- | --- | --- | --- | --- |
| Concurrent | 1 | 56 / 56 | 1 | 62.90 | 13.05 ms | 66.08 ms |
| Concurrent | 16 | 484 / 484 | 16 | 63.70 | 227.94 ms | 536.04 ms |
| Concurrent | 64 | 1,283 / 1,283 | 64 | 42.75 | 1,457.47 ms | 1,898.82 ms |
| Serial causal-round control | 16 | 484 / 484 | 1 | 61.15 | 14.95 ms | 45.45 ms |

The width-16 comparison uses the same binary and source hashes. Width 1 precedes
a change that replaces a compiled source path with a runtime source path; the
manifest records that distinction. All runs use 1 KiB request padding and the
same durability, lifecycle, read, snapshot, and restart checks. Concurrency
increased waiting latency without substantially increasing observed throughput.
The small samples and shared host do not establish a sustainable capacity limit.

Optional in-memory journal counters then measured the storage boundary directly.
The [counter manifest](evidence/journal-counters/manifest.json) records a matched
pair: 66.17 events/s with concurrent proposals and 64.08 events/s with serial
causal rounds. **Both** 484-event runs made 484 append calls, 484 applied-state
calls, and 484 committed-position writes, each ending in an Immediate durable
transaction. Maximum append and apply batch size was one log entry. Including
three snapshots, each workload made 1,455 successful durable transactions.
These counters count transactions, not individual filesystem sync calls or
device bytes. They are disabled by default and never affect authority state.

This matches the pinned OpenRaft 0.9.25 source: `append_to_log` awaits the storage
append and its `LogFlushed` callback, while `process_raft_msg` runs engine
commands after each API message. Returning early from our storage append would
still leave the leader waiting for the callback before processing the next
proposal. Waiting in the adapter for another leader append therefore cannot
create the desired group. The next experiment batches above this boundary.

### Bounded application batches

The [application-batch manifest](evidence/application-batches/manifest.json)
records five runs from one binary and identical source hashes. A checked
application command contains at most 64 ordered events and 1 MiB of canonical
JSON. Each command occupies one real Raft log entry. Committed application
revalidates structural bounds before applying any event, applies the ordinary
reducer sequentially, and atomically persists the complete state, final outbox,
and applied log position. Per-event replies are returned after that transaction.
Append, committed-position, and applied-state durability remain Immediate.

| Mode / width | Padding | Events / entries | Durable transactions | Events/s | Reply p50 | Reply p99 |
| --- | --- | --- | --- | --- | --- | --- |
| Application batch / 1 | 1 KiB | 56 / 56 | 173 | 70.11 | 12.18 ms | 53.49 ms |
| Application batch / 16 | 1 KiB | 484 / 34 | 105 | 801.44 | 14.85 ms | 29.03 ms |
| Application batch / 64 | 1 KiB | 1,924 / 34 | 105 | 1,153.05 | 26.64 ms | 601.61 ms |
| Application batch / 16 | 16 KiB | 484 / 34 | 105 | 395.30 | 29.87 ms | 54.04 ms |
| Serial causal-round control / 16 | 1 KiB | 484 / 484 | 1,455 | 43.76 | 14.98 ms | 58.73 ms |

Transaction totals include snapshots: five at width 1 and three for the other
runs. The width-16 and width-64 batched runs each contain 30 full causal-round
groups plus four singleton registration/time observations. Append and apply
still receive one log entry at a time; the event group is inside that entry.
Raw observations include each event's actual log index and ordinal, every
proposal's encoded size, and the observed group size at every log index.

The workload constructs groups from already available synthetic causal rounds.
It does not implement or measure a production ingress aggregation queue,
arrival-driven fill ratio, aggregation delay, fairness, or backpressure. Events
in one batch share its commit/apply latency; the table does not include a
production client's wait for batch formation. The shared host produced a
601.61 ms batch reply at width 64 and a 1.925 s reply in the serial control.
Exact transaction amortization is established; the observed rate ratio is
affected by those stalls and is not a supported load envelope.

The grouping boundary is tested against sequential reducer results, including
an ordinary rejected event followed by accepted events. Tests also cover exact
count/byte bounds, unknown fields/versions, malformed later batches preserving
the entire prior state, commit cutpoints, snapshot-plus-batch suffix replay,
and process loss after an accepted three-event reply. Retrying a batch is not
blanket exactly-once execution: event identities govern deduplication. Effects
must come from the final committed outbox, not intermediate per-event replies.
Future publication ordering must retain event ordinal or an equivalent
persisted acceptance sequence when several acceptances share a log index.

The experimental journal envelope moved from version 1 to version 2 and rejects
the earlier prototype explicitly. This is not a production migration mechanism
or a format freeze. Production replication remains unqualified: OpenRaft's default maximum
payload of 300 **entries** could now represent 19,200 events and roughly 300 MiB
before framing. A future peer transport must bound bytes and events explicitly;
an inherited entry count is insufficient. A subsequent real three-member test
transport bounds append groups to four entries and aggregate RPCs to 8 MiB.
It checks directed partitions, lost replies, snapshot interruption, membership
replacement, and durable restart. It is an in-process fault harness, not a
qualified deployment transport or replication-throughput measurement.

### Finite protection refinement

The separate [retention contract](retention-contract.md) records the executable
coarse cohort candidate and independent per-generation oracle. Exhaustive
enumeration for its fixed alphabet and depth checked 508 states and 4,060
transitions. Named fixtures include a 64-tick renewal history that preserves
mark progress while collecting unrelated objects, and an eight-generation fair
drain that settles in its derived 22-step bound. Five semantic mutations are
detected: acquisition across a closed fence, expired renewal, erased captured
manifests, stale delete acknowledgments, and forgotten unresolved writes.

These checks cover flat abstract manifests and a finite identity population.
They do not measure typed traversal, actual provider quiescence, compaction under
infinite churn, retention bytes, shared-authority cost, or sustained production
collection. Generation and write admission reserve space for every possible
final delete intent. The snapshot receive path separately caps accumulation at
64 MiB before allocation and poisons failed streams; this experimental ceiling
still needs an admission byte budget that reserves serialized completion and
snapshot headroom.

| Input/cost | Required provenance and result | Current status |
| --- | --- | --- |
| Historical Lane B baseline | Original location/revision, unmodified lock/config/fixtures/command, hosts, durability, batching, raw latency/throughput | Harness not located; not run |
| Workload demand | AC hit rate, hot-key concentration, execution/direct-update/progress rates, tails and maxima | Production telemetry not supplied |
| Closure shape | Outputs, distinct transitive blobs, bytes, cohort span, reuse distribution | No measured representative distribution |
| Log/write demand | Logical commands, proposals, entries, replication bytes, durable bytes, batches and delay separately | Serial/concurrent proposals and bounded application batches measured, with durable transaction counts; replication/write amplification, production aggregation delay, and combined load missing |
| Read demand | Barrier quorum traffic, applied-state latency, any grouping linearization argument | Singleton barrier/decode measured; network quorum missing |
| Apply/state | CPU/event, pending effect bytes, root/cohort/index memory, bounded batch latency | Bounded scheduler apply/snapshot measured; finite publication/retention encodings tested separately; combined cost under load missing |
| Closure/provider | Traversal, hashing, uploads, conditional operations, request retries and throttle | Functional typed-closure and filesystem/Azurite fixtures pass; combined cost, throughput and provider throttling remain unmeasured |
| Recovery | Snapshot size/time, concurrent load, catch-up/restart, install/truncation cutpoints | Singleton snapshot/reopen measured; actual three-member partition, learner catch-up, interrupted snapshot and restart tested; combined load missing |
| Collection | Scan/retire/delete rates, new-acquisition stalls, orphan lag, retained/copy amplification | Not measured |
| Protection refinement | Coarse state versus independent per-generation protection, renewal progress, fair drain | Finite independent comparison, typed-closure composition and filesystem/Azurite fixtures pass; sustained collection and fair drain under combined load remain unqualified |
| Control | Pinned workload/hardware, hard bounds, baseline and saturation/recovery objectives declared in advance | Not calibrated |

For each family `i`, record logical event rate `lambda_i`, average and tail
encoded bytes `b_i`, apply cost `c_i`, and actual batch occupancy. Sum
`lambda_i * b_i` for logical payload demand and `lambda_i * c_i` for apply demand;
separately measure journal framing, replication, flush, snapshot, and retransmit
overhead. Proposal batching does not erase bytes or per-event CPU. Read barriers
are a separate budget from lease extensions. Admission headroom must cover
inflight completion, cancellation, reconciliation, and collection during storage
slowdown and snapshot work.

A synthetic experiment may report only its measured envelope. Record repository
and dependency revisions, machine/CPU/memory/disk, topology, payload generator,
seed, arrival pattern, limits, durability settings, warm/cold cache state,
commands, raw observations, and failure schedules. Do not attribute a new
synthetic benchmark to Lane B or reconstruct its reported headline number.

M0 qualification requires sufficient measured envelope and a checked refinement
argument before production format freeze. M5 repeats the costs with actual
filesystem/Azure adapters; M9 repeats with snapshots, collection, and control
enabled. A failing workload is a concrete blocker to report, not a reason to
move the threshold after the run.
