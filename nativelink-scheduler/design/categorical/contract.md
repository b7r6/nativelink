# Categorical scheduler contract

Status: implementation contract under qualification, 2026-10-22. Draft 0.3 of
`/home/b7r6/nativelink-categorical-elimination-plan.md` supplies the objectives.
The subsequent user instruction authorizes continued implementation beyond M1;
it does not waive evidence gates, authorize deployment, or freeze formats.
This document specifies intended behavior, not completed implementation.

See [source-map.md](source-map.md), [budget.md](budget.md), and
[morning-ledger.md](morning-ledger.md). Validation receipts belong in `evidence/`.
Production command, snapshot, and retention representations remain provisional.

## State and transition boundary

`step(state, recorded_event)` returns new state, reply data, and ordered effect
intents. Only committed events reach this function in a live authority. An
accepted reply follows application; no dispatch or acceptance follows a mere
proposal. Pending effects are part of state, installed atomically with their
originating decision. An effect result is another recorded event, never a direct
mutation by an asynchronous callback. Duplicate or uncertain delivery reuses the
effect identity and exact recorded payload.

The reducer does not read time, randomness, the environment, or external storage.
IDs, elapsed-time observations, external evidence, and controller proposals are
inputs. Ordered collections or explicit sorting define iteration and comparison.
Snapshots preserve uncertainty and effects as well as the visible lifecycle.

| Entity | Distinct identity and authoritative content |
| --- | --- |
| Authority | Cluster epoch, membership, applied position, active format capabilities |
| Action | Instance/namespace, digest function, action digest including size |
| Operation | Stable client handle, priority, interest, observation version, terminal result |
| Execution | Action key, coalescing scope, request requirements, original enqueue ordinal |
| Attempt | Execution, attempt ID, worker incarnation, current authority, lifecycle progress |
| Worker | Worker ID, incarnation, transport session, declared and reconciled resources |
| Reservation | Original charged vector; charged, quarantined, or released state |
| Effect | Effect ID, originating transition, exact payload, destination, unresolved outcome |
| Publication | Publication ID, immutable result and typed closure, provenance, acceptance order |
| Object | Content identity, store incarnation, physical generation, write/retirement state |

Operation, execution, attempt, session, and incarnation IDs are never
interchangeable. An election changes none of them by itself.

## Events and lifecycle

Event families below are semantic requirements; names and wire encodings are
not a persistent-format commitment.

| Event family | Commit-time checks and result |
| --- | --- |
| Register/reconcile | Validate epoch and incarnation; account for uncertain old processes before exposing capacity |
| Admit | Validate bounded input, identity, cacheability, coalescing scope, requirements, and completion headroom |
| Match/reserve | Choose eligible queued work and a fitting incarnation; charge once and install dispatch intent atomically |
| Dispatch outcome/worker accept | Match effect, attempt, incarnation, and session; transport acknowledgment alone is not execution completion |
| Input/running/output progress | Advance only the identified current phase/deadline generation; heartbeat does not renew action progress |
| Complete | Validate current attempt/incarnation, stabilize terminal handles, install accepted publication/history intents |
| Cancel/interest expiry | Make only the affected handle terminal; request physical cancellation when no interested handles remain |
| Cleanup/reconcile | Release original reservation once only when termination/ownership evidence permits |
| Time/timeout | Monotonic logical observation and matching deadline generation; stale timers do nothing |
| Effect outcome | Deduplicate by stable effect identity; preserve unknown external outcomes across restart |
| Prepare/publish/protect/retire | From M5 onward, validate generation and preparation fences before changing reliance or retirement |

Lifecycle phases distinguish queueing, reservation/dispatch, worker acceptance,
input preparation, execution, output publication, and terminal observation.
Public REAPI stages project these phases. Execution timeout applies to execution;
queueing and transfers have separate progress obligations. Removing an observer
does not remove operation interest or stop cleanup.

An authority revocation may allow a replacement attempt elsewhere, but does not
prove the old process stopped. Its original reservation stays charged or the
affected physical capacity stays quarantined. Changed resource advertisements,
reconnect, and a new incarnation cannot mint capacity over uncertain obligations.

The current bounded implementation interprets a worker's `available` vector as
an aggregate admission ceiling **including** existing charges. Placement uses
the smaller of capacity and that ceiling, then subtracts charged reservations.
A raw free-memory sample must be translated at the protocol boundary; treating
it as reusable headroom could spend one observation twice. A confirmed dispatch
decline closes admission until a fresh capacity observation; heartbeat alone
does not reopen it.

Progress-stall deadlines have renewable generations. Execution duration has a
separate immutable deadline and `ExpireExecution` event, fenced by attempt ID.
Repeated progress must not invalidate the absolute execution timer. Constants
in `authority/types.rs` are virtual test ticks, not calibrated seconds or a
production timing promise.

The prototype accepts explicit monotonically allocated operation IDs and retains
a retirement watermark, preventing a retired client handle from binding to new
work. The eventual admission sequencer must allocate/enqueue those IDs in
authority order, or move allocation into committed application. Arbitrary
client-selected numeric IDs and out-of-order allocation are not supported by
this prototype. This is an integration obligation, not a REAPI requirement.

## Fixed request and placement rules

| Request | Completed lookup | Inflight sharing | AC publication |
| --- | --- | --- | --- |
| Cacheable, ordinary | Permitted | Compatible nonterminal execution | Successful, error-free accepted result |
| Cacheable, skip lookup | Bypassed | Actual compatible execution, not a pending lookup or retained terminal result | Same eligibility |
| `Action.do_not_cache` | Bypassed | Never; independent execution | Never; operation-result retention is separate |

The qualified AC key excludes invocation scope. Coalescing compatibility includes
scope, normalized requirements, and action identity. Preserve Buck2 invocation
scope as a request fact: valid Buck2 metadata with a nonempty invocation ID uses
the existing identity/invocation derivation; other requests have no such scope.
Distinct scopes do not share running execution, but completed AC results remain
shared. The old configurable isolation switch is not a new scheduler selector;
the converter must report its replacement by this fixed rule. Identity metadata
is not authorization. Preserve the existing action-platform/command-platform
fallback for clients such as Goma, with explicit validation of malformed or
conflicting duplicate properties.

Provisional fixed platform semantics:

- The internal request has a typed nonnegative resource vector and exact string
  requirements. Resource dimensions carry fixed units and checked arithmetic.
  Reservation subtracts requested amounts from reconciled capacity.
- A request's exact property requires the worker to advertise the same value.
  Missing worker properties do not silently satisfy requirements.
- The protocol boundary owns one versioned, implementation-defined mapping from
  recognized resource names/units to typed dimensions. Its inventory must be
  completed before RPC integration; this document does not invent unit aliases.
  Unsupported typed dimensions, malformed quantities, ambiguous aliases, and
  unknown legacy `Minimum`, `Priority`, or `Ignore` rules are errors.
- Ordinary properties without resource types are exact requirements. Legacy behavior that
  ignored a missing worker property is a compatibility change, called out by
  migration fixtures. Arbitrary property rewriting is retired; a converter must
  prove equivalence to a fixed normalization rule or block the candidate.

Queue order is `(minimum live-handle priority, original enqueue ordinal,
execution ID)`, with lower numeric priority more urgent and default zero.
Joins/departures change the ordering key of queued work without resetting age. Assigned attempts are
not preempted. A deterministic bounded worker selection rule must be shared by
the model and implementation specification, while their code stays independent.
Strict priority gives no unconditional starvation bound under endless more
urgent demand; progress claims assume admissible higher-priority load and fair
eligible selection.

`single_use` is a worker lifetime fact, not an allocation-policy selector. Such
an incarnation admits at most one distinct action and never becomes reusable
after reconnect, completion, or decline after acceptance. Duplicate delivery of
that same attempt remains idempotent. A launcher must establish a fresh isolated
incarnation and reconcile old processes before advertising replacement capacity.
The Buck2 file-capture integration is inventoried separately; preserving scope
does not by itself certify capture behavior.

## Invariants and fault envelope

| Invariant | Observable predicate / first gate |
| --- | --- |
| AUTH 1–2 | At most one authoritative attempt; stale epoch/incarnation/attempt/session cannot mutate a successor / M1 |
| EFFECT 1–2 | Effects originate in applied committed state and retain logical identity across uncertain retry / M1 |
| RES 1–2 | Original vector charged once, released at most once; uncertainty cannot increase usable capacity / M1 |
| TERM 1 | Terminal result immutable; observation version cannot regress / M1 |
| LOOKUP 1 | Request matrix and coalescing compatibility hold / M1 |
| PUB 0 | Every worker sink write/history association follows current-attempt acceptance / M4a |
| PUB 1–2 | Latest accepted authoritative AC head; complete typed output closure protected while served / M6 |
| GC 1 | Protected generations cannot retire; old deletes cannot reach replacements / M5–M7 |
| REC 1–2 | Snapshot plus suffix preserves effects, uncertainty, protections, and format capability boundaries / M1, M3a |
| CFG 1 | Any legacy top-level `stores` or `schedulers` key fails before construction or effects / first canonical entry point |
| CTRL 1 | Bad controller proposals cannot breach authority, accounting, retention, or hard bounds / M1 onward |
| LIVE 1–2 | Fair recovery and supported sustained load permit completion, reconciliation, and reclamation / each applicable gate |

| Authority realization | Permitted recovery; qualification required |
| --- | --- |
| One durable member | Recover identity, vote, log, snapshot, applied state, and effects before serving / M3a |
| Several durable members | Available quorum and preserved history; minority cannot accept changes / M3b |
| One volatile member | Loss means loss of authority; fresh externally fenced epoch, no invented prior acceptance / M3b |
| Several volatile members | Lost history rejoins as a new learner under surviving quorum; no automatic replacement quorum / M3b |

No mode promises physical exactly-once execution or progress without quorum,
eventual delivery, advancing time, usable required storage, and admissible load.
Fresh epoch alone does not fence an old partition. Authority data is outside CAS
collection ownership. An empty catalogue cannot sweep a previous namespace.

Timing remains a qualification obligation: leader-local monotonic clocks are
not serialized deadlines. A new leader must conservatively reestablish elapsed
intervals; uncertain expiry may delay reclamation but cannot shorten a granted
serving window. Define clock/drift assumptions and finite request deadlines
before qualifying real-time serving guarantees.

## Integration and migration handoffs

M2 changes worker dispatch, receipt, progress, running reports, cancellation, and
completion to carry sufficient attempt/incarnation/effect identity. Unsupported
workers are rejected before dispatch. Worker-side receipt storage and process
reconciliation must handle crash between launch and receipt recording.

M4a moves AC and accepted history effects after committed acceptance. Its
temporary mutable sink permits an older accepted write to finish after a newer
one; operation results remain stable. Workers lose sink credentials and direct
AC/history write paths. The worker listener has CAS, ByteStream, and WorkerApi;
every writable client-AC route excludes worker credentials or traffic. Different
ports with permissive shared trust are insufficient. CAS mappings cannot alias
the mutable sink. History-browser response templates are explicitly retired.

M6 accepts immutable results and typed closures under the same authority as
completion. Its derived index selects the latest accepted publication in commit
order after a valid applied-state read barrier. Existing serving protection
survives head replacement. Noncacheable operation results have separate roots.
Direct AC updates validate Action/Command presence and cacheability, record
client provenance, and use the same publication/protection machinery. They do
not fabricate worker attempts.

M4-to-M6 activation assigns every old sink effect a committed disposition.
Unknown requests stay isolated in the old namespace. Retained terminal results
gain validated protection before new collection takes ownership; impossible
validation blocks that namespace's collection rather than discarding results.

The canonical schema contains role endpoints, instance/namespace identity,
authority identity and resource facts, storage placement, credentials, listener
trust, and collector ownership. Any occurrence of legacy `stores` or
`schedulers`, even null or empty, fails before side effects. The offline converter
parses legacy data without Redis clients or network access and emits a candidate
plus explicit placement, encoding, boundary, and removal diffs. Unresolved
destinations or ownership produce a non-deployable candidate. Cutover stops old
admission, drains or fences attempts, isolates old writes, establishes the new
epoch/cohort, then admits work. Cold cache is the default; import is finite and
passes the new publication protocol.

## Durable authority recommendation

Provisional selection: a local redb journal, one serialized writer, behind
OpenRaft **0.9.25** with `serde` and `storage-v2`. Root-owned dependency evidence
must record the exact redb pin, lockfile checksums, actual OpenRaft feature graph,
and Cargo/Bazel results. No adapter is qualified by this recommendation.

Persist vote, log metadata, append/truncate records, and snapshot metadata using
durable transactions. Successful log flush notification follows durable commit,
not enqueue or append return. A failed commit fails the storage operation; it
does not fall back to RAM. Apply decision and outbox atomically in recoverable
state. Bound blocking writer work outside async executor threads. On restart,
recover a validated snapshot plus committed suffix; never independently recover
an outbox at a different applied position.

Snapshots use a versioned checksummed envelope with authority/namespace,
membership, applied position, active feature epoch, and required reader
capabilities. Prepare and validate before atomic publication/install; truncate
only after a recoverable snapshot is durable. Unknown authority-bearing sections
fail closed. Pre-activation writers keep the old compatible format; installing
a new executable alone cannot cross the rollback boundary.

The implementation uses the pinned split storage interfaces and has exercised
their upstream storage suite, real singleton replies, restart, and read barriers.
The exact lockfile and experiment source hashes are recorded in `evidence/`.
Primary API references:
[OpenRaft storage](https://docs.rs/openraft/0.9.25/openraft/storage/trait.RaftLogStorage.html),
[state machine](https://docs.rs/openraft/0.9.25/openraft/storage/trait.RaftStateMachine.html),
and [read barrier](https://docs.rs/openraft/0.9.25/openraft/raft/struct.Raft.html#method.ensure_linearizable).
Initial documentation retrieval failed; subsequent inspection used the locally
resolved OpenRaft 0.9.25 source and executable storage tests. redb documents
immediate transaction durability; that documentation alone does not prove our
adapter ordering or hardware behavior:
[redb durability](https://docs.rs/redb/4.4.0/redb/enum.Durability.html).

Required cutpoints include vote/append/flush callback, apply/reply/effect send,
snapshot publication/install/truncation, pending effects during feature
activation, and injected I/O failure. Use the pinned OpenRaft storage suite and
real process-kill/restart tests as well as the independent state model.
