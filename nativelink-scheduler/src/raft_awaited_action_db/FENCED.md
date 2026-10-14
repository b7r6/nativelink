# The fenced scheduler (the swing-for-the-fences track)

Branch: `design/raft-fenced`, based on `design/raft-scheduler` (P1 and P2, see
[`DESIGN.md`](DESIGN.md)). This page is the shared contract for the parallel
workstreams. Where it says *provisional*, the TLA+ spec
(`nativelink-scheduler/spec/`) decides once it model-checks; where it says
*contract*, a workstream may rely on it without asking.

## The goal

**No timeout decides correctness.** A timeout may only *propose*. The one
place time enters is a failure detector, whose verdict takes effect only as a
committed, fenced log entry; everything after the commit is a deterministic
state transition keyed on epochs and attempt ids. The detector may be wrong
(a slow worker suspected, a partitioned leader still alive): safety holds
anyway, and only liveness depends on the detector eventually being right.

The pass condition this track is built to meet: with every scheduler timeout
in the config set to an absurd value (a day), no invariant fires on any
scenario that does not itself kill or partition something; and with them at
normal values, no invariant fires on any scenario at all.

## The model (provisional; the spec decides)

**Cluster.** N scheduler nodes run openraft. The leader serves: it alone
runs the matcher, holds worker sessions and client streams, and proposes.
Followers replicate and answer every serving RPC with `UNAVAILABLE`.

**Worker session.** A worker registers with the leader and is given a
session epoch, committed in the log (`OpenSession { worker, epoch }`). The
failure detector may propose `ExpireSession { worker, epoch }`; once that
commits, every message from that worker with that epoch is rejected
deterministically: results, acks, keepalives. A worker that returns
re-registers and gets a new epoch. A worker that reconnects after a leader
change *resumes* its session by presenting `(worker, epoch, running
attempts)`; the new leader checks that against the log.

**Attempt.** One execution of one operation on one worker session:

```
Offered ─accept─▶ Accepted ─▶ Running ─report─▶ Reported ─commit─▶ Committed
   │                 │            │
   └──── Revoked / Abandoned (session expired) ◀──┘
```

Every attempt carries `(attempt_id, worker, epoch)`. A result commits only if
the attempt is the operation's current attempt *and* its session epoch is
still live. At most one attempt's result commits per operation. The retry
budget counts attempts that reached `Accepted`, never timeouts that fired.

**Operation.** `Queued → Assigned(attempt) → Done(result)`. Every change is a
compare-and-set log entry (expected attempt / expected epoch), so there are no
retry-on-version-conflict loops and no stale writes.

**Client interest.** A reference count of attached watchers per client
operation id, not a keepalive. An operation is collected only when nobody
watches it and its retention has passed since the last watcher left.

**Barriers.**

- *Offer barrier.* Capacity is reserved at `Offered`; clients see `EXECUTING`
  only from `Accepted` (P2's `report_executing_on_accept`, made unconditional).
- *Revoke barrier.* An operation is requeued only after the holding worker
  acks the revoke, or its session's expiry has committed. Never two live
  attempts whose results could both commit.
- *Leadership barrier.* A new leader commits its term's blank entry, rebuilds
  derived state from the applied state, and only then reports ready and
  accepts sessions. A deposed leader drops every worker and client stream at
  once.
- *Read barrier.* Client reads (`WaitExecution`, `GetOperation`, listing) go
  through read-index (`ensure_linearizable`), so a deposed leader never
  answers from stale state.

**Deleted, by design.** `max_action_executing_timeout_s` (a hung action is
bounded by its REAPI `Action.timeout`, enforced by the worker; a slow action on
a live worker is just slow); the periodic sweep (events drive everything);
every retry-on-conflict loop; per-operation leases; `client_action_timeout_s`
(interest is a reference count). What remains time-based: the failure
detector (`worker_timeout_s` becomes its input), raft's election and heartbeat
timers, and retention (garbage collection, never a correctness decision).

## Cluster contract (wave 1)

**Config** (`ExperimentalRaftSchedulerBackend`), *contract*:

```json5
experimental_backend: {
  raft: {
    node_id: 1,
    data_dir: "/var/lib/nativelink/raft",
    snapshot_every: 1000,
    // Absent: a single node, exactly the P1/P2 behavior.
    cluster: {
      // Where this node listens for raft peer traffic (a dedicated listener
      // the backend starts itself, not a `servers` entry).
      listen: "127.0.0.1:7401",
      // Every voter, this node included. `address` is what this node dials,
      // so a test rig may point it at a fault proxy for that link.
      peers: [
        { node_id: 1, address: "http://127.0.0.1:7401" },
        { node_id: 2, address: "http://127.0.0.1:7402" },
        { node_id: 3, address: "http://127.0.0.1:7403" },
      ],
      // Liveness only. Defaults: 250 / 1000 / 2000.
      heartbeat_interval_ms: 250,
      election_timeout_min_ms: 1000,
      election_timeout_max_ms: 2000,
    },
  },
}
```

**Bootstrap**, *contract*: the lowest `node_id` in `peers` initializes the
membership with every peer as a voter when its log is empty; the others wait to
be contacted. Initializing an already-initialized cluster is not an error.

**Serving**, *contract (amended)*: on a node that is not the leader, `Execute`,
`WaitExecution`, `GetOperation`/listing and the worker API's `ConnectWorker`
fail with `UNAVAILABLE` and the message `not the raft leader; leader is node
<id>` (or `not the raft leader; leader unknown`). No address: the only address
a node knows for a peer is its raft peer address, which clients cannot use.
CAS, AC, ByteStream and Capabilities are served on every node (they are
storage, not scheduler state).

**Readiness**, *contract (amended)*: the health server's readiness route
(`/ready` by default) answers ready on a node only when all of these hold: it
is the leader; it has applied its term's blank entry; and a quorum has
acknowledged it within the last `election_timeout_min_ms` (so a deposed leader
that has not yet heard of the new term stops reporting ready on its own,
before any new leader can be elected). The ready answer's body is
`{"node_id":<id>,"term":<term>}`; a router that sees two ready nodes takes the
higher term. A single node (no `cluster` block) is ready only after its log
and snapshot are replayed and its open-time proposals are applied. The
liveness route (`/status`) is unchanged.

**Peer transport**, *contract*: a gRPC service on `cluster.listen`, one RPC per
openraft network call, each carrying the openraft request and response
serialized as JSON in a `bytes` field. The transport is generic over how
connections are made and accepted, so a deterministic simulator can supply
both ends.

**Leader change**, *wave 1 behavior*: a new leader, after the leadership
barrier, proposes `LeaderChanged { node_id, term, now }`, which for now has
`ReplicaRestarted`'s semantics (requeue what was executing, clear sessions).
Session resume replaces it in wave 2.

**Deleted knobs stay parseable**, *contract*: a timeout this track deletes
(`max_action_executing_timeout_s`, `client_action_timeout_s`, ...) stays in the
config schema, is accepted and ignored with a one-time warning, so existing
configs (and `deny_unknown_fields`) keep working.

**The absurd-timeouts set**, *contract*: the knobs set to a day for the pass
condition are [`TIMEOUTS.md`](../../spec/TIMEOUTS.md) K1–K8 and K21
(`retain_completed_for_s`, `client_action_timeout_s`, `worker_timeout_s`,
`max_action_executing_timeout_s`, `unacknowledged_kill_timeout_s`,
`dispatch_ack_timeout_s`, `unsatisfiable_action_timeout_s`,
`no_worker_action_timeout_s`, `kill_revoked_operations_interval_s`). Worker-side
bounds (K26–K37) are a separate, later pass condition: today several of them
still decide outcomes with no fault injected (see TIMEOUTS.md "FENCED.md gets
wrong or misses" item 16).

## Amendments after wave-1 review

Findings from the inventory (stream B) and the rig (stream D) that change this
page; the spec (stream A) has them as inputs.

- The revoke barrier needs a defined ack: "the worker has stopped the attempt
  and will not publish anything for it", which today's kill path does not give
  (kills are visible only as `ABORTED` after upload and cleanup; persistent
  kills reach only the pid; `setsid` children survive).
- The Action Cache is outside the fence: workers write it before reporting, so
  a superseded attempt can publish. AC publication must be fenced (attempt and
  epoch) or move to the leader on commit.
- Retention collects finished operations only; an unwatched live operation is
  cancelled through the revoke barrier first. Watcher counts are leader-local,
  so retention restarts for every operation at leader-ready.
- Dispatch-ack and kill-ack deadlines are detector inputs (proposals measured
  from the send), like worker silence.
- `unsatisfiable_action_timeout_s` and `no_worker_action_timeout_s` are leader
  policy timers that propose a logged `FailQueued`; client-visible, never
  safety-relevant.
- Time inside replicated records is leader-stamped and clamped to log time; no
  safety property may depend on it.
- Reads are side-effect free (today `WaitExecution`, `GetOperation` and
  listing cancel stale operations).
- Ownership (holder, ack, kill, capacity) moves from the in-memory worker
  scheduler into the log, so a new leader can rebuild obligations.
- Worker-local state (action directories, persistent processes) is keyed by
  attempt, not operation.
- Workers' reconnect backoff (doubling to 30 s, reset only on success) bounds
  how fast a fleet returns after a leader change; wave 2 caps it and resets it
  on a readiness change.

## Test rig contract (rechaos fleet rig)

- Storage is its own process (CAS + AC on a filesystem store). Scheduler
  nodes serve execution, capabilities and the worker API, and reach storage
  through `grpc` stores, so a failover never strands blobs on a dead node.
- A leader proxy (two listeners: client, worker API) polls every node's
  readiness route and forwards new connections to the ready node; when
  leadership moves it closes every forwarded connection.
- Every directed peer link `a → b` goes through its own link proxy, so a step
  can cut, delay or blackhole one link.
- New steps: `leader-kill` (kill -9 the current leader for `for_ms`, then
  restart it on its state), `node-kill` (`node`), `link-cut` (`from`, `to`,
  `for_ms`, optional `both`), `partition` (`nodes`: isolate a set from the
  rest). In a single-node rig, `scheduler-kill` keeps its meaning.

## Workstreams and file ownership

| Stream | Owns | Delivers |
|---|---|---|
| A: spec | `nativelink-scheduler/spec/*.tla`, `*.cfg`, `spec/README.md`, `spec/PROTOCOL.md` | TLA+ model, TLC runs, the protocol proposal for the wave-2 contract freeze |
| B: inventory | `nativelink-scheduler/spec/TIMEOUTS.md` | every timeout, retry, sleep, interval and wall-clock decision, classified, with its replacement |
| C: cluster | `raft_awaited_action_db/**` (except this file), raft config in `nativelink-config`, a new proto, minimal serving gates | multi-voter openraft, peer transport, leader-serves, read-index, readiness, leader change |
| D: rig | `~/src/b7r6/rechaos-nativelink-raft` (local git only) | multi-node fleet rig, leader and link proxies, the new steps, the absurd-timeouts mode |
| E: sim | a new `nativelink-sim` crate | deterministic simulation (turmoil) of nodes, workers and clients with fault injection and a history checker |

Each stream works on its own branch `fenced/<stream>` off `design/raft-fenced`
and may push it to `origin` at will. Commits use
`PRE_COMMIT_ALLOW_NO_CONFIG=1` and end with the co-author line. Merges into
`design/raft-fenced` are done by the coordinator after review.
