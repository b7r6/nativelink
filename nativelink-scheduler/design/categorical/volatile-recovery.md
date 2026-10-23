# Experimental volatile recovery

The durable and RAM realizations use the same `Journal`, redb transactions,
serialized writer queue, command validation, reducer, snapshots, outbox, and
OpenRaft 0.9.25 split storage interfaces. `Journal::fresh_volatile` substitutes
redb's `InMemoryBackend`. It allocates a new empty backend; clones share that
backend, and a subsequent constructor cannot recover it.

This is local protocol evidence. Production factories still construct the legacy
runtime. The test transport and identity allocator are explicit fixture code,
not a production membership reconciler or authenticated listener.

## Persistence boundary

`JournalPersistence` distinguishes the two backends. File-backed successful
commit notification follows an Immediate durable transaction. RAM notification
follows an atomic transaction whose state lasts only as long as that backend.
Measurements count these separately as `durable_commits` and `volatile_commits`.
A durable open failure never selects RAM.

The volatile constructor does not initialize Raft, join a cluster, or promote a
voter. It accepts a caller-supplied node ID and cannot establish whether that ID
has ever been used. Fresh identity allocation remains a responsibility of the
surviving authority and its eventual membership procedure. The cluster fixture
keeps its used-ID set outside lost members and refuses reuse explicitly.

## Exercised histories

The five tests in `tests/authority_volatile_test.rs` exercise the pinned storage
suite, shared-handle lifetime and complete loss, callback ordering and atomic
outbox cutpoints, admission limits, and malformed membership/snapshot rejection.
They use the same journal implementation as durable tests.

The ten tests in `tests/authority_cluster_test.rs` include the existing durable
histories and these additional cases:

- A lost volatile voter returns under a fresh ID as an empty learner. It cannot
  serve a quorum read or acknowledge writes. A surviving quorum sends a compacted
  snapshot and suffix containing a pending dispatch, an unreleased reservation,
  and lookup effects. Explicit promotion follows catch-up. The replacement then
  wins an election without losing the reservation or duplicating effects.
- With two voting histories lost, the remaining member cannot acknowledge a new
  write or serve an authoritative read. After total loss, three empty new IDs
  remain learners even when elections are requested. No fixture calls
  `initialize` to repair that lost quorum; no old acceptance is synthesized.
- Transport domain checks reject a higher-term foreign vote, a higher-term empty
  heartbeat, and first/middle/final foreign snapshot fragments before any Raft
  call. The test compares persisted vote, log position, membership, leader/term,
  applied state, and snapshot position. A legitimate snapshot still installs
  afterward.
- A response matrix holds actual vote, append, and snapshot responses, covering
  both success and a stopped peer's error. Changing either endpoint's domain
  before delivery rejects all twelve delayed cases. Six unchanged-domain
  controls preserve their original success or error. Error responses therefore
  cannot bypass the final identity check.

- A fresh volatile learner loses its received snapshot prefix and RAM. The
  fixture refuses reuse of that learner ID and admits another fresh learner
  through the surviving quorum. Complete snapshot plus suffix recovery retains
  the pending dispatch and charge before promotion or an election.
- A sender cutpoint withholds the final uniform membership entry after joint
  membership commits. All recipients retain the joint configuration. After the
  leader stops, survivors containing a majority of both configurations elect a
  successor, finish membership, and preserve pending effects. Restarting the old
  durable leader converges without treating its old proposal as committed.

The domain cases matter because OpenRaft's `cluster_name` is a tracing label.
Application events carry an epoch, but votes, membership entries, empty
heartbeats, and snapshot transport also need an authenticated domain boundary.
The fixture supplies trusted identity directly. It does not establish how a
production listener obtains that identity or protects against impersonation.

## Commands and remaining gates

The focused Cargo runs passed on this host:

```sh
cargo test --locked --offline -p nativelink-scheduler --profile smol \
  --test authority_volatile_test -j 24
cargo test --locked --offline -p nativelink-scheduler --profile smol \
  --test authority_cluster_test -j 24
```

These are in-process members with serialized RPC boundaries, not three-host
latency, throughput, durability-hardware, or deployed network qualification.
Automatic disaster reset remains unsupported. A new epoch cannot stop an old partition's
workers or storage writes; external fencing or an isolated execution/storage
domain remains necessary. Singleton volatile loss likewise cannot invent prior
operations, publication acceptance, or ownership of old objects.

[Composed recovery receipts](evidence/composed-recovery/manifest.json) retain the
final ten-test Cargo and Bazel passes. The first joint-membership fixture used
an overly broad sender cutpoint that also held an earlier membership catch-up;
its failure is retained. The cutpoint now matches the intended final voter set.
It delivers a valid preceding RPC prefix, not partially decoded wire bytes.

Production membership allocation, durable bootstrap manifests, capability-aware
peer admission, compatible persistent writers, and actual listener identity
remain unimplemented. The explicit experimental format still rejects earlier
prototype versions. This slice does not pass M3a/M3b as a whole or authorize a
production-format freeze.
