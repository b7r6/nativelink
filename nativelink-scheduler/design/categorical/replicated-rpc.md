# Experimental replicated RPC coordinator

`authority_replicated_rpc_test.rs` runs the existing automatic RPC driver over
three real OpenRaft members with separate file-backed journals. The public
`Singleton { raft, journal }` fields already permit this test composition; the
driver, reducer, journal and production construction paths are unchanged.

The fixture follows the scheduler cluster tests' bounded serialized transport
pattern. Requests and responses cross JSON serialization; append batches contain
at most four entries, snapshot fragments at most 512 bytes, and aggregate RPC
messages at most 8 MiB. Its nodes have fixed local identities in one authority
domain. Heartbeats are 100 ms and the election interval is 400–800 ms. Membership
starts with one voter, adds two caught-up learners, then commits three voters.
The test explicitly elects the successor after imposing a bidirectional
partition. It does not qualify peer authentication or membership discovery.

Execution and worker connections use actual tonic RPCs. A fixture TCP forwarding
endpoint closes existing worker connections when its backend changes. The same
recipient and reconnect supervisor remain alive through the change; sessions
advance without replacing the worker incarnation or its retained receipts. This
is an explicit routing procedure, not a production leader-discovery mechanism.

The history first accepts action A while holding its publication intent. Action
B starts a real process in a PID namespace and remains running. Both surviving
replicas must apply B's grant before the leader is isolated. A distinct action X
submitted through the isolated leader cannot produce a committed handle or
appear in its applied state. The fixture retains that actual RPC: stopping a
client wait would not erase an unresolved proposal.

The other two voters elect a higher-term leader. B completes locally during the
coordinator outage. The successor starts the same service with publication still
held and must retain A's exact terminal result and full publication effect,
including its identity. After routing changes, the existing worker reconnects,
and its full B candidate becomes the terminal response for B's original handle.
B keeps its original dispatch identity and has one attempt. Both process marker
files contain exactly one execution.

The higher-term majority commits before the old partition heals. If the old
driver has already rejected its RPC after a failed quorum read, the fixture
removes its stopped endpoint instead of reviving it. Otherwise the live old
member receives the higher-term log, settles its unresolved RPC as an error, and
shuts down. The final state excludes X. Releasing publication drains both
accepted results without changing the retained terminal response. Evidence
records terms, committed positions, worker sessions, effect and attempt IDs,
and whether the old RPC settled after healing.

Run the fixture with:

```sh
cargo test --locked --profile smol -p nativelink-worker \
  --test authority_replicated_rpc_test -- --show-output
bazel test //nativelink-worker:authority_replicated_rpc_test
```

The dedicated Bazel target uses the moderate test timeout. Individual observation
waits remain 20 seconds. A host without PID namespace support reports an explicit
skip and does not qualify physical execution. Receipt-retirement activation is
not part of this history; the separate activated outbox fixture covers that
feature. This experiment does not establish deployed durability across hosts,
authenticated process identity, automatic fleet routing, arbitrary-load latency,
mutable action-cache ordering, or canonical production cutover.
