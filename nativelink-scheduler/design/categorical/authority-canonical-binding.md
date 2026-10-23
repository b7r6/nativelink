# Private canonical journal binding

This storage prerequisite adds no canonical serving constructor. Every public
journal and singleton constructor still requires an unbound experimental
journal. Only private tests request the new contract. A binding is compatible
storage metadata; it does not prove that a clock, filesystem namespace, worker
or running process followed that contract.

The fixed `singleton_seconds_v1` profile carries one opaque 32-byte namespace
commitment. Zero is reserved for an unbound commitment and refuses before a
factory runs. No operator setting selects time units, offsets or proposal rules.
The future owned constructor must mint this commitment from the four retained
namespace capabilities. The current tests use synthetic bytes.

Two required metadata records accompany a bound journal:

- `canonical_contract` contains the immutable profile and namespace commitment.
- `canonical_bootstrap` records local `preparing` or `sealed` progress.

The applied-state envelope also carries the immutable contract. Its absent
optional field serializes exactly as the existing generic envelope did. Saved
snapshots inherit that applied-state field; the mutable bootstrap phase never
travels in a snapshot. Bound, foreign and legacy snapshots cannot be exchanged
by a live install or recovered snapshot read, even within the same logical
authority domain.

Only a factory that positively reports exclusive creation may initialize a new
journal. Its Immediate transaction writes the identity, initial applied state,
contract and `preparing` phase together, before the existing parent sync callback.
A parent sync error can leave readable complete metadata, but its phase remains
`preparing`. Ordinary existing opens require `sealed` when a contract is expected;
missing, partial or mismatched binding facts refuse before member admission.
Existing empty databases are never initialized by this path. Every generic
constructor and admission recovery expects no contract and rejects bound or
partially bound metadata. Admission recovery also checks the contract in any
retained snapshot; that check does not certify full snapshot or membership
recovery.

No production code writes `sealed`. Tests synthesize it only to exercise exact
reopen validation. The complete bootstrap must still order namespace and layout
synchronization, journal parent synchronization, singleton recovery and clock
validation before sealing. Readable seal bytes after an error cannot prove a
past acknowledgement; a future explicit reopen must validate and synchronize its
current capabilities. Cancellation must retain all custody until blocking work
and authority shutdown finish. These storage tests do not qualify that protocol.

Live reads and writes recheck the small required metadata. The private cold
handle permits both recognized phases; this patch does not prevent a live phase
downgrade or authorize a seal transition. Applied-state reads,
application, feature replay and snapshot build/read/install compare the immutable
contract with the journal's private expectation. Existing generic serialization
and runtime clock behavior remain unchanged. The private expectation never
escapes through a new public raw journal, Raft or arbitrary-event constructor.

The separate [clock proposal](authority-clock-proposal.md) still requires a fixed
source and proposal facade. The maximum finite Action projection is
`315_576_000_001`, so strict execution expiry needs one more representable tick.
Checking that headroom only during startup is insufficient. A source cutoff can
protect additions while stopping the coordinator before a late admitted deadline;
it does not establish eventual expiry or cleanup. This patch adds no lifetime
cutoff, duration guarantee, clock migration or process-incarnation permission.

The storage fixtures cover legacy byte preservation, encoding refusal, every partial
metadata/applied binding combination, cold parent-callback failure, synthetic
sealed reopen, exact snapshot binding and refusal before durable mutation. They
use controlled storage and metadata changes. They do not exercise a real parent
`fsync` fault, a power loss, a canonical service, or an actual worker process.
