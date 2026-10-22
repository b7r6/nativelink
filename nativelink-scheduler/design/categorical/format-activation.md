# Experimental format activation boundary

Status: implementation proposal, not an implemented upgrade protocol. The
current journal envelope is experimental format 2, its application batch is
version 1, and its embedded scheduler snapshot is version 4. Their readers
reject unsupported versions and unknown authority sections. They provide no
compatible writer activation, previous-binary rollback, or migration promise.
This proposal does not name or freeze a production format 3, qualify deployment,
or waive the M0 representation and workload gates.

## The base reader must understand activation

A newer reader cannot introduce an activation command that the previously
compatible reader cannot decode. The proposal can reach a follower's journal
before it commits. If the older binary rejects that uncommitted entry, merely
proposing an upgrade has already broken rollback before activation.

Establish an explicitly experimental base encoding first. Both the base reader and
the upgraded reader must understand its bounded feature-control vocabulary:

- An ordinary application batch, carrying the feature epoch that selected its
  encoding.
- A preparation record identifying a target feature set, authority domain,
  membership revision, and the evidence required for activation.
- An activation record identifying the preceding feature epoch, the next epoch,
  the prepared feature set, and the matching preparation evidence.

The exact Rust types and numeric versions remain undecided. Feature identifiers
are bounded data in a known control record, not a new enum variant for each
future feature. The base reader can decode and preserve an unsupported target
feature in a preparation or uncommitted activation record. It must refuse to
apply an activation that requires an unsupported feature. This is different
from ignoring an unknown authority-bearing section: the control's meaning is
already defined, its bytes remain intact, and unsupported committed semantics
stop authority recovery.

The activation command itself always uses the preceding compatible control
encoding. Its parsing requirements must not advertise the target feature as
already required merely because the control names that feature. Otherwise an
older reader would reject the uncommitted proposal again. Consensus, rather
than a restart helper, decides whether an uncommitted proposal is retained or
overwritten. Rollback never silently drops it from a log.

This base reader is a new experimental compatibility baseline. It is not a
claimed conversion from the current strict format-2 prototype. Any conversion
from that prototype would require a separate explicit, lossless migration and
its own evidence.

## Separate executable capability from activated authority

Keep two distinct values:

1. Immutable reader capabilities supplied by the executable. These describe
   the formats and semantics that this process can actually decode, validate,
   execute, and write.
2. Committed active-format state. This includes the activated feature epoch,
   required capabilities, section versions, authority and namespace identity,
   and the membership revision authorizing the activation.

The active-format state belongs in the same atomic applied-state transaction as
the scheduler state, applied position, membership, and outbox. Installing a newer
binary does not change it. A persisted capability advertisement is not proof
that the current process still has that capability after a restart or downgrade.

The required membership is an explicit set. For the first experiment it should
include every voter in either side of a joint configuration and every retained
learner that can receive authority state or become a voter. It must not mean
only the members that answered a probe. A change to this set or its committed
membership revision invalidates prepared activation evidence.

## Membership and transport capability gate

An activation request supplied by a client must not certify its own compatibility
by listing member IDs or capability bits. A coordinator gathers evidence from
admitted member processes, binding each response to:

- The exact authority domain and member identity.
- The current process incarnation and admission generation.
- The prepared target feature set and required membership revision.
- An applied position sufficient to validate the preparation and its state.

Evidence must come through the authenticated or otherwise explicitly trusted
member transport. A quorum-owned preparation record can make the evidence
deterministic for the reducer, but committing arbitrary advertisements does not
make them true. The admission mechanism must verify their origin and prevent
an old process incarnation from continuing to exercise that member's authority.
In the crash-fault model, an authenticated process may report capabilities
compiled into its executable; that does not constitute protection against a
malicious executable or operator.

Preparation also needs a stable admission interval. If a member restarts,
downgrades, or changes incarnation between its report and activation, the
preparation must be invalidated or the replacement must remain fenced until the
activation decision is known. An unfenced report plus a later majority commit
does not prove that every required member was compatible at activation. The
experiment must inject this race.

For the smallest controlled test, a fixture registry can own immutable reader
levels and admitted process incarnations, serialize replacement with activation,
and reject stale evidence. That is a declared trusted test boundary, not a
production certification service. An arbitrary caller-provided capability map
is insufficient even for this fixture.

After activation, both restart and membership admission require compatibility
with the active feature set. A new learner must obtain and validate the active
format before promotion. An old process cannot bypass the gate using a vote,
empty heartbeat, snapshot fragment, or direct membership proposal. OpenRaft's
cluster name is a tracing field; it is not an authority-domain or capability
check on RPCs.

Actual canonical ingress therefore needs a member admission and fencing
protocol, authenticated transport identity, a single membership reconciler, and
an activation coordinator that shares their serialization rules. An encoding test
does not supply those components.

## Writer compatibility before activation

Select output encoders from committed active-format state, never from the newest
format understood by the executable. While the base feature epoch is active,
the upgraded executable must keep writing base-compatible:

- Identity, vote, committed-position, and other metadata records.
- Application commands and their journal envelopes.
- Applied scheduler state, pending effects, membership, and control state.
- Built snapshots, installed snapshots, compacted state, and replacement files.

Reading an old representation into a richer in-memory type does not permit
writing the richer representation back. Keep an explicit base encoder rather
than relying on new fields being absent by convention. Compare canonical bytes
for identical logical records, including checksums and required-section lists.
Where a record has a legitimately different position or snapshot identifier,
compare it with the base encoder using those same inputs. Deterministic defaults
must be specified conversions, not silent omission of unknown state.

No feature-dependent application command is proposed until the activation is
committed and applied by the ingress authority. Incoming replicated entries
need a separate rule: a follower can receive an activation and its later suffix
before its local applied position catches up. Validate supported decoding and
the log's feature-epoch sequence; do not incorrectly compare every received
entry only with the follower's current applied epoch. Application still crosses
the activation exactly at its committed position.

## Commit, apply, and rollback cutpoints

The activation decision is made by the committed log, not by a local executable
flag. Recovery must account for all of these boundaries:

| Cutpoint | Required behavior |
| --- | --- |
| New binary installed, no preparation | Base writer bytes and base-reader rollback remain valid. |
| Preparation committed, activation absent | The base reader preserves preparation and can still serve the base feature set. |
| Activation appended but not committed | The base reader decodes and preserves it; consensus may retain or overwrite it. |
| Activation committed but not applied | Recovery discovers the committed decision before serving authority; an unsupported reader cannot serve the preceding snapshot as current state. |
| Activation apply transaction fails before commit | Previous applied state remains complete; recovery retries the committed activation. |
| Activation apply commits but its reply is lost | Active format, scheduler state, membership, and outbox remain one transaction; retry cannot activate a second epoch. |
| Snapshot replacement or installation is interrupted | Either the old complete representation and suffix remain recoverable, or the new complete representation is installed. |
| Compatible restart after activation | Recover all active sections and pending effects, then pass the real Raft read barrier before authority reads. |
| Unsupported restart after activation | Refuse participation or serving authority; do not downgrade the snapshot, discard sections, or reset the cluster. |

Do not infer that an activation is uncommitted solely because it is missing from
the last applied snapshot. Check the durable committed position when available,
and process newly established committed entries before enabling ingress. An
isolated restarting process may not know the current cluster decision. It must
pass admission and consensus recovery; it cannot use that uncertainty to claim
rollback is safe. An unsupported activation encountered during replay must stop
the member before any read or effect execution is authorized.

After activation, rollback means an executable that supports the active format
or a separately tested lossless reverse migration. Deleting outbox, accepted
publication, protection, retirement, or uncertainty state is not such a
migration. The first experiment should refuse downgrade and provide no reverse
migration command.

## Old snapshot plus suffix

A snapshot records its applied position, membership, feature epoch, required
capabilities, and versioned sections within its checked authority envelope. A
later journal suffix may contain preparation, activation, and new-feature
commands. Recovery must:

1. Bound the input, validate its identity, lengths, checksums, and sections, and
   select a supported snapshot decoder.
2. Convert known old sections into staged state through explicit deterministic
   conversions. Preserve pending effects, resource uncertainty, and terminal
   associations exactly.
3. Replay the contiguous committed suffix in order, checking each command's
   feature epoch against the state at that position. Cross activation once.
4. Validate the completed staged state and atomically install the representation
   and applied-format state. Do not expose partially migrated state or execute
   its outbox during staging.

A target feature named in an uncommitted known control record can be preserved.
An unknown active authority section or command that requires unsupported
semantics cannot be ignored. A snapshot and suffix disagreeing about identity,
membership history, or activation order fail closed. A snapshot at the old epoch
plus a valid activation suffix must produce the same state and effect identities
as uninterrupted execution.

## Smallest next implementation and evidence

After the current checkpoint, start with an independently versioned experimental
encoding module and fixtures, without changing the active journal format. Implement
the base control vocabulary, two reader levels, bounded active-format state,
base and upgraded encoders, and staged conversion. Use one bounded additional
authority section whose value must survive conversion; name it as an experiment,
not as a completed publication or retention implementation.

Freeze the base reader fixture before adding the upgraded writer. A separately
compiled base fixture must restore artifacts produced by the upgraded executable before
activation. Passing two modes through one permissive decoder is not sufficient
rollback evidence. This establishes only the declared experimental baseline,
not compatibility with a previously released NativeLink binary.

The first fixture matrix must cover:

- Exact base writer output after upgrade, ordinary writes, compaction, snapshot
  replacement, and restart before activation.
- An unsupported target named in a known uncommitted activation record, with
  rollback retaining the complete record.
- Refusal to activate with one incompatible required member, a newly admitted
  learner, a changed membership revision, or a restarted process whose old
  capability evidence remains in the log.
- Commit before apply, interrupted conversion, and failure before and after the
  atomic installation transaction.
- Old snapshot plus preparation, activation, and upgraded suffix; compare with
  uninterrupted execution and repeated restore.
- Pending dispatch, uncertain charged resources, and an accepted terminal result
  with a pending publication effect crossing activation without identity loss.
- Unsupported active features, unknown authority sections, feature-epoch gaps,
  mismatched domains, and downgrade refusal after committed activation.

Only after those fixtures pass should a separate journal integration slice add
active-format state to the shared RAM/file transaction path and exercise the
same cutpoints through real OpenRaft commit, apply, and read barriers. That slice
must include the admission fixture and membership races, not just direct encoding
calls. Until the actual transport, admission, and membership prerequisites
exist, the result remains an experimental format protocol and cannot qualify
canonical production ingress or satisfy all of M3a.
