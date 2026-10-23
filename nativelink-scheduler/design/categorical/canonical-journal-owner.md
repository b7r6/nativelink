# Inert canonical journal owner

`PreparedCanonicalOwner` is a Linux-only, storage-only constructor. It consumes a
`SyncedNamespace`, derives node, epoch, nonzero authority namespace and the fixed
namespace commitment from that retained capability, and keeps all four cooperative
root locks until its journal backend is released. The public surface consists of
cold preparation, sealed reopening, copied diagnostics and consuming release.
It exposes no journal, Raft, arbitrary event, factory, clock, seal transition,
snapshot or singleton constructor. It starts no scheduler, listener or worker.

Cold preparation exclusively creates the fixed journal and writes compatible
identity, fixed finite limits and `preparing` metadata in one Immediate
transaction, followed by the creation-parent sync. Those limits are 64 executions,
256 operations, 32 workers, 512 effects and 65,536 payload bytes. The owner does
not use generic default limits or initialize a preexisting database. A current
parent capability is retained on both paths; its availability is never evidence
of exclusive creation.

Both successful paths then verify current namespace records and configured root
attachment, validate the exact expected bootstrap phase and immutable contract in
a new Immediate transaction, commit, and synchronize the pinned journal directory
between further current-record and attachment checks. The owner is returned only
after this current barrier succeeds. The transaction changes no authority record.
An earlier readable commit or seal cannot substitute for its acknowledgement.
Redb may repair during opening; a fixed internal observer records this fact in the
successful diagnostic without exposing callbacks or changing policy. A failed
repair, validation or current sync returns no owner and may leave artifacts.

Ordinary reopening requires an existing sealed journal and validates retained
snapshot and log compatibility. No production code writes the seal. Tests alone
synthesize sealed singleton history to exercise recovery, including actual redb
repair after abrupt process exit. Cold preparation leaves `preparing`, and either
ordinary reopen or another cold attempt refuses that storage. This remains a
prerequisite for the future bootstrap protocol, not an operational deployment.

Blocking opening, synchronization and redb destruction retain namespace custody
when their async waiter is cancelled. Explicit release waits for blocking
destruction; redb destruction can suppress errors and grants no new durability
acknowledgement. Existing experimental ownership aliases remain possible. The
cooperative lock and attachment checks do not defend against a hostile writer in
the same process or prove lifetime physical-directory continuity.

The external API test exercises the exact filesystem crate identity used by the
Linux Bazel dependency and Cargo feature union. Separate compiler controls reject
raw bytes, raw ownership and access to internal owner functionality; compiler
refusals are API evidence, not semantic mutation detections. The synchronization
harness observes real file and parent syncs while the owner is alive, injects
current-stage EIO failures, and requires explicit repair observation. Its syscall
receipts establish the tested software boundary, not power-loss durability or
production readiness.
