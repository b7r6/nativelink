# Descriptor-backed journal construction

This experiment connects the standalone canonical filesystem ownership guard to
the existing authority journal. It does not expose a canonical runtime, change
the reducer or persistence format, authorize bootstrap, or replace the existing
closed-member admission recovery procedure.

## Ownership and file operations

The optional `redb` feature in `tools/canonical-filesystem` adds a fixed
`authority.redb` leaf. This is an experimental layout name. `JournalDirectory`
checks the expected node, epoch and namespace against the owned roots before
opening anything. Its distinct operations have different failure behavior:

- `create` requires a root containing only its ownership record and exclusively
  creates the fixed file. Partial failure leaves artifacts for review.
- `reopen` requires an existing regular file. Missing, empty, symbolic and
  observed multiply linked files refuse. Failure never falls back to creation.

All file opens and the new-file parent sync use the pinned journal directory
descriptor. A renamed root remains the target of those capabilities. This is
not a guarantee that the directory remains under its original ambient path.

The private redb backend owns the ordinary redb `FileBackend` and an
`Arc<Ownership>`. All twelve `StorageBackend` methods are implemented. Reads,
writes, resize, data sync, close and all six locking methods delegate to the
pinned redb 4.4.0 implementation. No `File`, raw descriptor, descriptor conversion
or ambient journal path is returned by these handles.

Redb's public descriptor/backend constructors permit initialization of an empty
file. The wrapper therefore refuses zero length for an existing file from its
`len` method. Redb calls that method after acquiring its opening locks. This
also refuses a file truncated between obtaining the backend and opening redb;
a metadata check only before handing over the backend would miss that case.

Backend ownership persists through database clones and live transactions. It
does not depend on the parent sync callback remaining alive. The wrapper's file
backend is declared before the owner so its resources drop before the final
namespace locks. Advisory ownership and the existing cooperating-owner threat
model remain unchanged; these checks do not fence hostile same-user processes,
prove continuity after inode reuse, or prevent later external hard links.

## Portable journal factory

`Journal::open_from_factory` accepts a `JournalBinding`, the expected node, limits,
a `JournalProfile` and a synchronous factory. The binding includes the entire
authority domain: epoch and namespace. Binding and state-limit checks precede
factory invocation on the blocking pool. The factory itself owns the storage
capability before that work starts.

The factory returns `JournalDatabase::created(database, sync_parent)` or
`JournalDatabase::existing(database)`. Its documentation requires the backend to
retain custody; this portable interface cannot make an arbitrary caller's
factory trustworthy. The concrete filesystem backend satisfies that requirement
without a platform dependency in the scheduler crate.

The created path commits the existing identity and applied-state initialization
with immediate durability, then runs the one-shot parent callback. A sync error
returns an error and leaves the initialized artifact. Readability afterward is
not proof that the failed directory sync was durable, nor permission to bootstrap
the artifact automatically.

The existing path preserves the entire previous validation: identity, applied
state, domain, every metadata section, snapshots, log continuity, committed
suffix and format controls. Dynamic admission registration still follows this
validation. Legacy path constructors use the same helper and retain their
existing behavior and error mapping. `recover_admission` still opens ambient
paths and is outside this bridge's claim.

Cancellation does not stop a blocking operation that is already running. The
factory holds custody before opening, the backend holds it through initialization
and validation, and ordinary journal I/O captures the shared database together
with its serialization guard. A canceled caller cannot release either while
that I/O continues.

## Executable checks

Standalone Cargo with `--features redb` executes the filesystem tests. They cover
all four role locks held by the database, read/write transactions and parent token;
ordinary redb duplicate-open refusal; every delegated lock operation and close;
binding, missing/empty/corrupt and special-file refusal; truncation after backend
open; and creation/reopening through a renamed root. The default feature set
continues to exercise ownership and CAS/sink capabilities without redb.

The same compiled parent-sync test also runs under a recorded `strace` fault:
the ninth `fsync`, identified as the pinned journal parent after the eight
ownership-record and root syncs, receives `EIO`. The test requires that exact
error, verifies the initialized artifact remains and reads its value after
reopening. This is a syscall-error experiment, not power-loss qualification.

Portable scheduler tests check node/epoch/namespace and limit refusal before
factory invocation, refusal to adopt a pristine database as existing authority,
and cancellation while a private blocking I/O closure holds the database and
serialization lock. The last test observes database/queue lifetimes; the
standalone tests separately establish the backend's actual role-lock lifetimes.

The manual Linux Bazel target
`//tools/canonical-filesystem:journal_bridge_test` composes the actual filesystem
backend and scheduler. Standalone Cargo does not execute it. It covers persisted
vote/state round trips, every role lock after parent sync and journal cloning,
missing/empty refusal, constructor cancellation before file open and during
parent sync, callback-error artifact preservation, foreign persisted node/epoch/
namespace refusal, and root replacement. The callback-error case injects an I/O
error at the callback boundary; the standalone syscall experiment covers actual
parent `fsync` failure.

The author runs passed 35 feature-enabled filesystem tests, 25 default-feature
tests, three portable scheduler tests and strict Clippy for both packages. Three
selected semantic mutations compiled and were detected by assertions: permitting
existing-empty initialization, removing backend ownership, and omitting parent
sync. The last detection uses the expected syscall-fault witness. The custody
mutation uses two edits for one semantic fault; these selected cases are not a
general mutation score. The exact source was restored and all 35 tests passed
again.

Qualification commands, source hashes and limitations are recorded in the
[companion evidence receipt](evidence/descriptor-journal/manifest.json).
None of these checks qualifies canonical bootstrap,
production clock binding, distributed recovery, worker materialization or the
remaining path-based storage constructors.
