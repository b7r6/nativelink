# Experimental filesystem namespace ownership

This Linux library qualifies two small parts of canonical construction: cold
namespace identity and descriptor capabilities with ownership that lasts through
data access among cooperating local callers.
**No runtime calls it, and it never constructs a store, opens a journal, serves
traffic, starts collection, or authorizes a deployment.** The canonical parser
remains a separate pure package; the offline converter does not link this library.

The input is the experimental manifest from
[`../canonical-config`](../canonical-config). It declares four disjoint roles:
authority journal, CAS, temporary mutable AC sink, and worker workspace. Their
directories must already exist. Provisioning the directories, choosing their
identities and filesystem, and fencing old owners are explicit external tasks.
No path or migration fact is inferred.

## Cold claim and reopen

`Ownership::claim_cold` first validates the schema without I/O, then opens every
path component using directory-relative `openat` with `O_NOFOLLOW` and
`O_DIRECTORY`. A symlink in the final path or any ancestor is refused. No missing
directory is created. The check rejects ordinary lexical overlap and compares
device/inode identities along the opened paths to catch physical equality and
visible ancestry. All four root directories receive exclusive advisory `flock`
locks without waiting; the returned guard holds the directory descriptors and
locks until dropped. Lock failure never waits or changes configuration.

Every root must be completely empty before the first write, including the worker
and journal roots. Existing data, hidden files, or an ownership record reject a
cold claim. After checking every root and preparing bounded records in memory,
the library creates `.nativelink-namespace-v1.json` relative to each pinned root
with `O_EXCL` and `O_NOFOLLOW`. It writes the record, syncs the file, and syncs
the parent directory before moving to the next role. Records are outside the
fixed data layout. File mode is `0600`, further restricted by the process `umask`.

Each versioned record identifies its role and binds the complete group of four
device/inode identities to the authority node, epoch, authority namespace,
principal, REAPI instance, physical CAS and sink namespace IDs, worker ID, and
execution configuration identity. Configuration is copied into owned values;
later caller mutations cannot alter the guard. The format is experimental and
not a stable migration format.

`Ownership::reopen` takes the same locks and requires all four exact matching
records. Reads use `O_NOFOLLOW` and `O_NONBLOCK`, require regular files and are limited to
64 KiB. Unknown, duplicate, missing or malformed record fields fail. Changed
domain, namespace, role, execution configuration or observed device/inode
bindings fail without rewriting records. Reopen does not scan, validate, prune,
or alter data.
Copying records onto replacement directories fails when their observed
device/inode pairs differ. Those pairs can be reused after deletion: a stopped
CAS directory was deleted and recreated with the same pair on its first
allocation, and copying its saved record made reopen accept. Current
observations therefore do not prove a persistent physical generation or a
directory's historical continuity. Directory replacement, metadata restoration,
cloning and relocation need separate reviewed evidence. No stronger physical
generation basis is qualified here; this remains a runtime integration blocker.

No rollback deletes records after an I/O error. A crash leaving only a subset of
the four records, or a truncated record, causes both cold claim and reopen to
fail. Neither operation repairs or cleans up those partial states. Review must
establish what happened. Four matching readable records establish current
binding observations, not successful completion of every historical sync.
Injected errors on the last record sync and the last directory sync each made
cold claim fail while leaving all four exact records readable; reopen accepted
both states. Reopen performs no sync and does not establish durable recovery.
No authority or worker necessarily started. The runtime durability and recovery
gates remain unresolved.

Worker incarnation deliberately does not belong to persistent namespace
identity. Reopening a worker directory grants no registration, process identity,
incarnation reuse, or recovery permission. Those decisions require the separate
durable authority protocol. Likewise matching an epoch record neither restores
its journal nor proves an older authority was fenced.

## Placement and construction limitations

The existing `FilesystemStore` constructor starts by creating content/temp
subdirectories; it can migrate existing content, prune temp files, and replace
the executable-copy directory. It also owns `content.exec` and, on macOS,
`content.flush_sentinel` siblings. A future fixed layout must place `content`,
`temp`, `content.exec`, and any sentinel inside the corresponding owned role
root, with the ownership record outside those scanned children. The separately
owned journal and its snapshots must remain outside both store roots and any
collector's lifetime ownership. The experimental `FixedLayout` described below
creates the three Linux child directories; it does not qualify macOS behavior.

Directory descriptors make this library's record operations independent of
subsequent path traversal. They do **not** make the existing path-based
`FilesystemStore` constructor safe. `verify_attached` detects replacement of a
declared path at the instant checked, but a later path open can still race a
rename or symlink substitution. A guard can continue holding the original inode
after its path is moved. Safe construction needs an adapter that retains and
uses directory capabilities, plus a declared host namespace/permission boundary;
passing strings after preflight is still an unresolved integration blocker. The
descriptor API below supplies a prerequisite for a future adapter. It does not
change or qualify the legacy constructor.

## Descriptor capabilities

`FixedLayout::create(&Arc<Ownership>)` checks both data roots for entries other
than their ownership records before creating `content`, `temp`, and
`content.exec` in each role. Journal and worker roots remain unchanged. The
record schema and cold-root checks remain unchanged. A nonempty data root
refuses creation. Partial creation remains on disk; no rollback deletes evidence.
`FixedLayout::reopen` requires all six children to exist and opens each with
`O_DIRECTORY | O_NOFOLLOW`. Symbolic, missing, and non-directory children refuse.
Neither operation scans, prunes, migrates or certifies existing child contents.

Each capability retains pinned descriptors and an `Arc<Ownership>`, holding all
four role locks. Cloned directory capabilities and returned file handles retain
that ownership after the original layout and caller drop. The handles may move
into blocking closures without shortening the ownership lifetime. No public
handle provides a `File`, raw descriptor, descriptor conversion or ambient path.
The API provides no `/proc/self/fd` path escape.

`DigestLeaf` is constructed from SHA-256 or BLAKE3, 32 digest bytes and a `u64`
size. Its strict parser accepts only that canonical single-component encoding.
It cannot name a child directory or ownership record. `TemporaryId` encodes one
numeric staging name; exclusive creation rejects any existing entry. The API
does not verify content hashes or lengths. These are digest slots in the first
fixed layout, **not non-reused physical generations or collector authorization**.

`CasDirectory::publish` consumes a unique writable staging handle and uses
descriptor-relative `renameat2(RENAME_NOREPLACE)` into the same role's content
directory. An existing destination is conflict without overwrite. The mutable
`SinkDirectory::replace` consumes the same kind of handle and atomically replaces
a regular destination. The returned handle supports read and seek only; staging
handles cannot be cloned or converted into writable aliases. A capability from
another layout instance or role cannot publish the handle.

Read and remove accept only `DigestLeaf`. A read first checks the entry without
following symlinks, opens with `O_NOFOLLOW | O_NONBLOCK`, verifies regular-file
type and the observed device/inode, then retains the opened file. Type checks
also refuse directories, named pipes, sockets and observed multiple hard links. A
rename of the ancestor, role root or child directory does not redirect these
operations. They keep using the original directory even if it has moved outside
the configured ambient path; this is descriptor identity, not containment in a
fixed visible path.

These checks assume cooperating owners. Stat followed by open, rename or unlink
is not an atomic defense against a hostile writer of the same UID. Sink rename
does not follow a substituted destination symlink, but its earlier type check
does not guarantee atomic refusal of every racing destination of another type.
Observed link counts do not prevent later hard-link creation or retained external
writable descriptors. Mount changes, administrator interference, old workers and
external collectors remain outside this qualification.
Remove addresses the current named slot; callers must serialize mutations of
that slot. It is not an atomic compare-and-unlink operation on an earlier file
identity and cannot implement deletion of a persistent physical generation.

Publication guarantees an atomic name operation on the host filesystem, not a
durable authority commit. File sync and directory sync are explicit separate
operations; a caller needs an ordering and recovery protocol. Directory sync
retains ownership through the temporary, content and executable directory calls.
`FixedLayout::sync_layout` syncs all six children followed by the two pinned
data-role parents, making parent-entry synchronization available after creation.
No failure or handle drop removes temporary files, so ambiguous or replaced
artifacts remain for review. The API provides no automatic retry adoption, orphan cleanup,
power-loss qualification or new persistent generation basis. The earlier
inode-reuse and final-sync counterexamples still apply.

## Future store bridge

A future capability implementation of `StoreDriver` can retain `CasDirectory`
or `SinkDirectory` in its shared context, use typed leaves in `FileEntry`, move a
`PendingWrite` into a blocking write task, and retain `ReadHandle` through the
last streamed read. Finalization consumes the writer through publish or replace;
the resulting entry retains the directory capability for later reads and
eviction. File and directory sync errors must remain observable to the caller.

The existing `FilesystemStore`, `EncodedFilePath`, `get_file_path_locked` and
path-based `FileSlot` reopen protocol cannot consume these handles safely.
The next bridge needs a dedicated capability `FileEntry` and descriptor-based
enumeration, bounded I/O admission, eviction and executable-copy operations.
It must carry the guard into every delayed operation and avoid returning to raw
paths. This package supplies none of that runtime wiring; exposing the legacy
constructor after `verify_attached` remains unsupported.

Locks are advisory. Every participating owner must honor them for the entire
data-access lifetime. The legacy constructor does not, and a second process
that ignores locks can modify data. This protocol does not establish ownership
against other local users, administrators, external collectors or old workers.
Local filesystem locking and sync semantics are assumptions; network filesystems
are unqualified. Permissions and old-domain fencing remain deployment evidence.

Device/inode ancestry is not a comprehensive bind-mount alias proof. A mount of
a subtree can hide source-side ancestors behind the mount's parent. Mount
namespace changes can also invalidate path observations. No bind-mount topology
test or hostile namespace-mutation guarantee is claimed here. Physical placement
qualification must rule out these cases or introduce stronger mount-aware
checks before runtime exposure. Existing data can contain aliases too; reopen
does not certify its descendants for store construction.

## Verification

```bash
cargo test --manifest-path tools/canonical-filesystem/Cargo.toml --locked --offline --profile smol
cargo clippy --manifest-path tools/canonical-filesystem/Cargo.toml --locked --offline --profile smol --all-targets --all-features -- -D warnings
bazel test //tools/canonical-filesystem:ownership_test
```

Tests exercise cold claim/reopen, lock denial in the same process and another
process, release on guard drop, refusal before writing when the last root is
dirty or absent, symlink roots/ancestors/records, ordinary aliases/overlap,
wrong domain/namespace/role, copied records on replaced directories, partial
initialization, torn records, and caller mutation. The partial-initialization
tests construct persisted crash shapes; they are not power-loss experiments.
The capability cases additionally cover ancestor/root/child rename and symlink
substitution, immutable CAS and mutable sink publication, unsafe leaves and
special-file refusal, preserved conflicting/replaced staging artifacts, and all
four locks retained by directory/file handles and a delayed blocking closure.
An independent [boundary review receipt](../../nativelink-scheduler/design/categorical/evidence/canonical-filesystem-review/README.md)
records the inode-reuse and final-sync witnesses above, plus actual injected
failures at all eight sync sites and four record writes and a FIFO refusal
without blocking. These 14 cases preserve the frozen source, harness, results
and syscall traces; injected syscall errors are not power-loss experiments. No test here
establishes process admission, journal durability, store data durability,
old-domain fencing, TLS exclusion, or complete M4a cutover.

The standalone Cargo lock uses existing repository versions and checksums.
The library adds no dependency to the server or pure parser. Bazel targets are
manual and Linux-only. Root workspace Cargo commands do not discover this
standalone package.
