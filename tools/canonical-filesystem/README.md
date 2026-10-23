# Experimental filesystem namespace ownership

This Linux library qualifies one small part of canonical construction: cold
namespace identity and exclusive ownership among cooperating local callers.
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
domain, namespace, role, physical directories or execution configuration fail
without rewriting records. Reopen does not scan, validate, prune, or alter data.
Copying records onto replacement directories fails because device/inode binding
changes. Restore or copy to a new physical placement needs a separate reviewed
relocation procedure; automatic rebinding is absent.

No rollback deletes records after an I/O error. A crash leaving only a subset of
the four records, or a truncated record, causes both cold claim and reopen to
fail. Neither operation automatically adopts, repairs, cleans up, or infers that a cold
initialization finished. Review must establish what happened. A completed group
of records only proves this namespace protocol completed under its filesystem
assumptions; no authority or worker necessarily started.

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
collector's lifetime ownership. This library creates none of that layout and
does not qualify macOS behavior.

Directory descriptors make this library's record operations independent of
subsequent path traversal. They do **not** make the existing path-based
`FilesystemStore` constructor safe. `verify_attached` detects replacement of a
declared path at the instant checked, but a later path open can still race a
rename or symlink substitution. A guard can continue holding the original inode
after its path is moved. Safe construction needs an adapter that retains and
uses directory capabilities, plus a declared host namespace/permission boundary;
passing strings after preflight is still an unresolved integration blocker.

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
No test here establishes process admission, journal durability, store data
durability, old-domain fencing, TLS exclusion, or complete M4a cutover.

The standalone Cargo lock uses existing repository versions and checksums.
The library adds no dependency to the server or pure parser. Bazel targets are
manual and Linux-only. Root workspace Cargo commands do not discover this
standalone package.
