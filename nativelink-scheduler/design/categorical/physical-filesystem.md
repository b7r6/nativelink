# Exact-generation filesystem experiment

Status: experimental local adapter and finite retention bridge. This is not M5
certification, a production catalogue, or an activated runtime path. The adapter
has no `StoreDriver`, selector, factory branch, gateway, or Azure connection.

## Ownership and identity

`nativelink-store/src/physical_filesystem.rs` accepts an exact physical key:
nonzero 128-bit store incarnation plus nonzero 64-bit generation. The caller
allocates these identities and never reuses them. It also owns write and delete
IDs, committed protection, retirement, pending effects, and their reconciliation.
The adapter reports physical outcomes; it cannot grant a publication or decide
that a generation is collectible.

Bootstrap creates a new `incarnation-<32 lowercase hex digits>` directory beneath
an already provisioned local root. A fixed 88-byte checksummed manifest stores
the incarnation and limits. Bootstrap uses exclusive creation, and open requires
that exact manifest. Missing, malformed, or unknown-format metadata never starts
an empty replacement catalogue. An existing incarnation directory remains
occupied even when all its objects are gone.

The manifest lives outside `objects-v1` and `staging-v1`. It is not a collector
target. An exclusive OS file lock prevents two local owners. Every staged write
retains that lock, even if its initiating adapter is dropped. Reopening requires
all previous local handles to have ended or the previous process to have exited.

Freshness after total namespace or authority loss cannot be inferred from disk.
Explicit reset must supply a fresh incarnation allocated by the owner. The
experiment does not implement that allocator, nor defend against an external
actor deleting or replacing its locked namespace. Those are deployment and
authority integration requirements, not properties of a filename scan.

## Fixed physical encoding and acknowledgment

Object files use `objects-v1/<16 lowercase hex generation digits>`. Their fixed
73-byte header contains a format marker, complete physical key, algorithm, size,
and digest; the remainder is raw payload. Both SHA256 and BLAKE3 are supported.
No string keys, compression, chunk composition, or arbitrary path components are
accepted. Staging filenames contain only generation and write identity.

A staged writer hashes actual appended bytes and checks exact size and digest.
It makes the file read-only, syncs and closes it, publishes with a hardlink that
cannot replace an existing destination, then syncs the object directory before
returning a receipt. An identical retry verifies the existing complete object;
a conflicting descriptor never replaces it. Corrupt existing payload remains a
corruption outcome. A receipt includes the exact write identity and object.

A delete removes only its exact generation and syncs the directory. Already
absent is an idempotent physical result. Neither that result nor one successful
delete establishes quiescence of unresolved writes. A staged writer may finish
after deletion and leave a retired-generation orphan. The retention owner must
keep its tombstone and unresolved write, observe the late result, and issue a
new delete at the newer write revision.

Dropping a staged handle cleans only its private staging filename. A crash may
leave that filename behind. Explicit stage discard rejects active handles;
after exclusive reopen, the caller can discard an abandoned local stage without
claiming that a remote provider request has ended. No Azure quiescence inference
is derived from this local process-ownership rule.

## Reads and limits

Full and ranged reads check the header, exact length, and digest of the entire
object. They retain only the requested range and expose it after verification.
Even an empty range verifies the complete object. This avoids claiming that a
partial range independently proves a whole-object digest. It deliberately costs
a full-object read; a qualified verified-cache path is future work.

Failures distinguish missing objects, unavailable I/O, corrupt content,
conflicting identities, invalid requests, and exhausted capacity. Losing the
object directory is unavailable, not a successful empty store. The namespace
does not recover by selecting a different backend or interpreting legacy files.

Persisted limits bound object bytes, object count, staged-file count, and
concurrent reads. Defaults are 16 MiB per object, 1024 objects, 16 staged files,
and eight reads. Absolute experimental bounds are 64 MiB per object, 65,536
files in each directory, and 32 reads. Each read retains at most the object-byte
limit plus a 64 KiB buffer. Hashing state and active write identities have fixed size;
paths have an explicit input bound. Scans stop when a count bound is exceeded.
Abandoned stages consume capacity until explicitly cleaned.

The adapter performs synchronous I/O and does not create background tasks or
request queues. An async integration must provide bounded blocking execution.
These count and byte bounds are not a measured RSS, disk-performance, or
supported-workload claim. Objects exceeding the experimental byte cap cannot be
admitted; that is a concrete limitation of this slice.

## Executable evidence and remaining work

`nativelink-store/tests/physical_filesystem_test.rs` covers immutable duplicate
publication, both algorithms, ranges, corruption outside the requested range,
missing and unavailable paths, admission bounds, namespace ownership through
staged handles, failed bootstrap/open, empty-catalogue incarnation replacement,
delayed old deletion, and late publication after successful deletion. Child
process fixtures exit without running Rust drop handlers after a partial upload and after
an acknowledged write; reopen checks unpublished staging and durable bytes.

`nativelink-scheduler/tests/physical_retention_test.rs` drives the real adapter
from the existing coarse retention candidate and independent per-generation
oracle. Every accepted transition is persisted before its physical effect.
The bounded test checkpoint is outside collector ownership and syncs its file
and containing directory. It is not a second production authority format.

The bridge covers committed protection rejecting retirement, pending write and
retirement recovery, successful deletion followed by actual late publication,
lost write outcome, recovery of that orphan, a fresh delete revision, both
orderings of an old delete acknowledgment, and an unaffected replacement under
a new store incarnation. Physical presence never restores authoritative
readiness. The bridge restarts checkpoint drivers and filesystem handles; the
separate provider fixtures exercise real process exit.

The integrated Cargo run passed ten physical tests and three retention bridge
tests. All four affected Bazel targets passed, including the scheduler core and
independent authority harness. Three injected faults were detected: deleting a
foreign incarnation, replacing an immutable generation, and exposing a range
without whole-object digest verification. The mutation baseline passed; no
compiler rejection counted as detection. [Checkpoint receipts](evidence/physical-recovery-checkpoint/manifest.json)
retain the exact commands, source hashes, and raw logs. These mutations do not
qualify file or directory sync under power loss. No power-loss storage stack, shared
filesystem placement, malicious namespace mutation, typed manifest traversal,
production checkpoint compatibility, infinite-churn compaction, gateway cache,
M0 provider budget, or Azure implementation is qualified by these fixtures.
