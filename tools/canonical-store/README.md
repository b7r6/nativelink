# Experimental canonical store bridge

This Linux library connects the descriptor capabilities in
[`canonical-filesystem`](../canonical-filesystem) to NativeLink's real
`StoreDriver` interface. It is a separate package and manual Bazel target.
No production factory, listener, scheduler, worker or collector constructs it.
It qualifies storage acknowledgements through real CAS, ByteStream and AC RPCs
in an in-process HTTP/2 harness. It does not qualify a deployable canonical server.

`CanonicalStores::from_layout(Arc<FixedLayout>)` synchronizes all fixed children
and both data-role parents before returning an `Arc<CanonicalCas>` and an
`Arc<CanonicalSink>`. Callers provide a previously claimed or reopened layout;
this constructor does not claim empty roots, adopt files, restore authority or
prove historical durability. Both adapters retain all four role locks.

## Storage contract

Only digest keys are accepted. The request's SHA-256 or BLAKE3 context is captured
before entering a blocking closure, and its typed leaf includes the algorithm,
hash and key size. Strings are rejected instead of silently hashed into aliases.
CAS uploads require genuine channel EOF, the caller's transfer size constraint,
and exact digest size and hash. Each algorithm's canonical empty CAS object is
virtual; a different zero-size hash is invalid. The mutable sink uses the digest
as a key only: `UploadSizeInfo` constrains the value's transfer length, and the
value need not hash to or have the size of its action key. AC protobuf bytes are
preserved. The adapter does not accept or validate an ActionResult output graph.

Each nonempty upload creates one exclusive temporary file. Chunk writes run in
blocking closures owning the file, directory capabilities, hash state and
admission permit. No blocking thread waits for the next network chunk. EOF and
validation precede the final closure: sync the file, publish by CAS NOREPLACE or
sink replacement, then sync temporary, content and executable directories. Only
successful completion of every step returns an acknowledgement. An I/O error
after publication leaves a visible object and an error response; it never rolls
back or guesses whether an earlier sync reached stable storage.

A duplicate CAS publication preserves the existing slot, verifies its entire
size and hash, synchronizes that existing file, then synchronizes the directories
before success. The duplicate temporary remains on disk. A corrupt existing
object causes an error and is never overwritten. CAS `has` verifies the complete
file and runs file and directory synchronization before returning `Some`, because
ByteStream `QueryWriteStatus` treats that result as a completed upload. Sink `has`
also synchronizes and returns the actual value size. A failed batch installs no
partial `has` results.

Reads verify the entire CAS object before exposing any requested prefix; sink
reads check actual length without hashing the opaque value. A requested range is
buffered until verification finishes and then sent in bounded chunks. Offsets
past EOF and overflowing ranges fail; a length extending past EOF is clamped.
Corrupt file lengths are checked as bytes are read, independently of the claimed
key size. Read handles and permits stay alive through the final send.

Cancellation can stop a task awaiting a chunk or queued blocking work. Once a
blocking closure starts, it retains ownership and its permit until it finishes.
Cancellation during finalization can therefore leave committed storage without
a client acknowledgement. The existing ByteStream service may report incomplete
status during its interrupted-upload resume window; after that window, a query
consults and synchronizes the CAS. Neither cancellation nor handle drop deletes
files.

## Finite experimental limits

| Resource | Limit |
| --- | --- |
| Actual CAS object or sink value | 64 MiB |
| Incoming backend channel chunk | 1 MiB |
| Blocking I/O and outgoing chunk | 64 KiB |
| Active operations shared by the pair | 4 |
| Keys in a `has` batch | 64, with matching result length |

Admission uses a semaphore that never waits. A fifth operation fails immediately;
there is no backend admission wait queue. A batch holds one permit and verifies
keys sequentially, so a successful batch scans at most 4 GiB. A corrupt file
can consume one additional 64 KiB read buffer before length rejection stops the
batch; the failing-batch bound is 4 GiB plus 64 KiB. `update_oneshot` splits
its existing input into 64 KiB chunks and acquires only the update's permit.
These are finite experimental constants, not M0 workload or latency qualification.
Caller-owned payloads, returned data, service decoding, other store instances
and other processes are outside this backend admission accounting.

Abandoned uploads and successful duplicate uploads accumulate temporary entries.
**Lifetime disk bounds, orphan cleanup, eviction and disk quotas are absent**.
`remove` keeps its default refusal and listing is unsupported. The adapter has no
physical generation index, compare-and-unlink, recursive root acquisition or GC.
The slot layout cannot stand in for the authority's physical-object model.

The HTTP/2 harness caps decoded messages at 1 MiB. CAS batch wire compression is
disabled there because the generic service decompresses before invoking the
backend; a backend size check alone cannot bound that earlier allocation.
ByteStream identity and compressed reads are exercised separately. The harness
is not an admission or memory proof for an arbitrary service configuration.

A second fixture serves the fixed experimental `AuthorityListeners` roles over
local TCP with these CAS and sink drivers. It checks chunked transfers larger
than one wire message, refusal before filesystem mutation, fixed CAS batch
admission and late Action Cache encoding refusals. Its journal is a separate test file. The
[listener wire-limit audit](../../nativelink-scheduler/design/categorical/authority-listener-wire-limits.md)
records the remaining aggregate memory and capability advertisement gaps;
this fixture does not qualify a canonical server or aggregate memory admission.

A separate test executable sets the process default to BLAKE3 before creating
its runtime. Fixed listener CAS and AC requests with `Unknown`, and ByteStream
resources without an algorithm, use SHA-256 consistently with Execute. Explicit
BLAKE3 remains supported. Public legacy service constructors retain the process
default. The test exercises fixed and legacy services through loopback HTTP/2.

Fixed ByteStream requests use the local transfer facade even with a `GrpcStore`
backend, preserving the selected algorithm and the 64 KiB read-frame limit.
The remote fixture uses explicit upstream resource names; legacy upstream names
remain outside this qualification. Completed `QueryWriteStatus` uses local
storage presence, which a remote store implements through `FindMissingBlobs`.
The active-upload UUID lookup remains a separate identity-binding residual.

## Filesystem boundary

All backend namespace access uses opaque directory-relative handles. The only capability
extension is `ReadHandle::sync_all`, with no writable file or descriptor escape.
The implementation reuses no legacy `FilesystemStore`, `EncodedFilePath`,
migration, startup pruning or executable-copy path. The inherited
`StoreDriver::update_with_whole_file` can read a caller-provided `FileSlot` as
upload input through the generic slow path; it then uses the same validated
canonical update and publication barrier. It cannot reopen or bypass a canonical
CAS slot. The worker cannot consume this adapter as a materialization capability.

The same cooperative host premise as `canonical-filesystem` applies: every
participant honors advisory locks, and the host excludes hostile writers,
external collectors, hard-link insertion, mount substitution and old owners.
Concurrent sink updates may supersede one another; an acknowledged value may
already have been replaced by another update. Reads retain the opened inode.
The adapter assumes local filesystem rename and sync semantics. Successful
system calls and injected EIO witnesses are not power-loss or hardware durability
qualification. Reopening current inode observations does not prove persistent
physical generation or directory continuity.

## Verification

```bash
cargo test --manifest-path tools/canonical-store/Cargo.toml --locked --offline --profile smol
cargo clippy --manifest-path tools/canonical-store/Cargo.toml --locked --offline --profile smol --all-targets -- -D warnings
bazel test //tools/canonical-store:canonical_store_test
bazel test //tools/canonical-store:fixed_digest_default_test
```

The focused cases exercise both algorithms, algorithm partitioning, mutable
sink replacement with unrelated key size, exact size limits, malformed uploads,
corruption before output, duplicate preservation, admission, cancellation and
lock lifetime. An independently written held-output test saturates all four
permits with CAS and sink reads, cancels one to restore admission, and probes
every role lock separately before and after cancelling the remaining outputs.
Actual HTTP/2 tests hold CAS batch status, ByteStream WriteResponse
and AC UpdateActionResult behind the final directory-sync barrier, then release
it with success or an error. QueryWriteStatus has its own error barrier after an
ambiguous publication. A cancelled ByteStream call demonstrates a lost response
followed by committed storage. A separate single-blocking-worker test supports
real `fsync` EIO injection; its expected response is a failed per-blob RPC status
with the temporary or published evidence retained.
