# Fixed authority CAS admission

`AuthorityListeners` constructs CAS through a private, fixed constructor.
It has no operator selector or caller-supplied limits. `CasServer::new` and
production configuration retain their existing behavior. The implementation
lives in `nativelink-service/src/cas_server/authority.rs`.

These are small experimental qualification limits. They do not establish M0
workload sizing, total process memory admission or a deployable canonical server.

## Batch admission and integrity

Both fixed roles accept at most 64 entries and 64 KiB of content in a CAS batch.
The content constant is the same constant that Capabilities advertises. Counts
include duplicate and empty entries. Upload admission checks actual inline bytes
and nonnegative declared sizes independently; download admission checks declared
sizes. Checked arithmetic and all aggregate checks precede every store call.
An excessive count or content total returns outer `InvalidArgument` with no I/O.
Clients can split a batch or use ByteStream for larger individual blobs.
`FindMissingBlobs` also rejects more than 64 keys before calling the store.

Within an admitted batch, malformed digests, unsupported compression, upload
length/hash errors and backend failures are individual statuses. Other entries
can succeed. Identity uploads are hashed before publication; compressed uploads
are rejected before decoding. Attempts run sequentially, with the existing
30-second per-blob timeout.
This avoids one batch immediately competing for all four canonical backend
permits. Other requests may still cause individual `ResourceExhausted` results;
there is no hidden retry loop or transactional batch rollback.

Downloads use a complete-object channel read. The collector checks length before
copying each chunk, verifies exact size and the selected SHA-256 or BLAKE3 hash,
and requires both real EOF and successful producer completion. Excess data
cancels the producer future instead of draining it. A ranged read ending at the
declared size cannot prove EOF and is not used. Failed entries return no data.
Only the first 256 characters of a backend diagnostic enter a batch response.
Response encoded length is checked before protobuf encoding; the 1 MiB wire
ceiling remains in force. Malformed digest envelopes still consume request and
response bytes independently of the 64 KiB content total.

The fixed handlers do not forward whole CAS RPCs through `GrpcStore`. They use
its store interface and apply local verification to returned bytes.
SplitBlob and SpliceBlob remain disabled even when the backing driver is a
`GrpcStore`.

## Bounded tree traversal and pagination

GetTree first completes one bounded breadth-first walk, then returns one page.
The entire walk admits at most 64 Directory occurrences and 64 KiB of declared
Directory bytes. Root, repeated shared-subtree occurrences and missing children
all count. Children are charged before insertion into the queue, so a broad
frontier cannot accumulate beyond the occurrence budget. No output is returned
when the complete walk exceeds either limit, even if `page_size` is one.
An oversized walk returns `ResourceExhausted`; a client can fetch Directory
blobs through ByteStream and traverse them itself. A smaller requested page
does not change the walk's work budget.

The same verified whole-object collector runs before Directory decoding.
An absent root returns `NotFound`; an absent descendant is omitted. Other
backend failures propagate with a bounded diagnostic. Corrupt protobuf, bad
child digests, and length/hash mismatch fail instead of being omitted as absent.
The previous shared helper's 10 MiB prefix decode is never used here: a valid
protobuf prefix is not sufficient evidence of the complete stored Directory.

Negative page sizes are invalid. Zero uses the server limit and honors a
continuation token; positive sizes are capped at 64. The strict, at-most-160-byte
`v1` token contains a binding hash, a traversal fingerprint and the next ordinal.
The binding includes the instance, selected digest algorithm and root hash/size.
The fingerprint records each attempted digest in traversal order and whether
it was present. Occurrence ordinals distinguish equal digests in a shared DAG.
The parser rejects extra components, ordinals with leading zeroes, impossible offsets
and mismatched bindings. No cursor registry is retained.

Each continuation repeats the same bounded whole walk. If its fingerprint has
changed because availability changed, the request fails with
`FailedPrecondition` and the client must restart. It never silently shifts an
offset. A terminal page has an empty token. Tokens grant no permission or
mutation authority and require no authentication secret; fabricated offsets
remain subject to the same work and validation limits. Backend reads finish
inside the request's digest context before the one-message stream is returned.

## Limits of the claim

The raw Directory budget precedes protobuf decoding. It does not mean 64 decoded
nodes or 64 KiB of heap: repeated tiny messages, strings, capacities and allocator
metadata expand the representation. The construction provides neither a shape
parser before decoding nor exact heap accounting. Concurrent RPCs, HTTP/2
buffers and cumulative client work are
also outside this per-request construction.

An arbitrary store can allocate a large chunk or ignore cancellation before
the collector receives anything. No finite backend I/O or memory bound follows
from this interface alone. The canonical backend supplies separate evidence:
64 KiB I/O chunks, whole-object verification and four shared permits with
immediate refusal.
For a bounded walk with at most N attempts and B declared bytes, a conservative
canonical disk-read allowance is B plus N times 64 KiB for corrupt oversized
reads. Backend fsync/read latency remains unbounded; started blocking work keeps
its ownership and permit until it finishes. FindMissing can still perform the
existing 4 GiB canonical scan, plus one 64 KiB corrupt-read allowance.

Action Cache still uses the 10 MiB decode helper and can fail at the later 1 MiB
encoder ceiling. ByteStream's active/idle upload registry, lifetime disk usage
and abandoned temporary files are unchanged. Capabilities still reports
`max_cas_blob_size_bytes = 0`; tying the canonical 64 MiB backend limit and its
advertisement to one source requires a typed canonical storage construction.
The present arbitrary-Store listener cannot certify that property.

## Qualification

Actual HTTP/2 fixtures exercise both digest algorithms and both listener roles.
A counting store establishes zero calls for semantic batch excess below the
wire ceiling. Exact boundaries succeed; invalid entries preserve independent
statuses. Adversarial reads cover wrong hash, short data, excess trailing data,
missing EOF and cancellation of a producer that would otherwise wait forever.
Tree fixtures cover exact count/byte limits, broad frontiers, missing attempts,
repeated DAG pagination, token binding/range, zero and negative page sizes,
and changes in descendant availability.

A separate generic upstream deliberately serves incorrect digest-bearing data
successfully. Fixed listeners backed by a real `GrpcStore` reject that same data
and Directory with `DataLoss`, establishing that whole-RPC forwarding cannot
bypass verification. Canonical filesystem fixtures retain their mutation checks;
large CAS batches now fail at admission, while the distinct large Action Cache
fixture continues to establish the remaining late encoder refusal.
