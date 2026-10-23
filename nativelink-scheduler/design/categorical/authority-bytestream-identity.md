# Fixed ByteStream upload identity

The fixed client and worker listeners share one CAS and one ByteStream upload
registry per instance. An upload identity is the exact opaque upload UUID,
resolved digest algorithm, digest hash, and declared size. The fixed role resolves
an omitted algorithm to SHA256. It validates the algorithm and compressor before
consulting progress. Only identity transfer is enabled; `blobs` and
`compressed-blobs/identity` address the same upload. Optional resource metadata
has no identity effect across RPCs.

The Remote Execution API permits the same UUID for different blobs. Those uploads
therefore occupy independent registry entries. Distinct raw UUID strings also remain
distinct, including strings which the legacy hexadecimal parser aliases. Completed
status falls back to the requested algorithm and digest in CAS when the exact
upload has no active or idle entry.

An active exact identity has one owner. A competing write receives `Unavailable`
and can retry after the original stream becomes idle. An idle exact identity
resumes its retained store future. A nonzero offset without that exact entry is
rejected before registration or backend work; it cannot leave an idle entry
shadowing an independently completed CAS object.

Every fixed write uses the tracked streaming guard, including single-message
writes to stores advertising `update_oneshot`. Progress remains visible while the
backend completes, and a competing exact write cannot bypass the guard. The local
facade remains in control for GrpcStore backends. The retained backend future
continues to carry the digest context chosen when the upload was admitted.

Within one Write RPC, each later nonempty resource name must exactly equal the
first resource name. This comparison precedes forwarding the frame's data or EOF.
An empty later resource name is valid. Equivalent spellings such as explicit and
omitted SHA256 can identify the same upload across RPCs, but cannot replace each
other within a Write RPC.

These rules apply to the fixed listener constructor. Public legacy constructors
retain their UUID parsing, collision behavior, `update_oneshot`, and proxy paths.
The read-producer completion change is a separate prerequisite.

The map still has no aggregate entry quota. Exact raw UUID strings also retain more bytes
per entry than the legacy integer key; the existing decoded message limit bounds
a single request, not the total registry. This change makes no total memory or
M0 completion claim and does not add a credentials-based namespace.

## Qualification boundaries

The focused fixture uses actual tonic TCP/HTTP2 on client and worker listeners,
a store spy with digest-aware values, and a real GrpcStore/upstream ByteStream
pair. Backend settlement is observed separately from the server's progress
counter before local counter and disconnect assertions. The pending single-message case
holds backend completion behind a bounded semaphore. Rejected offset admission
is also checked through an independently populated CAS result, so an unpolled
backend future cannot hide an inserted idle entry from the oracle.

Execution counts, exact baseline failures, compiled mutation observations, source
hashes, and restored test binaries belong in the qualification packet. A compiler
failure is not a semantic mutant detection. A failure in setup or a positive
control is distinguished from reaching a later negative assertion.

The [root integration receipt](evidence/authority-bytestream-identity/root-integration.json)
records seven executed Bazel targets and the server build at `a6bcfb158a`:
169 Rust tests pass and one local TLS fixture remains normally ignored. The
source remains unchanged across that run, with all 1,349 tracked inputs verified
against its commit. Identity runtime and fixtures match the author source exactly;
four prerequisite inputs retain earlier root dependency and diagnostic changes.
The packet includes all seven executed binaries and independent source reviews.
