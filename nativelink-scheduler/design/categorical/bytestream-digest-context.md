# ByteStream digest context at deferred backend calls

This development repair is a prerequisite for the canonical server store
adapter. It does not select that adapter in production or qualify filesystem
durability.

The existing ByteStream upload path carries the resource name's digest function
to its store update. Two other paths lost that selection:

- Completed `QueryWriteStatus` called `Store::has` without a digest context. A
  lookup could use the process default or an unrelated ambient context.
- Both identity and compressed `Read` handlers installed a context while
  constructing the response stream, but the stream polled its backend future
  after the handler's context ended.

The repair scopes the completed-status lookup to the declared digest function,
or the configured default when omitted. Each deferred read future captures the
handler context and restores it on every poll. Direct `GrpcStore` forwarding
and active-upload UUID progress handling retain their existing paths.

## Executed counterexamples

The new test backend separates SHA-256 and BLAKE3 namespaces even for identical
hash/size pairs. It uses the existing memory stores and makes no content-integrity
or physical-persistence claim. Actual tonic HTTP/2 requests exercise the service
against that backend.

Before the repair, the HTTP/2 case found a SHA-256 blob when queried under
BLAKE3 with the same hash and size. A separate direct-handler case failed when
the ambient algorithm differed from the requested one. After repairing only
the status lookup, a completed BLAKE3 upload had a successful status response
but its subsequent streaming read returned `NotFound`. The logs and original
sources preserve these failures.

## Qualification boundary

The final tests cover explicit SHA-256, explicit BLAKE3 and an omitted algorithm;
identity and Zstandard uploads; both read encodings; completed and absent status;
and refusal to find another algorithm's object through the same hash/size pair.
The store yields before choosing its namespace, so the request context must
survive a pending backend future. The direct-handler test also verifies that
each resource name overrides a conflicting ambient context.

These are finite service-composition checks. The fixture uses the streaming
upload path, not the optional direct-update optimization. Existing tests retain
coverage of active uploads, reconnects, validation and large transfers. The
experiment does not qualify another backend's own task spawning, physical
storage acknowledgment, namespace lifecycle or production cutover.

Exact source hashes, commands, failures, final test results and independent
review are recorded in `evidence/bytestream-digest-context/manifest.json`.
