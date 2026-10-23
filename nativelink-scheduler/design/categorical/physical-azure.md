# Exact-generation Azure SDK experiment

This is a local Azurite qualification of physical identity and conditional
effects. The adapter lives under `nativelink-store/tests/physical_azure/` and is
compiled only into its explicit test target. It changes neither `AzureBlobStore`
nor any constructor, store factory, runtime selector, or production topology.

## Provider operations and identity

The fixture uses the repository's `azure_storage_blob` 1.0 client APIs and its
existing Azurite 3.37 runner. Every endpoint must be an explicit HTTP loopback IP
and port with the development account and one container path. DNS names, remote
addresses, redirects, proxy discovery, and environment credential discovery are
excluded. Authentication comes from the runner's documented public development
account and short-lived container SAS; request traces omit the SAS query.

The shared `PhysicalKey` carries the full nonzero 128-bit incarnation and a
nonzero generation. Object names are
`physical-v1/incarnation-<32 hex digits>/objects-v1/<16 hex generation digits>`.
Neither a logical digest nor an `ETag` substitutes for this physical identity.
The blob contains the same 73-byte descriptor and payload as the filesystem
experiment. The test compares actual file and blob envelopes.

Bootstrap conditionally creates a separate fixed manifest containing its
format, full incarnation, and object-byte limit. An occupied incarnation
conflicts even when its objects have been deleted. Open checks the supplied
incarnation's exact manifest; absence fails closed. The manifest does not grant
exclusive distributed ownership. Loss of all namespace or authority evidence
still requires a fresh caller-allocated incarnation.

`BlockBlobClient::upload` receives `if_not_exists()`, explicit concurrency one,
and a partition threshold covering the complete bounded envelope. The pinned
SDK takes its single-request path for that known-size body; the request trace
rejects staged blocks and block-list commits. Azure documents that
`If-None-Match: *` permits creation only when the name is absent.
An existing object is accepted only after verifying its entire descriptor,
length, and digest. [Conditional operation semantics](https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations),
[Put Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob).

Reads obtain the exact length and `ETag` with `get_properties`, then issue a
bounded `download` with `If-Match`. They validate the complete physical envelope
and whole-object SHA256 or BLAKE3 digest before exposing any requested range.
Even an empty range performs verification. This does not treat a provider range
checksum as a whole-object digest. [Get Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob).

Delete preparation captures the `ETag` at the exact physical locator. The delete
uses that `ETag` as `If-Match`. Azurite returns 412 when this condition finds an
absent blob, so the adapter reconciles that outcome with an exact HEAD: only
`BlobNotFound` becomes idempotent absence, and a present object still conflicts.
A delete prepared against absence only rechecks the exact locator with HEAD;
it cannot delete a late recreation. Every actual adapter DELETE requires an
observed `ETag`. The adapter has no unconditional fallback. Replaying a deletion after
an old write recreates the same retired
generation cannot delete that new provider revision through the stale token.
The caller can prepare a new delete after reconciling the late write. A genuine
`BlobNotFound` outcome is idempotent absence; missing containers, denied access,
unknown 404 responses, and other failures are not missing objects.
[Delete Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/delete-blob).

The local experiment exposed a provider gap: Azurite 3.37 ignores the wildcard
in `DELETE If-None-Match: *` against an existing blob and deletes it. Its pinned
`WriteConditionalHeadersValidator` explicitly skips this wildcard and leaves a
TODO about individual write operations. This adapter therefore never relies on
that condition for DELETE. The fixture separately checks genuine `DELETE 404`
classification with the raw SDK against a disposable, never-written probe.
The adapter's idempotent replay path in Azurite is `DELETE 412`, then `HEAD 404`.
This finding does not establish the behavior of an Azure cloud account.

## Bounded qualification

Objects are limited to 16 MiB. Each adapter and its clones share four operation
permits, acquired without an unbounded wait queue. SDK retries are disabled,
transfers use concurrency one, and the transport has a ten-second request
timeout. The adapter bounds application buffers and entity tags. These bounds do not
claim measured RSS, provider-side request quiescence, or a global namespace
object quota across independently opened clients.

The explicit fixture starts its own loopback emulator using the pinned lockfile.
It exercises both digests, full and ranged reads, exact envelope parity,
immutable retry, conflicting descriptors, bad input, absent objects, missing
namespaces/containers, denied authorization, and corruption outside requested
ranges. It verifies exact generation and full-incarnation isolation, actual
repeated deletion, genuine Delete 404 classification, stale `ETag` refusal, and
late old publication followed by a fresh delete.

A transport fault forwards a real PUT to Azurite, observes success, and then
discards that response before the SDK sees it. The first call returns an
unavailable outcome. Reopening the adapter and retrying the same write identity
observes the occupied locator and verifies its existing bytes before returning
a receipt. No automatic SDK retry hides this ambiguity.

Run from the repository root after installing the local runner dependencies:

```bash
cd nativelink-store/tests/azurite_runner
bun install --frozen-lockfile
bun run test
cd ../../..
CARGO_BUILD_JOBS=8 cargo test -p nativelink-store --test physical_azure_test \
  local_azurite_exact_generation_contract -- --ignored --nocapture
```

The emulator qualification is explicitly ignored in general Cargo and Bazel
suites. Selecting the named test with `--ignored` makes a missing emulator a
hard prerequisite failure. A separate ordinary test rejects remote endpoints
without making requests. A default suite pass is not emulator qualification.

After the same dependency installation, an explicit local Bazel run is:

```bash
bazel test //nativelink-store:integration_tests/physical_azure_test_test --jobs=8 \
  --test_strategy=standalone \
  --test_env=NATIVELINK_AZURITE_TEST_RUNNER_DIR="$PWD/nativelink-store/tests/azurite_runner" \
  --test_env=PATH \
  --test_arg=--ignored --test_arg=--exact \
  --test_arg=local_azurite_exact_generation_contract
```

The runner resolves its installed executable under the supplied runner directory.
This test-only override avoids the relative crate directory set by `rules_rust`.
The explicit standalone strategy permits that dependency and loopback listener;
the ordinary target does not assume either prerequisite.

## Typed retention over actual provider objects

The separate `local_azurite_typed_retention_contract` fixture supplies this
adapter to the existing [typed retention bridge](typed-retention-bridge.md).
The bridge's catalogue, checkpoint, candidate, independent oracle, manifests,
and grant checks remain one shared implementation. The typed closure preparer
uses the same traversal and limits with an explicit verified reader. A plain
synchronous test owns a two-thread Tokio runtime for SDK calls; no runtime is
entered from an existing async task.

The fixture uploads an ActionResult, Tree, and file/stream blob, verifies all
three through actual provider GET requests, and acquires publication and serving roots.
It removes the head, reopens the authority and provider adapter, and reads all
three objects through the original serving grant. Retirement while protected
changes neither checkpoint nor state and issues no DELETE. A wrong descriptor
and an expired grant are rejected before HTTP. Captured marks remain conservative
after expiry, so the existing cohort protocol requires another close/mark cycle
with a fresh fence. Committed retirement then permits conditional deletion of
exactly those generations.

A transport fault loses the response after a real conditional DELETE succeeds.
The authority preserves its pending delete intent and exact checkpoint, including
across reopening. Retrying reconciles provider absence before committing
`DeleteDone`. The fixture reopens once more and checks both the committed deletion
receipts and actual provider absence. It creates only an authority directory;
no filesystem object fallback can supply the bytes. The SDK trace checks every
actual DELETE for `If-Match` and rejects multipart writes.

Run either command above with the exact test name replaced by
`local_azurite_typed_retention_contract`. It is explicitly ignored in general
suites and fails when selected without the emulator prerequisite. This is a
singleton local composition qualification, not a production publication policy,
replicated authority, cloud durability claim, or power-loss test.

`local_azurite_blake3_retention_contract` runs the same upload, preparation,
serving, restart, retirement, and conditional-delete sequence with BLAKE3
descriptors throughout. Its catalogue also contains a SHA256 copy of the file
payload. A BLAKE3 ActionResult referring to that SHA256 digest fails preparation
with a missing binding and changes neither checkpoint nor protection state.
The valid BLAKE3 closure still contains exactly its three provider objects.
After restart, a grant read changing only the algorithm in a descriptor is rejected
before HTTP; the SHA256 catalogue object is also outside the granted manifest.
This additional explicit emulator test uses the same runner commands and scope
restrictions as the SHA256 case.

## Missing capabilities

This fixture does not qualify an Azure account, cloud persistence, service
versioning, soft-delete or legal-hold policy, replication, credentials, network
placement, provider latency, or billing. Azurite behavior is not evidence of a
cloud durability contract. Client reconstruction preserves an immutable name;
it does not establish provider crash or power-loss persistence.

The first production adapter still needs an approved account and namespace
contract, authenticated transport, capacity and cost admission, bounded provider
reconciliation, and the committed retention authority that owns generations,
protection, retirement, and unresolved write/delete effects. Distributed owner
fencing and global object/staging quotas require their own protocol. No absence
response or timeout here proves that an earlier remote request can no longer
take effect.

The existing `AzureBlobStore` uses logical digest paths and supports multipart
uploads and their cleanup. Its overwrite and cleanup paths cannot be silently
reused as this immutable-generation authority. The tested SDK operations are
available for a future explicit adapter; this fixture does not retrofit those
semantics onto the canonical store.
