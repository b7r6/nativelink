# CAS writes surface raw `Unknown: transport error` instead of a well-formed retryable status (`Unavailable`)

- **Date:** 2026-07-18
- **Reporter:** b7r6
- **Component:** `nativelink-error` (`From<tonic::Status>`) + `nativelink-store` (`grpc_store.rs` shard-forward), surfaced at the CAS/ByteStream service boundary
- **Repo state:** branch `nix-cache`, `7316b864`
- **Topology:** client `buck2` on `ultraviolence` → public CAS endpoint `watchtower.sju1.s4.gl:50051` (scheduler/`public` server, `CAS_MAIN_STORE` = a shard ring over each node's `grpc://<node>:50052` `cas`)
- **Severity:** Medium — non-fatal (the build completes; cache uploads are best-effort), but a **retryable transport failure is mislabelled `Unknown`**, so compliant clients stop retrying and silently drop cacheable artifacts. The operator/client also gets an opaque error instead of an actionable status.

## Summary

When a CAS `ByteStream.Write` (or `GetActionResult`) fails at the **transport
layer** — the backend shard resets the connection mid-stream, or is momentarily
unreachable — NativeLink propagates tonic's default `Code::Unknown` with the
message `"transport error"` straight through to the client. Per the gRPC error
model, a transport-level failure is **`UNAVAILABLE`** (transient, retryable);
`UNKNOWN` is reserved for genuinely unmapped errors and is **not** in most
clients' retry set. The result: a blip that should be retried is surfaced as a
terminal, opaque failure.

Observed under a legitimate load pattern (a `buck2 build //...` uploading many
action outputs concurrently, including multi-GiB `nix_build` content dirs). The
server is **not** down — it sheds/​resets connections under the burst and then
reports the wrong code.

## Observed (client side)

From `buck2 build //...` (RE cache mode, `[buck2_re_client]` → `watchtower:50051`,
`instance_name = main`):

```
WARN buck2_execute::execute::cache_uploader: Cache upload for
  `45b96599…:141` failed: Error uploading outputs: Remote Execution Error on upload …
  Error: (code: 'Unknown error', message: "status: Unknown, message: \"transport error\",
  details: [], metadata: MetadataMap { headers: {} } : in GrpcStore::write : On attempt 1")

WARN remote_execution::client: Transient error (attempt 1/5), retrying in 117ms:
  code: 'The service is currently unavailable', message: "status: Unavailable,
  message: \"tcp connect error\" … : in GrpcStore::get_action_result : On attempt 1"
```

Two failure shapes from the **same** shard-forwarding store, inconsistently coded:

| operation | underlying failure | code returned | client behaviour |
|---|---|---|---|
| `ByteStream.Write` (mid-stream reset) | connection dropped during the large upload | **`Unknown`** "transport error" | **not retried** → upload dropped, cache miss persists |
| `GetActionResult` (connect) | backend shard unreachable | `Unavailable` "tcp connect error" | retried 5× (correct) |

The `Unavailable` path is handled gracefully by the client; the `Unknown` path is
the bug — same class of failure, wrong (non-retryable) code.

## Expected vs. actual

- **Expected:** a transport-layer failure returns `UNAVAILABLE` with a well-formed
  gRPC status (proper trailers), so the client retries per its backoff policy;
  a capacity/backpressure failure returns `RESOURCE_EXHAUSTED`. The client should
  never see `UNKNOWN` for "the connection broke."
- **Actual:** `ByteStream.Write` transport resets surface as `UNKNOWN: transport
  error`. Compliant clients treat `UNKNOWN` as terminal and give up.

## Root cause

`impl From<tonic::Status> for Error` forwards the code verbatim
(`nativelink-error/src/lib.rs:355`):

```rust
impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        Self::new(status.code(), status.to_string())   // code preserved as-is
    }
}
```

tonic sets `code() == Unknown` for transport/`hyper` errors (a mid-stream
connection reset produces `Status { code: Unknown, message: "transport error" }`).
The shard-forwarding backend then annotates but does **not** normalise it —
`grpc_store.rs:457`:

```rust
let res = ByteStreamClient::new(channel)
    .write(enrich_request(...))
    .await
    .err_tip(|| "in GrpcStore::write");   // adds context, keeps Code::Unknown
```

and on the way back out, `From<Error> for tonic::Status`
(`nativelink-error/src/lib.rs:361`) re-emits `val.code` unchanged, so the public
CAS server hands the client `Unknown`. Nothing on the path maps a transport error
to the semantically-correct `Unavailable`.

(The `GetActionResult` "tcp connect error" *does* arrive as `Unavailable` — tonic
codes a failed connect as `Unavailable`, a failed mid-stream write as `Unknown` —
which is why only the write path misbehaves. NativeLink should not depend on that
distinction.)

## Impact

- **Silent, permanent cache misses for exactly the expensive artifacts.** The
  outputs that trigger mid-stream resets are the large ones (multi-GiB `nix_build`
  content dirs); their uploads fail with `Unknown`, the client doesn't retry, and
  those blobs never enter the CAS — so every consumer rebuilds/refetches them.
- **Defeats client retry.** A retryable transient is dressed as non-retryable.
- **Opaque to operators.** `Unknown: transport error` gives no signal about
  *why* (backend shard reset? capacity? timeout?), which is the user-facing
  complaint: a server that can't complete an upload should reject it with a
  well-formed, correctly-coded status, not a bare transport error.

## Reproduction

1. Point a buck2 (or any REAPI client) at the public CAS with cache uploads on,
   and drive a build that uploads many/large outputs concurrently (a
   `buck2 build //...` whose graph includes several multi-GiB action outputs does
   it reliably).
2. Watch the client log for `status: Unknown, message: "transport error" … in
   GrpcStore::write`. Cross-check the shard backend's journal for the connection
   reset at the same timestamp. *(Server-side journal on `watchtower` not captured
   in this report — the client-side code and the source path are conclusive; the
   journal would confirm which shard node reset.)*

## Proposed fixes

1. **Normalise transport errors to `Unavailable`.** Where a `tonic::Status`
   originates from a transport/`hyper` failure (inspect `status.source()` for a
   `tonic::transport::Error` / connection error, or match the `Unknown` +
   transport-error shape), map it to `Code::Unavailable` rather than preserving
   `Unknown`. Best done once at the `From<tonic::Status> for Error` boundary
   (`nativelink-error/src/lib.rs:355`) so every forwarding store benefits, or, if
   a blanket remap is too broad, at the `GrpcStore` write/read boundary
   (`grpc_store.rs`). A transport failure must never reach a client as `Unknown`.
2. **Distinguish capacity from availability.** If the reset is backpressure/limit
   (queue full, max concurrent streams, memory), return `RESOURCE_EXHAUSTED` so
   clients back off rather than hammer. Requires surfacing the reason from the
   shard backend rather than collapsing everything to a transport error.
3. **Guarantee a well-formed status on the wire.** Ensure the CAS/ByteStream
   service always closes the RPC with gRPC trailers carrying a real status code,
   never by dropping the HTTP/2 stream (which is what yields the client's bare
   "transport error").
4. **Test the transport-failure mapping.** Add a `nativelink-store` /
   `nativelink-error` test that injects a mid-stream connection reset into a
   `GrpcStore::write` and asserts the surfaced code is `Unavailable` (retryable),
   not `Unknown` — this is the untested networking-layer edge the bug lives in.

## Immediate workaround (client side)

None that fixes the coding; the failed large-output uploads simply don't land, so
those artifacts stay served by their origin (here, the nix binary cache) instead
of the CAS. Re-running the build lets the smaller/​less-contended uploads land over
time. A per-client `max_cache_upload_mebibytes` cap avoids *attempting* the giant
blobs (no error spam), but that trades away caching them at all — orthogonal to
this bug, which is about returning the right status when an attempt fails.

## Resolution

Split into the floor (proposed fix 1, done) and prevention (the "make a toolchain
upload land" goal, scoped).

**Floor — correct the code (done).** `From<tonic::Status> for Error`
(`nativelink-error/src/lib.rs`) now remaps a transport-originated `Unknown`
("transport error") to `Unavailable`, detected by walking the status source chain
for a `tonic::transport::Error` with a message fallback. So the reset is retryable
by both our own `Retrier` and the REAPI client — buck2 re-uploads instead of
dropping the blob. Regression test in `tests/error_tests.rs`
(`transport_unknown_status_maps_to_unavailable`).

**Prevention — land the upload in one shot (scoped, not yet applied).** The
mid-stream reset is HTTP/2 flow-control window exhaustion: the default per-stream
window (~64 KiB) drains almost immediately, and the in-process `buf_channel` is a
2-slot queue (`nativelink-util/src/buf_channel.rs`), so a slow downstream shard
stalls the inbound stream → the window stays empty → keepalive/read timeout →
reset. Levers, by value/risk:

- **Config (highest value, lowest risk):** enable `experimental_http2_adaptive_window`
  and raise `experimental_http2_initial_stream_window_size` /
  `…_initial_connection_window_size` on the RE servers (public **and** shard) —
  the knobs already exist (`src/bin/nativelink.rs:667-707`, config `advanced_http`).
  A fleet RE-server config change (Dhall), not a code change.
- **Code (needs validation):** raise the `buf_channel` capacity so a slow shard
  doesn't instantly stall the stream, and revisit the 2-message streaming-resume
  window (`nativelink-util/src/proto_stream_utils.rs`). Note the internal `Retrier`
  cannot rewind a consumed multi-GiB stream, so it never retries these in place —
  client re-upload (now enabled by the floor fix) is the recovery path.
- **Timeouts:** ensure the forwarding `GrpcStore` uses `rpc_timeout_s = 0` (or a
  large value) for uploads, with a generous `persist_stream_on_disconnect_timeout_s`.

Prevention is tracked in `PRODUCTION-READINESS.md`; proposed fixes 2 (RESOURCE_EXHAUSTED
for capacity) and 3 (always close with well-formed trailers) remain open there too.
