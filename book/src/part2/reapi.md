# REAPI from First Principles

The Remote Execution API (REAPI) is a gRPC protocol defined by the Bazel team and adopted by the industry. It consists of five services. Every remote cache and remote execution system — NativeLink, Buildbarn, EngFlow, BuildBuddy — implements these same five services.

Understanding REAPI is not optional. If you don't understand the protocol, you cannot debug cache misses, you cannot reason about performance, and you cannot configure NativeLink correctly.

## The Five Services

### 1. ContentAddressableStorage (CAS)

```protobuf
service ContentAddressableStorage {
  rpc FindMissingBlobs(FindMissingBlobsRequest) returns (FindMissingBlobsResponse);
  rpc BatchUpdateBlobs(BatchUpdateBlobsRequest) returns (BatchUpdateBlobsResponse);
  rpc BatchReadBlobs(BatchReadBlobsRequest) returns (BatchReadBlobsResponse);
  rpc GetTree(GetTreeRequest) returns (stream GetTreeResponse);
}
```

CAS is the content-addressed blob store. You put bytes in (keyed by their digest), you get bytes out. `FindMissingBlobs` is the critical operation — before uploading an input tree, the client asks "which of these blobs do you already have?" and only uploads the missing ones.

In NativeLink, CAS is backed by a store — any store. Memory, filesystem, S3, a composition of all three. The CAS service is a thin gRPC adapter over the store trait.

**Source:** [`nativelink-service/src/cas_server.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-service/src/cas_server.rs)

### 2. ActionCache (AC)

```protobuf
service ActionCache {
  rpc GetActionResult(GetActionResultRequest) returns (ActionResult);
  rpc UpdateActionResult(UpdateActionResultRequest) returns (ActionResult);
}
```

AC maps action digests to action results. This is where cache hits come from. The client computes the digest of its action (command + inputs + platform), calls `GetActionResult`, and if it gets a result back, it skips execution entirely and downloads the outputs from CAS.

`UpdateActionResult` is called after successful execution to store the result for future lookups. Some deployments disable client-side AC updates (only the server writes to AC after execution) to prevent cache poisoning.

**Source:** [`nativelink-service/src/ac_server.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-service/src/ac_server.rs)

### 3. ByteStream

```protobuf
service ByteStream {
  rpc Read(ReadRequest) returns (stream ReadResponse);
  rpc Write(stream WriteRequest) returns (WriteResponse);
  rpc QueryWriteStatus(QueryWriteStatusRequest) returns (QueryWriteStatusResponse);
}
```

CAS batch operations have size limits (typically 4MB). For large blobs — compiled binaries, tarballs, container images — clients use ByteStream. It's a chunked streaming interface for uploading and downloading blobs by digest.

The resource name encodes the digest: `{instance_name}/blobs/{hash}/{size}` for reads, `{instance_name}/uploads/{uuid}/blobs/{hash}/{size}` for writes.

**Source:** [`nativelink-service/src/bytestream_server.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-service/src/bytestream_server.rs)

### 4. Execution

```protobuf
service Execution {
  rpc Execute(ExecuteRequest) returns (stream Operation);
  rpc WaitExecution(WaitExecutionRequest) returns (stream Operation);
}
```

This is remote execution. The client sends an `ExecuteRequest` containing the action digest, and gets back a stream of `Operation` messages tracking progress. The operation goes through states: QUEUED → EXECUTING → COMPLETED.

NativeLink's execution service forwards the request to the scheduler, which dispatches to a matching worker. The operation stream is held open (long-polling) until the worker completes.

**Source:** [`nativelink-service/src/execution_server.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-service/src/execution_server.rs)

### 5. Capabilities

```protobuf
service Capabilities {
  rpc GetCapabilities(GetCapabilitiesRequest) returns (ServerCapabilities);
}
```

Feature negotiation. The client asks "what do you support?" and the server responds with digest functions, max batch sizes, supported compressors, execution priority ranges, etc. Clients use this to adapt their behavior.

**Source:** [`nativelink-service/src/capabilities_server.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-service/src/capabilities_server.rs)

## Instance Names

Every REAPI request carries an `instance_name` string. This is an opaque namespace — the server can use it to route to different storage backends, apply different policies, or isolate tenants.

NativeLink maps instance names to service configurations in the server config. Buck2 requires `instance_name: "main"` by convention. Bazel defaults to empty string but can be configured with `--remote_instance_name`.

## The Digest

The fundamental unit of identity in REAPI is the `Digest`:

```protobuf
message Digest {
  string hash = 1;      // hex-encoded hash
  int64 size_bytes = 2; // uncompressed size
}
```

Hash function is negotiated via Capabilities (SHA-256 is standard, Blake3 is supported by NativeLink). The size is part of the identity — it allows the server to pre-allocate and detect corruption without reading the full blob.

## How It All Fits Together

A complete remote execution flow:

1. Client builds a `Command` proto (argv, env, output paths)
2. Client builds an input `Directory` Merkle tree
3. Client uploads missing blobs to CAS via `FindMissingBlobs` + `BatchUpdateBlobs`/`ByteStream.Write`
4. Client constructs an `Action` (command digest + input root digest + platform)
5. Client checks AC: `GetActionResult(action_digest)` — if hit, done
6. Client calls `Execute(action_digest)` — receives operation stream
7. Scheduler dispatches to worker
8. Worker calls `FindMissingBlobs` on CAS (for its local cache), downloads inputs
9. Worker executes the command
10. Worker uploads outputs to CAS
11. Worker reports `ActionResult` to scheduler
12. Scheduler stores result in AC, forwards to client
13. Client downloads outputs from CAS

Steps 3-6 are the client's responsibility. Steps 7-12 are NativeLink's. Step 13 is the client again.
