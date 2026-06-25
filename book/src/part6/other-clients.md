# Other Clients

NativeLink speaks REAPI. Any client that speaks REAPI works with NativeLink — not just Bazel and Buck2. This chapter covers the other clients: Goma/Reclient (Chromium), recc (CMake), Pants, and custom integrations.

## Goma / Reclient (Chromium)

Google's Chromium project uses Goma (deprecated) and its successor Reclient for distributed compilation. Both speak REAPI for the CAS and execution layer.

NativeLink has been validated with Chromium builds:

**Source:** [`deploy/chromium-example/`](https://github.com/straylight-prelude/straylight-nativelink/tree/main/deploy/chromium-example)

Reclient configuration:
```
RBE_service=nativelink.example.com:50051
RBE_instance=main
RBE_platform=container-image=docker://chromium-toolchain@sha256:...
```

Chromium's build system wraps `clang` calls with Reclient, which handles hashing, cache lookup, and remote dispatch. From NativeLink's perspective, it's just another REAPI client.

## recc (CMake)

recc is a remote execution client for CMake. It wraps compiler invocations and turns them into REAPI Execute calls. This lets you add remote caching and execution to CMake projects without changing the build system.

```bash
# Use recc as the compiler wrapper
export CC="recc cc"
export CXX="recc c++"

# Configure recc to use NativeLink
export RECC_SERVER=nativelink.example.com:50051
export RECC_INSTANCE=main
export RECC_CACHE_ONLY=1  # remote cache only (no execution)

# Run CMake as usual
cmake -B build && cmake --build build
```

recc computes the action hash from the preprocessed source, command arguments, and a platform descriptor. It checks the AC, and on miss either runs locally (cache-only mode) or submits for remote execution.

## Pants

Pants (v2+) supports REAPI remote caching and execution. Configuration is in `pants.toml`:

```toml
# pants.toml
[GLOBAL]
remote_store_address = "grpc://nativelink.example.com:50051"
remote_execution_address = "grpc://nativelink.example.com:50051"
remote_instance_name = "main"
remote_cache_read = true
remote_cache_write = true
```

Pants' process execution layer translates internal "Process" objects to REAPI Execute calls. The same NativeLink server config works for Pants as for Bazel.

## BuildStream

BuildStream can use REAPI-compatible caches. NativeLink integration exists in the test suite:

**Source:** [`integration_tests/buildstream/`](https://github.com/straylight-prelude/straylight-nativelink/tree/main/integration_tests/buildstream) (if present)

## Custom REAPI Clients

If you're building your own CI pipeline, code generation system, or build tool, you can speak REAPI directly. The protocol is defined in protobuf:

- [`build/bazel/remote/execution/v2/remote_execution.proto`](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto)

Minimal client workflow:
1. Serialize your inputs as blobs, compute digests
2. Upload missing blobs (`FindMissingBlobs` → `BatchUpdateBlobs`/`ByteStream.Write`)
3. Construct an `Action` proto (command digest + input root digest + platform)
4. Check AC: `GetActionResult(action_digest)`
5. On miss: `Execute(action_digest)` → wait for operation completion
6. Download outputs via `ByteStream.Read`

Libraries for REAPI clients exist in:
- **Rust:** `tonic` + generated protos (see NativeLink's `nativelink-proto` crate)
- **Go:** `bazelbuild/remote-apis-sdks`
- **Python:** `grpcio` + generated stubs
- **C++:** Bazel's `remote_execution` library

## Universal Configuration

Regardless of client, the NativeLink server config is the same. The only client-specific consideration is `instance_name`:

| Client | Default Instance Name |
|--------|----------------------|
| Bazel | `""` (empty string) |
| Buck2 | `"main"` |
| Reclient | Configurable, often `"default"` |
| recc | Configurable |
| Pants | Configurable |

If you need to support multiple clients, configure multiple instance names in NativeLink:

```json5
services: {
  cas: [
    { instance_name: "", cas_store: "CAS_STORE" },
    { instance_name: "main", cas_store: "CAS_STORE" },
    { instance_name: "default", cas_store: "CAS_STORE" }
  ],
  // ... same for ac, execution, capabilities, bytestream
}
```

All instance names can point to the same underlying stores. They're just routing labels.

## Client Feature Matrix

| Feature | Bazel | Buck2 | Reclient | recc | Pants |
|---------|:-----:|:-----:|:--------:|:----:|:-----:|
| Remote cache | Yes | Yes | Yes | Yes | Yes |
| Remote execution | Yes | Yes | Yes | Yes | Yes |
| Build without the bytes | Yes | Yes | No | No | Yes |
| Compression (zstd) | Yes | Partial | No | No | Yes |
| BEP | Yes | No | No | No | No |
| Persistent workers | Yes | Yes | No | No | No |
| Hybrid local/remote | Basic | Advanced | Yes | Basic | Yes |
| mTLS | Yes | Yes | Yes | Yes | Yes |

NativeLink supports all features in this matrix on the server side. Client support varies.
