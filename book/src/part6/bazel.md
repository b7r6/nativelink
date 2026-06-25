# Bazel

Bazel is the most common REAPI client. Its remote execution support is mature, well-documented, and works out of the box with NativeLink. This chapter covers the configuration, the gotchas, and the optimization paths.

## Basic Configuration

Remote caching only (no execution):

```bash
# .bazelrc
build --remote_cache=grpc://nativelink:50051
build --remote_upload_local_results=true
```

Remote caching and execution:

```bash
# .bazelrc
build --remote_cache=grpc://nativelink:50051
build --remote_executor=grpc://nativelink:50051
build --remote_default_exec_properties=cpu_count=1
build --remote_default_exec_properties=OSFamily=Linux
```

That's it for a basic setup. Bazel handles the REAPI protocol — `FindMissingBlobs`, `Execute`, `ByteStream` — automatically.

## Instance Names

NativeLink routes requests by instance name. Bazel defaults to empty string. You can set it:

```bash
build --remote_instance_name=main
```

This must match the `instance_name` in the NativeLink server config:

```json5
services: {
  cas: [{ instance_name: "main", cas_store: "CAS_STORE" }],
  ac: [{ instance_name: "main", ac_store: "AC_STORE" }],
  execution: [{ instance_name: "main", cas_store: "CAS_STORE", scheduler: "SCHEDULER" }]
}
```

If you omit `--remote_instance_name`, the NativeLink config should use `instance_name: ""` (empty string).

## Platform Configuration

For remote execution, you need to tell Bazel what platform the remote workers provide:

```python
# platforms/BUILD.bazel
platform(
    name = "linux_x86_64",
    constraint_values = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
    exec_properties = {
        "container-image": "docker://toolchain@sha256:abc123...",
        "OSFamily": "Linux",
        "ISA": "x86-64",
    },
)
```

```bash
# .bazelrc
build --extra_execution_platforms=//platforms:linux_x86_64
build --host_platform=//platforms:linux_x86_64
```

The `exec_properties` map becomes the action's platform properties in the `Execute` RPC. These must match the scheduler's `supported_platform_properties` configuration.

## NativeLink Integration Tests

The repository includes a complete test of Bazel remote execution and caching:

**Source:** [`integration_tests/simple_remote_execution_test.sh`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/integration_tests/simple_remote_execution_test.sh)

The relevant `.bazelrc` flags:

```bash
# From .bazelrc (lines 61-65)
build:self_test --remote_cache=grpc://127.0.0.1:50051
build:self_execute --remote_executor=grpc://127.0.0.1:50052
build:self_execute --remote_default_exec_properties=cpu_count=1
```

**Source:** [`.bazelrc`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/.bazelrc)

## Toolchain Integration

### With LRE

```bash
# Import generated LRE config (created by nix develop shell hook)
try-import %workspace%/lre.bazelrc
```

The generated `lre.bazelrc` adds:
```bash
build --action_env=BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN=1
build --define=EXECUTOR=remote
build --extra_execution_platforms=@local-remote-execution//generated-cc/config:platform
build --extra_toolchains=@local-remote-execution//generated-cc/config:cc-toolchain
```

### With zig-cc

```bash
build --config=zig-cc
build --remote_executor=grpc://nativelink:50051
```

### With toolchains_llvm

```bash
build --config=llvm
build --remote_executor=grpc://nativelink:50051
```

See the [toolchain-examples](https://github.com/straylight-prelude/straylight-nativelink/tree/main/toolchain-examples) directory for complete working configurations of each approach.

## Optimization Flags

### Compression

```bash
build --experimental_remote_cache_compression
build --experimental_remote_cache_compression_threshold=100
```

Compresses blobs before transfer. NativeLink supports this via the Capabilities service (advertises `zstd` compressor support). Reduces bandwidth significantly for text-heavy artifacts.

### Remote Output Mode

```bash
build --remote_download_minimal    # don't download outputs you don't need
build --remote_download_toplevel   # only download top-level outputs
```

`remote_download_minimal` (a.k.a. "Build without the Bytes") is the biggest single optimization. Bazel skips downloading intermediate outputs — it trusts that they're in CAS and only downloads final outputs. For large builds, this reduces bandwidth by 10-100x.

### Disk Cache + Remote Cache

```bash
build --disk_cache=/tmp/bazel-disk-cache
build --remote_cache=grpc://nativelink:50051
```

Bazel checks the local disk cache before hitting the remote. This eliminates network round-trips for recently-built artifacts and gives you cache hits even when offline.

### Build Event Protocol

```bash
build --bes_backend=grpc://nativelink:50051
build --bes_results_url=https://your-dashboard.example.com/invocation/
```

NativeLink's experimental BEP server can ingest build events for monitoring and debugging.

**Source:** [`nativelink-service/src/bep_server.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-service/src/bep_server.rs)

## TLS Configuration

For production deployments:

```bash
build --remote_cache=grpcs://nativelink:50051
build --tls_certificate=/path/to/ca.crt
build --tls_client_certificate=/path/to/client.crt
build --tls_client_key=/path/to/client.key
```

NativeLink TLS config:

```json5
listener: {
  http: {
    socket_address: "0.0.0.0:50051",
    tls: {
      cert_file: "/certs/server.crt",
      key_file: "/certs/server.key",
      client_ca_file: "/certs/ca.crt",  // for mTLS
      client_auth_optional: false
    }
  }
}
```

## Common Issues

### "NOT_FOUND: Action result not found in cache"

The AC returned a result referencing CAS blobs that no longer exist (evicted). Solutions:
- Increase CAS size (or use durable backend like S3)
- Use `completeness_checking` store wrapper on the AC
- Reduce eviction pressure by deduplicating with `dedup` store

### "DEADLINE_EXCEEDED" on Execute

The action exceeded its timeout. Check:
- `max_action_timeout` in worker config
- `--remote_timeout` in Bazel flags (default: 600s)
- The action itself (is it actually hanging?)

### "UNAVAILABLE: Connection refused"

Bazel can't reach NativeLink. Check:
- Is NativeLink running? (`curl grpc://host:port` won't work — use `grpcurl`)
- Is the address correct? (no `http://` prefix for gRPC)
- Are there firewall rules blocking the port?
- If using TLS, are certificates valid and trusted?

### Low cache hit rate

Almost always a toolchain problem. Run `bazel aquery //target` on two machines and compare the action keys. If they differ, check:
- Are platform properties identical?
- Is the toolchain binary identical? (check paths, versions, hashes)
- Are environment variables leaking? (`--incompatible_strict_action_env`)
- Is there non-deterministic input ordering? (glob patterns, genrules)
