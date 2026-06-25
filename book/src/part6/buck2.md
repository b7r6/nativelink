# Buck2

Buck2 is a first-class REAPI client. Its remote execution support is production-grade — Meta runs it at enormous scale internally. NativeLink supports Buck2 without compromise, and this chapter shows you how to set it up properly.

If you're coming from Bazel, the model is conceptually identical (both speak REAPI) but the configuration surface is completely different. Buck2 configures remote execution through `.buckconfig` and Starlark platform rules, not command-line flags.

## Basic Configuration

All Buck2 remote execution configuration lives in `.buckconfig`:

```ini
# .buckconfig
[buck2_re_client]
engine_address = nativelink.example.com:50051
action_cache_address = nativelink.example.com:50051
cas_address = nativelink.example.com:50051
tls = true
instance_name = main
```

**Source:** [`integration_tests/buck2/.buckconfig`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/integration_tests/buck2/.buckconfig)

Key differences from Bazel:
- **Single config file** (not CLI flags). Changes require editing `.buckconfig`.
- **Separate addresses** for engine (execution), AC, and CAS. They can point to different servers (for multi-tier architectures) or the same server.
- **`instance_name` is required.** Buck2 always sends an instance name. Convention is `"main"`.
- **TLS is per-connection**, not per-flag.

## The Instance Name Requirement

Buck2 **always** sends `instance_name: "main"` (or whatever you configure). Your NativeLink server config must match:

```json5
services: {
  cas: [{ instance_name: "main", cas_store: "CAS_STORE" }],
  ac: [{ instance_name: "main", ac_store: "AC_STORE" }],
  execution: [{ instance_name: "main", cas_store: "CAS_STORE", scheduler: "SCHEDULER" }],
  capabilities: [{ instance_name: "main", remote_execution: { scheduler: "SCHEDULER" } }],
  bytestream: [{ instance_name: "main", cas_store: "CAS_STORE" }]
}
```

**Source:** [`integration_tests/buck2/buck2_cas.json5`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/integration_tests/buck2/buck2_cas.json5)

If instance names don't match, you'll get `NOT_FOUND` errors on every request. Bazel defaults to empty string; Buck2 defaults to `"main"`. This is the most common cross-client configuration mistake.

## Execution Platforms

Buck2's execution platform is defined in Starlark using `ExecutionPlatformInfo`:

```python
# platforms/defs.bzl

def _platforms(ctx):
    configuration = ConfigurationInfo(
        constraints = {},
        values = {},
    )

    platform = ExecutionPlatformInfo(
        label = ctx.label.raw_target(),
        configuration = configuration,
        executor_config = CommandExecutorConfig(
            local_enabled = True,
            remote_enabled = True,
            use_limited_hybrid = True,
            remote_execution_properties = {
                "container-image": "docker://toolchain@sha256:abc123",
                "OSFamily": "Linux",
            },
            remote_execution_use_case = "buck2-default",
            remote_output_paths = "output_paths",
        ),
    )

    return [
        DefaultInfo(),
        ExecutionPlatformRegistrationInfo(platforms = [platform]),
    ]

platforms = rule(attrs = {}, impl = _platforms)
```

**Source:** [`integration_tests/buck2/platforms/defs.bzl`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/integration_tests/buck2/platforms/defs.bzl)

Then register the platform:

```python
# platforms/BUCK
load(":defs.bzl", "platforms")

platforms(name = "platforms")
```

```ini
# .buckconfig
[build]
execution_platforms = root//platforms:platforms
```

### CommandExecutorConfig Fields

| Field | Purpose |
|-------|---------|
| `local_enabled` | Allow local execution as fallback |
| `remote_enabled` | Enable remote execution |
| `use_limited_hybrid` | Try local first, fall back to remote (or vice versa) |
| `remote_execution_properties` | Platform properties sent in Execute RPC |
| `remote_execution_use_case` | Opaque string for server-side routing |
| `remote_output_paths` | Use `"output_paths"` for REAPI v2.1+ output handling |

### Hybrid Execution

Buck2's `use_limited_hybrid` mode runs some actions locally and some remotely, choosing based on which is likely faster. This is more sophisticated than Bazel's `--remote_local_fallback` — Buck2 can race local vs remote and take whichever finishes first.

Configuration:
```python
executor_config = CommandExecutorConfig(
    local_enabled = True,
    remote_enabled = True,
    use_limited_hybrid = True,
    # When hybrid is enabled, Buck2 decides per-action
    # based on predicted execution time
)
```

For actions that are fast locally (simple file copies, small compilations), hybrid mode avoids the network round-trip. For slow actions (linking, testing), it uses remote execution.

## NativeLink Server Config for Buck2

A complete minimal config:

```json5
{
  stores: [
    {
      name: "AC_MAIN_STORE",
      filesystem: {
        content_path: "/data/ac/content",
        temp_path: "/data/ac/tmp",
        eviction_policy: { max_bytes: "1gb" }
      }
    },
    {
      name: "CAS_STORE",
      fast_slow: {
        fast: {
          filesystem: {
            content_path: "/data/cas/content",
            temp_path: "/data/cas/tmp",
            eviction_policy: { max_bytes: "50gb" }
          }
        },
        slow: { noop: {} }
      }
    }
  ],

  schedulers: [{
    name: "MAIN_SCHEDULER",
    simple: {
      supported_platform_properties: {
        cpu_count: "minimum",
        OSFamily: "priority",
        "container-image": "priority",
        ISA: "exact"
      }
    }
  }],

  workers: [{
    local: {
      worker_api_endpoint: { uri: "grpc://127.0.0.1:50061" },
      cas_fast_slow_store: "CAS_STORE",
      upload_action_result: { ac_store: "AC_MAIN_STORE" },
      work_directory: "/data/work",
      platform_properties: {
        cpu_count: { query_cmd: "nproc" },
        OSFamily: { values: [""] },
        "container-image": { values: [""] },
        ISA: { values: ["x86-64"] }
      }
    }
  }],

  servers: [
    {
      name: "public",
      listener: { http: { socket_address: "0.0.0.0:50051" } },
      services: {
        cas: [{ instance_name: "main", cas_store: "CAS_STORE" }],
        ac: [{ instance_name: "main", ac_store: "AC_MAIN_STORE" }],
        execution: [{
          instance_name: "main",
          cas_store: "CAS_STORE",
          scheduler: "MAIN_SCHEDULER"
        }],
        capabilities: [{
          instance_name: "main",
          remote_execution: { scheduler: "MAIN_SCHEDULER" }
        }],
        bytestream: [{ instance_name: "main", cas_store: "CAS_STORE" }]
      }
    },
    {
      name: "worker_api",
      listener: { http: { socket_address: "0.0.0.0:50061" } },
      services: {
        worker_api: { scheduler: "MAIN_SCHEDULER" },
        admin: {},
        health: {}
      }
    }
  ],

  global: { max_open_files: 24576 }
}
```

Note the two server blocks: one public (port 50051) for Buck2 clients, one private (port 50061) for workers. This separation is important for security — the worker API should not be accessible to clients.

## Toolchain Approaches for Buck2

### Container-Based

The most common approach for Buck2. A dedicated container toolchain builder:

**Source:** [`tools/toolchain-buck2/toolchain-buck2.sh`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/tools/toolchain-buck2/toolchain-buck2.sh)

Set the container image in platform properties:

```python
remote_execution_properties = {
    "container-image": "docker://registry/buck2-toolchain@sha256:...",
}
```

### Nix/LRE for Buck2

LRE's generated Bazel configs don't directly apply to Buck2. But the principle is the same — use Nix store paths as platform property values:

```python
remote_execution_properties = {
    "lre-cc": "/nix/store/abc123-clang-17/bin/clang",
    "lre-rs": "/nix/store/def456-rust-1.75/bin/rustc",
}
```

The worker must have these paths available (via Nix closure in its container image or directly on the host via Nix).

### Host Toolchain (Simplest)

For single-machine or homogeneous-fleet deployments:

```python
remote_execution_properties = {
    "OSFamily": "Linux",
}
```

No toolchain identity in the properties. This works when all workers are identical (same image, same packages). It breaks the moment workers diverge.

## Buck2 vs Bazel: Key Differences

| Aspect | Bazel | Buck2 |
|--------|-------|-------|
| Config location | CLI flags (`.bazelrc`) | `.buckconfig` + Starlark |
| Instance name default | Empty string | `"main"` |
| Platform properties | `--remote_default_exec_properties` | `remote_execution_properties` dict |
| Hybrid execution | `--remote_local_fallback` | `use_limited_hybrid` (smarter) |
| Output handling | `--remote_download_*` flags | `remote_output_paths` |
| Separate CAS/AC/exec addresses | No (one `--remote_cache` + `--remote_executor`) | Yes (separate fields) |
| Auth | CLI flags + credential helpers | `.buckconfig` + TLS certs |
| Action result upload | Always (or `--remote_upload_local_results=false`) | Via worker config `upload_action_result` |

## Running the Integration Test

NativeLink's Buck2 integration test provides a complete reference:

**Source:** [`integration_tests/buck2/buck2-with-nativelink-test.nix`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/integration_tests/buck2/buck2-with-nativelink-test.nix)

This test:
1. Starts NativeLink with the Buck2-specific config
2. Runs `buck2 build //...` with remote execution enabled
3. Verifies cache hits on subsequent builds
4. Tests hybrid (local + remote) execution

To run it locally (requires Nix):
```bash
nix build .#buck2-integration-test
```

## Common Issues (Buck2-Specific)

### "Connection refused" or "TLS handshake failed"

Buck2's TLS configuration:
```ini
[buck2_re_client]
tls = true
# Buck2 uses the system certificate store by default
# For custom CAs, you may need to set SSL_CERT_FILE
```

### "Instance name mismatch"

If you see `NOT_FOUND` on every request, check that `.buckconfig`'s `instance_name` matches the NativeLink server config's `instance_name` on every service. They must be identical strings.

### "Remote execution not enabled"

Verify:
1. `remote_enabled = True` in the platform's `CommandExecutorConfig`
2. The execution platform is registered in `[build] execution_platforms`
3. NativeLink has an `execution` service configured (not just `cas` + `ac`)

### Inconsistent results between local and remote

This is the toolchain problem (Part V). Buck2's hybrid mode makes it more visible — if local and remote toolchains differ, you'll see non-deterministic results depending on which executor "won" for each action.

Fix: use the same toolchain everywhere, or disable hybrid mode (`local_enabled = False`).
