# Container-Based Toolchains

If you can't or won't use Nix, containers are the next-best approach to toolchain management. The idea: package your toolchain into a container image, pin it by digest, and run every action inside that image.

## The Model

```
┌─────────────────────────────────────────────────────┐
│  Dockerfile (defines toolchain)                      │
├─────────────────────────────────────────────────────┤
│  Container Registry (stores images by digest)        │
├─────────────────────────────────────────────────────┤
│  Platform Property: container-image=sha256:abc...    │
├─────────────────────────────────────────────────────┤
│  Worker Entrypoint (pulls image, runs action inside) │
└─────────────────────────────────────────────────────┘
```

The container image is the toolchain. The image digest is the identity. If two workers pull the same digest, they have the same toolchain.

## Building Toolchain Images

A minimal C++ toolchain image:

```dockerfile
FROM ubuntu:22.04@sha256:abc123...
RUN apt-get update && apt-get install -y \
    clang-17 \
    lld-17 \
    libc++-17-dev \
    && rm -rf /var/lib/apt/lists/*
ENV CC=/usr/bin/clang-17
ENV CXX=/usr/bin/clang++-17
```

**Critical: pin the base image by digest, not tag.** `ubuntu:22.04` is mutable (it changes with security updates). `ubuntu:22.04@sha256:abc123...` is immutable. Mutable tags break cache correctness.

## NativeLink Worker Image Builder

NativeLink provides a Nix function to wrap any toolchain image as a worker container:

**Source:** [`tools/create-worker-experimental.nix`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/tools/create-worker-experimental.nix)

This takes a base toolchain image and adds:
- The `nativelink` binary (worker mode)
- A `nativelink` user and basic filesystem structure
- `/tmp`, `/usr/bin/env` symlink

The result is a container that is both a toolchain environment and a NativeLink worker.

## Buck2 Container Toolchain

For Buck2, NativeLink provides a complete toolchain container builder:

**Source:** [`tools/toolchain-buck2/toolchain-buck2.sh`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/tools/toolchain-buck2/toolchain-buck2.sh)

This script:
1. Builds a Docker image with the Buck2 toolchain
2. Pushes it to a registry (ECR in the reference implementation)
3. Wraps it with NativeLink worker support

## Worker Configuration

The worker uses the `container-image` platform property and an entrypoint script:

```json5
workers: [{
  local: {
    entrypoint: "/usr/local/bin/container-run.sh",
    platform_properties: {
      OSFamily: { values: ["Linux"] },
      "container-image": { values: [""] },  // accepts any
      cpu_count: { query_cmd: "nproc" }
    },
    additional_environment: {
      CONTAINER_IMAGE: { property: "container-image" },
      ACTION_DIRECTORY: { action_directory: {} },
      ACTION_TIMEOUT: { timeout_millis: {} }
    }
  }
}]
```

The scheduler passes `container-image` through (as a `priority` property that doesn't restrict matching), and the worker's entrypoint script reads it from the environment:

```bash
#!/bin/bash
# container-run.sh
set -euo pipefail

IMAGE="${CONTAINER_IMAGE}"
WORKDIR="${ACTION_DIRECTORY}"
TIMEOUT_MS="${ACTION_TIMEOUT:-600000}"
TIMEOUT_S=$((TIMEOUT_MS / 1000))

exec timeout "${TIMEOUT_S}" docker run --rm \
  --network=none \
  -v "${WORKDIR}:${WORKDIR}" \
  -w "${WORKDIR}" \
  "${IMAGE}" "$@"
```

## Client Configuration

### Bazel

```bash
# .bazelrc
build --remote_executor=grpc://nativelink:50051
build --remote_default_exec_properties=container-image=docker://registry.example.com/toolchain@sha256:abc123
build --remote_default_exec_properties=OSFamily=Linux
```

### Buck2

```ini
# .buckconfig
[buck2_re_client]
engine_address = nativelink:50051
action_cache_address = nativelink:50051
cas_address = nativelink:50051
instance_name = main
```

```python
# platforms/defs.bzl
platform = ExecutionPlatformInfo(
    label = ctx.label.raw_target(),
    configuration = configuration,
    executor_config = CommandExecutorConfig(
        local_enabled = False,
        remote_enabled = True,
        remote_execution_properties = {
            "container-image": "docker://registry.example.com/toolchain@sha256:abc123",
            "OSFamily": "Linux",
        },
        remote_execution_use_case = "buck2-default",
    ),
)
```

## The Digest Pinning Problem

Tags are mutable. Digests are not. This is critical.

```
# BAD - tag can change, breaking cache correctness:
container-image: docker://my-toolchain:latest

# BAD - tag can change even with a version:
container-image: docker://my-toolchain:v1.2.3

# GOOD - digest is immutable content-addressing:
container-image: docker://my-toolchain@sha256:a1b2c3d4e5f6...
```

If you use a mutable tag:
1. Monday: worker pulls `my-toolchain:latest` (resolves to digest A)
2. Tuesday: you push a new image with the same tag (now resolves to digest B)
3. Tuesday: some workers have A, some have B
4. Action hashes are identical (same `container-image` property value)
5. Cache serves results built with A to machines running B
6. Builds are silently broken

With digest pinning, step 2 would change the property value (different digest = different property = different action hash = cache miss). Correctness is maintained.

## Container Startup Overhead

The main cost of container-based toolchains is startup time. Every action incurs:
- Container creation: ~50-200ms
- Image layer mount: ~10-50ms (if already pulled)
- Image pull: seconds to minutes (first time only)

For builds with thousands of sub-second actions (e.g., individual C++ compilations), this overhead is unacceptable. Options:
- **Persistent workers** — keep the container running, send multiple actions to it (see deployment chapter)
- **Larger action granularity** — compile multiple files per action
- **Local execution for fast actions** — only use remote execution for slow actions (tests, links)
- **Pre-pulled images** — ensure workers have the image cached before accepting actions

## Multi-Language, Multi-Image

Different actions can use different container images. A C++ compilation might use `clang-toolchain@sha256:...` while a Rust build uses `rust-toolchain@sha256:...`. The scheduler's `priority` type for `container-image` passes the value through without restricting worker matching — the same worker can handle both.

```python
# Bazel platform for C++
platform(
    name = "cpp_platform",
    exec_properties = {
        "container-image": "docker://registry/cpp-toolchain@sha256:aaa...",
    },
)

# Bazel platform for Rust
platform(
    name = "rust_platform",
    exec_properties = {
        "container-image": "docker://registry/rust-toolchain@sha256:bbb...",
    },
)
```

Each platform produces different action hashes (different `container-image` property) even for the same source files. Actions land on the same workers but execute in different containers. Cache correctness is maintained because the platform is part of the action hash.
