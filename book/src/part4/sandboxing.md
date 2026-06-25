# Sandboxing and Isolation

Actions must not observe each other's state. A test that passes because it reads a file left behind by a previous action is a correctness bug. NativeLink provides multiple isolation mechanisms, from filesystem sandboxing to full Linux namespace isolation.

## Filesystem Isolation

Every action gets its own working directory under the configured `work_directory`:

```
/data/work/
├── action-a1b2c3d4/
│   ├── src/main.c     (hardlinked from CAS)
│   ├── include/foo.h  (hardlinked from CAS)
│   └── output/        (created by action)
├── action-e5f6g7h8/
│   └── ...
```

After an action completes, its working directory is cleaned up. The next action starts with a fresh directory.

Input files are hardlinked from the worker's local CAS cache — they are read-only by convention but not enforced at the filesystem level without mount namespaces.

## Linux Namespace Isolation

For stronger isolation, NativeLink supports Linux namespaces:

```json5
local: {
  use_namespaces: true,       // PID, user, IPC, UTS namespaces
  use_mount_namespace: true   // mount namespace (filesystem isolation)
}
```

**Source:** [`nativelink-worker/src/namespace_utils.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-worker/src/namespace_utils.rs)

When enabled, the worker calls `unshare()` with:
- `CLONE_NEWPID` — isolated PID namespace (action sees only its own processes)
- `CLONE_NEWUSER` — user namespace (action runs as "root" inside the namespace, unprivileged outside)
- `CLONE_NEWIPC` — IPC namespace (no shared memory between actions)
- `CLONE_NEWUTS` — UTS namespace (isolated hostname)
- `CLONE_NEWNS` (if `use_mount_namespace`) — mount namespace (filesystem mounts are private)

With mount namespaces, input files can be bind-mounted read-only, preventing actions from modifying their inputs (which would corrupt the CAS cache via hardlinks).

## What Each Level Gives You

| Isolation Level | Filesystem | Process | Network | Use Case |
|----------------|-----------|---------|---------|----------|
| None | Shared host | Shared host | Shared host | Development only |
| Directory only | Separate workdir | Shared host | Shared host | Basic CI |
| Namespaces (no mount) | Separate workdir | Isolated PID/IPC | Shared host | Standard production |
| Namespaces (with mount) | Read-only inputs, private mounts | Isolated PID/IPC | Shared host | Strict production |
| Container (via entrypoint) | Container filesystem | Container isolation | Configurable | Maximum isolation |

## Container-Based Isolation

The strongest isolation comes from running actions inside containers via the entrypoint:

```json5
local: {
  entrypoint: "/usr/local/bin/container-entrypoint.sh",
  additional_environment: {
    CONTAINER_IMAGE: { property: "container-image" },
    ACTION_DIRECTORY: { action_directory: {} }
  }
}
```

The entrypoint script launches each action in a fresh container:

```bash
#!/bin/bash
exec docker run --rm \
  --network=none \
  --memory=4g \
  --cpus=4 \
  -v "${ACTION_DIRECTORY}:${ACTION_DIRECTORY}" \
  -w "${ACTION_DIRECTORY}" \
  "${CONTAINER_IMAGE}" "$@"
```

This gives you:
- Full filesystem isolation (container has its own root)
- Network isolation (`--network=none`)
- Resource limits (cgroups via Docker)
- Hermetic toolchain (inside the container image)

The tradeoff is startup latency. Container creation adds 100-500ms per action. For builds with thousands of small actions (e.g., C++ compilation), this overhead is significant. For builds with fewer, longer actions (e.g., integration tests), it's negligible.

## The Tradeoff Spectrum

```
Faster, less isolated                    Slower, more isolated
├─────────────────────────────────────────────────────────────┤
│  No isolation  │  Namespaces  │  Mount NS  │  Containers   │
│  (dev only)    │  (standard)  │  (strict)  │  (maximum)    │
```

Most production deployments use namespaces without containers. The namespace overhead is near-zero (it's a kernel flag, not a new process), and it prevents the most common isolation failures (PID collisions, shared memory leaks, leftover files).

Container isolation is used when:
- Actions need specific OS/package combinations (different from the worker host)
- You need strict network isolation (prevent test actions from hitting production services)
- You need resource limits per action (prevent one action from starving others)

## Security Considerations

Namespace isolation is not a security boundary. A determined action can escape user namespaces on older kernels. Container isolation (with properly configured Docker/Podman) is a stronger boundary but still not equivalent to a VM.

For truly untrusted workloads (running arbitrary user code), use:
- Containers with `--security-opt=no-new-privileges`
- Seccomp profiles
- Read-only root filesystem
- Network namespaces with no connectivity
- Consider gVisor or Firecracker for VM-level isolation

For typical build workloads (compiling your own code), namespace isolation is sufficient. The goal is correctness (preventing accidental state leakage), not security (preventing malicious escape).
