# Local Remote Execution with Nix

This is the advanced setup: Nix-based toolchains that are bit-for-bit identical between your laptop, CI, and remote workers. Near-perfect cache hit rates. Structural correctness. The full LRE experience.

## Prerequisites

- Nix 2.19+ with flakes enabled
- Docker (for container images)
- Bazel 7+ (LRE toolchain generation currently targets Bazel)
- A NativeLink instance (local or remote)

## Step 1: Project Flake

Create `flake.nix` in your project root:

```nix
{
  inputs = {
    nixpkgs = {
      url = "github:nixos/nixpkgs/nixos-unstable";
      # CRITICAL: follow nativelink's nixpkgs for path alignment
      follows = "nativelink/nixpkgs";
    };
    nativelink = {
      url = "github:TraceMachina/nativelink";
    };
    flake-parts.url = "github:hercules-ci/flake-parts";
  };

  outputs = inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" ];

      imports = [
        inputs.nativelink.flakeModule
      ];

      perSystem = { pkgs, config, ... }: {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            bazel_7
            grpcurl
          ];
          shellHook = ''
            # Generate lre.bazelrc with toolchain paths
            ${config.lre.installationScript}
          '';
        };
      };
    };
}
```

The key line is `follows = "nativelink/nixpkgs"` — this ensures your local Nix packages use the same nixpkgs as NativeLink's worker images. Same nixpkgs = same store paths = cache hits.

## Step 2: Enter the Dev Shell

```bash
# If using direnv, it happens automatically:
echo "use flake" > .envrc
direnv allow

# Or manually:
nix develop
```

This generates `lre.bazelrc` in your workspace:

```bash
# lre.bazelrc (auto-generated, do not edit)
build --action_env=BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN=1
build --define=EXECUTOR=remote
build --extra_execution_platforms=@local-remote-execution//generated-cc/config:platform
build --extra_toolchains=@local-remote-execution//generated-cc/config:cc-toolchain
```

## Step 3: Import LRE in Bazel Config

`.bazelrc`:
```bash
# Import LRE config (only active inside nix develop)
try-import %workspace%/lre.bazelrc

# Remote execution targeting NativeLink
build --remote_cache=grpc://nativelink.example.com:50051
build --remote_executor=grpc://nativelink.example.com:50051
build --remote_instance_name=main
```

`MODULE.bazel`:
```python
module(name = "my-lre-project", version = "0.0.0")

bazel_dep(name = "rules_cc", version = "0.2.18")
bazel_dep(name = "platforms", version = "1.1.0")

# Import the LRE toolchain module
bazel_dep(name = "local-remote-execution", version = "0.0.0")
local_path_override(
    module_name = "local-remote-execution",
    path = "./local-remote-execution",  # symlinked from nativelink repo
)
```

## Step 4: Build the Worker Image

The LRE worker image is a minimal container built from the Nix closure:

```bash
# Build the LRE C++ container image
nix build .#lre-cc

# Load it into Docker
docker load < result

# Tag it (the image name includes the Nix hash)
docker tag lre-cc:zms5771rx1yqb4wd6qbj5f9sb2paq75k \
  registry.example.com/lre-cc:zms5771rx1yqb4wd6qbj5f9sb2paq75k

# Push to registry
docker push registry.example.com/lre-cc:zms5771rx1yqb4wd6qbj5f9sb2paq75k
```

The image tag includes the Nix derivation hash. If the toolchain changes (new Clang version, different flags), the hash changes, the tag changes, and the platform config changes automatically.

## Step 5: Configure NativeLink Workers

Workers must have the LRE container image and advertise matching platform properties:

```json5
workers: [{
  local: {
    worker_api_endpoint: { uri: "grpc://scheduler:50061" },
    cas_fast_slow_store: "WORKER_CAS",
    work_directory: "/data/work",
    entrypoint: "/usr/local/bin/lre-entrypoint.sh",
    platform_properties: {
      cpu_count: { query_cmd: "nproc" },
      OSFamily: { values: ["Linux"] },
      "container-image": {
        values: ["docker://lre-cc:zms5771rx1yqb4wd6qbj5f9sb2paq75k"]
      }
    },
    additional_environment: {
      CONTAINER_IMAGE: { property: "container-image" },
      ACTION_DIRECTORY: { action_directory: {} }
    }
  }
}]
```

Alternatively, run the worker directly inside the LRE container (no entrypoint script needed — the Nix store paths are available directly):

```json5
workers: [{
  local: {
    worker_api_endpoint: { uri: "grpc://scheduler:50061" },
    cas_fast_slow_store: "WORKER_CAS",
    work_directory: "/data/work",
    // No entrypoint — toolchain is in the worker's filesystem
    platform_properties: {
      cpu_count: { query_cmd: "nproc" },
      OSFamily: { values: ["Linux"] },
      "container-image": {
        values: ["docker://lre-cc:zms5771rx1yqb4wd6qbj5f9sb2paq75k"]
      }
    }
  }
}]
```

## Step 6: Build

```bash
# Inside nix develop:
bazel build //...

# First build: actions execute remotely with LRE toolchain
# Subsequent builds (same machine or different): cache hits
```

## The Cache Hit Chain

Why this achieves near-perfect hit rates:

1. **You and CI share the same Nix flake** → same nixpkgs → same Nix store paths
2. **Same Nix store paths** → same toolchain binaries (bit-for-bit)
3. **Same toolchain** → same `container-image` platform property value
4. **Same platform property** → same action hash
5. **Same action hash** → cache hit

If any link in this chain breaks (different nixpkgs, stale flake lock, wrong image), you get cache misses. The fix is always: update the flake lock, re-enter the dev shell, regenerate `lre.bazelrc`.

## Verifying Cache Alignment

```bash
# Check your local toolchain paths:
cat lre.bazelrc | grep toolchain

# Check the remote worker's image:
# (the platform property in lre.bazelrc should match the worker's advertised value)
```

If they match, you'll get cache hits. If they don't, run:
```bash
nix flake update
# Re-enter dev shell to regenerate lre.bazelrc
exit && nix develop
```

## Multi-Language LRE

LRE supports multiple toolchains simultaneously:

- **C/C++** — `lre-cc` (Clang from Nix)
- **Rust** — `lre-rs` (rustc from Nix)

Each language has its own container image and platform config. The images can be layered (one base with both toolchains) or separate (one per language, matched by separate platform properties).

**Source:** [`local-remote-execution/rust/BUILD.bazel`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/local-remote-execution/rust/BUILD.bazel)

## Limitations and Workarounds

| Limitation | Workaround |
|-----------|-----------|
| x86_64-linux only | Cross-compile, or wait for ARM support |
| Requires Nix | No workaround (Nix is foundational to LRE) |
| nixpkgs must align | Use `follows` in flake.nix religiously |
| Complex initial setup | Use this guide; once working, maintenance is low |
| Bazel-focused | Buck2 can use the same images but needs manual platform config |

## When LRE is Worth the Complexity

- Teams with 10+ developers (cache sharing saves significant CI time)
- Monorepos with multiple languages (shared Nix closure for all toolchains)
- Organizations where "it works on my machine" is a frequent complaint
- Any team where cache hit rate directly impacts developer velocity
