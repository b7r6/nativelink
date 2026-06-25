# Nix and LRE

Local Remote Execution (LRE) is NativeLink's answer to the toolchain problem. It uses Nix to create toolchains that are identical — bit-for-bit — on your laptop, in CI, and on remote workers. This gives you structural cache correctness and near-perfect hit rates.

LRE is the most powerful approach to toolchains in the remote execution world. It is also the most complex to set up. This chapter explains both.

## The Idea

Nix is a package manager where every package is identified by a store path derived from its build inputs:

```
/nix/store/zms5771rx1yqb4wd6qbj5f9sb2paq75k-clang-17.0.6/bin/clang
```

That path is a hash of: the Clang source, the build flags, the compiler used to compile Clang, the libc, and all transitive dependencies. If any of these change, the hash changes, and you get a different path.

This means:
- Same source + same nixpkgs revision = same store path = same binary, everywhere
- Different source or different nixpkgs = different store path = different binary, unambiguously

LRE exploits this property: if your local development environment and the remote worker both have `/nix/store/zms5771...clang-17.0.6/bin/clang`, they have byte-identical compilers. Actions that use this compiler produce byte-identical outputs. Cache hits are guaranteed.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Your Nix flake (defines toolchain derivations)             │
├─────────────────────────────────────────────────────────────┤
│  LRE Flake Module (generates lre.bazelrc, platform config)  │
├─────────────────────────────────────────────────────────────┤
│  Worker Container Image (Nix closure with toolchain)        │
├─────────────────────────────────────────────────────────────┤
│  Platform Properties (Nix store paths as values)            │
└─────────────────────────────────────────────────────────────┘
```

The flow:
1. A Nix flake defines your toolchains (C/C++, Rust, etc.)
2. The LRE flake module generates Bazel configuration that points to Nix store paths
3. A container image is built from the Nix closure (containing the toolchain binaries)
4. Workers run this container image
5. Platform properties include content-addressed identifiers (container image hash or Nix paths)
6. Client and worker agree on toolchain identity via content-addressing

## Setting Up LRE

**Source:** [`local-remote-execution/`](https://github.com/straylight-prelude/straylight-nativelink/tree/main/local-remote-execution)

### Step 1: Flake Inputs

Your project's `flake.nix` imports NativeLink's flake and follows its nixpkgs:

```nix
# flake.nix
{
  inputs = {
    nixpkgs = {
      url = "github:nixos/nixpkgs";
      # CRITICAL: follows nativelink's nixpkgs so store paths match
      follows = "nativelink/nixpkgs";
    };
    nativelink.url = "github:TraceMachina/nativelink/<commit>";
  };
}
```

The `follows` is essential. If your local nixpkgs and the worker's nixpkgs differ, Nix store paths will differ (even for the same package version), and you'll get cache misses.

**Source:** [`local-remote-execution/README.md`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/local-remote-execution/README.md)

### Step 2: LRE Flake Module

Import the flake module and generate `lre.bazelrc` in your shell hook:

```nix
# flake.nix (continued)
{
  imports = [ inputs.nativelink.flakeModule ];

  devShells.default = pkgs.mkShell {
    shellHook = ''
      ${config.lre.installationScript}
    '';
  };
}
```

When you enter the dev shell (via `direnv` or `nix develop`), it generates:

```bash
# lre.bazelrc (auto-generated)
build --action_env=BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN=1
build --define=EXECUTOR=remote
build --extra_execution_platforms=@local-remote-execution//generated-cc/config:platform
build --extra_toolchains=@local-remote-execution//generated-cc/config:cc-toolchain
```

**Source:** [`local-remote-execution/flake-module.nix`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/local-remote-execution/flake-module.nix)

### Step 3: Generated Toolchain Configuration

The LRE module generates Bazel BUILD files that reference Nix store paths:

```python
# local-remote-execution/generated-cc/config/BUILD (auto-generated)
platform(
    name = "platform",
    constraint_values = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
    exec_properties = {
        "container-image": "docker://lre-cc:zms5771rx1yqb4wd6qbj5f9sb2paq75k",
        "OSFamily": "Linux",
    },
)
```

**Source:** [`local-remote-execution/generated-cc/config/BUILD`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/local-remote-execution/generated-cc/config/BUILD)

The container image tag includes the Nix hash — if the toolchain derivation changes, the tag changes, and the platform changes, and all action hashes change. Cache correctness is structural.

### Step 4: Worker Container Image

The LRE container image is built from the Nix closure:

**Source:** [`local-remote-execution/overlays/lre-cc.nix`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/local-remote-execution/overlays/lre-cc.nix)

This produces a minimal OCI image containing only the Nix store paths needed by the toolchain — no full OS, no package manager, just the exact binaries and libraries required.

### Step 5: Worker Configuration

Workers advertise their LRE capabilities:

```json5
platform_properties: {
  "lre-cc": { values: ["/nix/store/abc123-clang-17/bin/clang"] },
  "lre-rs": { values: ["/nix/store/def456-rust-1.75/bin/rustc"] },
  OSFamily: { values: ["Linux"] }
}
```

## Why This Works

The chain of content-addressing:

1. Nix derivation input → Nix store path (hash of inputs)
2. Nix store path → container image tag (includes Nix hash)
3. Container image tag → platform property value
4. Platform property → action hash (via `Platform` in the `Action` proto)

If the toolchain changes at step 1, everything downstream changes. If it doesn't change, everything stays the same. There's no ambiguity, no "did someone update the tag?", no "is the worker running the right version?"

## The Rust Toolchain

LRE includes pre-built Rust toolchain configurations:

**Source:** [`local-remote-execution/rust/BUILD.bazel`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/local-remote-execution/rust/BUILD.bazel)

These define `rust_toolchain` targets pointing at Nix store paths for `rustc`, `cargo`, `clippy`, `rustfmt`, and the standard library. Same principle: paths are content-addressed, identity is structural.

## Limitations

1. **x86_64-linux only** (currently). ARM and macOS support is planned.
2. **Requires Nix.** Your team must install and understand Nix. This is a non-trivial ask.
3. **nixpkgs alignment is critical.** If local and remote nixpkgs diverge, store paths diverge, and you lose cache sharing. The `follows` in flake.nix is not optional.
4. **First-time setup is complex.** The Nix ecosystem has a steep learning curve. Once set up, maintenance is low (update the flake input, regenerate).
5. **Bazel-focused.** LRE's toolchain generation currently targets Bazel. Buck2 integration requires manual platform configuration (see Part VI).

## When to Use LRE

Use LRE when:
- Cache hit rates matter (large team, expensive builds)
- You need local/CI/remote parity (debugging remote failures locally)
- You're willing to invest in Nix infrastructure
- You want structural correctness, not correctness-by-convention

Don't use LRE when:
- Your team is small and all on the same machine type
- You can't justify the Nix learning curve
- You only need remote caching (not execution)
- Your builds are already fast enough without cache sharing
