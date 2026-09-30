# Building the nativelink-keeper shim

The shim (`keeper_shim.cpp`) links against a vendored ClickHouse tree built in
Keeper-only ("FastTest-minimal") configuration. ClickHouse requires clang and
lld; we use LLVM 21 from nixpkgs.

## Prerequisites

- Vendored ClickHouse source with contrib submodules checked out
  (default location: `nativelink-keeper/vendor/clickhouse`, or pass
  `-DNLK_CLICKHOUSE_DIR=...`).

## Toolchain shell

```bash
nix-shell -p \
  llvmPackages_21.clang llvmPackages_21.bintools lld_21 \
  cmake ninja nasm yasm rustc cargo \
  --run bash
```

## Configure and build

```bash
SRC=/path/to/nativelink/nativelink-keeper/shim
BUILD=/path/to/build-keeper-shim   # out-of-tree; ~40 GB for a full CH build

cmake -G Ninja -S "$SRC" -B "$BUILD" \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_C_COMPILER=clang \
  -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_LINKER_TYPE=LLD \
  -DNLK_CLICKHOUSE_DIR=/path/to/nativelink-keeper/vendor/clickhouse

ninja -C "$BUILD" nativelink_keeper_shim
```

Notes:

- `ENABLE_RUST=1` (needed by the minimal config) requires `rustc`/`cargo` on
  PATH inside the shell; ClickHouse's corrosion integration picks them up.
- `nasm`/`yasm` are needed by some contrib assembly even in the minimal
  config.
- First build is long (it compiles `dbms`); subsequent shim-only rebuilds are
  seconds.

## Artifacts / build.rs wiring

The static shim archive lands at:

```
$BUILD/libnativelink_keeper_shim.a
```

with its ClickHouse dependency archives under `$BUILD/clickhouse/` (e.g.
`$BUILD/clickhouse/src/libdbms.a`, `$BUILD/clickhouse/contrib/...`).

For `nativelink-keeper/build.rs`, set:

```bash
export NLK_LIB_DIR="$BUILD"
```

`build.rs` is expected to emit `cargo:rustc-link-search=native=$NLK_LIB_DIR`
(plus the `clickhouse` subdirectories) and link `nativelink_keeper_shim`
followed by the ClickHouse archives and `stdc++`/`c++abi` as appropriate for
the toolchain.
