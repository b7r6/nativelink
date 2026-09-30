#!/usr/bin/env bash
# Fan built artifacts out from the build host to the other quorum members
# and link the example there (cheap: archive exists, only rustc links).
set -euo pipefail
SRC=${SRC:-b7r6@100.71.82.73}
DESTS=(${DESTS:-b7r6@100.89.101.109 b7r6@100.111.80.81})
FORGE=${FORGE:-keeper-forge}
for d in "${DESTS[@]}"; do
  echo "== $d"
  ssh "$SRC" "tar -C \$HOME/$FORGE -cf - nativelink-keeper \$(cd \$HOME/$FORGE && dirname \$(find build -name 'libnativelink_keeper_shim.a' | head -1))" \
    | ssh "$d" "mkdir -p \$HOME/$FORGE && tar -C \$HOME/$FORGE -xf -"
  ssh "$d" "cd \$HOME/$FORGE/nativelink-keeper && nix-shell -p llvmPackages_21.clang llvmPackages_21.libclang lld_21 rustc cargo --run 'LIBCLANG_PATH=\$(nix eval --raw nixpkgs#llvmPackages_21.libclang.lib)/lib NLK_LIB_DIR=\$(dirname \$(find \$HOME/$FORGE -name libnativelink_keeper_shim.a | head -1)) cargo build --release --features embedded --example node'"
done
