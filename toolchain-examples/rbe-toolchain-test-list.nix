{
  writeShellScriptBin,
  jo,
}:
# Discovery-only target: prints the rbe-toolchain command matrix as a JSON
# array and nothing else. Deliberately carries NO nativelink (or bazel)
# dependency, so `nix run .#rbe-toolchain-test-list` does not rebuild
# nativelink just to enumerate the lanes. The full
# rbe-toolchain-with-nativelink-test target still owns execution; this one
# only feeds generate-rbe-commands. The key list must stay in sync with the
# COMMANDS table in rbe-toolchain-test.nix (a mismatch only changes which
# shards fan out, and the lanes themselves re-validate, so it fails safe).
writeShellScriptBin "rbe-toolchain-test-list" ''
  set -euo pipefail
  ${jo}/bin/jo -a cpp-zig cpp-llvm python go rust java curl zstd abseil-py circl
''
