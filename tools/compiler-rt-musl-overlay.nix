# Fix compiler-rt aarch64 cpu_model for musl (missing sys/auxv.h).
#
# VENDORED from ../straylight-toolchain/nix/overlays/compiler-rt-musl.nix.
# Keep in sync with that copy — it is the source of truth. This is a verbatim
# vendor (no new flake input) so the nativelink server builds for
# aarch64-unknown-linux-musl without depending on the toolchain-builder flake.
#
# The aarch64/cpu_model.c includes <sys/auxv.h> unconditionally on Linux, but
# musl doesn't provide this header (glibc does). The getauxval() function IS
# available in musl — just not via sys/auxv.h. We patch the source to use
# __has_include for portable detection, falling back to declaring getauxval()
# externally when the header is absent (safe on glibc too, where it exists).
#
# IMPORTANT: the baremetal compiler-rt build (compiler-rt-no-libc) uses
# -nodefaultlibs, so we cannot include ANY system headers. We declare
# getauxval() with explicit types. Without this fix compiler-rt-no-libc fails
# to build for aarch64-musl, which is what blocks nativelink-aarch64-linux.
_final: prev: let
  inherit (prev) lib;

  # Check if this is an aarch64 target (where cpu_model/aarch64.c is compiled)
  isAarch64Target = prev.stdenv.hostPlatform.isAarch64;

  # Patch for compiler-rt - use __has_include for portable detection
  # CRITICAL: Do not include sys/types.h - baremetal builds have no headers
  compilerRtPatch = old:
    lib.optionalAttrs isAarch64Target {
      postPatch =
        (old.postPatch or "")
        + ''
                # Fix for musl: sys/auxv.h doesn't exist but getauxval() is available
                # Use __has_include to detect and fall back to extern declaration
                # NOTE: We cannot use sys/types.h either - baremetal builds have no libc headers
                if [ -f lib/builtins/cpu_model/aarch64.c ]; then
                  substituteInPlace lib/builtins/cpu_model/aarch64.c \
                    --replace-fail '#include <sys/auxv.h>' '
          // Fixed for musl: use __has_include to detect sys/auxv.h availability
          #if __has_include(<sys/auxv.h>)
          #include <sys/auxv.h>
          #else
          // musl provides getauxval() but not via sys/auxv.h
          // Declare directly without any headers (baremetal builds have no libc)
          extern unsigned long getauxval(unsigned long __type) __attribute__((__weak__));
          #ifndef AT_HWCAP
          #define AT_HWCAP  16
          #endif
          #ifndef AT_HWCAP2
          #define AT_HWCAP2 26
          #endif
          #endif
          '
                fi
        '';
    };

  # Apply patch via overrideScope, preserving the llvmPackages structure.
  # IMPORTANT: nixpkgs has THREE compiler-rt variants:
  #   - compiler-rt (alias to compiler-rt-libc)
  #   - compiler-rt-libc (full runtime with libc)
  #   - compiler-rt-no-libc (baremetal/bootstrap - THIS ONE FAILS on aarch64-musl)
  # We must patch compiler-rt-no-libc specifically.
  patchedLlvmPackages = llvmPkgs: let
    patched = llvmPkgs.overrideScope (_llvmFinal: llvmPrev: {
      compiler-rt = llvmPrev.compiler-rt.overrideAttrs compilerRtPatch;
      compiler-rt-libc = llvmPrev.compiler-rt-libc.overrideAttrs compilerRtPatch;
      compiler-rt-no-libc = llvmPrev.compiler-rt-no-libc.overrideAttrs compilerRtPatch;
    });
  in
    # Preserve override/overrideDerivation from original for self-host chain
    patched
    // {
      inherit (llvmPkgs) override overrideDerivation;
    };
in {
  llvmPackages_22 = patchedLlvmPackages prev.llvmPackages_22;
}
