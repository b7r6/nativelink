#!@bash@
# §12 floor-loader worker entrypoint for NativeLink remote execution.
#
# NativeLink runs every action command through this wrapper: `entrypoint cmd…`,
# with CWD = the action's materialized input root. Standard OCI Toolchain §12
# finalizes a toolchain's binaries so their PT_INTERP is the ABSOLUTE
# /lib/ld-std-oci-toolchain.so and their glibc closure resolves from an absolute
# /cas — "carried, not baked": the loader + closure travel as action inputs, and
# the worker binds them at those fixed paths just-in-time. Without that bind a
# floor binary cannot exec (kernel can't find its interpreter → exit 127).
#
# This wrapper is CONDITIONAL and additive: if the action carries no floor cell
# (no */lib/ld-std-oci-toolchain.so among its inputs) it exec's the command
# unchanged — so non-floor actions (e.g. the nix_build cxx toolchain) run exactly
# as before. If it does, we bwrap: bind each floor cell's loader → /lib and its
# cas/<blake3> entries → /cas (unioned across cells; BLAKE3-named, so identical
# content across cells is one bind, never a conflict), preserving /nix/store, /etc
# and the writable work dir. Mirrors buck2's local forkserver floor sandbox
# (app/buck2_forkserver/src/service.rs) and its local.rs bind computation.
set -euo pipefail

# Absolute tool paths (the scan runs here, in the entrypoint's own env).
find=@find@
basename=@basename@
bwrap=@bwrap@
# A pinned baseline toolchain (bash/coreutils/findutils/sed/grep/awk) that the
# sandbox puts on PATH — remote actions inherit no usable PATH, and the bwrap
# fresh root drops the worker's ambient /run/current-system, so a bare `sh` in
# the action's command must resolve from here. Prepended, so the action's own
# PATH still wins where it points at bound (e.g. /nix/store) locations.
baseline=@baseline@

# Collect every floor loader among the materialized inputs (CWD = input root).
mapfile -d '' -t loaders < <("$find" . -type f -path '*/lib/ld-std-oci-toolchain.so' -print0 2>/dev/null)

# No floor cell → behave exactly like a bare worker: exec the command unchanged.
if [ "${#loaders[@]}" -eq 0 ]; then
  exec "$@"
fi

# One loader bind (content-identical across cells), then union every cell's /cas.
binds=(--ro-bind "${loaders[0]}" /lib/ld-std-oci-toolchain.so)
declare -A seen
for l in "${loaders[@]}"; do
  cell="${l%/lib/ld-std-oci-toolchain.so}"
  [ -d "$cell/cas" ] || continue
  for e in "$cell"/cas/*; do
    [ -e "$e" ] || continue
    b="$("$basename" "$e")"
    [ -n "${seen[$b]:-}" ] && continue
    seen[$b]=1
    binds+=(--ro-bind "$e" "/cas/$b")
  done
done

# Fresh root (bwrap default) so /lib and /cas can be created as mount points —
# a --dev-bind / cannot. Preserve /nix/store (action scaffolding: sh/coreutils),
# /etc, and the writable work dir; give /dev /proc /tmp; then the floor binds.
exec "$bwrap" \
  --die-with-parent --unshare-user --unshare-pid --unshare-uts --unshare-ipc \
  --dev /dev --proc /proc --tmpfs /tmp \
  --ro-bind /nix/store /nix/store \
  --ro-bind-try /etc /etc \
  --bind "$PWD" "$PWD" \
  "${binds[@]}" \
  --setenv PATH "$baseline:${PATH:-}" \
  --chdir "$PWD" \
  -- "$@"
