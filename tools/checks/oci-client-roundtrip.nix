# Independent-client round trips against a live `oci_registry` instance —
# design 3, oracle 2 of design/oci-registry-over-cas.md: skopeo (monolithic
# PUTs) and crane (a second client lineage with different auth flow and
# chunking behavior) each push→pull byte-identical through the CAS-backed
# registry. Runs entirely in the nix build sandbox on loopback.
{
  crane,
  curl,
  nativelink,
  python3,
  runCommand,
  skopeo,
}:
runCommand "oci-client-roundtrip" {
  nativeBuildInputs = [nativelink skopeo crane curl python3];
} ''
  set -eu
  export HOME="$TMPDIR"
  port=51987

  # A small but structurally real OCI layout: gzipped tar layer, config,
  # manifest, index.
  python3 ${./make-oci-layout.py} "$TMPDIR/layout"

  cat > "$TMPDIR/config.json5" <<EOF
  {
    stores: [
      {
        name: "OCI_BLOB_STORE",
        verify: {
          verify_size: true,
          verify_hash: true,
          backend: {
            filesystem: {
              content_path: "$TMPDIR/data/blobs/content",
              temp_path: "$TMPDIR/data/blobs/temp",
              eviction_policy: {max_bytes: 1000000000},
            },
          },
        },
      },
      {
        name: "OCI_INDEX_STORE",
        completeness_checking: {
          backend: {
            filesystem: {
              content_path: "$TMPDIR/data/index/content",
              temp_path: "$TMPDIR/data/index/temp",
              eviction_policy: {max_bytes: 100000000},
            },
          },
          cas_store: {ref_store: {name: "OCI_BLOB_STORE"}},
        },
      },
      {
        name: "OCI_REF_STORE",
        filesystem: {
          content_path: "$TMPDIR/data/refs/content",
          temp_path: "$TMPDIR/data/refs/temp",
          eviction_policy: {max_bytes: 100000000},
        },
      },
    ],
    servers: [
      {
        name: "main",
        listener: {http: {socket_address: "127.0.0.1:$port"}},
        services: {
          oci_registry: [
            {
              instance_name: "main",
              cas_store: "OCI_BLOB_STORE",
              index_store: "OCI_INDEX_STORE",
              ref_store: "OCI_REF_STORE",
              digest_function: "BLAKE3",
              spool_path: "$TMPDIR/data/spool",
              enable_delete: true,
            },
          ],
        },
      },
    ],
  }
  EOF

  nativelink "$TMPDIR/config.json5" &
  server_pid=$!
  trap 'kill "$server_pid" 2>/dev/null || true' EXIT
  for _ in $(seq 1 100); do
    # The server must both be ALIVE and answering — a dead server plus a
    # stale listener on a shared loopback must fail, not false-pass.
    kill -0 "$server_pid" 2>/dev/null || {
      echo "nativelink exited during startup" >&2
      exit 1
    }
    if curl -fsS "http://127.0.0.1:$port/v2/" >/dev/null 2>&1; then break; fi
    sleep 0.2
  done
  curl -fsS "http://127.0.0.1:$port/v2/" >/dev/null

  # ---- skopeo: push, pull, byte-identical diff. --------------------------
  skopeo --insecure-policy copy --dest-tls-verify=false \
    "oci:$TMPDIR/layout:v1" "docker://127.0.0.1:$port/rt/skopeo:v1"
  skopeo --insecure-policy copy --src-tls-verify=false \
    "docker://127.0.0.1:$port/rt/skopeo:v1" "oci:$TMPDIR/skopeo-out:v1"
  for f in "$TMPDIR"/layout/blobs/sha256/*; do
    cmp "$f" "$TMPDIR/skopeo-out/blobs/sha256/$(basename "$f")"
  done
  echo "skopeo round trip: byte-identical"

  # ---- crane: second lineage — copy, digest agreement, tags, pull. -------
  crane copy --insecure \
    "127.0.0.1:$port/rt/skopeo:v1" "127.0.0.1:$port/rt/crane:v1"
  d1=$(crane digest --insecure "127.0.0.1:$port/rt/skopeo:v1")
  d2=$(crane digest --insecure "127.0.0.1:$port/rt/crane:v1")
  [ "$d1" = "$d2" ]
  crane ls --insecure "127.0.0.1:$port/rt/crane" | grep -qx "v1"
  crane pull --insecure --format=oci \
    "127.0.0.1:$port/rt/crane:v1" "$TMPDIR/crane-out"
  for f in "$TMPDIR"/layout/blobs/sha256/*; do
    cmp "$f" "$TMPDIR/crane-out/blobs/sha256/$(basename "$f")"
  done
  echo "crane round trip: byte-identical"

  # Falsifier: a pull of a tag nobody pushed must FAIL, or the diffs above
  # prove nothing about serving real content.
  if skopeo --insecure-policy copy --src-tls-verify=false \
    "docker://127.0.0.1:$port/rt/skopeo:never-pushed" "oci:$TMPDIR/none:v1" 2>/dev/null; then
    echo "registry served a tag that was never pushed" >&2
    exit 1
  fi

  kill -0 "$server_pid" 2>/dev/null || {
    echo "nativelink died mid-check" >&2
    exit 1
  }
  kill "$server_pid"
  touch $out
''
