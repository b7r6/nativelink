# The projection differential — design 3, oracle 3 of
# design/oci-registry-over-cas.md: push a toolchain-shaped image to a live
# `oci_registry` instance, then run `FetchDirectory` over BOTH acquisition
# paths — the network client (`oci://127.0.0.1:<port>/...`) and the local
# `oci://self/...` short-circuit — and demand the SAME REAPI root Directory
# digest. The two paths share the projection but not the acquisition code;
# divergence means the registry served different bytes than it accepted.
{
  cacert,
  curl,
  grpcurl,
  nativelink,
  python3,
  runCommand,
  skopeo,
}:
runCommand "oci-projection-differential" {
  nativeBuildInputs = [nativelink skopeo grpcurl curl python3];
} ''
  set -eu
  export HOME="$TMPDIR"
  # The fetch service's OCI client builds a TLS-capable HTTP client even
  # for plaintext loopback registries; give the sandbox real CA roots.
  export SSL_CERT_FILE="${cacert}/etc/ssl/certs/ca-bundle.crt"
  port=51990

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
      {
        name: "REAPI_CAS",
        filesystem: {
          content_path: "$TMPDIR/data/cas/content",
          temp_path: "$TMPDIR/data/cas/temp",
          eviction_policy: {max_bytes: 1000000000},
        },
      },
    ],
    servers: [
      {
        name: "main",
        listener: {http: {socket_address: "127.0.0.1:$port"}},
        services: {
          cas: [{instance_name: "main", cas_store: "REAPI_CAS"}],
          bytestream: [{instance_name: "main", cas_store: "REAPI_CAS"}],
          fetch: [
            {
              instance_name: "main",
              fetch_store: "REAPI_CAS",
              oci: {
                cas_store: "REAPI_CAS",
                digest_function: "BLAKE3",
                registries: [{host: "127.0.0.1:$port", scheme: "http"}],
                self_registry: {
                  blob_store: "OCI_BLOB_STORE",
                  index_store: "OCI_INDEX_STORE",
                  ref_store: "OCI_REF_STORE",
                },
              },
            },
          ],
          oci_registry: [
            {
              instance_name: "main",
              cas_store: "OCI_BLOB_STORE",
              index_store: "OCI_INDEX_STORE",
              ref_store: "OCI_REF_STORE",
              digest_function: "BLAKE3",
              spool_path: "$TMPDIR/data/spool",
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
    kill -0 "$server_pid" 2>/dev/null || {
      echo "nativelink exited during startup" >&2
      exit 1
    }
    if curl -fsS "http://127.0.0.1:$port/v2/" >/dev/null 2>&1; then break; fi
    sleep 0.2
  done
  curl -fsS "http://127.0.0.1:$port/v2/" >/dev/null

  skopeo --insecure-policy copy --dest-tls-verify=false \
    "oci:$TMPDIR/layout:v1" "docker://127.0.0.1:$port/diff/tc:v1"

  run_fetch() {
    grpcurl -plaintext \
      -import-path ${../../nativelink-proto} \
      -proto build/bazel/remote/asset/v1/remote_asset.proto \
      -d "{\"instance_name\":\"main\",\"uris\":[\"$1\"],\"digest_function\":\"BLAKE3\"}" \
      "127.0.0.1:$port" build.bazel.remote.asset.v1.Fetch/FetchDirectory
  }
  extract_digest() {
    python3 -c 'import sys, json; d = json.load(sys.stdin)["rootDirectoryDigest"]; print(d["hash"], d.get("sizeBytes", 0))'
  }

  net=$(run_fetch "oci://127.0.0.1:$port/diff/tc:v1" | extract_digest)
  self=$(run_fetch "oci://self/diff/tc:v1" | extract_digest)
  echo "network path root digest: $net"
  echo "self path root digest:    $self"
  [ -n "$net" ]
  [ "$net" = "$self" ]
  echo "projection differential: identical REAPI root digest via both paths"

  # Falsifier: a never-pushed reference must FAIL through the self path,
  # or digest agreement proves nothing about real resolution.
  if run_fetch "oci://self/diff/tc:never-pushed" 2>/dev/null; then
    echo "oci://self resolved a tag that was never pushed" >&2
    exit 1
  fi

  kill -0 "$server_pid" 2>/dev/null || {
    echo "nativelink died mid-check" >&2
    exit 1
  }
  kill "$server_pid"
  touch $out
''
