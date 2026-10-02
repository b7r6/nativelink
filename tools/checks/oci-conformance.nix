# The official opencontainers/distribution-spec conformance suite run
# against a real nativelink `oci_registry` instance — design 3, oracle 1 of
# design/oci-registry-over-cas.md: never self-validate a wire format (the
# §7 field-4 lesson). All four workflow categories run: Pull, Push, Content
# Discovery, Content Management. Sandboxed: both processes in one
# derivation on loopback, no KVM.
#
# The suite rev is PINNED (v1.1.1); its JUnit report and HTML report are
# the check's artifacts, copied into $out.
{
  buildGoModule,
  curl,
  fetchFromGitHub,
  nativelink,
  runCommand,
}: let
  conformanceSrc = fetchFromGitHub {
    owner = "opencontainers";
    repo = "distribution-spec";
    # v1.1.1
    rev = "a139cc423184af6078077b9b7ee336eddbd03f8f";
    hash = "sha256-cD5/9vwqcgI1ZIbIfnS3xdv806SCK1KCcNe/UYToWWk=";
  };
  # The suite is a Go test package; compile it once into a standalone
  # ginkgo test binary so the check derivation needs no network.
  conformanceTest = buildGoModule {
    pname = "oci-distribution-conformance";
    version = "1.1.1";
    src = conformanceSrc;
    modRoot = "conformance";
    vendorHash = "sha256-OYNnPlWc3IvqGl9L8zO60vaq+2bUtK/uP31cDgXw8u4=";
    env.CGO_ENABLED = 0;
    buildPhase = ''
      runHook preBuild
      go test -c -o conformance.test .
      runHook postBuild
    '';
    installPhase = ''
      runHook preInstall
      install -Dm755 conformance.test $out/bin/conformance.test
      runHook postInstall
    '';
    doCheck = false;
  };
in
  runCommand "oci-conformance" {
    nativeBuildInputs = [nativelink curl conformanceTest];
  } ''
    set -eu
    export HOME="$TMPDIR"
    port=51989

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
                // Content Management is the fourth conformance category.
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
      kill -0 "$server_pid" 2>/dev/null || {
        echo "nativelink exited during startup" >&2
        exit 1
      }
      if curl -fsS "http://127.0.0.1:$port/v2/" >/dev/null 2>&1; then break; fi
      sleep 0.2
    done
    curl -fsS "http://127.0.0.1:$port/v2/" >/dev/null

    export OCI_ROOT_URL="http://127.0.0.1:$port"
    export OCI_NAMESPACE="myorg/myrepo"
    export OCI_CROSSMOUNT_NAMESPACE="myorg/other"
    export OCI_TEST_PULL=1
    export OCI_TEST_PUSH=1
    export OCI_TEST_CONTENT_DISCOVERY=1
    export OCI_TEST_CONTENT_MANAGEMENT=1
    export OCI_DELETE_MANIFEST_BEFORE_BLOBS=1

    mkdir -p "$TMPDIR/report" && cd "$TMPDIR/report"
    conformance.test -test.v 2>&1 | tee "$TMPDIR/report/conformance.log" \
      | grep -E "SUCCESS!|FAIL!|Passed|Failed" | tail -2

    # The suite's own reports are the artifact (design 3): keep them.
    mkdir -p $out
    cp "$TMPDIR"/report/junit.xml $out/ 2>/dev/null || true
    cp "$TMPDIR"/report/report.html $out/ 2>/dev/null || true
    cp "$TMPDIR"/report/conformance.log $out/

    # Falsify the harness itself: the JUnit report must exist and contain
    # zero failures, or a silently-crashed suite would false-pass.
    test -s $out/junit.xml
    grep -q 'failures="0"' $out/junit.xml

    kill "$server_pid"
  ''
