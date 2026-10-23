#!/usr/bin/env python3
"""Qualify the actual fixed authority listeners with disposable local mTLS.

Run an already-built authority_listener_test binary. OpenSSL creates temporary
identities using the existing listener-boundary certificate generator. Only
public certificate hashes, source/binary hashes, RPC observations and test output
are retained. This does not qualify any deployed endpoint or credential setup.
"""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


TEST_NAME = "tls::fixed_authority_mtls_boundary"


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def certificate_generator(root):
    path = root / "tools/test-authority-listener-boundary.py"
    spec = importlib.util.spec_from_file_location("authority_listener_certificates", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load existing certificate generator")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def redact_keys(text, private_keys):
    for key in private_keys:
        text = text.replace(key, "[REDACTED PRIVATE KEY]")
        for line in key.splitlines():
            if line and not line.startswith("-----"):
                text = text.replace(line, "[REDACTED PRIVATE KEY LINE]")
    return text


def validate_observations(report):
    if report.get("passed") is not True:
        raise RuntimeError("Rust fixture did not report success")
    observations = report.get("observations", [])
    expected = {
        (topology, algorithm)
        for topology in ("isolated_trust", "shared_trust_negative_control")
        for algorithm in ("SHA256", "BLAKE3")
    }
    actual = {(item["topology"], item["digest_algorithm"]) for item in observations}
    if len(observations) != 4 or actual != expected:
        raise RuntimeError("Fixture did not exercise all four declared cases")
    for item in observations:
        unsafe = item["topology"] == "shared_trust_negative_control"
        if item["client_listener_count"] != 1 or item["client_listeners_probed"] != [0]:
            raise RuntimeError("Not every declared client listener was probed")
        if item["worker_client_route_write_admitted"] != unsafe:
            raise RuntimeError("Worker credential admission did not match the topology")
        if item["classification"] != ("unsafe" if unsafe else "isolated_in_this_fixture"):
            raise RuntimeError("Unsafe topology was not classified explicitly")
        for assertion in (
            "authorized_client_write_before_and_after",
            "worker_cas_upload_succeeded",
            "worker_bytestream_upload_succeeded",
            "client_bytestream_roundtrip_succeeded",
            "same_key_object_uploads_preserved_sink",
        ):
            if item[assertion] is not True:
                raise RuntimeError(f"Required positive control failed: {assertion}")
        if item["accepted_publication_and_cache_hit_exercised"] != (not unsafe):
            raise RuntimeError("Safe topology did not exercise the authority pipeline")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    binary = args.test_binary.resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise SystemExit("--test-binary must identify an executable Rust integration test")
    if shutil.which("openssl") is None:
        raise SystemExit("Missing required tool: openssl")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    for name in ("manifest.json", "observations.json", "test.log"):
        if (output / name).exists():
            raise SystemExit(f"Refusing to overwrite prior evidence: {output / name}")
    generator = certificate_generator(root)
    sources = [
        "tools/test-authority-listener-tls.py",
        "tools/test-authority-listener-boundary.py",
        "nativelink-service/tests/authority_listener_test.rs",
        "nativelink-service/tests/authority_listener/tls.rs",
        "nativelink-service/src/authority_listener.rs",
        "nativelink-service/src/authority_server.rs",
        "nativelink-service/src/capabilities_server.rs",
        "Cargo.lock",
    ]
    report = {
        "schema_version": 1,
        "scope": "Actual fixed authority listener stack; temporary local identities and stores; no deployment claim",
        "source_sha256": {name: sha256(root / name) for name in sources},
        "test_binary_sha256": sha256(binary),
        "build_provenance": "Build command and source-to-binary provenance must accompany this execution receipt",
        "openssl": generator.checked(["openssl", "version"]),
        "test_name": TEST_NAME,
        "passed": False,
        "private_keys_archived": False,
    }
    command = [str(binary), "--ignored", "--exact", TEST_NAME, "--test-threads=1"]
    report["command"] = command
    temporary_path = None
    private_keys = []
    previous_umask = os.umask(0o077)
    try:
        with tempfile.TemporaryDirectory(prefix="nativelink-authority-mtls-") as temporary:
            directory = Path(temporary)
            temporary_path = directory
            generator.certificates(directory)
            private_keys = [path.read_text() for path in directory.glob("*.key")]
            report["public_certificate_sha256"] = {}
            for path in directory.glob("*.pem"):
                if b"PRIVATE KEY" in path.read_bytes():
                    raise RuntimeError("A public certificate file unexpectedly contains a private key")
                report["public_certificate_sha256"][path.name] = sha256(path)
            result = subprocess.run(
                command,
                cwd=root,
                env=dict(
                    os.environ,
                    NL_AUTHORITY_LISTENER_CERT_DIR=str(directory),
                    NL_AUTHORITY_LISTENER_TLS_REPORT=str(output / "observations.json"),
                ),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=120,
                check=False,
            )
            log = redact_keys(result.stdout, private_keys)
            (output / "test.log").write_text(log)
            report["test_exit_code"] = result.returncode
            if result.returncode != 0:
                raise RuntimeError("Explicit TLS qualification failed; see test.log")
            if "test result: ok. 1 passed; 0 failed; 0 ignored;" not in log:
                raise RuntimeError("The explicitly selected test did not execute exactly once")
            observations = json.loads((output / "observations.json").read_text())
            validate_observations(observations)
            report["observations_sha256"] = sha256(output / "observations.json")
            report["test_log_sha256"] = sha256(output / "test.log")
        report["passed"] = True
    except Exception as error:
        report["error"] = redact_keys(str(error), private_keys)
        raise
    finally:
        os.umask(previous_umask)
        report["temporary_certificate_directory_removed"] = (
            temporary_path is not None and not temporary_path.exists()
        )
        (output / "manifest.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"passed": True, "cases": 4, "manifest": str(output / "manifest.json")}))


if __name__ == "__main__":
    main()
