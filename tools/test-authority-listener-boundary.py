#!/usr/bin/env python3
"""Exercise fixed listener trust and storage separation on a local real binary.

Requires openssl and grpcurl. Creates ephemeral CAs, keys, memory stores and
loopback listeners; terminates only its own server children. This is a declared
local topology experiment, not a deployed fleet or coordinator qualification.
"""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time


REAPI = "build.bazel.remote.execution.v2."
METHOD_PROTO = "build/bazel/remote/execution/v2/remote_execution.proto"
BYTE_PROTO = "google/bytestream/bytestream.proto"


def checked(command):
    result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise RuntimeError(f"Command failed: {command[0]}: {result.stderr}")
    return result.stdout.strip()


def certificates(directory):
    for role in ("server", "client", "worker"):
        prefix = directory / role
        checked([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-noenc",
            "-sha256", "-days", "2", "-subj", f"/CN=Disposable {role} test CA",
            "-addext", "basicConstraints=critical,CA:TRUE",
            "-addext", "keyUsage=critical,keyCertSign,cRLSign",
            "-keyout", f"{prefix}-ca.key", "-out", f"{prefix}-ca.pem",
        ])
        checked([
            "openssl", "req", "-new", "-newkey", "rsa:2048", "-noenc",
            "-subj", "/CN=localhost", "-keyout", f"{prefix}.key",
            "-out", f"{prefix}.csr",
        ])
        extensions = directory / f"{role}.ext"
        extensions.write_text(
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            f"extendedKeyUsage={'serverAuth' if role == 'server' else 'clientAuth'}\n"
            "subjectAltName=DNS:localhost,IP:127.0.0.1\n"
        )
        checked([
            "openssl", "x509", "-req", "-in", f"{prefix}.csr", "-sha256",
            "-days", "2", "-CA", f"{prefix}-ca.pem", "-CAkey", f"{prefix}-ca.key",
            "-CAcreateserial", "-extfile", str(extensions), "-out", f"{prefix}.pem",
        ])
    (directory / "shared-ca.pem").write_bytes(
        (directory / "client-ca.pem").read_bytes() + (directory / "worker-ca.pem").read_bytes()
    )


def free_ports():
    sockets = []
    try:
        for _ in range(3):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def config(directory, ports, mode):
    cas = "sink" if mode == "aliased-store" else "objects"
    worker_services = {
        "cas": [{"instance_name": "", "cas_store": cas}],
        "bytestream": [{"instance_name": "", "cas_store": cas}],
    }
    client_services = {"ac": [{"instance_name": "", "ac_store": "sink"}]}
    if mode == "worker-ac-route":
        worker_services.update(client_services)
    servers = []
    for index, port in enumerate(ports):
        trust = "worker" if index == 0 else ("shared" if mode == "shared-trust" else "client")
        servers.append({
            "name": f"boundary-{index}",
            "listener": {"http": {
                "socket_address": f"127.0.0.1:{port}",
                "tls": {
                    "cert_file": str(directory / "server.pem"),
                    "key_file": str(directory / "server.key"),
                    "client_ca_file": str(directory / f"{trust}-ca.pem"),
                },
            }},
            "services": worker_services if index == 0 else client_services,
        })
    return {"stores": [{"name": name, "memory": {}} for name in ("objects", "sink")],
            "servers": servers}


def exercise(root, binary, directory, output, mode):
    ports = free_ports()
    configuration = directory / f"{mode}.json5"
    configuration.write_text(json.dumps(config(directory, ports, mode)))
    calls = []

    def rpc(role, index, method, body, proto=METHOD_PROTO, record=True):
        command = [
            "grpcurl", "-max-time", "5", "-cacert", str(directory / "server-ca.pem"),
            "-cert", str(directory / f"{role}.pem"), "-key", str(directory / f"{role}.key"),
            "-import-path", str(root / "nativelink-proto"), "-proto", proto,
            "-d", json.dumps(body), f"127.0.0.1:{ports[index]}", method,
        ]
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)
        if record:
            calls.append({"role": role, "listener": index, "method": method,
                          "exit_code": result.returncode, "stdout": result.stdout,
                          "stderr": result.stderr})
        return result

    # This is a valid encoded ActionResult(exit_code=17), and the same bytes are
    # valid CAS/ByteStream uploads. Matching digest/size removes malformed-input
    # rejection as an explanation for an unchanged AC head.
    payload = bytes([0x20, 17])
    digest = {"hash": hashlib.sha256(payload).hexdigest(), "size_bytes": len(payload)}
    get = {"action_digest": digest, "digest_function": "SHA256"}

    def update(role, index, code):
        return rpc(role, index, REAPI + "ActionCache/UpdateActionResult",
                   {**get, "action_result": {"exit_code": code}})

    def exit_code():
        result = rpc("client", 1, REAPI + "ActionCache/GetActionResult", get)
        assert result.returncode == 0, result.stderr
        return int(json.loads(result.stdout).get("exitCode", 0))

    violations = []
    log = output / f"server-{mode}.log"
    with log.open("w") as server_log:
        process = subprocess.Popen([str(binary), str(configuration)],
                                   stdout=server_log, stderr=subprocess.STDOUT,
                                   env=dict(os.environ, RUST_LOG="warn"))
        try:
            deadline = time.monotonic() + 20
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"Server exited early: {log.read_text()}")
                result = rpc("client", 1, REAPI + "ActionCache/GetActionResult", get, record=False)
                if "NotFound" in result.stderr:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"Listener readiness failed: {result.stderr}; {log.read_text()}")
                time.sleep(0.05)
            # Every writable client route has a positive control before denial.
            for index in (1, 2):
                result = update("client", index, 7 + index)
                assert result.returncode == 0, result.stderr
                assert exit_code() == 7 + index
                result = update("worker", index, 33)
                if result.returncode == 0 or exit_code() != 7 + index:
                    violations.append(f"worker-can-write-client-route-{index}")
            assert update("client", 1, 7).returncode == 0
            result = update("worker", 0, 33)
            if result.returncode == 0 or exit_code() != 7:
                violations.append("worker-listener-serves-ac")
            assert update("client", 1, 7).returncode == 0
            result = rpc("worker", 0, REAPI + "ContentAddressableStorage/BatchUpdateBlobs", {
                "digest_function": "SHA256",
                "requests": [{"digest": digest, "data": base64.b64encode(payload).decode()}],
            })
            assert result.returncode == 0, result.stderr
            response = json.loads(result.stdout)["responses"][0]
            assert int(response.get("status", {}).get("code", 0)) == 0, response
            if exit_code() != 7:
                violations.append("cas-upload-mutates-ac")
            assert update("client", 1, 7).returncode == 0
            result = rpc("worker", 0, "google.bytestream.ByteStream/Write", {
                "resource_name": f"uploads/boundary/blobs/{digest['hash']}/{len(payload)}",
                "write_offset": 0, "finish_write": True,
                "data": base64.b64encode(payload).decode(),
            }, proto=BYTE_PROTO)
            assert result.returncode == 0, result.stderr
            assert int(json.loads(result.stdout)["committedSize"]) == len(payload)
            if exit_code() != 7:
                violations.append("bytestream-upload-mutates-ac")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
    return {"topology": mode, "violations": violations, "calls": calls,
            "server_log": log.name}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    binary = args.binary.resolve(strict=True)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    for tool in ("openssl", "grpcurl"):
        if shutil.which(tool) is None:
            raise SystemExit(f"Missing required tool: {tool}")
    report = {
        "scope": "Local real legacy listener/TLS stack only; no authority coordinator or deployed boundary claim.",
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "openssl": checked(["openssl", "version"]),
        "topologies": [],
    }
    try:
        with tempfile.TemporaryDirectory(prefix="nativelink-listener-boundary-") as temporary:
            directory = Path(temporary)
            certificates(directory)
            for mode in ("isolated", "shared-trust", "aliased-store", "worker-ac-route"):
                result = exercise(root, binary, directory, output, mode)
                report["topologies"].append(result)
                print(json.dumps({"topology": mode, "violations": result["violations"]}), flush=True)
                expected = {
                    "isolated": [],
                    "shared-trust": ["worker-can-write-client-route-1", "worker-can-write-client-route-2"],
                    "aliased-store": ["cas-upload-mutates-ac", "bytestream-upload-mutates-ac"],
                    "worker-ac-route": ["worker-listener-serves-ac"],
                }[mode]
                assert result["violations"] == expected, (mode, result["violations"])
        report["passed"] = True
    finally:
        (output / "manifest.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
