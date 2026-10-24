#!/usr/bin/env python3
"""Check coordinator wire-fence fault detection in an isolated source copy.

These mutations test named RPC identity, progress, and payload assertions.
They do not certify deployed authentication or throughput. Compiler errors and unmatched filters are not kills.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


SOURCE = Path("nativelink-service/src/authority_server.rs")
TEST = Path("nativelink-worker/tests/authority_rpc_test.rs")
WIRE_TEST = "wire_invalid_reports_and_cancel_fences_cannot_accept_or_refund_work"
MUTANTS = [
    (
        "accept_wrong_dispatch_identity",
        "if !dispatch_matches && !cancel_matches {",
        "if false && !dispatch_matches && !cancel_matches {",
        WIRE_TEST,
    ),
    (
        "accept_zero_observation_sequence",
        "|| report.observation_sequence == 0",
        "|| (false && report.observation_sequence == 0)",
        WIRE_TEST,
    ),
    (
        "accept_wrong_cancel_identity",
        "effect.id.0 == report.cancel_effect_id && matches!",
        "(effect.id.0 == report.cancel_effect_id || true) && matches!",
        WIRE_TEST,
    ),
    (
        "skip_committed_payload_bound",
        "serde_json::to_vec(&captured).ok()?.len() <= FRAME_BYTES",
        "serde_json::to_vec(&captured).ok()?.len() <= FRAME_BYTES || true",
        WIRE_TEST,
    ),
]


def run(checkout, evidence, name, test, target):
    command = [
        "cargo", "test", "--locked", "--offline", "--profile", "smol",
        "-p", "nativelink-worker", "--test", "authority_rpc_test", "-j", "24",
    ]
    if test:
        command.extend([test, "--", "--exact"])
    started = time.monotonic()
    log = evidence / f"{name}.txt"
    with log.open("w") as output:
        result = subprocess.run(
            command, cwd=checkout,
            env=dict(os.environ, CARGO_TARGET_DIR=str(target)),
            stdout=output, stderr=subprocess.STDOUT,
        )
    contents = log.read_text()
    executed = "Running tests/authority_rpc_test.rs" in contents
    failed = bool(test) and f"test {test} ... FAILED" in contents
    return {
        "name": name, "command": command, "exit_code": result.returncode,
        "seconds": time.monotonic() - started, "executed": executed,
        "selected_test_failed": failed, "log": log.name,
        "sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target-dir", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    evidence = args.output.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    target = (args.target_dir or root / "target/categorical-rpc-mutations").resolve()
    original = (root / SOURCE).read_text()
    for _, anchor, _, _ in MUTANTS:
        if original.count(anchor) != 1:
            raise SystemExit(f"Expected one mutation anchor: {anchor!r}")
    report = {
        "repository_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True,
        ).strip(),
        "working_tree_included": True,
        "scope": "Four semantic faults at the actual RPC adapter; no deployed authentication, physical process closure, or throughput qualification.",
        "source_sha256": {}, "mutants": [],
    }
    with tempfile.TemporaryDirectory(prefix="nativelink-rpc-mutants-") as temp:
        checkout = Path(temp)
        files = subprocess.check_output(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=root,
        ).decode().split("\0")
        for relative in sorted(set(files) - {""}):
            source = root / relative
            if not source.is_file() or source.is_symlink():
                continue
            destination = checkout / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            if relative in ("Cargo.toml", "Cargo.lock", str(SOURCE), str(TEST)):
                report["source_sha256"][relative] = hashlib.sha256(source.read_bytes()).hexdigest()
        baseline = run(checkout, evidence, "baseline", WIRE_TEST, target)
        report["baseline"] = baseline
        if baseline["exit_code"] == 0 and baseline["executed"]:
            for name, anchor, replacement, test in MUTANTS:
                (checkout / SOURCE).write_text(original.replace(anchor, replacement, 1))
                outcome = run(checkout, evidence, name, test, target)
                outcome["killed"] = (
                    outcome["exit_code"] != 0 and outcome["executed"]
                    and outcome["selected_test_failed"]
                )
                report["mutants"].append(outcome)
                print(json.dumps({"name": name, "killed": outcome["killed"]}), flush=True)
        report["all_killed"] = len(report["mutants"]) == len(MUTANTS) and all(
            outcome["killed"] for outcome in report["mutants"]
        )
        report["runner_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        (evidence / "mutations.json").write_text(json.dumps(report, indent=2) + "\n")
        raise SystemExit(0 if report["all_killed"] else 1)


if __name__ == "__main__":
    main()
