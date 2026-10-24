#!/usr/bin/env python3
"""Qualify six accepted-history delivery faults in an isolated source snapshot.

Each mutant must compile and fail its exact named test at the expected assertion.
Compilation failures, process failures, signals and runner timeouts never count
as semantic detection. Three RPC cases use their bounded sink-observation panic;
these are test assertions about delivery, not a production latency claim.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time


SERVICE = "nativelink-service/src/authority_server.rs"
UNIT = "nativelink-service/src/history_delivery_tests.rs"
RPC = "nativelink-worker/tests/authority_publication_ordering_test.rs"
CASES = "nativelink-worker/tests/publication_history/cases.rs"
OWNED_SOURCES = [SERVICE, UNIT, RPC, CASES]
FAIRNESS = (
    "authority_server::history_delivery_tests::"
    "due_history_retries_share_four_slots_and_cannot_starve_fresh_ac"
)
RESTART = (
    "history::accepted_history_retries_exact_bytes_after_restart_and_lost_ack_"
    "without_changing_terminal"
)
FAILED = (
    "history::failed_history_delivery_keeps_original_association_pending_until_exact_retry"
)
NAMESPACE = (
    "history::history_service_refuses_a_different_logical_cas_namespace_before_delivery"
)
MUTANTS = [
    (
        "force_sha256_write_context",
        "HistoryAlgorithm::Blake3 => DigestHasherFunc::Blake3,",
        "HistoryAlgorithm::Blake3 => DigestHasherFunc::Sha256,",
        "rpc", RESTART, "bounded_sink_observation",
    ),
    (
        "route_history_to_ac_sink",
        "self.config.cas.clone(),\n                        StorageWork::History",
        "self.config.publication_sink.clone(),\n                        StorageWork::History",
        "rpc", RESTART, "bounded_sink_observation",
    ),
    (
        "acknowledge_failed_cas_write",
        ".await\n                            .map_err(|error| error.to_string())?;\n"
        "                            Ok(Event::HistoryPublication {",
        ".await\n                            .unwrap_or_default();\n"
        "                            Ok(Event::HistoryPublication {",
        "rpc", FAILED, "bounded_sink_observation",
    ),
    (
        "skip_startup_namespace_guard",
        "if initial\n            .accepted_history_namespace()",
        "if false && initial\n            .accepted_history_namespace()",
        "rpc", NAMESPACE, "namespace_assertion",
    ),
    (
        "omit_storage_rotation",
        "storage.rotate_left(split);",
        "let _ = split;",
        "unit", FAIRNESS, "fairness_assertion",
    ),
    (
        "omit_shared_slot_bound",
        "                || self.in_flight.len() >= 4\n",
        "",
        "unit", FAIRNESS, "slot_bound_assertion",
    ),
]


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def passing_tests(log):
    return set(re.findall(r"^test (\S+) \.\.\. ok$", log, re.MULTILINE))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target-dir", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    target = args.target_dir.resolve()
    original = (root / SERVICE).read_text()
    for name, before, *_ in MUTANTS:
        if original.count(before) != 1:
            raise ValueError(f"{name}: mutation requires one unique source anchor")

    # Pin each expected panic to its actual source assertion, not an unrelated
    # failure in the same test. No source line numbers are hard-coded.
    def assertion_location(relative, needle, offset=0):
        source = (root / relative).read_text()
        if source.count(needle) != 1:
            raise ValueError(f"assertion anchor is not unique: {relative}: {needle}")
        line = source[:source.index(needle)].count("\n") + 1 + offset
        return f"{relative}:{line}:"

    expected_panics = {
        "bounded_sink_observation": (
            assertion_location(RPC, "async fn next_write(&mut self)", 3),
            "called `Result::unwrap()` on an `Err` value: Elapsed(())",
        ),
        "namespace_assertion": (
            assertion_location(CASES, "assert!(matches!(result, Err(ref error)"),
            "assertion failed:",
        ),
        "fairness_assertion": (
            assertion_location(UNIT, "driver.in_flight.contains(&publication_id),", -1),
            "fresh AC delivery must advance despite every old history retry being due",
        ),
        "slot_bound_assertion": (
            assertion_location(UNIT, "assert_eq!(driver.in_flight, older);"),
            "assertion `left == right` failed",
        ),
    }
    base = ["cargo", "test", "--locked", "--offline", "--profile", "smol", "-j", "8"]
    commands = {
        "unit": base + ["-p", "nativelink-service", "--lib"],
        "rpc": base + ["-p", "nativelink-worker", "--test", "authority_publication_ordering_test"],
    }
    report = {
        "scope": "Six compiled delivery faults; logical CAS fixture, not provider or production qualification",
        "base_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip(),
        "runner_sha256": sha256(Path(__file__).read_bytes()),
        "source_snapshot_sha256": {},
        "restored_snapshot_sha256": {},
        "original_owned_source_final_sha256": {},
        "commands": [],
        "mutants": [],
        "expected_panics": expected_panics,
    }

    def save():
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")

    with tempfile.TemporaryDirectory(prefix="nativelink-history-service-mutants-") as temporary:
        checkout = Path(temporary)
        paths = subprocess.check_output(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=root,
        ).decode().split("\0")
        for relative in sorted(set(paths) - {""}):
            source = root / relative
            if not source.is_file() or source.is_symlink():
                continue
            destination = checkout / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            # New mtimes prevent Cargo from reusing a previous mutant build.
            shutil.copyfile(source, destination)
            if source.suffix == ".rs" or source.name in {"Cargo.toml", "Cargo.lock"}:
                report["source_snapshot_sha256"][relative] = sha256(destination.read_bytes())
        for relative in OWNED_SOURCES + ["tools/test-history-service-mutations.py"]:
            destination = output / "original" / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(checkout / relative, destination)
        save()
        print(json.dumps({"snapshot_captured": str(checkout),
                          "source_count": len(report["source_snapshot_sha256"])}), flush=True)

        def run(name, argv, timeout=900):
            log = output / f"{name}.log"
            started = time.monotonic()
            try:
                with log.open("wb") as stream:
                    result = subprocess.run(
                        argv, cwd=checkout,
                        env=dict(os.environ, CARGO_TARGET_DIR=str(target), CARGO_BUILD_JOBS="8"),
                        stdout=stream, stderr=subprocess.STDOUT,
                        timeout=timeout, check=False,
                    )
                status = result.returncode
            except subprocess.TimeoutExpired:
                status = "timeout"
            row = {
                "name": name, "argv": argv, "cwd": str(checkout),
                "CARGO_TARGET_DIR": str(target), "CARGO_BUILD_JOBS": 8,
                "status": status, "seconds": time.monotonic() - started,
                "log": log.name, "sha256": sha256(log.read_bytes()),
            }
            report["commands"].append(row)
            save()
            return status, log.read_text(errors="replace")

        def full_suite(stage):
            results = {}
            for kind, count in [("unit", 5), ("rpc", 8)]:
                status, log = run(f"{stage}-{kind}", commands[kind])
                names = passing_tests(log)
                required = {row[4] for row in MUTANTS if row[3] == kind}
                results[kind] = {
                    "passed": status == 0 and len(names) == count and required <= names,
                    "tests": sorted(names),
                }
            report[stage] = results
            save()
            return all(result["passed"] for result in results.values())

        try:
            if not full_suite("baseline"):
                raise RuntimeError("baseline must pass all five service and eight RPC tests")
            for name, before, after, kind, test, panic in MUTANTS:
                mutated = original.replace(before, after, 1)
                (checkout / SERVICE).write_text(mutated)
                mutant_path = output / "mutant-sources" / f"{name}.rs"
                mutant_path.parent.mkdir(exist_ok=True)
                mutant_path.write_text(mutated)
                row = {
                    "name": name, "source": SERVICE, "test": test,
                    "before": before, "after": after,
                    "mutant_sha256": sha256(mutated.encode()),
                    "expected_panic": panic,
                }
                status, _ = run(name + "-compile", commands[kind] + ["--no-run"])
                if status != 0:
                    row["classification"] = "compile_error" if status == 101 else "process_error"
                else:
                    status, log = run(
                        name + "-test", commands[kind] + [test, "--", "--exact"], 120
                    )
                    location, message = expected_panics[panic]
                    expected = (
                        f"test {test} ... FAILED" in log
                        and re.search(rf"thread '{re.escape(test)}'.*panicked at {re.escape(location)}", log)
                        and message in log
                        and "test result: FAILED. 0 passed; 1 failed;" in log
                    )
                    if status == 0:
                        row["classification"] = "survived"
                    elif status == 101 and expected:
                        row["classification"] = "semantic_kill"
                    else:
                        row["classification"] = "unexpected_test_failure" if status == 101 else "process_error"
                report["mutants"].append(row)
                save()
                print(json.dumps({"name": name, "classification": row["classification"]}), flush=True)
        finally:
            (checkout / SERVICE).write_text(original)
            report["restored_passed"] = full_suite("restored")
            for relative in report["source_snapshot_sha256"]:
                report["restored_snapshot_sha256"][relative] = sha256((checkout / relative).read_bytes())
            report["snapshot_restored_exactly"] = (
                report["source_snapshot_sha256"] == report["restored_snapshot_sha256"]
            )
            clippy = [
                "cargo", "clippy", "--locked", "--offline", "--profile", "smol", "-j", "8",
                "-p", "nativelink-service", "--lib", "-p", "nativelink-worker",
                "--test", "authority_publication_ordering_test", "--", "-D", "warnings",
            ]
            status, _ = run("restored-clippy-libs-rpc", clippy)
            report["strict_clippy_libs_rpc_passed"] = status == 0
            status, _ = run("restored-clippy-unit", [
                "cargo", "clippy", "--locked", "--offline", "--profile", "smol", "-j", "8",
                "-p", "nativelink-service", "--tests", "--", "-D", "warnings",
            ])
            report["strict_clippy_service_tests_passed"] = status == 0
            # The caller may integrate unrelated authority work concurrently.
            # Audit only this campaign's service/fixture ownership in the live
            # tree, after all commands; the isolated snapshot retains its full
            # before/after hash comparison above.
            for relative in OWNED_SOURCES:
                report["original_owned_source_final_sha256"][relative] = sha256(
                    (root / relative).read_bytes()
                )
            report["original_owned_source_unchanged"] = all(
                report["source_snapshot_sha256"][relative] == checksum
                for relative, checksum in report["original_owned_source_final_sha256"].items()
            )
            report["all_killed"] = (
                all(row["passed"] for row in report.get("baseline", {}).values())
                and bool(report.get("baseline")) and report["restored_passed"]
                and report["snapshot_restored_exactly"]
                and len(report["mutants"]) == len(MUTANTS)
                and all(row["classification"] == "semantic_kill" for row in report["mutants"])
            )
            save()
    print(json.dumps({key: report[key] for key in [
        "all_killed", "snapshot_restored_exactly", "original_owned_source_unchanged",
        "strict_clippy_libs_rpc_passed", "strict_clippy_service_tests_passed",
    ]}), flush=True)
    raise SystemExit(0 if report["all_killed"]
                     and report["strict_clippy_libs_rpc_passed"]
                     and report["strict_clippy_service_tests_passed"] else 1)


if __name__ == "__main__":
    main()
