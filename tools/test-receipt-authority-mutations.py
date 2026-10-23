#!/usr/bin/env python3
"""Qualify named authority receipt faults in an isolated source snapshot.

Compilation and process errors never count as semantic detection. Each mutant
must compile, execute its exact named test, and fail an assertion or panic.
"""

import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


CORE = "nativelink-scheduler/src/authority/mod.rs"
RECEIPTS = "nativelink-scheduler/src/authority/receipt_retirement.rs"
PREFIX = "authority::tests::receipts::"
MUTANTS = [
    (
        "omit_uncertain_exceptions", RECEIPTS,
        "attempt.incarnation == incarnation\n                && !self.reservations[&attempt.reservation].released",
        "attempt.incarnation == incarnation\n                && attempt.authorized",
        "oversized_exception_set_defers_atomically_with_all_uncertain_attempts_intact",
    ),
    (
        "acknowledge_changed_body", RECEIPTS,
        "|| stream.pending.as_ref() != Some(certificate)",
        "|| stream.pending.is_none()",
        "ack_matches_full_certificate_and_authenticated_identity_without_partial_changes",
    ),
    (
        "ack_erases_later_releases", RECEIPTS,
        "stream.pending = None;",
        "stream.pending = None; stream.dirty = false;",
        "immutable_pending_certificate_pins_released_attempts_and_ack_never_releases_resources",
    ),
    (
        "release_does_not_schedule_retirement", RECEIPTS,
        "stream.dirty = true;",
        "stream.dirty = false;",
        "sustained_retirement_has_constant_metadata_with_an_old_active_hole",
    ),
    (
        "forget_pending_attempt_pins", CORE,
        "if self.receipt_attempts_pinned(&attempts)",
        "if false && self.receipt_attempts_pinned(&attempts)",
        "immutable_pending_certificate_pins_released_attempts_and_ack_never_releases_resources",
    ),
    (
        "spend_promised_finalization_id", RECEIPTS,
        "if !self.identities_have_headroom(false) {",
        "if false && !self.identities_have_headroom(false) {",
        "identity_exhaustion_cannot_spend_prepaid_finalization_on_a_certificate",
    ),
    (
        "replacement_keeps_old_intent", RECEIPTS,
        "*stream = ReceiptStream::fresh(incarnation);",
        "stream.incarnation = incarnation;",
        "later_cancellation_does_not_rewrite_pending_and_replacement_preserves_uncertain_charge",
    ),
    (
        "receipt_ack_refunds_resources", RECEIPTS,
        "stream.pending = None;",
        "stream.pending = None; for reservation in self.reservations.values_mut() { reservation.released = true; }",
        "immutable_pending_certificate_pins_released_attempts_and_ack_never_releases_resources",
    ),
]


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target-dir", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    original = {name: (root / name).read_text() for name in [CORE, RECEIPTS]}
    for name, source, before, _, _ in MUTANTS:
        if original[source].count(before) != 1:
            raise ValueError(f"mutation {name} needs a unique source anchor")
    target = (args.target_dir or root / "target/receipt-authority-mutations").resolve()
    command = ["cargo", "test", "--locked", "--offline", "--profile", "smol",
               "-p", "nativelink-scheduler", "--lib", "-j", "16"]
    report = {
        "scope": "Eight compiled authority receipt faults; no production rollout or throughput claim",
        "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "source_snapshot_sha256": {}, "mutants": [], "commands": [],
        "runner_sha256": digest(Path(__file__).read_bytes()),
    }

    def save():
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")

    with tempfile.TemporaryDirectory(prefix="nativelink-receipt-mutants-") as temporary:
        checkout = Path(temporary)
        paths = subprocess.check_output([
            "git", "ls-files", "-z", "--cached", "--others", "--exclude-standard",
        ], cwd=root).decode().split("\0")
        for relative in sorted(set(paths) - {""}):
            source = root / relative
            if not source.is_file() or source.is_symlink():
                continue
            destination = checkout / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            # Fresh mtimes prevent a prior mutant's library from being reused
            # merely because immutable original files predate that compilation.
            shutil.copyfile(source, destination)
            if source.suffix == ".rs" or source.name in {"Cargo.toml", "Cargo.lock"}:
                report["source_snapshot_sha256"][relative] = digest(source.read_bytes())

        def run(name, argv, timeout=600):
            log = output / f"{name}.log"
            started = time.monotonic()
            try:
                with log.open("wb") as stream:
                    result = subprocess.run(argv, cwd=checkout,
                        env=dict(os.environ, CARGO_TARGET_DIR=str(target)),
                        stdout=stream, stderr=subprocess.STDOUT, timeout=timeout,
                        check=False)
                status = result.returncode
            except subprocess.TimeoutExpired:
                status = "timeout"
            row = {"name": name, "argv": argv, "cwd": str(checkout),
                   "CARGO_TARGET_DIR": str(target), "status": status,
                   "seconds": time.monotonic() - started,
                   "log": log.name, "sha256": digest(log.read_bytes())}
            report["commands"].append(row)
            save()
            return status, log.read_text(errors="replace")

        try:
            status, text = run("baseline", command + [PREFIX])
            baseline_tests = set(re.findall(r"^test (authority::tests::receipts::\w+) \.\.\. ok$", text, re.MULTILINE))
            required_tests = {PREFIX + row[4] for row in MUTANTS}
            report["baseline_tests"] = sorted(baseline_tests)
            report["baseline_passed"] = status == 0 and required_tests.issubset(baseline_tests)
            if not report["baseline_passed"]:
                raise RuntimeError("baseline did not execute every named mutation assertion")
            for name, source, before, after, test in MUTANTS:
                for path, contents in original.items():
                    (checkout / path).write_text(contents)
                mutated = original[source].replace(before, after, 1)
                (checkout / source).write_text(mutated)
                row = {"name": name, "source": source, "mutant_sha256": digest(mutated.encode()),
                       "before": before, "after": after, "test": PREFIX + test}
                status, _ = run(name + "-compile", command + ["--no-run"])
                if status != 0:
                    row["classification"] = "compile_error" if isinstance(status, int) else "process_error"
                else:
                    status, text = run(name + "-test", command + [PREFIX + test, "--", "--exact"], 120)
                    if status == 0:
                        row["classification"] = "survived"
                    elif isinstance(status, int) and status > 0 and (
                        f"test {PREFIX}{test} ... FAILED" in text
                        and "panicked at" in text and "test result: FAILED" in text
                    ):
                        row["classification"] = "semantic_kill"
                    else:
                        row["classification"] = "process_error"
                report["mutants"].append(row)
                save()
                print(json.dumps({"name": name, "classification": row["classification"]}), flush=True)
        finally:
            for path, contents in original.items():
                (checkout / path).write_text(contents)
            status, text = run("restored", command + [PREFIX])
            restored_tests = set(re.findall(r"^test (authority::tests::receipts::\w+) \.\.\. ok$", text, re.MULTILINE))
            report["restored_passed"] = (status == 0 and restored_tests
                == set(report.get("baseline_tests", [])) and bool(restored_tests))
            report["all_killed"] = (report.get("baseline_passed", False)
                and report["restored_passed"] and len(report["mutants"]) == len(MUTANTS)
                and all(row["classification"] == "semantic_kill" for row in report["mutants"]))
            save()
    raise SystemExit(0 if report["all_killed"] else 1)


if __name__ == "__main__":
    main()
