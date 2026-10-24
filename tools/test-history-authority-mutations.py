#!/usr/bin/env python3
"""Qualify named authority accepted-history faults in an isolated source snapshot.

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
HISTORY = "nativelink-scheduler/src/authority/accepted_history.rs"
PREFIX = "authority::tests::histories::"
MUTANTS = [
    ('wrong_digest_algorithm', HISTORY, 'hash: hash(algorithm.hasher(), &[&prefix, response]),', 'hash: hash(DigestHasherFunc::Sha256, &[&prefix, response]),', 'fixed_history_policy_preserves_exact_objects_and_terminal_responses'),
    ('acknowledge_wrong_object', HISTORY, 'if &association.recipe.object != object {', 'if false && &association.recipe.object != object {', 'pending_history_pins_terminal_until_exact_outcome_even_after_worker_replacement'),
    ('failed_history_becomes_materialized', HISTORY, 'outcome != HistoryOutcome::Stored || association.status == HistoryStatus::Materialized', '(outcome != HistoryOutcome::Stored && false) || association.status == HistoryStatus::Materialized', 'pending_history_pins_terminal_until_exact_outcome_even_after_worker_replacement'),
    ('forget_pending_history_pin', CORE, 'if self.history_pins(execution)', 'if false && self.history_pins(execution)', 'pending_history_pins_terminal_until_exact_outcome_even_after_worker_replacement'),
    ('spend_history_finalization_identity', CORE, '+ u64::from(self.accepted_history_enabled());', '+ 0;', 'activation_prepays_identity_effect_and_byte_capacity_atomically'),
    ('omit_final_resource_history', CORE, 'TerminalResult::ResourceFailed { result, reason },\n            );\n            self.accept_history(attempt, history);', 'TerminalResult::ResourceFailed { result, reason },\n            );\n            drop(history);', 'memory_retry_is_excluded_but_final_resource_failure_has_history'),
    ('revalidate_current_worker_after_acceptance', HISTORY, 'attempt.incarnation != association.incarnation', 'self.workers[&association.worker].incarnation != association.incarnation', 'pending_history_pins_terminal_until_exact_outcome_even_after_worker_replacement'),
    ('skip_restored_recipe_validation', HISTORY, '|| association.recipe\n                    != recipe(history.namespace, &execution.request.action, result)?', '|| (false && association.recipe\n                    != recipe(history.namespace, &execution.request.action, result)?)', 'restore_rejects_missing_or_changed_history_provenance'),
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
    original = {name: (root / name).read_text() for name in [CORE, HISTORY]}
    for name, source, before, _, _ in MUTANTS:
        if original[source].count(before) != 1:
            raise ValueError(f"mutation {name} needs a unique source anchor")
    target = (args.target_dir or root / "target/history-authority-mutations").resolve()
    command = ["cargo", "test", "--locked", "--offline", "--profile", "smol",
               "-p", "nativelink-scheduler", "--lib", "-j", "8"]
    report = {
        "scope": "Eight compiled authority accepted-history faults; no production rollout or throughput claim",
        "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "source_snapshot_sha256": {}, "mutants": [], "commands": [],
        "runner_sha256": digest(Path(__file__).read_bytes()),
    }

    def save():
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")

    with tempfile.TemporaryDirectory(prefix="nativelink-history-mutants-") as temporary:
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
            baseline_tests = set(re.findall(r"^test (authority::tests::histories::\w+) \.\.\. ok$", text, re.MULTILINE))
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
            restored_tests = set(re.findall(r"^test (authority::tests::histories::\w+) \.\.\. ok$", text, re.MULTILINE))
            report["restored_passed"] = (status == 0 and restored_tests
                == set(report.get("baseline_tests", [])) and bool(restored_tests))
            report["all_killed"] = (report.get("baseline_passed", False)
                and report["restored_passed"] and len(report["mutants"]) == len(MUTANTS)
                and all(row["classification"] == "semantic_kill" for row in report["mutants"]))
            save()
    raise SystemExit(0 if report["all_killed"] else 1)


if __name__ == "__main__":
    main()
