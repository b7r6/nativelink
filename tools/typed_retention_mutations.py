#!/usr/bin/env python3
"""Compiled semantic mutations of the test-only typed retention wrapper."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def run(command, workspace, output):
    with output.open("wb") as log:
        result = subprocess.run(command, cwd=workspace, stdout=log, stderr=subprocess.STDOUT, check=False)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    workspace = args.workspace.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    source = workspace / "nativelink-scheduler/tests/retention/typed_bridge.rs"
    original = source.read_text()
    command = ["cargo", "test", "-p", "nativelink-scheduler", "--profile=smol", "--test", "typed_physical_retention_test"]
    mutations = [
        ("omit_required_object", "            objects: rows,", "            objects: rows.into_iter().skip(1).collect(),"),
        ("ignore_catalogue_revision", "if pending.revision != self.state.metadata.revision {", "if false && pending.revision != self.state.metadata.revision {"),
        ("ignore_physical_identity", ".find(|row| row.binding() == object.binding)", ".find(|row| row.binding().object == object.binding.object)"),
        ("replace_captured_fences", ".filter(|(cohort, _)| touched.contains(cohort))\n            .collect();", ".filter(|(cohort, _)| touched.contains(cohort))\n            .map(|(cohort, _)| (cohort, self.view().cohorts[&cohort].fence))\n            .collect();"),
        ("expose_head_before_checkpoint", "if let Err(error) = self.persist(&next, cut) {", "if let Err(error) = if next.metadata.head != self.state.metadata.head { Ok(()) } else { self.persist(&next, cut) } {"),
        ("release_serving_with_head", "        // Serving roots are independent of the derived head's lifetime.", "        for (root, record) in next.candidate.view().roots {\n            if record.kind == RootKind::Serving { next.apply(Event::Release { root, lease: record.lease })?; }\n        }"),
    ]
    report = {"source": str(source.relative_to(workspace)), "baseline_sha256": hashlib.sha256(original.encode()).hexdigest(), "compile_command": command + ["--no-run"], "test_command": command, "mutations": []}
    try:
        report["baseline_exit"] = run(command, workspace, output / "baseline.log")
        if report["baseline_exit"] != 0:
            raise RuntimeError("baseline failed")
        for name, before, after in mutations:
            if original.count(before) != 1:
                raise RuntimeError(f"mutation anchor is not unique: {name}")
            changed = original.replace(before, after)
            source.write_text(changed)
            row = {"name": name, "sha256": hashlib.sha256(changed.encode()).hexdigest()}
            row["compile_exit"] = run(command + ["--no-run"], workspace, output / f"{name}.compile.log")
            if row["compile_exit"] != 0:
                row["classification"] = "compile_error"
            else:
                test_log = output / f"{name}.test.log"
                row["test_exit"] = run(command, workspace, test_log)
                text = test_log.read_text(errors="replace")
                if row["test_exit"] == 0:
                    row["classification"] = "survived"
                elif "test result: FAILED" in text and "panicked at" in text:
                    row["classification"] = "semantic_kill"
                else:
                    row["classification"] = "process_error"
            report["mutations"].append(row)
            print(name, row["classification"], flush=True)
    finally:
        source.write_text(original)
        report["restored_sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        report["restored_exit"] = run(command, workspace, output / "restored.log")
        (output / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    if report["restored_exit"] != 0 or any(row["classification"] != "semantic_kill" for row in report["mutations"]):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
