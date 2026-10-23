#!/usr/bin/env python3
# Copyright 2026 The NativeLink Authors. All rights reserved.
# Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future
# License. See the repository LICENSE file for details.
"""Verify relative source, log, compiler-probe, and binary evidence; optionally extract."""

import argparse
import hashlib
import json
import os
import pathlib
import tarfile


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def hashed(data):
    return hashlib.sha256(data).hexdigest()


def safe_path(base, relative):
    value = pathlib.PurePosixPath(relative)
    if value.is_absolute() or ".." in value.parts:
        raise ValueError(f"unsafe relative path: {relative}")
    path = base.joinpath(*value.parts)
    if not path.resolve().is_relative_to(base.resolve()):
        raise ValueError(f"path escapes destination: {relative}")
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extract", type=pathlib.Path)
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parent
    manifest = json.loads((root / "manifest.json").read_text())
    assert digest(root / "verify-bundle.py") == manifest["verifier_sha256"]
    contents, members, binaries = {}, {}, {}
    for archive in manifest["archives"]:
        path = safe_path(root, archive["path"])
        assert path.stat().st_size == archive["bytes"]
        assert digest(path) == archive["sha256"], path.name
        expected = {entry["path"]: entry for entry in archive["members"]}
        assert len(expected) == len(archive["members"])
        seen = set()
        with tarfile.open(path, mode="r:gz") as bundle:
            for member in bundle:
                assert member.isfile() and member.name in expected
                assert member.name not in seen and member.name not in members
                entry = expected[member.name]
                assert member.size == entry["bytes"]
                with bundle.extractfile(member) as source:
                    data = source.read()
                assert len(data) == entry["bytes"] and hashed(data) == entry["sha256"]
                if archive["kind"] == "binaries":
                    assert data.startswith(b"\x7fELF") or data.startswith(b"!<arch>\n")
                    assert entry["sha256"] not in binaries
                    binaries[entry["sha256"]] = entry
                else:
                    contents[member.name] = data
                if args.extract is not None:
                    output = safe_path(args.extract, member.name)
                    output.parent.mkdir(parents=True, exist_ok=True)
                    if output.exists():
                        assert digest(output) == entry["sha256"]
                    else:
                        with output.open("xb") as target:
                            target.write(data)
                        output.chmod(0o755 if data.startswith(b"\x7fELF") else 0o644)
                members[member.name] = entry
                seen.add(member.name)
        assert seen == set(expected)
    aliases = {entry["path"]: entry for entry in manifest["binary_aliases"]}
    assert len(aliases) == len(manifest["binary_aliases"])
    for alias in aliases.values():
        binary = binaries[alias["sha256"]]
        assert alias["bytes"] == binary["bytes"] and alias["kind"] == binary["kind"]
        if args.extract is not None:
            output = safe_path(args.extract, alias["path"])
            source = safe_path(args.extract, binary["path"])
            output.parent.mkdir(parents=True, exist_ok=True)
            if output.exists():
                assert digest(output) == alias["sha256"]
            else:
                os.link(source, output)
            assert digest(output) == alias["sha256"]
    assert contents["author/verify-bundle.py"] == (root / "verify-bundle.py").read_bytes()

    def load(name):
        return json.loads(contents["author/" + name])

    boundaries = {}
    for name, filename, directory in [
        ("initial-runtime", "runtime-source.json", "runtime-source"),
        ("qualified", "qualified-source.json", "qualified-source"),
    ]:
        frozen = load(filename)
        boundaries[name] = {row["path"]: row["sha256"] for row in frozen["source_inputs"]}
        assert len(boundaries[name]) == 44
        for relative, expected_hash in boundaries[name].items():
            assert members["author/" + directory + "/" + relative]["sha256"] == expected_hash
    assert hashed(contents["author/qualified-source.json"]) == manifest["qualified_source_manifest_sha256"]
    plan = load("planned-faults.json")
    for case in plan["cases"]:
        expected = dict(boundaries["qualified"])
        expected[case["path"]] = case["source_sha256"]
        boundaries[case["name"]] = expected
        assert members["author/" + case["variant_copy"]]["sha256"] == case["source_sha256"]
        applied = load("fault-" + case["name"] + "-applied.json")
        assert applied["expected_source"] == expected and applied["source_matches_variant"]
    for attempt in manifest["attempts"]:
        receipt = json.loads(contents[attempt["receipt"]])
        assert hashed(contents[attempt["receipt"]]) == attempt["sha256"]
        assert receipt["source_unchanged"] and not receipt["artifact_copy_errors"]
        assert receipt["source_before"] == receipt["source_after"] == boundaries[attempt["source_boundary"]]
        # This original field identifies input-path selection, not current source bytes.
        assert receipt["source_manifest_sha256"] == hashed(contents["author/runtime-source.json"])
        for relative, expected_hash in receipt["source_before"].items():
            archived = "author/" + receipt["source_directory"] + "/" + relative
            assert members[archived]["sha256"] == expected_hash
        log = contents[attempt["log"]]
        assert hashed(log) == receipt["log_sha256"]
        lines = log.decode().splitlines()
        assert [line for line in lines if line.startswith("test result:")] == receipt["test_results"]
        errors = sum('"level":"error"' in line and 'compiler-message' in line for line in lines)
        assert errors == receipt["compiler_errors"]
        for artifact in receipt["artifacts"] + receipt["library_artifacts"]:
            alias = aliases["author/" + artifact["copy"]]
            assert alias["sha256"] == artifact["sha256"] and alias["bytes"] == artifact["bytes"]
    restoration = load("source-restoration.json")
    assert hashed(contents["author/source-restoration.json"]) == manifest["source_restoration_receipt_sha256"]
    assert restoration["all44_exact"] and restoration["after"] == boundaries["qualified"]
    fault_results = load("fault-results.json")
    assert hashed(contents["author/fault-results.json"]) == manifest["fault_results_sha256"]
    assert fault_results["counts"] == {"logical_controls": 6, "actual_invocations": 6, "compiled": 6, "distinct_control_executables": 6, "direct_semantic_detections": 6, "prerequisite_failures": 0, "survivors": 0}
    for case in fault_results["cases"]:
        receipt = load(case["receipt"])
        assert hashed(contents["author/" + case["receipt"]]) == case["receipt_sha256"]
        assert receipt["exit_code"] == 101 and receipt["compiler_errors"] == 0
        assert case["actual_executable"] in receipt["artifacts"]
        raw = contents["author/" + case["log"]]
        assert hashed(raw) == case["log_sha256"]
        assert "\n".join(case["first_failure_excerpt"]) in raw.decode()
    for field in ["artifacts", "library_artifacts"]:
        qualified_artifacts = {row["target"]: row["sha256"] for row in load("qualified-broad.json")[field]}
        restored_artifacts = {row["target"]: row["sha256"] for row in load("restored-broad.json")[field]}
        assert qualified_artifacts == restored_artifacts
    api_counts = {}
    probe_plan = load("api-probes-amended-plan.json")
    required = {probe_plan["positive"]: None, **{r["file"]: r["required_code"] for r in probe_plan["negative"]}}
    for label in ["api-first", "api-restored"]:
        summary = load(label + "/summary.json")
        receipt = load(summary["library_receipt"])
        assert summary["library_receipt_sha256"] == hashed(contents["author/" + summary["library_receipt"]])
        assert summary["source_unchanged"] and summary["source_before"] == summary["source_after"] == boundaries["qualified"]
        for name, library in summary["libraries"].items():
            artifact = next(row for row in receipt["library_artifacts"] if row["target"] == name)
            filename = pathlib.PurePosixPath(library["probe_copy"]).name
            assert library["sha256"] == artifact["sha256"] == aliases["author/" + label + "/libraries/" + filename]["sha256"]
        assert len(summary["rows"]) == 17 and {row["source"] for row in summary["rows"]} == set(required)
        observed = 0
        for row in summary["rows"]:
            prefix = "author/" + label + "/"
            filename = row["source"]
            probe_path = row["command"][-3]
            assert pathlib.PurePosixPath(probe_path).name == filename
            assert row["required_code"] == required[filename]
            assert members[prefix + filename]["sha256"] == row["source_sha256"]
            assert row["source_sha256"] == boundaries["qualified"]["nativelink-scheduler/tests/canonical_recovered_owner_api/" + filename]
            raw = contents[prefix + row["log"]]
            assert hashed(raw) == row["log_sha256"]
            diagnostics = [json.loads(line) for line in raw.decode().splitlines()]
            errors = [d for d in diagnostics if d.get("level") == "error" and d.get("spans")]
            assert errors == row["primary_errors"]
            if required[filename] is None:
                assert row["exit_code"] == 0 and row["positive_passed"] and not errors
            else:
                expected = required[filename]
                matched = [d for d in errors if (d.get("code") or {}).get("code") == expected and any(s.get("is_primary") and s["file_name"] == probe_path for s in d["spans"])]
                assert row["exit_code"] != 0 and matched
                if filename == "constructor.rs":
                    assert any(s.get("is_primary") and s["file_name"] == probe_path and s["line_start"] == 22 for d in matched for s in d["spans"])
                def related(diagnostic):
                    if filename != "constructor.rs" or diagnostic.get("code") is not None or diagnostic.get("message") != "type `journal::canonical_owner::canonical_recovery::LifecycleOwner` is private":
                        return False
                    for span in diagnostic.get("spans", []):
                        while span:
                            if span["file_name"] == probe_path and span.get("line_start") == 22:
                                return True
                            span = (span.get("expansion") or {}).get("span")
                    return False
                assert all((d.get("code") or {}).get("code") == expected or related(d) for d in errors)
                if label == "api-first" and filename == "constructor.rs":
                    assert row["intended_rejection_observed"] is False
                else:
                    assert row["intended_rejection_observed"] is True
                observed += 1
            if row["metadata"]:
                metadata = row["metadata"]
                assert members[prefix + metadata["path"]]["sha256"] == metadata["sha256"]
                assert members[prefix + metadata["path"]]["bytes"] == metadata["bytes"]
        assert observed == 16 and summary["positive_passed"] and summary["negative_planned"] == 16
        assert summary["negative_observed"] == (15 if label == "api-first" else 16)
        api_counts[label] = {"positive": 1, "required_rejections": observed, "original_classifier_count": summary["negative_observed"]}
    assert len(binaries) == manifest["distinct_binary_count"]
    assert len(aliases) == manifest["binary_alias_count"]
    print(json.dumps({"verified_archives": len(manifest["archives"]), "verified_archive_members": len(members), "verified_distinct_binaries": len(binaries), "verified_retained_binary_paths": len(aliases), "verified_attempt_receipt_bindings": len(manifest["attempts"]), "verified_external_api_probes": api_counts, "extracted_to": str(args.extract) if args.extract else None}, indent=2))


if __name__ == "__main__":
    main()
