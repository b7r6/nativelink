#!/usr/bin/env python3
# Copyright 2026 The NativeLink Authors. All rights reserved.
# Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future
# License. See the repository LICENSE file for details.
"""Verify this relative source/log/executable bundle, optionally extracting it."""

import argparse
import hashlib
import json
import os
import pathlib
import tarfile


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


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
    contents = {}
    all_members = {}
    executables = {}
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
                assert member.name not in seen and member.name not in all_members
                entry = expected[member.name]
                assert member.size == entry["bytes"]
                with bundle.extractfile(member) as source:
                    data = source.read()
                assert len(data) == entry["bytes"]
                assert hashlib.sha256(data).hexdigest() == entry["sha256"]
                if archive["kind"] == "executables":
                    assert data[:4] == b"\x7fELF"
                    assert entry["sha256"] not in executables
                    executables[entry["sha256"]] = entry
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
                        output.chmod(0o755 if archive["kind"] == "executables" else 0o644)
                all_members[member.name] = entry
                seen.add(member.name)
        assert seen == set(expected)
    aliases = {entry["path"]: entry for entry in manifest["executable_aliases"]}
    assert len(aliases) == len(manifest["executable_aliases"])
    for alias in aliases.values():
        executable = executables[alias["sha256"]]
        assert alias["bytes"] == executable["bytes"]
        if args.extract is not None:
            output = safe_path(args.extract, alias["path"])
            source = safe_path(args.extract, executable["path"])
            output.parent.mkdir(parents=True, exist_ok=True)
            if output.exists():
                assert digest(output) == alias["sha256"]
            else:
                os.link(source, output)
            assert digest(output) == alias["sha256"]
    assert contents["author/verify-bundle.py"] == (root / "verify-bundle.py").read_bytes()
    qualified = json.loads(contents["author/qualified-source.json"])
    original = json.loads(contents["author/runtime-corrected-source.json"])
    boundaries = {
        "original-runtime": {row["path"]: row["sha256"] for row in original["source_inputs"]},
        "qualified": {row["path"]: row["sha256"] for row in qualified["source_inputs"]},
    }
    boundaries["fixture-corrected"] = dict(boundaries["original-runtime"])
    fixture = json.loads(contents["author/fixture-correction.json"])
    boundaries["fixture-corrected"][fixture["path"]] = fixture["corrected_sha256"]
    plan = json.loads(contents["author/planned-faults.json"])
    for fault in plan["faults"]:
        expected_inputs = dict(boundaries["qualified"])
        expected_inputs["nativelink-scheduler/src/authority/canonical_recovery.rs"] = fault["sha256"]
        boundaries[fault["name"]] = expected_inputs
    for attempt in manifest["attempts"]:
        receipt = json.loads(contents[attempt["receipt"]])
        assert hashlib.sha256(contents[attempt["receipt"]]).hexdigest() == attempt["sha256"]
        assert receipt["source_unchanged"]
        assert receipt["source_before"] == receipt["source_after"]
        assert receipt["source_before"] == boundaries[attempt["source_boundary"]]
        for path, expected_hash in receipt["source_before"].items():
            archived = "author/" + receipt["source_directory"] + "/" + path
            assert all_members[archived]["sha256"] == expected_hash
        log = contents[attempt["log"]]
        assert hashlib.sha256(log).hexdigest() == receipt["log_sha256"]
        lines = log.decode().splitlines()
        summaries = [line for line in lines if line.startswith("test result:")]
        assert summaries == receipt["test_results"]
        errors = sum('"level":"error"' in line and 'compiler-message' in line for line in lines)
        assert errors == receipt["compiler_errors"]
        for artifact in receipt["artifacts"]:
            alias = aliases["author/" + artifact["copy"]]
            assert alias["sha256"] == artifact["sha256"]
            assert alias["bytes"] == artifact["bytes"]
    assert len(executables) == manifest["distinct_executable_count"]
    assert len(aliases) == manifest["executable_alias_count"]
    print(json.dumps({"verified_archives": len(manifest["archives"]), "verified_archive_members": len(all_members), "verified_distinct_executables": len(executables), "verified_retained_executable_paths": len(aliases), "verified_attempt_receipt_bindings": len(manifest["attempts"]), "extracted_to": str(args.extract) if args.extract else None}, indent=2))


if __name__ == "__main__":
    main()
