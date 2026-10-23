#!/usr/bin/env python3
# Copyright 2026 The NativeLink Authors. All rights reserved.
# Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future
# License. See the repository LICENSE file for details.
"""Verify the local executable bundle; optionally reconstruct its retained paths."""

import argparse
import hashlib
import json
import pathlib
import shutil
import tarfile
import tempfile


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
    manifest = json.loads((root / "executables-manifest.json").read_text())
    for key in ("source_packet_manifest", "source_packet_archive", "verifier"):
        entry = manifest[key]
        assert digest(safe_path(root, entry["path"])) == entry["sha256"], key
    original = json.loads((root / manifest["source_packet_manifest"]["path"]).read_text())
    assert manifest["aliases"] == original["external_executables"]
    archives = []
    for entry in manifest["archives"]:
        path = safe_path(root, entry["path"])
        assert path.stat().st_size == entry["bytes"] < manifest["archive_limit_bytes"]
        assert digest(path) == entry["sha256"], path.name
        archives.append((path, entry))
    expected = {entry["member"]: entry for entry in manifest["executables"]}
    assert len(expected) == len(manifest["executables"]) == manifest["distinct_executable_count"]
    seen = set()
    with tempfile.TemporaryDirectory(prefix="canonical-temporal-elf-") as temporary:
        staged = pathlib.Path(temporary)
        for path, description in archives:
            archive_seen = set()
            with tarfile.open(path, mode="r:gz") as archive:
                for member in archive:
                    assert member.isfile() and member.name in expected and member.name not in seen
                    entry = expected[member.name]
                    assert member.size == entry["bytes"]
                    output = staged / entry["sha256"]
                    actual = hashlib.sha256()
                    with archive.extractfile(member) as source, output.open("wb") as target:
                        magic = source.read(4)
                        assert magic == b"\x7fELF"
                        actual.update(magic)
                        target.write(magic)
                        while block := source.read(1024 * 1024):
                            actual.update(block)
                            target.write(block)
                    assert output.stat().st_size == entry["bytes"]
                    assert actual.hexdigest() == entry["sha256"]
                    seen.add(member.name)
                    archive_seen.add(member.name)
            assert archive_seen == set(description["members"])
        assert seen == set(expected)
        assert len(manifest["aliases"]) == manifest["alias_count"]
        for entry in manifest["aliases"]:
            blob = staged / entry["sha256"]
            assert blob.stat().st_size == entry["bytes"]
            if args.extract is not None:
                output = safe_path(args.extract, entry["path"])
                output.parent.mkdir(parents=True, exist_ok=True)
                if output.exists():
                    assert digest(output) == entry["sha256"], str(output)
                else:
                    with blob.open("rb") as source, output.open("xb") as target:
                        shutil.copyfileobj(source, target)
                    output.chmod(0o755)
                assert digest(output) == entry["sha256"]
    print(json.dumps({"verified_distinct_executables": len(seen), "verified_retained_paths": manifest["alias_count"], "archive_sha256": [entry["sha256"] for _, entry in archives], "extracted_to": str(args.extract) if args.extract else None}, indent=2))


if __name__ == "__main__":
    main()
