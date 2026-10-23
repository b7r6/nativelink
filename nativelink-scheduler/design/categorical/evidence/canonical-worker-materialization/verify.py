"""Verify the archived evidence without extracting files or executing binaries."""

import hashlib
import json
from pathlib import Path, PurePosixPath
import tarfile


def sha(data):
    return hashlib.sha256(data).hexdigest()


root = Path(__file__).resolve().parent
seal_path = root / "package-seal.json"
if seal_path.exists():
    seal = json.loads(seal_path.read_bytes())
    names = {entry["path"] for entry in seal["files"]}
    actual = {
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file() and path != seal_path
    }
    assert names == actual, "Top-level packet file set differs from seal"
    for entry in seal["files"]:
        payload = (root / entry["path"]).read_bytes()
        assert len(payload) == entry["bytes"]
        assert sha(payload) == entry["sha256"], entry["path"]
package_bytes = (root / "package.json").read_bytes()
package = json.loads(package_bytes)
expected = {entry["path"]: entry for entry in package["members"]}
assert len(expected) == len(package["members"]), "Duplicate member manifest"
seen = set()
total_bytes = 0
for archive in package["archives"]:
    path = root / archive["path"]
    data = path.read_bytes()
    assert len(data) == archive["bytes"] and sha(data) == archive["sha256"]
    count = 0
    with tarfile.open(path, "r:gz") as contents:
        for member in contents:
            relative = PurePosixPath(member.name)
            assert not relative.is_absolute() and ".." not in relative.parts
            assert member.isfile() and member.name not in seen
            record = expected[member.name]
            assert record["archive"] == archive["path"]
            payload = contents.extractfile(member).read()
            assert len(payload) == member.size == record["bytes"]
            assert member.mode == record["mode"]
            assert sha(payload) == record["sha256"], member.name
            seen.add(member.name)
            count += 1
            total_bytes += len(payload)
    assert count == archive["files"]
assert seen == set(expected), "Archive member set differs from manifest"
print(json.dumps({
    "status": "PASS",
    "package_sha256": sha(package_bytes),
    "archives": len(package["archives"]),
    "members": len(seen),
    "member_bytes": total_bytes,
    "operation": "Read and hash only; no extraction or executable invocation",
}, indent=2))
