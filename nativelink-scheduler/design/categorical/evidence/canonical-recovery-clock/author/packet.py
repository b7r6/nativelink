#!/usr/bin/env python3
"""Verify all archived bytes; optionally materialize the exact evidence tree."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import tarfile


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    package = Path(__file__).resolve().parent
    manifest = json.loads((package / 'package.json').read_text())
    files = manifest['files']
    by_sha = {}
    for item in files:
        relative = PurePosixPath(item['path'])
        assert not relative.is_absolute() and '..' not in relative.parts, relative
        by_sha.setdefault(item['sha256'], []).append(item)
    if args.output:
        assert not args.output.exists(), 'Output directory must not already exist'
        args.output.mkdir(parents=True)
    seen = set()
    for archive in manifest['archives']:
        source = package / archive['path']
        assert source.stat().st_size == archive['bytes']
        assert digest(source) == archive['sha256'], source
        with tarfile.open(source, 'r:gz') as stream:
            members = stream.getmembers()
            assert len(members) == archive['unique_blobs']
            for member in members:
                assert member.isfile() and member.name.startswith('blobs/')
                sha = member.name.removeprefix('blobs/')
                assert len(sha) == 64 and sha in by_sha and sha not in seen
                data = stream.extractfile(member).read()
                assert hashlib.sha256(data).hexdigest() == sha, member.name
                for item in by_sha[sha]:
                    assert len(data) == item['bytes'], item['path']
                    if args.output:
                        target = args.output / item['path']
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_bytes(data)
                        target.chmod(item['mode'])
                seen.add(sha)
    assert seen == set(by_sha)
    summary = {'files': len(files), 'unique_blobs': len(seen),
               'archives': len(manifest['archives']),
               'total_original_bytes': sum(x['bytes'] for x in files),
               'result': 'Every archive, blob, original path, size and mode mapping verified.'}
    if args.output:
        receipts = json.loads((args.output / 'receipts.json').read_text())
        assert digest(args.output / 'receipts.json') == manifest['receipts_sha256']
        for item in receipts['artifacts']:
            target = args.output / item['path']
            assert target.stat().st_size == item['bytes'] and digest(target) == item['sha256']
        assert digest(args.output / 'verification.json') == manifest['verification_sha256']
        summary['materialized_output'] = str(args.output)
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
