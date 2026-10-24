#!/usr/bin/env python3
"""Verify sealed archives and reconstructed files; optionally restore the evidence tree."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import tarfile
import tempfile


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def relative(name):
    path = PurePosixPath(name)
    assert str(path) == name and not path.is_absolute() and '..' not in path.parts
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--allow-unsealed', action='store_true', help='Only for the initial package build.')
    args = parser.parse_args()
    package = Path(__file__).resolve().parent
    seal_path = package / 'package-seal.json'
    seal_checked = seal_path.exists()
    if seal_checked:
        seal = json.loads(seal_path.read_text())
        actual = {str(p.relative_to(package)) for p in package.rglob('*') if p.is_file() and p != seal_path}
        assert actual == {x['path'] for x in seal['files']}
        for item in seal['files']:
            target = package / relative(item['path'])
            assert target.is_file() and not target.is_symlink()
            assert target.stat().st_size == item['bytes'] and sha(target) == item['sha256'], target
    else:
        assert args.allow_unsealed, 'Missing package seal'
    manifest = json.loads((package / 'package.json').read_text())
    assert manifest['format'] == 'sha256-chunked-evidence-v2'
    files, wanted, paths = manifest['files'], {}, set()
    for item in files:
        relative(item['path'])
        assert item['path'] not in paths
        paths.add(item['path'])
        assert sum(x['bytes'] for x in item['chunks']) == item['bytes']
        for chunk in item['chunks']:
            assert re.fullmatch('[0-9a-f]{64}', chunk['sha256'])
            assert 0 < chunk['bytes'] <= manifest['chunk_bytes']
            assert wanted.setdefault(chunk['sha256'], chunk['bytes']) == chunk['bytes']
    assert len(files) == manifest['source_files'] and len(wanted) == manifest['unique_blobs']
    if args.output:
        assert not args.output.exists(), 'Output directory must not already exist'
        args.output.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix='identity-packet-chunks-') as cache_name:
        cache = Path(cache_name)
        seen = set()
        for archive in manifest['archives']:
            source = package / relative(archive['path'])
            assert source.stat().st_size == archive['bytes'] < manifest['archive_limit_bytes']
            assert sha(source) == archive['sha256'], source
            with tarfile.open(source, 'r:gz') as stream:
                members = stream.getmembers()
                assert len(members) == archive['unique_blobs']
                for member in members:
                    assert member.isfile() and member.name.startswith('blobs/')
                    digest = member.name.removeprefix('blobs/')
                    assert digest in wanted and digest not in seen
                    assert member.size == wanted[digest]
                    data = stream.extractfile(member).read()
                    assert len(data) == member.size and hashlib.sha256(data).hexdigest() == digest
                    (cache / digest).write_bytes(data)
                    seen.add(digest)
        assert seen == set(wanted)
        for item in files:
            digest, size = hashlib.sha256(), 0
            output = None
            if args.output:
                target = args.output / item['path']
                target.parent.mkdir(parents=True, exist_ok=True)
                output = target.open('wb')
            try:
                for chunk in item['chunks']:
                    data = (cache / chunk['sha256']).read_bytes()
                    digest.update(data)
                    size += len(data)
                    if output:
                        output.write(data)
            finally:
                if output:
                    output.close()
            assert digest.hexdigest() == item['sha256'] and size == item['bytes'], item['path']
            if args.output:
                target.chmod(item['mode'])
                assert sha(target) == item['sha256']
        for name, key in [('receipts.json', 'receipts_sha256'), ('verification.json', 'verification_sha256'),
                          ('qualification.json', 'qualification_sha256')]:
            assert sha(package / name) == manifest[key]
            recorded = next(x for x in files if x['path'] == name)
            assert recorded['sha256'] == manifest[key]
        receipts = json.loads((package / 'receipts.json').read_text())
        declared = {x['path']: (x['bytes'], x['sha256']) for x in receipts['artifacts']}
        reconstructed = {x['path']: (x['bytes'], x['sha256']) for x in files if x['path'] != 'receipts.json'}
        assert declared == reconstructed
    print(json.dumps({'files': len(files), 'unique_chunks': len(seen), 'archives': len(manifest['archives']),
                      'original_bytes': sum(x['bytes'] for x in files), 'seal_verified': seal_checked,
                      'package_sha256': sha(package / 'package.json'),
                      'materialized_output': str(args.output) if args.output else None,
                      'result': 'Every archive, chunk, reconstructed file, receipt, size and mode mapping verified.'}, indent=2))


if __name__ == '__main__':
    main()
