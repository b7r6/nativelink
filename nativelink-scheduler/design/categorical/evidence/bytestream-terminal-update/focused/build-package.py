#!/usr/bin/env python3
"""Archive the author evidence as deduplicated, bounded SHA256 chunks."""
import datetime
import hashlib
import io
import json
from pathlib import Path
import shutil
import stat
import tarfile

E = Path('/tmp/nativelink-bytestream-terminal-update-package-evidence')
P = Path(__file__).resolve().parent
CHUNK = 16 * 1024 * 1024
GROUP = 40 * 1024 * 1024
LIMIT = 45 * 1024 * 1024


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    assert not (P / 'package.json').exists()
    receipts = json.loads((E / 'receipts.json').read_text())
    inputs = receipts['artifacts'] + [{'path': 'receipts.json',
               'bytes': (E / 'receipts.json').stat().st_size, 'sha256': sha(E / 'receipts.json')}]
    files, unique = [], {}
    for item in inputs:
        source = E / item['path']
        assert source.is_file() and not source.is_symlink(), source
        assert source.stat().st_size == item['bytes'] and sha(source) == item['sha256'], source
        chunks = []
        with source.open('rb') as stream:
            offset = 0
            while data := stream.read(CHUNK):
                digest = hashlib.sha256(data).hexdigest()
                chunks.append({'sha256': digest, 'bytes': len(data)})
                unique.setdefault(digest, {'source': source, 'offset': offset, 'bytes': len(data)})
                offset += len(data)
        files.append({**item, 'mode': stat.S_IMODE(source.stat().st_mode), 'chunks': chunks})
    groups, current, size = [], [], 0
    for digest, item in sorted(unique.items()):
        addition = item['bytes'] + 2048
        if current and size + addition > GROUP:
            groups.append(current)
            current, size = [], 0
        current.append((digest, item))
        size += addition
    if current:
        groups.append(current)
    archives = []
    for number, group in enumerate(groups, 1):
        destination = P / f'author-{number:02}.tar.gz'
        with tarfile.open(destination, 'w:gz', compresslevel=6, format=tarfile.PAX_FORMAT) as archive:
            for digest, item in group:
                with item['source'].open('rb') as stream:
                    stream.seek(item['offset'])
                    data = stream.read(item['bytes'])
                assert hashlib.sha256(data).hexdigest() == digest
                info = tarfile.TarInfo('blobs/' + digest)
                info.size, info.mode, info.mtime = len(data), 0o644, 0
                archive.addfile(info, io.BytesIO(data))
        assert destination.stat().st_size < LIMIT
        archives.append({'path': destination.name, 'bytes': destination.stat().st_size,
                         'sha256': sha(destination), 'unique_blobs': len(group)})
        print(json.dumps(archives[-1]), flush=True)
    manifest = {
        'format': 'sha256-chunked-evidence-v2',
        'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'source_commit': receipts['source_commit'], 'chunk_bytes': CHUNK,
        'archive_limit_bytes': LIMIT, 'receipts_sha256': sha(E / 'receipts.json'),
        'verification_sha256': sha(E / 'verification.json'),
        'qualification_sha256': sha(E / 'qualification.json'),
        'source_files': len(files), 'unique_blobs': len(unique),
        'original_bytes': sum(x['bytes'] for x in files),
        'unique_bytes': sum(x['bytes'] for x in unique.values()),
        'compressed_bytes': sum(x['bytes'] for x in archives),
        'archives': archives, 'files': files,
    }
    (P / 'package.json').write_text(json.dumps(manifest, indent=2) + '\n')
    for name in ['receipts.json', 'verification.json', 'qualification.json']:
        shutil.copy2(E / name, P / name)
    print(json.dumps({k: v for k, v in manifest.items() if k not in ['files', 'archives']}, indent=2))


if __name__ == '__main__':
    main()
