import gzip, hashlib, io, json, pathlib, shutil, tarfile
base = pathlib.Path('/tmp/nativelink-temporal-terminal-checkpoint-packaged-evidence')
out = pathlib.Path(__file__).parent
hashfile = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
files = []
blobs = {}
for source in sorted(base.rglob('*')):
    if not source.is_file():
        continue
    sha = hashfile(source)
    files.append({'path': str(source.relative_to(base)), 'bytes': source.stat().st_size, 'sha256': sha, 'mode': source.stat().st_mode & 0o777})
    blobs.setdefault(sha, source)
groups = []
current = []
size = 0
for sha, source in sorted(blobs.items()):
    count = source.stat().st_size
    if current and size + count > 32 * 1024 * 1024:
        groups.append(current)
        current = []
        size = 0
    current.append((sha, source))
    size += count
if current:
    groups.append(current)
archives = []
for number, group in enumerate(groups, 1):
    path = out / ('checkpoint-%02d.tar.gz' % number)
    with path.open('xb') as target:
        with gzip.GzipFile(filename='', fileobj=target, mode='wb', compresslevel=6, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode='w') as archive:
                for sha, source in group:
                    content = source.read_bytes()
                    assert hashlib.sha256(content).hexdigest() == sha
                    info = tarfile.TarInfo('blobs/' + sha)
                    info.size = len(content)
                    info.mode = 0o644
                    info.mtime = 0
                    archive.addfile(info, io.BytesIO(content))
    archives.append({'path': path.name, 'bytes': path.stat().st_size, 'sha256': hashfile(path), 'unique_blobs': len(group)})
    print(json.dumps(archives[-1]), flush=True)
    assert path.stat().st_size < 45 * 1024 * 1024, 'archive exceeds split target'
for name in ('verification.json', 'receipts.json'):
    shutil.copy2(base / name, out / name)
shutil.copy2('/tmp/nativelink-canonical-journal-owner-package/packet.py', out / 'packet.py')
manifest = {'version': 1, 'kind': 'temporal-terminal-full-checkpoint-with-focused-rerun', 'source_commit': '092d570ab9708b360bb4514bfddeb96e121a5e4c', 'evidence_root': str(base), 'verification_sha256': hashfile(base / 'verification.json'), 'receipts_sha256': hashfile(base / 'receipts.json'), 'files': files, 'archives': archives}
(out / 'package.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps({'package_sha256': hashfile(out / 'package.json'), 'files': len(files), 'unique_blobs': len(blobs), 'archives': len(archives), 'total_archive_bytes': sum(a['bytes'] for a in archives)}, indent=2))
