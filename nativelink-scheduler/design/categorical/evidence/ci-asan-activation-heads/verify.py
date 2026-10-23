from pathlib import Path, PurePosixPath
import hashlib, json, tarfile, tempfile

root = Path(__file__).resolve().parent

def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

for name, expected in json.loads((root / 'seal.json').read_text()).items():
    path = root / name
    assert path.stat().st_size == expected['bytes'] and sha(path) == expected['sha256'], name
manifest = json.loads((root / 'manifest.json').read_text())
verified = 0
for archive in manifest['archives']:
    with tempfile.TemporaryFile() as joined:
        for part in archive['parts']:
            path = root / part['path']
            assert path.stat().st_size == part['bytes'] and sha(path) == part['sha256']
            with path.open('rb') as source:
                while chunk := source.read(1024 * 1024):
                    joined.write(chunk)
        assert joined.tell() == archive['bytes']
        joined.seek(0)
        assert hashlib.file_digest(joined, 'sha256').hexdigest() == archive['sha256']
        joined.seek(0)
        expected = {member['path']: member for member in archive['members']}
        observed = set()
        with tarfile.open(fileobj=joined, mode='r:gz') as stream:
            for member in stream:
                path = PurePosixPath(member.name)
                assert member.isfile() and not path.is_absolute() and '..' not in path.parts
                assert member.name not in observed
                observed.add(member.name)
                record = expected[member.name]
                assert member.size == record['bytes']
                assert hashlib.file_digest(stream.extractfile(member), 'sha256').hexdigest() == record['sha256']
        assert observed == set(expected)
        verified += len(observed)
print(json.dumps({'archives': len(manifest['archives']), 'members': verified, 'result': 'All sealed files, archive fragments, joined archives, and member hashes verified.'}))
