import datetime
import hashlib
import json
from pathlib import Path
import shutil
import stat
import tarfile

E = Path('/tmp/nativelink-canonical-journal-owner-evidence')
P = Path(__file__).parent

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

receipts = json.loads((E/'receipts.json').read_text())
files = []
for artifact in receipts['artifacts']:
    source = E/artifact['path']
    assert source.stat().st_size == artifact['bytes'] and sha(source) == artifact['sha256']
    files.append({**artifact, 'mode': stat.S_IMODE(source.stat().st_mode)})
files.append({'path':'receipts.json','bytes':(E/'receipts.json').stat().st_size,
              'sha256':sha(E/'receipts.json'),'mode':stat.S_IMODE((E/'receipts.json').stat().st_mode)})
unique = {}
for item in files:
    unique.setdefault(item['sha256'], item)
# Bound uncompressed payloads conservatively so even incompressible archives remain <45MiB.
groups = []
current = []
size = 0
for item in sorted(unique.values(), key=lambda x:x['sha256']):
    assert item['bytes'] <= 40*1024*1024
    addition = item['bytes'] + 2048
    if current and size + addition > 40*1024*1024:
        groups.append(current);current=[];size=0
    current.append(item);size += addition
if current: groups.append(current)
archives = []
for i, group in enumerate(groups, 1):
    dest = P/f'author-{i:02}.tar.gz'
    with tarfile.open(dest, 'w:gz', compresslevel=6, format=tarfile.PAX_FORMAT) as archive:
        for item in group:
            source = E/item['path']
            info = tarfile.TarInfo('blobs/'+item['sha256'])
            info.size = item['bytes'];info.mode=0o644;info.mtime=0
            with source.open('rb') as stream:
                archive.addfile(info, stream)
    assert dest.stat().st_size <= 45*1024*1024
    archives.append({'path':dest.name,'bytes':dest.stat().st_size,'sha256':sha(dest),'unique_blobs':len(group)})
manifest = {'format':'sha256-deduplicated-evidence-v1',
 'created_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
 'source_commit':receipts['source_commit'],
 'receipts_sha256':sha(E/'receipts.json'),
 'verification_sha256':sha(E/'verification.json'),
 'archive_limit_bytes':45*1024*1024,
 'source_files':len(files),'unique_blobs':len(unique),
 'original_bytes':sum(x['bytes'] for x in files),
 'unique_bytes':sum(x['bytes'] for x in unique.values()),
 'compressed_bytes':sum(x['bytes'] for x in archives),
 'archives':archives,'files':files}
(P/'package.json').write_text(json.dumps(manifest,indent=2)+'\n')
for name in ('verification.json','receipts.json'):
    shutil.copy2(E/name,P/name)
print(json.dumps({k:v for k,v in manifest.items() if k not in ('files',)},indent=2))
