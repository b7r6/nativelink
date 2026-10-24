#!/usr/bin/env python3
"""Seal final clock author evidence and independent reviews without altering inputs."""
from pathlib import Path
import gzip,hashlib,io,json,shutil,tarfile,subprocess
root=Path('/tmp/nativelink-canonical-recovery-clock-evidence')
out=Path('/tmp/nativelink-canonical-recovery-clock-package')
hashfile=lambda p:hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
verification=json.loads((root/'verification.json').read_text())
assert verification['restored_broad_passed'] and verification['restored_strict_passed'] and verification['all_sources_restored']
assert verification['compiled_logical_controls']==12
assert not out.exists()
out.mkdir()
roots={'author':root,'independent-storage-review':Path('/tmp/nativelink-canonical-recovery-clock-independent-storage-review'),'independent-owner-review':Path('/tmp/nativelink-canonical-recovery-clock-independent-owner-review')}
files=[];blobs={};artifacts=[]
for prefix,base in roots.items():
 for source in sorted(base.rglob('*')):
  if source.is_dir() or '__pycache__' in source.parts:continue
  assert source.is_file() and not source.is_symlink(),source
  sha=hashfile(source);relative=prefix+'/'+str(source.relative_to(base));count=source.stat().st_size
  files.append({'path':relative,'bytes':count,'sha256':sha,'mode':source.stat().st_mode&0o777})
  blobs.setdefault(sha,source)
  with source.open('rb') as f:elf=f.read(4)==b'\x7fELF'
  if elf:artifacts.append({'path':relative,'bytes':count,'sha256':sha})
receipts={'source_commit':verification['source_commit'],'artifacts':artifacts,'logical_controls':12,'scope':'Actual executable paths retained in author and independent reviews. Distinct hashes need not equal paths or test invocations.'}
(out/'receipts.json').write_text(json.dumps(receipts,indent=2)+'\n');shutil.copy2(root/'verification.json',out/'verification.json')
for name in ['receipts.json','verification.json']:
 p=out/name;sha=hashfile(p);files.append({'path':name,'bytes':p.stat().st_size,'sha256':sha,'mode':0o644});blobs.setdefault(sha,p)
groups=[];current=[];size=0
for sha,p in sorted(blobs.items()):
 count=p.stat().st_size
 if current and size+count>28*1024*1024:groups.append(current);current=[];size=0
 current.append((sha,p));size+=count
if current:groups.append(current)
archives=[]
for number,group in enumerate(groups,1):
 p=out/f'clock-{number:02d}.tar.gz'
 with p.open('xb') as f:
  with gzip.GzipFile(filename='',fileobj=f,mode='wb',compresslevel=6,mtime=0) as compressed:
   with tarfile.open(fileobj=compressed,mode='w') as archive:
    for sha,source in group:
     data=source.read_bytes();assert hashlib.sha256(data).hexdigest()==sha
     info=tarfile.TarInfo('blobs/'+sha);info.size=len(data);info.mode=0o644;info.mtime=0;archive.addfile(info,io.BytesIO(data))
 assert p.stat().st_size<45*1024*1024
 archives.append({'path':p.name,'bytes':p.stat().st_size,'sha256':hashfile(p),'unique_blobs':len(group)})
 print(json.dumps(archives[-1]),flush=True)
shutil.copy2('/tmp/nativelink-canonical-recovery-custody-package/packet.py',out/'packet.py')
shutil.copy2(__file__,out/'build.py')
manifest={'version':1,'kind':'canonical-recovery-clock-author-evidence','source_commit':verification['source_commit'],'evidence_roots':{k:str(v) for k,v in roots.items()},'verification_sha256':hashfile(out/'verification.json'),'receipts_sha256':hashfile(out/'receipts.json'),'files':files,'archives':archives,'distinct_executable_count':len({x['sha256'] for x in artifacts}),'executable_path_count':len(artifacts)}
(out/'package.json').write_text(json.dumps(manifest,indent=2)+'\n')
command=['python3',str(out/'packet.py')];v=subprocess.run(command,capture_output=True,text=True);(out/'package-verification.log').write_text(v.stdout+v.stderr);assert v.returncode==0
(out/'package-verification.json').write_text(json.dumps({'command':command,'exit_code':v.returncode,'stdout':v.stdout,'stderr':v.stderr},indent=2)+'\n')
seal={'source_commit':verification['source_commit'],'package_sha256':hashfile(out/'package.json'),'verification_sha256':hashfile(out/'verification.json'),'receipts_sha256':hashfile(out/'receipts.json'),'verifier_sha256':hashfile(out/'packet.py'),'builder_sha256':hashfile(out/'build.py'),'package_verification_sha256':hashfile(out/'package-verification.json'),'input_custody':'Every archived source/log/ELF was byte-checked against input manifest while archiving; explicit prior failure/source boundaries retained.','archives':archives}
(out/'package-seal.json').write_text(json.dumps(seal,indent=2)+'\n')
print(json.dumps({'package_sha256':hashfile(out/'package.json'),'seal_sha256':hashfile(out/'package-seal.json'),'files':len(files),'unique_blobs':len(blobs),'archives':len(archives),'distinct_executables':manifest['distinct_executable_count'],'executable_paths':len(artifacts),'total_archive_bytes':sum(x['bytes'] for x in archives)},indent=2))
