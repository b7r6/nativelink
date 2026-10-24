import gzip,hashlib,io,json,pathlib,tarfile,sys
W=pathlib.Path('/tmp/nativelink-canonical-worker-materialization')
D=W/'nativelink-scheduler/design/categorical/evidence/canonical-worker-materialization'
D.mkdir(parents=True,exist_ok=True)
mapping={
 'source-audit':'review','tests-first':'tests-first','tests-addendum':'tests-addendum',
 'runtime-source':'runtime-review','positive':'evidence','controls':'controls',
 'mutations':'mutation-evidence','independent-root':'independent-root-review',
 'independent-storage':'independent-storage-review'
}
def sha(b):return hashlib.sha256(b).hexdigest()
def write(p,obj):p.write_text(json.dumps(obj,indent=2)+'\n')
members=[];archives=[]
for label,suffix in mapping.items():
 if len(sys.argv)>1 and label not in sys.argv[1:]:continue
 source=pathlib.Path('/tmp/nativelink-canonical-worker-materialization-'+suffix)
 files=sorted(f for f in source.rglob('*') if f.is_file())
 groups=[];current=[];size=0
 for f in files:
  if current and size+f.stat().st_size>24*1024*1024:groups.append(current);current=[];size=0
  current.append(f);size+=f.stat().st_size
 if current:groups.append(current)
 for i,group in enumerate(groups):
  archive=D/f'{label}-{i+1:02}.tar.gz'
  assert not archive.exists(),archive
  batch=[]
  with archive.open('wb') as raw:
   with gzip.GzipFile(fileobj=raw,mode='wb',filename='',mtime=0,compresslevel=9) as compressed:
    with tarfile.open(fileobj=compressed,mode='w|',format=tarfile.PAX_FORMAT) as tar:
     for f in group:
      data=f.read_bytes();name=str(pathlib.PurePosixPath(label)/f.relative_to(source))
      info=tarfile.TarInfo(name);info.size=len(data);info.mode=0o755 if f.stat().st_mode&0o111 else 0o644;info.mtime=0;info.uid=info.gid=0;info.uname=info.gname=''
      tar.addfile(info,io.BytesIO(data));batch.append({'path':name,'bytes':len(data),'sha256':sha(data),'mode':info.mode,'archive':archive.name})
  b=archive.read_bytes();archives.append({'path':archive.name,'bytes':len(b),'sha256':sha(b),'files':len(batch),'source_bytes':sum(x['bytes'] for x in batch)});members.extend(batch)
  print(json.dumps(archives[-1]),flush=True)
 write(D/f'{label}-index.json',{'source_directory':str(source),'archives':[r for r in archives if r['path'].startswith(label+'-')],'members':[r for r in members if r['path'].startswith(label+'/')]})
