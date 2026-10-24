from pathlib import Path
import collections,hashlib,json,os,re,shutil,subprocess,urllib.parse
root=Path('/home/b7r6/src/tracemachina/nativelink/nativelink-b7r6-0x03')
base='092d570ab9708b360bb4514bfddeb96e121a5e4c'
src=Path('/tmp/nativelink-categorical-evidence')
out=Path('/tmp/nativelink-temporal-terminal-checkpoint-packaged-evidence')
assert out.is_dir()
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
def write(n,v):(out/n).write_text(json.dumps(v,indent=2)+'\n')
attempts=['temporal-terminal-checkpoint','temporal-terminal-checkpoint-format-rerun']

full=out/'attempts'/attempts[0];rerun=out/'attempts'/attempts[1]
inputs=json.loads((full/'sources-before.json').read_text())
source_receipts=json.loads((out/'source-receipts.json').read_text())['inputs']
assert len(inputs)==len(source_receipts)==1354
for r in source_receipts:assert sha(out/'source'/r['path'])==inputs[r['path']]==r['sha256']
for p in [full,rerun]:
 assert (p/'head.txt').read_text().strip()==base
 for n in ['sources-before.json','sources-after.json']:assert json.loads((p/n).read_text())==inputs
full_events=[json.loads(x) for x in (full/'events.jsonl').read_text().splitlines()]
rerun_events=[json.loads(x) for x in (rerun/'events.jsonl').read_text().splitlines()]
artifacts=json.loads((out/'artifact-receipts.json').read_text())['artifacts']
for a in artifacts:
 if a['copied']:assert sha(out/a['copy'])==a['bep_file']['digest']
def summarize(events):
 summaries=[dict(label=e['id']['testSummary']['label'],**e['testSummary']) for e in events if 'testSummary'in e]
 results=[e['testResult'] for e in events if 'testResult'in e]
 assert all(not r.get('cachedLocally',False) and not r.get('cachedRemotely',False) for r in results)
 return summaries,dict(collections.Counter(s['overallStatus'] for s in summaries))
fs,fc=summarize(full_events);rs,rc=summarize(rerun_events)
assert fc=={'PASSED':173,'TIMEOUT':1} and rc=={'PASSED':1}
for p,evsumm in [(full,fs),(rerun,rs)]:assert json.loads((p/'test-summaries.json').read_text())==evsumm
formatlabel='//nativelink-scheduler:integration_tests/authority_format_test_test'
assert [s['label'] for s in fs if s['overallStatus']!='PASSED']==[formatlabel]
assert [s['label'] for s in rs]==[formatlabel]
for events in [full_events,rerun_events]:
 c=[e['completed'] for e in events if e.get('id',{}).get('targetCompleted',{}).get('label')==formatlabel and 'aspect'not in e['id']['targetCompleted']]
 assert len(c)==1 and c[0]['testTimeoutSeconds']=='60'
logs=[];counts=collections.Counter()
for p in sorted((full/'testlogs').rglob('test.log')):
 text=p.read_text();matches=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',text)
 for passed,failed,ignored in matches:counts.update(passed=int(passed),failed=int(failed),ignored=int(ignored))
 logs.append({'path':str(p.relative_to(out)),'sha256':sha(p),'complete_rust_summaries':len(matches)})
assert len(logs)==173 and counts=={'passed':2346,'failed':0,'ignored':11}
fp='testlogs/nativelink-scheduler/integration_tests/authority_format_test_test/test.log'
ftime=(full/fp).read_text();fpass=(rerun/fp).read_text()
assert len(re.findall(r'^test .* \.\.\. ok$',ftime,re.M))==27
assert 'test result: ok. 28 passed; 0 failed; 0 ignored;'in fpass
assert len(re.findall(r'^test .* \.\.\. ok$',fpass,re.M))==28
fmtsha=[f['digest'] for e in rerun_events if 'namedSetOfFiles'in e for f in e['namedSetOfFiles'].get('files',[]) if f['name']=='nativelink-scheduler/integration_tests/authority_format_test_test']
assert fmtsha==['ad572f7a9a2cafaaf31095a568380074ebc65e9a72119ff78938c440af2e80ff']
assert any(a['bep_file']['digest']==fmtsha[0] and a['copied'] for a in artifacts)
dummy_event=next(e for e in full_events if e.get('id',{}).get('testResult',{}).get('label')=='//:dummy_test')
dummy_receipts=[]
for f in dummy_event['testResult']['testActionOutput']:
 source=Path(urllib.parse.unquote(urllib.parse.urlparse(f['uri']).path))
 dest=out/'supplemental/dummy_test'/f['name'];dest.parent.mkdir(parents=True,exist_ok=True)
 assert dest.is_file()
 dummy_receipts.append({'source':str(source),'copy':str(dest.relative_to(out)),'sha256':sha(dest),'size':dest.stat().st_size,'copied_mtime_ns':dest.stat().st_mtime_ns})
write('supplemental/dummy_test/provenance.json',{'reason':'Original recorder mapped //:dummy_test to an absolute /dummy_test path and did not copy its output. These retained outputs were copied later from the exact BEP URIs; filesystem timestamps lie approximately two seconds after the recorded attempt.','bep_event':dummy_event,'files':dummy_receipts,'limitation':'BEP testActionOutput carries no digest, so byte identity to the originally executed output cannot be independently proved by a contemporaneous hash. PASSED status is established directly by the immutable raw BEP.'})
verification={
 'reviewer':'/root/scheduler_review','kind':'External independent evidence packaging; no new execution or repository edits','source_commit':base,'tracked_inputs':len(inputs),'four_source_snapshots_equal':True,'all_copied_git_blobs_match_snapshots':True,
 'attempts':[{'path':'attempts/'+attempts[0],'exit_code':3,'target_status':fc,'original_test_logs':173,'supplemental_dummy_test_log':1,'completed_rust_summaries':sum(x['complete_rust_summaries'] for x in logs),'completed_rust_counts':dict(counts),'format_individual_ok_before_timeout':27,'format_timeout_seconds':60},{'path':'attempts/'+attempts[1],'exit_code':0,'target_status':rc,'test_logs':1,'rust_passed':28,'rust_failed':0,'rust_ignored':0,'format_timeout_seconds':60}],
 'conclusion':'All 174 distinct targets have a passing result across two attempts on the identical recorded 092 source boundary. The full attempt was 173 PASSED plus 1 TIMEOUT and exit 3; it was not a fully green workspace run. The unchanged focused rerun passed the format target with 28 Rust tests under the same 60-second limit. No cached test results were reported in either BEP.',
 'counting':'2374 completed Rust test passes across complete summaries in the two attempts, plus 11 ignored. The 27 individual ok lines from the timed-out format attempt are preserved but not added to that total.',
 'selected_artifact_count':sum(a['copied'] for a in artifacts),'considered_but_unavailable':[a['label'] for a in artifacts if not a['copied']],
 'limits':['This checkpoint predates cleanup helper integration and recovered clock runtime, and does not qualify those later changes.','Only selected executable artifacts are archived; all 174 test executables are not claimed preserved. The original server ELF was overwritten and is not included.', 'Original recorder omitted the root dummy test log. Its current retained log and XML are supplemental, with post-hoc provenance and no claim of contemporaneous output digest.','The 1354 snapshots cover tracked repository inputs except prior categorical evidence; external dependency/toolchain bytes are outside this source-copy boundary.','No diagnosis of the original format timeout is asserted. A passing isolated rerun does not erase it.'],
 'log_receipts':logs}
write('verification.json',verification)
shutil.copy2(Path(__file__),out/'collect.py')
receipts=[]
for p in sorted(out.rglob('*')):
 if p.is_file():receipts.append({'path':str(p.relative_to(out)),'bytes':p.stat().st_size,'sha256':sha(p),'mode':p.stat().st_mode&0o777})
write('receipts.json',{'artifacts':receipts})
print(json.dumps({'source_count':len(inputs),'source_bytes':sum(r['bytes'] for r in source_receipts),'selected_artifacts':verification['selected_artifact_count'],'unavailable':verification['considered_but_unavailable'],'evidence_files':len(receipts)+1,'verification_sha256':sha(out/'verification.json'),'receipts_sha256':sha(out/'receipts.json')},indent=2),flush=True)
