import difflib
import gzip
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

ROOT = Path('/tmp/nativelink-history-format')
OUT = Path('/tmp/nativelink-categorical-evidence/history-format')
FORMAT = 'nativelink-scheduler/src/authority/format.rs'
JOURNAL = 'nativelink-scheduler/src/authority/journal.rs'
TEST = 'nativelink-scheduler/tests/authority_format_test.rs'
FILES = [FORMAT, JOURNAL, TEST]
original = {path: (ROOT / path).read_bytes() for path in FILES}
env = dict(os.environ, CARGO_TARGET_DIR='/tmp/nativelink-categorical-runtime-target', CARGO_BUILD_JOBS='8')

def sha(value):
    return hashlib.sha256(value).hexdigest()

def replace_once(text, old, new):
    assert text.count(old) == 1, (old, text.count(old))
    return text.replace(old, new, 1)

def staged_only(text):
    begin = text.index('fn validate_feature_append(')
    end = text.index('\n/// Determine the committed format', begin)
    portion = text[begin:end]
    portion = replace_once(portion,
        'replay_state(&mut applied, &mut state, entry, reader)?;',
        'let _ = &mut state; replay_format(&mut applied.format, &mut applied.membership, entry, reader)?;')
    portion = replace_once(portion,
        'replay_state(&mut applied, &mut state, &entry, reader)?;',
        'let _ = &mut state; replay_format(&mut applied.format, &mut applied.membership, &entry, reader)?;')
    return text[:begin] + portion + text[end:]

mutants = [
    ('admission_pause_disabled', FORMAT,
     lambda s: replace_once(s, 'let paused = state.pending.as_ref().is_some_and(|pending| {',
                             'let paused = false && state.pending.as_ref().is_some_and(|pending| {'),
     'history_prepare_pauses_before_enqueue_and_abort_releases_cancelled_callers'),
    ('committed_applied_epoch_gate_disabled', JOURNAL,
     lambda s: replace_once(s, 'if active.required != required || active.freezes_proposals() {',
                             'if false && (active.required != required || active.freezes_proposals()) {'),
     'history_committed_activation_waits_for_applied_epoch_before_enqueuing_work'),
    ('staged_core_preflight_omitted', JOURNAL, staged_only,
     'history_append_stages_preceding_work_and_refuses_infeasible_activation_atomically'),
    ('unsupported_history_reader_admitted', FORMAT,
     lambda s: replace_once(s, '|| !reader.supports(&self.required)',
                             '|| (!reader.supports(&self.required) && !self.required.contains(&ACCEPTED_HISTORY))'),
     'history_commands_gate_append_apply_and_unapplied_activation_suffix'),
    ('older_feature_epoch_accepted', JOURNAL,
     lambda s: replace_once(s, 'batch.feature_epoch != active.feature_epoch',
                             'batch.feature_epoch > active.feature_epoch'),
     'history_commands_gate_append_apply_and_unapplied_activation_suffix'),
    ('duplicate_abort_enqueued_twice', FORMAT,
     lambda s: replace_once(s, 'if prior == owner {\n                        return Ok(ControlAdmission::Wait);',
                             'if prior == owner {\n                        return Ok(ControlAdmission::Enqueue);'),
     'authority::format::tests::duplicate_abort_waits_for_original_decision_position'),
    ('stale_prepare_metadata_accepted', FORMAT,
     lambda s: replace_once(s, 'if active.required != state.required || active.prepared.is_some() {',
                             'if false && (active.required != state.required || active.prepared.is_some()) {'),
     'history_second_preparation_retries_after_commit_before_apply'),
    ('stale_decision_owner_accepted', FORMAT,
     lambda s: replace_once(s, 'if prior.term >= owner.term {', 'if false && prior.term >= owner.term {'),
     'authority::format::tests::abort_waits_for_live_activation_but_successor_can_resolve_old_owner'),
    ('foreign_history_namespace_accepted', JOURNAL,
     lambda s: replace_once(s, 'namespace != accepted_history_namespace(self.format.domain)',
                             'namespace != accepted_history_namespace(self.format.domain) && namespace == [0; 32]'),
     'history_snapshot_component_and_namespace_mismatches_are_atomic'),
]

def run(name, args, limit=180):
    log = OUT / (name + '.log')
    started = time.monotonic()
    with log.open('wb') as output:
        child = subprocess.Popen(args, cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = child.wait(timeout=limit)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()
            code = 'timeout'
    raw = log.read_bytes()
    with (OUT / (name + '.log.gz')).open('wb') as target:
        with gzip.GzipFile(filename='', mode='wb', fileobj=target, mtime=0) as zipped:
            zipped.write(raw)
    log.unlink()
    return dict(command=args, exit_code=code, elapsed_seconds=round(time.monotonic()-started,3),
                log_sha256=sha(raw), compressed_log=name+'.log.gz',
                compiled=(b'Running tests/authority_format_test.rs' in raw or b'Running unittests src/lib.rs' in raw),
                semantic_failure=b'panicked at' in raw and b'test result: FAILED.' in raw)

manifest = dict(base='552aaa890c', prior_qualification='/tmp/nativelink-categorical-evidence/history-format-v2-before-fixture-drain/manifest.json',
    opaque_core_patches=[
        dict(path='/tmp/nativelink-history-candidate-v1.patch', sha256='2c1ce10e6a5327ecf2ec74451705810d71675eba4dafd5f01ee2790e970f9de1'),
        dict(path='/tmp/nativelink-history-candidate-v1-followup.patch', sha256='fefc9567d266a194f4ba364e8f52638118c44e066e2a570fc8acfa003b6464d8')],
    source_sha256={path:sha(value) for path,value in original.items()},
    core_dependency_sha256={path:sha((ROOT/path).read_bytes()) for path in [
        'nativelink-scheduler/src/authority/mod.rs',
        'nativelink-scheduler/src/authority/accepted_history.rs',
        'nativelink-scheduler/src/authority/receipt_retirement.rs',
        'nativelink-scheduler/src/authority/types.rs']},
    mutants=[])

try:
    for name, path, transform, test in mutants:
        before = original[path].decode()
        after = transform(before)
        (ROOT/path).write_text(after)
        diff = ''.join(difflib.unified_diff(before.splitlines(True), after.splitlines(True), fromfile=path, tofile=path))
        (OUT/(name+'.patch')).write_text(diff)
        args = ['cargo','test','-p','nativelink-scheduler'] + (['--lib'] if test.startswith('authority::format::tests::') else ['--test','authority_format_test']) + [test,'--','--exact']
        result = run(name, args)
        result.update(name=name, file=path, test=test, mutated_source_sha256=sha(after.encode()),
                      patch_sha256=sha(diff.encode()))
        result['killed'] = result['exit_code'] == 101 and result['compiled'] and result['semantic_failure']
        manifest['mutants'].append(result)
        (ROOT/path).write_bytes(original[path])
        print(name, result['exit_code'], 'compiled', result['compiled'], 'killed', result['killed'], flush=True)
        (OUT/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
finally:
    for path, value in original.items():
        (ROOT/path).write_bytes(value)

manifest['restored_source_sha256'] = {path:sha((ROOT/path).read_bytes()) for path in FILES}
assert manifest['restored_source_sha256'] == manifest['source_sha256']
manifest['restored_run'] = run('restored_format', ['cargo','test','-p','nativelink-scheduler','--test','authority_format_test','--','--test-threads=4'])
(OUT/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
manifest['restored_unit_run'] = run('restored_units', ['cargo','test','-p','nativelink-scheduler','--lib','authority::format::tests','--','--test-threads=4'])
(OUT/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
print('restored', manifest['restored_run']['exit_code'], manifest['restored_unit_run']['exit_code'], flush=True)
assert all(item['killed'] for item in manifest['mutants'])
assert manifest['restored_run']['exit_code'] == 0
