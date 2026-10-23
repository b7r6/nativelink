#!/usr/bin/env python3
# Copyright 2026 The NativeLink Authors. All rights reserved.
#
# Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    See LICENSE file for details
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise the real current redb and pinned-parent synchronization boundary."""
import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

TEST = 'authority::journal::canonical_owner::tests::syscall_witness'
MARKERS = ['CURRENT_BEGIN', 'COMMIT_ACK', 'CURRENT_ACK', 'SUCCESS_ALIVE', 'RELEASED', 'REFUSED', 'REPAIR_OBSERVED']

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def parse_trace(path):
    calls, marks = [], {}
    counts = collections.Counter()
    pending = {}
    for line_index, raw in enumerate(path.read_text().splitlines()):
        m = re.match(r'^(\d+)\s+(.*)$', raw)
        if not m:
            continue
        pid, line = int(m[1]), m[2]
        if '<unfinished ...>' in line:
            pending[pid] = line.replace('<unfinished ...>', '')
            continue
        resumed = re.match(r'<\.\.\. \w+ resumed>(.*)', line)
        if resumed:
            line = pending.pop(pid) + resumed[1]
        for marker in MARKERS:
            if 'NL_OWNER_' + marker in line:
                marks[marker] = line_index
        m = re.match(r'(fsync|fdatasync)\(\d+<([^>]*)>\)\s+= (.*)', line)
        if m:
            syscall, file, result = m.groups()
            counts[pid, syscall] += 1
            calls.append({'pid': pid, 'syscall': syscall, 'path': file, 'result': result,
                          'ordinal_in_tracee': counts[pid, syscall], 'line': line_index})
    return calls, marks

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--test-binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--baseline-only', action='store_true')
    args = parser.parse_args()
    binary = args.test_binary.resolve(strict=True)
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    (out / 'roots').mkdir()
    results = []

    def invoke(name, mode, root, trace=False, fault=None, expected_refusal=False):
        env = dict(os.environ, NL_OWNER_MODE=mode, NL_OWNER_ROOT=str(root))
        if expected_refusal:
            env['NL_OWNER_EXPECT_REFUSAL'] = '1'
        command = [str(binary), '--exact', TEST, '--ignored', '--test-threads=1', '--nocapture']
        trace_path = out / (name + '.strace')
        if trace:
            prefix = ['strace', '-f', '-qq', '-yy', '-s', '512', '-e', 'trace=fsync,fdatasync,write,writev', '-o', str(trace_path)]
            if fault:
                prefix += ['-e', f'inject={fault["syscall"]}:error=EIO:when={fault["ordinal_in_tracee"]}']
            command = prefix + command
        try:
            result = subprocess.run(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=90)
            returncode, output, timed_out = result.returncode, result.stdout, False
        except subprocess.TimeoutExpired as error:
            returncode, output, timed_out = None, error.stdout or b'', True
        log = out / (name + '.log')
        log.write_bytes(output)
        receipt = {'name': name, 'mode': mode, 'command': command,
                   'environment': {k:v for k,v in env.items() if k.startswith('NL_OWNER_')},
                   'returncode': returncode, 'timed_out': timed_out, 'log_sha256': digest(log)}
        if trace:
            calls, marks = parse_trace(trace_path)
            receipt.update(calls=calls, markers=marks, trace_sha256=digest(trace_path))
        results.append(receipt)
        # Preserve the receipt even if a semantic assertion below detects a fault.
        (out / 'attempts.json').write_text(json.dumps(results, indent=2) + '\n')
        assert returncode == 0, (name, returncode, timed_out, output.decode()[-2000:])
        if mode != 'crash':
            assert 'test result: ok. 1 passed; 0 failed;' in output.decode(), name
        else:
            assert 'NL_OWNER_ABRUPT_EXIT' in output.decode(), name
        return receipt

    def prepare(name, scenario):
        root = out / 'roots' / name
        if scenario != 'cold':
            invoke(name + '-seed', 'seed', root)
        if scenario == 'repair':
            invoke(name + '-crash', 'crash', root)
        return root

    for scenario in ['cold', 'reopen', 'repair']:
        root = prepare(scenario, scenario)
        baseline = invoke(scenario, scenario, root, trace=True)
        calls, marks = baseline['calls'], baseline['markers']
        assert marks['CURRENT_BEGIN'] < marks['COMMIT_ACK'] < marks['CURRENT_ACK'] < marks['SUCCESS_ALIVE'] < marks['RELEASED'], scenario
        journal = [c for c in calls if marks['CURRENT_BEGIN'] < c['line'] < marks['COMMIT_ACK']]
        parent = [c for c in calls if marks['COMMIT_ACK'] < c['line'] < marks['CURRENT_ACK']]
        assert journal and all(c['syscall'] == 'fdatasync' and c['path'] == str(root / 'journal/authority.redb') and c['result'] == '0' for c in journal), (scenario, journal)
        assert len(parent) == 1 and parent[0]['syscall'] == 'fsync' and parent[0]['path'] == str(root / 'journal') and parent[0]['result'] == '0', (scenario, parent)
        assert len({c['pid'] for c in journal + parent}) == 1, scenario
        assert ('REPAIR_OBSERVED' in marks) == (scenario == 'repair'), (scenario, marks)
        if args.baseline_only:
            continue
        for position, target in enumerate(journal + parent):
            name = f'{scenario}-fault-{position + 1}'
            root = prepare(name, scenario)
            attempt = invoke(name, scenario, root, trace=True, fault=target, expected_refusal=True)
            calls, marks = attempt['calls'], attempt['markers']
            assert 'CURRENT_BEGIN' in marks and 'REFUSED' in marks and 'SUCCESS_ALIVE' not in marks, (name, marks)
            failures = [c for c in calls if 'INJECTED' in c['result'] and 'EIO' in c['result']]
            current = [c for c in failures if marks['CURRENT_BEGIN'] < c['line'] < marks['REFUSED']]
            assert len(current) == 1, (name, failures, marks)
            actual = current[0]
            assert actual['syscall'] == target['syscall'] and actual['ordinal_in_tracee'] == target['ordinal_in_tracee'], (name, actual, target)
            wanted_path = root / ('journal/authority.redb' if target['syscall'] == 'fdatasync' else 'journal')
            assert actual['path'] == str(wanted_path), (name, actual)
            if target['syscall'] == 'fdatasync':
                assert 'COMMIT_ACK' not in marks, (name, marks)
            else:
                assert marks['COMMIT_ACK'] < actual['line'] and 'CURRENT_ACK' not in marks, (name, marks)
            assert (root / 'journal/authority.redb').is_file(), name
    manifest = {'binary': str(binary), 'binary_sha256': digest(binary), 'script_sha256': digest(Path(__file__)),
                'baseline_only': args.baseline_only, 'attempts': results,
                'claim': ('Observed real current file and pinned-parent syncs before owner release and explicit repair callback observation. ' + ('Targeted EIO refusals. ' if not args.baseline_only else 'No injected faults in this run. ') + 'Not power-loss or production qualification.')}
    (out / 'verification.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({'passed': True, 'attempts': len(results), 'verification': str(out / 'verification.json')}))

if __name__ == '__main__':
    main()
