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

"""Check the owner surface against exact compiled dependency artifacts."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

PREFIX = """#![allow(dead_code, unused_imports)]
use nativelink_scheduler::authority::journal::{PreparedCanonicalOwner, OwnerDiagnostic, OwnerError};
use nativelink_canonical_filesystem::{SyncedNamespace, Ownership};
"""
CASES = {
    'typed_positive': (None, """async fn cold(token: SyncedNamespace) -> Result<OwnerDiagnostic, OwnerError> {
        let owner = PreparedCanonicalOwner::prepare_cold(token).await?;
        let diagnostic = owner.diagnostic(); owner.release().await?; Ok(diagnostic)
    }
    async fn reopen(token: SyncedNamespace) -> Result<OwnerDiagnostic, OwnerError> {
        let owner = PreparedCanonicalOwner::reopen_sealed(token).await?;
        let diagnostic = owner.diagnostic(); owner.release().await?; Ok(diagnostic)
    }"""),
    'raw_journal': ('E0616', 'fn probe(owner: PreparedCanonicalOwner) { let _ = owner.journal; }'),
    'raw_diagnostic': ('E0616', 'fn probe(owner: PreparedCanonicalOwner) { let _ = owner.diagnostic; }'),
    'clone': ('E0599', 'fn probe(owner: PreparedCanonicalOwner) { let _ = owner.clone(); }'),
    'into_journal': ('E0599', 'fn probe(owner: PreparedCanonicalOwner) { let _ = owner.into_journal(); }'),
    'into_singleton': ('E0599', 'fn probe(owner: PreparedCanonicalOwner) { let _ = owner.into_singleton(); }'),
    'write': ('E0599', 'fn probe(owner: PreparedCanonicalOwner) { let _ = owner.write(()); }'),
    'raft': ('E0599', 'fn probe(owner: PreparedCanonicalOwner) { let _ = owner.raft(); }'),
    'clock': ('E0599', 'fn probe(owner: PreparedCanonicalOwner) { let _ = owner.clock(); }'),
    'seal': ('E0599', 'fn probe(owner: PreparedCanonicalOwner) { let _ = owner.seal(); }'),
    'snapshot': ('E0599', 'fn probe(owner: PreparedCanonicalOwner) { let _ = owner.snapshot(); }'),
    'factory': ('E0599', 'fn probe() { let _ = PreparedCanonicalOwner::open_with_factory(|| ()); }'),
    'private_open': ('E0624', 'fn probe(token: SyncedNamespace) { let _ = PreparedCanonicalOwner::open(token, true); }'),
    'digest_substitution': ('E0308', 'fn probe() { let _ = PreparedCanonicalOwner::prepare_cold([1u8; 32]); }'),
    'ownership_substitution': ('E0308', 'fn probe(owner: std::sync::Arc<Ownership>) { let _ = PreparedCanonicalOwner::prepare_cold(owner); }'),
}

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scheduler', type=Path, required=True)
    parser.add_argument('--filesystem', type=Path, required=True)
    parser.add_argument('--dependency-dir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    scheduler = args.scheduler.resolve(strict=True)
    filesystem = args.filesystem.resolve(strict=True)
    dependencies = args.dependency_dir.resolve(strict=True)
    results = []
    for name, (expected, body) in CASES.items():
        source = out / (name + '.rs')
        source.write_text(PREFIX + body + '\n')
        command = ['rustc', '--edition=2024', '--crate-type=lib', '--emit=metadata',
                   '--error-format=json', '-L', 'dependency=' + str(dependencies),
                   '--extern', 'nativelink_scheduler=' + str(scheduler),
                   '--extern', 'nativelink_canonical_filesystem=' + str(filesystem),
                   '-o', str(out / (name + '.rmeta')), str(source)]
        try:
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=90)
            status, output, timed_out = result.returncode, result.stdout, False
        except subprocess.TimeoutExpired as error:
            status, output, timed_out = None, error.stdout or b'', True
        log = out / (name + '.log')
        log.write_bytes(output)
        codes = []
        for line in output.decode().splitlines():
            try:
                diagnostic = json.loads(line)
            except json.JSONDecodeError:
                continue
            if diagnostic.get('level') == 'error' and diagnostic.get('code'):
                codes.append(diagnostic['code']['code'])
        receipt = {'name': name, 'expected_error': expected, 'error_codes': codes,
                   'command': command, 'returncode': status, 'timed_out': timed_out,
                   'source_sha256': digest(source), 'log_sha256': digest(log)}
        results.append(receipt)
        (out / 'attempts.json').write_text(json.dumps(results, indent=2) + '\n')
        assert not timed_out, receipt
        if expected is None:
            assert status == 0, receipt
        else:
            assert status != 0 and expected in codes, receipt
    manifest = {'claim': 'External API compilation controls, not semantic mutation detection.',
                'scheduler_sha256': digest(scheduler), 'filesystem_sha256': digest(filesystem),
                'script_sha256': digest(Path(__file__)), 'attempts': results}
    (out / 'verification.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({'passed': True, 'cases': len(results)}))

if __name__ == '__main__':
    main()
