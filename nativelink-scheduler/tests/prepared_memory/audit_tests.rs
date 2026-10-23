// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Post-freeze counterexamples from auditing the candidate's existing contract.

use super::*;

#[test]
fn audit_preparation_coverage_has_the_existing_lease_bound() {
    let mut fixture = Fixture::new(false, 3, 32_768);
    let attempt = fixture.assign();
    let stage = fixture.prepare(attempt, None);
    let mut evidence = fixture.proof(stage);
    evidence.until = h::LEASE_TICKS + 1;
    let before = fixture.model.snapshot().unwrap();
    let result = fixture
        .model
        .apply(&Command::Prepare { stage, evidence }, &fixture.facts);
    assert!(
        result.is_err(),
        "preparation coverage must obey the existing 100-tick lease bound; got {result:?}"
    );
    assert_eq!(fixture.model.snapshot().unwrap(), before);
}

#[test]
fn audit_write_identity_is_local_to_its_stage() {
    let mut fixture = Fixture::new(false, 3, 32_768);
    let first = fixture.assign();
    let mut first_command = fixture.stage_command(first, None);
    let first_key = fixture.candidates[&(200 + first.id.0)]
        .required
        .first()
        .unwrap()
        .key;
    let first_stage = 300 + first.id.0;
    if let Command::Stage { writes, .. } = &mut first_command {
        *writes = BTreeMap::from([(7, first_key)]);
    }
    fixture.apply(first_command);
    fixture.apply(Command::Prepare {
        stage: first_stage,
        evidence: fixture.proof(first_stage),
    });
    assert!(matches!(
        fixture.accept(first_stage),
        Disposition::RetryScheduled { .. }
    ));
    let second = fixture.assign();
    let mut command = fixture.stage_command(second, None);
    let second_key = fixture.candidates[&(200 + second.id.0)]
        .required
        .first()
        .unwrap()
        .key;
    if let Command::Stage { writes, .. } = &mut command {
        *writes = BTreeMap::from([(7, second_key)]);
    }
    let result = fixture.model.apply(&command, &fixture.facts);
    assert_eq!(
        result,
        Ok(Reply::Applied),
        "the external write ID belongs to its stage"
    );
    for (stage, key) in [(first_stage, first_key), (300 + second.id.0, second_key)] {
        let object = fixture
            .facts
            .physical
            .iter()
            .find(|row| row.key == key)
            .unwrap()
            .object
            .clone();
        let receipt = h::Receipt {
            stage,
            write: 7,
            key,
            object,
            success: true,
        };
        fixture.facts.receipts.insert(receipt.clone());
        fixture.apply(Command::Observe(receipt));
    }
    assert!(fixture.model.view().stages[&first_stage].outcomes[&7]);
    assert!(fixture.model.view().stages[&(300 + second.id.0)].outcomes[&7]);
    let bytes = fixture.model.snapshot().unwrap();
    assert_eq!(Model::restore(&bytes).unwrap().view(), fixture.model.view());
}

#[test]
fn audit_repeated_abandon_is_a_refusal_without_mutation() {
    let mut fixture = Fixture::new(false, 3, 32_768);
    let attempt = fixture.assign();
    let stage = fixture.prepare(attempt, None);
    fixture.apply(Command::Abandon(stage));
    let before = fixture.model.snapshot().unwrap();
    let result = fixture
        .model
        .apply(&Command::Abandon(stage), &fixture.facts);
    assert!(
        result.is_err(),
        "an abandoned stage is no longer open; got {result:?}"
    );
    assert_eq!(fixture.model.snapshot().unwrap(), before);
}

#[test]
fn audit_duplicate_admission_is_a_refusal_without_mutation() {
    let mut fixture = Fixture::new(false, 3, 32_768);
    let before = fixture.model.snapshot().unwrap();
    let result = fixture.model.apply(
        &Command::Admit {
            operation: a::OperationId(1),
            request: fixture.request.clone(),
        },
        &fixture.facts,
    );
    assert!(
        result.is_err(),
        "the outer admission identity is single-use; got {result:?}"
    );
    assert_eq!(fixture.model.snapshot().unwrap(), before);
}

#[test]
fn audit_admission_resolves_an_absent_cache_head_atomically() {
    let captured = original("audit-cache-miss", 3);
    let mut wire = serde_json::to_value(captured.payload.unwrap()).unwrap();
    let bytes: Vec<u8> = serde_json::from_value(wire["execute"].clone()).unwrap();
    let mut execute = ExecuteRequest::decode(bytes.as_slice()).unwrap();
    execute.skip_cache_lookup = false;
    wire["execute"] = serde_json::to_value(execute.encode_to_vec()).unwrap();
    let parts: Vec<Vec<u8>> = ["execute", "queued", "action", "command"]
        .iter()
        .map(|name| serde_json::from_value(wire[*name].clone()).unwrap())
        .collect();
    let mut hash = Sha256::new();
    hash.update(b"nativelink-request-v1");
    hash.update(1_u32.to_be_bytes());
    hash.update([0]);
    for part in [
        parts[0].as_slice(),
        &[],
        parts[1].as_slice(),
        context().principal.as_bytes(),
        parts[2].as_slice(),
        parts[3].as_slice(),
    ] {
        hash.update(u64::try_from(part.len()).unwrap().to_be_bytes());
        hash.update(part);
    }
    wire["sha256"] = serde_json::to_value(<[u8; 32]>::from(hash.finalize())).unwrap();
    let mut request = m1::Reference::captured_request_facts(wire.clone()).unwrap();
    request.payload = Some(Box::new(serde_json::from_value(wire).unwrap()));
    assert!(!request.skip_cache_lookup);
    let mut model = Model::new(
        context(),
        h::Scenario {
            actions: BTreeMap::new(),
            candidates: BTreeMap::new(),
        },
        vec![],
    )
    .unwrap();
    model
        .apply(
            &Command::Admit {
                operation: a::OperationId(1),
                request,
            },
            &h::Facts::default(),
        )
        .unwrap();
    assert_eq!(
        model.view().scheduler.executions[0].phase,
        a::Phase::Queued,
        "outer admission resolves an absent cache head before returning"
    );
    assert!(model.view().scheduler.effects.is_empty());
    let bytes = model.snapshot().unwrap();
    assert_eq!(Model::restore(&bytes).unwrap().view(), model.view());
    let current = String::from_utf8(bytes).unwrap();
    let old = current.replacen(
        "independent-prepared-memory-history-v2",
        "independent-prepared-memory-history-v1",
        1,
    );
    assert_ne!(old, current, "the test must replace the current format");
    assert!(Model::restore(old.as_bytes()).is_err());
}
