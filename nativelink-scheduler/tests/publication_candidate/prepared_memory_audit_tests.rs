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

//! Added only after both independent source and test freezes.

use super::*;

#[test]
fn audit_preparation_coverage_has_the_existing_lease_bound() {
    let (mut state, _, attempt) = active(1, 3, 32_768);
    state.enable_prepared_memory().unwrap();
    stage_graph(&mut state, 1, attempt, 1);
    let mut evidence = state.stages()[&1].preparation.clone().unwrap();
    evidence.until = LEASE_TICKS + 1;
    let before = state.snapshot().unwrap();
    assert!(state.prepare(1, evidence).is_err());
    unchanged(&state, &before);
    recover(&mut state);
}

#[test]
fn audit_write_identity_is_local_to_its_stage() {
    let (mut state, _, first) = active(1, 3, 32_768);
    state.enable_prepared_memory().unwrap();
    let first_value = graph(&mut state, 1, first, 1, true);
    let first_report = memory_report(&state, first, 1);
    state
        .stage_memory(
            1,
            first_value.clone(),
            first_report,
            10,
            BTreeMap::from([(7, key(1))]),
        )
        .unwrap();
    prepare(&mut state, 1);
    assert_eq!(
        state.accept_prepared_memory(1, 10, 80).unwrap(),
        NoGraphOutcome::RetryScheduled
    );
    let second = assign(&mut state);
    let value = graph(&mut state, 1, second, 2, true);
    let report = memory_report(&state, second, 2);
    let result = state.stage_memory(2, value.clone(), report, 10, BTreeMap::from([(7, key(2))]));
    assert_eq!(result, Ok(()), "the external write ID belongs to its stage");
    assert_ne!(
        state.stages()[&1].internal_writes[&7],
        state.stages()[&2].internal_writes[&7]
    );
    state
        .observe(1, 7, key(1), first_value.result, true)
        .unwrap();
    assert!(state.stages()[&2].outcomes.is_empty());
    state.observe(2, 7, key(2), value.result, true).unwrap();
    assert!(state.stages()[&1].outcomes[&7]);
    assert!(state.stages()[&2].outcomes[&7]);
    recover(&mut state);
}

#[test]
fn audit_repeated_abandon_is_a_refusal_without_mutation() {
    let (mut state, _, attempt) = active(1, 3, 32_768);
    state.enable_prepared_memory().unwrap();
    stage_graph(&mut state, 1, attempt, 1);
    state.abandon(1).unwrap();
    let before = state.snapshot().unwrap();
    assert!(state.abandon(1).is_err());
    unchanged(&state, &before);
    recover(&mut state);
}

#[test]
fn audit_duplicate_admission_is_a_refusal_without_mutation() {
    let (mut state, request, _) = active(1, 3, 32_768);
    state.enable_prepared_memory().unwrap();
    let before = state.snapshot().unwrap();
    assert!(
        state
            .admit_request(1, "action".into(), &context(), request, 100)
            .is_err()
    );
    unchanged(&state, &before);
    recover(&mut state);
}

#[test]
fn audit_admission_resolves_an_absent_cache_head_atomically() {
    let request = request(
        1,
        false,
        false,
        "audit-cache-miss",
        &context().principal,
        false,
    );
    let mut state = real(&request);
    state.enable_prepared_memory().unwrap();
    state
        .admit_request(1, "action".into(), &context(), request, 100)
        .unwrap();
    assert_eq!(state.scheduler_view().executions[0].phase, a::Phase::Queued);
    assert!(state.scheduler_view().effects.is_empty());
    recover(&mut state);
}
