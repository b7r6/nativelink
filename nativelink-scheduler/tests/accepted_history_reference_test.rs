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

#[path = "accepted_history/reference.rs"]
mod reference;
#[path = "accepted_history/types.rs"]
mod types;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use reference::{Model, recipe};
use sha2::{Digest as _, Sha256};
use types::{
    ActionDigest, Algorithm, Association, Binding, Event, ExecutionFacts, Facts, HEADER_CREDIT,
    MAX_PREFIX_BYTES, MAX_RESPONSE_BYTES, Namespace, Outcome, Phase, PublicationOutcome,
    PublicationState, ROW_CREDIT, Reject,
};

const NAMESPACE: Namespace = Namespace([17; 32]);
const BINDING: Binding = Binding {
    attempt: 3,
    worker: 5,
    incarnation: 2,
};

const fn execution() -> ExecutionFacts {
    ExecutionFacts {
        action: ActionDigest {
            algorithm: Algorithm::Sha256,
            hash: [0xab; 32],
            size_bytes: 19,
        },
        phase: Phase::Open,
        authorized: Some(BINDING),
        accepted_operations: BTreeSet::new(),
        retirement_permitted: false,
    }
}

const fn binding_for(execution: u64) -> Binding {
    Binding {
        attempt: BINDING.attempt + execution - 1,
        ..BINDING
    }
}

const fn execution_with_id(id: u64) -> ExecutionFacts {
    let mut fact = execution();
    fact.authorized = Some(binding_for(id));
    fact
}

fn base_facts() -> Facts {
    Facts {
        epoch: 7,
        qualified_namespace: Some(NAMESPACE),
        executions: BTreeMap::from([(1, execution())]),
        next_effect: 13,
        other_future_effect_ids: 7,
        effect_capacity: 32,
        other_effect_slots: 4,
        metadata_capacity: 64 * 1024,
        other_metadata_bytes: 512,
    }
}

fn activate(model: &mut Model, facts: &Facts) {
    assert_eq!(
        model.apply(
            &Event::Activate {
                namespace: NAMESPACE
            },
            facts
        ),
        Ok(Outcome::Applied)
    );
}

fn terminal(facts: &mut Facts, execution: u64, response: &[u8], cacheable: bool, success: bool) {
    let fact = facts.executions.get_mut(&execution).unwrap();
    fact.phase = Phase::WorkerTerminal {
        original: binding_for(execution),
        response: Arc::from(response),
        cacheable,
        success,
    };
    fact.accepted_operations = BTreeSet::from([execution * 100 + 1, execution * 100 + 2]);
}

fn accept(model: &mut Model, facts: &mut Facts, execution: u64) -> u64 {
    let expected = facts.next_effect;
    assert_eq!(
        model.apply(
            &Event::AcceptTerminal {
                execution,
                proposal: None
            },
            facts
        ),
        Ok(Outcome::Allocated {
            effect: expected,
            next_effect: expected + 1
        })
    );
    facts.next_effect += 1;
    expected
}

fn observed(model: &Model, execution: u64, outcome: PublicationOutcome) -> Event {
    let row = &model.associations[&execution];
    Event::ObservePublication {
        epoch: row.epoch,
        effect: row.effect,
        object: row.recipe.object.clone(),
        outcome,
    }
}

#[test]
fn canonical_prefix_exact_payload_and_both_digest_algorithms() {
    let mut action = execution().action;
    action.hash = [0; 32];
    action.size_bytes = 0;
    let empty = recipe(NAMESPACE, &action, &[]).unwrap();
    let mut expected = vec![10, 66, 10, 64];
    expected.extend_from_slice(&[b'0'; 64]);
    expected.extend_from_slice(&[26, 0]);
    assert_eq!(empty.prefix, expected);
    assert_eq!(empty.object.size_bytes, 70);
    assert_eq!(empty.response_sha256, <[u8; 32]>::from(Sha256::digest([])));

    action.size_bytes = i64::MAX as u64;
    let response = vec![255; MAX_RESPONSE_BYTES];
    for algorithm in [Algorithm::Sha256, Algorithm::Blake3] {
        action.algorithm = algorithm;
        let derived = recipe(NAMESPACE, &action, &response).unwrap();
        assert_eq!(derived.prefix.len(), MAX_PREFIX_BYTES);
        assert_eq!(&derived.prefix[..4], &[10, 76, 10, 64]);
        assert_eq!(
            &derived.prefix[68..78],
            &[16, 255, 255, 255, 255, 255, 255, 255, 255, 127]
        );
        assert_eq!(&derived.prefix[78..], &[26, 128, 128, 16]);
        let object = [derived.prefix.as_slice(), &response].concat();
        assert_eq!(derived.object.size_bytes, object.len() as u64);
        let expected: [u8; 32] = match algorithm {
            Algorithm::Sha256 => Sha256::digest(&object).into(),
            Algorithm::Blake3 => *blake3::hash(&object).as_bytes(),
        };
        assert_eq!(derived.object.hash, expected);
    }
    action.size_bytes = i64::MAX as u64 + 1;
    assert_eq!(recipe(NAMESPACE, &action, &[]), Err(Reject::InvalidRecipe));
    action.size_bytes = 0;
    assert_eq!(
        recipe(NAMESPACE, &action, &vec![0; MAX_RESPONSE_BYTES + 1]),
        Err(Reject::InvalidRecipe)
    );
}

#[test]
fn fixed_policy_covers_errors_noncacheable_and_final_resource_failure() {
    for (cacheable, success, response) in [
        (true, true, b"success".as_slice()),
        (false, true, b"do_not_cache".as_slice()),
        (true, false, b"nonzero exit".as_slice()),
        (false, false, b"terminal infrastructure error".as_slice()),
        (
            true,
            false,
            b"final ResourceFailed with exact response".as_slice(),
        ),
    ] {
        let mut facts = base_facts();
        let mut model = Model::default();
        activate(&mut model, &facts);
        terminal(&mut facts, 1, response, cacheable, success);
        accept(&mut model, &mut facts, 1);
        assert_eq!(model.associations.len(), 1);
        assert_eq!(
            model.associations[&1].recipe.response_sha256,
            <[u8; 32]>::from(Sha256::digest(response))
        );
        assert_eq!(model.pins(), BTreeSet::from([(1, BINDING.attempt)]));
        let duplicate = Event::AcceptTerminal {
            execution: 1,
            proposal: None,
        };
        let before = model.clone();
        assert_eq!(model.apply(&duplicate, &facts), Ok(Outcome::Ignored));
        assert_eq!(model, before);
    }
}

#[test]
fn retry_stale_cache_hit_direct_update_and_pre_activation_terminal_do_not_publish() {
    let mut facts = base_facts();
    let mut model = Model::default();
    let completion = Event::AcceptTerminal {
        execution: 1,
        proposal: None,
    };
    assert_eq!(model.apply(&completion, &facts), Err(Reject::Disabled));
    activate(&mut model, &facts);
    // A retry-only memory failure leaves the independent execution Open.
    assert_eq!(model.apply(&completion, &facts), Ok(Outcome::Ignored));
    terminal(&mut facts, 1, b"late stale response", true, false);
    facts.executions.get_mut(&1).unwrap().authorized = None;
    assert_eq!(model.apply(&completion, &facts), Ok(Outcome::Ignored));
    assert!(model.associations.is_empty());
    // Cache hits, direct-client updates, and scheduler cancellation are all
    // supplied as non-worker terminal outcomes; none may consume a receipt.
    facts.executions.get_mut(&1).unwrap().phase = Phase::OtherTerminal;
    assert_eq!(model.apply(&completion, &facts), Ok(Outcome::Ignored));
    assert_eq!(
        model.apply(&Event::ReleaseUnused { execution: 1 }, &facts),
        Ok(Outcome::Applied)
    );
    assert_eq!(model.effect_slots(), 0);

    let mut old_facts = facts.clone();
    terminal(
        &mut old_facts,
        1,
        b"accepted before feature activation",
        true,
        true,
    );
    old_facts.executions.get_mut(&1).unwrap().authorized = Some(BINDING);
    let mut old = Model::default();
    activate(&mut old, &old_facts);
    assert_eq!(old.apply(&completion, &old_facts), Ok(Outcome::Ignored));
    assert_eq!(
        old.apply(&Event::Admit { execution: 1 }, &old_facts),
        Ok(Outcome::Ignored)
    );
    let restored = Model::restore(&old.snapshot(), &old_facts).unwrap();
    assert!(restored.associations.is_empty());
    assert!(restored.reserved.is_empty());
}

#[test]
fn acceptance_before_revocation_survives_and_cancelled_operation_does_not_project() {
    let mut facts = base_facts();
    let mut model = Model::default();
    activate(&mut model, &facts);
    terminal(&mut facts, 1, b"immutable accepted bytes", true, true);
    facts.executions.get_mut(&1).unwrap().accepted_operations = BTreeSet::from([101, 103]);
    accept(&mut model, &mut facts, 1);
    assert_eq!(model.operation(101, &facts), model.operation(103, &facts));
    assert!(model.operation(102, &facts).is_none());
    facts.executions.get_mut(&1).unwrap().authorized = None;
    let stored = observed(&model, 1, PublicationOutcome::Stored);
    let mut recovered = Model::restore(&model.snapshot(), &facts).unwrap();
    assert_eq!(recovered.apply(&stored, &facts), Ok(Outcome::Applied));
    assert_eq!(recovered.associations[&1].original, BINDING);
    assert_eq!(
        recovered.associations[&1].state,
        PublicationState::Materialized
    );
    assert_eq!(recovered.apply(&stored, &facts), Ok(Outcome::Ignored));
}

#[test]
fn proposed_namespace_prefix_fingerprint_payload_digest_and_algorithm_are_checked() {
    let mut facts = base_facts();
    let mut model = Model::default();
    activate(&mut model, &facts);
    terminal(
        &mut facts,
        1,
        b"unknown protobuf fields and noncanonical inner bytes survive",
        true,
        true,
    );
    let Phase::WorkerTerminal { response, .. } = &facts.executions[&1].phase else {
        unreachable!()
    };
    let good = recipe(NAMESPACE, &facts.executions[&1].action, response).unwrap();
    for mutation in 0..7 {
        let mut bad = good.clone();
        match mutation {
            0 => bad.object.namespace.0[0] ^= 1,
            1 => bad.prefix[0] ^= 1,
            2 => bad.response_sha256[0] ^= 1,
            3 => bad.object.hash[0] ^= 1,
            4 => bad.object.size_bytes += 1,
            5 => bad.object.algorithm = Algorithm::Blake3,
            6 => {
                bad = recipe(
                    NAMESPACE,
                    &facts.executions[&1].action,
                    b"different response",
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let before = model.clone();
        assert_eq!(
            model.apply(
                &Event::AcceptTerminal {
                    execution: 1,
                    proposal: Some(bad)
                },
                &facts
            ),
            Err(Reject::InvalidRecipe)
        );
        assert_eq!(model, before);
    }
    assert!(matches!(
        model.apply(
            &Event::AcceptTerminal {
                execution: 1,
                proposal: Some(good)
            },
            &facts
        ),
        Ok(Outcome::Allocated { .. })
    ));
}

#[test]
fn activation_and_admission_preflight_all_three_budgets_atomically() {
    for failure in 0..3 {
        let mut facts = base_facts();
        facts.executions.insert(2, execution_with_id(2));
        match failure {
            0 => facts.effect_capacity = facts.other_effect_slots + 1,
            1 => facts.metadata_capacity = facts.other_metadata_bytes + HEADER_CREDIT + ROW_CREDIT,
            2 => facts.next_effect = u64::MAX - facts.other_future_effect_ids - 1,
            _ => unreachable!(),
        }
        let mut model = Model::default();
        assert_eq!(
            model.apply(
                &Event::Activate {
                    namespace: NAMESPACE
                },
                &facts
            ),
            Err(if failure == 2 {
                Reject::Headroom
            } else {
                Reject::Capacity
            })
        );
        assert_eq!(model, Model::default());
    }
    let mut facts = base_facts();
    let mut model = Model::default();
    activate(&mut model, &facts);
    facts.executions.insert(2, execution_with_id(2));
    facts.effect_capacity = facts.other_effect_slots + 1;
    let before = model.clone();
    assert_eq!(
        model.apply(&Event::Admit { execution: 2 }, &facts),
        Err(Reject::Capacity)
    );
    assert_eq!(model, before);
    facts.effect_capacity += 1;
    assert_eq!(
        model.apply(&Event::Admit { execution: 2 }, &facts),
        Ok(Outcome::Applied)
    );
    assert_eq!(
        model.apply(&Event::Admit { execution: 2 }, &facts),
        Ok(Outcome::Ignored)
    );
    terminal(&mut facts, 1, b"first terminal", false, true);
    accept(&mut model, &mut facts, 1);
    terminal(&mut facts, 2, b"second terminal", false, false);
    accept(&mut model, &mut facts, 2);
    assert_eq!(model.effect_slots(), 2);
    assert!(model.reserved.is_empty());
}

#[test]
fn accepted_history_uses_global_cursor_after_ac_and_preserves_other_future_credit() {
    let mut facts = base_facts();
    facts.next_effect = u64::MAX - facts.other_future_effect_ids - 1;
    let mut model = Model::default();
    activate(&mut model, &facts);
    terminal(&mut facts, 1, b"last permitted history effect", false, true);
    let before = facts.clone();
    let effect = accept(&mut model, &mut facts, 1);
    assert_eq!(effect, before.next_effect);
    assert_eq!(facts.next_effect + facts.other_future_effect_ids, u64::MAX);
    assert_eq!(
        facts.other_future_effect_ids,
        before.other_future_effect_ids
    );
    assert_eq!(facts.executions, before.executions);

    let mut ordinary = base_facts();
    let mut ac_model = Model::default();
    activate(&mut ac_model, &ordinary);
    // Independent lifecycle emits its AC effect first. This model neither
    // invents that effect nor borrows its budget.
    ordinary.next_effect += 1;
    ordinary.other_future_effect_ids -= 1;
    terminal(&mut ordinary, 1, b"cacheable completion", true, true);
    assert_eq!(accept(&mut ac_model, &mut ordinary, 1), 14);
}

#[test]
fn lost_ack_restart_wrong_ack_and_late_ack_do_not_change_terminal_or_charge() {
    let mut facts = base_facts();
    let mut model = Model::default();
    activate(&mut model, &facts);
    terminal(&mut facts, 1, b"original terminal", true, true);
    accept(&mut model, &mut facts, 1);
    facts.executions.get_mut(&1).unwrap().retirement_permitted = true;
    let untouched_facts = facts.clone();
    let snapshot = model.snapshot();
    let mut restored = Model::restore(&snapshot, &facts).unwrap();
    for outcome in [PublicationOutcome::Unknown, PublicationOutcome::Failed] {
        let event = observed(&restored, 1, outcome);
        assert_eq!(restored.apply(&event, &facts), Ok(Outcome::Ignored));
        assert_eq!(restored.snapshot(), snapshot);
    }
    assert_eq!(
        restored.apply(&Event::Retire { execution: 1 }, &facts),
        Err(Reject::Busy)
    );
    for mutation in 0..4 {
        let mut event = observed(&restored, 1, PublicationOutcome::Stored);
        let Event::ObservePublication {
            epoch,
            effect,
            object,
            ..
        } = &mut event
        else {
            unreachable!()
        };
        match mutation {
            0 => *epoch += 1,
            1 => *effect += 1,
            2 => object.namespace.0[0] ^= 1,
            3 => object.hash[0] ^= 1,
            _ => unreachable!(),
        }
        assert_eq!(restored.apply(&event, &facts), Ok(Outcome::Ignored));
        assert_eq!(restored.snapshot(), snapshot);
    }
    let event = observed(&restored, 1, PublicationOutcome::Stored);
    let credit = restored.metadata_credit();
    assert_eq!(restored.apply(&event, &facts), Ok(Outcome::Applied));
    assert_eq!(restored.effect_slots(), 0);
    assert_eq!(restored.metadata_credit(), credit);
    assert!(restored.pins().is_empty());
    assert_eq!(restored.apply(&event, &facts), Ok(Outcome::Ignored));
    assert_eq!(
        restored.apply(&Event::Retire { execution: 1 }, &facts),
        Ok(Outcome::Applied)
    );
    assert_eq!(restored.metadata_credit(), HEADER_CREDIT);
    assert_eq!(restored.apply(&event, &facts), Ok(Outcome::Ignored));
    assert!(restored.associations.is_empty());
    assert_eq!(facts, untouched_facts);
}

#[test]
fn identical_objects_have_distinct_acceptance_provenance_and_independent_retirement() {
    let mut facts = base_facts();
    facts.executions.insert(2, execution_with_id(2));
    let mut model = Model::default();
    activate(&mut model, &facts);
    for execution in [1, 2] {
        terminal(
            &mut facts,
            execution,
            b"identical accepted bytes",
            true,
            true,
        );
        accept(&mut model, &mut facts, execution);
    }
    assert_eq!(
        model.associations[&1].recipe.object,
        model.associations[&2].recipe.object
    );
    assert_ne!(model.associations[&1].effect, model.associations[&2].effect);
    let event = observed(&model, 1, PublicationOutcome::Stored);
    assert_eq!(model.apply(&event, &facts), Ok(Outcome::Applied));
    assert_eq!(model.associations[&2].state, PublicationState::Pending);
    assert_eq!(model.pins(), BTreeSet::from([(2, binding_for(2).attempt)]));
}

#[test]
fn bounded_metadata_contains_no_response_copy_and_restore_rechecks_bindings() {
    let mut facts = base_facts();
    let mut model = Model::default();
    activate(&mut model, &facts);
    let response = vec![0xb7; MAX_RESPONSE_BYTES];
    terminal(&mut facts, 1, &response, false, false);
    accept(&mut model, &mut facts, 1);
    assert!(model.snapshot().len() < model.metadata_credit());
    assert!(model.snapshot().len() < ROW_CREDIT);
    let frozen = model.snapshot();
    for mutation in 0..9 {
        let mut changed = model.clone();
        let row = changed.associations.get_mut(&1).unwrap();
        match mutation {
            0 => row.recipe.prefix[0] ^= 1,
            1 => row.recipe.response_sha256[0] ^= 1,
            2 => row.original.attempt += 1,
            3 => row.execution += 1,
            4 => row.effect = facts.next_effect,
            5 => row.recipe.object.namespace.0[0] ^= 1,
            6 => row.recipe.object.hash[0] ^= 1,
            7 => row.epoch = facts.epoch + 1,
            8 => row.epoch = 0,
            _ => unreachable!(),
        }
        assert_eq!(
            Model::restore(&changed.snapshot(), &facts),
            Err(Reject::Snapshot)
        );
    }
    let mut wrong_payload = facts.clone();
    terminal(
        &mut wrong_payload,
        1,
        b"changed accepted response",
        false,
        false,
    );
    assert_eq!(
        Model::restore(&frozen, &wrong_payload),
        Err(Reject::Snapshot)
    );
    let mut wrong_action = facts.clone();
    wrong_action.executions.get_mut(&1).unwrap().action.hash[0] ^= 1;
    assert_eq!(
        Model::restore(&frozen, &wrong_action),
        Err(Reject::Snapshot)
    );
    let mut wrong_namespace = facts.clone();
    wrong_namespace.qualified_namespace = Some(Namespace([18; 32]));
    assert_eq!(
        Model::restore(&frozen, &wrong_namespace),
        Err(Reject::Namespace)
    );
    assert_eq!(Model::restore(&frozen, &facts).unwrap(), model);
}

#[test]
fn worst_case_json_metadata_credit_is_independent_of_terminal_response_length() {
    let mut row = Association {
        epoch: u64::MAX,
        effect: u64::MAX,
        execution: u64::MAX,
        original: Binding {
            attempt: u64::MAX,
            worker: u64::MAX,
            incarnation: u64::MAX,
        },
        recipe: recipe(Namespace([255; 32]), &execution().action, b"").unwrap(),
        state: PublicationState::Materialized,
    };
    // An upper bound on decimal JSON widths, including every hash octet.
    row.recipe.prefix = vec![255; MAX_PREFIX_BYTES];
    row.recipe.response_sha256 = [255; 32];
    row.recipe.object.hash = [255; 32];
    row.recipe.object.size_bytes = u64::MAX;
    row.recipe.object.algorithm = Algorithm::Blake3;
    let row_bytes = serde_json::to_vec(&row).unwrap().len();
    assert!(row_bytes + 24 < ROW_CREDIT, "row bytes: {row_bytes}");
    let empty = Model {
        namespace: Some(Namespace([255; 32])),
        ..Model::default()
    };
    assert!(empty.snapshot().len() < HEADER_CREDIT);
    let mut many = empty;
    for execution in 1..=32 {
        let mut copy = row.clone();
        copy.execution = execution;
        many.associations.insert(execution, copy);
    }
    assert!(many.snapshot().len() < many.metadata_credit());
}

#[test]
fn every_snapshot_cut_replays_the_same_suffix_and_preserves_prepaid_admissions() {
    let mut open = base_facts();
    open.executions.insert(2, execution_with_id(2));
    let mut completed = open.clone();
    terminal(
        &mut completed,
        1,
        b"accepted bytes survive every recovery cut",
        false,
        false,
    );
    let mut allocated = completed.clone();
    allocated.next_effect += 1;
    let mut retireable = allocated.clone();
    retireable
        .executions
        .get_mut(&1)
        .unwrap()
        .retirement_permitted = true;
    let Phase::WorkerTerminal { response, .. } = &completed.executions[&1].phase else {
        unreachable!()
    };
    let object = recipe(NAMESPACE, &completed.executions[&1].action, response)
        .unwrap()
        .object;
    let epoch = completed.epoch;
    let effect = completed.next_effect;
    let observation = |outcome| Event::ObservePublication {
        epoch,
        effect,
        object: object.clone(),
        outcome,
    };
    let steps = [
        (
            Event::Activate {
                namespace: NAMESPACE,
            },
            open.clone(),
            open.clone(),
        ),
        (
            Event::AcceptTerminal {
                execution: 1,
                proposal: None,
            },
            completed,
            allocated.clone(),
        ),
        (
            observation(PublicationOutcome::Unknown),
            allocated.clone(),
            allocated.clone(),
        ),
        (
            observation(PublicationOutcome::Stored),
            allocated.clone(),
            allocated,
        ),
        (
            Event::Retire { execution: 1 },
            retireable.clone(),
            retireable,
        ),
    ];
    let mut expected = Model::default();
    let mut cuts = vec![(expected.snapshot(), open)];
    for (event, before, after) in &steps {
        expected.apply(event, before).unwrap();
        cuts.push((expected.snapshot(), after.clone()));
    }
    assert_eq!(expected.reserved, BTreeSet::from([2]));
    assert!(expected.associations.is_empty());
    for (cut, (snapshot, facts)) in cuts.iter().enumerate() {
        let mut recovered = Model::restore(snapshot, facts).unwrap();
        for (event, before, _) in &steps[cut..] {
            recovered.apply(event, before).unwrap();
        }
        assert_eq!(recovered, expected, "recovery cut {cut}");
    }
}

#[test]
fn bounded_fair_success_schedule_drains_pending_effects_without_changing_terminal_facts() {
    let mut facts = base_facts();
    facts.executions = (1..=20).map(|id| (id, execution_with_id(id))).collect();
    let mut model = Model::default();
    activate(&mut model, &facts);
    for id in 1_u64..=20 {
        terminal(&mut facts, id, &id.to_le_bytes(), id % 2 == 0, id % 3 == 0);
        facts.executions.get_mut(&id).unwrap().accepted_operations = BTreeSet::from([100 + id]);
        accept(&mut model, &mut facts, id);
    }
    let terminals = facts.clone();
    let metadata_credit = model.metadata_credit();
    let schedule = (1..=20).rev().step_by(2).chain((1..=19).step_by(2));
    for (index, id) in schedule.enumerate() {
        for outcome in [PublicationOutcome::Unknown, PublicationOutcome::Failed] {
            let event = observed(&model, id, outcome);
            assert_eq!(model.apply(&event, &facts), Ok(Outcome::Ignored));
        }
        let event = observed(&model, id, PublicationOutcome::Stored);
        assert_eq!(model.apply(&event, &facts), Ok(Outcome::Applied));
        assert_eq!(model.effect_slots(), 19 - index);
        assert_eq!(model.metadata_credit(), metadata_credit);
        if index == 9 {
            model = Model::restore(&model.snapshot(), &facts).unwrap();
        }
    }
    assert!(model.pins().is_empty());
    assert_eq!(facts, terminals);
    assert_eq!(model.associations.len(), 20);
}

#[test]
fn zero_caller_identities_are_valid_but_zero_allocator_identities_are_not() {
    let namespace = Namespace([0; 32]);
    let original = Binding {
        attempt: 1,
        worker: 0,
        incarnation: 0,
    };
    let mut facts = base_facts();
    facts.epoch = 0;
    facts.qualified_namespace = Some(namespace);
    facts.executions.get_mut(&1).unwrap().authorized = Some(original);
    let mut model = Model::default();
    assert_eq!(
        model.apply(&Event::Activate { namespace }, &facts),
        Ok(Outcome::Applied)
    );
    facts.executions.get_mut(&1).unwrap().phase = Phase::WorkerTerminal {
        original,
        response: Arc::from(b"caller identities may be zero".as_slice()),
        cacheable: true,
        success: true,
    };
    accept(&mut model, &mut facts, 1);
    assert_eq!(Model::restore(&model.snapshot(), &facts), Ok(model.clone()));
    let event = observed(&model, 1, PublicationOutcome::Stored);
    assert_eq!(model.apply(&event, &facts), Ok(Outcome::Applied));
    assert_eq!(model.associations[&1].epoch, 0);
    assert_eq!(model.associations[&1].original, original);
    for field in 0..3 {
        let mut bad = model.clone();
        let row = bad.associations.get_mut(&1).unwrap();
        match field {
            0 => row.effect = 0,
            1 => row.original.attempt = 0,
            2 => row.execution = 0,
            _ => unreachable!(),
        }
        assert_eq!(
            Model::restore(&bad.snapshot(), &facts),
            Err(Reject::Snapshot)
        );
    }
    for field in 0..2 {
        let mut invalid = base_facts();
        if field == 0 {
            invalid.executions.insert(0, execution());
        } else {
            invalid.next_effect = 0;
        }
        let mut empty = Model::default();
        assert_eq!(
            empty.apply(
                &Event::Activate {
                    namespace: NAMESPACE
                },
                &invalid
            ),
            Err(Reject::Facts)
        );
        assert_eq!(empty, Model::default());
    }
    let mut zero_attempt_facts = base_facts();
    let mut fresh = Model::default();
    activate(&mut fresh, &zero_attempt_facts);
    let zero_attempt = Binding {
        attempt: 0,
        ..BINDING
    };
    let fact = zero_attempt_facts.executions.get_mut(&1).unwrap();
    fact.authorized = Some(zero_attempt);
    fact.phase = Phase::WorkerTerminal {
        original: zero_attempt,
        response: Arc::from(b"invalid zero allocator ID".as_slice()),
        cacheable: true,
        success: true,
    };
    assert_eq!(
        fresh.apply(
            &Event::AcceptTerminal {
                execution: 1,
                proposal: None
            },
            &zero_attempt_facts
        ),
        Ok(Outcome::Ignored)
    );
    assert!(fresh.associations.is_empty());
}
