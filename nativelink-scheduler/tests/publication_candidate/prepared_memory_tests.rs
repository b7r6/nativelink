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

//! Candidate scenarios authored before inspecting the new independent oracle.

use super::*;

fn graph(
    state: &mut PublicationAuthority,
    algorithm: u8,
    attempt: a::AttemptView,
    id: u64,
    ready: bool,
) -> Candidate {
    let result = ActionResult {
        exit_code: 137,
        stdout_raw: format!("partial output {id}").into_bytes().into(),
        ..ActionResult::default()
    };
    let bytes = result.encode_to_vec();
    let object = Object::for_bytes(algorithm, &bytes).unwrap();
    state
        .install_physical_for(Some(attempt.execution), key(id), object, ready)
        .unwrap();
    Candidate {
        action: "action".into(),
        publisher: worker(attempt),
        result: object,
        result_bytes: bytes,
        response: a::ResultPayload::from_response(
            &ExecuteResponse {
                result: Some(result),
                status: Some(Status {
                    code: 8,
                    message: "memory pressure λ".into(),
                    ..Status::default()
                }),
                message: format!("exact failure response {id}"),
                ..ExecuteResponse::default()
            },
            MAX_PAYLOAD,
        )
        .unwrap(),
        closure: BTreeSet::from([Member {
            object,
            key: key(id),
            role: Role::Result,
        }]),
    }
}

fn memory_report(
    state: &PublicationAuthority,
    attempt: a::AttemptView,
    id: u64,
) -> PreparedMemoryReport {
    PreparedMemoryReport {
        id,
        epoch: a::ClusterEpoch(1),
        charged: reservation(state, attempt).charged,
        peak_memory_bytes: None,
        quiescent: true,
    }
}

fn stage_graph(
    state: &mut PublicationAuthority,
    algorithm: u8,
    attempt: a::AttemptView,
    id: u64,
) -> Candidate {
    let value = graph(state, algorithm, attempt, id, true);
    let report = memory_report(state, attempt, id);
    state
        .stage_memory(
            id,
            value.clone(),
            report,
            state.scheduler_view().now + 10,
            BTreeMap::from([(id, key(id))]),
        )
        .unwrap();
    prepare(state, id);
    value
}

fn capacity(state: &mut PublicationAuthority, bytes: u64) {
    let resources = a::Resources {
        cpu_millis: 20_000,
        memory_bytes: bytes,
        slots: 8,
    };
    state
        .scheduler_event(a::Event::Capacity {
            worker: a::WorkerId(1),
            incarnation: a::IncarnationId(1),
            session: a::SessionId(1),
            capacity: resources,
            available: resources,
            draining: false,
        })
        .unwrap();
}

fn unchanged(state: &PublicationAuthority, bytes: &[u8]) {
    assert_eq!(state.snapshot().unwrap(), bytes);
}

#[test]
fn prepared_memory_recomputes_current_capacity_in_both_directions() {
    for algorithm in [1, 9] {
        for (before_capacity, after_capacity, retry) in
            [(32_768, 3_072, false), (3_072, 32_768, true)]
        {
            let (mut state, _, attempt) = active(algorithm, 3, before_capacity);
            state.enable_prepared_memory().unwrap();
            let value = stage_graph(&mut state, algorithm, attempt, 1);
            assert!(current_attempt(&state, attempt.id).authorized);
            assert!(!reservation(&state, attempt).released);
            assert!(
                state.stages()[&1]
                    .memory
                    .as_ref()
                    .unwrap()
                    .decision
                    .is_none()
            );
            recover(&mut state);
            capacity(&mut state, after_capacity);
            let roots = state.retention_view();
            let outcome = state.accept_prepared_memory(1, 10, 100).unwrap();
            if retry {
                assert_eq!(outcome, NoGraphOutcome::RetryScheduled);
                assert_eq!(state.retention_view(), roots);
                assert!(state.accepted().is_empty());
                assert!(operation(&state, 1).terminal.is_none());
                assert_eq!(
                    state.scheduler_view().executions[0]
                        .reservation_target
                        .memory_bytes,
                    6_144
                );
            } else {
                let expected = a::TerminalResult::ResourceFailed {
                    result: value.response.clone(),
                    reason: a::ResourceFailureReason::NoLargerEligibleWorker,
                };
                assert_eq!(outcome, NoGraphOutcome::TerminalAccepted(expected.clone()));
                assert_eq!(terminal(&state, 1), expected);
                let accepted = &state.accepted()[&10];
                assert!(!accepted.eligible);
                assert_eq!(
                    state.retention_view().roots[&r::RootId(accepted.root)].until,
                    0
                );
                assert!(state.operations()[&1].root.is_some());
                assert_eq!(
                    state.operations()[&1].payload.as_ref(),
                    Some(&value.response)
                );
            }
            assert!(reservation(&state, attempt).released);
            assert!(state.heads().is_empty());
            no_sink(&state);
            recover(&mut state);
            state
                .scheduler_event(a::Event::WorkerUnavailable {
                    worker: attempt.worker,
                    incarnation: attempt.incarnation,
                    session: a::SessionId(1),
                })
                .unwrap();
            let bytes = state.snapshot().unwrap();
            assert_eq!(
                state.accept_prepared_memory(1, 10, 100).unwrap(),
                NoGraphOutcome::Duplicate
            );
            unchanged(&state, &bytes);
            assert!(state.accept_prepared_memory(1, 11, 100).is_err());
            unchanged(&state, &bytes);
        }
    }
}

#[test]
fn prepared_memory_final_roots_follow_only_newly_terminated_operations() {
    for algorithm in [1, 9] {
        for (memory_kb, limit, reason) in [
            (0, 32_768, a::ResourceFailureReason::MissingGrowthEvidence),
            (3, 3_072, a::ResourceFailureReason::NoLargerEligibleWorker),
        ] {
            let (mut state, request, attempt) = active(algorithm, memory_kb, limit);
            state.enable_prepared_memory().unwrap();
            state
                .admit_request(2, "action".into(), &context(), request.clone(), 100)
                .unwrap();
            state
                .admit_request(3, "action".into(), &context(), request, 100)
                .unwrap();
            state
                .scheduler_event(a::Event::Cancel {
                    operation: a::OperationId(2),
                })
                .unwrap();
            let value = stage_graph(&mut state, algorithm, attempt, 1);
            let expected = a::TerminalResult::ResourceFailed {
                result: value.response,
                reason,
            };
            assert_eq!(
                state.accept_prepared_memory(1, 10, 100).unwrap(),
                NoGraphOutcome::TerminalAccepted(expected.clone())
            );
            assert_eq!(terminal(&state, 1), expected);
            assert_eq!(terminal(&state, 3), expected);
            assert_eq!(terminal(&state, 2), a::TerminalResult::Cancelled);
            for id in [1, 3] {
                assert!(state.operations()[&id].root.is_some());
            }
            assert!(state.operations()[&2].root.is_none());
            assert!(state.heads().is_empty());
            assert!(state.terminal_acceptances().unwrap().is_empty());
            recover(&mut state);
            state.advance(100).unwrap();
            recover(&mut state);
            assert_eq!(terminal(&state, 1), expected);
        }
    }
}

#[test]
fn prepared_memory_three_attempts_preserve_prior_writes_and_final_reason() {
    for algorithm in [1, 9] {
        let (mut state, _, mut attempt) = active(algorithm, 3, 32_768);
        state.enable_prepared_memory().unwrap();
        for id in 1..=3 {
            let value = stage_graph(&mut state, algorithm, attempt, id);
            let stage_before = state.stages()[&id].clone();
            let retention = state.retention_view();
            let outcome = state.accept_prepared_memory(id, 10, 100).unwrap();
            if id < 3 {
                assert_eq!(outcome, NoGraphOutcome::RetryScheduled);
                assert_eq!(state.retention_view(), retention);
                assert_eq!(state.stages()[&id].writes, stage_before.writes);
                assert!(state.accepted().is_empty());
                recover(&mut state);
                attempt = assign(&mut state);
                assert_eq!(
                    reservation(&state, attempt).charged.memory_bytes,
                    3_072 << id
                );
            } else {
                let expected = a::TerminalResult::ResourceFailed {
                    result: value.response,
                    reason: a::ResourceFailureReason::AttemptBudgetExhausted,
                };
                assert_eq!(outcome, NoGraphOutcome::TerminalAccepted(expected.clone()));
                assert_eq!(terminal(&state, 1), expected);
            }
            recover(&mut state);
        }
        assert_eq!(state.stages().len(), 3);
        for id in 1..=3 {
            let member = state.stages()[&id]
                .candidate
                .closure
                .first()
                .unwrap()
                .clone();
            state
                .observe(id, id, member.key, member.object, true)
                .unwrap();
            recover(&mut state);
        }
        let bytes = state.snapshot().unwrap();
        // The same publication ID was unused by both retries, then used by the final.
        assert_eq!(
            state.accept_prepared_memory(1, 10, 100).unwrap(),
            NoGraphOutcome::Duplicate
        );
        assert_eq!(
            state.accept_prepared_memory(2, 10, 100).unwrap(),
            NoGraphOutcome::Duplicate
        );
        unchanged(&state, &bytes);
        assert!(state.heads().is_empty());
    }
}

#[test]
fn prepared_memory_and_no_graph_share_identity_and_retry_chain() {
    for algorithm in [1, 9] {
        for graph_first in [false, true] {
            let (mut state, _, first) = active(algorithm, 3, 32_768);
            state.enable_prepared_memory().unwrap();
            let payload = diagnostic("no graph sibling report");
            if graph_first {
                stage_graph(&mut state, algorithm, first, 1);
                let bytes = state.snapshot().unwrap();
                for report_id in [1, 99] {
                    assert!(
                        state
                            .report_no_graph(
                                report_id,
                                report(first, memory_kind(&state, first), &payload)
                            )
                            .is_err()
                    );
                    unchanged(&state, &bytes);
                }
                state.accept_prepared_memory(1, 10, 100).unwrap();
            } else {
                assert_eq!(
                    state
                        .report_no_graph(1, report(first, memory_kind(&state, first), &payload))
                        .unwrap(),
                    NoGraphOutcome::RetryScheduled
                );
            }
            let second = assign(&mut state);
            if graph_first {
                assert_eq!(
                    state
                        .report_no_graph(2, report(second, memory_kind(&state, second), &payload))
                        .unwrap(),
                    NoGraphOutcome::RetryScheduled
                );
            } else {
                let value = graph(&mut state, algorithm, second, 2, true);
                let bytes = state.snapshot().unwrap();
                let conflict = memory_report(&state, second, 1);
                assert!(
                    state
                        .stage_memory(2, value.clone(), conflict, 10, BTreeMap::new())
                        .is_err()
                );
                unchanged(&state, &bytes);
                let valid = memory_report(&state, second, 2);
                state
                    .stage_memory(2, value, valid, 10, BTreeMap::new())
                    .unwrap();
                prepare(&mut state, 2);
                assert_eq!(
                    state.accept_prepared_memory(2, 10, 100).unwrap(),
                    NoGraphOutcome::RetryScheduled
                );
            }
            recover(&mut state);
            let third = assign(&mut state);
            if graph_first {
                let value = stage_graph(&mut state, algorithm, third, 3);
                let expected = a::TerminalResult::ResourceFailed {
                    result: value.response,
                    reason: a::ResourceFailureReason::AttemptBudgetExhausted,
                };
                assert_eq!(
                    state.accept_prepared_memory(3, 10, 100).unwrap(),
                    NoGraphOutcome::TerminalAccepted(expected)
                );
                assert!(state.operations()[&1].root.is_some());
            } else {
                let expected = a::TerminalResult::ResourceFailed {
                    result: payload.clone(),
                    reason: a::ResourceFailureReason::AttemptBudgetExhausted,
                };
                assert_eq!(
                    state
                        .report_no_graph(3, report(third, memory_kind(&state, third), &payload))
                        .unwrap(),
                    NoGraphOutcome::TerminalAccepted(expected)
                );
                assert!(state.operations()[&1].root.is_none());
            }
            recover(&mut state);
        }
    }
}

#[test]
fn prepared_memory_refuses_bad_report_before_spending_stage_or_charge() {
    let (mut state, _, attempt) = active(1, 3, 32_768);
    let value = graph(&mut state, 1, attempt, 1, true);
    let valid = memory_report(&state, attempt, 1);
    let bytes = state.snapshot().unwrap();
    assert!(
        state
            .stage_memory(1, value.clone(), valid, 10, BTreeMap::new())
            .is_err()
    );
    unchanged(&state, &bytes);
    state.enable_prepared_memory().unwrap();
    let mut variants = vec![];
    let mut invalid = valid;
    invalid.id = 0;
    variants.push(invalid);
    let mut invalid = valid;
    invalid.epoch = a::ClusterEpoch(2);
    variants.push(invalid);
    let mut invalid = valid;
    invalid.quiescent = false;
    variants.push(invalid);
    let mut invalid = valid;
    invalid.charged.cpu_millis += 1;
    variants.push(invalid);
    let mut invalid = valid;
    invalid.charged.memory_bytes += 1;
    variants.push(invalid);
    let mut invalid = valid;
    invalid.charged.slots += 1;
    variants.push(invalid);
    let bytes = state.snapshot().unwrap();
    for invalid in variants {
        assert!(
            state
                .stage_memory(1, value.clone(), invalid, 10, BTreeMap::new())
                .is_err()
        );
        unchanged(&state, &bytes);
    }
    state
        .stage_memory(1, value, valid, 10, BTreeMap::new())
        .unwrap();
    let bytes = state.snapshot().unwrap();
    assert!(state.publish(1, 10, 100).is_err());
    assert!(state.accept_prepared_memory(1, 10, 100).is_err());
    unchanged(&state, &bytes);
    prepare(&mut state, 1);
    state
        .scheduler_event(a::Event::WorkerUnavailable {
            worker: attempt.worker,
            incarnation: attempt.incarnation,
            session: a::SessionId(1),
        })
        .unwrap();
    let bytes = state.snapshot().unwrap();
    assert!(state.accept_prepared_memory(1, 10, 100).is_err());
    unchanged(&state, &bytes);
    assert!(!reservation(&state, attempt).released);
}

#[test]
fn prepared_memory_requires_complete_current_graph_evidence() {
    for algorithm in [1, 9] {
        let (mut state, _, attempt) = active(algorithm, 3, 3_072);
        state.enable_prepared_memory().unwrap();
        let value = graph(&mut state, algorithm, attempt, 1, false);
        let valid = memory_report(&state, attempt, 1);
        state
            .stage_memory(1, value.clone(), valid, 10, BTreeMap::from([(1, key(1))]))
            .unwrap();
        let evidence = Evidence {
            stage: 1,
            catalogue_revision: state.catalogue_revision(),
            closure: value.closure.clone(),
            fences: state.fences(&value.closure).unwrap(),
            until: 100,
        };
        let bytes = state.snapshot().unwrap();
        assert!(state.prepare(1, evidence).is_err());
        unchanged(&state, &bytes);
        state.observe(1, 1, key(1), value.result, true).unwrap();
        let mut evidence = Evidence {
            stage: 1,
            catalogue_revision: state.catalogue_revision(),
            closure: value.closure.clone(),
            fences: state.fences(&value.closure).unwrap(),
            until: 100,
        };
        let bytes = state.snapshot().unwrap();
        evidence.closure.clear();
        assert!(state.prepare(1, evidence).is_err());
        unchanged(&state, &bytes);
        prepare(&mut state, 1);
        // A new physical catalogue entry invalidates the old exact revision.
        let extra = Object::for_bytes(algorithm, b"later fact").unwrap();
        state
            .install_physical_for(Some(attempt.execution), key(2), extra, true)
            .unwrap();
        let bytes = state.snapshot().unwrap();
        assert!(state.accept_prepared_memory(1, 10, 100).is_err());
        unchanged(&state, &bytes);
        prepare(&mut state, 1);
        let handle = *state.fences(&value.closure).unwrap().keys().next().unwrap();
        state.fence_namespace(handle).unwrap();
        let bytes = state.snapshot().unwrap();
        assert!(state.accept_prepared_memory(1, 10, 100).is_err());
        unchanged(&state, &bytes);
        assert!(current_attempt(&state, attempt.id).authorized);
        assert!(!reservation(&state, attempt).released);
    }
}

fn direct_graph(
    state: &mut PublicationAuthority,
    binding: &ActionBinding,
    generation: u64,
) -> Candidate {
    let result = ActionResult {
        stdout_raw: format!("successful direct result {generation}")
            .into_bytes()
            .into(),
        ..ActionResult::default()
    };
    let object = Object::for_bytes(binding.result_algorithm(), &result.encode_to_vec()).unwrap();
    state
        .install_physical(key(generation), object, true)
        .unwrap();
    state
        .direct_update_candidate(
            1,
            &context(),
            binding,
            result,
            BTreeSet::from([Member {
                object,
                key: key(generation),
                role: Role::Result,
            }]),
        )
        .unwrap()
}

#[test]
fn prepared_memory_preserves_prior_head_and_failed_operation_is_not_cached() {
    for algorithm in [1, 9] {
        let (mut state, request, attempt) = active(algorithm, 3, 3_072);
        state.enable_prepared_memory().unwrap();
        let binding = ActionBinding::capture(&context(), &request).unwrap();
        state
            .authorize_update(1, &context(), "action", &binding)
            .unwrap();
        let cached = direct_graph(&mut state, &binding, 9);
        stage(&mut state, 9, cached.clone());
        state.publish(9, 99, 100).unwrap();
        let head_root = state.accepted()[&99].root;
        let value = stage_graph(&mut state, algorithm, attempt, 1);
        let expected = a::TerminalResult::ResourceFailed {
            result: value.response,
            reason: a::ResourceFailureReason::NoLargerEligibleWorker,
        };
        assert_eq!(
            state.accept_prepared_memory(1, 10, 100).unwrap(),
            NoGraphOutcome::TerminalAccepted(expected.clone())
        );
        assert_eq!(state.heads()["action"], 99);
        assert_eq!(
            state.retention_view().roots[&r::RootId(head_root)].until,
            100
        );
        assert_eq!(terminal(&state, 1), expected);
        assert!(!state.operations()[&1].cached);
        state.serve("action", 99, state.commit(), 1, 90).unwrap();
        assert!(state.can_read(1, cached.closure.first().unwrap()));
        recover(&mut state);
    }
}

#[test]
fn prepared_memory_retry_reserves_another_stage_and_refuses_at_the_existing_cap() {
    let (mut state, request, attempt) = active(1, 3, 32_768);
    state.enable_prepared_memory().unwrap();
    stage_graph(&mut state, 1, attempt, 1);
    let binding = ActionBinding::capture(&context(), &request).unwrap();
    state
        .authorize_update(1, &context(), "action", &binding)
        .unwrap();
    for id in 2..=8 {
        let value = direct_graph(&mut state, &binding, id);
        state.stage(id, value, 10, BTreeMap::new()).unwrap();
    }
    // Installing other graphs changes catalogue revision, so prepare again.
    prepare(&mut state, 1);
    assert_eq!(
        state.accept_prepared_memory(1, 10, 100).unwrap(),
        NoGraphOutcome::RetryScheduled
    );
    let bytes = state.snapshot().unwrap();
    assert_eq!(state.scheduler_event(a::Event::Match), Err(Error::Bounds));
    unchanged(&state, &bytes);
    assert_eq!(state.stages().len(), MAX_STAGES);
    assert_eq!(state.stages()[&1].writes.len(), 1);
    assert!(state.stages()[&1].outcomes.is_empty());
    assert!(reservation(&state, attempt).released);
    recover(&mut state);
}

#[test]
fn prepared_memory_operation_protection_refusal_rolls_back_actual_acceptance() {
    let (mut state, _, attempt) = active(1, 3, 3_072);
    state.enable_prepared_memory().unwrap();
    let value = graph(&mut state, 1, attempt, 1, true);
    let report = memory_report(&state, attempt, 1);
    state
        .stage_memory(1, value.clone(), report, 10, BTreeMap::new())
        .unwrap();
    state
        .prepare(
            1,
            Evidence {
                stage: 1,
                catalogue_revision: state.catalogue_revision(),
                closure: value.closure.clone(),
                fences: state.fences(&value.closure).unwrap(),
                until: 50,
            },
        )
        .unwrap();
    let bytes = state.snapshot().unwrap();
    assert_eq!(
        state.accept_prepared_memory(1, 10, 50),
        Err(Error::Unprotected)
    );
    unchanged(&state, &bytes);
    assert!(current_attempt(&state, attempt.id).authorized);
    assert!(!reservation(&state, attempt).released);
    assert!(state.accepted().is_empty());
    prepare(&mut state, 1);
    assert!(matches!(
        state.accept_prepared_memory(1, 10, 100).unwrap(),
        NoGraphOutcome::TerminalAccepted(_)
    ));
}

#[test]
fn prepared_memory_empty_action_result_still_requires_graph_path() {
    for algorithm in [1, 9] {
        let (mut state, _, attempt) = active(algorithm, 0, 32_768);
        state.enable_prepared_memory().unwrap();
        let result = ActionResult::default();
        let bytes = result.encode_to_vec();
        assert!(bytes.is_empty());
        let object = Object::for_bytes(algorithm, &bytes).unwrap();
        state
            .install_physical_for(Some(attempt.execution), key(1), object, true)
            .unwrap();
        let response = a::ResultPayload::from_response(
            &ExecuteResponse {
                result: Some(result),
                status: Some(Status {
                    code: 8,
                    message: "failure with present empty result".into(),
                    ..Status::default()
                }),
                ..ExecuteResponse::default()
            },
            MAX_PAYLOAD,
        )
        .unwrap();
        let value = Candidate {
            action: "action".into(),
            publisher: worker(attempt),
            result: object,
            result_bytes: bytes,
            response: response.clone(),
            closure: BTreeSet::from([Member {
                object,
                key: key(1),
                role: Role::Result,
            }]),
        };
        let before = state.snapshot().unwrap();
        assert!(
            state
                .report_no_graph(1, report(attempt, memory_kind(&state, attempt), &response))
                .is_err()
        );
        unchanged(&state, &before);
        let classification = memory_report(&state, attempt, 1);
        state
            .stage_memory(1, value, classification, 10, BTreeMap::new())
            .unwrap();
        prepare(&mut state, 1);
        let expected = a::TerminalResult::ResourceFailed {
            result: response,
            reason: a::ResourceFailureReason::MissingGrowthEvidence,
        };
        assert_eq!(
            state.accept_prepared_memory(1, 10, 100).unwrap(),
            NoGraphOutcome::TerminalAccepted(expected)
        );
        recover(&mut state);
    }
}

fn corrupt_memory(encoded: &str, old: &PreparedMemoryStage, new: &PreparedMemoryStage) {
    let before = format!("\"memory\":{}", serde_json::to_string(old).unwrap());
    let after = format!("\"memory\":{}", serde_json::to_string(new).unwrap());
    assert_ne!(before, after);
    assert_eq!(encoded.matches(&before).count(), 1);
    let corrupt = encoded.replacen(&before, &after, 1);
    assert!(PublicationAuthority::restore(corrupt.as_bytes()).is_err());
}

#[test]
fn prepared_memory_restore_rejects_classification_origin_charge_target_and_downgrade() {
    for retry in [false, true] {
        let (mut state, _, attempt) = active(1, 3, if retry { 32_768 } else { 3_072 });
        state.enable_prepared_memory().unwrap();
        stage_graph(&mut state, 1, attempt, 1);
        state.accept_prepared_memory(1, 10, 100).unwrap();
        let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
        let old = state.stages()[&1].memory.as_ref().unwrap().clone();
        let mut invalid = old.clone();
        invalid.report.epoch = a::ClusterEpoch(2);
        corrupt_memory(&encoded, &old, &invalid);
        let mut invalid = old.clone();
        invalid.report.id = 0;
        corrupt_memory(&encoded, &old, &invalid);
        let mut invalid = old.clone();
        invalid.report.quiescent = false;
        corrupt_memory(&encoded, &old, &invalid);
        let mut invalid = old.clone();
        invalid.report.charged.slots += 1;
        corrupt_memory(&encoded, &old, &invalid);
        let mut invalid = old.clone();
        invalid.report.charged.cpu_millis += 1;
        corrupt_memory(&encoded, &old, &invalid);
        let mut invalid = old.clone();
        invalid.report.charged.memory_bytes += 1;
        corrupt_memory(&encoded, &old, &invalid);
        let mut invalid = old.clone();
        invalid.decision.as_mut().unwrap().publication = 0;
        corrupt_memory(&encoded, &old, &invalid);
        if !retry {
            let mut invalid = old.clone();
            invalid.decision.as_mut().unwrap().publication = 11;
            corrupt_memory(&encoded, &old, &invalid);
        }
        let mut invalid = old.clone();
        invalid.decision = None;
        corrupt_memory(&encoded, &old, &invalid);
        let mut invalid = old.clone();
        invalid.decision.as_mut().unwrap().position = 0;
        corrupt_memory(&encoded, &old, &invalid);
        let mut invalid = old.clone();
        invalid.decision.as_mut().unwrap().at = 1;
        corrupt_memory(&encoded, &old, &invalid);
        let mut invalid = old.clone();
        invalid.decision.as_mut().unwrap().until = 101;
        corrupt_memory(&encoded, &old, &invalid);
        let mut invalid = old.clone();
        invalid.decision.as_mut().unwrap().disposition = if retry {
            PreparedMemoryDisposition::Retry {
                target: a::Resources {
                    memory_bytes: 6_145,
                    ..old.report.charged
                },
            }
        } else {
            PreparedMemoryDisposition::Terminal {
                reason: a::ResourceFailureReason::MissingGrowthEvidence,
            }
        };
        corrupt_memory(&encoded, &old, &invalid);
        assert!(
            PublicationAuthority::restore(
                encoded
                    .replacen("\"version\":5", "\"version\":4", 1)
                    .as_bytes()
            )
            .is_err()
        );
        let value: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        let marker = format!(
            "\"prepared_memory_at\":{},",
            value["metadata"]["prepared_memory_at"]
        );
        let no_marker =
            encoded
                .replacen(&marker, "", 1)
                .replacen("\"version\":5", "\"version\":4", 1);
        assert_ne!(encoded, no_marker);
        assert!(PublicationAuthority::restore(no_marker.as_bytes()).is_err());
        let no_classification = encoded.replacen(
            &format!("\"memory\":{},", serde_json::to_string(&old).unwrap()),
            "",
            1,
        );
        assert_ne!(encoded, no_classification);
        assert!(PublicationAuthority::restore(no_classification.as_bytes()).is_err());
        recover(&mut state);
    }
}

#[test]
fn prepared_memory_retry_abandon_and_late_write_keep_classification_immutable() {
    let (mut state, _, attempt) = active(1, 3, 32_768);
    state.enable_prepared_memory().unwrap();
    let value = stage_graph(&mut state, 1, attempt, 1);
    state.accept_prepared_memory(1, 10, 100).unwrap();
    let report = state.stages()[&1].memory.clone();
    let proof = state.stages()[&1].preparation.clone().unwrap();
    let bytes = state.snapshot().unwrap();
    assert!(state.prepare(1, proof).is_err());
    unchanged(&state, &bytes);
    state.abandon(1).unwrap();
    recover(&mut state);
    let second = assign(&mut state);
    let charge = reservation(&state, second).charged;
    state.observe(1, 1, key(1), value.result, true).unwrap();
    assert_eq!(state.stages()[&1].memory, report);
    assert_eq!(state.scheduler_view().workers[0].charged, charge);
    assert!(state.heads().is_empty());
    assert!(state.accepted().is_empty());
    recover(&mut state);
}

#[test]
fn prepared_memory_zero_original_uses_peak_and_restores_after_capacity_changes() {
    for algorithm in [1, 9] {
        let (mut state, _, attempt) = active(algorithm, 0, 32_768);
        state.enable_prepared_memory().unwrap();
        let value = graph(&mut state, algorithm, attempt, 1, true);
        let mut report = memory_report(&state, attempt, 1);
        report.peak_memory_bytes = Some(1_024);
        state
            .stage_memory(1, value, report, 10, BTreeMap::new())
            .unwrap();
        prepare(&mut state, 1);
        assert_eq!(
            state.accept_prepared_memory(1, 10, 100).unwrap(),
            NoGraphOutcome::RetryScheduled
        );
        assert_eq!(
            state.scheduler_view().executions[0]
                .reservation_target
                .memory_bytes,
            2_048
        );
        let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
        let old = state.stages()[&1].memory.as_ref().unwrap().clone();
        let mut no_peak = old.clone();
        no_peak.report.peak_memory_bytes = None;
        corrupt_memory(&encoded, &old, &no_peak);
        capacity(&mut state, 1_000);
        recover(&mut state);
        assert_eq!(
            state.scheduler_view().executions[0]
                .reservation_target
                .memory_bytes,
            2_048
        );
        capacity(&mut state, 32_768);
        recover(&mut state);
        let second = assign(&mut state);
        assert_eq!(reservation(&state, second).charged.memory_bytes, 2_048);
        recover(&mut state);
    }
}

#[test]
fn prepared_memory_activation_refusal_preserves_older_format_atomically() {
    let state = authority(&[("action", false)]);
    let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
    let encoded = encoded.replacen("\"commit\":0", &format!("\"commit\":{}", u64::MAX - 1), 1);
    let mut state = PublicationAuthority::restore(encoded.as_bytes()).unwrap();
    assert_eq!(state.enable_prepared_memory(), Err(Error::Bounds));
    unchanged(&state, encoded.as_bytes());
    assert!(state.terminal_reports().is_none());
}
