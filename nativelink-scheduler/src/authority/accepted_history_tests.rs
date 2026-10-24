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

use nativelink_proto::build::bazel::remote::execution::v2::{ActionResult, ExecuteResponse};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::HistoricalExecuteResponse;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use prost::Message;

use super::*;
use crate::authority::accepted_history::{HistoryOutcome, HistoryStatus};

fn step(state: &mut State, event: Event) -> Reply {
    let reply = state.apply(RecordedEvent {
        epoch: state.epoch,
        event,
    });
    state.check_invariants().unwrap();
    reply
}

fn request(algorithm: &str, no_cache: bool) -> Request {
    Request {
        action: ActionKey {
            instance: "scope".into(),
            digest_function: algorithm.into(),
            digest: "ab".repeat(32),
            digest_size_bytes: 0,
        },
        execution_scope: None,
        do_not_cache: no_cache,
        skip_cache_lookup: true,
        priority: 0,
        properties: BTreeMap::new(),
        resources: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            slots: 1,
        },
        execution_timeout: 1000,
        payload: None,
    }
}

fn fixture(algorithm: &str, no_cache: bool) -> State {
    fixture_request(request(algorithm, no_cache))
}

fn fixture_request(request: Request) -> State {
    let mut state = State::new(ClusterEpoch(0), Limits::default()).unwrap();
    let capacity = Resources {
        cpu_millis: 1,
        memory_bytes: 16,
        slots: 1,
    };
    assert_eq!(
        step(
            &mut state,
            Event::Register {
                worker: WorkerId(0),
                incarnation: IncarnationId(0),
                session: SessionId(0),
                properties: BTreeMap::new(),
                capacity,
                available: capacity
            }
        ),
        Reply::Applied
    );
    assert!(matches!(
        step(
            &mut state,
            Event::Submit {
                operation: OperationId(1),
                request
            }
        ),
        Reply::Submitted { .. }
    ));
    assert!(matches!(
        step(&mut state, Event::Match),
        Reply::Matched { .. }
    ));
    state
}

fn result(exit_code: i32) -> ResultPayload {
    ResultPayload::from_response(
        &ExecuteResponse {
            result: Some(ActionResult {
                exit_code,
                stdout_raw: b"exact diagnostic bytes".to_vec().into(),
                ..Default::default()
            }),
            message: "stable captured response".into(),
            ..Default::default()
        },
        reapi::MAX_RESPONSE_BYTES,
    )
    .unwrap()
}

fn completion(state: &State, attempt: AttemptId, result: ResultPayload) -> Event {
    let source = state.attempts[&attempt];
    Event::Complete {
        attempt,
        worker: source.worker,
        incarnation: source.incarnation,
        result,
    }
}

fn acknowledge(state: &mut State, execution: ExecutionId) {
    let association = state.accepted_history().unwrap().associations[&execution].clone();
    assert_eq!(
        step(
            state,
            Event::HistoryPublication {
                effect: association.effect,
                object: association.recipe.object,
                outcome: HistoryOutcome::Stored
            }
        ),
        Reply::Applied
    );
}

#[test]
fn fixed_history_policy_preserves_exact_objects_and_terminal_responses() {
    for algorithm in ["sha256", "blake3"] {
        for no_cache in [false, true] {
            for exit in [0, 9] {
                let mut state = fixture(algorithm, no_cache);
                let before = state.snapshot().unwrap();
                assert!(
                    !String::from_utf8(before.clone())
                        .unwrap()
                        .contains("accepted_history")
                );
                state.enable_accepted_history([0; 32]).unwrap();
                let captured = result(exit);
                let event = completion(&state, AttemptId(1), captured.clone());
                assert_eq!(step(&mut state, event.clone()), Reply::Applied);
                let terminal = state.view();
                let association = state.history_for_operation(OperationId(1)).unwrap().clone();
                assert_eq!(association.status, HistoryStatus::Pending);
                let bytes = state.history_object_bytes(association.effect).unwrap();
                let diagnostic = HistoricalExecuteResponse::decode(bytes.as_slice()).unwrap();
                assert_eq!(diagnostic.action_digest.unwrap().hash, "ab".repeat(32));
                assert_eq!(
                    diagnostic.execute_response.unwrap(),
                    captured.require_response().unwrap().decode().unwrap()
                );
                assert!(bytes.ends_with(captured.require_response().unwrap().encoded()));
                assert_eq!(association.recipe.object.size_bytes, bytes.len() as u64);
                let mut hasher = match algorithm {
                    "sha256" => DigestHasherFunc::Sha256.hasher(),
                    "blake3" => DigestHasherFunc::Blake3.hasher(),
                    _ => unreachable!(),
                };
                hasher.update(&bytes);
                assert_eq!(
                    association.recipe.object.hash,
                    **hasher.finalize_digest().packed_hash()
                );
                let publications: Vec<_> = state
                    .effects
                    .values()
                    .filter(|e| matches!(e.kind, EffectKind::PublishResult { .. }))
                    .collect();
                assert_eq!(publications.len(), usize::from(!no_cache && exit == 0));
                if let Some(publication) = publications.first() {
                    assert!(publication.id < association.effect);
                }
                let serialized = state.snapshot().unwrap();
                assert!(serialized.starts_with(b"{\"version\":7,"));
                assert_eq!(State::restore(&serialized).unwrap(), state);
                assert!(matches!(step(&mut state, event), Reply::Rejected(_)));
                assert_eq!(state.snapshot().unwrap(), serialized);
                acknowledge(&mut state, ExecutionId(1));
                assert_eq!(state.view(), terminal);
                assert_eq!(
                    state.history_object_bytes(association.effect).unwrap(),
                    bytes
                );
                assert!(state.enable_accepted_history([1; 32]).is_err());
            }
        }
    }
}

#[test]
fn pending_history_pins_terminal_until_exact_outcome_even_after_worker_replacement() {
    let mut state = fixture("sha256", true);
    state.enable_accepted_history([3; 32]).unwrap();
    let event = completion(&state, AttemptId(1), result(1));
    step(&mut state, event);
    let association = state.history_for_operation(OperationId(1)).unwrap().clone();
    let capacity = state.workers[&WorkerId(0)].capacity;
    step(
        &mut state,
        Event::Register {
            worker: WorkerId(0),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            properties: BTreeMap::new(),
            capacity,
            available: capacity,
        },
    );
    step(
        &mut state,
        Event::AdvanceTime {
            now: TERMINAL_RETENTION,
        },
    );
    assert_eq!(
        step(
            &mut state,
            Event::RetireExecution {
                execution: ExecutionId(1)
            }
        ),
        Reply::Rejected(Rejection::Busy)
    );
    let unchanged = state.snapshot().unwrap();
    for outcome in [HistoryOutcome::Failed, HistoryOutcome::Unknown] {
        assert_eq!(
            step(
                &mut state,
                Event::HistoryPublication {
                    effect: association.effect,
                    object: association.recipe.object,
                    outcome
                }
            ),
            Reply::Ignored
        );
        assert_eq!(state.snapshot().unwrap(), unchanged);
    }
    let mut wrong = association.recipe.object;
    wrong.namespace[31] ^= 1;
    assert_eq!(
        step(
            &mut state,
            Event::HistoryPublication {
                effect: association.effect,
                object: wrong,
                outcome: HistoryOutcome::Stored
            }
        ),
        Reply::Rejected(Rejection::Stale)
    );
    state = State::restore(&unchanged).unwrap();
    acknowledge(&mut state, ExecutionId(1));
    assert_eq!(
        step(
            &mut state,
            Event::RetireExecution {
                execution: ExecutionId(1)
            }
        ),
        Reply::Applied
    );
    assert_eq!(
        step(
            &mut state,
            Event::HistoryPublication {
                effect: association.effect,
                object: association.recipe.object,
                outcome: HistoryOutcome::Stored
            }
        ),
        Reply::Ignored
    );
    assert!(state.accepted_history().unwrap().associations.is_empty());
}

#[test]
fn stale_candidates_and_preexisting_terminals_create_no_history() {
    let mut before = fixture("sha256", true);
    let event = completion(&before, AttemptId(1), result(0));
    step(&mut before, event);
    let view = before.view();
    before.enable_accepted_history([0; 32]).unwrap();
    assert_eq!(before.view(), view);
    assert!(before.accepted_history().unwrap().associations.is_empty());
    assert_eq!(State::restore(&before.snapshot().unwrap()).unwrap(), before);
    let mut revoked = fixture("sha256", true);
    revoked.enable_accepted_history([0; 32]).unwrap();
    let event = completion(&revoked, AttemptId(1), result(0));
    step(
        &mut revoked,
        Event::Cancel {
            operation: OperationId(1),
        },
    );
    let unchanged = revoked.snapshot().unwrap();
    assert!(matches!(step(&mut revoked, event), Reply::Rejected(_)));
    assert_eq!(revoked.snapshot().unwrap(), unchanged);
    assert!(!revoked.reservations[&ReservationId(1)].released);
}

#[test]
fn canceled_shared_handle_does_not_gain_the_accepted_history_association() {
    let mut state = fixture("sha256", false);
    state.enable_accepted_history([0; 32]).unwrap();
    step(
        &mut state,
        Event::Submit {
            operation: OperationId(2),
            request: request("sha256", false),
        },
    );
    step(
        &mut state,
        Event::Cancel {
            operation: OperationId(1),
        },
    );
    let event = completion(&state, AttemptId(1), result(4));
    step(&mut state, event);
    assert!(state.history_for_operation(OperationId(1)).is_none());
    assert!(state.history_for_operation(OperationId(2)).is_some());
    assert_eq!(State::restore(&state.snapshot().unwrap()).unwrap(), state);
}

#[test]
fn memory_retry_is_excluded_but_final_resource_failure_has_history() {
    let mut state = fixture("sha256", true);
    state.enable_accepted_history([0; 32]).unwrap();
    for attempt in 1..=3 {
        let row = state.attempts[&AttemptId(attempt)];
        let charged = state.reservations[&row.reservation].charged;
        assert_eq!(
            step(
                &mut state,
                Event::MemoryFailure {
                    attempt: row.id,
                    worker: row.worker,
                    incarnation: row.incarnation,
                    charged,
                    peak_memory_bytes: None,
                    quiescent: true,
                    result: result(137)
                }
            ),
            Reply::Applied
        );
        if attempt < 3 {
            assert!(state.accepted_history().unwrap().associations.is_empty());
            assert!(matches!(
                step(&mut state, Event::Match),
                Reply::Matched { .. }
            ));
        }
    }
    assert!(matches!(
        state.executions[&ExecutionId(1)].terminal,
        Some(TerminalResult::ResourceFailed { .. })
    ));
    assert_eq!(
        state.history_for_operation(OperationId(1)).unwrap().attempt,
        AttemptId(3)
    );
}

#[test]
fn activation_prepays_identity_effect_and_byte_capacity_atomically() {
    let mut insufficient = fixture("sha256", false);
    // Two remaining dispatch/cancel pairs, current cancellation, and AC result.
    insufficient.next_effect = u64::MAX - 6;
    insufficient.check_invariants().unwrap();
    let before = insufficient.snapshot().unwrap();
    assert!(insufficient.enable_accepted_history([0; 32]).is_err());
    assert_eq!(insufficient.snapshot().unwrap(), before);
    let mut effects = fixture("sha256", false);
    effects.limits.max_effects = 8;
    let before = effects.snapshot().unwrap();
    assert!(effects.enable_accepted_history([0; 32]).is_err());
    assert_eq!(effects.snapshot().unwrap(), before);
    let mut bytes = fixture("sha256", false);
    for id in 2..=100 {
        step(
            &mut bytes,
            Event::Submit {
                operation: OperationId(id),
                request: request("sha256", false),
            },
        );
    }
    let copies = bytes.operations.len() + bytes.executions.len() * 2;
    let fixed = bytes.byte_credit().unwrap() - copies * bytes.limits.max_payload_bytes;
    bytes.limits.max_payload_bytes = (MAX_STATE_BYTES - fixed) / copies;
    bytes.check_invariants().unwrap();
    let before = bytes.snapshot().unwrap();
    assert!(bytes.enable_accepted_history([0; 32]).is_err());
    assert_eq!(bytes.snapshot().unwrap(), before);
}

#[test]
fn receipt_and_history_effects_compose_without_losing_terminal_pins() {
    let mut state = fixture("sha256", true);
    state.enable_receipt_retirement().unwrap();
    state.enable_accepted_history([0; 32]).unwrap();
    step(
        &mut state,
        Event::IssueReceipts {
            worker: WorkerId(0),
            incarnation: IncarnationId(0),
            after_ack: None,
        },
    );
    let certificate = state.receipt_retirement().unwrap().streams[&WorkerId(0)]
        .pending
        .clone()
        .unwrap();
    let event = completion(&state, AttemptId(1), result(1));
    step(&mut state, event);
    let association = state.history_for_operation(OperationId(1)).unwrap().clone();
    assert!(association.effect > certificate.effect);
    acknowledge(&mut state, ExecutionId(1));
    step(
        &mut state,
        Event::AdvanceTime {
            now: TERMINAL_RETENTION,
        },
    );
    assert_eq!(
        step(
            &mut state,
            Event::RetireExecution {
                execution: ExecutionId(1)
            }
        ),
        Reply::Rejected(Rejection::Busy)
    );
    step(
        &mut state,
        Event::AcknowledgeReceipts {
            worker: WorkerId(0),
            incarnation: IncarnationId(0),
            certificate,
        },
    );
    assert_eq!(
        step(
            &mut state,
            Event::RetireExecution {
                execution: ExecutionId(1)
            }
        ),
        Reply::Applied
    );
    assert_eq!(State::restore(&state.snapshot().unwrap()).unwrap(), state);
}

#[test]
fn restore_rejects_missing_or_changed_history_provenance() {
    let mut state = fixture("sha256", true);
    state.enable_accepted_history([0; 32]).unwrap();
    let event = completion(&state, AttemptId(1), result(1));
    step(&mut state, event);
    let original: serde_json::Value = serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
    for path in [
        "effect",
        "execution",
        "attempt",
        "worker",
        "incarnation",
        "epoch",
    ] {
        let mut broken = original.clone();
        broken["state"]["accepted_history"]["associations"]["1"][path] = 42.into();
        assert!(
            State::restore(&serde_json::to_vec(&broken).unwrap()).is_err(),
            "{path}"
        );
    }
    let mut missing = original.clone();
    missing["state"]["accepted_history"]["associations"] = serde_json::json!({});
    assert!(State::restore(&serde_json::to_vec(&missing).unwrap()).is_err());
    let mut changed = original;
    changed["state"]["accepted_history"]["associations"]["1"]["recipe"]["prefix"][0] = 0.into();
    assert!(State::restore(&serde_json::to_vec(&changed).unwrap()).is_err());
}

#[test]
fn maximum_diagnostic_prefix_and_metadata_fit_the_reserved_credit() {
    let mut request = request("blake3", true);
    request.action.digest_size_bytes = i64::MAX as u64;
    let mut state = fixture_request(request);
    state.limits.max_payload_bytes = MAX_PAYLOAD_BYTES;
    state.enable_accepted_history([255; 32]).unwrap();
    let response = ExecuteResponse {
        result: Some(ActionResult {
            stdout_raw: vec![255; 64 * 1024].into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let result = ResultPayload::from_response(&response, reapi::MAX_RESPONSE_BYTES).unwrap();
    let event = completion(&state, AttemptId(1), result);
    assert_eq!(step(&mut state, event), Reply::Applied);
    let mut widest = state.history_for_operation(OperationId(1)).unwrap().clone();
    assert_eq!(
        widest.recipe.prefix.len(),
        accepted_history::MAX_HISTORY_PREFIX_BYTES
    );
    let diagnostic = state.history_object_bytes(widest.effect).unwrap();
    assert_eq!(
        HistoricalExecuteResponse::decode(diagnostic.as_slice())
            .unwrap()
            .execute_response
            .unwrap(),
        response
    );
    widest.epoch = ClusterEpoch(u64::MAX);
    widest.effect = EffectId(u64::MAX);
    widest.execution = ExecutionId(u64::MAX);
    widest.attempt = AttemptId(u64::MAX);
    widest.worker = WorkerId(u64::MAX);
    widest.incarnation = IncarnationId(u64::MAX);
    widest.recipe.prefix.fill(255);
    widest.recipe.response_sha256.fill(255);
    widest.recipe.object.hash.fill(255);
    widest.recipe.object.size_bytes = u64::MAX;
    widest.status = HistoryStatus::Materialized;
    let encoded = encoded_size(&BTreeMap::from([(u64::MAX, widest)]));
    assert!(
        encoded <= accepted_history::HISTORY_ROW_BYTES,
        "maximum row is {encoded} bytes"
    );
    assert!(state.snapshot().unwrap().len() <= state.byte_credit().unwrap());
}

#[test]
fn active_history_refuses_unrepresentable_input_before_any_mutation() {
    let mut state = fixture("sha256", true);
    state.enable_accepted_history([0; 32]).unwrap();
    for action in [
        ActionKey {
            digest_function: "MD5".into(),
            ..request("sha256", true).action
        },
        ActionKey {
            digest: "AB".repeat(32),
            ..request("sha256", true).action
        },
        ActionKey {
            digest_size_bytes: u64::MAX,
            ..request("sha256", true).action
        },
    ] {
        let before = state.snapshot().unwrap();
        let request = Request {
            action,
            ..request("sha256", true)
        };
        assert_eq!(
            step(
                &mut state,
                Event::Submit {
                    operation: OperationId(2),
                    request
                }
            ),
            Reply::Rejected(Rejection::Invalid)
        );
        assert_eq!(state.snapshot().unwrap(), before);
    }
    let before = state.snapshot().unwrap();
    let synthetic = ResultPayload {
        digest: "synthetic".into(),
        exit_code: 0,
        error: None,
        response: None,
    };
    let event = completion(&state, AttemptId(1), synthetic);
    assert_eq!(step(&mut state, event), Reply::Rejected(Rejection::Invalid));
    assert_eq!(state.snapshot().unwrap(), before);
    assert!(!state.reservations[&ReservationId(1)].released);
}
