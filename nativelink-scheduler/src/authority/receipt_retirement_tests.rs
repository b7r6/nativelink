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

use super::*;

fn issue(state: &mut State) -> ReceiptCertificate {
    let stream = &state.receipt_retirement().unwrap().streams[&WorkerId(1)];
    let event = Event::IssueReceipts {
        worker: WorkerId(1),
        incarnation: stream.incarnation,
        after_ack: stream.acknowledged.map(|floors| floors.effect),
    };
    assert_eq!(apply(state, event), Reply::Applied);
    state.receipt_retirement().unwrap().streams[&WorkerId(1)]
        .pending
        .clone()
        .unwrap()
}

fn acknowledge(state: &mut State, certificate: ReceiptCertificate) -> Reply {
    let incarnation = state.workers[&WorkerId(1)].incarnation;
    apply(
        state,
        Event::AcknowledgeReceipts {
            worker: WorkerId(1),
            incarnation,
            certificate,
        },
    )
}

fn pending(state: &State) -> &ReceiptCertificate {
    state.receipt_retirement().unwrap().streams[&WorkerId(1)]
        .pending
        .as_ref()
        .unwrap()
}

fn complete(state: &mut State, attempt: AttemptId) {
    let entry = state.attempts[&attempt];
    assert_eq!(
        apply(
            state,
            Event::Complete {
                attempt,
                worker: entry.worker,
                incarnation: entry.incarnation,
                result: ResultPayload {
                    digest: "result".into(),
                    exit_code: 1,
                    error: None,
                    response: None
                },
            }
        ),
        Reply::Applied
    );
}

#[test]
fn activation_preserves_base_bytes_and_snapshot_version_tracks_the_component() {
    let mut state = charged_state();
    let before = state.snapshot().unwrap();
    assert_eq!(
        apply(
            &mut state,
            Event::IssueReceipts {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                after_ack: None,
            }
        ),
        Reply::Rejected(Rejection::Invalid)
    );
    assert_eq!(state.snapshot().unwrap(), before);
    assert!(
        !std::str::from_utf8(&before)
            .unwrap()
            .contains("receipt_retirement")
    );
    let original = state.view();
    state.enable_receipt_retirement().unwrap();
    assert_eq!(state.view(), original);
    assert_eq!(
        state.byte_credit(),
        State::restore(&before).unwrap().byte_credit()
    );
    let enabled = state.snapshot().unwrap();
    assert!(enabled.starts_with(b"{\"version\":6,"));
    state.enable_receipt_retirement().unwrap();
    assert_eq!(state.snapshot().unwrap(), enabled);
    assert_eq!(State::restore(&enabled).unwrap(), state);
    let mut wrong_version: serde_json::Value = serde_json::from_slice(&enabled).unwrap();
    wrong_version["version"] = 5.into();
    assert!(State::restore(&serde_json::to_vec(&wrong_version).unwrap()).is_err());
    wrong_version["version"] = 6.into();
    wrong_version["state"]
        .as_object_mut()
        .unwrap()
        .remove("receipt_retirement");
    assert!(State::restore(&serde_json::to_vec(&wrong_version).unwrap()).is_err());
}

#[test]
fn immutable_pending_certificate_pins_released_attempts_and_ack_never_releases_resources() {
    let mut state = charged_state();
    state.enable_receipt_retirement().unwrap();
    let before = state.view();
    let certificate = issue(&mut state);
    assert_eq!(certificate.effect, EffectId(2));
    assert_eq!(certificate.effects_through, certificate.effect);
    assert_eq!(certificate.retain.len(), 1);
    assert_eq!(certificate.retain[0].dispatch_effect, EffectId(1));
    assert_eq!(before, state.view());
    let mut acknowledged_while_active = state.clone();
    assert_eq!(
        acknowledge(&mut acknowledged_while_active, certificate.clone()),
        Reply::Applied
    );
    assert_eq!(acknowledged_while_active.view(), before);
    assert!(!acknowledged_while_active.reservations[&ReservationId(1)].released);
    complete(&mut state, AttemptId(1));
    assert_eq!(pending(&state), &certificate);
    assert!(state.receipt_retirement().unwrap().streams[&WorkerId(1)].dirty);
    apply(
        &mut state,
        Event::AdvanceTime {
            now: TERMINAL_RETENTION,
        },
    );
    assert_eq!(
        apply(
            &mut state,
            Event::RetireExecution {
                execution: ExecutionId(1)
            }
        ),
        Reply::Rejected(Rejection::Busy)
    );
    let snapshot = state.snapshot().unwrap();
    assert_eq!(
        apply(
            &mut state,
            Event::IssueReceipts {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                after_ack: None,
            }
        ),
        Reply::Ignored
    );
    assert_eq!(state.snapshot().unwrap(), snapshot);
    state = State::restore(&snapshot).unwrap();
    let before_ack = state.view();
    assert_eq!(acknowledge(&mut state, certificate.clone()), Reply::Applied);
    assert_eq!(state.view(), before_ack);
    assert_eq!(
        acknowledge(&mut state, certificate.clone()),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(
        apply(
            &mut state,
            Event::RetireExecution {
                execution: ExecutionId(1)
            }
        ),
        Reply::Applied
    );
    let next = issue(&mut state);
    assert!(next.effect > certificate.effects_through);
    assert!(next.retain.is_empty());
    assert_eq!(next.attempts_through, certificate.attempts_through);
    assert_eq!(acknowledge(&mut state, next), Reply::Applied);
    let clean = state.snapshot().unwrap();
    assert_eq!(
        apply(
            &mut state,
            Event::IssueReceipts {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                after_ack: Some(certificate.effect),
            }
        ),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(clean, state.snapshot().unwrap());
}

#[test]
fn ack_matches_full_certificate_and_authenticated_identity_without_partial_changes() {
    let mut state = charged_state();
    state.enable_receipt_retirement().unwrap();
    let certificate = issue(&mut state);
    let before = state.snapshot().unwrap();
    let mut mutations = vec![certificate.clone(); 8];
    mutations[0].effect.0 += 1;
    mutations[1].attempts_through.0 += 1;
    mutations[2].effects_through.0 += 1;
    mutations[3].reservations_through.0 += 1;
    mutations[4].retain.clear();
    mutations[5].retain[0].dispatch_effect.0 += 1;
    mutations[6].retain[0].execution.0 += 1;
    mutations[7].retain[0].cancel_effect = Some(EffectId(1));
    for changed in mutations {
        assert_eq!(
            acknowledge(&mut state, changed),
            Reply::Rejected(Rejection::Stale)
        );
        assert_eq!(state.snapshot().unwrap(), before);
    }
    for (worker, incarnation, rejection) in [
        (WorkerId(1), IncarnationId(2), Rejection::Stale),
        (WorkerId(2), IncarnationId(1), Rejection::Unknown),
    ] {
        assert_eq!(
            apply(
                &mut state,
                Event::AcknowledgeReceipts {
                    worker,
                    incarnation,
                    certificate: certificate.clone(),
                }
            ),
            Reply::Rejected(rejection)
        );
        assert_eq!(state.snapshot().unwrap(), before);
    }
}

#[test]
fn later_cancellation_does_not_rewrite_pending_and_replacement_preserves_uncertain_charge() {
    let mut state = charged_state();
    state.enable_receipt_retirement().unwrap();
    let certificate = issue(&mut state);
    apply(
        &mut state,
        Event::Cancel {
            operation: OperationId(1),
        },
    );
    assert_eq!(pending(&state), &certificate);
    assert_eq!(certificate.retain[0].cancel_effect, None);
    let cancel = state
        .effects
        .values()
        .find(|effect| matches!(effect.kind, EffectKind::Cancel { .. }))
        .unwrap()
        .id;
    assert!(cancel > certificate.effect);
    state = State::restore(&state.snapshot().unwrap()).unwrap();
    let worker = state.workers[&WorkerId(1)].clone();
    apply(
        &mut state,
        Event::Register {
            worker: worker.id,
            incarnation: worker.incarnation,
            session: SessionId(2),
            properties: worker.properties.clone(),
            capacity: worker.capacity,
            available: worker.available,
        },
    );
    assert_eq!(pending(&state), &certificate);
    apply(
        &mut state,
        Event::Register {
            worker: worker.id,
            incarnation: IncarnationId(2),
            session: SessionId(1),
            properties: worker.properties,
            capacity: worker.capacity,
            available: worker.available,
        },
    );
    assert!(
        state.receipt_retirement().unwrap().streams[&WorkerId(1)]
            .pending
            .is_none()
    );
    assert!(!state.reservations[&ReservationId(1)].released);
    assert!(state.effects.contains_key(&cancel));
    assert!(state.view().workers[0].quarantined);
    assert_eq!(
        apply(
            &mut state,
            Event::AcknowledgeReceipts {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                certificate,
            }
        ),
        Reply::Rejected(Rejection::Stale)
    );
    let replacement = issue(&mut state);
    assert!(replacement.retain.is_empty());
    assert_eq!(acknowledge(&mut state, replacement), Reply::Applied);
    assert!(!state.reservations[&ReservationId(1)].released);
    apply(
        &mut state,
        Event::Cleanup {
            attempt: AttemptId(1),
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
        },
    );
    assert!(state.reservations[&ReservationId(1)].released);
    // Cleanup of the superseded incarnation cannot dirty the new stream.
    assert!(!state.receipt_retirement().unwrap().streams[&WorkerId(1)].dirty);
}

#[test]
fn identity_exhaustion_cannot_spend_prepaid_finalization_on_a_certificate() {
    let mut state = charged_state();
    // Two remaining attempts, cancellation of the current attempt, publication.
    state.next_effect = u64::MAX - 6;
    state.check_invariants().unwrap();
    state.enable_receipt_retirement().unwrap();
    let before = state.snapshot().unwrap();
    assert_eq!(
        apply(
            &mut state,
            Event::IssueReceipts {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                after_ack: None,
            }
        ),
        Reply::Rejected(Rejection::Overloaded)
    );
    assert_eq!(before, state.snapshot().unwrap());
    apply(
        &mut state,
        Event::Cancel {
            operation: OperationId(1),
        },
    );
    // Cancellation still obtains its previously promised unique identity.
    assert!(state.effects.contains_key(&EffectId(u64::MAX - 6)));
}

#[test]
fn restore_rejects_forged_retirement_relations_and_omitted_active_exceptions() {
    let mut state = charged_state();
    state.enable_receipt_retirement().unwrap();
    issue(&mut state);
    let encoded: serde_json::Value = serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
    let mut mutations = vec![encoded; 7];
    let path = "/state/receipt_retirement/streams/1";
    mutations[0].pointer_mut(path).unwrap()["incarnation"] = 2.into();
    mutations[1].pointer_mut(path).unwrap()["pending"]["retain"] = serde_json::json!([]);
    mutations[2].pointer_mut(path).unwrap()["pending"]["retain"][0]["execution"] = 99.into();
    mutations[3].pointer_mut(path).unwrap()["pending"]["effect"] = 1.into();
    mutations[4].pointer_mut(path).unwrap()["pending"]["effects_through"] = 1.into();
    mutations[5].pointer_mut(path).unwrap()["pending"]["retain"][0]["cancel_effect"] = 1.into();
    mutations[6]["state"]["receipt_retirement"]["streams"] = serde_json::json!({});
    for mutated in mutations {
        assert!(State::restore(&serde_json::to_vec(&mutated).unwrap()).is_err());
    }
}

#[test]
fn sustained_retirement_has_constant_metadata_with_an_old_active_hole() {
    let mut state = charged_state();
    state.limits.max_executions = 2;
    state.limits.max_operations = 2;
    let capacity = Resources {
        cpu_millis: 2000,
        memory_bytes: 2048,
        slots: 2,
    };
    apply(
        &mut state,
        Event::Capacity {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            capacity,
            available: capacity,
            draining: false,
        },
    );
    state.enable_receipt_retirement().unwrap();
    let mut request = state.executions[&ExecutionId(1)].request.clone();
    request.do_not_cache = true;
    for ordinal in 2..130 {
        request.action.digest = format!("work-{ordinal}");
        let Reply::Submitted { execution, .. } = apply(
            &mut state,
            Event::Submit {
                operation: OperationId(ordinal),
                request: request.clone(),
            },
        ) else {
            panic!("admitted bounded sequential work");
        };
        let Reply::Matched { attempt, .. } = apply(&mut state, Event::Match) else {
            panic!("second slot stays available");
        };
        complete(&mut state, attempt);
        let certificate = issue(&mut state);
        assert_eq!(certificate.retain.len(), 1);
        assert_eq!(certificate.retain[0].attempt, AttemptId(1));
        assert_eq!(acknowledge(&mut state, certificate), Reply::Applied);
        let now = state.now + TERMINAL_RETENTION;
        apply(&mut state, Event::AdvanceTime { now });
        assert_eq!(
            apply(&mut state, Event::RetireExecution { execution }),
            Reply::Applied
        );
        state = State::restore(&state.snapshot().unwrap()).unwrap();
        assert_eq!(state.attempts.len(), 1);
        assert_eq!(state.receipt_retirement().unwrap().streams.len(), 1);
        assert!(
            state.receipt_retirement().unwrap().streams[&WorkerId(1)]
                .pending
                .is_none()
        );
        assert!(state.attempts[&AttemptId(1)].authorized);
        assert!(encoded_size(state.receipt_retirement().unwrap()) < 256);
    }
}

#[test]
fn maximum_certificate_rows_and_ack_frame_fit_the_prepaid_groups() {
    let u = u64::MAX;
    let binding = ReceiptBinding {
        attempt: AttemptId(u),
        execution: ExecutionId(u),
        dispatch_effect: EffectId(u),
        reservation: ReservationId(u),
        cancel_effect: Some(EffectId(u)),
    };
    let certificate = ReceiptCertificate {
        effect: EffectId(u),
        attempts_through: AttemptId(u),
        effects_through: EffectId(u),
        reservations_through: ReservationId(u),
        retain: vec![binding; MAX_RECEIPT_BINDINGS],
    };
    assert!(encoded_size(&certificate) <= MAX_RECEIPT_CERTIFICATE_BYTES);
    let batch = journal::Batch::new(vec![RecordedEvent {
        epoch: ClusterEpoch(u),
        event: Event::AcknowledgeReceipts {
            worker: WorkerId(u),
            incarnation: IncarnationId(u),
            certificate,
        },
    }])
    .unwrap()
    .at_feature_epoch(1);
    assert!(batch.encoded_bytes().unwrap() <= MAX_RECEIPT_CERTIFICATE_BYTES + FIXED_ROW_BYTES);
    let wire = nativelink_proto::com::github::trace_machina::nativelink::remote_execution::AuthorityReceiptCertificate {
        effect_id: u, attempts_through: u, effects_through: u, reservations_through: u,
        retain: (0..MAX_RECEIPT_BINDINGS).map(|_| nativelink_proto::com::github::trace_machina::nativelink::remote_execution::AuthorityRetainedReceipt {
            attempt_id: u, execution_id: u, dispatch_effect_id: u, reservation_id: u, cancel_effect_id: u,
        }).collect(),
    };
    assert!(prost::Message::encoded_len(&wire) <= 64 * 1024);
    let mut state = charged_state();
    state.enable_receipt_retirement().unwrap();
    issue(&mut state);
    state.check_receipt_invariants().unwrap();
}

#[test]
fn maximum_width_retirement_fields_fit_existing_prepaid_rows() {
    let u = u64::MAX;
    let resources = Resources {
        cpu_millis: u,
        memory_bytes: u,
        slots: u,
    };
    let worker = WorkerView {
        id: WorkerId(u),
        incarnation: IncarnationId(u),
        session: SessionId(u),
        properties: BTreeMap::new(),
        capacity: resources,
        available: resources,
        draining: false,
        last_heartbeat: u,
        charged: resources,
        quarantined: false,
    };
    let hold = HoldView {
        execution: ExecutionId(u),
        worker: WorkerId(u),
        incarnation: IncarnationId(u),
        started_at: u,
        until: u,
    };
    let certificate = ReceiptCertificate {
        effect: EffectId(u),
        attempts_through: AttemptId(u),
        effects_through: EffectId(u),
        reservations_through: ReservationId(u),
        retain: Vec::new(),
    };
    let stream = ReceiptStream {
        incarnation: IncarnationId(u),
        acknowledged: Some(certificate.floors()),
        pending: Some(certificate),
        dirty: false,
    };
    let worker_group = encoded_size(&BTreeMap::from([(u, &worker)]))
        - encoded_size(&worker.properties)
        + encoded_size(&BTreeMap::from([(u, hold)]))
        + encoded_size(&BTreeMap::from([(u, stream)]));
    assert!(
        worker_group <= 2 * FIXED_ROW_BYTES,
        "worker group {worker_group}"
    );
    let attempt = AttemptView {
        id: AttemptId(u),
        dispatch_effect: EffectId(u),
        execution: ExecutionId(u),
        worker: WorkerId(u),
        incarnation: IncarnationId(u),
        reservation: ReservationId(u),
        authorized: false,
        phase: Phase::Dispatching,
        progress: u,
        deadline: Deadline {
            at: u,
            generation: u,
        },
        running_deadline: Some(u),
    };
    let binding = ReceiptBinding {
        attempt: AttemptId(u),
        execution: ExecutionId(u),
        dispatch_effect: EffectId(u),
        reservation: ReservationId(u),
        cancel_effect: Some(EffectId(u)),
    };
    let attempt_group = encoded_size(&BTreeMap::from([(u, attempt)])) + encoded_size(&binding) + 1;
    assert!(
        attempt_group <= FIXED_ROW_BYTES,
        "attempt group {attempt_group}"
    );
    let mut state = State::new(
        ClusterEpoch(u),
        Limits {
            max_executions: usize::MAX,
            max_operations: usize::MAX,
            max_workers: usize::MAX,
            max_effects: usize::MAX,
            max_payload_bytes: MAX_PAYLOAD_BYTES,
        },
    )
    .unwrap();
    state.now = u;
    state.next_execution = u;
    state.operation_watermark = u;
    state.next_attempt = u;
    state.next_reservation = u;
    state.next_effect = u;
    state.enable_receipt_retirement().unwrap();
    assert!(state.snapshot().unwrap().len() <= FIXED_ROW_BYTES);
}

#[test]
fn oversized_exception_set_defers_atomically_with_all_uncertain_attempts_intact() {
    let mut state = State::new(
        ClusterEpoch(1),
        Limits {
            max_executions: 400,
            max_operations: 400,
            max_workers: 1,
            max_effects: 3200,
            max_payload_bytes: 1024,
        },
    )
    .unwrap();
    let capacity = Resources {
        cpu_millis: 2_000_000,
        memory_bytes: 2_000_000,
        slots: 2000,
    };
    apply(
        &mut state,
        Event::Register {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            properties: BTreeMap::new(),
            capacity,
            available: capacity,
        },
    );
    state.enable_receipt_retirement().unwrap();
    let mut request = charged_state().executions[&ExecutionId(1)].request.clone();
    request.resources = Resources {
        cpu_millis: 1,
        memory_bytes: 1,
        slots: 1,
    };
    request.do_not_cache = true;
    // Three uncertain attempts per exhausted execution exceed the certificate
    // cap while remaining within the authority's aggregate prepaid byte limit.
    for operation in 1..=342 {
        request.action.digest = format!("uncertain-{operation}");
        assert!(matches!(
            state.apply(RecordedEvent {
                epoch: ClusterEpoch(1),
                event: Event::Submit {
                    operation: OperationId(operation),
                    request: request.clone()
                }
            }),
            Reply::Submitted { .. }
        ));
        for _ in 0..3 {
            let Reply::Matched { attempt, .. } = state.apply(RecordedEvent {
                epoch: ClusterEpoch(1),
                event: Event::Match,
            }) else {
                panic!("uncertain grant fits prepaid capacity");
            };
            let deadline = state.attempts[&attempt].deadline;
            state.apply(RecordedEvent {
                epoch: ClusterEpoch(1),
                event: Event::AdvanceTime { now: deadline.at },
            });
            assert_eq!(
                state.apply(RecordedEvent {
                    epoch: ClusterEpoch(1),
                    event: Event::ExpireAttempt {
                        attempt,
                        generation: deadline.generation
                    }
                }),
                Reply::Applied
            );
        }
    }
    state.check_invariants().unwrap();
    assert_eq!(state.attempts.len(), 1026);
    assert!(
        state
            .reservations
            .values()
            .all(|reservation| !reservation.released)
    );
    let before = state.snapshot().unwrap();
    assert_eq!(
        apply(
            &mut state,
            Event::IssueReceipts {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                after_ack: None
            }
        ),
        Reply::Rejected(Rejection::Overloaded)
    );
    assert_eq!(before, state.snapshot().unwrap());
    for attempt in [AttemptId(1), AttemptId(2)] {
        apply(
            &mut state,
            Event::Cleanup {
                attempt,
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
            },
        );
    }
    let certificate = issue(&mut state);
    assert_eq!(certificate.retain.len(), MAX_RECEIPT_BINDINGS);
    assert_eq!(certificate.retain.first().unwrap().attempt, AttemptId(3));
    assert!(
        certificate
            .retain
            .iter()
            .all(|binding| binding.cancel_effect.is_some())
    );
    assert_eq!(acknowledge(&mut state, certificate), Reply::Applied);
    assert_eq!(
        state
            .reservations
            .values()
            .filter(|reservation| !reservation.released)
            .count(),
        1024
    );
}

fn charged_state_after_completed_lookup() -> State {
    let mut state = charged_state();
    let mut request = state.executions[&ExecutionId(1)].request.clone();
    complete(&mut state, AttemptId(1));
    request.skip_cache_lookup = false;
    request.action.digest = "lookup-before-dispatch".into();
    assert!(matches!(
        apply(
            &mut state,
            Event::Submit {
                operation: OperationId(2),
                request,
            },
        ),
        Reply::Submitted { .. }
    ));
    assert_eq!(
        apply(
            &mut state,
            Event::LookupResult {
                effect: EffectId(2),
                result: None,
            },
        ),
        Reply::Applied
    );
    assert!(matches!(
        apply(&mut state, Event::Match),
        Reply::Matched { .. }
    ));
    assert_eq!(state.attempts[&AttemptId(2)].dispatch_effect, EffectId(3));
    state
}

#[test]
fn restore_rejects_a_cancel_binding_for_a_still_authorized_attempt() {
    let mut state = charged_state_after_completed_lookup();
    let mut request = state.executions[&ExecutionId(2)].request.clone();
    request.action.digest = "later-consumed-lookup".into();
    assert!(matches!(
        apply(
            &mut state,
            Event::Submit {
                operation: OperationId(3),
                request,
            },
        ),
        Reply::Submitted { .. }
    ));
    assert_eq!(
        apply(
            &mut state,
            Event::LookupResult {
                effect: EffectId(4),
                result: None,
            },
        ),
        Reply::Applied
    );
    state.enable_receipt_retirement().unwrap();
    issue(&mut state);
    let mut encoded: serde_json::Value =
        serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
    encoded["state"]["receipt_retirement"]["streams"]["1"]["pending"]["retain"][0]["cancel_effect"] =
        4.into();
    assert!(State::restore(&serde_json::to_vec(&encoded).unwrap()).is_err());
}

#[test]
fn restore_rejects_receipt_cuts_that_omit_an_earlier_allocated_dispatch() {
    let mut state = charged_state();
    state.enable_receipt_retirement().unwrap();
    issue(&mut state);
    let mut encoded: serde_json::Value =
        serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
    let certificate = &mut encoded["state"]["receipt_retirement"]["streams"]["1"]["pending"];
    certificate["attempts_through"] = 0.into();
    certificate["reservations_through"] = 0.into();
    certificate["retain"] = serde_json::json!([]);
    assert!(State::restore(&serde_json::to_vec(&encoded).unwrap()).is_err());
}

#[test]
fn restore_checks_pending_and_acknowledged_cuts_against_other_workers() {
    let mut state = charged_state();
    state.enable_receipt_retirement().unwrap();
    let certificate = issue(&mut state);
    let capacity = state.workers[&WorkerId(1)].capacity;
    assert_eq!(
        apply(
            &mut state,
            Event::Register {
                worker: WorkerId(2),
                incarnation: IncarnationId(1),
                session: SessionId(1),
                properties: BTreeMap::new(),
                capacity,
                available: capacity,
            },
        ),
        Reply::Applied
    );
    let mut request = state.executions[&ExecutionId(1)].request.clone();
    request.do_not_cache = true;
    request.action.digest = "other-worker-after-cut".into();
    assert!(matches!(
        apply(
            &mut state,
            Event::Submit {
                operation: OperationId(2),
                request,
            },
        ),
        Reply::Submitted { .. }
    ));
    assert!(matches!(
        apply(&mut state, Event::Match),
        Reply::Matched { .. }
    ));
    let later = state.attempts[&AttemptId(2)];
    assert_eq!(later.worker, WorkerId(2));
    assert!(later.dispatch_effect > certificate.effect);
    let pending_snapshot = state.snapshot().unwrap();
    assert_eq!(acknowledge(&mut state, certificate), Reply::Applied);
    let acknowledged_snapshot = state.snapshot().unwrap();
    for (snapshot, field) in [
        (pending_snapshot, "pending"),
        (acknowledged_snapshot, "acknowledged"),
    ] {
        for cut in [0, 2] {
            let mut encoded: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
            let floors = &mut encoded["state"]["receipt_retirement"]["streams"]["1"][field];
            floors["attempts_through"] = cut.into();
            floors["reservations_through"] = cut.into();
            if field == "pending" && cut == 0 {
                floors["retain"] = serde_json::json!([]);
            }
            assert!(
                State::restore(&serde_json::to_vec(&encoded).unwrap()).is_err(),
                "{field} cut {cut} contradicts the captured global allocator prefix"
            );
        }
    }
}

#[test]
fn restore_rejects_cancel_identity_before_dispatch_after_cleanup() {
    let mut state = charged_state_after_completed_lookup();
    assert_eq!(
        apply(
            &mut state,
            Event::Cancel {
                operation: OperationId(2),
            },
        ),
        Reply::Applied
    );
    state.enable_receipt_retirement().unwrap();
    let certificate = issue(&mut state);
    assert_eq!(certificate.retain[0].cancel_effect, Some(EffectId(4)));
    assert_eq!(
        apply(
            &mut state,
            Event::Cleanup {
                attempt: AttemptId(2),
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
            },
        ),
        Reply::Applied
    );
    assert_eq!(pending(&state), &certificate);
    assert!(!state.effects.contains_key(&EffectId(4)));
    let mut encoded: serde_json::Value =
        serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
    // The consumed lookup leaves a genuine old ID gap before this dispatch.
    encoded["state"]["receipt_retirement"]["streams"]["1"]["pending"]["retain"][0]["cancel_effect"] =
        2.into();
    assert!(State::restore(&serde_json::to_vec(&encoded).unwrap()).is_err());
}
