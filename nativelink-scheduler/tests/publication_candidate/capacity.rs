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

fn capture(bytes: usize, error: bool) -> (ActionResult, a::ResultPayload) {
    let result = ActionResult {
        stdout_raw: vec![255; bytes].into(),
        ..ActionResult::default()
    };
    let response = a::ResultPayload::from_response(
        &ExecuteResponse {
            result: Some(result.clone()),
            status: error.then(|| Status {
                code: 13,
                message: "\0".repeat(100),
                ..Status::default()
            }),
            ..ExecuteResponse::default()
        },
        MAX_PAYLOAD,
    )
    .unwrap();
    (result, response)
}

fn supported_capture(error: bool) -> (ActionResult, a::ResultPayload) {
    let count = (0..=1000)
        .rev()
        .find(|count| {
            let (result, payload) = capture(*count, error);
            let cached = a::ResultPayload::from_response(
                &ExecuteResponse {
                    result: Some(result),
                    cached_result: true,
                    ..ExecuteResponse::default()
                },
                MAX_PAYLOAD,
            )
            .unwrap();
            serde_json::to_vec(&payload).unwrap().len() <= MAX_PAYLOAD
                && (error || serde_json::to_vec(&cached).unwrap().len() <= MAX_PAYLOAD)
        })
        .unwrap();
    capture(count, error)
}

fn audit(state: &PublicationAuthority, maximum: &mut usize) {
    let snapshot = state
        .snapshot()
        .expect("exposed state lost its bounded snapshot");
    assert!(snapshot.len() <= MAX_SNAPSHOT);
    *maximum = (*maximum).max(snapshot.len());
    let recovered = PublicationAuthority::restore(&snapshot).unwrap();
    assert_eq!(recovered.snapshot().unwrap(), snapshot);
}

#[test]
fn stage_rejects_response_above_real_scheduler_encoded_payload_limit() {
    for error in [false, true] {
        let mut state = authority(&[("a", false)]);
        register(&mut state, 1);
        state.admit(1, "a".into(), true, 100).unwrap();
        let attempt = assign(&mut state);
        let (result, response) = capture(1500, error);
        assert!(response.require_response().unwrap().encoded().len() < MAX_PAYLOAD);
        assert!(serde_json::to_vec(&response).unwrap().len() > MAX_PAYLOAD);
        let result_bytes = result.encode_to_vec();
        let object = Object::for_bytes(1, &result_bytes).unwrap();
        state
            .install_physical_for(Some(attempt.execution), key(1), object, true)
            .unwrap();
        let value = Candidate {
            action: "a".into(),
            publisher: worker(attempt),
            result: object,
            result_bytes,
            response,
            closure: BTreeSet::from([Member {
                object,
                key: key(1),
                role: Role::Result,
            }]),
        };
        let before = state.snapshot().unwrap();
        assert_eq!(
            state.stage(1, value, 10, BTreeMap::new()),
            Err(Error::Bounds)
        );
        assert_eq!(state.snapshot().unwrap(), before);
    }
}

#[test]
fn full_fixed_caps_complete_and_snapshot_with_escaping_and_large_child_rows() {
    for error in [false, true] {
        let actions: Vec<_> = (0..MAX_STAGES)
            .map(|id| format!("{id}{}", "\0".repeat(255)))
            .collect();
        let mut state = PublicationAuthority::new(
            actions
                .iter()
                .map(|action| (action.clone(), false))
                .collect(),
        )
        .unwrap();
        let mut maximum = 0;
        let capacity = a::Resources {
            cpu_millis: u64::MAX,
            memory_bytes: u64::MAX,
            slots: u64::MAX,
        };
        let properties = BTreeMap::from([("p".to_owned(), "\0".repeat(681))]);
        assert_eq!(
            serde_json::to_vec(&properties).unwrap().len(),
            MAX_PAYLOAD - 2
        );
        for id in 1..=MAX_STAGES as u64 {
            state
                .scheduler_event(a::Event::Register {
                    worker: a::WorkerId(u64::MAX - id),
                    incarnation: a::IncarnationId(u64::MAX),
                    session: a::SessionId(u64::MAX),
                    properties: properties.clone(),
                    capacity,
                    available: capacity,
                })
                .unwrap();
            audit(&state, &mut maximum);
        }
        for (id, action) in actions.iter().enumerate() {
            state
                .admit((id * 2 + 1) as u64, action.clone(), true, 100)
                .unwrap();
            audit(&state, &mut maximum);
            state
                .admit((id * 2 + 2) as u64, action.clone(), true, 100)
                .unwrap();
            audit(&state, &mut maximum);
        }
        let mut values = Vec::new();
        let (result, response) = supported_capture(error);
        let encoded_size = serde_json::to_vec(&response).unwrap().len();
        assert!(
            encoded_size > MAX_PAYLOAD - 128,
            "largest selected payload {encoded_size}"
        );
        for (id, action) in actions.iter().enumerate() {
            let attempt = assign(&mut state);
            audit(&state, &mut maximum);
            for sequence in 1..=8 {
                state
                    .scheduler_event(a::Event::Progress {
                        attempt: attempt.id,
                        worker: attempt.worker,
                        incarnation: attempt.incarnation,
                        phase: a::Phase::Running,
                        progress: u64::MAX - 8 + sequence,
                    })
                    .unwrap();
                audit(&state, &mut maximum);
            }
            state
                .scheduler_event(a::Event::Capacity {
                    worker: attempt.worker,
                    incarnation: attempt.incarnation,
                    session: a::SessionId(u64::MAX),
                    capacity,
                    available: capacity,
                    draining: false,
                })
                .unwrap();
            audit(&state, &mut maximum);
            state
                .scheduler_event(a::Event::Heartbeat {
                    worker: attempt.worker,
                    incarnation: attempt.incarnation,
                    session: a::SessionId(u64::MAX),
                })
                .unwrap();
            audit(&state, &mut maximum);
            let result_bytes = result.encode_to_vec();
            let object = Object::for_bytes(1, &result_bytes).unwrap();
            let mut closure = BTreeSet::new();
            for index in 0..MAX_MEMBERS {
                let unique = id * MAX_MEMBERS + index;
                let mut namespace = [255; 16];
                namespace[0] = u8::try_from(unique).unwrap();
                let physical = Key {
                    namespace,
                    incarnation: [255; 16],
                    generation: u64::MAX - unique as u64,
                };
                state
                    .install_physical_for(Some(attempt.execution), physical, object, true)
                    .unwrap();
                audit(&state, &mut maximum);
                closure.insert(Member {
                    object,
                    key: physical,
                    role: if index == 0 { Role::Result } else { Role::Blob },
                });
            }
            values.push(Candidate {
                action: action.clone(),
                publisher: worker(attempt),
                result: object,
                result_bytes,
                response: response.clone(),
                closure,
            });
        }
        for (id, value) in values.into_iter().enumerate() {
            let writes = value
                .closure
                .iter()
                .enumerate()
                .map(|(index, member)| (u64::MAX - index as u64, member.key))
                .collect();
            stage_with_writes(&mut state, (id + 1) as u64, value, writes);
            audit(&state, &mut maximum);
        }
        for id in 1..=MAX_STAGES as u64 {
            state.publish(id, id, 100).unwrap();
            audit(&state, &mut maximum);
        }
        assert_eq!(state.operations().len(), MAX_OPERATIONS);
        assert_eq!(state.stages().len(), MAX_STAGES);
        assert_eq!(state.retention_view().generations.len(), MAX_PHYSICAL);
        assert!(
            state
                .scheduler_view()
                .operations
                .iter()
                .all(|row| matches!(row.terminal, Some(a::TerminalResult::Completed(_))))
        );
        if !error {
            for grant in 1..=MAX_GRANTS as u64 {
                let publication = ((grant - 1) / 2) + 1;
                state
                    .serve(
                        &actions[usize::try_from(publication - 1).unwrap()],
                        publication,
                        state.commit(),
                        grant,
                        100,
                    )
                    .unwrap();
                audit(&state, &mut maximum);
            }
        }
        assert_eq!(state.retention_view().writes.len(), 128);
        if !error {
            assert_eq!(state.retention_view().roots.len(), 112);
            assert_eq!(state.grants().len(), MAX_GRANTS);
        }
        for cohort in 1..=MAX_PHYSICAL as u64 {
            state.fence_namespace(cohort).unwrap();
            audit(&state, &mut maximum);
        }
        let completed = state.scheduler_view().operations;
        for now in [100, 1_000_000, u64::MAX - 1000] {
            state.advance(now).unwrap();
            audit(&state, &mut maximum);
            assert_eq!(state.scheduler_view().operations, completed);
        }
        assert!(
            maximum > 1_000_000,
            "capacity witness became smaller: {maximum}"
        );
    }
}

fn stage_with_writes(
    state: &mut PublicationAuthority,
    id: u64,
    value: Candidate,
    writes: BTreeMap<u64, Key>,
) {
    state.stage(id, value, 10, writes).unwrap();
    prepare(state, id);
}

#[test]
fn advancing_time_does_not_invent_excluded_terminal_transitions() {
    let mut state = authority(&[("a", false)]);
    register(&mut state, 1);
    state.admit(1, "a".into(), true, 100).unwrap();
    let attempt = assign(&mut state);
    state
        .scheduler_event(a::Event::Progress {
            attempt: attempt.id,
            worker: attempt.worker,
            incarnation: attempt.incarnation,
            phase: a::Phase::Running,
            progress: 1,
        })
        .unwrap();
    state.advance(u64::MAX - 1000).unwrap();
    assert!(state.scheduler_view().operations[0].terminal.is_none());
    assert!(state.scheduler_view().attempts[0].authorized);
    let before = state.snapshot().unwrap();
    assert_eq!(state.cancel_joined(1), Err(Error::Invalid));
    assert_eq!(
        state.scheduler_event(a::Event::Cancel {
            operation: a::OperationId(1)
        }),
        Err(Error::Invalid)
    );
    assert_eq!(
        state.scheduler_event(a::Event::ExpireExecution {
            attempt: attempt.id
        }),
        Err(Error::Invalid)
    );
    assert_eq!(state.snapshot().unwrap(), before);
}

#[test]
fn stage_rejects_cache_projection_above_the_real_payload_limit() {
    let (result, response) = (0..=1000)
        .rev()
        .map(|count| capture(count, false))
        .find(|(result, response)| {
            let cached = a::ResultPayload::from_response(
                &ExecuteResponse {
                    result: Some(result.clone()),
                    cached_result: true,
                    ..ExecuteResponse::default()
                },
                MAX_PAYLOAD,
            )
            .unwrap();
            serde_json::to_vec(response).unwrap().len() <= MAX_PAYLOAD
                && serde_json::to_vec(&cached).unwrap().len() > MAX_PAYLOAD
        })
        .expect("near-limit wire projection witness");
    let mut state = authority(&[("a", false)]);
    register(&mut state, 1);
    state.admit(1, "a".into(), true, 100).unwrap();
    let attempt = assign(&mut state);
    let result_bytes = result.encode_to_vec();
    let object = Object::for_bytes(1, &result_bytes).unwrap();
    state
        .install_physical_for(Some(attempt.execution), key(1), object, true)
        .unwrap();
    let value = Candidate {
        action: "a".into(),
        publisher: worker(attempt),
        result: object,
        result_bytes,
        response,
        closure: BTreeSet::from([Member {
            object,
            key: key(1),
            role: Role::Result,
        }]),
    };
    let before = state.snapshot().unwrap();
    assert_eq!(
        state.stage(1, value, 10, BTreeMap::new()),
        Err(Error::Bounds)
    );
    assert_eq!(state.snapshot().unwrap(), before);
}

#[test]
fn restore_rejects_a_different_real_scheduler_payload_ceiling() {
    let state = authority(&[("a", false)]);
    let snapshot = String::from_utf8(state.snapshot().unwrap()).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
    let child: Vec<u8> = serde_json::from_value(parsed["scheduler"].clone()).unwrap();
    let child_text = String::from_utf8(child.clone()).unwrap();
    assert_eq!(child_text.matches("\"max_payload_bytes\":4096").count(), 1);
    let changed = child_text
        .replace("\"max_payload_bytes\":4096", "\"max_payload_bytes\":8192")
        .into_bytes();
    let original = serde_json::to_string(&child).unwrap();
    assert_eq!(snapshot.matches(&original).count(), 1);
    let corrupt = snapshot.replacen(&original, &serde_json::to_string(&changed).unwrap(), 1);
    assert_eq!(
        PublicationAuthority::restore(corrupt.as_bytes()),
        Err(Error::Corrupt)
    );
}

#[test]
fn stage_cannot_spend_execution_credit_on_another_qualified_action() {
    let mut state = authority(&[("a", false), ("other/a", false)]);
    register(&mut state, 1);
    state.admit(1, "a".into(), true, 100).unwrap();
    let attempt = assign(&mut state);
    let value = candidate(&mut state, "a", worker(attempt), 1, 0, 0);
    let mut unrelated = value.clone();
    unrelated.action = "other/a".into();
    let before = state.snapshot().unwrap();
    assert_eq!(
        state.stage(1, unrelated, 10, BTreeMap::new()),
        Err(Error::Unauthorized)
    );
    assert_eq!(state.snapshot().unwrap(), before);
    stage(&mut state, 1, value);
    let snapshot = String::from_utf8(state.snapshot().unwrap()).unwrap();
    let original = serde_json::to_string(&state.stages()[&1]).unwrap();
    let mut changed = state.stages()[&1].clone();
    changed.candidate.action = "other/a".into();
    let corrupt = snapshot.replacen(&original, &serde_json::to_string(&changed).unwrap(), 1);
    assert_eq!(
        PublicationAuthority::restore(corrupt.as_bytes()),
        Err(Error::Corrupt)
    );
    state.publish(1, 1, 100).unwrap();
}

#[test]
fn near_ceiling_success_completes_and_serves_real_cached_projection() {
    let mut state = authority(&[("a", false)]);
    register(&mut state, 1);
    state.admit(1, "a".into(), true, 100).unwrap();
    let attempt = assign(&mut state);
    let (result, response) = supported_capture(false);
    let result_bytes = result.encode_to_vec();
    let object = Object::for_bytes(1, &result_bytes).unwrap();
    state
        .install_physical_for(Some(attempt.execution), key(1), object, true)
        .unwrap();
    let value = Candidate {
        action: "a".into(),
        publisher: worker(attempt),
        result: object,
        result_bytes,
        response: response.clone(),
        closure: BTreeSet::from([Member {
            object,
            key: key(1),
            role: Role::Result,
        }]),
    };
    stage(&mut state, 1, value);
    state.publish(1, 1, 100).unwrap();
    state.admit(2, "a".into(), false, 100).unwrap();
    assert_eq!(terminal(&state, 1), a::TerminalResult::Completed(response));
    let a::TerminalResult::Completed(cached) = terminal(&state, 2) else {
        panic!("cache miss")
    };
    assert!(
        cached
            .require_response()
            .unwrap()
            .decode()
            .unwrap()
            .cached_result
    );
    assert!(serde_json::to_vec(&cached).unwrap().len() <= MAX_PAYLOAD);
    let mut maximum = 0;
    audit(&state, &mut maximum);
}

#[test]
fn late_predecessor_cannot_consume_replacement_attempt_stage_credit() {
    for predecessor_staged in [false, true] {
        let mut state = authority(&[("a", false)]);
        register(&mut state, 1);
        state.admit(1, "a".into(), true, 100).unwrap();
        let old = assign(&mut state);
        let old_value = candidate(&mut state, "a", worker(old), 1, 0, 0);
        if predecessor_staged {
            stage(&mut state, 1, old_value.clone());
        }
        register(&mut state, 2);
        state
            .scheduler_event(a::Event::Cleanup {
                attempt: old.id,
                worker: old.worker,
                incarnation: old.incarnation,
            })
            .unwrap();
        let current = assign(&mut state);
        assert_eq!(old.execution, current.execution);
        let before = state.snapshot().unwrap();
        assert_eq!(
            state.stage(3, old_value, 10, BTreeMap::new()),
            Err(Error::Scheduler(a::Rejection::Stale))
        );
        assert_eq!(state.snapshot().unwrap(), before);
        let current_value = candidate(&mut state, "a", worker(current), 2, 0, 0);
        stage(&mut state, 2, current_value);
        state.publish(2, 2, 100).unwrap();
        if predecessor_staged {
            assert_eq!(state.stages()[&1].state, StageState::Open);
        }
        let recovered = PublicationAuthority::restore(&state.snapshot().unwrap()).unwrap();
        assert_eq!(recovered, state);
    }
}
