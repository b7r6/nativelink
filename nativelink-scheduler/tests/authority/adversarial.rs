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

//! Additional incident-derived histories. These exercise the replacement
//! contract, not legacy optional scheduling policies. M1 recipient behavior is
//! simulated; no claim about production transport/process fencing follows.

use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, Command, Digest, ExecuteRequest, ExecuteResponse, ExecutionPolicy,
    Platform, RequestMetadata, ToolDetails,
};
use nativelink_proto::google::rpc::Status;
use nativelink_scheduler::authority::*;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use prost::Message;

use super::harness::Harness;
use super::recipient::{Reception, Recipient};
use super::{apply, assign, finish, harness, register, request, result, submit};

/// Construct neutral DTO input without the candidate's capture/facts helpers.
fn captured_result(response: &ExecuteResponse) -> ResultPayload {
    let encoded = response.encode_to_vec();
    let mut hash = DigestHasherFunc::Sha256.hasher();
    hash.update(&encoded);
    let digest = hash.finalize_digest();
    let sha256: [u8; 32] = **digest.packed_hash();
    let status = response.status.as_ref().map_or(0, |status| status.code);
    ResultPayload {
        digest: digest.packed_hash().to_string(),
        exit_code: response
            .result
            .as_ref()
            .map_or(0, |result| result.exit_code),
        error: (status != 0).then(|| {
            format!(
                "REAPI status {status}: {}",
                response.status.as_ref().unwrap().message
            )
        }),
        response: Some(
            serde_json::from_value(
                serde_json::json!({ "version": 1, "encoded": encoded, "sha256": sha256 }),
            )
            .unwrap(),
        ),
    }
}

#[derive(Clone)]
struct RequestFixture {
    execute: ExecuteRequest,
    metadata: Option<RequestMetadata>,
    queued: prost_types::Timestamp,
    principal: String,
    action: Action,
    command: Command,
}

fn raw_digest(algorithm: DigestHasherFunc, bytes: &[u8]) -> Digest {
    let mut hasher = algorithm.hasher();
    hasher.update(bytes);
    Digest {
        hash: hasher.finalize_digest().packed_hash().to_string(),
        size_bytes: i64::try_from(bytes.len()).unwrap(),
    }
}

fn buck2_scope(principal: &str, invocation: &str) -> String {
    let mut bytes = Vec::from((principal.len() as u64).to_be_bytes());
    bytes.extend_from_slice(principal.as_bytes());
    bytes.extend_from_slice(invocation.as_bytes());
    raw_digest(DigestHasherFunc::Sha256, &bytes).hash
}

impl RequestFixture {
    fn seal(&self, request: &mut Request) {
        request.payload = Some(serde_json::from_value(self.wire(request)).unwrap());
    }

    fn wire(&self, request: &mut Request) -> serde_json::Value {
        let algorithm = if self.execute.digest_function == 9 {
            DigestHasherFunc::Blake3
        } else {
            DigestHasherFunc::Sha256
        };
        let command = self.command.encode_to_vec();
        let mut action = self.action.clone();
        action.command_digest = Some(raw_digest(algorithm, &command));
        let action = action.encode_to_vec();
        let mut execute = self.execute.clone();
        execute.action_digest = Some(raw_digest(algorithm, &action));
        request.action = ActionKey {
            instance: execute.instance_name.clone(),
            digest_function: if self.execute.digest_function == 9 {
                "blake3"
            } else {
                "sha256"
            }
            .into(),
            digest: execute.action_digest.as_ref().unwrap().hash.clone(),
            digest_size_bytes: u64::try_from(action.len()).unwrap(),
        };
        let execute = execute.encode_to_vec();
        let metadata = self.metadata.as_ref().map(Message::encode_to_vec);
        let queued = self.queued.encode_to_vec();
        let mut framed = b"nativelink-request-v1".to_vec();
        framed.extend_from_slice(&1_u32.to_be_bytes());
        framed.push(u8::from(metadata.is_some()));
        for bytes in [
            execute.as_slice(),
            metadata.as_deref().unwrap_or_default(),
            queued.as_slice(),
            self.principal.as_bytes(),
            action.as_slice(),
            command.as_slice(),
        ] {
            framed.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
            framed.extend_from_slice(bytes);
        }
        let mut hasher = DigestHasherFunc::Sha256.hasher();
        hasher.update(&framed);
        let sha256: [u8; 32] = **hasher.finalize_digest().packed_hash();
        serde_json::json!({
            "version": 1, "execute": execute, "metadata": metadata, "queued": queued,
            "principal": self.principal, "action": action, "command": command, "sha256": sha256,
        })
    }
}

fn captured_request() -> (RequestFixture, Request) {
    let platform = |cpu: &str, memory: &str| Platform {
        properties: vec![
            Property {
                name: "os".into(),
                value: "linux".into(),
            },
            Property {
                name: "cpu_count".into(),
                value: cpu.into(),
            },
            Property {
                name: "memory_kb".into(),
                value: memory.into(),
            },
        ],
    };
    let fixture = RequestFixture {
        execute: ExecuteRequest {
            instance_name: "captured-instance".into(),
            skip_cache_lookup: true,
            digest_function: 1,
            execution_policy: Some(ExecutionPolicy { priority: -5 }),
            ..Default::default()
        },
        metadata: Some(RequestMetadata {
            tool_details: Some(ToolDetails {
                tool_name: "BuCk2".into(),
                tool_version: "test".into(),
            }),
            tool_invocation_id: "build-1".into(),
            action_id: "action-1".into(),
            ..Default::default()
        }),
        queued: prost_types::Timestamp {
            seconds: 100,
            nanos: 7,
        },
        principal: "alice".into(),
        action: Action {
            input_root_digest: Some(Digest {
                hash: "a".repeat(64),
                size_bytes: 0,
            }),
            timeout: Some(prost_types::Duration {
                seconds: 1,
                nanos: 1,
            }),
            platform: Some(platform("1", "2")),
            ..Default::default()
        },
        command: Command {
            arguments: vec!["echo".into(), "preserved command".into()],
            platform: Some(platform("2", "4")),
            ..Default::default()
        },
    };
    let mut request = request(1);
    request.execution_scope = Some(buck2_scope("alice", "build-1"));
    request.priority = -5;
    request.resources = Resources {
        cpu_millis: 1000,
        memory_bytes: 2048,
        slots: 1,
    };
    request.execution_timeout = 2;
    fixture.seal(&mut request);
    (fixture, request)
}

#[test]
fn captured_request_normalization_is_independent_and_retains_each_original_ingress() {
    let (fixture, original) = captured_request();
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 4));
    let execution = submit(&mut h, 1, original.clone());
    let mut joined = original.clone();
    let mut later = fixture;
    later.queued.seconds += 1;
    later.metadata.as_mut().unwrap().action_id = "different action metadata".into();
    later.execute.execution_policy = Some(ExecutionPolicy { priority: -9 });
    joined.priority = -9;
    later.seal(&mut joined);
    assert_eq!(submit(&mut h, 2, joined.clone()), execution);
    assert_eq!(h.view().executions[0].request, original);
    assert_eq!(h.view().executions[0].effective_priority, Some(-9));
    assert_eq!(
        apply(
            &mut h,
            Event::Submit {
                operation: OperationId(2),
                request: original.clone()
            }
        ),
        Reply::Rejected(Rejection::Invalid)
    );
    assert_eq!(submit(&mut h, 2, joined.clone()), execution);
    let attempt = assign(&mut h);
    let EffectKind::Dispatch {
        request: dispatched,
        ..
    } = dispatch(&h, attempt.id).kind
    else {
        panic!("expected dispatch")
    };
    assert_eq!(dispatched, original);
    h.checkpoint().unwrap();
    h.crash_restore().unwrap();
    assert_eq!(submit(&mut h, 2, joined), execution);

    let before = h.oracle.snapshot().unwrap();
    for relation in ["interests", "jobs", "outbox"] {
        let mut snapshot: serde_json::Value = serde_json::from_slice(&before).unwrap();
        let captured = match relation {
            "interests" => &mut snapshot[1][relation][0]["original"],
            "jobs" => &mut snapshot[1][relation][0]["request"],
            _ => &mut snapshot[1][relation][0]["kind"]["Dispatch"]["request"],
        };
        assert!(captured.is_object());
        captured["resources"]["memory_bytes"] = serde_json::json!(1);
        assert!(
            super::reference::Reference::restore(&serde_json::to_vec(&snapshot).unwrap()).is_err()
        );
    }
}

#[test]
fn captured_request_raw_fields_reject_forgery_without_consuming_an_operation() {
    let (fixture, original) = captured_request();
    let mut h = harness();
    for case in 0..16 {
        let mut raw = fixture.clone();
        let mut invalid = original.clone();
        match case {
            0 => raw
                .action
                .platform
                .as_mut()
                .unwrap()
                .properties
                .push(Property {
                    name: "os".into(),
                    value: "linux".into(),
                }),
            1 => raw
                .action
                .platform
                .as_mut()
                .unwrap()
                .properties
                .push(Property {
                    name: "slots".into(),
                    value: "1".into(),
                }),
            2 => raw.action.platform.as_mut().unwrap().properties[1].value = "01".into(),
            3 => raw.action.platform.as_mut().unwrap().properties[2].value = u64::MAX.to_string(),
            4 => raw.action.timeout.as_mut().unwrap().seconds = -1,
            5 => raw.action.timeout.as_mut().unwrap().nanos = 1_000_000_000,
            6 => raw.queued.nanos = 1_000_000_000,
            7 => raw.action.input_root_digest.as_mut().unwrap().hash = "A".repeat(64),
            8 => raw.execute.digest_function = 2,
            9 => invalid.resources.cpu_millis += 1,
            10 => invalid.execution_timeout += 1,
            11 => invalid.execution_scope = None,
            12 => invalid.priority += 1,
            13 => invalid.skip_cache_lookup = false,
            14 => invalid.do_not_cache = true,
            _ => {
                invalid.properties.insert("extra".into(), "forged".into());
            }
        }
        let wire = raw.wire(&mut invalid);
        if case < 9 {
            assert!(super::reference::Reference::captured_request_facts(wire.clone()).is_none());
            assert!(serde_json::from_value::<request::RequestPayload>(wire).is_err());
            continue;
        }
        invalid.payload = Some(serde_json::from_value(wire).unwrap());
        assert_eq!(
            apply(
                &mut h,
                Event::Submit {
                    operation: OperationId(1),
                    request: invalid
                }
            ),
            Reply::Rejected(Rejection::Invalid),
            "invalid raw request case {case}"
        );
        assert!(h.view().operations.is_empty());
    }
    submit(&mut h, 1, original);
}

#[test]
fn captured_request_platform_timeout_and_scope_boundaries_have_exact_projection() {
    for case in 0..9 {
        let (mut fixture, mut request) = captured_request();
        match case {
            0 => fixture.execute.digest_function = 9,
            1 => fixture.execute.digest_function = 0,
            2 => {
                fixture.action.timeout = None;
                request.execution_timeout = u64::MAX;
            }
            3 => {
                fixture.action.timeout = Some(prost_types::Duration::default());
                request.execution_timeout = 1;
            }
            4 => {
                fixture.action.platform = Some(Platform::default());
                request.resources.cpu_millis = 2000;
                request.resources.memory_bytes = 4096;
            }
            5 => {
                // Invalid ignored Command requirements do not override Action.
                fixture
                    .command
                    .platform
                    .as_mut()
                    .unwrap()
                    .properties
                    .push(Property {
                        name: "slots".into(),
                        value: "nonsense".into(),
                    });
            }
            6 => {
                fixture.metadata = None;
                request.execution_scope = None;
            }
            7 => {
                fixture
                    .metadata
                    .as_mut()
                    .unwrap()
                    .tool_details
                    .as_mut()
                    .unwrap()
                    .tool_name = "bazel".into();
                request.execution_scope = None;
            }
            _ => {
                fixture.principal = "bob".into();
                request.execution_scope = Some(buck2_scope("bob", "build-1"));
            }
        }
        fixture.seal(&mut request);
        let mut h = harness();
        submit(&mut h, 1, request.clone());
        assert_eq!(
            h.view().executions[0].request,
            request,
            "boundary case {case}"
        );
        h.checkpoint().unwrap();
        h.crash_restore().unwrap();
    }
}

fn held_queue(slots: u64, priority: i32) -> (Harness, ExecutionId, Vec<AttemptView>, Request) {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 4));
    let mut busy = Vec::new();
    for operation in 1..=3 {
        submit(&mut h, operation, request(operation));
        busy.push(assign(&mut h));
    }
    let mut large = request(10);
    large.resources.slots = slots;
    large.priority = priority;
    let execution = submit(&mut h, 10, large.clone());
    apply(&mut h, Event::AdvanceTime { now: HOLD_AFTER });
    assert_eq!(apply(&mut h, Event::Match), Reply::Applied);
    assert_eq!(h.view().holds[0].execution, execution);
    (h, execution, busy, large)
}

fn memory_worker(worker: u64, memory: u64, cpu: u64, slots: u64) -> Event {
    let Event::Register { properties, .. } = register(worker, 1, 1, slots) else {
        unreachable!()
    };
    let capacity = Resources {
        cpu_millis: cpu,
        memory_bytes: memory,
        slots,
    };
    Event::Register {
        worker: WorkerId(worker),
        incarnation: IncarnationId(1),
        session: SessionId(1),
        properties,
        capacity,
        available: capacity,
    }
}

fn memory_event(
    h: &Harness,
    attempt: &AttemptView,
    peak: Option<u64>,
    result: ResultPayload,
) -> Event {
    Event::MemoryFailure {
        attempt: attempt.id,
        worker: attempt.worker,
        incarnation: attempt.incarnation,
        charged: h
            .view()
            .reservations
            .iter()
            .find(|row| row.attempt == attempt.id)
            .unwrap()
            .charged,
        peak_memory_bytes: peak,
        quiescent: true,
        result,
    }
}

fn memory_result() -> ResultPayload {
    captured_result(&ExecuteResponse {
        result: Some(ActionResult {
            exit_code: 137,
            stderr_raw: b"preserved failure evidence".to_vec().into(),
            ..Default::default()
        }),
        message: "typed OOM observation is separate from this full response".into(),
        ..Default::default()
    })
}

fn byte_limits() -> Limits {
    Limits {
        max_payload_bytes: 512 * 1024,
        ..Limits::default()
    }
}

/// Spend the exact remaining credit on admitted worker properties. The filler
/// workers have zero slots and cannot change placement choices.
fn fill_byte_credit(h: &mut Harness) {
    let empty_properties = std::collections::BTreeMap::from([
        ("os".to_owned(), "linux".to_owned()),
        ("padding".to_owned(), String::new()),
    ]);
    let empty_size = serde_json::to_vec(&empty_properties).unwrap().len();
    let minimum = 2 * 1024 + empty_size;
    while h.oracle.reserved_bytes().unwrap() < super::reference::CREDIT_LIMIT {
        let gap = super::reference::CREDIT_LIMIT - h.oracle.reserved_bytes().unwrap();
        let mut spend = gap.min(512 * 1024 + 2 * 1024);
        if gap > spend && gap - spend < minimum {
            spend -= minimum - (gap - spend);
        }
        assert!(spend >= minimum);
        let worker = h
            .view()
            .workers
            .iter()
            .map(|worker| worker.id.0)
            .max()
            .unwrap_or(0)
            + 1;
        let mut event = memory_worker(worker, 0, 0, 0);
        if let Event::Register { properties, .. } = &mut event {
            *properties = empty_properties.clone();
            properties.insert("padding".into(), "a".repeat(spend - minimum));
        }
        assert_eq!(apply(h, event), Reply::Applied);
    }
    assert_eq!(
        h.oracle.reserved_bytes(),
        Some(super::reference::CREDIT_LIMIT)
    );
}

fn largest_captured_success(limit: usize) -> ResultPayload {
    let candidate = |length: usize| {
        captured_result(&ExecuteResponse {
            result: Some(ActionResult {
                stdout_raw: vec![255; length].into(),
                ..Default::default()
            }),
            ..Default::default()
        })
    };
    let mut low = 0;
    let mut high = 256 * 1024;
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        if serde_json::to_vec(&candidate(middle)).unwrap().len() <= limit {
            low = middle;
        } else {
            high = middle;
        }
    }
    let result = candidate(low);
    assert!((limit - 8..=limit).contains(&serde_json::to_vec(&result).unwrap().len()));
    result
}

#[test]
fn byte_credit_counts_distinct_coalesced_ingress_and_prepays_complete_terminal_copies() {
    let mut h = Harness::new(ClusterEpoch(1), byte_limits());
    apply(&mut h, register(1, 1, 1, 4));
    let (mut fixture, mut original) = captured_request();
    fixture.principal = "\"\\".repeat(32 * 1024);
    original.execution_scope = Some(buck2_scope(&fixture.principal, "build-1"));
    let mut accepted = Vec::new();
    let rejected = loop {
        let operation = accepted.len() as u64 + 1;
        fixture.metadata.as_mut().unwrap().action_id = format!("distinct ingress {operation}");
        fixture.queued.nanos = i32::try_from(operation).unwrap();
        fixture.seal(&mut original);
        let before = h.oracle.reserved_bytes().unwrap();
        match apply(
            &mut h,
            Event::Submit {
                operation: OperationId(operation),
                request: original.clone(),
            },
        ) {
            Reply::Submitted { execution, .. } => {
                assert_eq!(execution, ExecutionId(1));
                if !accepted.is_empty() {
                    assert_eq!(
                        h.oracle.reserved_bytes().unwrap() - before,
                        serde_json::to_vec(&original).unwrap().len()
                            + byte_limits().max_payload_bytes
                            + 1024
                    );
                }
                accepted.push(original.clone());
                assert!(accepted.len() < 64);
            }
            Reply::Rejected(Rejection::Overloaded) => break operation,
            reply => panic!("unexpected byte admission reply {reply:?}"),
        }
    };
    assert!(accepted.len() > 8);
    assert_eq!(h.view().executions.len(), 1);
    fill_byte_credit(&mut h);
    let unchanged = h.core.snapshot().unwrap();
    assert_eq!(
        apply(
            &mut h,
            Event::Submit {
                operation: OperationId(rejected),
                request: original.clone()
            }
        ),
        Reply::Rejected(Rejection::Overloaded)
    );
    assert_eq!(h.core.snapshot().unwrap(), unchanged);
    assert_eq!(submit(&mut h, 1, accepted[0].clone()), ExecutionId(1));
    h.checkpoint().unwrap();
    h.crash_restore().unwrap();
    let attempt = assign(&mut h);
    let result = largest_captured_success(byte_limits().max_payload_bytes);
    assert_eq!(
        apply(
            &mut h,
            Event::Complete {
                attempt: attempt.id,
                worker: attempt.worker,
                incarnation: attempt.incarnation,
                result: result.clone(),
            }
        ),
        Reply::Applied
    );
    assert_eq!(
        h.oracle.reserved_bytes(),
        Some(super::reference::CREDIT_LIMIT)
    );
    assert!(
        h.view()
            .operations
            .iter()
            .all(|operation| operation.terminal == Some(TerminalResult::Completed(result.clone())))
    );
    h.checkpoint().unwrap();
    h.crash_restore().unwrap();
    let publication = h
        .view()
        .effects
        .iter()
        .find(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
        .unwrap()
        .id;
    apply(
        &mut h,
        Event::EffectOutcome {
            effect: publication,
            success: true,
        },
    );
    assert_eq!(
        h.oracle.reserved_bytes(),
        Some(super::reference::CREDIT_LIMIT)
    );
    apply(
        &mut h,
        Event::AdvanceTime {
            now: TERMINAL_RETENTION,
        },
    );
    apply(
        &mut h,
        Event::RetireExecution {
            execution: ExecutionId(1),
        },
    );
    assert!(h.oracle.reserved_bytes().unwrap() < super::reference::CREDIT_LIMIT / 2);
    assert_eq!(submit(&mut h, rejected, original), ExecutionId(2));
}

#[test]
fn byte_credit_worker_replacement_refuses_atomically_and_cancellation_cleanup_stay_available() {
    let mut h = Harness::new(ClusterEpoch(1), byte_limits());
    apply(&mut h, register(1, 1, 1, 4));
    for operation in 1..=9 {
        submit(&mut h, operation, request(operation));
    }
    let attempt = assign(&mut h);
    fill_byte_credit(&mut h);
    let original = h.view();
    let mut replacement = register(1, 2, 1, 4);
    if let Event::Register { properties, .. } = &mut replacement {
        properties.insert("new".into(), "x".into());
    }
    assert_eq!(
        apply(&mut h, replacement.clone()),
        Reply::Rejected(Rejection::Overloaded)
    );
    assert_eq!(h.view(), original);
    if let Event::Register { incarnation, .. } = &mut replacement {
        *incarnation = IncarnationId(0);
    }
    assert_eq!(
        apply(&mut h, replacement),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(h.view(), original);
    assert_eq!(
        apply(
            &mut h,
            Event::Cancel {
                operation: OperationId(1)
            }
        ),
        Reply::Applied
    );
    assert_eq!(
        h.oracle.reserved_bytes(),
        Some(super::reference::CREDIT_LIMIT)
    );
    assert!(!h.view().reservations[0].released);
    assert_eq!(apply(&mut h, cleanup(&attempt)), Reply::Applied);
    assert!(h.view().reservations[0].released);
    let another = assign(&mut h);
    finish(&mut h, &another, 2);
    assert_eq!(
        h.oracle.reserved_bytes(),
        Some(super::reference::CREDIT_LIMIT)
    );
}

#[test]
fn byte_credit_accepts_exact_limit_and_rejects_one_more_encoded_byte() {
    let mut h = Harness::new(ClusterEpoch(1), byte_limits());
    for operation in 1..=9 {
        submit(&mut h, operation, request(operation));
    }
    fill_byte_credit(&mut h);
    let worker = h.view().workers.last().unwrap().clone();
    let mut properties = worker.properties.clone();
    properties.get_mut("padding").unwrap().push('a');
    let replacement = |properties, session| Event::Register {
        worker: worker.id,
        incarnation: worker.incarnation,
        session: SessionId(session),
        properties,
        capacity: worker.capacity,
        available: worker.available,
    };
    let before = h.core.snapshot().unwrap();
    assert_eq!(
        apply(&mut h, replacement(properties, 2)),
        Reply::Rejected(Rejection::Overloaded)
    );
    assert_eq!(h.core.snapshot().unwrap(), before);
    let mut smaller = worker.properties.clone();
    assert_eq!(smaller.get_mut("padding").unwrap().pop(), Some('a'));
    assert_eq!(apply(&mut h, replacement(smaller, 2)), Reply::Applied);
    assert_eq!(
        h.oracle.reserved_bytes(),
        Some(super::reference::CREDIT_LIMIT - 1)
    );
    assert_eq!(
        apply(&mut h, replacement(worker.properties.clone(), 3)),
        Reply::Applied
    );
    assert_eq!(
        h.oracle.reserved_bytes(),
        Some(super::reference::CREDIT_LIMIT)
    );
}

#[test]
fn byte_credit_limits_and_restore_cannot_admit_uncommittable_or_unpaid_state() {
    for bound in [512 * 1024 + 1, usize::MAX] {
        let limits = Limits {
            max_payload_bytes: bound,
            ..Limits::default()
        };
        assert!(State::new(ClusterEpoch(1), limits).is_err());
        assert!(super::reference::Reference::new(ClusterEpoch(1), limits).is_err());
    }
    for bound in [0, 512 * 1024] {
        let limits = Limits {
            max_payload_bytes: bound,
            max_executions: usize::MAX,
            max_operations: usize::MAX,
            max_workers: usize::MAX,
            max_effects: usize::MAX,
        };
        assert!(State::new(ClusterEpoch(1), limits).is_ok());
        assert!(super::reference::Reference::new(ClusterEpoch(1), limits).is_ok());
    }
    let mut h = Harness::new(ClusterEpoch(1), byte_limits());
    for operation in 1..=9 {
        submit(&mut h, operation, request(operation));
    }
    fill_byte_credit(&mut h);
    let bytes = h.oracle.snapshot().unwrap();
    let mut invalid: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    invalid[1]["limits"]["max_payload_bytes"] = serde_json::json!(usize::MAX);
    assert!(super::reference::Reference::restore(&serde_json::to_vec(&invalid).unwrap()).is_err());
    let mut unpaid: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let mut added = unpaid[1]["interests"][0].clone();
    added["operation"] = serde_json::json!(10);
    unpaid[1]["interests"].as_array_mut().unwrap().push(added);
    unpaid[1]["highest_admitted_operation"] = serde_json::json!(10);
    let error =
        super::reference::Reference::restore(&serde_json::to_vec(&unpaid).unwrap()).unwrap_err();
    assert!(error.contains("future byte credits"));
    let mut oversized = bytes;
    oversized.resize(super::reference::CREDIT_LIMIT + 1, b' ');
    assert!(
        super::reference::Reference::restore(&oversized)
            .unwrap_err()
            .contains("raw snapshot")
    );
    assert!(State::restore(&oversized).is_err());
}

#[test]
fn memory_growth_preserves_accepted_request_age_join_and_each_original_grant() {
    let (_, original) = captured_request();
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 4));
    let execution = submit(&mut h, 1, original.clone());
    let first = assign(&mut h);
    apply(&mut h, Event::AdvanceTime { now: 17 });
    let failure = memory_event(&h, &first, Some(1), memory_result());
    assert_eq!(apply(&mut h, failure.clone()), Reply::Applied);
    let grown = h.view().executions[0].clone();
    assert_eq!(grown.request, original);
    assert_eq!(grown.reservation_target.memory_bytes, 4096);
    assert_eq!(grown.enqueue_at, 0);
    assert_eq!(grown.phase, Phase::Queued);
    assert!(h.view().effects.is_empty());
    assert_eq!(submit(&mut h, 2, original.clone()), execution);
    h.checkpoint().unwrap();
    h.crash_restore().unwrap();
    let second = assign(&mut h);
    let EffectKind::Dispatch {
        charged,
        request: captured,
        ..
    } = dispatch(&h, second.id).kind
    else {
        unreachable!()
    };
    assert_eq!(captured, original);
    assert_eq!(charged, grown.reservation_target);
    assert_eq!(h.view().reservations[0].charged.memory_bytes, 2048);
    assert!(h.view().reservations[0].released);
    assert_eq!(h.view().reservations[1].charged.memory_bytes, 4096);
    let before = h.view();
    assert_eq!(apply(&mut h, failure), Reply::Rejected(Rejection::Stale));
    assert_eq!(h.view(), before);
    finish(&mut h, &second, 2);
    assert!(
        h.view()
            .operations
            .iter()
            .all(|row| matches!(row.terminal, Some(TerminalResult::Completed(_))))
    );
}

#[test]
fn memory_growth_requires_one_compatible_shape_and_ignores_transient_free_memory() {
    let mut h = harness();
    apply(&mut h, memory_worker(1, 8, 100, 1));
    let mut incompatible = memory_worker(2, 64, 100, 1);
    if let Event::Register { properties, .. } = &mut incompatible {
        properties.insert("os".into(), "other".into());
    }
    apply(&mut h, incompatible);
    apply(&mut h, memory_worker(3, 64, 50, 1));
    apply(&mut h, memory_worker(4, 64, 100, 0));
    let mut original = request(1);
    original.resources.memory_bytes = 4;
    submit(&mut h, 1, original);
    let first = assign(&mut h);
    assert_eq!(first.worker, WorkerId(1));
    apply(
        &mut h,
        Event::Capacity {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            capacity: Resources {
                cpu_millis: 100,
                memory_bytes: 8,
                slots: 1,
            },
            available: Resources {
                cpu_millis: 100,
                memory_bytes: 4,
                slots: 1,
            },
            draining: false,
        },
    );
    let event = memory_event(&h, &first, None, memory_result());
    apply(&mut h, event);
    assert_eq!(h.view().executions[0].reservation_target.memory_bytes, 8);
    assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
    apply(
        &mut h,
        Event::Capacity {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            capacity: Resources {
                cpu_millis: 100,
                memory_bytes: 8,
                slots: 1,
            },
            available: Resources {
                cpu_millis: 100,
                memory_bytes: 8,
                slots: 1,
            },
            draining: false,
        },
    );
    let second = assign(&mut h);
    assert_eq!(second.worker, WorkerId(1));
    let event = memory_event(&h, &second, None, memory_result());
    apply(&mut h, event);
    assert!(matches!(
        h.view().executions[0].terminal,
        Some(TerminalResult::ResourceFailed {
            reason: ResourceFailureReason::NoLargerEligibleWorker,
            ..
        })
    ));
}

#[test]
fn memory_growth_subtracts_immovable_cpu_and_slots_before_choosing_a_ceiling() {
    for (cpu, slots) in [(100, 3), (1000, 1)] {
        let mut h = harness();
        apply(&mut h, memory_worker(2, 64, cpu, slots));
        let mut previous = request(1);
        previous.resources.memory_bytes = 1;
        submit(&mut h, 1, previous);
        let old = assign(&mut h);
        apply(
            &mut h,
            Event::Cancel {
                operation: OperationId(1),
            },
        );
        assert!(!h.view().reservations[0].released);
        apply(&mut h, memory_worker(1, 8, 100, 1));
        let mut original = request(2);
        original.resources.memory_bytes = 4;
        submit(&mut h, 2, original);
        let current = assign(&mut h);
        assert_eq!(current.worker, WorkerId(1));
        let event = memory_event(&h, &current, None, memory_result());
        apply(&mut h, event);
        assert_eq!(h.view().executions[1].reservation_target.memory_bytes, 8);
        assert_eq!(h.view().workers[1].charged.memory_bytes, 1);
        assert!(!h.view().reservations[0].released);
        // At 4 bytes both the real 8-byte ceiling and a fictitious 64-byte
        // ceiling permit doubling. Fail the 8-byte retry while the unrelated
        // CPU/slot charge is still immovable: only the fictitious shape would
        // now allow a 16-byte target.
        let retry = assign(&mut h);
        assert_eq!(retry.worker, WorkerId(1));
        let event = memory_event(&h, &retry, None, memory_result());
        apply(&mut h, event);
        assert_eq!(h.view().executions[1].reservation_target.memory_bytes, 8);
        assert!(matches!(
            h.view().executions[1].terminal,
            Some(TerminalResult::ResourceFailed {
                reason: ResourceFailureReason::NoLargerEligibleWorker,
                ..
            })
        ));
        assert!(!h.view().reservations[0].released);
        apply(&mut h, cleanup(&old));
        assert_eq!(h.view().workers[1].charged, Resources::default());
    }
}

#[test]
fn memory_failure_rejects_uncertain_or_contradictory_evidence_without_mutation() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 2));
    submit(&mut h, 1, request(1));
    let current = assign(&mut h);
    let baseline = h.view();
    for case in 0..6 {
        let mut event = memory_event(&h, &current, None, memory_result());
        let Event::MemoryFailure {
            quiescent,
            charged,
            result,
            worker,
            incarnation,
            ..
        } = &mut event
        else {
            unreachable!()
        };
        match case {
            0 => *quiescent = false,
            1 => charged.memory_bytes -= 1,
            2 => {
                *result = captured_result(&ExecuteResponse {
                    result: Some(ActionResult::default()),
                    ..Default::default()
                });
            }
            3 => result.exit_code += 1,
            4 => *worker = WorkerId(2),
            _ => *incarnation = IncarnationId(2),
        }
        assert_eq!(
            apply(&mut h, event),
            Reply::Rejected(if case >= 4 {
                Rejection::Stale
            } else {
                Rejection::Invalid
            })
        );
        assert_eq!(h.view(), baseline);
    }
}

#[test]
fn memory_zero_evidence_and_capped_doubling_have_no_overflow_or_invented_minimum() {
    for (initial, peak, ceiling, expected, reason) in [
        (
            0,
            None,
            64,
            0,
            Some(ResourceFailureReason::MissingGrowthEvidence),
        ),
        (
            0,
            Some(0),
            64,
            0,
            Some(ResourceFailureReason::MissingGrowthEvidence),
        ),
        (0, Some(3), 64, 6, None),
        (0, Some(u64::MAX), u64::MAX, u64::MAX, None),
        (u64::MAX / 2 + 1, None, u64::MAX, u64::MAX, None),
        (
            u64::MAX,
            None,
            u64::MAX,
            u64::MAX,
            Some(ResourceFailureReason::NoLargerEligibleWorker),
        ),
    ] {
        let mut h = harness();
        apply(&mut h, memory_worker(1, ceiling, 100, 1));
        let mut original = request(1);
        original.resources.memory_bytes = initial;
        submit(&mut h, 1, original);
        let attempt = assign(&mut h);
        let response = memory_result();
        let event = memory_event(&h, &attempt, peak, response.clone());
        assert_eq!(apply(&mut h, event), Reply::Applied);
        assert_eq!(
            h.view().executions[0].reservation_target.memory_bytes,
            expected
        );
        assert_eq!(
            h.view().executions[0].terminal,
            reason.map(|reason| TerminalResult::ResourceFailed {
                result: response,
                reason
            })
        );
        h.checkpoint().unwrap();
        h.crash_restore().unwrap();
    }
}

#[test]
fn memory_retry_budget_is_three_total_attempts_and_preserves_full_final_failure() {
    let mut h = harness();
    apply(&mut h, memory_worker(1, 64, 100, 1));
    let mut original = request(1);
    original.resources.memory_bytes = 4;
    submit(&mut h, 1, original);
    let response = memory_result();
    for (index, expected) in [4, 8, 16].into_iter().enumerate() {
        let attempt = assign(&mut h);
        assert_eq!(h.view().reservations[index].charged.memory_bytes, expected);
        let event = memory_event(&h, &attempt, None, response.clone());
        apply(&mut h, event);
        assert_eq!(apply(&mut h, cleanup(&attempt)), Reply::Ignored);
    }
    assert_eq!(h.view().executions[0].attempts_started, MAX_ATTEMPTS);
    assert_eq!(
        h.view().operations[0].terminal,
        Some(TerminalResult::ResourceFailed {
            result: response,
            reason: ResourceFailureReason::AttemptBudgetExhausted,
        })
    );
    assert!(h.view().effects.is_empty());
    assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
    h.checkpoint().unwrap();
    h.crash_restore().unwrap();
}

#[test]
fn memory_growth_never_changes_an_uncertain_predecessors_cleanup_charge() {
    let mut h = harness();
    apply(&mut h, memory_worker(1, 64, 1000, 3));
    let mut original = request(1);
    original.resources.memory_bytes = 4;
    submit(&mut h, 1, original);
    let first = assign(&mut h);
    apply(&mut h, Event::AdvanceTime { now: ACK_TIMEOUT });
    apply(
        &mut h,
        Event::ExpireAttempt {
            attempt: first.id,
            generation: first.deadline.generation,
        },
    );
    let second = assign(&mut h);
    let event = memory_event(&h, &second, None, memory_result());
    apply(&mut h, event);
    assert_eq!(h.view().executions[0].reservation_target.memory_bytes, 8);
    assert_eq!(h.view().workers[0].charged.memory_bytes, 4);
    let stale = memory_event(&h, &first, Some(u64::MAX), memory_result());
    let before = h.view();
    assert_eq!(apply(&mut h, stale), Reply::Rejected(Rejection::Stale));
    assert_eq!(h.view(), before);
    h.checkpoint().unwrap();
    h.crash_restore().unwrap();
    let third = assign(&mut h);
    assert_eq!(h.view().workers[0].charged.memory_bytes, 12);
    apply(&mut h, cleanup(&first));
    assert_eq!(h.view().workers[0].charged.memory_bytes, 8);
    assert_eq!(h.view().reservations[0].charged.memory_bytes, 4);
    assert_eq!(h.view().reservations[2].charged.memory_bytes, 8);
    finish(&mut h, &third, 3);
}

#[test]
fn held_worker_uses_grown_target_while_immutable_request_keeps_original_minimum() {
    let mut h = harness();
    apply(&mut h, memory_worker(1, 40, 1000, 4));
    let mut original = request(1);
    original.priority = -5;
    submit(&mut h, 1, original);
    let first = assign(&mut h);
    let event = memory_event(&h, &first, None, memory_result());
    apply(&mut h, event);
    let mut busy = Vec::new();
    for operation in 2..=4 {
        let mut small = request(operation);
        small.priority = -10;
        submit(&mut h, operation, small);
        busy.push(assign(&mut h));
    }
    assert_eq!(h.view().workers[0].charged.memory_bytes, 30);
    apply(&mut h, Event::AdvanceTime { now: HOLD_AFTER });
    assert_eq!(apply(&mut h, Event::Match), Reply::Applied);
    assert_eq!(h.view().holds[0].execution, ExecutionId(1));
    submit(&mut h, 5, request(5));
    assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
    finish(&mut h, &busy[0], 2);
    let grown = assign(&mut h);
    assert_eq!(grown.execution, ExecutionId(1));
    assert_eq!(h.view().executions[0].request.resources.memory_bytes, 10);
    assert_eq!(h.view().workers[0].charged.memory_bytes, 40);
}

#[test]
fn aged_one_completion_wait_cannot_be_starved_by_repeated_small_backfill() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 4));
    let mut busy = std::collections::VecDeque::new();
    for operation in 1..=3 {
        submit(&mut h, operation, request(operation));
        busy.push_back(assign(&mut h));
    }
    let mut large = request(4);
    large.resources.slots = 2;
    large.priority = -10;
    let large_id = submit(&mut h, 4, large);
    // Before the fixed age threshold, every completion frees one slot and a
    // small arrival consumes it. "Fits after one completion" repeats forever
    // unless the free remainder stops being spent on lower-ranked work.
    for now in 1..HOLD_AFTER {
        apply(&mut h, Event::AdvanceTime { now });
        submit(&mut h, now + 4, request(now + 4));
        busy.push_back(assign(&mut h));
        finish(&mut h, &busy.pop_front().unwrap(), now);
        assert!(h.view().holds.is_empty());
        assert_eq!(h.view().workers[0].charged.slots, 3);
    }
    apply(&mut h, Event::AdvanceTime { now: HOLD_AFTER });
    submit(&mut h, HOLD_AFTER + 4, request(HOLD_AFTER + 4));
    assert_eq!(apply(&mut h, Event::Match), Reply::Applied);
    assert_eq!(h.view().holds[0].execution, large_id);
    assert_eq!(h.view().workers[0].charged.slots, 3);
    h.checkpoint().unwrap();
    h.crash_restore().unwrap();
    finish(&mut h, &busy.pop_front().unwrap(), 100);
    let dispatched = assign(&mut h);
    assert_eq!(dispatched.execution, large_id);
    assert_eq!(h.view().workers[0].charged.slots, 4);
    assert!(h.view().holds.is_empty());
}

#[test]
fn held_worker_accumulates_capacity_without_minting_or_refunding_reservations() {
    let (mut h, execution, busy, _) = held_queue(4, -10);
    let before = h.view().reservations;
    submit(&mut h, 11, request(11));
    assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
    assert_eq!(h.view().reservations, before);
    for (index, attempt) in busy.iter().enumerate() {
        finish(&mut h, attempt, index as u64);
        if index < 2 {
            assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
        }
    }
    let attempt = assign(&mut h);
    assert_eq!(attempt.execution, execution);
    assert!(h.view().holds.is_empty());
    assert_eq!(h.view().workers[0].charged.slots, 4);
}

#[test]
fn hold_uses_current_effective_priority_and_keeps_deadline_across_reconnect() {
    let (mut h, execution, _, mut joining) = held_queue(4, 0);
    let held = h.view().holds[0];
    let mut urgent_small = request(11);
    urgent_small.priority = -5;
    let small = submit(&mut h, 11, urgent_small);
    joining.priority = -10;
    assert_eq!(submit(&mut h, 12, joining), execution);
    assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
    apply(&mut h, register(1, 1, 2, 4));
    apply(
        &mut h,
        Event::Heartbeat {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(2),
        },
    );
    assert_eq!(h.view().holds, [held]);
    assert_eq!(
        apply(
            &mut h,
            Event::WorkerUnavailable {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(1)
            }
        ),
        Reply::Rejected(Rejection::Stale)
    );
    apply(
        &mut h,
        Event::Cancel {
            operation: OperationId(12),
        },
    );
    let backfilled = assign(&mut h);
    assert_eq!(backfilled.execution, small);
    assert_eq!(h.view().holds, [held]);
    h.checkpoint().unwrap();
    h.crash_restore().unwrap();
    assert_eq!(h.view().holds, [held]);
}

#[test]
fn hold_expiry_records_cooldown_without_observer_or_match_and_does_not_extend() {
    let (mut h, execution, _, _) = held_queue(4, -10);
    let until = h.view().holds[0].until;
    for now in HOLD_AFTER + 1..HOLD_AFTER + 4 {
        apply(&mut h, Event::AdvanceTime { now });
        apply(
            &mut h,
            Event::Heartbeat {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(1),
            },
        );
        assert_eq!(h.view().holds[0].until, until);
    }
    apply(&mut h, Event::AdvanceTime { now: until });
    assert!(h.view().holds.is_empty());
    let owner = h
        .view()
        .executions
        .into_iter()
        .find(|row| row.id == execution)
        .unwrap();
    assert_eq!(owner.hold_cooldown_until, until + HOLD_COOLDOWN);
    assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
    h.checkpoint().unwrap();
    h.crash_restore().unwrap();
    apply(
        &mut h,
        Event::AdvanceTime {
            now: until + HOLD_COOLDOWN,
        },
    );
    assert_eq!(apply(&mut h, Event::Match), Reply::Applied);
    assert_eq!(h.view().holds[0].started_at, until + HOLD_COOLDOWN);
    assert_eq!(
        h.view().holds[0].until,
        until + HOLD_COOLDOWN + HOLD_DURATION
    );
}

#[test]
fn hold_invalidations_never_treat_worker_loss_or_capacity_veto_as_cleanup() {
    for case in 0..6 {
        let (mut h, execution, busy, _) = held_queue(4, -10);
        let capacity = h.view().workers[0].capacity;
        let event = match case {
            0 => Event::Cancel {
                operation: OperationId(10),
            },
            1 => Event::Capacity {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(1),
                capacity,
                available: capacity,
                draining: true,
            },
            2 => Event::Capacity {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(1),
                capacity,
                available: Resources {
                    slots: 2,
                    ..capacity
                },
                draining: false,
            },
            3 => register(1, 2, 2, 4),
            4 => Event::WorkerUnavailable {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(1),
            },
            _ => Event::Decline {
                attempt: busy[0].id,
                worker: busy[0].worker,
                incarnation: busy[0].incarnation,
            },
        };
        assert_eq!(apply(&mut h, event), Reply::Applied);
        assert!(h.view().holds.is_empty());
        assert_eq!(
            h.view().workers[0].charged.slots,
            if case == 5 { 2 } else { 3 }
        );
        assert_eq!(
            h.view()
                .executions
                .iter()
                .find(|row| row.id == execution)
                .unwrap()
                .hold_cooldown_until,
            0
        );
        if case == 4 {
            assert!(h.view().attempts.iter().all(|attempt| !attempt.authorized));
            assert_eq!(
                apply(
                    &mut h,
                    Event::WorkerUnavailable {
                        worker: WorkerId(1),
                        incarnation: IncarnationId(1),
                        session: SessionId(1)
                    }
                ),
                Reply::Ignored
            );
            apply(
                &mut h,
                Event::Heartbeat {
                    worker: WorkerId(1),
                    incarnation: IncarnationId(1),
                    session: SessionId(1),
                },
            );
            assert!(h.view().workers[0].draining);
        }
        h.checkpoint().unwrap();
        h.crash_restore().unwrap();
    }
}

#[test]
fn uncertain_capacity_is_an_immovable_floor_and_cleanup_can_place_holder_elsewhere() {
    for revoke_healthy in [false, true] {
        let mut h = harness();
        apply(&mut h, register(1, 1, 1, 4));
        let mut full = request(1);
        full.resources.slots = 4;
        submit(&mut h, 1, full);
        let uncertain = assign(&mut h);
        apply(
            &mut h,
            Event::Cancel {
                operation: OperationId(1),
            },
        );
        apply(&mut h, register(2, 1, 1, 4));
        for operation in 2..=4 {
            submit(&mut h, operation, request(operation));
            assert_eq!(assign(&mut h).worker, WorkerId(2));
        }
        let mut large = request(10);
        large.resources.slots = 4;
        large.priority = -10;
        let execution = submit(&mut h, 10, large);
        apply(&mut h, Event::AdvanceTime { now: HOLD_AFTER });
        assert_eq!(apply(&mut h, Event::Match), Reply::Applied);
        assert_eq!(h.view().holds[0].worker, WorkerId(2));
        if revoke_healthy {
            apply(
                &mut h,
                Event::Cancel {
                    operation: OperationId(2),
                },
            );
            assert!(h.view().holds.is_empty());
            assert_eq!(h.view().workers[1].charged.slots, 3);
            assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
        } else {
            apply(&mut h, cleanup(&uncertain));
            let placed = assign(&mut h);
            assert_eq!((placed.execution, placed.worker), (execution, WorkerId(1)));
            assert!(h.view().holds.is_empty());
            assert_eq!(h.view().workers[1].charged.slots, 3);
        }
    }
}

#[test]
fn fleet_hold_cap_trims_by_current_rank_and_excludes_zero_admission_workers() {
    let mut h = harness();
    for worker in 1..=20 {
        let mut event = register(worker, 1, 1, 4);
        let Event::Register { properties, .. } = &mut event else {
            unreachable!()
        };
        properties.insert("worker-id".into(), worker.to_string());
        properties.insert("bulk".into(), if worker <= 2 { "yes" } else { "no" }.into());
        apply(&mut h, event);
    }
    for operation in 1..=6 {
        let worker = if operation <= 3 { 1 } else { 2 };
        let mut small = request(operation);
        small
            .properties
            .insert("worker-id".into(), worker.to_string());
        submit(&mut h, operation, small);
        assert_eq!(assign(&mut h).worker, WorkerId(worker));
    }
    let mut owners = Vec::new();
    for operation in 10..=11 {
        let mut large = request(operation);
        large.properties.insert("bulk".into(), "yes".into());
        large.resources.slots = 4;
        owners.push(submit(&mut h, operation, large));
    }
    apply(&mut h, Event::AdvanceTime { now: HOLD_AFTER });
    assert_eq!(apply(&mut h, Event::Match), Reply::Applied);
    assert_eq!(h.view().holds.len(), 2);
    let capacity = h.view().workers[19].capacity;
    apply(
        &mut h,
        Event::Capacity {
            worker: WorkerId(20),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            capacity,
            available: Resources {
                slots: 0,
                ..capacity
            },
            draining: false,
        },
    );
    assert_eq!(h.view().holds.len(), 1);
    assert_eq!(h.view().holds[0].execution, owners[0]);
    assert_eq!(
        h.view()
            .workers
            .iter()
            .map(|worker| worker.charged.slots)
            .sum::<u64>(),
        6
    );
}

#[test]
fn captured_response_facts_refine_oracle_and_preserve_terminal_and_publication_bytes() {
    for (exit_code, status) in [(0, 0), (7, 0), (0, 13)] {
        let response = ExecuteResponse {
            result: Some(ActionResult {
                exit_code,
                stdout_raw: b"stdout\0with binary".to_vec().into(),
                stderr_raw: b"stderr".to_vec().into(),
                ..Default::default()
            }),
            status: Some(Status {
                code: status,
                message: "exact status message".into(),
                details: Vec::new(),
            }),
            message: "an execution response field outside ActionResult".into(),
            ..Default::default()
        };
        let payload = captured_result(&response);
        let mut h = harness();
        apply(&mut h, register(1, 1, 1, 1));
        submit(&mut h, 1, request(1));
        let attempt = assign(&mut h);
        let before = h.view();
        for field in 0..3 {
            let mut forged = payload.clone();
            match field {
                0 => forged.exit_code += 1,
                1 => forged.error = Some("forged status".into()),
                _ => forged.digest = "0".repeat(64),
            }
            assert_eq!(
                apply(
                    &mut h,
                    Event::Complete {
                        attempt: attempt.id,
                        worker: attempt.worker,
                        incarnation: attempt.incarnation,
                        result: forged
                    }
                ),
                Reply::Rejected(Rejection::Invalid)
            );
            assert_eq!(h.view(), before);
        }
        assert_eq!(
            apply(
                &mut h,
                Event::Complete {
                    attempt: attempt.id,
                    worker: attempt.worker,
                    incarnation: attempt.incarnation,
                    result: payload.clone()
                }
            ),
            Reply::Applied
        );
        let completed = h.view().operations[0].terminal.clone().unwrap();
        assert_eq!(completed, TerminalResult::Completed(payload.clone()));
        let publications: Vec<_> = h
            .view()
            .effects
            .into_iter()
            .filter_map(|effect| match effect.kind {
                EffectKind::PublishResult { result, .. } => Some(result),
                _ => None,
            })
            .collect();
        if exit_code == 0 && status == 0 {
            assert_eq!(publications.as_slice(), std::slice::from_ref(&payload));
        } else {
            assert!(publications.is_empty());
        }
        h.checkpoint().unwrap();
        h.crash_restore().unwrap();
        let Some(TerminalResult::Completed(restored)) = &h.view().operations[0].terminal else {
            panic!("missing restored exact response")
        };
        assert_eq!(
            restored.response.as_ref().unwrap().encoded(),
            response.encode_to_vec()
        );
        assert_eq!(
            ExecuteResponse::decode(restored.response.as_ref().unwrap().encoded()).unwrap(),
            response
        );
    }
}

#[test]
fn lookup_payload_validation_retains_intent_and_oracle_restore_rechecks_all_result_locations() {
    let response = ExecuteResponse {
        result: Some(ActionResult {
            stdout_raw: b"cached response".to_vec().into(),
            ..Default::default()
        }),
        cached_result: true,
        ..Default::default()
    };
    let payload = captured_result(&response);
    let mut h = harness();
    let mut lookup = request(1);
    lookup.skip_cache_lookup = false;
    submit(&mut h, 1, lookup);
    let effect = h.view().effects[0].id;
    let mut forged = payload.clone();
    forged.exit_code = 1;
    assert_eq!(
        apply(
            &mut h,
            Event::LookupResult {
                effect,
                result: Some(forged)
            }
        ),
        Reply::Rejected(Rejection::Invalid)
    );
    assert_eq!(h.view().effects[0].id, effect);
    assert_eq!(
        apply(
            &mut h,
            Event::LookupResult {
                effect,
                result: Some(payload.clone())
            }
        ),
        Reply::Applied
    );
    h.checkpoint().unwrap();
    h.crash_restore().unwrap();

    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    submit(&mut h, 1, request(1));
    let attempt = assign(&mut h);
    apply(
        &mut h,
        Event::Complete {
            attempt: attempt.id,
            worker: attempt.worker,
            incarnation: attempt.incarnation,
            result: payload,
        },
    );
    let bytes = h.oracle.snapshot().unwrap();
    for relation in ["interests", "jobs", "outbox"] {
        let mut snapshot: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let result = if relation == "outbox" {
            &mut snapshot[1][relation][0]["kind"]["PublishResult"]["result"]
        } else {
            &mut snapshot[1][relation][0]["result"]["Completed"]
        };
        assert!(result.is_object());
        result["exit_code"] = serde_json::json!(9);
        assert!(
            super::reference::Reference::restore(&serde_json::to_vec(&snapshot).unwrap()).is_err()
        );
    }
}

#[test]
fn oracle_restore_rejects_duplicate_publication_and_excess_owned_effects() {
    for publication in [true, false] {
        let mut h = harness();
        apply(&mut h, register(1, 1, 1, 1));
        submit(&mut h, 1, request(1));
        let attempt = assign(&mut h);
        if publication {
            finish(&mut h, &attempt, 1);
        } else {
            apply(
                &mut h,
                Event::Cancel {
                    operation: OperationId(1),
                },
            );
        }
        let bytes = h.oracle.snapshot().unwrap();
        super::reference::Reference::restore(&bytes).unwrap();
        let mut snapshot: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let outbox = snapshot[1]["outbox"].as_array_mut().unwrap();
        assert_eq!(outbox.len(), 1);
        let original = outbox[0].clone();
        let original_id = original["id"].as_u64().unwrap();
        let extra = if publication { 1 } else { 8 };
        for offset in 1..=extra {
            let mut duplicate = original.clone();
            duplicate["id"] = serde_json::json!(original_id + offset);
            outbox.push(duplicate);
        }
        snapshot[1]["next_effect"] = serde_json::json!(original_id + extra + 1);
        let error = super::reference::Reference::restore(&serde_json::to_vec(&snapshot).unwrap())
            .expect_err("unreachable effect multiplicity must not restore");
        assert!(error.contains("effect ownership bounds"));
    }
}

fn dispatch(h: &Harness, attempt: AttemptId) -> Effect {
    h.view()
        .effects
        .into_iter()
        .find(|effect| {
            matches!(effect.kind,
        EffectKind::Dispatch { attempt: target, .. } if target == attempt)
        })
        .unwrap()
}

fn cancellation(h: &Harness, attempt: AttemptId) -> Effect {
    h.view()
        .effects
        .into_iter()
        .find(|effect| {
            matches!(effect.kind,
        EffectKind::Cancel { attempt: target, .. } if target == attempt)
        })
        .unwrap()
}

const fn cleanup(attempt: &AttemptView) -> Event {
    Event::Cleanup {
        attempt: attempt.id,
        worker: attempt.worker,
        incarnation: attempt.incarnation,
    }
}

#[test]
fn finite_attempt_budget_finishes_despite_three_uncertain_old_processes() {
    // escalation_test: bounded retry budget and worker loss; no selectable
    // escalation ladder or mutation of the original request is carried over.
    let mut h = harness();
    for worker in 1..=3 {
        apply(&mut h, register(worker, 1, 1, 1));
    }
    let execution = submit(&mut h, 1, request(1));
    let mut attempts = Vec::new();
    for _ in 0..MAX_ATTEMPTS {
        let attempt = assign(&mut h);
        apply(
            &mut h,
            Event::AdvanceTime {
                now: attempt.deadline.at,
            },
        );
        apply(
            &mut h,
            Event::ExpireAttempt {
                attempt: attempt.id,
                generation: attempt.deadline.generation,
            },
        );
        assert!(!h.view().reservations.last().unwrap().released);
        attempts.push(attempt);
    }
    assert_eq!(h.view().executions[0].id, execution);
    assert_eq!(
        h.view().executions[0].terminal,
        Some(TerminalResult::Exhausted)
    );
    assert_eq!(
        h.view().operations[0].terminal,
        Some(TerminalResult::Exhausted)
    );
    assert_eq!(h.view().effects.len(), 3);
    assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
    let stable = h.view().operations[0].clone();
    for attempt in attempts {
        assert_eq!(
            apply(
                &mut h,
                Event::Complete {
                    attempt: attempt.id,
                    worker: attempt.worker,
                    incarnation: attempt.incarnation,
                    result: result(9),
                }
            ),
            Reply::Rejected(Rejection::Stale)
        );
        apply(&mut h, cleanup(&attempt));
    }
    assert!(
        h.view()
            .reservations
            .iter()
            .all(|reservation| reservation.released)
    );
    assert!(h.view().effects.is_empty());
    assert_eq!(h.view().operations[0], stable);
}

#[test]
fn late_decline_after_acceptance_cannot_refund_a_running_attempt() {
    // dispatch_ack_test: a delayed negative response cannot undo acceptance.
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    submit(&mut h, 1, request(1));
    submit(&mut h, 2, request(2));
    let attempt = assign(&mut h);
    apply(
        &mut h,
        Event::WorkerAccepted {
            attempt: attempt.id,
            worker: attempt.worker,
            incarnation: attempt.incarnation,
        },
    );
    apply(
        &mut h,
        Event::Progress {
            attempt: attempt.id,
            worker: attempt.worker,
            incarnation: attempt.incarnation,
            phase: Phase::Running,
            progress: 1,
        },
    );
    let before = h.view();
    assert_eq!(
        apply(
            &mut h,
            Event::Decline {
                attempt: attempt.id,
                worker: attempt.worker,
                incarnation: attempt.incarnation
            }
        ),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(h.view(), before);
    assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
}

#[test]
fn draining_and_live_capacity_require_fresh_nonstale_advertisements() {
    // backpressure_test / live_memory_veto_test / reservation_hold_test:
    // paused placement, final live slot, capacity shrink, and fresh wakeup.
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 4));
    submit(&mut h, 1, request(1));
    let mut zero_cpu = request(2);
    zero_cpu.resources.cpu_millis = 0;
    submit(&mut h, 2, zero_cpu);
    let cap = h.view().workers[0].capacity;
    apply(
        &mut h,
        Event::Capacity {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            capacity: cap,
            available: cap,
            draining: true,
        },
    );
    assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
    apply(
        &mut h,
        Event::Capacity {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            capacity: cap,
            available: cap,
            draining: false,
        },
    );
    let attempt = assign(&mut h);
    let shrunk = Resources {
        cpu_millis: 0,
        ..cap
    };
    apply(
        &mut h,
        Event::Capacity {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            capacity: shrunk,
            available: cap,
            draining: false,
        },
    );
    assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
    assert_eq!(h.view().workers[0].charged.cpu_millis, 100);
    assert_eq!(
        apply(
            &mut h,
            Event::Capacity {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(0),
                capacity: cap,
                available: cap,
                draining: false
            }
        ),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
    apply(
        &mut h,
        Event::Capacity {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            capacity: cap,
            available: cap,
            draining: false,
        },
    );
    let next = assign(&mut h);
    assert_ne!(attempt.id, next.id);
    assert_eq!(h.view().workers[0].charged.slots, 2);
}

#[test]
fn execution_timeout_starts_at_running_and_progress_cannot_extend_it() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    let mut req = request(1);
    req.execution_timeout = 5;
    submit(&mut h, 1, req);
    apply(&mut h, Event::AdvanceTime { now: 50 });
    let attempt = assign(&mut h);
    apply(
        &mut h,
        Event::WorkerAccepted {
            attempt: attempt.id,
            worker: attempt.worker,
            incarnation: attempt.incarnation,
        },
    );
    for (now, progress) in [(60, 1), (80, 2)] {
        apply(&mut h, Event::AdvanceTime { now });
        apply(
            &mut h,
            Event::Progress {
                attempt: attempt.id,
                worker: attempt.worker,
                incarnation: attempt.incarnation,
                phase: Phase::Preparing,
                progress,
            },
        );
    }
    assert!(h.view().attempts[0].authorized);
    assert_eq!(h.view().attempts[0].running_deadline, None);
    apply(
        &mut h,
        Event::Progress {
            attempt: attempt.id,
            worker: attempt.worker,
            incarnation: attempt.incarnation,
            phase: Phase::Running,
            progress: 1,
        },
    );
    let original_generation = h.view().attempts[0].deadline.generation;
    assert_eq!(h.view().attempts[0].running_deadline, Some(85));
    apply(&mut h, Event::AdvanceTime { now: 84 });
    apply(
        &mut h,
        Event::Progress {
            attempt: attempt.id,
            worker: attempt.worker,
            incarnation: attempt.incarnation,
            phase: Phase::Running,
            progress: 2,
        },
    );
    let current_generation = h.view().attempts[0].deadline.generation;
    apply(&mut h, Event::AdvanceTime { now: 85 });
    assert_eq!(
        apply(
            &mut h,
            Event::ExpireAttempt {
                attempt: attempt.id,
                generation: original_generation
            }
        ),
        Reply::Ignored
    );
    assert_eq!(
        apply(
            &mut h,
            Event::ExpireAttempt {
                attempt: attempt.id,
                generation: current_generation
            }
        ),
        Reply::Applied
    );
    assert!(!h.view().attempts[0].authorized);
    assert!(!h.view().reservations[0].released);
}

#[test]
fn renewed_handle_interest_fences_old_timer_and_expires_independently() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    let mut ordinary = request(1);
    ordinary.priority = 10;
    let mut urgent = ordinary.clone();
    urgent.priority = -5;
    submit(&mut h, 1, ordinary);
    submit(&mut h, 2, urgent);
    let attempt = assign(&mut h);
    apply(&mut h, Event::AdvanceTime { now: 200 });
    apply(
        &mut h,
        Event::RenewInterest {
            operation: OperationId(2),
        },
    );
    apply(
        &mut h,
        Event::AdvanceTime {
            now: INTEREST_TIMEOUT,
        },
    );
    assert_eq!(
        apply(
            &mut h,
            Event::ExpireInterest {
                operation: OperationId(2),
                generation: 1
            }
        ),
        Reply::Ignored
    );
    apply(
        &mut h,
        Event::ExpireInterest {
            operation: OperationId(1),
            generation: 1,
        },
    );
    assert_eq!(h.view().executions[0].effective_priority, Some(-5));
    assert!(h.view().attempts[0].authorized);
    let renewed = h.view().operations[1].clone();
    for _ in 0..3 {
        apply(
            &mut h,
            Event::Observe {
                operation: OperationId(2),
            },
        );
    }
    assert_eq!(h.view().operations[1], renewed);
    apply(&mut h, Event::AdvanceTime { now: 500 });
    apply(
        &mut h,
        Event::ExpireInterest {
            operation: OperationId(2),
            generation: 2,
        },
    );
    assert_eq!(
        h.view().executions[0].terminal,
        Some(TerminalResult::Cancelled)
    );
    assert!(!h.view().reservations[0].released);
    apply(&mut h, cleanup(&attempt));
    assert!(h.view().reservations[0].released);
}

#[test]
fn terminal_retirement_waits_for_effects_and_never_reuses_handles() {
    let limits = Limits {
        max_executions: 1,
        max_operations: 1,
        max_effects: 8,
        ..Limits::default()
    };
    let mut h = Harness::new(ClusterEpoch(1), limits);
    apply(&mut h, register(1, 1, 1, 1));
    let execution = submit(&mut h, 1, request(1));
    let attempt = assign(&mut h);
    finish(&mut h, &attempt, 1);
    let terminal = h.view().operations[0].clone();
    assert_eq!(
        apply(&mut h, Event::RetireExecution { execution }),
        Reply::Rejected(Rejection::TooEarly)
    );
    apply(
        &mut h,
        Event::AdvanceTime {
            now: TERMINAL_RETENTION,
        },
    );
    assert_eq!(
        apply(&mut h, Event::RetireExecution { execution }),
        Reply::Rejected(Rejection::Busy)
    );
    let publish = h.view().effects[0].clone();
    assert_eq!(
        apply(
            &mut h,
            Event::EffectOutcome {
                effect: publish.id,
                success: false
            }
        ),
        Reply::Ignored
    );
    assert_eq!(h.view().effects[0], publish);
    assert_eq!(h.view().operations[0], terminal);
    apply(
        &mut h,
        Event::EffectOutcome {
            effect: publish.id,
            success: true,
        },
    );
    apply(&mut h, Event::RetireExecution { execution });
    h.checkpoint().unwrap();
    h.crash_restore().unwrap();
    assert_eq!(
        apply(
            &mut h,
            Event::Submit {
                operation: OperationId(1),
                request: request(2)
            }
        ),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(
        apply(
            &mut h,
            Event::Observe {
                operation: OperationId(1)
            }
        ),
        Reply::Rejected(Rejection::Unknown)
    );
    let next = submit(&mut h, 2, request(2));
    assert!(next.0 > execution.0);
    assert!(assign(&mut h).id.0 > attempt.id.0);
}

#[test]
fn saturated_admission_preserves_completion_and_cancellation_headroom() {
    let limits = Limits {
        max_executions: 2,
        max_operations: 2,
        max_effects: 16,
        ..Limits::default()
    };
    let mut h = Harness::new(ClusterEpoch(1), limits);
    apply(&mut h, register(1, 1, 1, 2));
    submit(&mut h, 1, request(1));
    submit(&mut h, 2, request(2));
    let first = assign(&mut h);
    let second = assign(&mut h);
    let before = h.view();
    assert_eq!(
        apply(
            &mut h,
            Event::Submit {
                operation: OperationId(3),
                request: request(3)
            }
        ),
        Reply::Rejected(Rejection::Overloaded)
    );
    assert_eq!(h.view(), before);
    apply(
        &mut h,
        Event::Cancel {
            operation: OperationId(1),
        },
    );
    finish(&mut h, &second, 2);
    assert_eq!(
        h.view().operations[0].terminal,
        Some(TerminalResult::Cancelled)
    );
    assert_eq!(
        h.view().operations[1].terminal,
        Some(TerminalResult::Completed(result(2)))
    );
    assert_eq!(h.view().effects.len(), 2);
    apply(&mut h, cleanup(&first));
    assert!(
        h.view()
            .reservations
            .iter()
            .all(|reservation| reservation.released)
    );
}

#[test]
fn recipient_closure_precedes_refund_and_rejects_late_dispatch() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    submit(&mut h, 1, request(1));
    let attempt = assign(&mut h);
    let in_flight = dispatch(&h, attempt.id);
    let mut recipient = Recipient::new(ClusterEpoch(1), WorkerId(1), IncarnationId(1));
    // The sender died after sending; the copied network intent arrives only
    // after cancellation has already closed this not-yet-started attempt.
    apply(
        &mut h,
        Event::Cancel {
            operation: OperationId(1),
        },
    );
    assert_eq!(
        recipient.receive(cancellation(&h, attempt.id)),
        Reception::Closed
    );
    apply(&mut h, recipient.confirm_stopped(attempt.id).unwrap());
    assert!(h.view().reservations[0].released);
    assert_eq!(recipient.receive(in_flight.clone()), Reception::Closed);
    assert_eq!(recipient.receive(in_flight), Reception::Closed);
    assert_eq!(recipient.launches(), 0);
}

#[test]
fn recipient_deduplicates_launch_and_late_cancel_cannot_kill_successor() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    submit(&mut h, 1, request(1));
    let first = assign(&mut h);
    let mut recipient = Recipient::new(ClusterEpoch(1), WorkerId(1), IncarnationId(1));
    let old_dispatch = dispatch(&h, first.id);
    assert_eq!(recipient.receive(old_dispatch.clone()), Reception::Started);
    assert_eq!(
        recipient.receive(old_dispatch.clone()),
        Reception::Duplicate
    );
    assert_eq!(recipient.launches(), 1);
    // First acknowledgment is lost. Dispatch retry produces a second ack for
    // the same process rather than launching another process.
    apply(&mut h, recipient.acknowledge(first.id).unwrap());
    let deadline = h.view().attempts[0].deadline;
    apply(&mut h, Event::AdvanceTime { now: deadline.at });
    apply(
        &mut h,
        Event::ExpireAttempt {
            attempt: first.id,
            generation: deadline.generation,
        },
    );
    let old_cancel = cancellation(&h, first.id);
    assert_eq!(
        recipient.receive(old_cancel.clone()),
        Reception::Terminating
    );
    assert!(recipient.is_running(first.id));
    assert!(!h.view().reservations[0].released);
    assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
    apply(&mut h, recipient.confirm_stopped(first.id).unwrap());
    let next = assign(&mut h);
    assert_eq!(recipient.receive(dispatch(&h, next.id)), Reception::Started);
    assert_eq!(recipient.receive(old_cancel), Reception::Closed);
    assert_eq!(recipient.receive(old_dispatch), Reception::Closed);
    assert!(recipient.is_running(next.id));
    assert!(!recipient.is_running(first.id));
    assert_eq!(recipient.launches(), 2);
    assert_eq!(h.view().workers[0].charged.slots, 1);
}

#[test]
fn confirmed_decline_pauses_until_fresh_capacity_without_spinning_retries() {
    // dispatch_ack_test::a_decline_requeues_untried_and_pauses_until_the_load_fits
    // and backpressure_test::backpressure_pauses_an_idle_worker_until_its_next_keepalive:
    // new contract requires explicit capacity evidence; a heartbeat alone is not it.
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    submit(&mut h, 1, request(1));
    let first = assign(&mut h);
    let capacity = h.view().workers[0].capacity;
    assert_eq!(
        apply(
            &mut h,
            Event::Decline {
                attempt: first.id,
                worker: first.worker,
                incarnation: first.incarnation,
            }
        ),
        Reply::Applied
    );
    assert!(h.view().reservations[0].released);
    assert_eq!(h.view().workers[0].available, Resources::default());
    for _ in 0..5 {
        assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
    }
    apply(
        &mut h,
        Event::Heartbeat {
            worker: first.worker,
            incarnation: first.incarnation,
            session: SessionId(1),
        },
    );
    assert_eq!(apply(&mut h, Event::Match), Reply::Ignored);
    assert_eq!(h.view().executions[0].attempts_started, 1);
    apply(
        &mut h,
        Event::Capacity {
            worker: first.worker,
            incarnation: first.incarnation,
            session: SessionId(1),
            capacity,
            available: capacity,
            draining: false,
        },
    );
    let retry = assign(&mut h);
    assert_ne!(retry.id, first.id);
    assert_eq!(h.view().workers[0].charged.slots, 1);
}

#[test]
fn nonrenewable_execution_timer_survives_continuous_progress_and_old_stall_timers() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    apply(&mut h, register(2, 1, 1, 1));
    let mut req = request(1);
    req.execution_timeout = 5;
    submit(&mut h, 1, req);
    let first = assign(&mut h);
    apply(
        &mut h,
        Event::WorkerAccepted {
            attempt: first.id,
            worker: first.worker,
            incarnation: first.incarnation,
        },
    );
    apply(
        &mut h,
        Event::Progress {
            attempt: first.id,
            worker: first.worker,
            incarnation: first.incarnation,
            phase: Phase::Running,
            progress: 0,
        },
    );
    assert_eq!(
        apply(&mut h, Event::ExpireExecution { attempt: first.id }),
        Reply::Rejected(Rejection::TooEarly)
    );
    for now in 1..=6 {
        let old_generation = h.view().attempts[0].deadline.generation;
        apply(&mut h, Event::AdvanceTime { now });
        apply(
            &mut h,
            Event::Progress {
                attempt: first.id,
                worker: first.worker,
                incarnation: first.incarnation,
                phase: Phase::Running,
                progress: now,
            },
        );
        assert_eq!(
            apply(
                &mut h,
                Event::ExpireAttempt {
                    attempt: first.id,
                    generation: old_generation
                }
            ),
            Reply::Ignored
        );
    }
    assert_eq!(h.view().attempts[0].running_deadline, Some(5));
    assert_eq!(
        apply(&mut h, Event::ExpireExecution { attempt: first.id }),
        Reply::Applied
    );
    assert!(!h.view().attempts[0].authorized);
    assert!(!h.view().reservations[0].released);
    let replacement = assign(&mut h);
    assert_eq!(replacement.worker, WorkerId(2));
    assert_eq!(
        apply(&mut h, Event::ExpireExecution { attempt: first.id }),
        Reply::Ignored
    );
    assert!(h.view().attempts[1].authorized);
}
