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

//! Actual captured REAPI requests, with explicit trusted caller context.

use std::fmt::Write as _;

use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, Digest, ExecuteRequest, ExecutionPolicy, Platform, RequestMetadata,
    ToolDetails,
};
use nativelink_scheduler::authority::request::RequestPayload;
use prost_types::{Duration, Timestamp};

use super::*;

fn context() -> TrustedContext {
    TrustedContext {
        principal: "trusted-client".into(),
        execution_config: "fixed-config-v1".into(),
    }
}
fn digest(algorithm: u8, bytes: &[u8]) -> Digest {
    let object = Object::for_bytes(algorithm, bytes).unwrap();
    let hash = object
        .hash
        .iter()
        .fold(String::with_capacity(64), |mut hash, byte| {
            write!(&mut hash, "{byte:02x}").unwrap();
            hash
        });
    Digest {
        hash,
        size_bytes: i64::try_from(object.bytes).unwrap(),
    }
}
fn platform() -> Platform {
    Platform {
        properties: [
            ("OSFamily", "linux"),
            ("cpu_count", "2"),
            ("memory_kb", "3"),
        ]
        .into_iter()
        .map(|(name, value)| Property {
            name: name.into(),
            value: value.into(),
        })
        .collect(),
    }
}
fn request(
    algorithm: u8,
    skip: bool,
    no_cache: bool,
    invocation: &str,
    principal: &str,
    action_platform: bool,
) -> a::Request {
    let command = Command {
        arguments: vec!["/bin/true".into()],
        platform: Some(if action_platform {
            Platform {
                properties: vec![Property {
                    name: "ignored".into(),
                    value: "command fallback".into(),
                }],
            }
        } else {
            platform()
        }),
        ..Command::default()
    }
    .encode_to_vec();
    let mut action = Action {
        command_digest: Some(digest(algorithm, &command)),
        input_root_digest: Some(digest(algorithm, &[])),
        timeout: Some(Duration {
            seconds: 17,
            nanos: 1,
        }),
        do_not_cache: no_cache,
        platform: action_platform.then(platform),
        ..Action::default()
    }
    .encode_to_vec();
    // A valid unknown protobuf field must survive capture and dispatch verbatim.
    action.extend_from_slice(&[0xf8, 0x07, 0x01]);
    let execute = ExecuteRequest {
        instance_name: "tenant/instance".into(),
        action_digest: Some(digest(algorithm, &action)),
        digest_function: i32::from(algorithm),
        skip_cache_lookup: skip,
        execution_policy: Some(ExecutionPolicy { priority: -7 }),
        ..ExecuteRequest::default()
    };
    let metadata = RequestMetadata {
        tool_details: Some(ToolDetails {
            tool_name: "buck2".into(),
            tool_version: "fixture".into(),
        }),
        tool_invocation_id: invocation.into(),
        configuration_id: "untrusted-wire-config".into(),
        ..RequestMetadata::default()
    };
    a::Request::from_payload(
        RequestPayload::capture(
            &execute,
            Some(&metadata),
            &Timestamp {
                seconds: 123,
                nanos: 456,
            },
            principal,
            &action,
            &command,
            MAX_PAYLOAD,
        )
        .unwrap(),
    )
    .unwrap()
}
fn real(request: &a::Request) -> PublicationAuthority {
    PublicationAuthority::new_real(
        context(),
        BTreeMap::from([(
            "action".into(),
            ActionBinding::capture(&context(), request).unwrap(),
        )]),
    )
    .unwrap()
}
fn register_real(state: &mut PublicationAuthority) {
    let capacity = a::Resources {
        cpu_millis: 20_000,
        memory_bytes: 32_768,
        slots: 8,
    };
    state
        .scheduler_event(a::Event::Register {
            worker: a::WorkerId(1),
            incarnation: a::IncarnationId(1),
            session: a::SessionId(1),
            properties: BTreeMap::from([("OSFamily".into(), "linux".into())]),
            capacity,
            available: capacity,
        })
        .unwrap();
}
fn real_candidate(
    state: &mut PublicationAuthority,
    algorithm: u8,
    attempt: a::AttemptView,
) -> Candidate {
    let result = ActionResult {
        stdout_raw: b"actual response bytes".to_vec().into(),
        ..ActionResult::default()
    };
    let bytes = result.encode_to_vec();
    let object = Object::for_bytes(algorithm, &bytes).unwrap();
    state
        .install_physical_for(Some(attempt.execution), key(1), object, true)
        .unwrap();
    Candidate {
        action: "action".into(),
        publisher: worker(attempt),
        result: object,
        result_bytes: bytes,
        response: a::ResultPayload::from_response(
            &ExecuteResponse {
                result: Some(result),
                message: "retained execution diagnostic".into(),
                ..ExecuteResponse::default()
            },
            MAX_PAYLOAD,
        )
        .unwrap(),
        closure: BTreeSet::from([Member {
            object,
            key: key(1),
            role: Role::Result,
        }]),
    }
}
fn matrix(algorithm: u8) {
    for skip in [false, true] {
        for no_cache in [false, true] {
            let original = request(
                algorithm,
                skip,
                no_cache,
                "invocation-1",
                &context().principal,
                false,
            );
            let mut state = real(&original);
            register_real(&mut state);
            let execution = state
                .admit_request(1, "action".into(), &context(), original.clone(), 100)
                .unwrap();
            let joined = state
                .admit_request(2, "action".into(), &context(), original.clone(), 100)
                .unwrap();
            assert_eq!(execution == joined, !no_cache);
            let a::Reply::Matched { attempt: id, .. } =
                state.scheduler_event(a::Event::Match).unwrap()
            else {
                panic!("expected actual dispatch");
            };
            let view = state.scheduler_view();
            let attempt = *view.attempts.iter().find(|row| row.id == id).unwrap();
            assert_eq!(attempt.execution, execution);
            let dispatch = view
                .effects
                .iter()
                .find_map(|effect| match &effect.kind {
                    a::EffectKind::Dispatch {
                        request,
                        attempt: id,
                        execution: origin,
                        ..
                    } if *id == attempt.id && *origin == execution => Some(request),
                    _ => None,
                })
                .unwrap();
            assert_eq!(dispatch, &original);
            assert_eq!(
                dispatch
                    .require_payload()
                    .unwrap()
                    .dispatch_facts()
                    .unwrap()
                    .execute_request
                    .digest_function,
                i32::from(algorithm)
            );
            assert!(
                dispatch
                    .require_payload()
                    .unwrap()
                    .action_bytes()
                    .ends_with(&[0xf8, 0x07, 0x01])
            );
            state
                .scheduler_event(a::Event::WorkerAccepted {
                    attempt: attempt.id,
                    worker: attempt.worker,
                    incarnation: attempt.incarnation,
                })
                .unwrap();
            let value = real_candidate(&mut state, algorithm, attempt);
            let exact = value.response.clone();
            stage(&mut state, 1, value);
            state.publish(1, 1, 100).unwrap();
            assert_eq!(
                terminal(&state, 1),
                a::TerminalResult::Completed(exact.clone())
            );
            assert_eq!(state.accepted()[&1].eligible, !no_cache);
            assert_eq!(state.heads().get("action"), (!no_cache).then_some(&1));
            let ordinary = request(
                algorithm,
                false,
                no_cache,
                "invocation-2",
                &context().principal,
                false,
            );
            assert_ne!(ordinary.execution_scope, original.execution_scope);
            assert_eq!(
                ActionBinding::capture(&context(), &ordinary),
                ActionBinding::capture(&context(), &original)
            );
            let later = state
                .admit_request(3, "action".into(), &context(), ordinary, 100)
                .unwrap();
            assert_ne!(later, execution);
            if no_cache {
                assert!(
                    state
                        .scheduler_view()
                        .operations
                        .iter()
                        .find(|row| row.id.0 == 3)
                        .unwrap()
                        .terminal
                        .is_none()
                );
                assert!(state.heads().is_empty());
            } else {
                assert_eq!(
                    terminal(&state, 2),
                    a::TerminalResult::Completed(exact.clone())
                );
                let a::TerminalResult::Completed(cached) = terminal(&state, 3) else {
                    panic!("expected completed-cache result");
                };
                let response = cached.require_response().unwrap().decode().unwrap();
                assert!(response.cached_result);
                assert_eq!(response.message, "");
                assert_eq!(
                    response.result,
                    exact.require_response().unwrap().decode().unwrap().result
                );
                let bypass = request(
                    algorithm,
                    true,
                    false,
                    "invocation-2",
                    &context().principal,
                    false,
                );
                let bypass_execution = state
                    .admit_request(4, "action".into(), &context(), bypass, 100)
                    .unwrap();
                assert_ne!(bypass_execution, later);
                assert!(
                    state
                        .scheduler_view()
                        .operations
                        .iter()
                        .find(|row| row.id.0 == 4)
                        .unwrap()
                        .terminal
                        .is_none()
                );
            }
            let snapshot = state.snapshot().unwrap();
            let restored = PublicationAuthority::restore(&snapshot).unwrap();
            assert_eq!(restored.snapshot().unwrap(), snapshot);
            assert_eq!(restored.scheduler_view(), state.scheduler_view());
            assert_eq!(
                restored
                    .scheduler_view()
                    .executions
                    .iter()
                    .find(|row| row.id == execution)
                    .unwrap()
                    .request,
                original
            );
            eprintln!(
                "REAL_REQUEST_PASS algorithm={algorithm} skip={skip} do_not_cache={no_cache}"
            );
        }
    }
}
#[test]
fn actual_sha256_admission_dispatch_publication_and_request_matrix() {
    matrix(1);
}
#[test]
fn actual_blake3_admission_dispatch_publication_and_request_matrix() {
    matrix(9);
}

#[test]
fn full_binding_selected_platform_and_trusted_context_are_required_atomically() {
    for algorithm in [1, 9] {
        for action_platform in [false, true] {
            let original = request(
                algorithm,
                true,
                false,
                "invocation-1",
                &context().principal,
                action_platform,
            );
            let binding = ActionBinding::capture(&context(), &original).unwrap();
            assert_eq!(binding.action, original.action);
            assert_eq!(binding.context, context());
            assert_eq!(
                binding.platform,
                platform()
                    .properties
                    .into_iter()
                    .map(|property| (property.name, property.value))
                    .collect()
            );
            assert_eq!(binding.platform.len(), 3);
            assert_eq!(original.properties.len(), 1);
            let mut state = real(&original);
            let before = state.snapshot().unwrap();
            for foreign in [
                TrustedContext {
                    principal: "foreign".into(),
                    ..context()
                },
                TrustedContext {
                    execution_config: "foreign".into(),
                    ..context()
                },
            ] {
                assert_eq!(
                    state.admit_request(1, "action".into(), &foreign, original.clone(), 100),
                    Err(Error::Unauthorized)
                );
                assert_eq!(state.snapshot().unwrap(), before);
            }
            let foreign = request(
                algorithm,
                true,
                false,
                "invocation-1",
                "foreign",
                action_platform,
            );
            assert_eq!(
                state.admit_request(1, "action".into(), &context(), foreign, 100),
                Err(Error::Unauthorized)
            );
            let mut mutations = Vec::new();
            let mut changed = original.clone();
            changed.action.instance.push('x');
            mutations.push(changed);
            let mut changed = original.clone();
            changed.action.digest_function = "other".into();
            mutations.push(changed);
            let mut changed = original.clone();
            changed.action.digest = "ff".repeat(32);
            mutations.push(changed);
            let mut changed = original.clone();
            changed.action.digest_size_bytes += 1;
            mutations.push(changed);
            let mut changed = original.clone();
            changed.payload = None;
            mutations.push(changed);
            let mut changed = original.clone();
            changed.do_not_cache = true;
            mutations.push(changed);
            let mut changed = original.clone();
            changed.execution_scope = None;
            mutations.push(changed);
            for changed in mutations {
                assert_eq!(
                    state.admit_request(1, "action".into(), &context(), changed, 100),
                    Err(Error::Unauthorized)
                );
                assert_eq!(state.snapshot().unwrap(), before);
            }
            assert_eq!(state.authorize_client(1), Err(Error::Unauthorized));
            assert_eq!(
                state.admit(1, "action".into(), true, 100),
                Err(Error::Unauthorized)
            );
            assert_eq!(state.snapshot().unwrap(), before);
            assert!(
                PublicationAuthority::new_real(
                    context(),
                    BTreeMap::from([
                        ("action".into(), binding.clone()),
                        ("alias".into(), binding)
                    ])
                )
                .is_err()
            );
            state
                .admit_request(1, "action".into(), &context(), original, 100)
                .unwrap();
        }
    }
}

#[test]
fn real_binding_restore_rejects_relational_corruption() {
    let original = request(9, true, false, "invocation-1", &context().principal, false);
    let mut state = real(&original);
    register_real(&mut state);
    state
        .admit_request(1, "action".into(), &context(), original.clone(), 100)
        .unwrap();
    let attempt = assign(&mut state);
    let value = real_candidate(&mut state, 9, attempt);
    stage(&mut state, 1, value);
    state.publish(1, 1, 100).unwrap();
    let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
    let changes = vec![
        ("\"version\":2,".to_owned(), "\"version\":1,".to_owned()),
        (
            "\"principal\":\"trusted-client\"".into(),
            "\"principal\":\"foreign-client\"".into(),
        ),
        (
            "\"execution_config\":\"fixed-config-v1\"".into(),
            "\"execution_config\":\"foreign-config\"".into(),
        ),
        (
            "\"instance\":\"tenant/instance\"".into(),
            "\"instance\":\"other/instance\"".into(),
        ),
        (
            "\"digest_function\":\"blake3\"".into(),
            "\"digest_function\":\"sha256\"".into(),
        ),
        (
            format!("\"digest\":\"{}\"", original.action.digest),
            format!("\"digest\":\"{}\"", "ff".repeat(32)),
        ),
        (
            format!(
                "\"digest_size_bytes\":{}",
                original.action.digest_size_bytes
            ),
            format!(
                "\"digest_size_bytes\":{}",
                original.action.digest_size_bytes + 1
            ),
        ),
        ("\"cpu_count\":\"2\"".into(), "\"cpu_count\":\"3\"".into()),
        (
            "\"operations\":{\"1\":{\"execution\":1,\"action\":\"action\"".into(),
            "\"operations\":{\"1\":{\"execution\":1,\"action\":\"alias\"".into(),
        ),
    ];
    for (old, new) in changes {
        assert!(encoded.contains(&old), "missing corruption target {old}");
        assert_eq!(
            PublicationAuthority::restore(encoded.replacen(&old, &new, 1).as_bytes()),
            Err(Error::Corrupt),
            "accepted corruption {old}"
        );
    }
    // Change all copies of the trusted principal, leaving the captured request intact.
    assert_eq!(
        PublicationAuthority::restore(
            encoded
                .replace(
                    "\"principal\":\"trusted-client\"",
                    "\"principal\":\"foreign-client\""
                )
                .as_bytes()
        ),
        Err(Error::Corrupt)
    );
}

#[test]
fn stage_requires_the_registered_algorithm_and_actual_execution_binding() {
    let original = request(9, true, false, "invocation-1", &context().principal, false);
    let other = request(9, true, true, "invocation-1", &context().principal, false);
    let mut state = PublicationAuthority::new_real(
        context(),
        BTreeMap::from([
            (
                "action".into(),
                ActionBinding::capture(&context(), &original).unwrap(),
            ),
            (
                "other".into(),
                ActionBinding::capture(&context(), &other).unwrap(),
            ),
        ]),
    )
    .unwrap();
    register_real(&mut state);
    state
        .admit_request(1, "action".into(), &context(), original, 100)
        .unwrap();
    let attempt = assign(&mut state);
    let value = real_candidate(&mut state, 9, attempt);
    let mut wrong = value.clone();
    wrong.action = "other".into();
    let before = state.snapshot().unwrap();
    assert_eq!(
        state.stage(1, wrong, 10, BTreeMap::new()),
        Err(Error::Unauthorized)
    );
    assert_eq!(state.snapshot().unwrap(), before);
    let mut wrong = value.clone();
    wrong.result = Object::for_bytes(1, &wrong.result_bytes).unwrap();
    assert_eq!(
        state.stage(1, wrong, 10, BTreeMap::new()),
        Err(Error::Unauthorized)
    );
    assert_eq!(state.snapshot().unwrap(), before);
    stage(&mut state, 1, value);
    state.publish(1, 1, 100).unwrap();
}

#[test]
fn only_invocation_changes_in_flight_scope_and_wire_config_cannot_select_context() {
    for algorithm in [1, 9] {
        let original = request(
            algorithm,
            true,
            false,
            "invocation-1",
            &context().principal,
            false,
        );
        let payload = original.require_payload().unwrap();
        let mut facts = payload.dispatch_facts().unwrap();
        facts.request_metadata.as_mut().unwrap().configuration_id =
            "different-untrusted-wire-config".into();
        let changed_metadata = a::Request::from_payload(
            RequestPayload::capture(
                &facts.execute_request,
                facts.request_metadata.as_ref(),
                &facts.queued_timestamp,
                &context().principal,
                payload.action_bytes(),
                payload.command_bytes(),
                MAX_PAYLOAD,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            ActionBinding::capture(&context(), &original),
            ActionBinding::capture(&context(), &changed_metadata)
        );
        assert_eq!(original.execution_scope, changed_metadata.execution_scope);
        let mut state = real(&original);
        let first = state
            .admit_request(1, "action".into(), &context(), original.clone(), 100)
            .unwrap();
        let joined = state
            .admit_request(2, "action".into(), &context(), changed_metadata, 100)
            .unwrap();
        assert_eq!(first, joined);
        let other_invocation = request(
            algorithm,
            true,
            false,
            "invocation-2",
            &context().principal,
            false,
        );
        assert_eq!(
            ActionBinding::capture(&context(), &original),
            ActionBinding::capture(&context(), &other_invocation)
        );
        assert_ne!(original.execution_scope, other_invocation.execution_scope);
        let separate = state
            .admit_request(3, "action".into(), &context(), other_invocation, 100)
            .unwrap();
        assert_ne!(separate, first);
        assert_eq!(
            state
                .scheduler_view()
                .executions
                .iter()
                .find(|row| row.id == first)
                .unwrap()
                .request,
            original
        );
    }
}

#[test]
fn restore_validates_each_joined_operations_original_trusted_principal() {
    for algorithm in [1, 9] {
        let captured = request(
            algorithm,
            true,
            false,
            "invocation-1",
            &context().principal,
            false,
        );
        let payload = captured.require_payload().unwrap();
        let mut facts = payload.dispatch_facts().unwrap();
        facts
            .request_metadata
            .as_mut()
            .unwrap()
            .tool_details
            .as_mut()
            .unwrap()
            .tool_name = "bazel".into();
        let recapture = |principal: &str| {
            a::Request::from_payload(
                RequestPayload::capture(
                    &facts.execute_request,
                    facts.request_metadata.as_ref(),
                    &facts.queued_timestamp,
                    principal,
                    payload.action_bytes(),
                    payload.command_bytes(),
                    MAX_PAYLOAD,
                )
                .unwrap(),
            )
            .unwrap()
        };
        let original = recapture(&context().principal);
        assert!(original.execution_scope.is_none());
        let mut state = real(&original);
        let first = state
            .admit_request(1, "action".into(), &context(), original.clone(), 100)
            .unwrap();
        assert_eq!(
            state
                .admit_request(2, "action".into(), &context(), original, 100)
                .unwrap(),
            first
        );
        let snapshot = String::from_utf8(state.snapshot().unwrap()).unwrap();
        let outer: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
        let original_scheduler: Vec<u8> =
            serde_json::from_value(outer["scheduler"].clone()).unwrap();
        let mut scheduler: serde_json::Value = serde_json::from_slice(&original_scheduler).unwrap();
        scheduler["state"]["operations"]["2"]["original_request"] =
            serde_json::to_value(recapture("foreign-client")).unwrap();
        // The core has no external principal context: its compatibility relation
        // accepts this valid non-Buck2 request. The outer binding must reject it.
        let altered = a::State::restore(&serde_json::to_vec(&scheduler).unwrap()).unwrap();
        let altered_scheduler = altered.snapshot().unwrap();
        let original_field = format!(
            "\"scheduler\":{}",
            serde_json::to_string(&original_scheduler).unwrap()
        );
        let altered_field = format!(
            "\"scheduler\":{}",
            serde_json::to_string(&altered_scheduler).unwrap()
        );
        assert!(snapshot.contains(&original_field));
        let corrupt = snapshot.replacen(&original_field, &altered_field, 1);
        assert_eq!(
            PublicationAuthority::restore(corrupt.as_bytes()),
            Err(Error::Corrupt),
            "joined foreign principal restored for algorithm {algorithm}"
        );
    }
}

#[test]
fn restore_cannot_downgrade_captured_requests_into_synthetic_mode() {
    for algorithm in [1, 9] {
        let original = request(
            algorithm,
            true,
            false,
            "invocation-1",
            &context().principal,
            false,
        );
        let label = original.action.instance.clone();
        let mut state = PublicationAuthority::new_real(
            context(),
            BTreeMap::from([(
                label.clone(),
                ActionBinding::capture(&context(), &original).unwrap(),
            )]),
        )
        .unwrap();
        state
            .admit_request(1, label, &context(), original, 100)
            .unwrap();
        let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
        let outer: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        let binding: RealBindings =
            serde_json::from_value(outer["metadata"]["real"].clone()).unwrap();
        let field = format!("\"real\":{},", serde_json::to_string(&binding).unwrap());
        assert!(encoded.contains(&field));
        let downgraded =
            encoded
                .replacen(&field, "", 1)
                .replacen("\"version\":2,", "\"version\":1,", 1);
        assert!(
            matches!(
                PublicationAuthority::restore(downgraded.as_bytes()),
                Err(Error::Corrupt)
            ),
            "real requests restored into synthetic mode for algorithm {algorithm}"
        );
    }
}

#[path = "direct_client_tests.rs"]
mod direct_client;

#[path = "terminal_tests.rs"]
mod terminal_tests;
