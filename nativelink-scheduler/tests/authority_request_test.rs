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

//! Candidate request capture and strict fixed mapping; no production adapter.
use std::collections::BTreeMap;

use nativelink_config::cas_server::CpuUnit;
use nativelink_config::schedulers::PropertyType;
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, Digest, ExecuteRequest, ExecutionPolicy, Platform, RequestMetadata,
    ResultsCachePolicy, ToolDetails,
};
use nativelink_scheduler::authority::request::{
    MAX_REQUEST_BYTES, RequestPayload, legacy_property_blockers, normalize_platform,
};
use nativelink_scheduler::authority::{
    ClusterEpoch, Event, Limits, OperationId, RecordedEvent, Rejection, Reply, Request, State,
};
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use prost::Message;
use prost_types::{Duration, Timestamp};

fn platform(properties: &[(&str, &str)]) -> Platform {
    Platform {
        properties: properties
            .iter()
            .map(|(name, value)| Property {
                name: (*name).to_owned(),
                value: (*value).to_owned(),
            })
            .collect(),
    }
}
fn digest(bytes: &[u8], function: DigestHasherFunc) -> Digest {
    let mut hash = function.hasher();
    hash.update(bytes);
    hash.finalize_digest().into()
}
struct Fixture {
    execute: ExecuteRequest,
    action: Action,
    command: Command,
    metadata: RequestMetadata,
    queued: Timestamp,
}
impl Fixture {
    fn new() -> Self {
        Self {
            execute: ExecuteRequest {
                instance_name: "tenant/instance".to_owned(),
                skip_cache_lookup: true,
                digest_function: 1,
                execution_policy: Some(ExecutionPolicy { priority: -7 }),
                results_cache_policy: Some(ResultsCachePolicy { priority: 31 }),
                ..ExecuteRequest::default()
            },
            action: Action {
                input_root_digest: Some(digest(&[], DigestHasherFunc::Sha256)),
                timeout: Some(Duration {
                    seconds: 17,
                    nanos: 1,
                }),
                ..Action::default()
            },
            command: Command {
                arguments: vec!["/bin/true".to_owned()],
                platform: Some(platform(&[
                    ("cpu_count", "2"),
                    ("memory_kb", "3"),
                    ("OSFamily", "linux"),
                ])),
                ..Command::default()
            },
            metadata: RequestMetadata {
                tool_details: Some(ToolDetails {
                    tool_name: "BuCk2".to_owned(),
                    tool_version: "v-test".to_owned(),
                }),
                action_id: "action-17".to_owned(),
                tool_invocation_id: "inv-1".to_owned(),
                correlated_invocations_id: "correlated".to_owned(),
                action_mnemonic: "Compile".to_owned(),
                target_id: "//target".to_owned(),
                configuration_id: "configuration".to_owned(),
            },
            queued: Timestamp {
                seconds: 1_700_000_000,
                nanos: 123_456_789,
            },
        }
    }
    fn bytes(&mut self) -> (Vec<u8>, Vec<u8>) {
        let function = if self.execute.digest_function == 9 {
            DigestHasherFunc::Blake3
        } else {
            DigestHasherFunc::Sha256
        };
        let command = self.command.encode_to_vec();
        self.action.command_digest = Some(digest(&command, function));
        let action = self.action.encode_to_vec();
        self.execute.action_digest = Some(digest(&action, function));
        (action, command)
    }
    fn capture(&mut self) -> Result<RequestPayload, String> {
        let (action, command) = self.bytes();
        RequestPayload::capture(
            &self.execute,
            Some(&self.metadata),
            &self.queued,
            "principal",
            &action,
            &command,
            MAX_REQUEST_BYTES,
        )
    }
}
const fn record(event: Event) -> RecordedEvent {
    RecordedEvent {
        epoch: ClusterEpoch(1),
        event,
    }
}

#[test]
fn complete_rpc_facts_raw_cas_and_first_queue_time_roundtrip() {
    let mut fixture = Fixture::new();
    let payload = fixture.capture().unwrap();
    let bytes = serde_json::to_vec(&payload).unwrap();
    let restored: RequestPayload = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(restored, payload);
    let facts = restored.dispatch_facts().unwrap();
    assert_eq!(facts.execute_request, fixture.execute);
    assert_eq!(facts.request_metadata, Some(fixture.metadata));
    assert_eq!(facts.queued_timestamp, fixture.queued);
    assert_eq!(&facts.platform, fixture.command.platform.as_ref().unwrap());
    assert_eq!(restored.action_bytes(), fixture.action.encode_to_vec());
    assert_eq!(restored.command_bytes(), fixture.command.encode_to_vec());
    let projection = restored.project().unwrap();
    assert_eq!(
        (
            projection.resources.cpu_millis,
            projection.resources.memory_bytes,
            projection.resources.slots
        ),
        (2000, 3072, 1)
    );
    assert_eq!(
        projection.properties,
        BTreeMap::from([("OSFamily".to_owned(), "linux".to_owned())])
    );
    assert_eq!(projection.execution_timeout, 18);
    assert_eq!(projection.priority, -7);
}

#[test]
fn original_noncanonical_cas_encoding_is_hashed_without_reencoding() {
    let mut fixture = Fixture::new();
    let (_, mut command) = fixture.bytes();
    // Unknown field 100, varint 1. Prost drops it when decoded; raw CAS must keep it.
    command.extend_from_slice(&[0xa0, 0x06, 0x01]);
    fixture.action.command_digest = Some(digest(&command, DigestHasherFunc::Sha256));
    let mut action = fixture.action.encode_to_vec();
    action.extend_from_slice(&[0xa0, 0x06, 0x02]);
    fixture.execute.action_digest = Some(digest(&action, DigestHasherFunc::Sha256));
    let payload = RequestPayload::capture(
        &fixture.execute,
        None,
        &fixture.queued,
        "principal",
        &action,
        &command,
        MAX_REQUEST_BYTES,
    )
    .unwrap();
    assert_eq!(payload.action_bytes(), action);
    assert_eq!(payload.command_bytes(), command);
    assert_ne!(
        Action::decode(action.as_slice()).unwrap().encode_to_vec(),
        action
    );
    assert_ne!(
        Command::decode(command.as_slice()).unwrap().encode_to_vec(),
        command
    );
    action.push(0);
    assert!(
        RequestPayload::capture(
            &fixture.execute,
            None,
            &fixture.queued,
            "principal",
            &action,
            &command,
            MAX_REQUEST_BYTES
        )
        .is_err()
    );
}

#[test]
fn malformed_cas_protobuf_and_missing_references_fail_after_digest_verification() {
    let mut fixture = Fixture::new();
    let (_, command) = fixture.bytes();
    let malformed = [0xff];
    fixture.execute.action_digest = Some(digest(&malformed, DigestHasherFunc::Sha256));
    assert!(
        RequestPayload::capture(
            &fixture.execute,
            None,
            &fixture.queued,
            "",
            &malformed,
            &command,
            MAX_REQUEST_BYTES
        )
        .is_err()
    );
    fixture.action.command_digest = Some(digest(&malformed, DigestHasherFunc::Sha256));
    let action = fixture.action.encode_to_vec();
    fixture.execute.action_digest = Some(digest(&action, DigestHasherFunc::Sha256));
    assert!(
        RequestPayload::capture(
            &fixture.execute,
            None,
            &fixture.queued,
            "",
            &action,
            &malformed,
            MAX_REQUEST_BYTES
        )
        .is_err()
    );
    fixture.action.input_root_digest = None;
    assert!(fixture.capture().is_err());
    fixture.action.input_root_digest = Some(Digest {
        hash: "short".to_owned(),
        size_bytes: 0,
    });
    assert!(fixture.capture().is_err());
}

#[test]
fn action_platform_is_whole_list_winner_and_empty_action_falls_back() {
    let mut fixture = Fixture::new();
    fixture.action.platform = Some(platform(&[("cpu_count", "1")]));
    let projected = fixture.capture().unwrap().project().unwrap();
    assert_eq!(projected.resources.cpu_millis, 1000);
    assert_eq!(projected.resources.memory_bytes, 0);
    assert!(projected.properties.is_empty());
    fixture.command.platform = Some(platform(&[
        ("memory_kb", "bad"),
        ("OSFamily", "linux"),
        ("OSFamily", "linux"),
    ]));
    assert!(fixture.capture().is_ok());
    fixture.action.platform = Some(Platform::default());
    assert!(fixture.capture().is_err());
    fixture.command.platform = Some(platform(&[("memory_kb", "5")]));
    assert_eq!(
        fixture
            .capture()
            .unwrap()
            .project()
            .unwrap()
            .resources
            .memory_bytes,
        5120
    );
    fixture.action.platform = Some(platform(&[("cpu_count", "bad")]));
    assert!(fixture.capture().is_err());
}

#[test]
fn quantities_are_canonical_checked_and_always_charge_one_slot() {
    for name in ["cpu_count", "memory_kb"] {
        for invalid in [
            "",
            " 1",
            "1 ",
            "+1",
            "-1",
            "1.0",
            "1KiB",
            "00",
            "01",
            "١",
            "18446744073709551616",
        ] {
            assert!(
                normalize_platform(&platform(&[(name, invalid)])).is_err(),
                "{name}={invalid}"
            );
        }
        let scale = if name == "cpu_count" { 1000 } else { 1024 };
        let maximum = (u64::MAX / scale).to_string();
        assert!(normalize_platform(&platform(&[(name, &maximum)])).is_ok());
        let overflow = (u64::MAX / scale + 1).to_string();
        assert!(normalize_platform(&platform(&[(name, &overflow)])).is_err());
    }
    for p in [
        Platform::default(),
        platform(&[("cpu_count", "0"), ("memory_kb", "0")]),
    ] {
        let (_, r) = normalize_platform(&p).unwrap();
        assert_eq!((r.cpu_millis, r.memory_bytes, r.slots), (0, 0, 1));
    }
    for values in [
        [("cpu_count", "1"), ("cpu_count", "1")],
        [("custom", "a"), ("custom", "b")],
    ] {
        assert!(normalize_platform(&platform(&values)).is_err());
    }
    for reserved in [
        "cpu_millis",
        "cpu_millicores",
        "memory_bytes",
        "memory_mib",
        "memory_mb",
        "slots",
        "disk_kb",
        "network_kbps",
        "disk_read_iops",
        "disk_write_iops",
        "disk_read_bps",
        "disk_write_bps",
        "shm_size",
        "gpu_count",
    ] {
        assert!(
            normalize_platform(&platform(&[(reserved, "1")])).is_err(),
            "{reserved}"
        );
    }
    let (exact, _) =
        normalize_platform(&platform(&[("ordinary2", "  arbitrary value  ")])).unwrap();
    assert_eq!(exact["ordinary2"], "  arbitrary value  ");
}

#[test]
fn fixed_algorithms_digest_size_hash_and_timeout_validation() {
    for algorithm in [0, 1, 9] {
        let mut fixture = Fixture::new();
        fixture.execute.digest_function = algorithm;
        assert!(fixture.capture().is_ok());
    }
    let mut fixture = Fixture::new();
    fixture.execute.digest_function = 4;
    assert!(fixture.capture().is_err());
    fixture.execute.digest_function = 1;
    let (action, command) = fixture.bytes();
    for broken in [
        Digest {
            hash: "A".repeat(64),
            size_bytes: 1,
        },
        Digest {
            hash: "0".repeat(64),
            size_bytes: -1,
        },
        Digest {
            hash: "0".repeat(63),
            size_bytes: 1,
        },
        Digest {
            hash: fixture.execute.action_digest.as_ref().unwrap().hash.clone(),
            size_bytes: 1,
        },
    ] {
        fixture.execute.action_digest = Some(broken);
        assert!(
            RequestPayload::capture(
                &fixture.execute,
                None,
                &fixture.queued,
                "",
                &action,
                &command,
                MAX_REQUEST_BYTES
            )
            .is_err()
        );
    }
    fixture.action.timeout = None;
    assert_eq!(
        fixture
            .capture()
            .unwrap()
            .project()
            .unwrap()
            .execution_timeout,
        u64::MAX
    );
    fixture.action.timeout = Some(Duration {
        seconds: 0,
        nanos: 0,
    });
    assert_eq!(
        fixture
            .capture()
            .unwrap()
            .project()
            .unwrap()
            .execution_timeout,
        1
    );
    for timeout in [
        Duration {
            seconds: -1,
            nanos: 0,
        },
        Duration {
            seconds: 0,
            nanos: -1,
        },
        Duration {
            seconds: 1,
            nanos: 1_000_000_000,
        },
        Duration {
            seconds: 315_576_000_001,
            nanos: 0,
        },
    ] {
        fixture.action.timeout = Some(timeout);
        assert!(fixture.capture().is_err());
    }
}

#[test]
fn captured_timeout_boundaries_survive_restore_and_reject_changed_projection() {
    fn replace_timeout(value: &mut serde_json::Value, replacement: u64) -> usize {
        match value {
            serde_json::Value::Object(map) => {
                let changed = usize::from(
                    map.contains_key("payload") && map.contains_key("execution_timeout"),
                );
                if changed != 0 {
                    map.insert("execution_timeout".into(), replacement.into());
                }
                changed
                    + map
                        .values_mut()
                        .map(|value| replace_timeout(value, replacement))
                        .sum::<usize>()
            }
            serde_json::Value::Array(values) => values
                .iter_mut()
                .map(|value| replace_timeout(value, replacement))
                .sum(),
            _ => 0,
        }
    }

    for algorithm in [1, 9] {
        for (timeout, expected) in [
            (None, u64::MAX),
            (
                Some(Duration {
                    seconds: 0,
                    nanos: 0,
                }),
                1,
            ),
            (
                Some(Duration {
                    seconds: 0,
                    nanos: 1,
                }),
                1,
            ),
            (
                Some(Duration {
                    seconds: 1,
                    nanos: 123_456_789,
                }),
                2,
            ),
            (
                Some(Duration {
                    seconds: 315_576_000_000,
                    nanos: 0,
                }),
                315_576_000_000,
            ),
            (
                Some(Duration {
                    seconds: 315_576_000_000,
                    nanos: 999_999_999,
                }),
                315_576_000_001,
            ),
        ] {
            let mut fixture = Fixture::new();
            fixture.execute.digest_function = algorithm;
            fixture.action.timeout = timeout;
            let payload = fixture.capture().unwrap();
            let request = Request::from_payload(payload.clone()).unwrap();
            assert_eq!(request.execution_timeout, expected);
            let wrong = if expected == u64::MAX {
                315_576_000_001
            } else {
                expected + 1
            };
            let mut state = State::new(ClusterEpoch(1), Limits::default()).unwrap();
            let mut invalid = request.clone();
            invalid.execution_timeout = wrong;
            assert_eq!(
                state.apply(record(Event::Submit {
                    operation: OperationId(1),
                    request: invalid
                })),
                Reply::Rejected(Rejection::Invalid)
            );
            assert!(matches!(
                state.apply(record(Event::Submit {
                    operation: OperationId(1),
                    request: request.clone()
                })),
                Reply::Submitted { .. }
            ));
            let snapshot = state.snapshot().unwrap();
            let restored = State::restore(&snapshot).unwrap();
            let view = restored.view();
            assert_eq!(view.executions[0].request, request);
            assert_eq!(
                view.executions[0].request.require_payload().unwrap(),
                &payload
            );
            assert_eq!(
                Action::decode(
                    view.executions[0]
                        .request
                        .require_payload()
                        .unwrap()
                        .action_bytes()
                )
                .unwrap()
                .timeout,
                timeout
            );
            let mut forged: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
            // Change every duplicated projection consistently while retaining
            // the captured Action. Restore must rederive the original timeout.
            assert!(replace_timeout(&mut forged, wrong) >= 2);
            assert!(State::restore(&serde_json::to_vec(&forged).unwrap()).is_err());
        }
    }
}

#[test]
fn buck2_scope_is_principal_length_delimited_and_metadata_presence_is_preserved() {
    let mut fixture = Fixture::new();
    let payload = fixture.capture().unwrap();
    let mut hash = DigestHasherFunc::Sha256.hasher();
    hash.update(&9u64.to_be_bytes());
    hash.update(b"principal");
    hash.update(b"inv-1");
    assert_eq!(
        payload.project().unwrap().execution_scope,
        Some(hash.finalize_digest().packed_hash().to_string())
    );
    let (action, command) = fixture.bytes();
    let other = RequestPayload::capture(
        &fixture.execute,
        Some(&fixture.metadata),
        &fixture.queued,
        "other",
        &action,
        &command,
        MAX_REQUEST_BYTES,
    )
    .unwrap();
    assert_ne!(
        payload.project().unwrap().execution_scope,
        other.project().unwrap().execution_scope
    );
    fixture.metadata.tool_details.as_mut().unwrap().tool_name = "bazel".to_owned();
    assert_eq!(
        fixture
            .capture()
            .unwrap()
            .project()
            .unwrap()
            .execution_scope,
        None
    );
    fixture.metadata.tool_details.as_mut().unwrap().tool_name = "buck2".to_owned();
    fixture.metadata.tool_invocation_id.clear();
    assert_eq!(
        fixture
            .capture()
            .unwrap()
            .project()
            .unwrap()
            .execution_scope,
        None
    );
    fixture.execute.execution_policy = None;
    fixture.execute.results_cache_policy = None;
    let (action, command) = fixture.bytes();
    let absent = RequestPayload::capture(
        &fixture.execute,
        None,
        &fixture.queued,
        "",
        &action,
        &command,
        MAX_REQUEST_BYTES,
    )
    .unwrap()
    .dispatch_facts()
    .unwrap();
    assert!(absent.request_metadata.is_none());
    assert!(absent.execute_request.execution_policy.is_none());
    assert!(absent.execute_request.results_cache_policy.is_none());
    let present = RequestPayload::capture(
        &fixture.execute,
        Some(&RequestMetadata::default()),
        &fixture.queued,
        "",
        &action,
        &command,
        MAX_REQUEST_BYTES,
    )
    .unwrap()
    .dispatch_facts()
    .unwrap();
    assert_eq!(present.request_metadata, Some(RequestMetadata::default()));
}

#[test]
fn timestamp_bounds_and_capture_corruption_or_oversize_are_rejected() {
    let mut fixture = Fixture::new();
    let payload = fixture.capture().unwrap();
    let (action, command) = fixture.bytes();
    assert!(
        RequestPayload::capture(
            &fixture.execute,
            None,
            &fixture.queued,
            "",
            &action,
            &command,
            1
        )
        .is_err()
    );
    assert!(
        RequestPayload::capture(
            &fixture.execute,
            None,
            &fixture.queued,
            &"x".repeat(MAX_REQUEST_BYTES),
            &action,
            &command,
            usize::MAX
        )
        .is_err()
    );
    for invalid in [
        Timestamp {
            seconds: -62_135_596_801,
            nanos: 0,
        },
        Timestamp {
            seconds: 253_402_300_800,
            nanos: 0,
        },
        Timestamp {
            seconds: 0,
            nanos: -1,
        },
        Timestamp {
            seconds: 0,
            nanos: 1_000_000_000,
        },
    ] {
        fixture.queued = invalid;
        assert!(fixture.capture().is_err());
    }
    for field in [
        "version",
        "execute",
        "metadata",
        "queued",
        "principal",
        "action",
        "command",
        "sha256",
    ] {
        let mut wire = serde_json::to_value(&payload).unwrap();
        if field == "version" {
            wire[field] = 2.into();
        } else if field == "principal" {
            wire[field] = "changed".into();
        } else {
            wire[field] = serde_json::json!([255]);
        }
        assert!(
            serde_json::from_value::<RequestPayload>(wire).is_err(),
            "{field}"
        );
    }
    let mut wire = serde_json::to_value(&payload).unwrap();
    wire["action"] = serde_json::json!(vec![0; MAX_REQUEST_BYTES + 1]);
    assert!(serde_json::from_value::<RequestPayload>(wire).is_err());
}

fn corrupt(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            if map.contains_key("payload") && map.contains_key("execution_timeout") {
                map.insert("priority".to_owned(), 42.into());
            }
            for value in map.values_mut() {
                corrupt(value);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                corrupt(value);
            }
        }
        _ => {}
    }
}

#[test]
fn normalized_projection_cannot_be_forged_on_apply_or_restore() {
    let request = Request::from_payload(Fixture::new().capture().unwrap()).unwrap();
    assert!(request.require_payload().is_ok());
    let mut state = State::new(ClusterEpoch(1), Limits::default()).unwrap();
    let mut invalid = request.clone();
    invalid.resources.cpu_millis += 1;
    assert_eq!(
        state.apply(record(Event::Submit {
            operation: OperationId(1),
            request: invalid
        })),
        Reply::Rejected(Rejection::Invalid)
    );
    assert!(matches!(
        state.apply(record(Event::Submit {
            operation: OperationId(1),
            request
        })),
        Reply::Submitted { .. }
    ));
    let mut wire: serde_json::Value = serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
    // Corrupt every projection copy consistently; payload remains authentic.
    corrupt(&mut wire);
    assert!(State::restore(&serde_json::to_vec(&wire).unwrap()).is_err());
}

#[test]
fn coalescing_keeps_first_dispatch_metadata_and_each_operation_capture() {
    let mut fixture = Fixture::new();
    let first = Request::from_payload(fixture.capture().unwrap()).unwrap();
    fixture.metadata.action_id = "second-action-metadata".to_owned();
    fixture.queued.seconds += 1;
    fixture.execute.execution_policy = Some(ExecutionPolicy { priority: -19 });
    fixture.execute.results_cache_policy = Some(ResultsCachePolicy { priority: 42 });
    let second = Request::from_payload(fixture.capture().unwrap()).unwrap();
    let mut state = State::new(ClusterEpoch(1), Limits::default()).unwrap();
    let first_reply = state.apply(record(Event::Submit {
        operation: OperationId(1),
        request: first.clone(),
    }));
    let second_reply = state.apply(record(Event::Submit {
        operation: OperationId(2),
        request: second.clone(),
    }));
    let Reply::Submitted { execution, .. } = first_reply else {
        panic!("first admission failed")
    };
    assert_eq!(
        second_reply,
        Reply::Submitted {
            operation: OperationId(2),
            execution
        }
    );
    assert_eq!(state.view().executions[0].request, first);
    assert_eq!(state.view().executions[0].effective_priority, Some(-19));
    assert_eq!(
        state.apply(record(Event::Submit {
            operation: OperationId(2),
            request: second.clone()
        })),
        second_reply
    );
    // A retry recapturing a new queued timestamp is a different request, not silently accepted.
    fixture.queued.seconds += 1;
    let recaptured = Request::from_payload(fixture.capture().unwrap()).unwrap();
    assert_eq!(
        state.apply(record(Event::Submit {
            operation: OperationId(2),
            request: recaptured
        })),
        Reply::Rejected(Rejection::Invalid)
    );
    let restored = State::restore(&state.snapshot().unwrap()).unwrap();
    assert_eq!(restored.view(), state.view());
    let wire: serde_json::Value = serde_json::from_slice(&restored.snapshot().unwrap()).unwrap();
    assert_eq!(
        serde_json::from_value::<Request>(
            wire["state"]["operations"]["2"]["original_request"].clone()
        )
        .unwrap(),
        second
    );
}

#[test]
fn legacy_rules_have_explicit_dimension_and_unit_blockers() {
    let accepted = BTreeMap::from([
        ("cpu_count".to_owned(), PropertyType::Minimum),
        ("memory_kb".to_owned(), PropertyType::Minimum),
        ("cpu_arch".to_owned(), PropertyType::Exact),
    ]);
    assert!(legacy_property_blockers(&accepted, CpuUnit::Cores).is_empty());
    assert_eq!(
        legacy_property_blockers(&accepted, CpuUnit::Millicores).len(),
        1
    );
    for (name, rule) in [
        ("disk_kb", PropertyType::Minimum),
        ("cpu_arch", PropertyType::Minimum),
        ("cpu_count", PropertyType::Priority),
        ("OSFamily", PropertyType::Priority),
        ("InputRootAbsolutePath", PropertyType::Ignore),
        ("memory_kb", PropertyType::Exact),
        ("memory_bytes", PropertyType::Exact),
    ] {
        let blockers =
            legacy_property_blockers(&BTreeMap::from([(name.to_owned(), rule)]), CpuUnit::Cores);
        assert_eq!(blockers.len(), 1);
        assert!(blockers[0].starts_with(name));
    }
}
