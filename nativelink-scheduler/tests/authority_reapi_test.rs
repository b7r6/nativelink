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

//! Exact known-protobuf candidate retention; no production RPC wiring.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest, ExecuteResponse, ExecutedActionMetadata, LogFile, NodeProperties,
    NodeProperty, OutputDirectory, OutputFile, OutputSymlink,
};
use nativelink_proto::google::rpc::Status;
use nativelink_scheduler::authority::journal::Singleton;
use nativelink_scheduler::authority::reapi::{MAX_RESPONSE_BYTES, ResponsePayload};
use nativelink_scheduler::authority::{
    ActionKey, AttemptId, ClusterEpoch, EffectKind, Event, IncarnationId, Limits, OperationId,
    RecordedEvent, Rejection, Reply, Request, Resources, ResultPayload, SessionId, State,
    TerminalResult, WorkerId,
};
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use prost::Message;

fn digest(value: u8) -> Digest {
    Digest {
        hash: format!("{value:064x}"),
        size_bytes: 17,
    }
}

// Timestamp/Any types are exposed through generated fields; this fixture
// needs no additional dependency just to name their default constructors.
#[allow(clippy::default_trait_access)]
fn rich_response(status_code: i32, exit_code: i32) -> ExecuteResponse {
    let mut status = Status {
        code: status_code,
        message: "execution diagnostic with details".to_owned(),
        details: vec![Default::default()],
    };
    "type.googleapis.com/test.FailureDetail".clone_into(&mut status.details[0].type_url);
    status.details[0].value = vec![0, 255, 1, 128];
    let mut metadata = ExecutedActionMetadata {
        worker: "incarnation-43".to_owned(),
        queued_timestamp: Some(Default::default()),
        worker_start_timestamp: Some(Default::default()),
        worker_completed_timestamp: Some(Default::default()),
        input_fetch_start_timestamp: Some(Default::default()),
        input_fetch_completed_timestamp: Some(Default::default()),
        execution_start_timestamp: Some(Default::default()),
        execution_completed_timestamp: Some(Default::default()),
        output_upload_start_timestamp: Some(Default::default()),
        output_upload_completed_timestamp: Some(Default::default()),
        virtual_execution_duration: Some(Default::default()),
        auxiliary_metadata: vec![Default::default()],
    };
    metadata.queued_timestamp.as_mut().unwrap().seconds = 1234;
    metadata
        .execution_completed_timestamp
        .as_mut()
        .unwrap()
        .nanos = 123_456;
    metadata
        .virtual_execution_duration
        .as_mut()
        .unwrap()
        .seconds = 9;
    "type.googleapis.com/test.WorkerUsage".clone_into(&mut metadata.auxiliary_metadata[0].type_url);
    metadata.auxiliary_metadata[0].value = vec![0, 128, 255, 37];
    let properties = NodeProperties {
        properties: vec![NodeProperty {
            name: "xattr".to_owned(),
            value: "opaque".to_owned(),
        }],
        unix_mode: Some(0o755),
        mtime: Some(Default::default()),
    };
    let link = OutputSymlink {
        path: "link".to_owned(),
        target: "out/file".to_owned(),
        node_properties: Some(properties.clone()),
    };
    ExecuteResponse {
        result: Some(ActionResult {
            output_files: vec![OutputFile {
                path: "out/file".to_owned(),
                digest: Some(digest(1)),
                is_executable: true,
                contents: vec![0, 255, 3].into(),
                node_properties: Some(properties),
            }],
            output_directories: vec![OutputDirectory {
                path: "out/tree".to_owned(),
                tree_digest: Some(digest(2)),
                is_topologically_sorted: true,
            }],
            output_symlinks: vec![link.clone()],
            output_file_symlinks: vec![link.clone()],
            output_directory_symlinks: vec![link],
            exit_code,
            stdout_raw: vec![0, 255, 10].into(),
            stdout_digest: Some(digest(3)),
            stderr_raw: vec![128, 13, 255].into(),
            stderr_digest: Some(digest(4)),
            execution_metadata: Some(metadata),
        }),
        cached_result: true,
        status: Some(status),
        message: "stable client-visible message".to_owned(),
        server_logs: HashMap::from([
            (
                "worker log".to_owned(),
                LogFile {
                    digest: Some(digest(5)),
                    human_readable: true,
                },
            ),
            (
                "binary trace".to_owned(),
                LogFile {
                    digest: Some(digest(6)),
                    human_readable: false,
                },
            ),
        ]),
    }
}

#[test]
fn rich_known_protobuf_fields_and_exact_captured_bytes_survive_serde() {
    for (status, exit) in [(0, 0), (0, 17), (4, 0), (13, 2)] {
        let response = rich_response(status, exit);
        let payload = ResponsePayload::capture(&response, MAX_RESPONSE_BYTES).unwrap();
        let stored = payload.encoded().to_vec();
        let restored: ResponsePayload =
            serde_json::from_slice(&serde_json::to_vec(&payload).unwrap()).unwrap();
        assert_eq!(restored.encoded(), stored);
        assert_eq!(restored.decode().unwrap(), response);
        let facts = restored.facts().unwrap();
        assert_eq!(facts.exit_code, exit);
        assert_eq!(facts.error.is_some(), status != 0);
        assert_eq!(facts.cache_eligible(false), status == 0 && exit == 0);
        assert!(!facts.cache_eligible(true));
    }
}

#[test]
fn size_limits_apply_before_capture_and_during_stored_decode() {
    let response = rich_response(0, 0);
    let size = response.encoded_len();
    assert!(ResponsePayload::capture(&response, size).is_ok());
    assert!(ResponsePayload::capture(&response, size - 1).is_err());
    let mut huge = response.clone();
    huge.message = "x".repeat(MAX_RESPONSE_BYTES);
    assert!(ResponsePayload::capture(&huge, usize::MAX).is_err());
    let payload = ResponsePayload::capture(&response, size).unwrap();
    let mut stored = serde_json::to_value(payload).unwrap();
    stored["encoded"] = serde_json::to_value(vec![0u8; MAX_RESPONSE_BYTES + 1]).unwrap();
    assert!(serde_json::from_value::<ResponsePayload>(stored).is_err());
}

#[test]
fn corruption_versions_and_success_without_result_are_rejected() {
    let payload = ResponsePayload::capture(&rich_response(0, 0), MAX_RESPONSE_BYTES).unwrap();
    let stored = serde_json::to_value(&payload).unwrap();
    for field in ["encoded", "sha256"] {
        let mut broken = stored.clone();
        broken[field][0] = serde_json::json!(broken[field][0].as_u64().unwrap() ^ 1);
        assert!(serde_json::from_value::<ResponsePayload>(broken).is_err());
    }
    let mut broken = stored.clone();
    broken["version"] = serde_json::json!(2);
    assert!(serde_json::from_value::<ResponsePayload>(broken).is_err());
    let mut broken = stored;
    broken["unknown_required_section"] = serde_json::json!(true);
    assert!(serde_json::from_value::<ResponsePayload>(broken).is_err());
    assert!(ResponsePayload::capture(&ExecuteResponse::default(), MAX_RESPONSE_BYTES).is_err());
    let response = ExecuteResponse {
        status: Some(Status {
            code: 4,
            message: "no process result".to_owned(),
            details: Vec::new(),
        }),
        ..Default::default()
    };
    let payload = ResponsePayload::capture(&response, MAX_RESPONSE_BYTES).unwrap();
    assert_eq!(payload.decode().unwrap(), response);
    assert!(!payload.facts().unwrap().cache_eligible(false));
}

const EPOCH: ClusterEpoch = ClusterEpoch(77);
const fn record(event: Event) -> RecordedEvent {
    RecordedEvent {
        epoch: EPOCH,
        event,
    }
}

fn setup_events(do_not_cache: bool) -> Vec<RecordedEvent> {
    let resources = Resources {
        cpu_millis: 100,
        memory_bytes: 1024,
        slots: 1,
    };
    vec![
        record(Event::Register {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            properties: BTreeMap::new(),
            capacity: resources,
            available: resources,
        }),
        record(Event::Submit {
            operation: OperationId(1),
            request: Request {
                action: ActionKey {
                    instance: "response-test".to_owned(),
                    digest_function: "sha256".to_owned(),
                    digest: "a".repeat(64),
                    digest_size_bytes: 123,
                },
                execution_scope: None,
                do_not_cache,
                skip_cache_lookup: true,
                priority: 0,
                properties: BTreeMap::new(),
                resources,
                execution_timeout: 10,
                payload: None,
            },
        }),
        record(Event::Match),
    ]
}

fn setup_state(do_not_cache: bool) -> State {
    let mut state = State::new(EPOCH, Limits::default()).unwrap();
    for event in setup_events(do_not_cache) {
        assert!(!matches!(state.apply(event), Reply::Rejected(_)));
    }
    state
}

const fn completion(result: ResultPayload) -> RecordedEvent {
    record(Event::Complete {
        attempt: AttemptId(1),
        worker: WorkerId(1),
        incarnation: IncarnationId(1),
        result,
    })
}

fn retained_response(state: &State) -> ResultPayload {
    let Some(TerminalResult::Completed(result)) = state.view().operations[0].terminal.clone()
    else {
        panic!("missing completed operation");
    };
    result
}

// Corrupt all repeated normalized copies alike, avoiding a test that only
// proves the existing operation/execution/outbox equality checks.
fn change_all_facts(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            if object.contains_key("response") && object.contains_key("exit_code") {
                object.insert("exit_code".to_owned(), serde_json::json!(99));
            }
            for child in object.values_mut() {
                change_all_facts(child);
            }
        }
        serde_json::Value::Array(array) => {
            for child in array {
                change_all_facts(child);
            }
        }
        _ => {}
    }
}

#[test]
fn normalized_facts_are_checked_at_apply_and_restore() {
    let candidate = ResultPayload::from_response(&rich_response(0, 0), MAX_RESPONSE_BYTES).unwrap();
    let mut state = setup_state(false);
    let before = state.snapshot().unwrap();
    for field in ["digest", "exit", "error"] {
        let mut bad = candidate.clone();
        match field {
            "digest" => bad.digest = "bad".to_owned(),
            "exit" => bad.exit_code = 99,
            _ => bad.error = Some("invented failure".to_owned()),
        }
        assert_eq!(
            state.apply(completion(bad)),
            Reply::Rejected(Rejection::Invalid)
        );
        assert_eq!(state.snapshot().unwrap(), before);
    }
    assert_eq!(state.apply(completion(candidate)), Reply::Applied);
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
    change_all_facts(&mut snapshot);
    assert!(State::restore(&serde_json::to_vec(&snapshot).unwrap()).is_err());
}

#[test]
fn acceptance_preserves_bytes_and_fixed_cache_eligibility() {
    for (do_not_cache, status, exit, has_result) in [
        (false, 0, 0, true),
        (true, 0, 0, true),
        (false, 4, 0, true),
        (false, 0, 17, true),
        (false, 4, 0, false),
    ] {
        let mut response = rich_response(status, exit);
        if !has_result {
            response.result = None;
        }
        let candidate = ResultPayload::from_response(&response, MAX_RESPONSE_BYTES).unwrap();
        let bytes = candidate.require_response().unwrap().encoded().to_vec();
        let mut state = setup_state(do_not_cache);
        assert_eq!(state.apply(completion(candidate.clone())), Reply::Applied);
        let restored = State::restore(&state.snapshot().unwrap()).unwrap();
        let retained = retained_response(&restored);
        assert_eq!(retained.require_response().unwrap().encoded(), bytes);
        assert_eq!(
            retained.require_response().unwrap().decode().unwrap(),
            response
        );
        let publications: Vec<_> = restored
            .view()
            .effects
            .into_iter()
            .filter_map(|effect| match effect.kind {
                EffectKind::PublishResult { result, .. } => Some(result),
                _ => None,
            })
            .collect();
        assert_eq!(
            publications.len(),
            usize::from(!do_not_cache && status == 0 && exit == 0)
        );
        for publication in publications {
            assert_eq!(publication.require_response().unwrap().encoded(), bytes);
        }
        let before = state.snapshot().unwrap();
        assert_eq!(
            state.apply(completion(candidate)),
            Reply::Rejected(Rejection::Stale)
        );
        assert_eq!(state.snapshot().unwrap(), before);
    }
}

#[test]
fn rejected_candidates_never_create_publication_or_replace_terminal_bytes() {
    let candidate = ResultPayload::from_response(&rich_response(0, 0), MAX_RESPONSE_BYTES).unwrap();
    let mut state = setup_state(false);
    assert_eq!(
        state.apply(record(Event::Cancel {
            operation: OperationId(1)
        })),
        Reply::Applied
    );
    let before = state.snapshot().unwrap();
    assert_eq!(
        state.apply(completion(candidate)),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(state.snapshot().unwrap(), before);
    assert!(
        !state
            .view()
            .effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
    );
    let mut state = setup_state(false);
    let mut huge = rich_response(0, 0);
    huge.message = "x".repeat(20000);
    let candidate = ResultPayload::from_response(&huge, MAX_RESPONSE_BYTES).unwrap();
    let before = state.snapshot().unwrap();
    assert_eq!(
        state.apply(completion(candidate)),
        Reply::Rejected(Rejection::Invalid)
    );
    assert_eq!(state.snapshot().unwrap(), before);
    let synthetic = ResultPayload {
        digest: "fixture".to_owned(),
        exit_code: 0,
        error: None,
        response: None,
    };
    assert!(synthetic.require_response().is_err());
}

#[test]
fn malformed_protobuf_is_rejected_even_with_a_matching_checksum() {
    let payload = ResponsePayload::capture(&rich_response(0, 0), MAX_RESPONSE_BYTES).unwrap();
    let mut wire = serde_json::to_value(payload).unwrap();
    let broken = [0x0a, 0xff]; // incomplete embedded ActionResult length
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(&broken);
    wire["encoded"] = serde_json::json!(broken);
    wire["sha256"] = serde_json::json!(**hasher.finalize_digest().packed_hash());
    assert!(serde_json::from_value::<ResponsePayload>(wire).is_err());
}

struct JournalDirectory(PathBuf);
impl Drop for JournalDirectory {
    fn drop(&mut self) {
        let _cleanup = std::fs::remove_dir_all(&self.0);
    }
}

#[nativelink_test]
async fn durable_acceptance_and_restart_retain_exact_response_and_outbox_bytes() {
    let directory = JournalDirectory(
        std::env::temp_dir().join(format!("nativelink-response-{}", uuid::Uuid::new_v4())),
    );
    std::fs::create_dir(&directory.0).unwrap();
    let path = directory.0.join("authority.redb");
    let node = Singleton::open(&path, 1, EPOCH, Limits::default())
        .await
        .unwrap();
    for event in setup_events(false) {
        assert!(!matches!(
            node.write(event).await.unwrap().data,
            Reply::Rejected(_)
        ));
    }
    let response = rich_response(0, 0);
    let candidate = ResultPayload::from_response(&response, MAX_RESPONSE_BYTES).unwrap();
    let bytes = candidate.require_response().unwrap().encoded().to_vec();
    assert_eq!(
        node.write(completion(candidate.clone()))
            .await
            .unwrap()
            .data,
        Reply::Applied
    );
    let snapshot = node.read().await.unwrap().snapshot().unwrap();
    node.shutdown().await.unwrap();
    let node = Singleton::open(&path, 1, EPOCH, Limits::default())
        .await
        .unwrap();
    let state = node.read().await.unwrap();
    assert_eq!(state.snapshot().unwrap(), snapshot);
    assert_eq!(
        retained_response(&state)
            .require_response()
            .unwrap()
            .encoded(),
        bytes
    );
    assert_eq!(
        retained_response(&state)
            .require_response()
            .unwrap()
            .decode()
            .unwrap(),
        response
    );
    let publication = state
        .view()
        .effects
        .into_iter()
        .find_map(|effect| match effect.kind {
            EffectKind::PublishResult { result, .. } => Some(result),
            _ => None,
        })
        .unwrap();
    assert_eq!(publication.require_response().unwrap().encoded(), bytes);
    assert_eq!(
        node.write(completion(candidate)).await.unwrap().data,
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(node.read().await.unwrap().snapshot().unwrap(), snapshot);
    node.shutdown().await.unwrap();
}
