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

//! Fixture inputs are owned by this comparison, not copied from either engine's tests.

use std::collections::{BTreeMap, BTreeSet};

use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, Command, Digest, Directory, ExecuteRequest, ExecuteResponse,
    ExecutionPolicy, FileNode, OutputDirectory, OutputFile, Platform, RequestMetadata, Tree,
};
use nativelink_proto::google::rpc::Status;
use nativelink_scheduler::authority as a;
use nativelink_scheduler::authority::request::RequestPayload;
use prost::Message as _;
use prost_types::{Duration, Timestamp};
use sha2::{Digest as _, Sha256};

use super::head::types as h;
use super::publication_candidate::types as c;
use super::types as r;

pub(super) const LABEL: &str = "prepared-memory-differential";
pub(super) const NAMESPACE: [u8; 16] = [31; 16];
pub(super) const INCARNATION: [u8; 16] = [47; 16];

pub(super) fn context() -> c::TrustedContext {
    c::TrustedContext {
        principal: "differential-principal".into(),
        execution_config: "trusted-config-v1".into(),
    }
}

fn digest(bytes: &[u8], algorithm: u8) -> Digest {
    Digest {
        hash: match algorithm {
            1 => format!("{:x}", Sha256::digest(bytes)),
            9 => blake3::hash(bytes).to_hex().to_string(),
            _ => unreachable!(),
        },
        size_bytes: i64::try_from(bytes.len()).unwrap(),
    }
}

pub(super) fn request(algorithm: u8, memory_kb: u64, caller: i32, label: &str) -> a::Request {
    request_lookup(algorithm, memory_kb, caller, label, true)
}
pub(super) fn request_lookup(
    algorithm: u8,
    memory_kb: u64,
    caller: i32,
    label: &str,
    skip: bool,
) -> a::Request {
    let command = Command {
        arguments: vec!["/bin/false".into(), label.into()],
        ..Command::default()
    }
    .encode_to_vec();
    let action = Action {
        command_digest: Some(digest(&command, algorithm)),
        input_root_digest: Some(digest(&Directory::default().encode_to_vec(), algorithm)),
        timeout: Some(Duration {
            seconds: 5,
            nanos: 0,
        }),
        platform: Some(Platform {
            properties: vec![
                Property {
                    name: "cpu_count".into(),
                    value: "1".into(),
                },
                Property {
                    name: "memory_kb".into(),
                    value: memory_kb.to_string(),
                },
                Property {
                    name: "os".into(),
                    value: "linux".into(),
                },
            ],
        }),
        ..Action::default()
    }
    .encode_to_vec();
    a::Request::from_payload(
        RequestPayload::capture(
            &ExecuteRequest {
                instance_name: "terminal-differential-instance".into(),
                action_digest: Some(digest(&action, algorithm)),
                digest_function: i32::from(algorithm),
                skip_cache_lookup: skip,
                execution_policy: Some(ExecutionPolicy { priority: caller }),
                ..ExecuteRequest::default()
            },
            Some(&RequestMetadata {
                action_id: format!("caller-{caller}"),
                ..RequestMetadata::default()
            }),
            &Timestamp {
                seconds: 100 + i64::from(caller),
                nanos: 13,
            },
            &context().principal,
            &action,
            &command,
            c::MAX_PAYLOAD,
        )
        .unwrap(),
    )
    .unwrap()
}

pub(super) fn response(message: &str, graph: bool) -> a::ResultPayload {
    a::ResultPayload::from_response(
        &ExecuteResponse {
            status: Some(Status {
                code: 13,
                message: message.into(),
                details: vec![],
            }),
            result: graph.then(ActionResult::default),
            message: "exact terminal diagnostic bytes".into(),
            ..ExecuteResponse::default()
        },
        c::MAX_PAYLOAD,
    )
    .unwrap()
}

pub(super) fn register(capacity: u64, incarnation: u64) -> a::Event {
    let resources = a::Resources {
        cpu_millis: 8000,
        memory_bytes: capacity,
        slots: 8,
    };
    a::Event::Register {
        worker: a::WorkerId(1),
        incarnation: a::IncarnationId(incarnation),
        session: a::SessionId(incarnation),
        properties: BTreeMap::from([("os".into(), "linux".into())]),
        capacity: resources,
        available: resources,
    }
}

pub(super) fn oracle_context() -> r::Context {
    r::Context {
        principal: context().principal,
        execution_config: context().execution_config,
    }
}
pub(super) const fn key(value: h::Key) -> c::Key {
    c::Key {
        namespace: value.namespace,
        incarnation: value.incarnation,
        generation: value.generation,
    }
}
pub(super) const fn object(value: &h::Object) -> c::Object {
    c::Object {
        algorithm: match value.algorithm {
            h::Algorithm::Sha256 => 1,
            h::Algorithm::Blake3 => 9,
        },
        hash: value.hash,
        bytes: value.bytes,
    }
}
pub(super) const fn member(value: &h::Member) -> c::Member {
    c::Member {
        key: key(value.key),
        object: object(&value.object),
        role: match value.role {
            h::Role::Result => c::Role::Result,
            h::Role::Blob => c::Role::Blob,
            h::Role::Directory => c::Role::Directory,
            h::Role::Tree => c::Role::Tree,
        },
    }
}
fn graph_object(bytes: &[u8], algorithm: u8) -> h::Object {
    h::Object {
        algorithm: if algorithm == 1 {
            h::Algorithm::Sha256
        } else {
            h::Algorithm::Blake3
        },
        hash: if algorithm == 1 {
            Sha256::digest(bytes).into()
        } else {
            *blake3::hash(bytes).as_bytes()
        },
        bytes: bytes.len().try_into().unwrap(),
    }
}
pub(super) fn scenario(
    algorithm: u8,
    memory_kb: u64,
) -> (h::Scenario, BTreeMap<u64, a::ResultPayload>, h::Facts) {
    let action = serde_json::to_string(
        &super::reference::binding(&oracle_context(), &request(algorithm, memory_kb, 0, LABEL))
            .unwrap(),
    )
    .unwrap();
    let mut candidates = BTreeMap::new();
    let mut payloads = BTreeMap::new();
    let mut physical = vec![];
    for attempt in 1..=3 {
        let stdout = format!("retained stdout attempt {attempt}").into_bytes();
        let directory = Directory {
            files: vec![FileNode {
                name: format!("directory-file-{attempt}"),
                digest: Some(digest(&stdout, algorithm)),
                ..FileNode::default()
            }],
            ..Directory::default()
        };
        let directory_bytes = directory.encode_to_vec();
        let tree = Tree {
            root: Some(directory),
            children: vec![],
        }
        .encode_to_vec();
        let files: Vec<_> = (0..4)
            .map(|i| format!("attempt {attempt} output file {i}").into_bytes())
            .collect();
        let result = ActionResult {
            output_files: files
                .iter()
                .enumerate()
                .map(|(i, body)| OutputFile {
                    path: format!("file-{i}"),
                    digest: Some(digest(body, algorithm)),
                    ..OutputFile::default()
                })
                .collect(),
            exit_code: 17,
            stdout_digest: Some(digest(&stdout, algorithm)),
            output_directories: vec![OutputDirectory {
                path: "output".into(),
                tree_digest: Some(digest(&tree, algorithm)),
                ..OutputDirectory::default()
            }],
            ..ActionResult::default()
        };
        let bytes = result.encode_to_vec();
        let response = ExecuteResponse {
            result: Some(result),
            status: Some(Status {
                code: 13,
                message: format!("original worker failure {attempt}"),
                details: vec![],
            }),
            message: format!("exact retained response {attempt}"),
            ..ExecuteResponse::default()
        };
        let encoded = response.encode_to_vec();
        let payload = a::ResultPayload::from_response(&response, c::MAX_PAYLOAD).unwrap();
        let mut required = BTreeSet::new();
        for (index, (body, role)) in [
            (&bytes, h::Role::Result),
            (&stdout, h::Role::Blob),
            (&directory_bytes, h::Role::Directory),
            (&tree, h::Role::Tree),
        ]
        .into_iter()
        .chain(files.iter().map(|body| (body, h::Role::Blob)))
        .enumerate()
        {
            let member = h::Member {
                object: graph_object(body, algorithm),
                key: h::Key {
                    namespace: NAMESPACE,
                    incarnation: INCARNATION,
                    generation: attempt * 10 + u64::try_from(index).unwrap() + 1,
                },
                role,
            };
            physical.push(h::Physical {
                key: member.key,
                object: member.object.clone(),
                ready: true,
                retired: false,
            });
            required.insert(member);
        }
        candidates.insert(
            200 + attempt,
            h::Candidate {
                id: 200 + attempt,
                action: action.clone(),
                source: h::Source::Worker {
                    execution: 1,
                    attempt: h::Attempt {
                        id: attempt,
                        worker: 1,
                        incarnation: 1,
                    },
                },
                result: graph_object(&bytes, algorithm),
                bytes,
                response: encoded,
                successful: false,
                error_free: false,
                required,
            },
        );
        payloads.insert(attempt, payload);
    }
    let revision = physical.len().try_into().unwrap();
    (
        h::Scenario {
            actions: BTreeMap::from([(action, false)]),
            candidates,
        },
        payloads,
        h::Facts {
            catalogue_revision: revision,
            physical,
            fences: BTreeSet::from([h::Fence {
                namespace: NAMESPACE,
                revision: 0,
            }]),
            receipts: BTreeSet::new(),
        },
    )
}
