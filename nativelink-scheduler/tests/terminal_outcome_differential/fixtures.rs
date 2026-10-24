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

use std::collections::BTreeMap;

use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, Command, Digest, Directory, ExecuteRequest, ExecuteResponse,
    ExecutionPolicy, Platform, RequestMetadata,
};
use nativelink_proto::google::rpc::Status;
use nativelink_scheduler::authority as a;
use nativelink_scheduler::authority::request::RequestPayload;
use prost::Message as _;
use prost_types::{Duration, Timestamp};
use sha2::{Digest as _, Sha256};

use super::publication_candidate::types as c;
use super::types as r;

pub(super) const LABEL: &str = "terminal-differential";
pub(super) const SEED: &str = "protected-publication";
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
                skip_cache_lookup: true,
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

pub(super) const fn physical(generation: u64) -> r::Physical {
    r::Physical {
        namespace: NAMESPACE,
        incarnation: INCARNATION,
        generation,
    }
}

pub(super) const fn key(value: r::Physical) -> c::Key {
    c::Key {
        namespace: value.namespace,
        incarnation: value.incarnation,
        generation: value.generation,
    }
}
