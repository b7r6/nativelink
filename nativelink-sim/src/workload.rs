// Copyright 2024 The NativeLink Authors. All rights reserved.
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

//! The actions the clients run, drawn from the seed.

use core::time::Duration;
use std::collections::BTreeMap;

use nativelink_proto::build::bazel::remote::execution::v2::{Action, Command, Directory};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use prost::Message;
use prost::bytes::Bytes;
use rand::Rng;
use rand::rngs::StdRng;

use crate::config::SimConfig;

/// The instance name every client and the execution service use.
pub const INSTANCE_NAME: &str = "main";

/// One action the clients may execute.
#[derive(Clone, Debug)]
pub struct ActionDef {
    /// For traces: `a<index>`.
    pub label: String,
    /// The `Action` digest the clients send.
    pub digest: DigestInfo,
    /// The blobs the scheduler's CAS must hold for `Execute` to accept it.
    pub blobs: Vec<(DigestInfo, Bytes)>,
    /// How long a healthy worker takes to run it.
    pub duration: Duration,
    /// Whether clients send it with `skip_cache_lookup`: an uncacheable
    /// action is never merged with another client's.
    pub uncacheable: bool,
}

/// One job a client runs: execute an action until a final answer.
#[derive(Clone, Debug)]
pub struct JobDef {
    /// `c<client>.j<job>`.
    pub name: String,
    /// Index into the action table.
    pub action: usize,
    /// When the client submits it.
    pub start_at: Duration,
}

/// The whole workload.
#[derive(Clone, Debug)]
pub struct Workload {
    pub actions: Vec<ActionDef>,
    pub clients: Vec<Vec<JobDef>>,
    /// Action digest (hash) to its index, for workers.
    pub by_digest: BTreeMap<String, usize>,
}

fn digest_of(bytes: &[u8]) -> DigestInfo {
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(bytes);
    hasher.finalize_digest()
}

fn make_action(index: usize, duration: Duration, uncacheable: bool) -> ActionDef {
    let command = Command {
        arguments: vec!["sim-action".to_string(), index.to_string()],
        ..Default::default()
    };
    let command_bytes = Bytes::from(command.encode_to_vec());
    let command_digest = digest_of(&command_bytes);
    let input_root = Directory::default();
    let input_root_bytes = Bytes::from(input_root.encode_to_vec());
    let input_root_digest = digest_of(&input_root_bytes);
    let action = Action {
        command_digest: Some(command_digest.into()),
        input_root_digest: Some(input_root_digest.into()),
        do_not_cache: uncacheable,
        ..Default::default()
    };
    let action_bytes = Bytes::from(action.encode_to_vec());
    let digest = digest_of(&action_bytes);
    ActionDef {
        label: format!("a{index}"),
        digest,
        blobs: vec![
            (command_digest, command_bytes),
            (input_root_digest, input_root_bytes),
            (digest, action_bytes),
        ],
        duration,
        uncacheable,
    }
}

impl Workload {
    /// Draw a workload: each client gets its own actions, and one action is
    /// shared by every client (cacheable, so the scheduler merges them).
    pub fn generate(config: &SimConfig, rng: &mut StdRng) -> Self {
        let mut actions = Vec::new();
        let shared_duration = Duration::from_millis(rng.random_range(2_000..20_000));
        actions.push(make_action(0, shared_duration, false));
        let mut clients = Vec::with_capacity(config.clients);
        let submit_window = config.active.as_millis() / 2;
        let submit_window = u64::try_from(submit_window).unwrap_or(u64::MAX).max(1);
        for client in 0..config.clients {
            let mut jobs = Vec::with_capacity(config.jobs_per_client + 1);
            jobs.push(JobDef {
                name: format!("c{client}.shared"),
                action: 0,
                start_at: Duration::from_millis(rng.random_range(0..submit_window.min(20_000))),
            });
            for job in 0..config.jobs_per_client {
                let index = actions.len();
                let duration = Duration::from_millis(rng.random_range(1_000..20_000));
                let uncacheable = rng.random_bool(0.5);
                actions.push(make_action(index, duration, uncacheable));
                jobs.push(JobDef {
                    name: format!("c{client}.j{job}"),
                    action: index,
                    start_at: Duration::from_millis(rng.random_range(0..submit_window)),
                });
            }
            jobs.sort_by_key(|job| job.start_at);
            clients.push(jobs);
        }
        let by_digest = actions
            .iter()
            .enumerate()
            .map(|(index, action)| (action.digest.packed_hash().to_string(), index))
            .collect();
        Self {
            actions,
            clients,
            by_digest,
        }
    }
}
