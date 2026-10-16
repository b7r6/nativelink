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

//! Generate the entire workload before either version runs.
use std::collections::BTreeSet;

use bytes::Bytes;
use nativelink_proto::build::bazel::remote::execution::v2::command::EnvironmentVariable;
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, Directory, FileNode, Platform,
};
use nativelink_store::ac_utils::compute_buf_digest;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use prost::Message;
use serde::{Deserialize, Serialize};

pub(crate) const MIB: u64 = 1024 * 1024;
pub(crate) const NODES: usize = 4;

pub(crate) fn uniform(seed: u64, key: u64) -> f64 {
    let mut x = seed.wrapping_add(key.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (f64::from(u32::try_from((x ^ (x >> 31)) >> 32).unwrap()) + 0.5) / 4_294_967_296.0
}

fn lognormal(seed: u64, key: u64, median: f64, sigma: f64) -> f64 {
    let z = (-2.0 * uniform(seed, key * 2).ln()).sqrt()
        * (core::f64::consts::TAU * uniform(seed, key * 2 + 1)).cos();
    median * (sigma * z).exp()
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "rounded draw is clamped to the supplied u32 bounds before conversion"
)]
fn lognormal_count(seed: u64, key: u64, median: f64, sigma: f64, min: u32, max: u32) -> u32 {
    lognormal(seed, key, median, sigma)
        .round()
        .clamp(f64::from(min), f64::from(max)) as u32
}

#[derive(Clone)]
pub(crate) struct Blob {
    pub(crate) digest: DigestInfo,
    pub(crate) data: Bytes,
}

impl Blob {
    fn new(data: Bytes) -> Self {
        let digest = compute_buf_digest(&data, &mut DigestHasherFunc::Sha256.hasher());
        Self { digest, data }
    }
    pub(crate) fn name(&self) -> String {
        format!("{}-{}", self.digest.packed_hash(), self.digest.size_bytes())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Metadata {
    pub(crate) hash: String,
    pub(crate) size: u64,
    pub(crate) hex: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Job {
    pub(crate) id: u32,
    pub(crate) node: usize,
    pub(crate) arrival_s: f64,
    pub(crate) inputs: Vec<usize>,
    pub(crate) execution_s: f64,
    pub(crate) hash: String,
    pub(crate) size: u64,
}

impl Job {
    pub(crate) fn digest(&self) -> DigestInfo {
        DigestInfo::try_new(&self.hash, self.size).unwrap()
    }
}

pub(crate) struct Workload {
    pub(crate) blobs: Vec<Blob>,
    pub(crate) metadata: Vec<Metadata>,
    pub(crate) warmup: Vec<Job>,
    pub(crate) jobs: Vec<Job>,
    pub(crate) sentinel: Vec<Job>,
    pub(crate) catalogue_bytes: Vec<u64>,
}

impl Workload {
    pub(crate) fn new(seed: u64) -> Self {
        let mut w = Self {
            blobs: Vec::new(),
            metadata: Vec::new(),
            warmup: Vec::new(),
            jobs: Vec::new(),
            sentinel: Vec::new(),
            catalogue_bytes: Vec::new(),
        };
        // Declared stress distribution, not a fitted customer size law.
        for i in 0_u32..8 {
            let units = lognormal_count(seed, 100 + u64::from(i), 4.0, 0.55, 2, 6);
            // A declared large-input probe must exceed the three buffered store
            // channels; otherwise Azure can finish before ENOSPC reaches it.
            let size = if i == 1 {
                128 * MIB
            } else {
                u64::from(units) * 4 * MIB
            };
            w.catalogue_bytes.push(size);
            w.blobs.push(Blob::new(Bytes::from(vec![
                u8::try_from(i + 1).unwrap();
                usize::try_from(size).unwrap()
            ])));
        }
        for node in 0..NODES {
            let j = w.job(seed, u32::try_from(node).unwrap(), node, 0.0, vec![0]);
            w.warmup.push(j);
            let j = w.job(seed, 10 + u32::try_from(node).unwrap(), node, 0.0, vec![0]);
            w.sentinel.push(j);
        }
        // Two simultaneous offers per node expose the actual fill coalescer.
        for node in 0..NODES {
            for copy in 0_u32..2 {
                let j = w.job(
                    seed,
                    100 + u32::try_from(node).unwrap() * 2 + copy,
                    node,
                    0.0,
                    vec![1],
                );
                w.jobs.push(j);
            }
        }
        let zipf: Vec<f64> = (1..=8).map(|rank| f64::from(rank).powf(-1.1)).collect();
        let total: f64 = zipf.iter().sum();
        let mut time = 0.1;
        for i in 0_u32..32 {
            let hazard = -uniform(seed, 1000 + u64::from(i)).ln();
            time = if time < 1.0 {
                let remaining = (1.0 - time) * 10.0;
                if hazard <= remaining {
                    time + hazard / 10.0
                } else {
                    1.0 + (hazard - remaining) / 2.0
                }
            } else {
                time + hazard / 2.0
            };
            if time >= 3.0 {
                break;
            }
            let files = lognormal_count(seed, 2000 + u64::from(i), 2.0, 0.5, 1, 4);
            let mut inputs = BTreeSet::new();
            for k in 0..files {
                let mut pick = uniform(seed, 3000 + u64::from(i) * 8 + u64::from(k)) * total;
                let mut chosen = 7;
                for (index, weight) in zipf.iter().enumerate() {
                    pick -= weight;
                    if pick <= 0.0 {
                        chosen = index;
                        break;
                    }
                }
                inputs.insert(chosen);
            }
            let j = w.job(
                seed,
                200 + i,
                usize::try_from(i).unwrap() % NODES,
                time,
                inputs.into_iter().collect(),
            );
            w.jobs.push(j);
        }
        w.jobs
            .sort_by(|a, b| a.arrival_s.total_cmp(&b.arrival_s).then(a.id.cmp(&b.id)));
        w
    }

    fn message(&mut self, m: &impl Message) -> DigestInfo {
        let blob = Blob::new(Bytes::from(m.encode_to_vec()));
        self.metadata.push(Metadata {
            hash: blob.digest.packed_hash().to_string(),
            size: blob.digest.size_bytes(),
            hex: hex::encode(&blob.data),
        });
        let digest = blob.digest;
        self.blobs.push(blob);
        digest
    }

    fn job(&mut self, seed: u64, id: u32, node: usize, arrival_s: f64, inputs: Vec<usize>) -> Job {
        let root = self.message(&Directory {
            files: inputs
                .iter()
                .map(|&i| FileNode {
                    name: format!("input{i}"),
                    digest: Some(self.blobs[i].digest.into()),
                    is_executable: true,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        });
        let execution_s = lognormal(seed, 10_000 + u64::from(id), 0.02, 0.4).clamp(0.005, 0.1);
        let command = self.message(&Command {
            arguments: vec![
                "/usr/bin/env".into(),
                "sh".into(),
                "-c".into(),
                format!("sleep {execution_s:.6}; wc -c input* > out"),
                format!("job-{id}"),
            ],
            environment_variables: vec![EnvironmentVariable {
                name: "PATH".into(),
                value: std::env::var("PATH").unwrap_or_default(),
            }],
            output_files: vec!["out".into()],
            platform: Some(Platform {
                properties: vec![Property {
                    name: "fleet_node".into(),
                    value: node.to_string(),
                }],
            }),
            ..Default::default()
        });
        let action = self.message(&Action {
            command_digest: Some(command.into()),
            input_root_digest: Some(root.into()),
            do_not_cache: true,
            ..Default::default()
        });
        Job {
            id,
            node,
            arrival_s,
            inputs,
            execution_s,
            hash: action.packed_hash().to_string(),
            size: action.size_bytes(),
        }
    }
}
