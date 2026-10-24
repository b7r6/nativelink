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

//! The expected graph is built from this scenario, never from preparation output.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::PathBuf;

use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest, Directory, DirectoryNode, ExecuteResponse, FileNode, OutputDirectory,
    OutputFile, Tree,
};
use nativelink_scheduler::authority as a;
use nativelink_store::physical_closure::{
    Binding, Edge, EdgeKind, Limits as ClosureLimits, Preparation, PreparedClosure, RetainedRoot,
    Role as ClosureRole,
};
use nativelink_store::physical_filesystem::{
    Algorithm, Incarnation, Limits as PhysicalLimits, ObjectId, PhysicalFilesystem, PhysicalKey,
    StagedWrite, WriteReceipt,
};
use prost::Message;

use super::engine::PublicationAuthority;
use super::protocol as r;
use super::types::{Candidate, Evidence, Key, MAX_PAYLOAD, Member, Object, Publisher, Role};

pub(super) const ACTION: &str = "principal/instance/digest/action/platform/config";
pub(super) const NAMESPACE: [u8; 16] = [0x31; 16];
pub(super) const INCARNATION: [u8; 16] = [0x72; 16];

pub(super) fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(out, "{byte:02x}").unwrap();
    }
    out
}
pub(super) const fn algorithm_name(algorithm: Algorithm) -> &'static str {
    match algorithm {
        Algorithm::Sha256 => "SHA256",
        Algorithm::Blake3 => "BLAKE3",
    }
}
pub(super) const fn algorithm_number(algorithm: Algorithm) -> u8 {
    match algorithm {
        Algorithm::Sha256 => 1,
        Algorithm::Blake3 => 9,
    }
}
pub(super) const fn object(id: ObjectId) -> Object {
    Object {
        algorithm: algorithm_number(id.algorithm),
        hash: id.hash,
        bytes: id.size,
    }
}
pub(super) fn physical_key(key: Key) -> PhysicalKey {
    PhysicalKey::new(Incarnation::new(key.incarnation).unwrap(), key.generation).unwrap()
}
fn digest(id: ObjectId) -> Digest {
    Digest {
        hash: hex(&id.hash),
        size_bytes: i64::try_from(id.size).unwrap(),
    }
}
fn file(name: &str, id: ObjectId) -> FileNode {
    FileNode {
        name: name.into(),
        digest: Some(digest(id)),
        ..FileNode::default()
    }
}

#[derive(Clone)]
pub(super) struct Row {
    pub(super) key: Key,
    pub(super) object: ObjectId,
    pub(super) role: Role,
    pub(super) bytes: Vec<u8>,
}
impl Row {
    pub(super) const fn member(&self) -> Member {
        Member {
            key: self.key,
            object: object(self.object),
            role: self.role,
        }
    }
    pub(super) fn binding(&self) -> Binding {
        Binding {
            key: physical_key(self.key),
            object: self.object,
        }
    }
}

pub(super) struct Graph {
    pub(super) rows: Vec<Row>,
    pub(super) retained: Vec<RetainedRoot>,
    pub(super) edges: Vec<Edge>,
    pub(super) response: a::ResultPayload,
}
impl Graph {
    pub(super) fn new(
        algorithm: Algorithm,
        namespace: [u8; 16],
        incarnation: [u8; 16],
        first: u64,
        label: &str,
    ) -> Self {
        let mut rows = Vec::new();
        let mut add = |bytes: Vec<u8>, role| {
            let id = ObjectId::for_bytes(algorithm, &bytes);
            rows.push(Row {
                key: Key {
                    namespace,
                    incarnation,
                    generation: first + rows.len() as u64,
                },
                object: id,
                role,
                bytes,
            });
            id
        };
        let artifact = add(format!("artifact:{label}").into_bytes(), Role::Blob);
        let stdout = add(format!("stdout:{label}").into_bytes(), Role::Blob);
        let stderr = add(format!("stderr:{label}").into_bytes(), Role::Blob);
        let leaf = Directory {
            files: vec![file("leaf", artifact)],
            ..Directory::default()
        };
        let leaf_id = ObjectId::for_bytes(algorithm, &leaf.encode_to_vec());
        let root = Directory {
            files: vec![file("root", artifact)],
            directories: vec![DirectoryNode {
                name: "sub".into(),
                digest: Some(digest(leaf_id)),
            }],
            ..Directory::default()
        };
        let root_id = ObjectId::for_bytes(algorithm, &root.encode_to_vec());
        let tree = add(
            Tree {
                root: Some(root),
                children: vec![leaf],
            }
            .encode_to_vec(),
            Role::Tree,
        );
        let directory = add(
            Directory {
                files: vec![file("retained", stdout)],
                ..Directory::default()
            }
            .encode_to_vec(),
            Role::Directory,
        );
        let result = ActionResult {
            output_files: vec![OutputFile {
                path: "artifact".into(),
                digest: Some(digest(artifact)),
                ..OutputFile::default()
            }],
            output_directories: vec![OutputDirectory {
                path: "tree".into(),
                tree_digest: Some(digest(tree)),
                is_topologically_sorted: true,
            }],
            stdout_digest: Some(digest(stdout)),
            stderr_digest: Some(digest(stderr)),
            ..ActionResult::default()
        };
        let result_id = add(result.encode_to_vec(), Role::Result);
        let response = a::ResultPayload::from_response(
            &ExecuteResponse {
                result: Some(result),
                message: format!("captured:{label}"),
                ..ExecuteResponse::default()
            },
            MAX_PAYLOAD,
        )
        .unwrap();
        let edge = |source, embedded_directory, target, kind| Edge {
            source,
            embedded_directory,
            target,
            kind,
        };
        Self {
            rows,
            retained: vec![RetainedRoot {
                object: directory,
                role: ClosureRole::Directory,
            }],
            edges: vec![
                edge(result_id, None, artifact, EdgeKind::OutputFile),
                edge(result_id, None, tree, EdgeKind::OutputTree),
                edge(result_id, None, stdout, EdgeKind::Stdout),
                edge(result_id, None, stderr, EdgeKind::Stderr),
                edge(tree, Some(root_id), artifact, EdgeKind::File),
                edge(tree, Some(root_id), leaf_id, EdgeKind::EmbeddedDirectory),
                edge(tree, Some(leaf_id), artifact, EdgeKind::File),
                edge(directory, None, stdout, EdgeKind::File),
            ],
            response,
        }
    }
    pub(super) fn result(&self) -> &Row {
        self.rows.last().unwrap()
    }
    pub(super) fn closure(&self) -> BTreeSet<Member> {
        self.rows.iter().map(Row::member).collect()
    }
    pub(super) fn candidate(&self, publisher: Publisher) -> Candidate {
        Candidate {
            action: ACTION.into(),
            publisher,
            result: object(self.result().object),
            result_bytes: self.result().bytes.clone(),
            response: self.response.clone(),
            closure: self.closure(),
        }
    }
    pub(super) fn verify_prepared(&self, prepared: &PreparedClosure) -> BTreeSet<Member> {
        assert_eq!(prepared.action_result, self.result().object);
        assert_eq!(prepared.retained, self.retained);
        assert_eq!(prepared.edges.len(), self.edges.len());
        for edge in &self.edges {
            assert!(
                prepared.edges.contains(edge),
                "missing expected edge {edge:?}"
            );
        }
        let mut members = BTreeSet::new();
        for prepared_object in &prepared.objects {
            let row = self
                .rows
                .iter()
                .find(|row| row.binding() == prepared_object.binding)
                .unwrap();
            for role in &prepared_object.roles {
                let role = match role {
                    ClosureRole::ActionResult => Role::Result,
                    ClosureRole::Blob => Role::Blob,
                    ClosureRole::Directory => Role::Directory,
                    ClosureRole::Tree => Role::Tree,
                };
                members.insert(Member {
                    object: object(prepared_object.binding.object),
                    key: row.key,
                    role,
                });
            }
        }
        assert_eq!(members, self.closure());
        assert_eq!(prepared.objects.len(), self.rows.len());
        assert_eq!(
            prepared.read_bytes,
            self.rows.iter().map(|row| row.object.size).sum::<u64>()
        );
        members
    }
}

pub(super) struct Capture {
    pub(super) job: Preparation,
    pub(super) fences: BTreeMap<u64, u64>,
}
impl Capture {
    pub(super) fn finish(mut self, graph: &Graph, stage: u64) -> Evidence {
        let mut complete = false;
        for _ in 0..16 {
            if self.job.advance(1).unwrap() {
                complete = true;
                break;
            }
        }
        assert!(complete);
        let prepared = self.job.finish().unwrap();
        Evidence {
            stage,
            catalogue_revision: prepared.catalogue_revision,
            closure: graph.verify_prepared(&prepared),
            fences: self.fences,
            until: 100,
        }
    }
}

pub(super) struct Fixture {
    pub(super) state: PublicationAuthority,
    pub(super) base: PathBuf,
    stores: BTreeMap<([u8; 16], [u8; 16]), PhysicalFilesystem>,
}
impl Fixture {
    pub(super) fn new() -> Self {
        let base = std::env::temp_dir().join(format!(
            "nativelink-publication-physical-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir(&base).unwrap();
        fs::create_dir(base.join("authority")).unwrap();
        Self {
            state: PublicationAuthority::new(BTreeMap::from([(ACTION.into(), false)])).unwrap(),
            base,
            stores: BTreeMap::new(),
        }
    }
    fn namespace_path(&self, namespace: [u8; 16]) -> PathBuf {
        self.base.join(hex(&namespace))
    }
    pub(super) fn store(&self, key: Key) -> &PhysicalFilesystem {
        &self.stores[&(key.namespace, key.incarnation)]
    }
    fn ensure_store(&mut self, key: Key) {
        let identity = (key.namespace, key.incarnation);
        if self.stores.contains_key(&identity) {
            return;
        }
        let base = self.namespace_path(key.namespace);
        if !base.exists() {
            fs::create_dir(&base).unwrap();
            File::open(&self.base).unwrap().sync_all().unwrap();
        }
        let store = PhysicalFilesystem::bootstrap(
            &base,
            Incarnation::new(key.incarnation).unwrap(),
            PhysicalLimits {
                object_bytes: MAX_PAYLOAD as u64,
                objects: 64,
                staged_writes: 64,
                concurrent_reads: 4,
            },
        )
        .unwrap();
        self.stores.insert(identity, store);
    }
    pub(super) fn register_and_assign(&mut self, operation: u64) -> Publisher {
        let capacity = a::Resources {
            cpu_millis: 100,
            memory_bytes: 100,
            slots: 8,
        };
        if self.state.scheduler_view().workers.is_empty() {
            self.state
                .scheduler_event(a::Event::Register {
                    worker: a::WorkerId(1),
                    incarnation: a::IncarnationId(1),
                    session: a::SessionId(1),
                    properties: BTreeMap::new(),
                    capacity,
                    available: capacity,
                })
                .unwrap();
        }
        let execution = self
            .state
            .admit(operation, ACTION.into(), true, 100)
            .unwrap();
        let a::Reply::Matched { attempt, .. } =
            self.state.scheduler_event(a::Event::Match).unwrap()
        else {
            panic!("placement refused")
        };
        self.state
            .scheduler_event(a::Event::WorkerAccepted {
                attempt,
                worker: a::WorkerId(1),
                incarnation: a::IncarnationId(1),
            })
            .unwrap();
        Publisher::Worker {
            execution,
            attempt,
            worker: a::WorkerId(1),
            incarnation: a::IncarnationId(1),
        }
    }
    pub(super) fn stage(&mut self, graph: &Graph, id: u64, publisher: Publisher) {
        let owner = match publisher {
            Publisher::Worker { execution, .. } => Some(execution),
            Publisher::Client { .. } | Publisher::AuthorizedClient { .. } => None,
        };
        for row in &graph.rows {
            self.ensure_store(row.key);
            self.state
                .install_physical_for(owner, row.key, object(row.object), false)
                .unwrap();
            assert!(
                self.store(row.key)
                    .read(physical_key(row.key), row.object, 0, None)
                    .is_err()
            );
        }
        let writes = graph
            .rows
            .iter()
            .map(|row| (id * 100 + row.key.generation, row.key))
            .collect();
        self.state
            .stage(id, graph.candidate(publisher), 10, writes)
            .unwrap();
        let protected = self.state.protected_keys();
        assert!(graph.rows.iter().all(|row| protected.contains(&row.key)));
        assert!(
            self.state
                .retention_view()
                .generations
                .values()
                .filter(|row| !row.ready)
                .count()
                >= graph.rows.len()
        );
    }
    pub(super) fn begin(&self, graph: &Graph, stage: u64, row: usize) -> StagedWrite {
        let row = &graph.rows[row];
        let write = self.state.stages()[&stage]
            .writes
            .iter()
            .find_map(|(write, key)| (*key == row.key).then_some(*write))
            .unwrap();
        self.store(row.key)
            .begin_write(physical_key(row.key), write, row.object)
            .unwrap()
    }
    pub(super) fn finish_write(&self, graph: &Graph, stage: u64, index: usize) -> WriteReceipt {
        let row = &graph.rows[index];
        let mut write = self.begin(graph, stage, index);
        let middle = row.bytes.len() / 2;
        write.append(&row.bytes[..middle]).unwrap();
        write.append(&row.bytes[middle..]).unwrap();
        let receipt = write.finish().unwrap();
        assert_eq!(receipt.key, physical_key(row.key));
        assert_eq!(receipt.object, row.object);
        self.verify_row(row);
        receipt
    }
    pub(super) fn verify_row(&self, row: &Row) {
        let read = self
            .store(row.key)
            .read(physical_key(row.key), row.object, 0, None)
            .unwrap();
        assert_eq!(read.key, physical_key(row.key));
        assert_eq!(read.object, row.object);
        assert_eq!(read.bytes, row.bytes);
    }
    pub(super) fn observe(&mut self, stage: u64, row: &Row, receipt: WriteReceipt) {
        assert_eq!(receipt.key, physical_key(row.key));
        assert_eq!(receipt.object, row.object);
        self.verify_row(row);
        self.state
            .observe(stage, receipt.write, row.key, object(row.object), true)
            .unwrap();
    }
    pub(super) fn write_all(&mut self, graph: &Graph, stage: u64) {
        for (index, row) in graph.rows.iter().enumerate() {
            let receipt = self.finish_write(graph, stage, index);
            self.observe(stage, row, receipt);
        }
    }
    pub(super) fn capture(&self, graph: &Graph) -> Capture {
        let catalogue = graph.rows.iter().map(Row::binding).collect::<Vec<_>>();
        Capture {
            job: Preparation::new(
                self.store(graph.result().key).clone(),
                self.state.catalogue_revision(),
                &catalogue,
                graph.result().object,
                &graph.retained,
                ClosureLimits::default(),
            )
            .unwrap(),
            fences: self.state.fences(&graph.closure()).unwrap(),
        }
    }
    pub(super) fn prepare(&mut self, graph: &Graph, stage: u64) {
        let evidence = self.capture(graph).finish(graph, stage);
        self.state.prepare(stage, evidence).unwrap();
    }
    pub(super) fn read_grant(&self, grant: u64, row: &Row) {
        assert!(self.state.can_read(grant, &row.member()));
        assert!(self.state.protected_keys().contains(&row.key));
        self.verify_row(row);
    }
    pub(super) fn read_operation(&self, operation: u64, row: &Row) {
        let operation = &self.state.operations()[&operation];
        let stage = self.state.accepted()[&operation.publication.unwrap()].stage;
        assert!(
            self.state.stages()[&stage]
                .candidate
                .closure
                .contains(&row.member())
        );
        let retention = self.state.retention_view();
        let root = &retention.roots[&r::RootId(operation.root.unwrap())];
        assert_eq!(root.kind, r::RootKind::Operation);
        assert!(root.until > self.state.scheduler_view().now);
        let handles = self.state.fences(&BTreeSet::from([row.member()])).unwrap();
        assert_eq!(handles.len(), 1);
        assert!(root.closure.contains(&r::Key {
            incarnation: *handles.keys().next().unwrap(),
            generation: row.key.generation,
        }));
        assert!(self.state.protected_keys().contains(&row.key));
        self.verify_row(row);
    }
    pub(super) fn reopen(&mut self) {
        let bytes = self.state.snapshot().unwrap();
        let directory = self.base.join("authority");
        let pending = directory.join("checkpoint.pending");
        let path = directory.join("checkpoint");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&pending)
            .unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
        drop(file);
        fs::rename(pending, &path).unwrap();
        File::open(&directory).unwrap().sync_all().unwrap();
        self.stores.clear();
        self.state = PublicationAuthority::restore(&fs::read(path).unwrap()).unwrap();
        assert_eq!(self.state.snapshot().unwrap(), bytes);
        let identities: BTreeSet<_> = self
            .state
            .stages()
            .values()
            .flat_map(|stage| {
                stage
                    .candidate
                    .closure
                    .iter()
                    .map(|member| (member.key.namespace, member.key.incarnation))
            })
            .collect();
        for (namespace, incarnation) in identities {
            let store = PhysicalFilesystem::open(
                &self.namespace_path(namespace),
                Incarnation::new(incarnation).unwrap(),
            )
            .unwrap();
            self.stores.insert((namespace, incarnation), store);
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stores.clear();
        fs::remove_dir_all(&self.base).unwrap();
    }
}
