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

//! Compare the separately frozen publication reference with the actual-State /
//! Coarse composition. Fixture bytes, typed members and expected actions are
//! owned here. Scheduling uses the actual reducer, with explicit ID translation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest, Directory, ExecuteResponse, FileNode, OutputDirectory, Tree,
};
use nativelink_scheduler::authority as a;
use prost::Message as _;
use sha2::{Digest as _, Sha256};

#[allow(
    dead_code,
    reason = "The frozen reference also has its own complete scenario suite."
)]
#[path = "publication_head/reference.rs"]
mod reference;
#[allow(
    dead_code,
    reason = "This adapter exercises a declared common event alphabet."
)]
#[path = "publication_head/types.rs"]
mod types;

#[allow(
    dead_code,
    reason = "Candidate and retention have independent focused suites."
)]
mod publication_candidate {
    #[path = "../retention/coarse.rs"]
    mod coarse;
    pub(crate) mod engine;
    #[path = "../retention/protocol.rs"]
    mod protocol;
    pub(crate) mod types;
}

use publication_candidate::engine::PublicationAuthority;
use publication_candidate::types as c;
use types as r;

const ACTION: &str = "principal=tenant;instance=prod;digest=abc/20;platform=linux;config=one";
const OTHER: &str = "principal=tenant;instance=prod;digest=abc/20;platform=linux;config=two";
const UNCACHED: &str =
    "principal=tenant;instance=prod;digest=uncached/20;platform=linux;config=one";
const NAMESPACE: [u8; 16] = [7; 16];
const INCARNATION: [u8; 16] = [9; 16];
const RESULT_UNTIL: u64 = 80;
// The frozen reference uses publication coverage for worker operation roots.
// Equal deadlines define the common lease alphabet compared in this adapter.
const OPERATION_UNTIL: u64 = RESULT_UNTIL;

fn object(bytes: &[u8], algorithm: r::Algorithm) -> r::Object {
    r::Object {
        algorithm,
        hash: match algorithm {
            r::Algorithm::Sha256 => Sha256::digest(bytes).into(),
            r::Algorithm::Blake3 => *blake3::hash(bytes).as_bytes(),
        },
        bytes: bytes.len() as u64,
    }
}

fn digest(object: &r::Object) -> Digest {
    let mut hash = String::with_capacity(64);
    for byte in object.hash {
        write!(hash, "{byte:02x}").unwrap();
    }
    Digest {
        hash,
        size_bytes: i64::try_from(object.bytes).unwrap(),
    }
}

const fn key(generation: u64) -> r::Key {
    r::Key {
        namespace: NAMESPACE,
        incarnation: INCARNATION,
        generation,
    }
}

const fn candidate_key(key: r::Key) -> c::Key {
    c::Key {
        namespace: key.namespace,
        incarnation: key.incarnation,
        generation: key.generation,
    }
}

const fn candidate_object(object: &r::Object) -> c::Object {
    c::Object {
        algorithm: match object.algorithm {
            r::Algorithm::Sha256 => 1,
            r::Algorithm::Blake3 => 9,
        },
        hash: object.hash,
        bytes: object.bytes,
    }
}

const fn candidate_member(member: &r::Member) -> c::Member {
    c::Member {
        key: candidate_key(member.key),
        object: candidate_object(&member.object),
        role: match member.role {
            r::Role::Result => c::Role::Result,
            r::Role::Blob => c::Role::Blob,
            r::Role::Directory => c::Role::Directory,
            r::Role::Tree => c::Role::Tree,
        },
    }
}

fn scenario(algorithm: r::Algorithm) -> (r::Scenario, r::Facts) {
    let blob = r::Member {
        object: object(b"shared output and stdout", algorithm),
        key: key(1),
        role: r::Role::Blob,
    };
    let tree_bytes = Tree {
        root: Some(Directory {
            files: vec![FileNode {
                name: "file".into(),
                digest: Some(digest(&blob.object)),
                ..FileNode::default()
            }],
            ..Directory::default()
        }),
        ..Tree::default()
    }
    .encode_to_vec();
    let tree = r::Member {
        object: object(&tree_bytes, algorithm),
        key: key(2),
        role: r::Role::Tree,
    };
    let mut candidates = BTreeMap::new();
    for (id, action, source, exit_code) in [
        (
            1,
            ACTION,
            r::Source::Worker {
                execution: 1,
                attempt: r::Attempt {
                    id: 1,
                    worker: 1,
                    incarnation: 10,
                },
            },
            0,
        ),
        (2, ACTION, r::Source::Client { principal: 7 }, 0),
        (
            3,
            ACTION,
            r::Source::Worker {
                execution: 2,
                attempt: r::Attempt {
                    id: 2,
                    worker: 1,
                    incarnation: 10,
                },
            },
            0,
        ),
        (
            4,
            UNCACHED,
            r::Source::Worker {
                execution: 3,
                attempt: r::Attempt {
                    id: 3,
                    worker: 1,
                    incarnation: 10,
                },
            },
            0,
        ),
        (
            5,
            UNCACHED,
            r::Source::Worker {
                execution: 4,
                attempt: r::Attempt {
                    id: 4,
                    worker: 1,
                    incarnation: 10,
                },
            },
            7,
        ),
        (6, OTHER, r::Source::Client { principal: 7 }, 0),
        (
            7,
            ACTION,
            r::Source::Worker {
                execution: 5,
                attempt: r::Attempt {
                    id: 5,
                    worker: 1,
                    incarnation: 10,
                },
            },
            7,
        ),
        (
            8,
            ACTION,
            r::Source::Worker {
                execution: 6,
                attempt: r::Attempt {
                    id: 6,
                    worker: 1,
                    incarnation: 10,
                },
            },
            0,
        ),
    ] {
        let result = ActionResult {
            output_directories: vec![OutputDirectory {
                path: "out".into(),
                tree_digest: Some(digest(&tree.object)),
                ..OutputDirectory::default()
            }],
            stdout_digest: Some(digest(&blob.object)),
            stderr_raw: format!("candidate {id}").into_bytes().into(),
            exit_code,
            ..ActionResult::default()
        };
        let bytes = result.encode_to_vec();
        let result_object = object(&bytes, algorithm);
        let result_key = if id == 6 {
            // The numeric generation deliberately aliases candidate 1 while
            // the full physical incarnation remains distinct.
            r::Key {
                incarnation: [10; 16],
                ..key(101)
            }
        } else {
            key(100 + id)
        };
        let response = ExecuteResponse {
            result: Some(result),
            cached_result: false,
            status: (id == 8).then(|| nativelink_proto::google::rpc::Status {
                code: 13,
                message: "captured execution error".into(),
                details: vec![],
            }),
            message: if matches!(source, r::Source::Worker { .. }) {
                format!("source-only diagnostic {id}")
            } else {
                String::new()
            },
            ..ExecuteResponse::default()
        }
        .encode_to_vec();
        candidates.insert(
            id,
            r::Candidate {
                id,
                action: action.into(),
                source,
                result: result_object.clone(),
                bytes,
                response,
                successful: exit_code == 0,
                error_free: id != 8,
                required: BTreeSet::from([
                    blob.clone(),
                    tree.clone(),
                    r::Member {
                        object: result_object,
                        key: result_key,
                        role: r::Role::Result,
                    },
                ]),
            },
        );
    }
    let physical: BTreeMap<_, _> = candidates
        .values()
        .flat_map(|candidate| candidate.required.iter())
        .map(|member| {
            (
                member.key,
                r::Physical {
                    key: member.key,
                    object: member.object.clone(),
                    ready: true,
                    retired: false,
                },
            )
        })
        .collect();
    let facts = r::Facts {
        catalogue_revision: physical.len() as u64,
        physical: physical.into_values().collect(),
        fences: BTreeSet::from([r::Fence {
            namespace: NAMESPACE,
            revision: 0,
        }]),
        receipts: BTreeSet::new(),
    };
    (
        r::Scenario {
            actions: BTreeMap::from([
                (ACTION.into(), false),
                (OTHER.into(), false),
                (UNCACHED.into(), true),
            ]),
            candidates,
        },
        facts,
    )
}

struct Pair {
    expected: reference::Model,
    actual: PublicationAuthority,
    facts: r::Facts,
    position: u64,
    executions: BTreeMap<u64, a::ExecutionId>,
    attempts: BTreeMap<u64, a::AttemptId>,
}

impl Pair {
    fn new(algorithm: r::Algorithm) -> Self {
        let (scenario, facts) = scenario(algorithm);
        Self::from_scenario(scenario, facts)
    }

    fn from_scenario(scenario: r::Scenario, facts: r::Facts) -> Self {
        let mut actual = PublicationAuthority::new(scenario.actions.clone()).unwrap();
        for row in &facts.physical {
            actual
                .install_physical(candidate_key(row.key), candidate_object(&row.object), true)
                .unwrap();
        }
        assert_eq!(actual.catalogue_revision(), facts.catalogue_revision);
        Self {
            expected: reference::Model::new(scenario).unwrap(),
            actual,
            facts,
            position: 0,
            executions: BTreeMap::new(),
            attempts: BTreeMap::new(),
        }
    }

    fn expect(&mut self, event: &r::Event) -> r::Reply {
        self.position += 1;
        let reply = self
            .expected
            .apply(self.position, event, &self.facts)
            .unwrap();
        self.compare();
        reply
    }

    fn register(&mut self) {
        self.actual
            .scheduler_event(a::Event::Register {
                worker: a::WorkerId(1),
                incarnation: a::IncarnationId(10),
                session: a::SessionId(1),
                properties: BTreeMap::new(),
                capacity: a::Resources {
                    cpu_millis: 64,
                    memory_bytes: 64,
                    slots: 64,
                },
                available: a::Resources {
                    cpu_millis: 64,
                    memory_bytes: 64,
                    slots: 64,
                },
            })
            .unwrap();
        self.expect(&r::Event::Register {
            worker: 1,
            incarnation: 10,
        });
    }

    fn authorize(&mut self) {
        self.actual.authorize_client(7).unwrap();
        self.expect(&r::Event::AuthorizeClient(7));
    }

    fn admit(
        &mut self,
        operation: u64,
        action: &str,
        skip: bool,
        proposed_execution: u64,
    ) -> r::Reply {
        let execution = self
            .actual
            .admit(operation, action.into(), skip, OPERATION_UNTIL)
            .unwrap();
        self.position += 1;
        let reply = self
            .expected
            .apply(
                self.position,
                &r::Event::Admit(r::Request {
                    operation,
                    action: action.into(),
                    skip_lookup: skip,
                    execution_if_needed: proposed_execution,
                    result_until: OPERATION_UNTIL,
                }),
                &self.facts,
            )
            .unwrap();
        match reply {
            r::Reply::Started(id) => {
                assert!(self.executions.insert(id, execution).is_none());
            }
            r::Reply::Joined(id) => assert_eq!(execution, self.executions[&id]),
            r::Reply::Hit(_) => {}
            other => panic!("Unexpected admission outcome: {other:?}"),
        }
        self.compare();
        reply
    }

    fn assign(&mut self, execution: u64, attempt: u64) {
        let a::Reply::Matched {
            execution: actual_execution,
            attempt: actual_attempt,
            worker,
        } = self.actual.scheduler_event(a::Event::Match).unwrap()
        else {
            panic!("Actual scheduler did not match");
        };
        assert_eq!(actual_execution, self.executions[&execution]);
        assert_eq!(worker, a::WorkerId(1));
        self.attempts.insert(attempt, actual_attempt);
        self.actual
            .scheduler_event(a::Event::WorkerAccepted {
                attempt: actual_attempt,
                worker,
                incarnation: a::IncarnationId(10),
            })
            .unwrap();
        self.actual
            .scheduler_event(a::Event::Progress {
                attempt: actual_attempt,
                worker,
                incarnation: a::IncarnationId(10),
                phase: a::Phase::Running,
                progress: 1,
            })
            .unwrap();
        self.expect(&r::Event::Assign {
            execution,
            attempt: r::Attempt {
                id: attempt,
                worker: 1,
                incarnation: 10,
            },
        });
    }

    fn candidate(&self, id: u64) -> c::Candidate {
        let row = &self.expected.scenario.candidates[&id];
        c::Candidate {
            action: row.action.clone(),
            publisher: match row.source {
                r::Source::Worker { execution, attempt } => c::Publisher::Worker {
                    execution: self.executions[&execution],
                    attempt: self.attempts[&attempt.id],
                    worker: a::WorkerId(attempt.worker),
                    incarnation: a::IncarnationId(attempt.incarnation),
                },
                r::Source::Client { principal } => c::Publisher::Client { principal },
            },
            result: candidate_object(&row.result),
            result_bytes: row.bytes.clone(),
            response: a::ResultPayload::from_response(
                &ExecuteResponse::decode(row.response.as_slice()).unwrap(),
                c::MAX_PAYLOAD,
            )
            .unwrap(),
            closure: row.required.iter().map(candidate_member).collect(),
        }
    }

    fn stage(&mut self, id: u64) {
        let candidate = self.candidate(id);
        let result_key = candidate
            .closure
            .iter()
            .find(|member| member.role == c::Role::Result)
            .unwrap()
            .key;
        self.actual
            .stage(id, candidate, 8, BTreeMap::from([(1000 + id, result_key)]))
            .unwrap();
        let reference_key = self.expected.scenario.candidates[&id]
            .required
            .iter()
            .find(|member| member.role == r::Role::Result)
            .unwrap()
            .key;
        self.expect(&r::Event::Stage {
            id,
            candidate: id,
            until: 8,
            writes: BTreeMap::from([(1000 + id, reference_key)]),
        });
    }

    fn proof(&self, id: u64) -> r::Preparation {
        let candidate = &self.expected.scenario.candidates[&id];
        r::Preparation {
            id,
            stage: id,
            result: candidate.result.clone(),
            typed: candidate.required.clone(),
            protected: candidate.required.iter().map(|member| member.key).collect(),
            catalogue_revision: self.facts.catalogue_revision,
            fences: self.facts.fences.clone(),
            complete: true,
            verified: true,
            until: RESULT_UNTIL,
        }
    }

    fn prepare(&mut self, id: u64) {
        let proof = self.proof(id);
        let identities: BTreeSet<_> = self
            .facts
            .physical
            .iter()
            .map(|row| (row.key.namespace, row.key.incarnation))
            .collect();
        let handles: BTreeMap<_, _> = identities
            .into_iter()
            .enumerate()
            .map(|(index, identity)| (identity, index as u64 + 1))
            .collect();
        let fences = proof
            .typed
            .iter()
            .map(|member| (handles[&(member.key.namespace, member.key.incarnation)], 0))
            .collect();
        self.actual
            .prepare(
                id,
                c::Evidence {
                    stage: id,
                    catalogue_revision: proof.catalogue_revision,
                    closure: proof.typed.iter().map(candidate_member).collect(),
                    fences,
                    until: RESULT_UNTIL,
                },
            )
            .unwrap();
        self.expect(&r::Event::Prepare {
            stage: id,
            evidence: proof,
        });
    }

    fn publish(&mut self, id: u64) {
        self.actual.publish(id, id, RESULT_UNTIL).unwrap();
        self.expect(&r::Event::Publish {
            stage: id,
            publication: id,
            until: RESULT_UNTIL,
        });
    }

    fn accept(&mut self, id: u64) {
        self.stage(id);
        self.prepare(id);
        self.publish(id);
    }

    fn serve(&mut self, action: &str, publication: u64, grant: u64) {
        self.actual
            .serve(action, publication, self.actual.commit(), grant, 70)
            .unwrap();
        self.expect(&r::Event::Serve {
            action: action.into(),
            expected: publication,
            barrier: self.expected.applied,
            grant,
            until: 70,
        });
    }

    fn late_write(&mut self, id: u64) {
        let candidate = &self.expected.scenario.candidates[&id];
        let receipt = r::Receipt {
            stage: id,
            write: 1000 + id,
            key: key(100 + id),
            object: candidate.result.clone(),
            success: true,
        };
        self.facts.receipts.insert(receipt.clone());
        self.actual
            .observe(
                id,
                receipt.write,
                candidate_key(receipt.key),
                candidate_object(&receipt.object),
                true,
            )
            .unwrap();
        self.expect(&r::Event::Observe(receipt));
    }

    fn revoke(&mut self) {
        self.actual
            .scheduler_event(a::Event::WorkerUnavailable {
                worker: a::WorkerId(1),
                incarnation: a::IncarnationId(10),
                session: a::SessionId(1),
            })
            .unwrap();
        self.expect(&r::Event::Revoke {
            worker: 1,
            incarnation: 10,
        });
    }

    fn recovered(&mut self) {
        self.actual = PublicationAuthority::restore(&self.actual.snapshot().unwrap()).unwrap();
        self.expected =
            reference::Model::restore(&self.expected.snapshot().unwrap(), &self.expected.scenario)
                .unwrap();
        self.compare();
    }

    fn compare(&self) {
        assert_eq!(self.actual.heads(), &self.expected.heads);
        assert_eq!(
            self.actual.operations().len(),
            self.expected.operations.len()
        );
        assert_eq!(self.actual.stages().len(), self.expected.stages.len());
        for (&id, expected) in &self.expected.stages {
            let actual = &self.actual.stages()[&id];
            assert_eq!(actual.candidate, self.candidate(expected.candidate));
            assert_eq!(actual.until, expected.until);
            assert_eq!(actual.outcomes, expected.outcomes);
            assert_eq!(
                actual.writes,
                expected
                    .writes
                    .iter()
                    .map(|(id, key)| (*id, candidate_key(*key)))
                    .collect()
            );
            assert_eq!(
                actual.state,
                match expected.state {
                    r::StageState::Open => c::StageState::Open,
                    r::StageState::Abandoned => c::StageState::Abandoned,
                    r::StageState::Accepted(id) => c::StageState::Accepted(id),
                }
            );
        }
        assert_eq!(
            self.actual.accepted().keys().collect::<Vec<_>>(),
            self.expected.accepted.keys().collect::<Vec<_>>()
        );
        let view = self.actual.scheduler_view();
        assert!(
            !view
                .effects
                .iter()
                .any(|row| matches!(row.kind, a::EffectKind::PublishResult { .. })),
            "Mutable-sink effect escaped atomic publication"
        );
        for (&id, expected) in &self.expected.operations {
            let actual = &self.actual.operations()[&id];
            let terminal = &view
                .operations
                .iter()
                .find(|row| row.id == a::OperationId(id))
                .unwrap()
                .terminal;
            match &expected.state {
                r::OperationState::Waiting(execution) => {
                    assert_eq!(actual.execution, self.executions[execution]);
                    assert!(actual.publication.is_none());
                    assert!(terminal.is_none());
                }
                r::OperationState::Canceled(_) => {
                    assert!(actual.publication.is_none());
                    assert_eq!(*terminal, Some(a::TerminalResult::Cancelled));
                }
                r::OperationState::Terminal(expected) => {
                    assert_eq!(actual.publication, Some(expected.publication));
                    assert_eq!(actual.cached, expected.cached);
                    let Some(a::TerminalResult::Completed(payload)) = terminal else {
                        panic!("Missing actual captured terminal");
                    };
                    let response = payload.require_response().unwrap();
                    if expected.cached {
                        // The frozen model records the source response opaquely.
                        // Cache-hit wire projection is checked independently here.
                        let result = ActionResult::decode(
                            self.expected.scenario.candidates[&expected.publication]
                                .bytes
                                .as_slice(),
                        )
                        .unwrap();
                        let cached = ExecuteResponse {
                            result: Some(result),
                            cached_result: true,
                            ..ExecuteResponse::default()
                        }
                        .encode_to_vec();
                        assert_eq!(response.encoded(), cached);
                    } else {
                        assert_eq!(response.encoded(), expected.response);
                    }
                }
            }
        }
        for (&publication, expected) in &self.expected.accepted {
            let actual = &self.actual.accepted()[&publication];
            assert_eq!(actual.eligible, expected.eligible);
            assert_eq!(actual.stage, expected.stage);
            assert_eq!(actual.until, expected.until);
        }
        let mut actual_order: Vec<_> = self
            .actual
            .accepted()
            .iter()
            .map(|(id, row)| (row.commit, *id))
            .collect();
        let mut expected_order: Vec<_> = self
            .expected
            .accepted
            .iter()
            .map(|(id, row)| (row.commit, *id))
            .collect();
        actual_order.sort_unstable();
        expected_order.sort_unstable();
        assert_eq!(
            actual_order.iter().map(|(_, id)| id).collect::<Vec<_>>(),
            expected_order.iter().map(|(_, id)| id).collect::<Vec<_>>()
        );
        let protected: BTreeSet<_> = self
            .expected
            .protected()
            .into_iter()
            .map(candidate_key)
            .collect();
        assert_eq!(self.actual.protected_keys(), protected);
        for (&grant, expected) in &self.expected.grants {
            assert_eq!(
                self.actual.grants()[&grant].publication,
                expected.publication
            );
            assert_eq!(self.actual.grants()[&grant].until, expected.until);
            for member in &self.expected.scenario.candidates[&expected.publication].required {
                assert_eq!(
                    self.actual.can_read(grant, &candidate_member(member)),
                    self.expected.can_read(grant, member)
                );
                let mut wrong = member.clone();
                wrong.object.hash[0] ^= 1;
                assert!(!self.actual.can_read(grant, &candidate_member(&wrong)));
                assert!(!self.expected.can_read(grant, &wrong));
            }
        }
    }
}

#[test]
fn worker_stage_cannot_consume_credit_for_another_qualified_action() {
    let (mut scenario, facts) = scenario(r::Algorithm::Sha256);
    scenario.candidates.get_mut(&1).unwrap().action = OTHER.into();
    let mut pair = Pair::from_scenario(scenario, facts);
    pair.register();
    pair.admit(1, ACTION, true, 1);
    pair.assign(1, 1);
    let actual_before = pair.actual.snapshot().unwrap();
    let expected_before = pair.expected.snapshot().unwrap();
    pair.position += 1;
    assert!(
        pair.expected
            .apply(
                pair.position,
                &r::Event::Stage {
                    id: 1,
                    candidate: 1,
                    until: 8,
                    writes: BTreeMap::new(),
                },
                &pair.facts
            )
            .is_err()
    );
    assert_eq!(pair.expected.snapshot().unwrap(), expected_before);
    assert!(
        pair.actual
            .stage(1, pair.candidate(1), 8, BTreeMap::new())
            .is_err()
    );
    assert_eq!(pair.actual.snapshot().unwrap(), actual_before);
}

#[test]
fn composed_publication_keeps_terminal_and_serving_coverage_after_direct_replacement() {
    for algorithm in [r::Algorithm::Sha256, r::Algorithm::Blake3] {
        for recover_at in 0..=7 {
            let mut pair = Pair::new(algorithm);
            pair.register();
            pair.authorize();
            assert_eq!(pair.admit(1, ACTION, true, 1), r::Reply::Started(1));
            pair.assign(1, 1);
            for step in 0..7 {
                if step == recover_at {
                    pair.recovered();
                }
                match step {
                    0 => pair.stage(1),
                    1 => pair.prepare(1),
                    2 => pair.publish(1),
                    3 => pair.serve(ACTION, 1, 1),
                    4 => pair.accept(2),
                    5 => pair.late_write(1),
                    6 => pair.revoke(),
                    _ => unreachable!(),
                }
            }
            pair.recovered();
            pair.actual.lose_index();
            pair.expect(&r::Event::LoseIndex);
            pair.actual.rebuild_index();
            pair.expect(&r::Event::RebuildIndex);
            assert_eq!(pair.actual.index(), &pair.expected.index);
            assert_eq!(pair.actual.heads()[ACTION], 2);
            for now in [41, 70, 71, 80, 81] {
                pair.actual.advance(now).unwrap();
                pair.expect(&r::Event::Advance(now));
                pair.recovered();
                pair.actual.rebuild_index();
                pair.expect(&r::Event::RebuildIndex);
                assert_eq!(pair.actual.index(), &pair.expected.index);
            }
        }
    }
}

#[test]
fn composed_request_matrix_keeps_cache_hits_skip_lookup_and_noncacheable_work_distinct() {
    for algorithm in [r::Algorithm::Sha256, r::Algorithm::Blake3] {
        let mut pair = Pair::new(algorithm);
        pair.register();
        pair.authorize();
        pair.admit(1, ACTION, true, 1);
        pair.assign(1, 1);
        pair.accept(1);
        assert_eq!(pair.admit(2, ACTION, false, 2), r::Reply::Hit(1));
        assert_eq!(pair.admit(3, ACTION, true, 2), r::Reply::Started(2));
        assert_eq!(pair.admit(4, ACTION, true, 7), r::Reply::Joined(2));
        pair.actual.cancel_joined(4).unwrap();
        pair.expect(&r::Event::CancelJoined(4));
        pair.assign(2, 2);
        pair.accept(3);
        assert_eq!(pair.admit(5, UNCACHED, false, 3), r::Reply::Started(3));
        assert_eq!(pair.admit(6, UNCACHED, false, 4), r::Reply::Started(4));
        pair.assign(3, 3);
        pair.assign(4, 4);
        pair.accept(4);
        pair.accept(5);
        pair.accept(6);
        assert_eq!(pair.admit(7, ACTION, true, 5), r::Reply::Started(5));
        pair.assign(5, 5);
        pair.accept(7);
        assert_eq!(pair.admit(8, ACTION, true, 6), r::Reply::Started(6));
        pair.assign(6, 6);
        pair.accept(8);
        assert_eq!(pair.admit(9, OTHER, false, 7), r::Reply::Hit(6));
        assert!(!pair.actual.heads().contains_key(UNCACHED));
        assert_eq!(pair.actual.heads()[ACTION], 3);
        assert_eq!(pair.actual.heads()[OTHER], 6);
        pair.recovered();
        for now in [41, 80, 81] {
            pair.actual.advance(now).unwrap();
            pair.expect(&r::Event::Advance(now));
            pair.recovered();
        }
    }
}

#[test]
fn prepared_worker_cannot_publish_after_revocation_or_guard_expiry() {
    for expire in [false, true] {
        let mut pair = Pair::new(r::Algorithm::Blake3);
        pair.register();
        pair.admit(1, ACTION, true, 1);
        pair.assign(1, 1);
        pair.stage(1);
        pair.prepare(1);
        if expire {
            pair.actual.advance(9).unwrap();
            pair.expect(&r::Event::Advance(9));
        } else {
            pair.revoke();
        }
        let actual = pair.actual.snapshot().unwrap();
        let expected = pair.expected.snapshot().unwrap();
        assert!(pair.actual.publish(1, 1, RESULT_UNTIL).is_err());
        pair.position += 1;
        assert!(
            pair.expected
                .apply(
                    pair.position,
                    &r::Event::Publish {
                        stage: 1,
                        publication: 1,
                        until: RESULT_UNTIL
                    },
                    &pair.facts
                )
                .is_err()
        );
        assert_eq!(pair.actual.snapshot().unwrap(), actual);
        assert_eq!(pair.expected.snapshot().unwrap(), expected);
        pair.recovered();
    }
}

#[test]
fn stale_head_and_unprepared_publication_refuse_without_partial_acceptance() {
    let mut pair = Pair::new(r::Algorithm::Sha256);
    pair.register();
    pair.authorize();
    pair.admit(1, ACTION, true, 1);
    pair.assign(1, 1);
    pair.stage(1);
    let before = pair.actual.snapshot().unwrap();
    assert!(pair.actual.publish(1, 1, RESULT_UNTIL).is_err());
    assert_eq!(pair.actual.snapshot().unwrap(), before);
    pair.position += 1;
    assert!(
        pair.expected
            .apply(
                pair.position,
                &r::Event::Publish {
                    stage: 1,
                    publication: 1,
                    until: RESULT_UNTIL
                },
                &pair.facts
            )
            .is_err()
    );
    pair.prepare(1);
    pair.publish(1);
    pair.accept(2);
    let before = pair.actual.snapshot().unwrap();
    assert!(
        pair.actual
            .serve(ACTION, 1, pair.actual.commit(), 1, 70)
            .is_err()
    );
    assert_eq!(pair.actual.snapshot().unwrap(), before);
    pair.position += 1;
    assert!(
        pair.expected
            .apply(
                pair.position,
                &r::Event::Serve {
                    action: ACTION.into(),
                    expected: 1,
                    barrier: pair.expected.applied,
                    grant: 1,
                    until: 70
                },
                &pair.facts
            )
            .is_err()
    );
    pair.compare();
    pair.recovered();
}
