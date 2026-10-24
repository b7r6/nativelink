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

#[path = "activation_inventory/adapter.rs"]
mod adapter;
#[path = "publication_activation_v2/bounded.rs"]
mod bounded;
#[path = "publication_activation_v2/reference.rs"]
mod reference;
#[path = "publication_activation_v2/types.rs"]
mod types;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;

use adapter::{
    AuthorityDomain, Context, Destination, QualifiedAction, ReadDomain, SourceInventory,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, Command, Digest, Directory, DirectoryNode, ExecuteRequest,
    ExecuteResponse, ExecutionPolicy, FileNode, OutputDirectory, OutputFile, Tree,
};
use nativelink_proto::google::rpc::Status;
use nativelink_scheduler::authority::accepted_history::HistoryOutcome;
use nativelink_scheduler::authority::request::RequestPayload;
use nativelink_scheduler::authority::{
    self as authority, ActionKey, ClusterEpoch, EffectKind, Event, ExecutionId, IncarnationId,
    Limits, OperationId, RecordedEvent, Reply, Request, Resources, ResultPayload, SessionId, State,
    TerminalResult, WorkerId,
};
use nativelink_store::physical_closure::{Binding, VerifiedReader};
use nativelink_store::physical_filesystem::{
    Algorithm, Failure, FailureKind, Incarnation, ObjectId, PhysicalKey, VerifiedRead,
};
use prost::Message as _;
use prost_types::Timestamp;
use sha2::{Digest as _, Sha256};
use types as model;

const CAS: [u8; 32] = [31; 32];
const HISTORY: [u8; 32] = [41; 32];
const OLD: model::Namespace = model::Namespace([1; 16]);
const NEW: model::Namespace = model::Namespace([2; 16]);
const CAS_LABEL: model::Namespace = model::Namespace([3; 16]);
const HISTORY_LABEL: model::Namespace = model::Namespace([4; 16]);
const GRANT: Resources = Resources {
    cpu_millis: 100,
    memory_bytes: 1024,
    slots: 1,
};

#[derive(Clone, Default)]
struct Reader(Arc<BTreeMap<PhysicalKey, (ObjectId, Vec<u8>)>>);
impl VerifiedReader for Reader {
    fn read(
        &self,
        key: PhysicalKey,
        object: ObjectId,
        offset: u64,
        length: Option<u64>,
    ) -> Result<VerifiedRead, Failure> {
        let fail = |kind| Failure {
            kind,
            operation: "inventory fixture verified read",
            io_kind: None,
        };
        let (stored, bytes) = self.0.get(&key).ok_or_else(|| fail(FailureKind::Missing))?;
        if *stored != object || id(bytes, object.algorithm) != object {
            return Err(fail(FailureKind::Corrupt));
        }
        let start = usize::try_from(offset).map_err(|_| fail(FailureKind::Invalid))?;
        let length = length
            .map_or_else(|| Ok(bytes.len().saturating_sub(start)), usize::try_from)
            .map_err(|_| fail(FailureKind::Invalid))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| fail(FailureKind::Invalid))?;
        let output = bytes
            .get(start..end)
            .ok_or_else(|| fail(FailureKind::Invalid))?
            .to_vec();
        Ok(VerifiedRead {
            key,
            object,
            offset,
            bytes: output,
        })
    }
}
fn id(bytes: &[u8], algorithm: Algorithm) -> ObjectId {
    ObjectId {
        algorithm,
        hash: match algorithm {
            Algorithm::Sha256 => Sha256::digest(bytes).into(),
            Algorithm::Blake3 => *blake3::hash(bytes).as_bytes(),
        },
        size: bytes.len() as u64,
    }
}
const fn descriptor(object: ObjectId) -> model::Object {
    model::Object {
        algorithm: match object.algorithm {
            Algorithm::Sha256 => model::Algorithm::Sha256,
            Algorithm::Blake3 => model::Algorithm::Blake3,
        },
        hash: object.hash,
        bytes: object.size,
    }
}
fn digest(object: ObjectId) -> Digest {
    let mut hash = String::new();
    for byte in object.hash {
        write!(&mut hash, "{byte:02x}").unwrap();
    }
    Digest {
        hash,
        size_bytes: i64::try_from(object.size).unwrap(),
    }
}

fn key(number: u64) -> PhysicalKey {
    PhysicalKey::new(Incarnation::new([7; 16]).unwrap(), number).unwrap()
}
const fn member(
    namespace: model::Namespace,
    number: u64,
    object: ObjectId,
    role: model::Role,
) -> model::Member {
    model::Member {
        object: descriptor(object),
        generation: model::Generation {
            namespace,
            incarnation: [7; 16],
            number,
        },
        role,
    }
}

struct Assets {
    response: ExecuteResponse,
    bytes: BTreeMap<PhysicalKey, (ObjectId, Vec<u8>)>,
    expected: BTreeSet<model::Member>,
    algorithm: Algorithm,
}
impl Assets {
    fn new(algorithm: Algorithm, cached: bool) -> Self {
        let blob = b"opaque file and stdout bytes".to_vec();
        let blob_id = id(&blob, algorithm);
        let leaf = Directory {
            files: vec![FileNode {
                name: "f".into(),
                digest: Some(digest(blob_id)),
                ..FileNode::default()
            }],
            ..Directory::default()
        };
        let embedded = id(&leaf.encode_to_vec(), algorithm);
        let tree = Tree {
            root: Some(Directory {
                directories: vec![DirectoryNode {
                    name: "child".into(),
                    digest: Some(digest(embedded)),
                }],
                ..Directory::default()
            }),
            children: vec![leaf],
        }
        .encode_to_vec();
        let tree_id = id(&tree, algorithm);
        let result = ActionResult {
            output_files: vec![OutputFile {
                path: "file".into(),
                digest: Some(digest(blob_id)),
                ..OutputFile::default()
            }],
            output_directories: vec![OutputDirectory {
                path: "out".into(),
                tree_digest: Some(digest(tree_id)),
                is_topologically_sorted: true,
            }],
            stdout_digest: Some(digest(blob_id)),
            stderr_raw: b"inline stderr remains inline".to_vec().into(),
            ..ActionResult::default()
        };
        let result_bytes = result.encode_to_vec();
        let result_id = id(&result_bytes, algorithm);
        let bytes = BTreeMap::from([
            (key(1), (blob_id, blob)),
            (key(2), (tree_id, tree)),
            (key(3), (result_id, result_bytes)),
            (key(4), (id(b"unrelated", algorithm), b"unrelated".to_vec())),
        ]);
        let expected = BTreeSet::from([
            member(CAS_LABEL, 1, blob_id, model::Role::Blob),
            member(CAS_LABEL, 2, tree_id, model::Role::Tree),
            member(CAS_LABEL, 3, result_id, model::Role::Result),
        ]);
        Self {
            response: ExecuteResponse {
                result: Some(result),
                cached_result: cached,
                message: if cached {
                    "cached terminal"
                } else {
                    "worker terminal"
                }
                .into(),
                ..ExecuteResponse::default()
            },
            bytes,
            expected,
            algorithm,
        }
    }
    fn resultless(algorithm: Algorithm) -> Self {
        Self {
            response: ExecuteResponse {
                result: None,
                status: Some(Status {
                    code: 13,
                    message: "exact worker failure".into(),
                    details: vec![],
                }),
                message: "diagnostic without ActionResult".into(),
                ..ExecuteResponse::default()
            },
            bytes: BTreeMap::new(),
            expected: BTreeSet::new(),
            algorithm,
        }
    }
}

struct Harness {
    state: State,
    next_operation: u64,
}
impl Harness {
    fn new(history: bool) -> Self {
        let mut result = Self {
            state: State::new(ClusterEpoch(7), Limits::default()).unwrap(),
            next_operation: 1,
        };
        result.send(Event::Register {
            worker: WorkerId(9),
            incarnation: IncarnationId(11),
            session: SessionId(13),
            properties: BTreeMap::new(),
            capacity: Resources {
                cpu_millis: 6400,
                memory_bytes: 65536,
                slots: 64,
            },
            available: Resources {
                cpu_millis: 6400,
                memory_bytes: 65536,
                slots: 64,
            },
        });
        if history {
            result.state.enable_accepted_history(HISTORY).unwrap();
        }
        result
    }
    fn send(&mut self, event: Event) -> Reply {
        let reply = self.state.apply(RecordedEvent {
            epoch: ClusterEpoch(7),
            event,
        });
        assert!(
            !matches!(reply, Reply::Rejected(_)),
            "unexpected rejection {reply:?}"
        );
        self.state.check_invariants().unwrap();
        reply
    }
    fn submit(&mut self, request: Request) -> (ExecutionId, OperationId) {
        let operation = OperationId(self.next_operation);
        self.next_operation += 1;
        let Reply::Submitted {
            execution,
            operation: returned,
        } = self.send(Event::Submit { operation, request })
        else {
            panic!("submit reply")
        };
        assert_eq!(returned, operation);
        (execution, operation)
    }
    fn matched(&mut self) -> authority::AttemptId {
        let Reply::Matched { attempt, .. } = self.send(Event::Match) else {
            panic!("match reply")
        };
        assert_eq!(
            self.send(Event::WorkerAccepted {
                attempt,
                worker: WorkerId(9),
                incarnation: IncarnationId(11)
            }),
            Reply::Applied
        );
        attempt
    }
    fn complete(&mut self, response: &ExecuteResponse) {
        let attempt = self.matched();
        let result = ResultPayload::from_response(response, 64 * 1024).unwrap();
        assert_eq!(
            self.send(Event::Complete {
                attempt,
                worker: WorkerId(9),
                incarnation: IncarnationId(11),
                result
            }),
            Reply::Applied
        );
    }
    fn cached(&mut self, execution: ExecutionId, response: &ExecuteResponse) {
        let effect = self
            .state
            .view()
            .effects
            .iter()
            .find_map(|effect| {
                matches!(effect.kind,EffectKind::Lookup {execution:id,..} if id==execution)
                    .then_some(effect.id)
            })
            .unwrap();
        assert_eq!(
            self.send(Event::LookupResult {
                effect,
                result: Some(ResultPayload::from_response(response, 64 * 1024).unwrap())
            }),
            Reply::Applied
        );
    }
}
fn request(algorithm: Algorithm, key: u8, skip: bool) -> Request {
    Request {
        action: ActionKey {
            instance: "instance".into(),
            digest_function: match algorithm {
                Algorithm::Sha256 => "sha256",
                Algorithm::Blake3 => "blake3",
            }
            .into(),
            digest: format!("{key:02x}").repeat(32),
            digest_size_bytes: u64::from(key),
        },
        execution_scope: None,
        do_not_cache: false,
        skip_cache_lookup: skip,
        priority: 0,
        properties: BTreeMap::new(),
        resources: GRANT,
        execution_timeout: 100,
        payload: None,
    }
}

fn inputs(
    source: &SourceInventory,
    assets: &Assets,
) -> (Context, BTreeMap<Destination, ReadDomain<Reader>>) {
    let authority = AuthorityDomain {
        principal: "explicit authenticated principal/domain".into(),
        configuration: "explicit execution configuration".into(),
    };
    let mut action_labels = BTreeMap::new();
    for execution in &source.view.executions {
        let qualified = QualifiedAction {
            action: execution.request.action.clone(),
            authority: authority.clone(),
            properties: execution.request.properties.clone(),
        };
        if !action_labels.values().any(|value| value == &qualified) {
            action_labels.insert(format!("action-{}", action_labels.len() + 1), qualified);
        }
    }
    let mut context = Context {
        old_sink: Destination::Sink("qualified old endpoint/container/prefix/domain".into()),
        result_destination: Destination::Cas(CAS),
        namespace_labels: BTreeMap::from([
            (
                OLD,
                Destination::Sink("qualified old endpoint/container/prefix/domain".into()),
            ),
            (CAS_LABEL, Destination::Cas(CAS)),
            (HISTORY_LABEL, Destination::Cas(HISTORY)),
        ]),
        action_labels,
        authorities: source
            .view
            .executions
            .iter()
            .map(|execution| (execution.id, authority.clone()))
            .collect(),
    };
    if let Some(history) = &source.history {
        context.namespace_labels.remove(&HISTORY_LABEL);
        if history.namespace != CAS {
            context
                .namespace_labels
                .insert(HISTORY_LABEL, Destination::Cas(history.namespace));
        }
    }
    let catalogue = assets
        .bytes
        .iter()
        .map(|(&key, (object, _))| Binding {
            key,
            object: *object,
        })
        .collect();
    let mut reads = BTreeMap::from([(
        Destination::Cas(CAS),
        ReadDomain {
            reader: Reader(Arc::new(assets.bytes.clone())),
            catalogue_revision: 3,
            catalogue,
        },
    )]);
    if let Some(history) = &source.history {
        let mut bytes = BTreeMap::new();
        for association in history.associations.values() {
            let raw = &source.history_bytes[&association.effect.0];
            let payload = source
                .view
                .executions
                .iter()
                .find(|execution| execution.id == association.execution)
                .unwrap()
                .terminal
                .as_ref()
                .unwrap();
            let captured = match payload {
                TerminalResult::Completed(payload)
                | TerminalResult::ResourceFailed {
                    result: payload, ..
                } => payload.response.as_ref().unwrap().encoded(),
                _ => panic!("history needs captured terminal"),
            };
            assert_eq!(
                raw,
                [association.recipe.prefix.as_slice(), captured]
                    .concat()
                    .as_slice()
            );
            bytes.insert(
                key(100 + association.effect.0),
                (id(raw, assets.algorithm), raw.clone()),
            );
        }
        if let Some(existing) = reads.get(&Destination::Cas(history.namespace)) {
            for (key, value) in existing.reader.0.iter() {
                assert!(bytes.insert(*key, value.clone()).is_none());
            }
        }
        let catalogue = bytes
            .iter()
            .map(|(&key, (object, _))| Binding {
                key,
                object: *object,
            })
            .collect();
        reads.insert(
            Destination::Cas(history.namespace),
            ReadDomain {
                reader: Reader(Arc::new(bytes)),
                catalogue_revision: 3,
                catalogue,
            },
        );
    }
    (context, reads)
}

fn expected_value(response: &ExecuteResponse, algorithm: Algorithm) -> model::TerminalValue {
    let captured = response.encode_to_vec();
    let result = ResultPayload::from_response(response, 64 * 1024).unwrap();
    model::TerminalValue::Completed(model::Payload {
        digest: result.digest,
        exit_code: result.exit_code,
        error: result.error,
        response: Some(model::CapturedResponse {
            version: 1,
            sha256: Sha256::digest(&captured).into(),
            encoded: captured,
        }),
        result: response.result.as_ref().map(|result| {
            let bytes = result.encode_to_vec();
            model::ResultGraph {
                object: descriptor(id(&bytes, algorithm)),
                bytes,
            }
        }),
    })
}

fn activation_facts(
    legacy: &model::Legacy,
    expected: &BTreeMap<model::Subject, BTreeSet<model::Member>>,
) -> (model::Facts, model::Activation) {
    let mut catalogue = BTreeMap::new();
    let mut protections = BTreeMap::new();
    let mut fences = BTreeSet::new();
    for (root_id, root) in &legacy.roots {
        let required = &expected[&root.subject];
        assert_eq!(
            &root.required, required,
            "independent expected protocol closure"
        );
        for member in required {
            catalogue.insert(member.generation, member.object.clone());
            fences.insert(model::Fence {
                namespace: member.generation.namespace,
                revision: 3,
            });
        }
        protections.insert(
            *root_id,
            model::Protection {
                id: *root_id,
                root: *root_id,
                catalogue_revision: 3,
                complete: true,
                verified: true,
                typed: required.clone(),
                protected: required.iter().map(|member| member.generation).collect(),
                fences: required
                    .iter()
                    .map(|member| model::Fence {
                        namespace: member.generation.namespace,
                        revision: 3,
                    })
                    .collect(),
                until: 1000,
            },
        );
    }
    assert_eq!(
        legacy.roots.len(),
        expected.len(),
        "no omitted or invented owner root"
    );
    let facts = model::Facts {
        now: 100,
        catalogue_revision: 3,
        catalogue: catalogue
            .into_iter()
            .map(|(key, object)| model::CatalogueRow {
                key,
                object,
                state: model::Readiness::Ready,
            })
            .collect(),
        fences,
        protections,
        bindings: [OLD, NEW, CAS_LABEL, HISTORY_LABEL]
            .into_iter()
            .map(|namespace| model::Binding {
                namespace,
                physical_domain: [namespace.0[0]; 32],
                old_requests_confined: namespace == OLD,
            })
            .collect(),
        cold: BTreeSet::from([NEW]),
        ..model::Facts::default()
    };
    let activation = model::Activation {
        commit: 100,
        new_namespace: NEW,
        dispositions: legacy
            .effects
            .keys()
            .map(|id| (*id, model::Disposition::Retired))
            .collect(),
        imports: vec![],
        protections: legacy.roots.keys().map(|id| (*id, *id)).collect(),
    };
    (facts, activation)
}

fn expected_roots(
    source: &SourceInventory,
    assets: &Assets,
) -> BTreeMap<model::Subject, BTreeSet<model::Member>> {
    let mut roots = BTreeMap::new();
    for execution in &source.view.executions {
        if execution.terminal.is_some() && !assets.expected.is_empty() {
            roots.insert(
                model::Subject::ExecutionResult(execution.id.0),
                assets.expected.clone(),
            );
        }
    }
    for operation in &source.view.operations {
        if matches!(
            operation.terminal,
            Some(TerminalResult::Completed(_) | TerminalResult::ResourceFailed { .. })
        ) && !assets.expected.is_empty()
        {
            roots.insert(
                model::Subject::OperationResult(operation.id.0),
                assets.expected.clone(),
            );
        }
    }
    if let Some(history) = &source.history {
        for association in history.associations.values() {
            let raw = &source.history_bytes[&association.effect.0];
            roots.insert(
                model::Subject::History(association.effect.0),
                BTreeSet::from([member(
                    if history.namespace == CAS {
                        CAS_LABEL
                    } else {
                        HISTORY_LABEL
                    },
                    100 + association.effect.0,
                    id(raw, assets.algorithm),
                    model::Role::History,
                )]),
            );
        }
    }
    roots
}

fn qualify(harness: &Harness, assets: &Assets) -> (SourceInventory, model::Legacy) {
    let source = SourceInventory::capture(&harness.state).unwrap();
    assert_eq!(source.view, harness.state.view());
    assert_eq!(source.history.as_ref(), harness.state.accepted_history());
    assert_eq!(
        SourceInventory::capture(&State::restore(&source.snapshot).unwrap()).unwrap(),
        source
    );
    let (context, reads) = inputs(&source, assets);
    let legacy = adapter::project(&source, &context, &reads).unwrap();
    let expected = expected_roots(&source, assets);
    let (facts, activation) = activation_facts(&legacy, &expected);
    let mut model = bounded::Model::new(legacy.clone()).unwrap();
    model
        .apply(&model::Event::Activate(activation), &facts)
        .unwrap();
    assert_eq!(model.view().legacy, legacy);
    assert_eq!(model.view().heads, Vec::<types::Head>::new());
    assert!(model.view().unresolved_legacy_domain());
    assert!(model.view().unresolved_requests().is_empty());
    assert_eq!(
        bounded::Model::restore(&model.snapshot().unwrap(), &legacy).unwrap(),
        model
    );
    assert!(!model.view().may_collect(OLD, 100));
    assert_eq!(
        model.view().unresolved_effects().len(),
        legacy.effects.len() + legacy.histories.len()
    );
    let pending: BTreeSet<_> = legacy
        .histories
        .values()
        .filter(|history| !history.materialized)
        .map(|history| history.id)
        .collect();
    assert_eq!(model.view().pending_history(), pending);
    (source, legacy)
}

#[test]
fn actual_cache_hit_keeps_unknown_origin_exact_response_and_typed_owner_roots() {
    for algorithm in [Algorithm::Sha256, Algorithm::Blake3] {
        let assets = Assets::new(algorithm, true);
        let mut h = Harness::new(true);
        let (execution, operation) = h.submit(request(algorithm, 1, false));
        h.cached(execution, &assets.response);
        let (source, legacy) = qualify(&h, &assets);
        assert_eq!(
            legacy.terminals[&execution.0].provenance,
            model::Provenance::Unrecorded
        );
        assert_eq!(
            legacy.operations[&operation.0].value,
            expected_value(&assets.response, algorithm)
        );
        assert_eq!(
            legacy.terminals[&execution.0].value,
            expected_value(&assets.response, algorithm)
        );
        assert!(legacy.histories.is_empty());
        assert!(legacy.effects.is_empty());
        assert_eq!(source.view.executions[0].attempt, None);
        assert_eq!(legacy.roots.len(), 2);
        assert!(legacy.roots.values().all(|root| root.required.len() == 3));
    }
}

#[test]
fn actual_worker_and_canceled_joined_operation_preserve_independent_terminals_and_history() {
    let assets = Assets::new(Algorithm::Sha256, false);
    let mut h = Harness::new(true);
    let (execution, canceled) = h.submit(request(assets.algorithm, 2, true));
    let (joined, interested) = h.submit(request(assets.algorithm, 2, true));
    assert_eq!(joined, execution);
    h.send(Event::Cancel {
        operation: canceled,
    });
    h.complete(&assets.response);
    let (source, legacy) = qualify(&h, &assets);
    assert_eq!(
        legacy.operations[&canceled.0].value,
        model::TerminalValue::Cancelled
    );
    assert_eq!(
        legacy.operations[&interested.0].value,
        expected_value(&assets.response, assets.algorithm)
    );
    let association = &source.history.as_ref().unwrap().associations[&execution];
    assert_eq!(
        legacy.terminals[&execution.0].provenance,
        model::Provenance::RecordedWorkerAcceptance(model::Origin {
            epoch: 7,
            accepted_commit: None,
            effect: Some(association.effect.0),
            execution: execution.0,
            attempt: association.attempt.0,
            worker: 9,
            incarnation: 11
        })
    );
    assert_eq!(
        legacy.operations[&canceled.0].provenance,
        model::OperationProvenance::Unrecorded
    );
    assert!(h.state.history_for_operation(canceled).is_none());
    assert_eq!(legacy.roots.len(), 3);
}

#[test]
fn actual_resultless_worker_failure_retains_response_and_history_without_fake_graph() {
    let assets = Assets::resultless(Algorithm::Blake3);
    let mut h = Harness::new(true);
    let (execution, operation) = h.submit(request(assets.algorithm, 3, true));
    h.complete(&assets.response);
    let (_, legacy) = qualify(&h, &assets);
    assert_eq!(
        legacy.terminals[&execution.0].value,
        expected_value(&assets.response, assets.algorithm)
    );
    assert_eq!(
        legacy.operations[&operation.0].value,
        expected_value(&assets.response, assets.algorithm)
    );
    assert!(!legacy.terminals[&execution.0].ac_eligible);
    assert!(legacy.effects.is_empty());
    assert_eq!(legacy.roots.len(), 1);
    assert!(matches!(
        legacy.roots.values().next().unwrap().subject,
        model::Subject::History(_)
    ));
}

#[test]
fn actual_final_memory_failure_without_action_result_keeps_reason_and_history() {
    let mut assets = Assets::resultless(Algorithm::Sha256);
    assets.response.status.as_mut().unwrap().code = 8;
    let mut h = Harness::new(true);
    let mut zero_memory = request(assets.algorithm, 4, true);
    zero_memory.resources.memory_bytes = 0;
    let (execution, operation) = h.submit(zero_memory);
    let attempt = h.matched();
    let charged = h
        .state
        .view()
        .attempts
        .iter()
        .find(|row| row.id == attempt)
        .map(|row| {
            h.state
                .view()
                .reservations
                .iter()
                .find(|reservation| reservation.id == row.reservation)
                .unwrap()
                .charged
        })
        .unwrap();
    h.send(Event::MemoryFailure {
        attempt,
        worker: WorkerId(9),
        incarnation: IncarnationId(11),
        charged,
        peak_memory_bytes: None,
        quiescent: true,
        result: ResultPayload::from_response(&assets.response, 64 * 1024).unwrap(),
    });
    let (_, legacy) = qualify(&h, &assets);
    let model::TerminalValue::Completed(payload) =
        expected_value(&assets.response, assets.algorithm)
    else {
        panic!()
    };
    let expected = model::TerminalValue::ResourceFailed {
        result: payload,
        reason: model::ResourceFailureReason::MissingGrowthEvidence,
    };
    assert_eq!(legacy.terminals[&execution.0].value, expected);
    assert_eq!(legacy.operations[&operation.0].value, expected);
    assert_eq!(legacy.roots.len(), 1);
    assert!(legacy.effects.is_empty());
}

#[test]
fn materialized_history_still_preserves_opaque_older_requests_and_stable_operations() {
    let assets = Assets::new(Algorithm::Blake3, false);
    let mut h = Harness::new(true);
    let (execution, _) = h.submit(request(assets.algorithm, 5, true));
    h.complete(&assets.response);
    let (before, initial) = qualify(&h, &assets);
    let association = before.history.as_ref().unwrap().associations[&execution].clone();
    h.send(Event::HistoryPublication {
        effect: association.effect,
        object: association.recipe.object,
        outcome: HistoryOutcome::Unknown,
    });
    h.send(Event::HistoryPublication {
        effect: association.effect,
        object: association.recipe.object,
        outcome: HistoryOutcome::Stored,
    });
    let (after, materialized) = qualify(&h, &assets);
    assert_eq!(after.view.operations, before.view.operations);
    assert_eq!(after.history_bytes, before.history_bytes);
    assert_eq!(materialized.operations, initial.operations);
    let effect = &materialized.histories[&association.effect.0];
    assert!(effect.materialized);
    assert_eq!(effect.exposure, model::Exposure::Opaque);
    assert!(effect.requests.is_empty());
    assert_eq!(
        effect.payload,
        initial.histories[&association.effect.0].payload
    );
}

#[test]
fn worker_terminal_without_recorded_history_keeps_unknown_origin_and_original_metadata() {
    let assets = Assets::new(Algorithm::Sha256, false);
    let mut h = Harness::new(false);
    let (execution, operation) = h.submit(request(assets.algorithm, 6, true));
    h.complete(&assets.response);
    let (source, legacy) = qualify(&h, &assets);
    assert_eq!(
        legacy.terminals[&execution.0].provenance,
        model::Provenance::Unrecorded
    );
    assert_eq!(
        legacy.operations[&operation.0].value,
        expected_value(&assets.response, assets.algorithm)
    );
    assert!(legacy.histories.is_empty());
    assert_eq!(legacy.effects.len(), 1);
    let raw = &source.view.operations[0];
    assert_eq!(legacy.operations[&operation.0].version, raw.version);
    assert_eq!(
        legacy.operations[&operation.0].interest_deadline,
        raw.interest_deadline.at
    );
    assert!(raw.interest_deadline.generation > 0);
    assert_eq!(source.snapshot, h.state.snapshot().unwrap());
    assert_eq!(
        source.view,
        h.state.view(),
        "full priority and deadline generation stay in source inventory"
    );
}

#[test]
fn qualified_identity_uses_complete_authority_action_and_semantic_properties() {
    let authority = AuthorityDomain {
        principal: "principal".into(),
        configuration: "configuration".into(),
    };
    let original = request(Algorithm::Sha256, 7, true);
    let expected = QualifiedAction {
        action: original.action.clone(),
        authority: authority.clone(),
        properties: original.properties.clone(),
    };
    let mut unrelated = original.clone();
    unrelated.execution_scope = Some("another invocation".into());
    unrelated.priority = 99;
    unrelated.skip_cache_lookup = false;
    unrelated.do_not_cache = true;
    unrelated.resources.memory_bytes *= 2;
    unrelated.execution_timeout *= 2;
    assert_eq!(QualifiedAction::of(&unrelated, authority.clone()), expected);
    for field in 0..7 {
        let mut changed = original.clone();
        let mut domain = authority.clone();
        match field {
            0 => changed.action.instance.push_str("/other"),
            1 => changed.action.digest_function = "blake3".into(),
            2 => changed.action.digest = "08".repeat(32),
            3 => changed.action.digest_size_bytes += 1,
            4 => domain.principal.push_str("/other"),
            5 => domain.configuration.push_str("/other"),
            6 => {
                changed.properties.insert("os".into(), "linux".into());
            }
            _ => unreachable!(),
        }
        assert_ne!(QualifiedAction::of(&changed, domain), expected);
    }
    let assets = Assets::new(Algorithm::Sha256, false);
    let mut h = Harness::new(false);
    let mut first = original.clone();
    first.execution_scope = Some("one".into());
    let mut second = original;
    second.execution_scope = Some("two".into());
    let (a, _) = h.submit(first);
    let (b, _) = h.submit(second);
    assert_ne!(a, b);
    h.complete(&assets.response);
    h.complete(&assets.response);
    let (source, legacy) = qualify(&h, &assets);
    assert_eq!(legacy.terminals[&a.0].action, legacy.terminals[&b.0].action);
    assert_ne!(
        source.view.executions[0].request.execution_scope,
        source.view.executions[1].request.execution_scope
    );
}

#[test]
fn complete_destination_bijections_and_external_old_sink_binding_are_required() {
    let assets = Assets::new(Algorithm::Sha256, false);
    let mut h = Harness::new(true);
    h.submit(request(assets.algorithm, 8, true));
    h.complete(&assets.response);
    let source = SourceInventory::capture(&h.state).unwrap();
    let (context, reads) = inputs(&source, &assets);
    for mutation in 0..6 {
        let mut bad = context.clone();
        match mutation {
            0 => {
                bad.namespace_labels.remove(&OLD);
            }
            1 => {
                let mut other = HISTORY;
                other[31] ^= 1;
                bad.namespace_labels
                    .insert(HISTORY_LABEL, Destination::Cas(other));
            }
            2 => {
                bad.namespace_labels
                    .insert(model::Namespace([99; 16]), Destination::Cas(HISTORY));
            }
            3 => {
                bad.authorities.clear();
            }
            4 => {
                bad.action_labels
                    .values_mut()
                    .next()
                    .unwrap()
                    .authority
                    .principal
                    .push_str("/wrong");
            }
            5 => {
                bad.old_sink = Destination::Sink(String::new());
            }
            _ => unreachable!(),
        }
        assert!(
            adapter::project(&source, &bad, &reads).is_err(),
            "binding mutation {mutation}"
        );
        assert_eq!(
            SourceInventory::capture(&State::restore(&source.snapshot).unwrap()).unwrap(),
            source
        );
    }
}

#[test]
fn missing_preparation_or_retired_physical_facts_block_activation_without_loss() {
    let assets = Assets::new(Algorithm::Sha256, false);
    let mut h = Harness::new(true);
    h.submit(request(assets.algorithm, 9, true));
    h.complete(&assets.response);
    let source = SourceInventory::capture(&h.state).unwrap();
    let (context, reads) = inputs(&source, &assets);
    let mut missing = reads.clone();
    missing
        .get_mut(&Destination::Cas(CAS))
        .unwrap()
        .catalogue
        .retain(|binding| binding.key != key(1));
    assert!(adapter::project(&source, &context, &missing).is_err());
    let mut missing_history = reads.clone();
    missing_history.remove(&Destination::Cas(HISTORY));
    assert!(adapter::project(&source, &context, &missing_history).is_err());
    let legacy = adapter::project(&source, &context, &reads).unwrap();
    let (mut facts, activation) = activation_facts(&legacy, &expected_roots(&source, &assets));
    facts.catalogue[0].state = model::Readiness::Retired;
    let mut checked = bounded::Model::new(legacy).unwrap();
    let before = checked.snapshot().unwrap();
    assert_eq!(
        checked.apply(&model::Event::Activate(activation), &facts),
        Err(model::Reject::Unprotected)
    );
    assert_eq!(checked.snapshot().unwrap(), before);
    assert_eq!(source.snapshot, h.state.snapshot().unwrap());
}

#[test]
fn unsupported_original_wire_encoding_is_captured_losslessly_and_projection_refuses() {
    let assets = Assets::new(Algorithm::Sha256, false);
    let mut h = Harness::new(true);
    let (execution, _) = h.submit(request(assets.algorithm, 10, true));
    let attempt = h.matched();
    let mut payload = ResultPayload::from_response(&assets.response, 64 * 1024).unwrap();
    let mut raw = payload.response.as_ref().unwrap().encoded().to_vec();
    raw.extend_from_slice(&[0xa0, 0x06, 0x01]);
    let mut response = serde_json::to_value(payload.response.as_ref().unwrap()).unwrap();
    response["encoded"] = serde_json::to_value(&raw).unwrap();
    response["sha256"] = serde_json::to_value(<[u8; 32]>::from(Sha256::digest(&raw))).unwrap();
    payload.response = Some(serde_json::from_value(response).unwrap());
    payload.digest = payload.response.as_ref().unwrap().fingerprint();
    h.send(Event::Complete {
        attempt,
        worker: WorkerId(9),
        incarnation: IncarnationId(11),
        result: payload,
    });
    let source = SourceInventory::capture(&h.state).unwrap();
    let (context, reads) = inputs(&source, &assets);
    let terminal = source
        .view
        .executions
        .iter()
        .find(|row| row.id == execution)
        .unwrap()
        .terminal
        .as_ref()
        .unwrap();
    let TerminalResult::Completed(result) = terminal else {
        panic!()
    };
    assert_eq!(result.response.as_ref().unwrap().encoded(), raw);
    assert!(
        adapter::project(&source, &context, &reads)
            .unwrap_err()
            .contains("noncanonical")
    );
    assert_eq!(
        SourceInventory::capture(&State::restore(&source.snapshot).unwrap()).unwrap(),
        source
    );
    assert_eq!(source.snapshot, h.state.snapshot().unwrap());
}

#[test]
fn actual_attempt_budget_resource_failure_retains_result_graph_and_final_attempt_origin() {
    let mut assets = Assets::new(Algorithm::Sha256, false);
    assets.response.result.as_mut().unwrap().exit_code = 137;
    assets.response.status = Some(Status {
        code: 8,
        message: "explicit memory failure".into(),
        details: vec![],
    });
    let bytes = assets.response.result.as_ref().unwrap().encode_to_vec();
    let result_id = id(&bytes, assets.algorithm);
    assets.bytes.insert(key(3), (result_id, bytes));
    assets
        .expected
        .retain(|member| member.role != model::Role::Result);
    assets
        .expected
        .insert(member(CAS_LABEL, 3, result_id, model::Role::Result));
    let mut h = Harness::new(true);
    let (execution, operation) = h.submit(request(assets.algorithm, 11, true));
    let mut final_attempt = None;
    for index in 0..authority::MAX_ATTEMPTS {
        let attempt = h.matched();
        final_attempt = Some(attempt);
        let view = h.state.view();
        let reservation = view
            .attempts
            .iter()
            .find(|row| row.id == attempt)
            .unwrap()
            .reservation;
        let charged = view
            .reservations
            .iter()
            .find(|row| row.id == reservation)
            .unwrap()
            .charged;
        assert_eq!(
            h.send(Event::MemoryFailure {
                attempt,
                worker: WorkerId(9),
                incarnation: IncarnationId(11),
                charged,
                peak_memory_bytes: Some(charged.memory_bytes),
                quiescent: true,
                result: ResultPayload::from_response(&assets.response, 64 * 1024).unwrap()
            }),
            Reply::Applied
        );
        if index + 1 < authority::MAX_ATTEMPTS {
            let source = SourceInventory::capture(&h.state).unwrap();
            assert!(source.history.as_ref().unwrap().associations.is_empty());
            let (context, reads) = inputs(&source, &assets);
            let projected = adapter::project(&source, &context, &reads).unwrap();
            assert!(projected.terminals.is_empty());
            assert!(projected.operations.is_empty());
            assert!(projected.roots.is_empty());
            h.state = State::restore(&source.snapshot).unwrap();
        }
    }
    let (_, legacy) = qualify(&h, &assets);
    let model::TerminalValue::Completed(payload) =
        expected_value(&assets.response, assets.algorithm)
    else {
        panic!()
    };
    let expected = model::TerminalValue::ResourceFailed {
        result: payload,
        reason: model::ResourceFailureReason::AttemptBudgetExhausted,
    };
    assert_eq!(legacy.terminals[&execution.0].value, expected);
    assert_eq!(legacy.operations[&operation.0].value, expected);
    let model::Provenance::RecordedWorkerAcceptance(origin) =
        legacy.terminals[&execution.0].provenance
    else {
        panic!()
    };
    assert_eq!(origin.attempt, final_attempt.unwrap().0);
    assert_eq!(legacy.roots.len(), 3);
    assert!(legacy.effects.is_empty());
}

#[test]
fn real_snapshot_and_suffix_cuts_preserve_inventory_through_history_outcomes() {
    let assets = Assets::new(Algorithm::Sha256, false);
    let result = ResultPayload::from_response(&assets.response, 64 * 1024).unwrap();
    let mut events = vec![
        Event::Submit {
            operation: OperationId(1),
            request: request(assets.algorithm, 12, true),
        },
        Event::Match,
        Event::WorkerAccepted {
            attempt: authority::AttemptId(1),
            worker: WorkerId(9),
            incarnation: IncarnationId(11),
        },
        Event::Complete {
            attempt: authority::AttemptId(1),
            worker: WorkerId(9),
            incarnation: IncarnationId(11),
            result,
        },
    ];
    let mut uninterrupted = Harness::new(true);
    for event in &events {
        uninterrupted.send(event.clone());
    }
    let association =
        uninterrupted.state.accepted_history().unwrap().associations[&ExecutionId(1)].clone();
    for outcome in [HistoryOutcome::Unknown, HistoryOutcome::Stored] {
        let event = Event::HistoryPublication {
            effect: association.effect,
            object: association.recipe.object,
            outcome,
        };
        uninterrupted.send(event.clone());
        events.push(event);
    }
    let expected = qualify(&uninterrupted, &assets);
    for cut in 0..=events.len() {
        let mut replay = Harness::new(true);
        for event in &events[..cut] {
            replay.send(event.clone());
        }
        let source = SourceInventory::capture(&replay.state).unwrap();
        replay.state = State::restore(&source.snapshot).unwrap();
        assert_eq!(SourceInventory::capture(&replay.state).unwrap(), source);
        for event in &events[cut..] {
            replay.send(event.clone());
        }
        assert_eq!(qualify(&replay, &assets), expected, "real-State cut {cut}");
    }
}

#[test]
fn projection_rejects_changed_inventory_fields_against_the_real_state_snapshot() {
    let assets = Assets::new(Algorithm::Sha256, false);
    let mut h = Harness::new(true);
    h.submit(request(assets.algorithm, 13, true));
    h.complete(&assets.response);
    let source = SourceInventory::capture(&h.state).unwrap();
    let (context, reads) = inputs(&source, &assets);
    for mutation in 0..5 {
        let mut changed = source.clone();
        match mutation {
            0 => changed.view.operations[0].version += 1,
            1 => changed.view.operations[0].interest_deadline.generation += 1,
            2 => {
                changed.history_bytes.values_mut().next().unwrap().push(7);
            }
            3 => {
                changed.view.effects.clear();
            }
            4 => {
                changed.operation_requests.clear();
            }
            _ => unreachable!(),
        }
        assert!(
            adapter::project(&changed, &context, &reads)
                .unwrap_err()
                .contains("snapshot")
        );
        assert_eq!(changed.snapshot, source.snapshot);
    }
}

#[test]
fn canceled_operation_inventory_does_not_require_an_invented_execution_terminal() {
    let assets = Assets::new(Algorithm::Sha256, false);
    let mut h = Harness::new(true);
    let (execution, canceled) = h.submit(request(assets.algorithm, 14, true));
    let (joined, _) = h.submit(request(assets.algorithm, 14, true));
    assert_eq!(execution, joined);
    h.send(Event::Cancel {
        operation: canceled,
    });
    let source = SourceInventory::capture(&h.state).unwrap();
    let (context, reads) = inputs(&source, &assets);
    let projected = adapter::project(&source, &context, &reads).unwrap();
    assert!(projected.terminals.is_empty());
    assert_eq!(projected.operations.len(), 1);
    assert_eq!(
        projected.operations[&canceled.0].value,
        model::TerminalValue::Cancelled
    );
    assert!(projected.roots.is_empty());
    assert!(projected.histories.is_empty());
    assert_eq!(source.view.executions[0].terminal, None);
    let mut absent = context;
    absent.authorities.clear();
    assert!(adapter::project(&source, &absent, &reads).is_err());
    assert_eq!(source.snapshot, h.state.snapshot().unwrap());
}

#[test]
fn history_and_result_roles_share_one_actual_physical_namespace() {
    let assets = Assets::new(Algorithm::Sha256, false);
    let mut h = Harness::new(false);
    h.state.enable_accepted_history(CAS).unwrap();
    h.submit(request(assets.algorithm, 15, true));
    h.complete(&assets.response);
    let (source, legacy) = qualify(&h, &assets);
    assert_eq!(source.history.as_ref().unwrap().namespace, CAS);
    assert_eq!(legacy.roots.len(), 3);
    assert!(
        legacy
            .roots
            .values()
            .flat_map(|root| &root.required)
            .all(|member| {
                member.generation.namespace == CAS_LABEL && member.generation.incarnation == [7; 16]
            })
    );
    let (mut context, reads) = inputs(&source, &assets);
    assert_eq!(reads.len(), 1);
    assert_eq!(reads[&Destination::Cas(CAS)].catalogue.len(), 5);
    assert_eq!(context.namespace_labels.len(), 2);
    context
        .namespace_labels
        .insert(HISTORY_LABEL, Destination::Cas(CAS));
    assert!(
        adapter::project(&source, &context, &reads)
            .unwrap_err()
            .contains("nonbijective")
    );
}

#[test]
fn captured_request_principal_cannot_be_overridden_by_external_domain_binding() {
    let assets = Assets::new(Algorithm::Sha256, false);
    let command = Command {
        arguments: vec!["/bin/true".into()],
        ..Command::default()
    }
    .encode_to_vec();
    let action = Action {
        command_digest: Some(digest(id(&command, assets.algorithm))),
        input_root_digest: Some(digest(id(&[], assets.algorithm))),
        ..Action::default()
    }
    .encode_to_vec();
    let execute = ExecuteRequest {
        instance_name: "real/request/instance".into(),
        action_digest: Some(digest(id(&action, assets.algorithm))),
        digest_function: 1,
        skip_cache_lookup: true,
        ..ExecuteRequest::default()
    };
    let payload = RequestPayload::capture(
        &execute,
        None,
        &Timestamp::default(),
        "explicit authenticated principal/domain",
        &action,
        &command,
        64 * 1024,
    )
    .unwrap();
    let request = Request::from_payload(payload.clone()).unwrap();
    let mut h = Harness::new(true);
    h.submit(request);
    h.complete(&assets.response);
    let (source, _) = qualify(&h, &assets);
    assert_eq!(
        source.view.executions[0].request.payload.as_deref(),
        Some(&payload)
    );
    let (mut context, reads) = inputs(&source, &assets);
    for domain in context.authorities.values_mut() {
        domain.principal = "contradictory external principal".into();
    }
    for action in context.action_labels.values_mut() {
        action.authority.principal = "contradictory external principal".into();
    }
    assert!(
        adapter::project(&source, &context, &reads)
            .unwrap_err()
            .contains("captured principal")
    );
    assert_eq!(source.snapshot, h.state.snapshot().unwrap());
}

fn captured_request(principal: &str) -> Request {
    let algorithm = Algorithm::Sha256;
    let command = Command {
        arguments: vec!["/bin/true".into()],
        ..Command::default()
    }
    .encode_to_vec();
    let action = Action {
        command_digest: Some(digest(id(&command, algorithm))),
        input_root_digest: Some(digest(id(&[], algorithm))),
        ..Action::default()
    }
    .encode_to_vec();
    let execute = ExecuteRequest {
        instance_name: "joined/original/request".into(),
        action_digest: Some(digest(id(&action, algorithm))),
        digest_function: 1,
        skip_cache_lookup: true,
        ..ExecuteRequest::default()
    };
    Request::from_payload(
        RequestPayload::capture(
            &execute,
            None,
            &Timestamp::default(),
            principal,
            &action,
            &command,
            64 * 1024,
        )
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn joined_operation_original_principal_cannot_inherit_execution_authority() {
    let assets = Assets::new(Algorithm::Sha256, false);
    let original = captured_request("explicit authenticated principal/domain");
    let foreign = captured_request("foreign retained operation principal");
    assert_eq!(original.action, foreign.action);
    assert_eq!(original.execution_scope, None);
    assert_eq!(foreign.execution_scope, None);
    let mut h = Harness::new(true);
    let (execution, _) = h.submit(original.clone());
    let (joined, operation) = h.submit(foreign.clone());
    assert_eq!(execution, joined);
    h.complete(&assets.response);
    let source = SourceInventory::capture(&h.state).unwrap();
    assert_eq!(source.view.executions[0].request, original);
    let snapshot: serde_json::Value = serde_json::from_slice(&source.snapshot).unwrap();
    assert_eq!(
        snapshot["state"]["operations"][operation.0.to_string()]["original_request"],
        serde_json::to_value(&foreign).unwrap()
    );
    assert_eq!(source.operation_requests[&operation], foreign);
    let (context, reads) = inputs(&source, &assets);
    assert!(
        adapter::project(&source, &context, &reads)
            .unwrap_err()
            .contains("captured principal")
    );
    assert_eq!(
        SourceInventory::capture(&State::restore(&source.snapshot).unwrap()).unwrap(),
        source
    );
    assert_eq!(source.snapshot, h.state.snapshot().unwrap());
}

#[test]
fn joined_operation_keeps_its_original_priority_queue_time_and_lookup_policy() {
    let assets = Assets::new(Algorithm::Sha256, false);
    let original = captured_request("explicit authenticated principal/domain");
    let payload = original.require_payload().unwrap();
    let mut facts = payload.dispatch_facts().unwrap();
    facts.execute_request.skip_cache_lookup = false;
    facts.execute_request.execution_policy = Some(ExecutionPolicy { priority: -9 });
    facts.queued_timestamp.seconds = 123;
    let changed = Request::from_payload(
        RequestPayload::capture(
            &facts.execute_request,
            facts.request_metadata.as_ref(),
            &facts.queued_timestamp,
            payload.principal(),
            payload.action_bytes(),
            payload.command_bytes(),
            64 * 1024,
        )
        .unwrap(),
    )
    .unwrap();
    assert_ne!(original, changed);
    let mut h = Harness::new(true);
    let (execution, first) = h.submit(original.clone());
    let (joined, second) = h.submit(changed.clone());
    assert_eq!(execution, joined);
    h.complete(&assets.response);
    let (source, legacy) = qualify(&h, &assets);
    assert_eq!(source.operation_requests[&first], original);
    assert_eq!(source.operation_requests[&second], changed);
    assert_eq!(source.view.executions[0].request, original);
    assert_eq!(
        legacy.operations[&first.0].value,
        legacy.operations[&second.0].value
    );
}
