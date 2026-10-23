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

//! Test-only extraction from actual retained authority state.
//! Physical evidence and namespace/identity bindings are explicit external inputs.

use std::collections::{BTreeMap, BTreeSet};

use nativelink_proto::build::bazel::remote::execution::v2::ExecuteResponse;
use nativelink_scheduler::authority::accepted_history::{
    AcceptedHistory, HistoryAlgorithm, HistoryStatus,
};
use nativelink_scheduler::authority::{
    ActionKey, EffectKind, ExecutionId, ObservableState, Request, ResourceFailureReason,
    ResultPayload, State, TerminalResult,
};
use nativelink_store::physical_closure::{self, Binding, Preparation, VerifiedReader};
use nativelink_store::physical_filesystem::{Algorithm, ObjectId, PhysicalKey};
use prost::Message as _;

use super::types as model;

type Result<T> = std::result::Result<T, String>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SourceInventory {
    pub(crate) snapshot: Vec<u8>,
    pub(crate) view: ObservableState,
    pub(crate) history: Option<AcceptedHistory>,
    pub(crate) history_bytes: BTreeMap<u64, Vec<u8>>,
}

impl SourceInventory {
    pub(crate) fn capture(state: &State) -> Result<Self> {
        let snapshot = state.snapshot()?;
        let history = state.accepted_history().cloned();
        let history_bytes = history
            .as_ref()
            .map(|history| {
                history
                    .associations
                    .values()
                    .map(|association| {
                        state
                            .history_object_bytes(association.effect)
                            .map(|bytes| (association.effect.0, bytes))
                    })
                    .collect::<Result<BTreeMap<_, _>>>()
            })
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            snapshot,
            view: state.view(),
            history,
            history_bytes,
        })
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum Destination {
    /// Complete externally qualified identity, not inferred from the action instance.
    Sink(String),
    Cas([u8; 32]),
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct AuthorityDomain {
    pub(crate) principal: String,
    pub(crate) configuration: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct QualifiedAction {
    pub(crate) action: ActionKey,
    pub(crate) authority: AuthorityDomain,
    pub(crate) properties: BTreeMap<String, String>,
}

impl QualifiedAction {
    pub(crate) fn of(request: &Request, authority: AuthorityDomain) -> Self {
        Self {
            action: request.action.clone(),
            authority,
            properties: request.properties.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Context {
    pub(crate) old_sink: Destination,
    pub(crate) result_destination: Destination,
    pub(crate) namespace_labels: BTreeMap<model::Namespace, Destination>,
    pub(crate) action_labels: BTreeMap<String, QualifiedAction>,
    pub(crate) authorities: BTreeMap<ExecutionId, AuthorityDomain>,
}

impl Context {
    fn validate(&self) -> Result<()> {
        if !matches!(&self.old_sink, Destination::Sink(identity) if !identity.is_empty())
            || !matches!(self.result_destination, Destination::Cas(_))
            || self
                .namespace_labels
                .values()
                .collect::<BTreeSet<_>>()
                .len()
                != self.namespace_labels.len()
            || self.action_labels.values().collect::<BTreeSet<_>>().len()
                != self.action_labels.len()
            || self
                .action_labels
                .keys()
                .any(|label| label.is_empty() || label.len() > 256)
            || self
                .authorities
                .values()
                .any(|domain| domain.principal.is_empty() || domain.configuration.is_empty())
        {
            return Err("missing or nonbijective trusted identity binding".into());
        }
        self.namespace(&self.old_sink)?;
        self.namespace(&self.result_destination)?;
        Ok(())
    }

    fn namespace(&self, destination: &Destination) -> Result<model::Namespace> {
        self.namespace_labels
            .iter()
            .find_map(|(label, original)| (original == destination).then_some(*label))
            .ok_or_else(|| "missing complete destination binding".into())
    }

    fn action(&self, execution: ExecutionId, request: &Request) -> Result<String> {
        let authority = self
            .authorities
            .get(&execution)
            .ok_or("missing explicit authority domain")?;
        if request
            .payload
            .as_ref()
            .is_some_and(|payload| payload.principal() != authority.principal)
        {
            return Err("captured principal contradicts trusted action domain".into());
        }
        let key = QualifiedAction::of(request, authority.clone());
        self.action_labels
            .iter()
            .find_map(|(label, original)| (original == &key).then_some(label.clone()))
            .ok_or_else(|| "missing exact qualified action binding".into())
    }
}

#[derive(Clone)]
pub(crate) struct ReadDomain<R> {
    pub(crate) reader: R,
    pub(crate) catalogue_revision: u64,
    pub(crate) catalogue: Vec<Binding>,
}

fn algorithm(request: &Request) -> Result<Algorithm> {
    match request.action.digest_function.as_str() {
        "sha256" => Ok(Algorithm::Sha256),
        "blake3" => Ok(Algorithm::Blake3),
        _ => Err("unsupported original digest algorithm".into()),
    }
}

const fn object(value: ObjectId) -> model::Object {
    model::Object {
        algorithm: match value.algorithm {
            Algorithm::Sha256 => model::Algorithm::Sha256,
            Algorithm::Blake3 => model::Algorithm::Blake3,
        },
        hash: value.hash,
        bytes: value.size,
    }
}

const fn generation(namespace: model::Namespace, key: PhysicalKey) -> model::Generation {
    model::Generation {
        namespace,
        incarnation: key.incarnation.as_bytes(),
        number: key.generation(),
    }
}

fn payload(source: &ResultPayload, algorithm: Algorithm) -> Result<model::Payload> {
    let response = source.require_response()?;
    let decoded: ExecuteResponse = response.decode()?;
    if decoded.encode_to_vec() != response.encoded() {
        return Err("unsupported noncanonical captured response projection".into());
    }
    let captured =
        serde_json::from_value(serde_json::to_value(response).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    let result = decoded.result.map(|result| {
        let bytes = result.encode_to_vec();
        model::ResultGraph {
            object: object(ObjectId::for_bytes(algorithm, &bytes)),
            bytes,
        }
    });
    Ok(model::Payload {
        digest: source.digest.clone(),
        exit_code: source.exit_code,
        error: source.error.clone(),
        response: Some(captured),
        result,
    })
}

fn terminal(source: &TerminalResult, algorithm: Algorithm) -> Result<model::TerminalValue> {
    Ok(match source {
        TerminalResult::Completed(result) => {
            model::TerminalValue::Completed(payload(result, algorithm)?)
        }
        TerminalResult::ResourceFailed { result, reason } => model::TerminalValue::ResourceFailed {
            result: payload(result, algorithm)?,
            reason: match reason {
                ResourceFailureReason::AttemptBudgetExhausted => {
                    model::ResourceFailureReason::AttemptBudgetExhausted
                }
                ResourceFailureReason::MissingGrowthEvidence => {
                    model::ResourceFailureReason::MissingGrowthEvidence
                }
                ResourceFailureReason::NoLargerEligibleWorker => {
                    model::ResourceFailureReason::NoLargerEligibleWorker
                }
            },
        },
        TerminalResult::Cancelled => model::TerminalValue::Cancelled,
        TerminalResult::Exhausted => model::TerminalValue::Exhausted,
    })
}

fn result_members<R: VerifiedReader>(
    graph: &model::ResultGraph,
    context: &Context,
    reads: &BTreeMap<Destination, ReadDomain<R>>,
) -> Result<BTreeSet<model::Member>> {
    let domain = reads
        .get(&context.result_destination)
        .ok_or("missing result preparation domain")?;
    let namespace = context.namespace(&context.result_destination)?;
    let id = ObjectId {
        algorithm: match graph.object.algorithm {
            model::Algorithm::Sha256 => Algorithm::Sha256,
            model::Algorithm::Blake3 => Algorithm::Blake3,
        },
        hash: graph.object.hash,
        size: graph.object.bytes,
    };
    let catalogue: Vec<_> = domain
        .catalogue
        .iter()
        .copied()
        .filter(|binding| binding.object.algorithm == id.algorithm)
        .collect();
    let mut preparation = Preparation::new(
        domain.reader.clone(),
        domain.catalogue_revision,
        &catalogue,
        id,
        &[],
        physical_closure::Limits::default(),
    )
    .map_err(|error| format!("result preparation: {error:?}"))?;
    while !preparation
        .advance(32)
        .map_err(|error| format!("result traversal: {error:?}"))?
    {}
    let closure = preparation
        .finish()
        .map_err(|error| format!("incomplete result traversal: {error:?}"))?;
    if closure.action_result != id {
        return Err("prepared result changed identity".into());
    }
    Ok(closure
        .objects
        .into_iter()
        .flat_map(|prepared| {
            prepared.roles.into_iter().map(move |role| model::Member {
                object: object(prepared.binding.object),
                generation: generation(namespace, prepared.binding.key),
                role: match role {
                    physical_closure::Role::ActionResult => model::Role::Result,
                    physical_closure::Role::Blob => model::Role::Blob,
                    physical_closure::Role::Directory => model::Role::Directory,
                    physical_closure::Role::Tree => model::Role::Tree,
                },
            })
        })
        .collect())
}

fn append_root(
    legacy: &mut model::Legacy,
    subject: model::Subject,
    root: model::Object,
    required: BTreeSet<model::Member>,
) -> Result<()> {
    let id = u64::try_from(legacy.roots.len())
        .map_err(|error| error.to_string())?
        .checked_add(1)
        .ok_or("root counter overflow")?;
    legacy.roots.insert(
        id,
        model::Root {
            id,
            subject,
            object: root,
            required,
        },
    );
    Ok(())
}

pub(crate) fn project<R: VerifiedReader>(
    source: &SourceInventory,
    context: &Context,
    reads: &BTreeMap<Destination, ReadDomain<R>>,
) -> Result<model::Legacy> {
    let restored = State::restore(&source.snapshot)?;
    if SourceInventory::capture(&restored)? != *source {
        return Err("source inventory disagrees with original State snapshot".into());
    }
    context.validate()?;
    for domain in reads.values() {
        if domain.catalogue_revision == 0
            || domain
                .catalogue
                .iter()
                .map(|binding| binding.key)
                .collect::<BTreeSet<_>>()
                .len()
                != domain.catalogue.len()
        {
            return Err("ambiguous physical catalogue".into());
        }
    }
    let executions: BTreeMap<_, _> = source
        .view
        .executions
        .iter()
        .map(|execution| (execution.id, execution))
        .collect();
    let associations = source.history.as_ref().map(|history| &history.associations);
    let mut legacy = model::Legacy {
        old_sink: context.namespace(&context.old_sink)?,
        domain_exposure: model::Exposure::Opaque,
        terminals: BTreeMap::new(),
        operations: BTreeMap::new(),
        effects: BTreeMap::new(),
        histories: BTreeMap::new(),
        roots: BTreeMap::new(),
        late_outcomes: BTreeSet::new(),
        request_quiescence: BTreeSet::new(),
    };
    for execution in &source.view.executions {
        let Some(value) = &execution.terminal else {
            continue;
        };
        let value = terminal(value, algorithm(&execution.request)?)?;
        let provenance = associations
            .and_then(|associations| associations.get(&execution.id))
            .map_or(model::Provenance::Unrecorded, |history| {
                model::Provenance::RecordedWorkerAcceptance(model::Origin {
                    epoch: history.epoch.0,
                    accepted_commit: None,
                    effect: Some(history.effect.0),
                    execution: history.execution.0,
                    attempt: history.attempt.0,
                    worker: history.worker.0,
                    incarnation: history.incarnation.0,
                })
            });
        let ac_eligible = !execution.request.do_not_cache
            && matches!(&value,model::TerminalValue::Completed(payload) if payload.exit_code==0 && payload.error.is_none() && payload.result.is_some());
        if let Some(graph) = value.result() {
            append_root(
                &mut legacy,
                model::Subject::ExecutionResult(execution.id.0),
                graph.object.clone(),
                result_members(graph, context, reads)?,
            )?;
        }
        legacy.terminals.insert(
            execution.id.0,
            model::ExecutionTerminal {
                id: execution.id.0,
                action: context.action(execution.id, &execution.request)?,
                provenance,
                value,
                terminal_at: execution.terminal_at,
                ac_eligible,
            },
        );
    }
    for operation in &source.view.operations {
        let Some(value) = &operation.terminal else {
            continue;
        };
        let execution = executions
            .get(&operation.execution)
            .ok_or("retained operation lost execution request")?;
        context.action(operation.execution, &execution.request)?;
        let value = terminal(value, algorithm(&execution.request)?)?;
        if let Some(graph) = value.result() {
            append_root(
                &mut legacy,
                model::Subject::OperationResult(operation.id.0),
                graph.object.clone(),
                result_members(graph, context, reads)?,
            )?;
        }
        legacy.operations.insert(
            operation.id.0,
            model::OperationTerminal {
                id: operation.id.0,
                execution: operation.execution.0,
                value,
                provenance: model::OperationProvenance::Unrecorded,
                version: operation.version,
                interested: operation.interested,
                interest_deadline: operation.interest_deadline.at,
            },
        );
    }
    for effect in &source.view.effects {
        if let EffectKind::PublishResult {
            execution, result, ..
        } = &effect.kind
        {
            let execution_row = executions
                .get(execution)
                .ok_or("sink intent lost execution")?;
            let projected = payload(result, algorithm(&execution_row.request)?)?;
            let graph = projected.result.ok_or("sink intent has no ActionResult")?;
            legacy.effects.insert(
                effect.id.0,
                model::SinkEffect {
                    id: effect.id.0,
                    terminal: execution.0,
                    destination: legacy.old_sink,
                    exposure: model::Exposure::Opaque,
                    payload: graph.bytes,
                    requests: BTreeMap::new(),
                },
            );
        }
    }
    if let Some(history) = &source.history {
        for association in history.associations.values() {
            let destination = Destination::Cas(association.recipe.object.namespace);
            let label = context.namespace(&destination)?;
            let domain = reads
                .get(&destination)
                .ok_or("missing explicit history preparation domain")?;
            let original = association.recipe.object;
            let id = ObjectId {
                algorithm: match original.algorithm {
                    HistoryAlgorithm::Sha256 => Algorithm::Sha256,
                    HistoryAlgorithm::Blake3 => Algorithm::Blake3,
                },
                hash: original.hash,
                size: original.size_bytes,
            };
            let matches: Vec<_> = domain
                .catalogue
                .iter()
                .filter(|binding| binding.object == id)
                .collect();
            if matches.len() != 1 {
                return Err("missing or ambiguous history object binding".into());
            }
            let binding = matches[0];
            let bytes = source
                .history_bytes
                .get(&association.effect.0)
                .ok_or("retained history payload missing")?;
            let verified = domain
                .reader
                .read(binding.key, id, 0, None)
                .map_err(|error| format!("history verification: {error}"))?;
            if verified.key != binding.key
                || verified.object != id
                || verified.offset != 0
                || verified.bytes != *bytes
                || ObjectId::for_bytes(id.algorithm, bytes) != id
            {
                return Err("history payload or physical identity changed".into());
            }
            let origin = model::Origin {
                epoch: association.epoch.0,
                accepted_commit: None,
                effect: Some(association.effect.0),
                execution: association.execution.0,
                attempt: association.attempt.0,
                worker: association.worker.0,
                incarnation: association.incarnation.0,
            };
            let object = object(id);
            append_root(
                &mut legacy,
                model::Subject::History(association.effect.0),
                object.clone(),
                BTreeSet::from([model::Member {
                    object: object.clone(),
                    generation: generation(label, binding.key),
                    role: model::Role::History,
                }]),
            )?;
            legacy.histories.insert(
                association.effect.0,
                model::HistoryEffect {
                    id: association.effect.0,
                    terminal: association.execution.0,
                    origin,
                    destination: label,
                    exposure: model::Exposure::Opaque,
                    object,
                    payload: bytes.clone(),
                    requests: BTreeMap::new(),
                    materialized: association.status == HistoryStatus::Materialized,
                },
            );
        }
    }
    Ok(legacy)
}
