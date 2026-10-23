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

//! Activated immutable diagnostic associations for accepted worker terminals.
//! The namespace is a logical authority binding; provider placement and physical
//! protection require separate construction and retention qualification.

use std::collections::{BTreeMap, BTreeSet};

use nativelink_proto::build::bazel::remote::execution::v2::Digest;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use prost::Message;
use serde::{Deserialize, Serialize};

use super::{
    ActionKey, AttemptId, ClusterEpoch, EffectId, ExecutionId, IncarnationId, OperationId,
    PayloadSizeLimit, Rejection, Reply, ResultPayload, State, TerminalResult, WorkerId,
};

pub const MAX_HISTORY_PREFIX_BYTES: usize = 82;
pub(super) const HISTORY_ROW_BYTES: usize = 2048;
pub(super) const HISTORY_HEADER_BYTES: usize = 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum HistoryAlgorithm {
    Sha256,
    Blake3,
}

impl HistoryAlgorithm {
    const fn hasher(self) -> DigestHasherFunc {
        match self {
            Self::Sha256 => DigestHasherFunc::Sha256,
            Self::Blake3 => DigestHasherFunc::Blake3,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryObject {
    pub namespace: [u8; 32],
    pub algorithm: HistoryAlgorithm,
    pub hash: [u8; 32],
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRecipe {
    pub prefix: Vec<u8>,
    pub response_sha256: [u8; 32],
    pub object: HistoryObject,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum HistoryStatus {
    Pending,
    Materialized,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum HistoryOutcome {
    Stored,
    Failed,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryAssociation {
    pub epoch: ClusterEpoch,
    pub effect: EffectId,
    pub execution: ExecutionId,
    pub attempt: AttemptId,
    pub worker: WorkerId,
    pub incarnation: IncarnationId,
    pub recipe: HistoryRecipe,
    pub status: HistoryStatus,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedHistory {
    pub namespace: [u8; 32],
    /// Existing terminals receive no retroactive diagnostic association.
    pub preactivation_terminals: BTreeSet<ExecutionId>,
    pub associations: BTreeMap<ExecutionId, HistoryAssociation>,
}

fn action_digest(action: &ActionKey) -> Result<(HistoryAlgorithm, Digest), String> {
    let algorithm = match action.digest_function.as_str() {
        "sha256" => HistoryAlgorithm::Sha256,
        "blake3" => HistoryAlgorithm::Blake3,
        _ => return Err("history requires a supported action digest algorithm".into()),
    };
    if action.digest.len() != 64
        || !action
            .digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("history requires a canonical 256-bit action digest".into());
    }
    Ok((
        algorithm,
        Digest {
            hash: action.digest.clone(),
            size_bytes: i64::try_from(action.digest_size_bytes)
                .map_err(|_| "history action size exceeds protobuf int64")?,
        },
    ))
}

fn response_bytes(result: &ResultPayload) -> Result<&[u8], String> {
    result
        .response
        .as_ref()
        .map(super::reapi::ResponsePayload::encoded)
        .ok_or_else(|| "accepted history requires captured response bytes".into())
}

fn hash(algorithm: DigestHasherFunc, parts: &[&[u8]]) -> [u8; 32] {
    let mut digest = algorithm.hasher();
    for part in parts {
        digest.update(part);
    }
    **digest.finalize_digest().packed_hash()
}

fn recipe(
    namespace: [u8; 32],
    action: &ActionKey,
    result: &ResultPayload,
) -> Result<HistoryRecipe, String> {
    let (algorithm, digest) = action_digest(action)?;
    let response = response_bytes(result)?;
    if response.len() > super::reapi::MAX_RESPONSE_BYTES {
        return Err("history response exceeds its fixed byte bound".into());
    }
    // Encode only the diagnostic's Digest field with the pinned generated
    // message. The accepted ExecuteResponse bytes are never re-encoded.
    let digest_bytes = digest.encode_to_vec();
    let mut prefix = Vec::with_capacity(MAX_HISTORY_PREFIX_BYTES);
    prost::encoding::encode_key(1, prost::encoding::WireType::LengthDelimited, &mut prefix);
    prost::encoding::encode_varint(digest_bytes.len() as u64, &mut prefix);
    prefix.extend_from_slice(&digest_bytes);
    prost::encoding::encode_key(3, prost::encoding::WireType::LengthDelimited, &mut prefix);
    prost::encoding::encode_varint(response.len() as u64, &mut prefix);
    if prefix.len() > MAX_HISTORY_PREFIX_BYTES {
        return Err("history prefix exceeds its fixed byte bound".into());
    }
    let object = HistoryObject {
        namespace,
        algorithm,
        hash: hash(algorithm.hasher(), &[&prefix, response]),
        size_bytes: (prefix.len() + response.len()) as u64,
    };
    Ok(HistoryRecipe {
        prefix,
        response_sha256: hash(DigestHasherFunc::Sha256, &[response]),
        object,
    })
}

const fn worker_result(terminal: &TerminalResult) -> Option<&ResultPayload> {
    match terminal {
        TerminalResult::Completed(result) | TerminalResult::ResourceFailed { result, .. } => {
            Some(result)
        }
        TerminalResult::Cancelled | TerminalResult::Exhausted => None,
    }
}

impl State {
    /// Pure atomic preparation. The journal must authorize persistent feature
    /// activation; this method alone never grants writer permission.
    pub fn enable_accepted_history(&mut self, namespace: [u8; 32]) -> Result<(), String> {
        if let Some(current) = &self.accepted_history {
            return if current.namespace == namespace {
                Ok(())
            } else {
                Err("accepted-history namespace cannot change".into())
            };
        }
        self.check_invariants()?;
        for execution in self.executions.values().filter(|e| e.terminal.is_none()) {
            action_digest(&execution.request.action)?;
        }
        let mut candidate = self.clone();
        candidate.accepted_history = Some(AcceptedHistory {
            namespace,
            preactivation_terminals: candidate
                .executions
                .values()
                .filter(|e| e.terminal.is_some())
                .map(|e| e.id)
                .collect(),
            associations: BTreeMap::new(),
        });
        candidate.check_invariants()?;
        *self = candidate;
        Ok(())
    }

    #[must_use]
    pub const fn accepted_history_enabled(&self) -> bool {
        self.accepted_history.is_some()
    }

    #[must_use]
    pub fn accepted_history_namespace(&self) -> Option<[u8; 32]> {
        self.accepted_history.as_ref().map(|h| h.namespace)
    }

    #[must_use]
    pub const fn accepted_history(&self) -> Option<&AcceptedHistory> {
        self.accepted_history.as_ref()
    }

    #[must_use]
    pub fn history_for_operation(&self, operation: OperationId) -> Option<&HistoryAssociation> {
        let operation = self.operations.get(&operation)?;
        worker_result(operation.view.terminal.as_ref()?)?;
        self.accepted_history
            .as_ref()?
            .associations
            .get(&operation.view.execution)
    }

    /// Obtain the exact recorded diagnostic from a committed state read. Its
    /// payload is pinned by a pending association, not copied into metadata.
    pub fn history_object_bytes(&self, effect: EffectId) -> Result<Vec<u8>, String> {
        let state = self
            .accepted_history
            .as_ref()
            .ok_or("history is inactive")?;
        let association = state
            .associations
            .values()
            .find(|h| h.effect == effect)
            .ok_or("unknown accepted-history effect")?;
        let execution = &self.executions[&association.execution];
        let result = execution
            .terminal
            .as_ref()
            .and_then(worker_result)
            .ok_or("history terminal missing")?;
        if association.recipe != recipe(state.namespace, &execution.request.action, result)? {
            return Err("accepted-history recipe changed".into());
        }
        let mut bytes = association.recipe.prefix.clone();
        bytes.extend_from_slice(response_bytes(result)?);
        Ok(bytes)
    }

    pub(super) fn history_request_valid(&self, action: &ActionKey) -> bool {
        !self.accepted_history_enabled() || action_digest(action).is_ok()
    }

    pub(super) fn prepare_history(
        &self,
        execution: ExecutionId,
        result: &ResultPayload,
    ) -> Result<Option<HistoryRecipe>, String> {
        self.accepted_history
            .as_ref()
            .map(|history| {
                recipe(
                    history.namespace,
                    &self.executions[&execution].request.action,
                    result,
                )
            })
            .transpose()
    }

    pub(super) fn accept_history(&mut self, attempt: AttemptId, prepared: Option<HistoryRecipe>) {
        let Some(recipe) = prepared else {
            return;
        };
        let source = &self.attempts[&attempt];
        let effect = EffectId(self.next_effect);
        self.next_effect = self
            .next_effect
            .checked_add(1)
            .expect("prepaid history identity");
        let association = HistoryAssociation {
            epoch: self.epoch,
            effect,
            execution: source.execution,
            attempt,
            worker: source.worker,
            incarnation: source.incarnation,
            recipe,
            status: HistoryStatus::Pending,
        };
        let old = self
            .accepted_history
            .as_mut()
            .expect("activated history")
            .associations
            .insert(source.execution, association);
        assert!(old.is_none(), "terminal acceptance occurs once");
    }

    pub(super) fn observe_history(
        &mut self,
        effect: EffectId,
        object: &HistoryObject,
        outcome: HistoryOutcome,
    ) -> Reply {
        let Some(history) = &mut self.accepted_history else {
            return Reply::Rejected(Rejection::Invalid);
        };
        let Some(association) = history
            .associations
            .values_mut()
            .find(|h| h.effect == effect)
        else {
            return Reply::Ignored;
        };
        if &association.recipe.object != object {
            return Reply::Rejected(Rejection::Stale);
        }
        if outcome != HistoryOutcome::Stored || association.status == HistoryStatus::Materialized {
            return Reply::Ignored;
        }
        association.status = HistoryStatus::Materialized;
        Reply::Applied
    }

    pub(super) fn history_pins(&self, execution: ExecutionId) -> bool {
        self.accepted_history
            .as_ref()
            .and_then(|h| h.associations.get(&execution))
            .is_some_and(|h| h.status == HistoryStatus::Pending)
    }

    pub(super) fn retire_history(&mut self, execution: ExecutionId) {
        if let Some(history) = &mut self.accepted_history {
            history.associations.remove(&execution);
            history.preactivation_terminals.remove(&execution);
        }
    }

    pub(super) fn history_byte_credit(&self, additional: usize) -> Option<usize> {
        if !self.accepted_history_enabled() {
            return Some(0);
        }
        self.executions
            .len()
            .checked_add(additional)?
            .checked_mul(HISTORY_ROW_BYTES)?
            .checked_add(HISTORY_HEADER_BYTES)
    }

    pub(super) const fn effect_headroom(&self) -> usize {
        super::EFFECT_HEADROOM + self.accepted_history_enabled() as usize
    }

    pub(super) fn check_history_invariants(&self) -> Result<(), String> {
        let Some(history) = &self.accepted_history else {
            return Ok(());
        };
        let mut encoded = PayloadSizeLimit {
            used: 0,
            limit: self
                .history_byte_credit(0)
                .ok_or("history credit overflow")?,
        };
        serde_json::to_writer(&mut encoded, history)
            .map_err(|_| "history metadata exceeds prepaid credit")?;
        if history.preactivation_terminals.iter().any(|id| {
            self.executions.get(id).is_none_or(|e| e.terminal.is_none())
                || history.associations.contains_key(id)
        }) {
            return Err("history grandfathered terminal association is invalid".into());
        }
        for execution in self.executions.values() {
            if execution.terminal.is_none() {
                action_digest(&execution.request.action)?;
            }
            let requires = execution.attempts_started > 0
                && execution
                    .terminal
                    .as_ref()
                    .and_then(worker_result)
                    .is_some()
                && !history.preactivation_terminals.contains(&execution.id);
            if requires != history.associations.contains_key(&execution.id) {
                return Err("history association must match accepted terminal origin".into());
            }
        }
        let mut effects = BTreeSet::new();
        for (id, association) in &history.associations {
            let execution = self.executions.get(id).ok_or("history execution missing")?;
            let result = execution
                .terminal
                .as_ref()
                .and_then(worker_result)
                .ok_or("history terminal missing")?;
            let attempt = self
                .attempts
                .get(&association.attempt)
                .ok_or("history attempt missing")?;
            let last_attempt = self
                .attempts
                .values()
                .filter(|a| a.execution == *id)
                .map(|a| a.id)
                .max();
            if association.execution != *id
                || association.epoch != self.epoch
                || association.effect.0 == 0
                || association.effect.0 >= self.next_effect
                || !effects.insert(association.effect)
                || self.effects.contains_key(&association.effect)
                || self
                    .attempts
                    .values()
                    .any(|a| a.dispatch_effect == association.effect)
                || association.attempt != attempt.id
                || last_attempt != Some(attempt.id)
                || attempt.execution != *id
                || attempt.worker != association.worker
                || attempt.incarnation != association.incarnation
                || attempt.authorized
                || !self.reservations[&attempt.reservation].released
                || association.recipe.prefix.len() > MAX_HISTORY_PREFIX_BYTES
                || association.recipe
                    != recipe(history.namespace, &execution.request.action, result)?
            {
                return Err("history immutable acceptance binding is invalid".into());
            }
            if self.receipt_retirement.as_ref().is_some_and(|r| {
                r.streams.values().any(|s| {
                    s.pending
                        .as_ref()
                        .is_some_and(|p| p.effect == association.effect)
                        || s.acknowledged
                            .is_some_and(|p| p.effect == association.effect)
                })
            }) {
                return Err("history identity aliases a receipt certificate".into());
            }
        }
        Ok(())
    }
}
