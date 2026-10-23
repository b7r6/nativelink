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

//! Experimental format control and trusted in-process member admission.
//!
//! This registry derives evidence from admitted journal process profiles and
//! pins their incarnations until a committed activation or abort. It is not a
//! distributed authentication, fencing, or membership reconciliation service.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

use openraft::{BasicNode, LogId, StoredMembership};
use serde::{Deserialize, Serialize};

use super::ClusterEpoch;

pub const DIRECT_SCHEDULER: u16 = 1;
pub const RECEIPT_RETIREMENT: u16 = 2;
pub const MAX_FEATURES: usize = 8;
pub const MAX_FORMAT_MEMBERS: usize = 64;
pub const MAX_FORMAT_BYTES: usize = 64 * 1024;
pub type Features = BTreeSet<u16>;
type Result<T> = std::result::Result<T, String>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityDomain {
    pub epoch: ClusterEpoch,
    pub namespace: u64,
}

/// These profiles represent executable capabilities, never configuration bits
/// accepted from a remote member's advertisement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReaderCapabilities {
    Base,
    DirectScheduler,
    ReceiptRetirement,
}

impl ReaderCapabilities {
    pub fn supports(self, features: &Features) -> bool {
        features.iter().all(|feature| match self {
            Self::Base => false,
            Self::DirectScheduler => *feature == DIRECT_SCHEDULER,
            Self::ReceiptRetirement => matches!(*feature, DIRECT_SCHEDULER | RECEIPT_RETIREMENT),
        })
    }

    /// Project to the frozen control record's decoder vocabulary. This value
    /// does not certify support for the prepared feature set: the admission
    /// registry must check the pinned live executable profile separately.
    pub const fn encoding(self) -> ReaderEncoding {
        match self {
            Self::Base => ReaderEncoding::Base,
            Self::DirectScheduler | Self::ReceiptRetirement => ReaderEncoding::DirectScheduler,
        }
    }
}

/// Frozen member-evidence vocabulary understood by the experimental base
/// executable. New runtime profiles must not introduce new serialized variants
/// into an uncommitted preparation and thereby break pre-activation rollback.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ReaderEncoding {
    Base,
    DirectScheduler,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemberEvidence {
    pub incarnation: u64,
    pub reader: ReaderEncoding,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Preparation {
    pub domain: AuthorityDomain,
    pub permit: u64,
    pub from_epoch: u64,
    pub target_epoch: u64,
    pub required: Features,
    pub membership_revision: LogId<u64>,
    pub members: BTreeMap<u64, MemberEvidence>,
}

impl Preparation {
    pub fn validate(&self) -> Result<()> {
        if self.permit == 0
            || self.from_epoch.checked_add(1) != Some(self.target_epoch)
            || self.required.is_empty()
            || self.required.len() > MAX_FEATURES
            || self.required.contains(&0)
            || self.members.is_empty()
            || self.members.len() > MAX_FORMAT_MEMBERS
            || self.members.values().any(|member| member.incarnation == 0)
        {
            return Err("invalid or oversized format preparation".into());
        }
        Ok(())
    }

    pub(crate) fn matches_membership(&self, membership: &StoredMembership<u64, BasicNode>) -> bool {
        membership.log_id() == &Some(self.membership_revision)
            && membership
                .membership()
                .nodes()
                .map(|(id, _)| *id)
                .eq(self.members.keys().copied())
    }
}

/// Both reader levels understand this vocabulary, including unsupported target
/// feature IDs in an uncommitted control. All controls use the base encoding.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub enum FormatControl {
    Prepare(Preparation),
    Activate(Preparation),
    Abort {
        domain: AuthorityDomain,
        permit: u64,
    },
}

impl FormatControl {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Prepare(preparation) | Self::Activate(preparation) => preparation.validate(),
            Self::Abort { permit, .. } if *permit != 0 => Ok(()),
            Self::Abort { .. } => Err("invalid preparation permit".into()),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FormatDecision {
    pub permit: u64,
    pub position: LogId<u64>,
    pub activated: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveFormat {
    pub domain: AuthorityDomain,
    pub feature_epoch: u64,
    pub required: Features,
    pub prepared: Option<Preparation>,
    pub decision: Option<FormatDecision>,
    pub activated_at: Option<LogId<u64>>,
}

impl ActiveFormat {
    pub const fn base(domain: AuthorityDomain) -> Self {
        Self {
            domain,
            feature_epoch: 0,
            required: Features::new(),
            prepared: None,
            decision: None,
            activated_at: None,
        }
    }

    pub fn validate(&self, reader: ReaderCapabilities) -> Result<()> {
        if self.required.len() > MAX_FEATURES
            || usize::try_from(self.feature_epoch).map_or(true, |epoch| epoch > self.required.len())
            || (self.feature_epoch == 0) != self.required.is_empty()
            || (self.feature_epoch == 0) != self.activated_at.is_none()
            || !reader.supports(&self.required)
        {
            return Err("reader cannot recover the activated authority format".into());
        }
        if let Some(prepared) = &self.prepared {
            prepared.validate()?;
            let epoch_matches = prepared.from_epoch == self.feature_epoch;
            if prepared.domain != self.domain
                || !epoch_matches
                || !prepared.required.is_superset(&self.required)
                || prepared.required == self.required
            {
                return Err("preparation does not belong to active authority".into());
            }
        }
        if self.decision.is_some_and(|decision| {
            decision.permit == 0
                || (decision.activated && self.activated_at != Some(decision.position))
        }) {
            return Err("invalid persisted format decision".into());
        }
        Ok(())
    }

    pub fn apply(
        &mut self,
        control: &FormatControl,
        membership: &StoredMembership<u64, BasicNode>,
        reader: ReaderCapabilities,
        position: LogId<u64>,
    ) -> Result<()> {
        control.validate()?;
        match control {
            FormatControl::Prepare(prepared) => {
                let epoch_matches = prepared.from_epoch == self.feature_epoch;
                if prepared.domain != self.domain
                    || !epoch_matches
                    || !prepared.required.is_superset(&self.required)
                    || prepared.required == self.required
                    || !prepared.matches_membership(membership)
                {
                    return Err("stale preparation domain, epoch, or membership".into());
                }
                self.prepared = Some(prepared.clone());
            }
            FormatControl::Activate(prepared) => {
                if self.prepared.as_ref() != Some(prepared)
                    || !prepared.matches_membership(membership)
                    || !reader.supports(&prepared.required)
                {
                    return Err("unsupported or stale committed activation".into());
                }
                self.feature_epoch = prepared.target_epoch;
                self.activated_at = Some(position);
                self.required.clone_from(&prepared.required);
                self.prepared = None;
                self.decision = Some(FormatDecision {
                    permit: prepared.permit,
                    position,
                    activated: true,
                });
            }
            FormatControl::Abort { domain, permit } => {
                if *domain != self.domain {
                    return Err("foreign activation abort".into());
                }
                if self
                    .prepared
                    .as_ref()
                    .is_some_and(|prepared| prepared.permit == *permit)
                {
                    self.prepared = None;
                }
                self.decision = Some(FormatDecision {
                    permit: *permit,
                    position,
                    activated: false,
                });
            }
        }
        self.validate(reader)
    }

    pub fn membership_changed(&mut self) {
        self.prepared = None;
    }
}

#[derive(Debug)]
pub(crate) struct MemberLease {
    evidence: MemberEvidence,
    reader: ReaderCapabilities,
}

#[derive(Debug)]
struct Pending {
    preparation: Preparation,
    prepare_position: Option<LogId<u64>>,
    activation_position: Option<LogId<u64>>,
    recovering: bool,
    // Strong references pin the actual registered profiles while the proposal
    // may still commit, even if its initiating future is canceled.
    leases: Vec<Arc<MemberLease>>,
}

#[derive(Debug)]
struct Completed {
    preparation: Preparation,
    prepare_position: Option<LogId<u64>>,
    activation_position: Option<LogId<u64>>,
    decision: FormatDecision,
}

#[derive(Debug, Default)]
struct RegistryState {
    members: BTreeMap<u64, Weak<MemberLease>>,
    used_incarnations: BTreeMap<u64, u64>,
    required: Features,
    next_permit: u64,
    pending: Option<Pending>,
    completed: BTreeMap<u64, Completed>,
}

#[derive(Debug)]
pub struct AdmissionRegistry {
    domain: AuthorityDomain,
    state: Mutex<RegistryState>,
}

/// Constructed only by validated, closed journal artifacts. This does not
/// attest that old processes are fenced; the fixture's restart procedure must
/// establish that fact before reconstructing its trusted registry.
pub(crate) struct RecoveryEvidence {
    pub node: u64,
    pub active: ActiveFormat,
    pub controls: Vec<(LogId<u64>, FormatControl)>,
    pub decisions: Vec<FormatDecision>,
}

impl AdmissionRegistry {
    pub fn new(domain: AuthorityDomain) -> Arc<Self> {
        Arc::new(Self {
            domain,
            state: Mutex::new(RegistryState::default()),
        })
    }

    pub(crate) fn recover(
        domain: AuthorityDomain,
        evidence: Vec<RecoveryEvidence>,
    ) -> Result<Arc<Self>> {
        if evidence.is_empty() || evidence.len() > MAX_FORMAT_MEMBERS {
            return Err("recovery requires bounded member artifacts".into());
        }
        let nodes: BTreeSet<_> = evidence.iter().map(|item| item.node).collect();
        if nodes.len() != evidence.len() {
            return Err("duplicate recovery member".into());
        }
        let mut state = RegistryState::default();
        let mut records: BTreeMap<u64, Pending> = BTreeMap::new();
        let mut decisions = BTreeMap::new();
        for item in evidence {
            if item.active.domain != domain {
                return Err("foreign recovery artifact".into());
            }
            state.required.extend(&item.active.required);
            for decision in item.active.decision.into_iter().chain(item.decisions) {
                state.next_permit = state.next_permit.max(decision.permit);
                if decisions
                    .insert(decision.permit, decision)
                    .is_some_and(|prior| prior != decision)
                {
                    return Err("conflicting committed format decisions".into());
                }
            }
            let prepared = item
                .active
                .prepared
                .into_iter()
                .map(|value| (None, FormatControl::Prepare(value)));
            for (position, control) in prepared.chain(
                item.controls
                    .into_iter()
                    .map(|(position, control)| (Some(position), control)),
            ) {
                let (preparation, activation) = match control {
                    FormatControl::Prepare(value) => (value, false),
                    FormatControl::Activate(value) => (value, true),
                    FormatControl::Abort {
                        domain: abort_domain,
                        permit,
                    } => {
                        if abort_domain != domain {
                            return Err("foreign recovered abort".into());
                        }
                        state.next_permit = state.next_permit.max(permit);
                        continue;
                    }
                };
                preparation.validate()?;
                if preparation.domain != domain {
                    return Err("foreign recovered preparation".into());
                }
                state.next_permit = state.next_permit.max(preparation.permit);
                let record = records
                    .entry(preparation.permit)
                    .or_insert_with(|| Pending {
                        preparation: preparation.clone(),
                        prepare_position: None,
                        activation_position: None,
                        recovering: true,
                        leases: Vec::new(),
                    });
                if record.preparation != preparation {
                    return Err("conflicting recovered preparation".into());
                }
                let slot = if activation {
                    &mut record.activation_position
                } else {
                    &mut record.prepare_position
                };
                if let Some(position) = position {
                    if slot.is_some_and(|prior| prior != position) {
                        return Err("shifted recovered control position".into());
                    }
                    *slot = Some(position);
                }
                if records.len() > MAX_FORMAT_MEMBERS {
                    return Err("recovered control history is oversized".into());
                }
            }
        }
        for (permit, pending) in records {
            for (id, member) in &pending.preparation.members {
                state
                    .used_incarnations
                    .entry(*id)
                    .and_modify(|used| *used = (*used).max(member.incarnation))
                    .or_insert(member.incarnation);
            }
            if let Some(decision) = decisions.get(&permit) {
                state.completed.insert(
                    permit,
                    Completed {
                        preparation: pending.preparation,
                        prepare_position: pending.prepare_position,
                        activation_position: pending.activation_position,
                        decision: *decision,
                    },
                );
            } else {
                if state.pending.is_some()
                    || !pending
                        .preparation
                        .members
                        .keys()
                        .all(|id| nodes.contains(id))
                {
                    return Err("recovery lacks required members or has conflicting unresolved preparations".into());
                }
                state.pending = Some(pending);
            }
        }
        Ok(Arc::new(Self {
            domain,
            state: Mutex::new(state),
        }))
    }

    pub(crate) fn register(
        &self,
        domain: AuthorityDomain,
        node: u64,
        incarnation: u64,
        reader: ReaderCapabilities,
        committed: &ActiveFormat,
    ) -> Result<Arc<MemberLease>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "admission registry poisoned")?;
        if domain != self.domain
            || incarnation == 0
            || state
                .pending
                .as_ref()
                .is_some_and(|pending| !pending.recovering)
        {
            return Err("foreign domain or unresolved activation prevents admission".into());
        }
        if !reader.supports(&committed.required)
            || !reader.supports(&state.required)
            || state.pending.as_ref().is_some_and(|pending| {
                !pending.preparation.members.contains_key(&node)
                    || !reader.supports(&pending.preparation.required)
            })
        {
            return Err("process cannot participate in activated format".into());
        }
        if committed.prepared.as_ref().is_some_and(|preparation| {
            !state
                .pending
                .as_ref()
                .is_some_and(|pending| pending.preparation == *preparation)
                && !state.completed.contains_key(&preparation.permit)
        }) {
            return Err("unresolved preparation requires closed-journal registry recovery".into());
        }
        if state.members.get(&node).and_then(Weak::upgrade).is_some()
            || state
                .used_incarnations
                .get(&node)
                .is_some_and(|used| incarnation <= *used)
            || (!state.used_incarnations.contains_key(&node)
                && state.used_incarnations.len() == MAX_FORMAT_MEMBERS)
        {
            return Err("member incarnation is live, stale, or admission history is full".into());
        }
        state.required.extend(&committed.required);
        let lease = Arc::new(MemberLease {
            evidence: MemberEvidence {
                incarnation,
                reader: reader.encoding(),
            },
            reader,
        });
        state.members.insert(node, Arc::downgrade(&lease));
        state.used_incarnations.insert(node, incarnation);
        if let Some(pending) = &mut state.pending {
            pending.leases.push(lease.clone());
        }
        Ok(lease)
    }

    pub(crate) fn prepare(
        &self,
        active: &ActiveFormat,
        membership: &StoredMembership<u64, BasicNode>,
        required: Features,
    ) -> Result<Preparation> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "admission registry poisoned")?;
        if active.domain != self.domain
            || state.pending.is_some()
            || state.completed.len() == MAX_FORMAT_MEMBERS
        {
            return Err("foreign domain or another activation is unresolved".into());
        }
        let mut members = BTreeMap::new();
        let mut leases = Vec::new();
        for (id, _) in membership.membership().nodes() {
            let lease = state
                .members
                .get(id)
                .and_then(Weak::upgrade)
                .ok_or("required member has no admitted process")?;
            if !lease.reader.supports(&required) {
                return Err("required member cannot read the target format".into());
            }
            members.insert(*id, lease.evidence);
            leases.push(lease);
        }
        let preparation = Preparation {
            domain: self.domain,
            permit: state
                .next_permit
                .checked_add(1)
                .ok_or("preparation identity exhausted")?,
            from_epoch: active.feature_epoch,
            target_epoch: active
                .feature_epoch
                .checked_add(1)
                .ok_or("feature epoch exhausted")?,
            required,
            membership_revision: membership.log_id().ok_or("membership is not committed")?,
            members,
        };
        preparation.validate()?;
        if !preparation.required.is_superset(&active.required)
            || preparation.required == active.required
        {
            return Err("activation must add a feature".into());
        }
        state.next_permit = preparation.permit;
        state.pending = Some(Pending {
            preparation: preparation.clone(),
            prepare_position: None,
            activation_position: None,
            recovering: false,
            leases,
        });
        Ok(preparation)
    }

    pub(crate) fn authorize(&self, control: &FormatControl, position: LogId<u64>) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "admission registry poisoned")?;
        if let Some(pending) = &mut state.pending {
            if pending.recovering && pending.leases.len() != pending.preparation.members.len() {
                return Err("recovery has not admitted every required capable member".into());
            }
            match control {
                FormatControl::Prepare(value) if *value == pending.preparation => {
                    if (!pending.recovering || pending.prepare_position.is_some())
                        && pending.prepare_position.get_or_insert(position) == &position
                    {
                        return Ok(());
                    }
                }
                FormatControl::Activate(value) if *value == pending.preparation => {
                    if (!pending.recovering || pending.activation_position.is_some())
                        && pending.activation_position.get_or_insert(position) == &position
                    {
                        return Ok(());
                    }
                }
                FormatControl::Abort { domain, permit }
                    if *domain == self.domain && *permit == pending.preparation.permit =>
                {
                    return Ok(());
                }
                _ => {}
            }
        }
        let permit = match control {
            FormatControl::Prepare(value) | FormatControl::Activate(value) => value.permit,
            FormatControl::Abort { permit, .. } => *permit,
        };
        if let Some(Completed {
            preparation: prepared,
            prepare_position,
            decision,
            ..
        }) = state.completed.get(&permit)
        {
            match control {
                FormatControl::Prepare(value)
                    if value == prepared && *prepare_position == Some(position) =>
                {
                    return Ok(());
                }
                FormatControl::Activate(value)
                    if value == prepared && decision.activated && position == decision.position =>
                {
                    return Ok(());
                }
                FormatControl::Abort { domain, .. }
                    if *domain == self.domain
                        && !decision.activated
                        && position == decision.position =>
                {
                    return Ok(());
                }
                _ => {}
            }
        }
        Err("control has no matching live permit or committed history".into())
    }

    pub(crate) fn recovery_ready(&self) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| "admission registry poisoned")?;
        if state.pending.as_ref().is_some_and(|pending| {
            pending.recovering && pending.leases.len() != pending.preparation.members.len()
        }) {
            return Err("all recovering profiles must be admitted before Raft startup".into());
        }
        Ok(())
    }

    pub(crate) fn knows_persisted(
        &self,
        control: &FormatControl,
        position: LogId<u64>,
    ) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| "admission registry poisoned")?;
        if let Some(pending) = &state.pending {
            let known = match control {
                FormatControl::Prepare(value) => {
                    *value == pending.preparation && pending.prepare_position == Some(position)
                }
                FormatControl::Activate(value) => {
                    *value == pending.preparation && pending.activation_position == Some(position)
                }
                FormatControl::Abort { domain, permit } => {
                    *domain == self.domain && *permit == pending.preparation.permit
                }
            };
            if known {
                return Ok(());
            }
        }
        let permit = match control {
            FormatControl::Prepare(value) | FormatControl::Activate(value) => value.permit,
            FormatControl::Abort { permit, .. } => *permit,
        };
        if let Some(Completed {
            preparation: prepared,
            prepare_position: prepared_position,
            activation_position,
            decision,
        }) = state.completed.get(&permit)
        {
            let known = match control {
                FormatControl::Prepare(value) => {
                    value == prepared && *prepared_position == Some(position)
                }
                FormatControl::Activate(value) => {
                    value == prepared
                        && (*activation_position == Some(position)
                            || (decision.activated && position == decision.position))
                }
                FormatControl::Abort { domain, .. } => {
                    *domain == self.domain && !decision.activated && position == decision.position
                }
            };
            if known {
                return Ok(());
            }
        }
        Err("persisted control requires closed-journal registry recovery".into())
    }

    pub(crate) fn membership_allowed(
        &self,
        position: LogId<u64>,
        membership: &openraft::Membership<u64, BasicNode>,
    ) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| "admission registry poisoned")?;
        if state
            .pending
            .as_ref()
            .is_some_and(|pending| position.index > pending.preparation.membership_revision.index)
        {
            return Err("membership change conflicts with unresolved activation".into());
        }
        if !state.required.is_empty() {
            for (id, _) in membership.nodes() {
                let lease = state
                    .members
                    .get(id)
                    .and_then(Weak::upgrade)
                    .ok_or("new member has no admitted process")?;
                if !lease.reader.supports(&state.required) {
                    return Err("new member cannot read active format".into());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn observe_committed(&self, active: &ActiveFormat) {
        let mut state = self.state.lock().expect("admission registry poisoned");
        state.required.extend(&active.required);
        if let Some(decision) = &active.decision
            && state
                .pending
                .as_ref()
                .is_some_and(|pending| pending.preparation.permit == decision.permit)
        {
            let pending = state.pending.take().expect("matched pending preparation");
            state.completed.insert(
                decision.permit,
                Completed {
                    preparation: pending.preparation,
                    prepare_position: pending.prepare_position,
                    activation_position: pending.activation_position,
                    decision: *decision,
                },
            );
        }
    }
}

#[derive(Clone, Debug)]
pub struct JournalProfile {
    pub domain: AuthorityDomain,
    pub incarnation: u64,
    pub reader: ReaderCapabilities,
    pub admission: Arc<AdmissionRegistry>,
}
