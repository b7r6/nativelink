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

//! Experimental journal using the real `OpenRaft` commit/apply path.
//!
//! This bounded prototype records the complete scheduler snapshot per apply
//! batch. It is not a qualified production representation, replicated transport,
//! format-upgrade implementation, or power-loss certification. No external
//! effects are executed here; their intents remain in the core state. File
//! journals persist through restart. Fresh volatile journals lose every fact
//! with their backend and must never resume a member whose history was lost.

// The pinned split-storage traits require this unboxed third-party error type.
#![allow(clippy::result_large_err)]

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::{background_spawn, spawn_blocking};
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, Membership, Raft,
    RaftLogReader, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StoredMembership,
    Vote,
};
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction,
};
use serde::de::{DeserializeOwned, IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use tokio::sync::{Mutex, oneshot};

use super::format::{
    ACCEPTED_HISTORY, ActiveFormat, AdmissionRegistry, AuthorityDomain, ControlAdmission,
    DIRECT_SCHEDULER, Features, FormatControl, FormatDecision, JournalProfile, MAX_FORMAT_BYTES,
    MAX_FORMAT_MEMBERS, MemberLease, RECEIPT_RETIREMENT, ReaderCapabilities, RecoveryEvidence,
    accepted_history_namespace,
};
use super::{ClusterEpoch, Limits, RecordedEvent, Reply, State};

#[path = "snapshot_buffer.rs"]
mod snapshot_buffer;
pub use snapshot_buffer::{MAX_SNAPSHOT_BYTES, SnapshotBuffer};

openraft::declare_raft_types!(
    /// Types for the experimental authority; not a production wire format.
    pub TypeConfig:
        D = Batch,
        R = BatchReply,
        NodeId = u64,
        Node = BasicNode,
        Entry = Entry<Self>,
        SnapshotData = SnapshotBuffer,
        AsyncRuntime = openraft::TokioRuntime,
);

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("authority_meta");
const LOGS: TableDefinition<u64, &[u8]> = TableDefinition::new("authority_logs");
// This new experimental baseline understands stable feature controls. It does
// not claim migration or rollback compatibility with prototypes 1 or 2.
const FORMAT: u32 = 3;
const REQUIRED_SECTION: &str = "experimental-authority-base-v3";
type Result<T> = std::result::Result<T, StorageError<u64>>;

pub const MAX_BATCH_EVENTS: usize = 64;
pub const MAX_BATCH_BYTES: usize = 1024 * 1024;
/// Canonical membership bytes, independently bounded on append, apply and restore.
pub const MAX_MEMBERSHIP_BYTES: usize = 1024 * 1024;
/// UTF-8 bytes before JSON escaping, including IDs received from another member.
pub const MAX_SNAPSHOT_ID_BYTES: usize = 1024;
// A canonical log record embeds the already byte-bounded command in an escaped
// JSON payload plus fixed Raft/envelope fields. Reject oversized records before
// allocating strings during deserialization. This is an experimental bound on
// membership records too; no large-cluster membership envelope is qualified.
const MAX_LOG_RECORD_BYTES: usize = MAX_BATCH_BYTES * 2 + 4096;
const BATCH_VERSION: u32 = 1;

/// One bounded ordered application command in one Raft entry. Event ordinal,
/// together with the entry's log position, defines order inside the batch.
/// Intermediate replies are observations of those transitions, never permission
/// to deliver an effect absent from the final committed outbox.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Batch {
    version: u32,
    events: Vec<RecordedEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    control: Option<FormatControl>,
    #[serde(default, skip_serializing_if = "is_zero")]
    feature_epoch: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchWire {
    version: u32,
    #[serde(deserialize_with = "deserialize_bounded_events")]
    events: Vec<RecordedEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    control: Option<FormatControl>,
    #[serde(default, skip_serializing_if = "is_zero")]
    feature_epoch: u64,
}

// Serde's skip predicate receives the serialized field by reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn deserialize_bounded_events<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<RecordedEvent>, D::Error> {
    struct EventsVisitor;
    impl<'de> Visitor<'de> for EventsVisitor {
        type Value = Vec<RecordedEvent>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("one to 64 recorded events")
        }

        fn visit_seq<A: SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut events =
                Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX_BATCH_EVENTS));
            while events.len() < MAX_BATCH_EVENTS {
                let Some(event) = sequence.next_element()? else {
                    return Ok(events);
                };
                events.push(event);
            }
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom("too many batch events"));
            }
            Ok(events)
        }
    }
    deserializer.deserialize_seq(EventsVisitor)
}

impl<'de> Deserialize<'de> for Batch {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let wire = BatchWire::deserialize(deserializer)?;
        let batch = Self {
            version: wire.version,
            events: wire.events,
            control: wire.control,
            feature_epoch: wire.feature_epoch,
        };
        batch.validate().map_err(serde::de::Error::custom)?;
        Ok(batch)
    }
}

struct BoundedEncodedSize {
    used: usize,
    limit: usize,
}

impl io::Write for BoundedEncodedSize {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let size = self
            .used
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("encoded size overflow"))?;
        if size > self.limit {
            return Err(io::Error::other("value exceeds encoded byte bound"));
        }
        self.used = size;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Batch {
    /// Decode an untrusted command only after bounding its raw representation.
    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, String> {
        if bytes.len() > MAX_BATCH_BYTES {
            return Err("batch exceeds encoded byte bound".to_owned());
        }
        serde_json::from_slice(bytes).map_err(|error| error.to_string())
    }

    pub fn new(
        events: impl IntoIterator<Item = RecordedEvent>,
    ) -> std::result::Result<Self, String> {
        let mut bounded = Vec::new();
        for event in events {
            if bounded.len() == MAX_BATCH_EVENTS {
                return Err("too many batch events".to_owned());
            }
            bounded.push(event);
        }
        let batch = Self {
            version: BATCH_VERSION,
            events: bounded,
            control: None,
            feature_epoch: 0,
        };
        batch.validate()?;
        Ok(batch)
    }

    pub fn format(control: FormatControl) -> std::result::Result<Self, String> {
        let batch = Self {
            version: BATCH_VERSION,
            events: Vec::new(),
            control: Some(control),
            feature_epoch: 0,
        };
        batch.validate()?;
        Ok(batch)
    }

    #[must_use]
    pub const fn at_feature_epoch(mut self, epoch: u64) -> Self {
        self.feature_epoch = epoch;
        self
    }

    pub fn events(&self) -> &[RecordedEvent] {
        &self.events
    }

    pub fn encoded_bytes(&self) -> std::result::Result<usize, String> {
        let mut size = BoundedEncodedSize {
            used: 0,
            limit: MAX_BATCH_BYTES,
        };
        serde_json::to_writer(&mut size, self).map_err(|error| error.to_string())?;
        Ok(size.used)
    }

    fn validate(&self) -> std::result::Result<(), String> {
        if self.version != BATCH_VERSION
            || (self.events.is_empty() != self.control.is_some())
            || self.events.len() > MAX_BATCH_EVENTS
            || (self.control.is_some() && self.feature_epoch != 0)
        {
            return Err("unsupported batch version or invalid event count".to_owned());
        }
        if let Some(control) = &self.control {
            control.validate()?;
        }
        self.encoded_bytes()?;
        Ok(())
    }
}

/// Replies correspond exactly to event ordinals. No reply escapes before the
/// whole entry's applied state and final outbox have committed in the selected
/// backend. A volatile commit carries no process-loss durability claim.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BatchReply {
    pub replies: Vec<Reply>,
}

#[derive(Clone, Debug)]
pub struct SingleWriteResponse {
    pub log_id: LogId<u64>,
    pub data: Reply,
}

fn failure(verb: ErrorVerb, error: impl std::fmt::Display) -> StorageError<u64> {
    StorageError::from_io_error(
        ErrorSubject::Store,
        verb,
        io::Error::other(error.to_string()),
    )
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u32,
    epoch: ClusterEpoch,
    required_sections: Vec<String>,
    length: usize,
    checksum: String,
    payload: String,
}

fn checksum(bytes: &[u8]) -> String {
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(bytes);
    hasher.finalize_digest().packed_hash().to_string()
}

fn encode<T: Serialize>(epoch: ClusterEpoch, value: &T) -> Result<Vec<u8>> {
    let mut payload = SnapshotBuffer::default();
    serde_json::to_writer(&mut payload, value).map_err(|e| failure(ErrorVerb::Write, e))?;
    let payload = String::from_utf8(
        payload
            .into_inner()
            .map_err(|e| failure(ErrorVerb::Write, e))?,
    )
    .map_err(|e| failure(ErrorVerb::Write, e))?;
    let mut bytes = SnapshotBuffer::default();
    serde_json::to_writer(
        &mut bytes,
        &Envelope {
            version: FORMAT,
            epoch,
            required_sections: vec![REQUIRED_SECTION.to_owned()],
            length: payload.len(),
            checksum: checksum(payload.as_bytes()),
            payload,
        },
    )
    .map_err(|e| failure(ErrorVerb::Write, e))?;
    bytes.into_inner().map_err(|e| failure(ErrorVerb::Write, e))
}

fn decode<T: DeserializeOwned>(epoch: ClusterEpoch, bytes: &[u8]) -> Result<T> {
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err(failure(
            ErrorVerb::Read,
            "authority metadata exceeds byte bound",
        ));
    }
    let envelope: Envelope =
        serde_json::from_slice(bytes).map_err(|e| failure(ErrorVerb::Read, e))?;
    if envelope.version != FORMAT
        || envelope.epoch != epoch
        || envelope.required_sections != [REQUIRED_SECTION]
    {
        return Err(failure(
            ErrorVerb::Read,
            "unsupported journal format/required sections or wrong cluster epoch",
        ));
    }
    if envelope.length != envelope.payload.len()
        || envelope.checksum != checksum(envelope.payload.as_bytes())
    {
        return Err(failure(
            ErrorVerb::Read,
            "journal payload length/checksum mismatch",
        ));
    }
    serde_json::from_str(&envelope.payload).map_err(|e| failure(ErrorVerb::Read, e))
}

fn decode_log(epoch: ClusterEpoch, index: u64, bytes: &[u8]) -> Result<Entry<TypeConfig>> {
    if bytes.len() > MAX_LOG_RECORD_BYTES {
        return Err(failure(
            ErrorVerb::Read,
            "encoded log record exceeds byte bound",
        ));
    }
    let entry: Entry<TypeConfig> = decode(epoch, bytes)?;
    if entry.log_id.index != index {
        return Err(failure(ErrorVerb::Read, "journal key/log index mismatch"));
    }
    validate_entry(&entry, ErrorVerb::Read)?;
    Ok(entry)
}

fn validate_membership(membership: &Membership<u64, BasicNode>, verb: ErrorVerb) -> Result<()> {
    let mut size = BoundedEncodedSize {
        used: 0,
        limit: MAX_MEMBERSHIP_BYTES,
    };
    serde_json::to_writer(&mut size, membership)
        .map_err(|error| failure(verb, format!("membership exceeds byte bound: {error}")))
}

fn validate_entry(entry: &Entry<TypeConfig>, verb: ErrorVerb) -> Result<()> {
    match &entry.payload {
        EntryPayload::Blank => Ok(()),
        EntryPayload::Normal(batch) => batch.validate().map_err(|error| failure(verb, error)),
        EntryPayload::Membership(membership) => validate_membership(membership, verb),
    }
}

fn validate_snapshot_meta(meta: &SnapshotMeta<u64, BasicNode>) -> Result<()> {
    if meta.snapshot_id.len() > MAX_SNAPSHOT_ID_BYTES {
        return Err(failure(ErrorVerb::Read, "snapshot ID exceeds byte bound"));
    }
    validate_membership(meta.last_membership.membership(), ErrorVerb::Read)
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    node_id: u64,
    epoch: ClusterEpoch,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AppliedState {
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
    scheduler: SchedulerSection,
    format: ActiveFormat,
}

/// Both profiles recognize the base control structure. Only the upgraded
/// profile can recover an activated direct scheduler section.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum SchedulerSection {
    String(String),
    Direct(serde_json::Value),
}

impl SchedulerSection {
    fn bytes(&self) -> Result<Vec<u8>> {
        match self {
            Self::String(value) => {
                if value.len() > super::MAX_STATE_BYTES {
                    return Err(failure(
                        ErrorVerb::Read,
                        "scheduler section exceeds byte bound",
                    ));
                }
                Ok(value.as_bytes().to_vec())
            }
            Self::Direct(value) => {
                let mut size = BoundedEncodedSize {
                    used: 0,
                    limit: super::MAX_STATE_BYTES,
                };
                serde_json::to_writer(&mut size, value).map_err(|e| failure(ErrorVerb::Read, e))?;
                serde_json::to_vec(value).map_err(|e| failure(ErrorVerb::Read, e))
            }
        }
    }

    fn from_state(state: &State, format: &ActiveFormat) -> Result<Self> {
        let bytes = state.snapshot().map_err(|e| failure(ErrorVerb::Write, e))?;
        if format.required.contains(&DIRECT_SCHEDULER) {
            Ok(Self::Direct(
                serde_json::from_slice(&bytes).map_err(|e| failure(ErrorVerb::Write, e))?,
            ))
        } else {
            Ok(Self::String(
                String::from_utf8(bytes).map_err(|e| failure(ErrorVerb::Write, e))?,
            ))
        }
    }
}

impl AppliedState {
    fn state(&self, epoch: ClusterEpoch, reader: ReaderCapabilities) -> Result<State> {
        self.format
            .validate(reader)
            .map_err(|e| failure(ErrorVerb::Read, e))?;
        let mut format_size = BoundedEncodedSize {
            used: 0,
            limit: MAX_FORMAT_BYTES,
        };
        serde_json::to_writer(&mut format_size, &self.format)
            .map_err(|e| failure(ErrorVerb::Read, e))?;
        if self.format.domain.epoch != epoch
            || self.format.activated_at > self.last_applied
            || self
                .format
                .prepared
                .as_ref()
                .is_some_and(|prepared| !prepared.matches_membership(&self.membership))
            || self
                .format
                .decision
                .is_some_and(|decision| Some(decision.position) > self.last_applied)
            || matches!(self.scheduler, SchedulerSection::Direct(_))
                != self.format.required.contains(&DIRECT_SCHEDULER)
        {
            return Err(failure(
                ErrorVerb::Read,
                "scheduler section disagrees with activated domain/format",
            ));
        }
        validate_membership(self.membership.membership(), ErrorVerb::Read)?;
        let state =
            State::restore(&self.scheduler.bytes()?).map_err(|e| failure(ErrorVerb::Read, e))?;
        if state.receipt_retirement_enabled() != self.format.required.contains(&RECEIPT_RETIREMENT)
        {
            return Err(failure(
                ErrorVerb::Read,
                "receipt retirement state disagrees with activated format",
            ));
        }
        if state.accepted_history_enabled() != self.format.required.contains(&ACCEPTED_HISTORY)
            || state.accepted_history_namespace().is_some_and(|namespace| {
                namespace != accepted_history_namespace(self.format.domain)
            })
        {
            return Err(failure(
                ErrorVerb::Read,
                "accepted history state disagrees with activated domain/format",
            ));
        }
        if state.view().epoch != epoch {
            return Err(failure(
                ErrorVerb::Read,
                "scheduler snapshot epoch mismatch",
            ));
        }
        if (self.last_applied.is_none()
            && (state
                != State::new(epoch, state.limits).map_err(|e| failure(ErrorVerb::Read, e))?
                || self.membership != StoredMembership::default()))
            || self.membership.log_id() > &self.last_applied
        {
            return Err(failure(
                ErrorVerb::Read,
                "applied state has no supporting Raft position",
            ));
        }
        Ok(state)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SavedSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    applied: AppliedState,
}

impl SavedSnapshot {
    fn validate(&self, epoch: ClusterEpoch, reader: ReaderCapabilities) -> Result<Limits> {
        validate_snapshot_meta(&self.meta)?;
        let state = self.applied.state(epoch, reader)?;
        if self.meta.last_log_id != self.applied.last_applied
            || self.meta.last_membership != self.applied.membership
        {
            return Err(failure(ErrorVerb::Read, "snapshot metadata/state mismatch"));
        }
        Ok(state.limits)
    }
}

/// Construction policy only; generic journals retain replicated recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoveryExpectation {
    Any,
    Singleton,
}

fn validate_singleton_recovery(
    node_id: u64,
    frontier: Option<LogId<u64>>,
    membership: &StoredMembership<u64, BasicNode>,
    snapshot: Option<&SnapshotMeta<u64, BasicNode>>,
) -> Result<()> {
    let (Some(frontier), Some(member_position)) = (frontier, *membership.log_id()) else {
        return Err(failure(
            ErrorVerb::Read,
            "singleton recovery: existing journal lacks initialized membership",
        ));
    };
    if member_position > frontier || member_position.index > frontier.index {
        return Err(failure(
            ErrorVerb::Read,
            "singleton recovery membership position exceeds recovered frontier",
        ));
    }
    let configs = membership.membership().get_joint_config();
    if configs.len() != 1
        || !configs[0].iter().copied().eq([node_id])
        || !membership.nodes().map(|(id, _)| *id).eq([node_id])
    {
        return Err(failure(
            ErrorVerb::Read,
            "singleton recovery requires exactly one local voter and no other nodes",
        ));
    }
    if let Some(snapshot) = snapshot {
        if snapshot.last_log_id > Some(frontier)
            || snapshot
                .last_log_id
                .is_some_and(|position| position.index > frontier.index)
        {
            return Err(failure(
                ErrorVerb::Read,
                "singleton recovery snapshot exceeds recovered frontier",
            ));
        }
        // An older snapshot can legitimately name an earlier multi-member or
        // joint configuration. A snapshot covering the final membership entry
        // must contain that exact stored membership, including its position.
        if snapshot
            .last_log_id
            .is_some_and(|position| position.index >= member_position.index)
            && snapshot.last_membership != *membership
        {
            return Err(failure(
                ErrorVerb::Read,
                "singleton recovery snapshot contradicts recovered membership",
            ));
        }
    }
    Ok(())
}

fn replay_format(
    active: &mut ActiveFormat,
    membership: &mut StoredMembership<u64, BasicNode>,
    entry: &Entry<TypeConfig>,
    reader: ReaderCapabilities,
) -> Result<()> {
    match &entry.payload {
        EntryPayload::Normal(batch) => {
            if let Some(control) = &batch.control {
                active
                    .apply(control, membership, reader, entry.log_id)
                    .map_err(|e| failure(ErrorVerb::Read, e))?;
            } else if active.freezes_proposals() {
                return Err(failure(
                    ErrorVerb::Read,
                    "application command crosses unresolved accepted history preparation",
                ));
            } else if batch.feature_epoch != active.feature_epoch {
                return Err(failure(
                    ErrorVerb::Read,
                    "application command feature epoch mismatch",
                ));
            } else if !active.required.contains(&RECEIPT_RETIREMENT)
                && batch
                    .events
                    .iter()
                    .any(|recorded| recorded.event.requires_receipt_retirement())
            {
                return Err(failure(
                    ErrorVerb::Read,
                    "receipt retirement command precedes feature activation",
                ));
            } else if !active.required.contains(&ACCEPTED_HISTORY)
                && batch
                    .events
                    .iter()
                    .any(|recorded| recorded.event.requires_accepted_history())
            {
                return Err(failure(
                    ErrorVerb::Read,
                    "accepted history command precedes feature activation",
                ));
            }
        }
        EntryPayload::Membership(value) => {
            *membership = StoredMembership::new(Some(entry.log_id), value.clone());
            active.membership_changed();
        }
        EntryPayload::Blank => {}
    }
    Ok(())
}

fn enable_features(
    state: &mut State,
    before: &Features,
    active: &ActiveFormat,
) -> std::result::Result<(), String> {
    if !before.contains(&RECEIPT_RETIREMENT) && active.required.contains(&RECEIPT_RETIREMENT) {
        state.enable_receipt_retirement()?;
    }
    if !before.contains(&ACCEPTED_HISTORY) && active.required.contains(&ACCEPTED_HISTORY) {
        state.enable_accepted_history(accepted_history_namespace(active.domain))?;
    }
    Ok(())
}

/// The same pure transition runs while validating a proposed suffix and while
/// applying its committed entries. Staging never runs an external effect.
fn replay_state(
    applied: &mut AppliedState,
    state: &mut State,
    entry: &Entry<TypeConfig>,
    reader: ReaderCapabilities,
) -> Result<BatchReply> {
    let before = applied.format.required.clone();
    replay_format(&mut applied.format, &mut applied.membership, entry, reader)?;
    enable_features(state, &before, &applied.format).map_err(|e| failure(ErrorVerb::Write, e))?;
    applied.last_applied = Some(entry.log_id);
    Ok(match &entry.payload {
        EntryPayload::Normal(batch) => BatchReply {
            replies: if batch.control.is_some() {
                vec![Reply::Applied]
            } else {
                batch
                    .events
                    .iter()
                    .cloned()
                    .map(|event| state.apply(event))
                    .collect()
            },
        },
        EntryPayload::Blank | EntryPayload::Membership(_) => BatchReply::default(),
    })
}

/// Feature-bearing commands may arrive behind an unapplied activation. Check
/// their preceding log sequence, not only the receiver's applied feature set.
/// Plain controls remain readable without applying an unsupported target.
fn validate_feature_append(
    tx: &WriteTransaction,
    epoch: ClusterEpoch,
    reader: ReaderCapabilities,
    entries: &[Entry<TypeConfig>],
    admitted_features: bool,
) -> Result<()> {
    let Some(last_feature_index) = entries.iter().map(|entry| entry.log_id.index).max() else {
        return Ok(());
    };
    let meta = tx
        .open_table(META)
        .map_err(|e| failure(ErrorVerb::Read, e))?;
    let mut applied: AppliedState = decode(
        epoch,
        meta.get("applied")
            .map_err(|e| failure(ErrorVerb::Read, e))?
            .ok_or_else(|| failure(ErrorVerb::Read, "missing applied state"))?
            .value(),
    )?;
    let logs = tx
        .open_table(LOGS)
        .map_err(|e| failure(ErrorVerb::Read, e))?;
    let feature_entry = |entry: &Entry<TypeConfig>| {
        matches!(&entry.payload,
        EntryPayload::Normal(batch) if batch.control.is_some() || batch.events.iter().any(|recorded|
            recorded.event.requires_receipt_retirement() || recorded.event.requires_accepted_history()))
    };
    let start = applied
        .last_applied
        .map_or(Some(0), |position| position.index.checked_add(1));
    let mut feature_context = admitted_features
        || !applied.format.required.is_empty()
        || applied.format.freezes_proposals()
        || entries.iter().any(feature_entry);
    if !feature_context
        && let Some(start) = start
        && start <= last_feature_index
    {
        for row in logs
            .range(start..=last_feature_index)
            .map_err(|e| failure(ErrorVerb::Read, e))?
        {
            let (key, value) = row.map_err(|e| failure(ErrorVerb::Read, e))?;
            if feature_entry(&decode_log(epoch, key.value(), value.value())?) {
                feature_context = true;
                break;
            }
        }
    }
    if !feature_context {
        // Preserve the base storage contract, including OpenRaft rebuilding
        // older ordinary log rows after state-machine application. A plain
        // command behind any retained preparation still takes the checked path.
        return Ok(());
    }
    let mut state = applied.state(epoch, reader)?;
    let incoming: std::collections::BTreeMap<_, _> = entries
        .iter()
        .map(|entry| (entry.log_id.index, entry))
        .collect();
    if incoming.len() != entries.len() {
        return Err(failure(
            ErrorVerb::Write,
            "duplicate positions in feature-bearing append",
        ));
    }
    for entry in entries.iter().filter(|entry| {
        applied
            .last_applied
            .is_some_and(|position| entry.log_id.index <= position.index)
    }) {
        let existing = logs
            .get(entry.log_id.index)
            .map_err(|e| failure(ErrorVerb::Read, e))?
            .ok_or_else(|| failure(ErrorVerb::Write, "feature command precedes retained log"))?;
        if decode_log(epoch, entry.log_id.index, existing.value())? != *entry {
            return Err(failure(
                ErrorVerb::Write,
                "feature append would replace applied authority",
            ));
        }
    }
    let Some(start) = start else {
        return Ok(());
    };
    for index in start..=last_feature_index {
        if let Some(entry) = incoming.get(&index) {
            replay_state(&mut applied, &mut state, entry, reader)?;
        } else {
            let bytes = logs
                .get(index)
                .map_err(|e| failure(ErrorVerb::Read, e))?
                .ok_or_else(|| failure(ErrorVerb::Write, "feature append has no preceding log"))?;
            let entry = decode_log(epoch, index, bytes.value())?;
            replay_state(&mut applied, &mut state, &entry, reader)?;
        }
    }
    Ok(())
}

/// Determine the committed format even when state-machine application trails
/// the durable commit marker. This is also the startup participation gate.
fn committed_format(
    tx: &WriteTransaction,
    epoch: ClusterEpoch,
    reader: ReaderCapabilities,
) -> Result<ActiveFormat> {
    let meta = tx
        .open_table(META)
        .map_err(|e| failure(ErrorVerb::Read, e))?;
    let applied: AppliedState = decode(
        epoch,
        meta.get("applied")
            .map_err(|e| failure(ErrorVerb::Read, e))?
            .ok_or_else(|| failure(ErrorVerb::Read, "missing applied state"))?
            .value(),
    )?;
    applied.state(epoch, reader)?;
    let committed: Option<LogId<u64>> = meta
        .get("committed")
        .map_err(|e| failure(ErrorVerb::Read, e))?
        .map(|value| decode(epoch, value.value()))
        .transpose()?
        .flatten();
    let mut active = applied.format;
    let mut membership = applied.membership;
    if let Some(committed) = committed {
        let logs = tx
            .open_table(LOGS)
            .map_err(|e| failure(ErrorVerb::Read, e))?;
        let start = applied
            .last_applied
            .map_or(0, |id| id.index.saturating_add(1));
        if start <= committed.index {
            for row in logs
                .range(start..=committed.index)
                .map_err(|e| failure(ErrorVerb::Read, e))?
            {
                let (key, value) = row.map_err(|e| failure(ErrorVerb::Read, e))?;
                let entry = decode_log(epoch, key.value(), value.value())?;
                replay_format(&mut active, &mut membership, &entry, reader)?;
            }
        }
    }
    Ok(active)
}

fn state_json(state: &State) -> Result<String> {
    String::from_utf8(state.snapshot().map_err(|e| failure(ErrorVerb::Write, e))?)
        .map_err(|e| failure(ErrorVerb::Write, e))
}

/// One-shot storage fault used by the experimental adapter's cutpoint tests.
#[derive(Clone, Copy, Debug)]
pub enum JournalFault {
    BeforeCommit = 1,
    AfterCommit = 2,
}

#[derive(Clone, Copy)]
enum MeasuredOperation {
    ReadVote,
    ReadCommitted,
    ReadApplied,
    ReadSnapshot,
    ReadLogs,
    GetLogState,
    SaveVote,
    SaveCommitted,
    Append,
    Apply,
    Truncate,
    Purge,
    InstallSnapshot,
    BuildSnapshot,
}

const MEASURED_OPERATION_NAMES: [&str; 14] = [
    "read_vote",
    "read_committed",
    "read_applied",
    "read_snapshot",
    "read_logs",
    "get_log_state",
    "save_vote",
    "save_committed",
    "append",
    "apply",
    "truncate",
    "purge",
    "install_snapshot",
    "build_snapshot",
];

#[derive(Default)]
struct OperationCounters {
    calls: AtomicU64,
    completed_calls: AtomicU64,
    durable_commits: AtomicU64,
    volatile_commits: AtomicU64,
    batch_items: AtomicU64,
    max_batch_items: AtomicU64,
    queue_wait_ns: AtomicU64,
    io_elapsed_ns: AtomicU64,
    commit_elapsed_ns: AtomicU64,
}

#[derive(Default)]
struct MeasurementCounters {
    enabled: AtomicBool,
    operations: [OperationCounters; 14],
}

/// Optional experiment diagnostics. These counters never affect authority state,
/// persistence, callback ordering, or the selected durability mode. Timings are
/// cumulative wall durations, not device write volume or actual fsync counts.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct JournalOperationMeasurement {
    pub calls: u64,
    pub completed_calls: u64,
    /// Successful Immediate commits to the file backend, not device fsync counts.
    pub durable_commits: u64,
    /// Successful atomic RAM commits; these do not survive backend loss.
    pub volatile_commits: u64,
    /// Entries presented to append/apply, not logical commands inside an entry.
    pub batch_items: u64,
    pub max_batch_items: u64,
    pub queue_wait_ns: u64,
    pub io_elapsed_ns: u64,
    pub commit_elapsed_ns: u64,
}

fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// The failure envelope of a journal's transactions. Both backends use the same
/// ordered commit/apply path; only the file backend can recover member history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalPersistence {
    Durable,
    Volatile,
}

/// Facts supplied by an owned storage capability before opening its database.
/// The domain includes both the cluster epoch and authority namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalBinding {
    pub node_id: u64,
    pub domain: AuthorityDomain,
}

/// A database produced by a trusted factory. Its backend must retain any
/// namespace custody for the entire database/transaction lifetime. A separate
/// callback or caller guard is insufficient when blocking I/O outlives a caller.
pub struct JournalDatabase {
    database: Database,
    sync_new_parent: Option<Box<dyn FnOnce() -> io::Result<()> + Send>>,
}

impl Debug for JournalDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalDatabase")
            .field("created", &self.sync_new_parent.is_some())
            .finish_non_exhaustive()
    }
}

impl JournalDatabase {
    /// Use only for an exclusively created file. The parent callback runs after
    /// durable authority initialization and must refer to the opened parent.
    pub fn created(
        database: Database,
        sync_parent: impl FnOnce() -> io::Result<()> + Send + 'static,
    ) -> Self {
        Self {
            database,
            sync_new_parent: Some(Box::new(sync_parent)),
        }
    }

    /// Use only for existing storage; the factory must refuse missing or empty
    /// storage before it could initialize anything. All journal validation still
    /// runs before the database is admitted.
    pub fn existing(database: Database) -> Self {
        Self {
            database,
            sync_new_parent: None,
        }
    }
}

/// Cloneable split storage handles share one serialized blocking-I/O queue.
#[derive(Clone)]
pub struct Journal {
    database: Arc<Database>,
    queue: Arc<Mutex<()>>,
    proposal_gate: Arc<Mutex<()>>,
    history_apply_pause: Arc<std::sync::Mutex<Option<HistoryApplyPause>>>,
    feature_prepare_pause: Arc<std::sync::Mutex<Option<HistoryApplyPause>>>,
    fault: Arc<AtomicU8>,
    measurements: Arc<MeasurementCounters>,
    node_id: u64,
    epoch: ClusterEpoch,
    created: bool,
    persistence: JournalPersistence,
    domain: AuthorityDomain,
    reader: ReaderCapabilities,
    profile: Option<JournalProfile>,
    _admission: Option<Arc<MemberLease>>,
}

struct HistoryApplyPause {
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl Debug for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Journal")
            .field("node_id", &self.node_id)
            .field("epoch", &self.epoch)
            .field("persistence", &self.persistence)
            .finish_non_exhaustive()
    }
}

impl Journal {
    fn initialize_database(
        database: &Database,
        node_id: u64,
        epoch: ClusterEpoch,
        initial: &State,
        domain: AuthorityDomain,
    ) -> Result<()> {
        let mut tx = database
            .begin_write()
            .map_err(|e| failure(ErrorVerb::Write, e))?;
        tx.set_durability(Durability::Immediate)
            .map_err(|e| failure(ErrorVerb::Write, e))?;
        {
            let mut table = tx
                .open_table(META)
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            let identity = encode(epoch, &Identity { node_id, epoch })?;
            table
                .insert("identity", identity.as_slice())
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            let applied = encode(
                epoch,
                &AppliedState {
                    last_applied: None,
                    membership: StoredMembership::default(),
                    scheduler: SchedulerSection::String(state_json(initial)?),
                    format: ActiveFormat::base(domain),
                },
            )?;
            table
                .insert("applied", applied.as_slice())
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            tx.open_table(LOGS)
                .map_err(|e| failure(ErrorVerb::Write, e))?;
        }
        tx.commit().map_err(|e| failure(ErrorVerb::Write, e))
    }

    fn from_database(
        database: Database,
        node_id: u64,
        epoch: ClusterEpoch,
        created: bool,
        persistence: JournalPersistence,
        profile: Option<JournalProfile>,
        committed_format: &ActiveFormat,
    ) -> Result<Self> {
        let admission = profile
            .as_ref()
            .map(|profile| {
                profile
                    .admission
                    .register(
                        profile.domain,
                        node_id,
                        profile.incarnation,
                        profile.reader,
                        committed_format,
                    )
                    .map_err(|e| failure(ErrorVerb::Read, e))
            })
            .transpose()?;
        Ok(Self {
            database: Arc::new(database),
            queue: Arc::new(Mutex::new(())),
            proposal_gate: profile.as_ref().map_or_else(
                || Arc::new(Mutex::new(())),
                |profile| profile.admission.proposal_gate.clone(),
            ),
            history_apply_pause: Arc::new(std::sync::Mutex::new(None)),
            feature_prepare_pause: Arc::new(std::sync::Mutex::new(None)),
            fault: Arc::new(AtomicU8::new(0)),
            measurements: Arc::new(MeasurementCounters::default()),
            node_id,
            epoch,
            created,
            persistence,
            domain: committed_format.domain,
            reader: profile
                .as_ref()
                .map_or(ReaderCapabilities::Base, |p| p.reader),
            profile,
            _admission: admission,
        })
    }

    /// Allocate a pristine RAM journal, with no vote, log, membership or applied
    /// history. Clones share this backend; a new call never reopens a prior one.
    /// This does not initialize Raft, join a cluster, or promote a voter.
    ///
    /// After member history is lost, the caller must obtain a fresh member ID
    /// and join as a learner through the surviving quorum. This constructor
    /// cannot prove that an externally supplied ID has never been used. It must
    /// not be used to resume the lost identity or automatically bootstrap after
    /// quorum loss. A whole-cluster reset requires external fencing or an
    /// isolated authority domain; a changed epoch alone cannot fence old nodes.
    ///
    /// Redb's RAM backend has no durable sync operation. The shared transaction
    /// path preserves atomicity and callback order only while this backend lives.
    pub async fn fresh_volatile(node_id: u64, epoch: ClusterEpoch, limits: Limits) -> Result<Self> {
        Self::fresh_volatile_inner(node_id, epoch, limits, None).await
    }

    pub async fn fresh_volatile_with_profile(
        node_id: u64,
        limits: Limits,
        profile: JournalProfile,
    ) -> Result<Self> {
        Self::fresh_volatile_inner(node_id, profile.domain.epoch, limits, Some(profile)).await
    }

    async fn fresh_volatile_inner(
        node_id: u64,
        epoch: ClusterEpoch,
        limits: Limits,
        profile: Option<JournalProfile>,
    ) -> Result<Self> {
        let domain = profile.as_ref().map_or(
            AuthorityDomain {
                epoch,
                namespace: 0,
            },
            |p| p.domain,
        );
        let initial = State::new(epoch, limits).map_err(|e| failure(ErrorVerb::Write, e))?;
        let database = spawn_blocking!("authority_journal_fresh_volatile", move || {
            let database = Database::builder()
                .create_with_backend(redb::backends::InMemoryBackend::new())
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            Self::initialize_database(&database, node_id, epoch, &initial, domain)?;
            Ok::<_, StorageError<u64>>(database)
        })
        .await
        .map_err(|e| failure(ErrorVerb::Write, e))??;
        Self::from_database(
            database,
            node_id,
            epoch,
            true,
            JournalPersistence::Volatile,
            profile,
            &ActiveFormat::base(domain),
        )
    }

    pub const fn persistence(&self) -> JournalPersistence {
        self.persistence
    }

    /// Open only a valid existing database, or exclusively create a new one.
    /// An existing empty/corrupt file is never interpreted as a fresh cluster.
    /// Existing applied state and saved snapshots must exactly match `limits`.
    pub async fn open(
        path: impl AsRef<Path>,
        node_id: u64,
        epoch: ClusterEpoch,
        limits: Limits,
    ) -> Result<Self> {
        Self::open_inner(path, node_id, epoch, limits, None, RecoveryExpectation::Any).await
    }

    pub async fn open_with_profile(
        path: impl AsRef<Path>,
        node_id: u64,
        limits: Limits,
        profile: JournalProfile,
    ) -> Result<Self> {
        Self::open_inner(
            path,
            node_id,
            profile.domain.epoch,
            limits,
            Some(profile),
            RecoveryExpectation::Any,
        )
        .await
    }

    /// Construct from a capability-aware factory without depending on a
    /// platform-specific filesystem library. Bindings and limits are checked
    /// before the factory runs on the blocking pool. Existing format validation
    /// and admission registration are unchanged. This does not fence an old
    /// authority, authorize bootstrap, or replace closed-member recovery.
    pub async fn open_from_factory(
        binding: JournalBinding,
        node_id: u64,
        limits: Limits,
        profile: JournalProfile,
        factory: impl FnOnce() -> io::Result<JournalDatabase> + Send + 'static,
    ) -> Result<Self> {
        Self::open_from_factory_with_expectation(
            binding,
            node_id,
            limits,
            profile,
            RecoveryExpectation::Any,
            factory,
        )
        .await
    }

    async fn open_from_factory_with_expectation(
        binding: JournalBinding,
        node_id: u64,
        limits: Limits,
        profile: JournalProfile,
        expectation: RecoveryExpectation,
        factory: impl FnOnce() -> io::Result<JournalDatabase> + Send + 'static,
    ) -> Result<Self> {
        if binding.node_id != node_id || binding.domain != profile.domain {
            return Err(failure(ErrorVerb::Read, "journal factory binding mismatch"));
        }
        Self::open_with_factory_inner(
            node_id,
            profile.domain.epoch,
            limits,
            Some(profile),
            expectation,
            move || factory().map_err(|error| failure(ErrorVerb::Read, error)),
        )
        .await
    }

    /// Reconstruct the bounded trusted fixture registry from closed journals.
    /// The caller must stop and fence every prior member process first, and
    /// supply every member named by an unresolved preparation. New profiles use
    /// later incarnations and must all support that preparation before Raft is
    /// restarted. This is an explicit in-process recovery procedure, not a
    /// distributed fencing or disaster-recovery protocol.
    pub async fn recover_admission(
        domain: AuthorityDomain,
        reader: ReaderCapabilities,
        artifacts: Vec<(u64, PathBuf)>,
    ) -> Result<Arc<AdmissionRegistry>> {
        if artifacts.is_empty() || artifacts.len() > MAX_FORMAT_MEMBERS {
            return Err(failure(ErrorVerb::Read, "invalid recovery member count"));
        }
        let evidence = spawn_blocking!("authority_admission_recovery", move || {
            let mut evidence = Vec::new();
            for (node, path) in artifacts {
                // Redb's exclusive open also rejects an accidentally live
                // local journal. It cannot establish remote process fencing.
                let database = Database::open(path).map_err(|e| failure(ErrorVerb::Read, e))?;
                let tx = database
                    .begin_read()
                    .map_err(|e| failure(ErrorVerb::Read, e))?;
                let table = tx
                    .open_table(META)
                    .map_err(|e| failure(ErrorVerb::Read, e))?;
                let identity: Identity = decode(
                    domain.epoch,
                    table
                        .get("identity")
                        .map_err(|e| failure(ErrorVerb::Read, e))?
                        .ok_or_else(|| failure(ErrorVerb::Read, "missing identity"))?
                        .value(),
                )?;
                if identity.node_id != node || identity.epoch != domain.epoch {
                    return Err(failure(ErrorVerb::Read, "recovery identity mismatch"));
                }
                let applied: AppliedState = decode(
                    domain.epoch,
                    table
                        .get("applied")
                        .map_err(|e| failure(ErrorVerb::Read, e))?
                        .ok_or_else(|| failure(ErrorVerb::Read, "missing applied state"))?
                        .value(),
                )?;
                applied.state(domain.epoch, reader)?;
                if applied.format.domain != domain {
                    return Err(failure(ErrorVerb::Read, "recovery namespace mismatch"));
                }
                let committed: Option<LogId<u64>> = table
                    .get("committed")
                    .map_err(|e| failure(ErrorVerb::Read, e))?
                    .map(|value| decode(domain.epoch, value.value()))
                    .transpose()?
                    .flatten();
                let committed = committed.max(applied.last_applied);
                let mut active = applied.format;
                let mut membership = applied.membership;
                let mut controls = Vec::new();
                let mut decisions = Vec::new();
                let logs = tx
                    .open_table(LOGS)
                    .map_err(|e| failure(ErrorVerb::Read, e))?;
                for row in logs.iter().map_err(|e| failure(ErrorVerb::Read, e))? {
                    let (key, value) = row.map_err(|e| failure(ErrorVerb::Read, e))?;
                    let entry = decode_log(domain.epoch, key.value(), value.value())?;
                    let is_committed = committed.is_some_and(|position| entry.log_id <= position);
                    if is_committed
                        && applied
                            .last_applied
                            .is_none_or(|position| entry.log_id > position)
                    {
                        replay_format(&mut active, &mut membership, &entry, reader)?;
                    }
                    if let EntryPayload::Normal(batch) = entry.payload
                        && let Some(control) = batch.control
                    {
                        if is_committed {
                            let decision = match &control {
                                FormatControl::Activate(preparation) => {
                                    Some((preparation.permit, true))
                                }
                                FormatControl::Abort { permit, .. } => Some((*permit, false)),
                                FormatControl::Prepare(_) => None,
                            };
                            if let Some((permit, activated)) = decision {
                                decisions.push(FormatDecision {
                                    permit,
                                    position: entry.log_id,
                                    activated,
                                });
                            }
                        }
                        controls.push((entry.log_id, control));
                        if controls.len() > 3 * MAX_FORMAT_MEMBERS {
                            return Err(failure(
                                ErrorVerb::Read,
                                "recovery control history is oversized",
                            ));
                        }
                    }
                }
                evidence.push(RecoveryEvidence {
                    node,
                    active,
                    controls,
                    decisions,
                });
            }
            Ok::<_, StorageError<u64>>(evidence)
        })
        .await
        .map_err(|e| failure(ErrorVerb::Read, e))??;
        AdmissionRegistry::recover(domain, evidence).map_err(|e| failure(ErrorVerb::Read, e))
    }

    async fn open_inner(
        path: impl AsRef<Path>,
        node_id: u64,
        epoch: ClusterEpoch,
        limits: Limits,
        profile: Option<JournalProfile>,
        expectation: RecoveryExpectation,
    ) -> Result<Self> {
        let path = path.as_ref().to_owned();
        Self::open_with_factory_inner(node_id, epoch, limits, profile, expectation, move || {
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => {
                    let database = Database::builder()
                        .create_file(file)
                        .map_err(|e| failure(ErrorVerb::Write, e))?;
                    Ok(JournalDatabase::created(database, move || {
                        let parent = path
                            .parent()
                            .filter(|p| !p.as_os_str().is_empty())
                            .unwrap_or_else(|| Path::new("."));
                        File::open(parent)?.sync_all()
                    }))
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Database::open(&path)
                    .map(JournalDatabase::existing)
                    .map_err(|e| failure(ErrorVerb::Read, e)),
                Err(e) => Err(failure(ErrorVerb::Write, e)),
            }
        })
        .await
    }

    async fn open_with_factory_inner(
        node_id: u64,
        epoch: ClusterEpoch,
        limits: Limits,
        profile: Option<JournalProfile>,
        expectation: RecoveryExpectation,
        factory: impl FnOnce() -> Result<JournalDatabase> + Send + 'static,
    ) -> Result<Self> {
        let domain = profile.as_ref().map_or(
            AuthorityDomain {
                epoch,
                namespace: 0,
            },
            |p| p.domain,
        );
        let reader = profile
            .as_ref()
            .map_or(ReaderCapabilities::Base, |p| p.reader);
        // Refuse an invalid admission envelope before creating any durable file.
        let initial = State::new(epoch, limits).map_err(|e| failure(ErrorVerb::Write, e))?;
        let (database, created, committed_format, controls) =
            spawn_blocking!("authority_journal_open", move || {
                let JournalDatabase {
                    database,
                    sync_new_parent,
                } = factory()?;
                let created = sync_new_parent.is_some();
                let mut committed_format = ActiveFormat::base(domain);
                let mut controls = Vec::new();
                if let Some(sync_new_parent) = sync_new_parent {
                    Self::initialize_database(&database, node_id, epoch, &initial, domain)?;
                    // Persist the new directory entry as well as database contents.
                    sync_new_parent().map_err(|e| failure(ErrorVerb::Write, e))?;
                } else {
                    let tx = database
                        .begin_read()
                        .map_err(|e| failure(ErrorVerb::Read, e))?;
                    let table = tx
                        .open_table(META)
                        .map_err(|e| failure(ErrorVerb::Read, e))?;
                    let bytes = table
                        .get("identity")
                        .map_err(|e| failure(ErrorVerb::Read, e))?
                        .ok_or_else(|| failure(ErrorVerb::Read, "missing authority identity"))?;
                    let identity: Identity = decode(epoch, bytes.value())?;
                    if identity.node_id != node_id || identity.epoch != epoch {
                        return Err(failure(ErrorVerb::Read, "authority node/epoch mismatch"));
                    }
                    let bytes = table
                        .get("applied")
                        .map_err(|e| failure(ErrorVerb::Read, e))?
                        .ok_or_else(|| failure(ErrorVerb::Read, "missing applied state"))?;
                    let applied: AppliedState = decode(epoch, bytes.value())?;
                    if applied.state(epoch, reader)?.limits != limits {
                        return Err(failure(
                            ErrorVerb::Read,
                            "persisted authority limits differ from requested limits",
                        ));
                    }
                    if applied.format.domain != domain {
                        return Err(failure(ErrorVerb::Read, "authority namespace mismatch"));
                    }
                    committed_format = applied.format.clone();
                    let mut committed_membership = applied.membership.clone();
                    let mut purged: Option<LogId<u64>> = None;
                    let mut committed: Option<LogId<u64>> = None;
                    let mut singleton_snapshot = None;
                    // Every metadata section is authority-bearing in this format.
                    // Never ignore an unknown section, even if applied state alone
                    // would be sufficient to start this older executable.
                    for record in table.iter().map_err(|e| failure(ErrorVerb::Read, e))? {
                        let (key, value) = record.map_err(|e| failure(ErrorVerb::Read, e))?;
                        match key.value() {
                            "identity" | "applied" => {}
                            "vote" => {
                                decode::<Vote<u64>>(epoch, value.value())?;
                            }
                            "committed" => {
                                committed = decode(epoch, value.value())?;
                            }
                            "purged" => {
                                purged = Some(decode(epoch, value.value())?);
                            }
                            "snapshot_sequence" => {
                                decode::<u64>(epoch, value.value())?;
                            }
                            "snapshot" => {
                                let saved: SavedSnapshot = decode(epoch, value.value())?;
                                if saved.validate(epoch, reader)? != limits {
                                    return Err(failure(
                                        ErrorVerb::Read,
                                        "persisted snapshot limits differ from requested limits",
                                    ));
                                }
                                if saved.applied.format.domain != domain {
                                    return Err(failure(
                                        ErrorVerb::Read,
                                        "foreign snapshot namespace",
                                    ));
                                }
                                if expectation == RecoveryExpectation::Singleton {
                                    singleton_snapshot = Some(saved.meta);
                                }
                            }
                            unknown => {
                                return Err(failure(
                                    ErrorVerb::Read,
                                    format!("unknown required journal section: {unknown}"),
                                ));
                            }
                        }
                    }
                    let logs = tx
                        .open_table(LOGS)
                        .map_err(|e| failure(ErrorVerb::Read, e))?;
                    // OpenRaft 0.9.25 first clamps committed to applied, then
                    // replays by INDEX. Its later membership loader also sees
                    // uncommitted membership entries. Project without applying
                    // events, spawning Raft or registering the member profile.
                    let recovered_frontier = committed.max(applied.last_applied);
                    let mut singleton_membership = (expectation == RecoveryExpectation::Singleton)
                        .then(|| applied.membership.clone());
                    let mut previous = purged;
                    let mut first = true;
                    for record in logs.iter().map_err(|e| failure(ErrorVerb::Read, e))? {
                        let (key, value) = record.map_err(|e| failure(ErrorVerb::Read, e))?;
                        let entry = decode_log(epoch, key.value(), value.value())?;
                        if let Some(membership) = &mut singleton_membership
                            && let EntryPayload::Membership(value) = &entry.payload
                        {
                            if recovered_frontier.is_none_or(|position| entry.log_id.index > position.index) {
                                return Err(failure(
                                    ErrorVerb::Read,
                                    "singleton recovery has pending membership beyond committed frontier",
                                ));
                            }
                            if applied.last_applied.is_none_or(|position| entry.log_id.index > position.index) {
                                *membership = StoredMembership::new(Some(entry.log_id), value.clone());
                            }
                        }
                        if let EntryPayload::Normal(batch) = &entry.payload
                            && let Some(control) = &batch.control
                        {
                            controls.push((entry.log_id, control.clone()));
                            if controls.len() > 3 * MAX_FORMAT_MEMBERS {
                                return Err(failure(
                                    ErrorVerb::Read,
                                    "persisted control history is oversized",
                                ));
                            }
                        }
                        if committed.is_some_and(|position| entry.log_id <= position)
                            && applied
                                .last_applied
                                .is_none_or(|position| entry.log_id > position)
                        {
                            replay_format(
                                &mut committed_format,
                                &mut committed_membership,
                                &entry,
                                reader,
                            )?;
                        }
                        let expected = previous.map_or(Some(0), |id| id.index.checked_add(1));
                        // An installed snapshot may precede OpenRaft's log purge.
                        // It justifies a missing prefix, never an interior hole.
                        let covered_prefix = first
                            && applied
                                .last_applied
                                .is_some_and(|id| entry.log_id.index <= id.index.saturating_add(1))
                            && purged.is_none_or(|id| entry.log_id.index > id.index);
                        if expected != Some(entry.log_id.index) && !covered_prefix {
                            return Err(failure(
                                ErrorVerb::Read,
                                "noncontiguous persisted Raft logs",
                            ));
                        }
                        previous = Some(entry.log_id);
                        first = false;
                    }
                    if committed > previous.max(applied.last_applied) {
                        return Err(failure(
                            ErrorVerb::Read,
                            "committed Raft position is missing",
                        ));
                    }
                    if let Some(membership) = singleton_membership {
                        validate_singleton_recovery(
                            node_id,
                            recovered_frontier,
                            &membership,
                            singleton_snapshot.as_ref(),
                        )?;
                    }
                }
                Ok((database, created, committed_format, controls))
            })
            .await
            .map_err(|e| failure(ErrorVerb::Read, e))??;
        for (position, control) in controls {
            profile
                .as_ref()
                .ok_or_else(|| {
                    failure(ErrorVerb::Read, "format controls require admitted recovery")
                })?
                .admission
                .knows_persisted(&control, position)
                .map_err(|e| failure(ErrorVerb::Read, e))?;
        }
        Self::from_database(
            database,
            node_id,
            epoch,
            created,
            JournalPersistence::Durable,
            profile,
            &committed_format,
        )
    }

    async fn io<T: Send + 'static>(
        &self,
        kind: MeasuredOperation,
        batch_items: usize,
        operation: impl FnOnce(&Database) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let measurements = self.measurements.clone();
        let measured = measurements.enabled.load(Ordering::Relaxed);
        let queued_at = measured.then(Instant::now);
        if measured {
            let counter = &measurements.operations[kind as usize];
            counter.calls.fetch_add(1, Ordering::Relaxed);
            let items = u64::try_from(batch_items).unwrap_or(u64::MAX);
            counter.batch_items.fetch_add(items, Ordering::Relaxed);
            counter.max_batch_items.fetch_max(items, Ordering::Relaxed);
        }
        let guard = self.queue.clone().lock_owned().await;
        let database = self.database.clone();
        spawn_blocking!("authority_journal_io", move || {
            // A cancelled caller cannot release serialization while I/O continues.
            let _guard = guard;
            if let Some(queued_at) = queued_at {
                let counter = &measurements.operations[kind as usize];
                counter
                    .queue_wait_ns
                    .fetch_add(elapsed_ns(queued_at), Ordering::Relaxed);
                let started = Instant::now();
                let result = operation(&database);
                counter
                    .io_elapsed_ns
                    .fetch_add(elapsed_ns(started), Ordering::Relaxed);
                counter.completed_calls.fetch_add(1, Ordering::Relaxed);
                result
            } else {
                operation(&database)
            }
        })
        .await
        .map_err(|e| failure(ErrorVerb::Read, e))?
    }

    async fn write<T: Send + 'static>(
        &self,
        kind: MeasuredOperation,
        batch_items: usize,
        operation: impl FnOnce(&WriteTransaction) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let fault = self.fault.clone();
        let measurements = self.measurements.clone();
        let persistence = self.persistence;
        let profile = self.profile.clone();
        let epoch = self.epoch;
        let reader = self.reader;
        self.io(kind, batch_items, move |database| {
            let mut tx = database
                .begin_write()
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            tx.set_durability(Durability::Immediate)
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            let result = operation(&tx)?;
            let committed_format = if profile.is_some()
                && matches!(
                    kind,
                    MeasuredOperation::Apply
                        | MeasuredOperation::SaveCommitted
                        | MeasuredOperation::InstallSnapshot
                ) {
                Some(committed_format(&tx, epoch, reader)?)
            } else {
                None
            };
            let cutpoint = fault.swap(0, Ordering::SeqCst);
            if cutpoint == JournalFault::BeforeCommit as u8 {
                return Err(failure(ErrorVerb::Write, "injected before backend commit"));
            }
            let measured = measurements.enabled.load(Ordering::Relaxed);
            let commit_started = measured.then(Instant::now);
            tx.commit().map_err(|e| failure(ErrorVerb::Write, e))?;
            if let (Some(profile), Some(active)) = (profile, committed_format) {
                profile.admission.observe_committed(&active);
            }
            if let Some(started) = commit_started {
                let counter = &measurements.operations[kind as usize];
                let commits = match persistence {
                    JournalPersistence::Durable => &counter.durable_commits,
                    JournalPersistence::Volatile => &counter.volatile_commits,
                };
                commits.fetch_add(1, Ordering::Relaxed);
                counter
                    .commit_elapsed_ns
                    .fetch_add(elapsed_ns(started), Ordering::Relaxed);
            }
            if cutpoint == JournalFault::AfterCommit as u8 {
                return Err(failure(
                    ErrorVerb::Write,
                    "injected after backend commit before reply",
                ));
            }
            Ok(result)
        })
        .await
    }

    async fn read_meta<T: DeserializeOwned + Send + 'static>(
        &self,
        key: &'static str,
    ) -> Result<Option<T>> {
        let epoch = self.epoch;
        let kind = match key {
            "vote" => MeasuredOperation::ReadVote,
            "committed" => MeasuredOperation::ReadCommitted,
            "applied" => MeasuredOperation::ReadApplied,
            "snapshot" => MeasuredOperation::ReadSnapshot,
            _ => return Err(failure(ErrorVerb::Read, "unknown read metadata key")),
        };
        self.io(kind, 0, move |database| {
            let tx = database
                .begin_read()
                .map_err(|e| failure(ErrorVerb::Read, e))?;
            let table = tx
                .open_table(META)
                .map_err(|e| failure(ErrorVerb::Read, e))?;
            table
                .get(key)
                .map_err(|e| failure(ErrorVerb::Read, e))?
                .map(|value| decode(epoch, value.value()))
                .transpose()
        })
        .await
    }

    async fn save_meta<T: Serialize + Sync>(&self, key: &'static str, value: &T) -> Result<()> {
        let bytes = encode(self.epoch, value)?;
        let kind = match key {
            "vote" => MeasuredOperation::SaveVote,
            "committed" => MeasuredOperation::SaveCommitted,
            _ => return Err(failure(ErrorVerb::Write, "unknown saved metadata key")),
        };
        self.write(kind, 0, move |tx| {
            tx.open_table(META)
                .map_err(|e| failure(ErrorVerb::Write, e))?
                .insert(key, bytes.as_slice())
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            Ok(())
        })
        .await
    }

    pub async fn active_format(&self) -> Result<ActiveFormat> {
        let applied: AppliedState = self
            .read_meta("applied")
            .await?
            .ok_or_else(|| failure(ErrorVerb::Read, "missing applied state"))?;
        applied.state(self.epoch, self.reader)?;
        Ok(applied.format)
    }

    /// Trusted in-process qualification seam: derive evidence from admitted
    /// journal profiles after an actual Raft barrier and commit both controls.
    /// An interrupted proposal keeps admission pinned until activation or a
    /// consensus-committed abort resolves it; canceling this future is no abort.
    pub async fn prepare_features(
        &self,
        raft: &Raft<TypeConfig>,
        required: Features,
    ) -> std::result::Result<super::format::Preparation, String> {
        if raft.metrics().borrow().id != self.node_id {
            return Err("activation must use this journal's Raft member".into());
        }
        let profile = self
            .profile
            .as_ref()
            .ok_or("no trusted member admission registry")?;
        loop {
            raft.ensure_linearizable()
                .await
                .map_err(|e| e.to_string())?;
            let pause = self
                .feature_prepare_pause
                .lock()
                .map_err(|_| "feature prepare pause poisoned")?
                .take();
            if let Some(pause) = pause {
                let _ = pause.reached.send(());
                pause.release.await.map_err(|e| e.to_string())?;
            }
            let proposal = self.proposal_gate.lock().await;
            let applied: AppliedState = self
                .read_meta("applied")
                .await
                .map_err(|e| e.to_string())?
                .ok_or("missing applied state")?;
            let prepared = profile.admission.prepare(
                &applied.format,
                &applied.membership,
                required.clone(),
            )?;
            drop(proposal);
            if let Some(prepared) = prepared {
                return Ok(prepared);
            }
            // A previous decision committed after our barrier. Wait for Apply
            // outside the shared gate, then derive the next permit afresh.
        }
    }

    pub async fn activate_features(
        &self,
        raft: &Raft<TypeConfig>,
        required: Features,
    ) -> std::result::Result<(), String> {
        let preparation = self.prepare_features(raft, required).await?;
        self.propose_batch(
            raft,
            Batch::format(FormatControl::Prepare(preparation.clone()))?,
        )
        .await
        .map_err(|e| format!("activation unresolved (permit {}): {e}", preparation.permit))?;
        if let Err(reason) = self.preflight_activation(&preparation).await {
            self.abort_preparation(raft, preparation.permit)
                .await
                .map_err(|error| {
                    format!(
                        "activation unresolved (permit {}): preflight {reason}; abort {error}",
                        preparation.permit
                    )
                })?;
            return Err(format!(
                "activation refused after committed abort (permit {}): {reason}",
                preparation.permit
            ));
        }
        self.propose_batch(
            raft,
            Batch::format(FormatControl::Activate(preparation.clone()))?,
        )
        .await
        .map_err(|e| format!("activation unresolved (permit {}): {e}", preparation.permit))?;
        Ok(())
    }

    async fn preflight_activation(
        &self,
        preparation: &super::format::Preparation,
    ) -> std::result::Result<(), String> {
        let applied: AppliedState = self
            .read_meta("applied")
            .await
            .map_err(|e| e.to_string())?
            .ok_or("missing applied state")?;
        if applied.format.prepared.as_ref() != Some(preparation) {
            return Err("preflight requires the exact applied preparation".into());
        }
        let mut state = applied
            .state(self.epoch, self.reader)
            .map_err(|e| e.to_string())?;
        let mut target = applied.format.clone();
        target.required.clone_from(&preparation.required);
        enable_features(&mut state, &applied.format.required, &target)
    }

    pub async fn abort_preparation(
        &self,
        raft: &Raft<TypeConfig>,
        permit: u64,
    ) -> std::result::Result<(), String> {
        if raft.metrics().borrow().id != self.node_id {
            return Err("abort must use this journal's Raft member".into());
        }
        raft.ensure_linearizable()
            .await
            .map_err(|e| e.to_string())?;
        self.propose_batch(
            raft,
            Batch::format(FormatControl::Abort {
                domain: self.domain,
                permit,
            })?,
        )
        .await
        .map_err(|e| format!("abort unresolved (permit {permit}): {e}"))?;
        Ok(())
    }

    /// A capable current leader resolves an abandoned preparation before serving
    /// writes. An already committed activation remains active; otherwise the
    /// original permit is aborted. Cold recovery never invents a new control
    /// position for an old Activate. A retry uses a fresh preparation.
    pub async fn resolve_preparation(
        &self,
        raft: &Raft<TypeConfig>,
    ) -> std::result::Result<Option<FormatDecision>, String> {
        if raft.metrics().borrow().id != self.node_id {
            return Err("resolution must use this journal's Raft member".into());
        }
        raft.ensure_linearizable()
            .await
            .map_err(|e| e.to_string())?;
        let Some(profile) = &self.profile else {
            return Ok(None);
        };
        let Some(preparation) = profile.admission.pending_preparation()? else {
            return Ok(None);
        };
        self.abort_preparation(raft, preparation.permit).await?;
        profile.admission.decision(preparation.permit)
    }

    /// Canonical application proposal path, shared by singleton and replicated
    /// wrappers. Cancellation after enqueue leaves the command in Raft.
    pub async fn propose_batch(
        &self,
        raft: &Raft<TypeConfig>,
        batch: Batch,
    ) -> std::result::Result<openraft::raft::ClientWriteResponse<TypeConfig>, String> {
        let expected = if batch.control.is_some() {
            1
        } else {
            batch.events.len()
        };
        let receiver = self.enqueue_batch(raft, batch).await?;
        let response = receiver
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        if response.data.replies.len() != expected {
            return Err("batch reply count does not match committed command".into());
        }
        Ok(response)
    }

    /// Guarded fire-and-forget seam for interrupted-proposal qualification.
    /// The shared gate covers admission and the actual Raft enqueue, never the
    /// subsequent response wait. Normal writes pause without becoming failed
    /// committed commands or stopping the automatic coordinator.
    pub async fn enqueue_batch(
        &self,
        raft: &Raft<TypeConfig>,
        batch: Batch,
    ) -> std::result::Result<oneshot::Receiver<openraft::raft::ClientWriteResult<TypeConfig>>, String>
    {
        batch.validate()?;
        if raft.metrics().borrow().id != self.node_id {
            return Err("proposal must use this journal's Raft member".into());
        }
        if batch.control.is_some() {
            raft.ensure_linearizable()
                .await
                .map_err(|e| e.to_string())?;
        }
        let mut changed = self
            .profile
            .as_ref()
            .map(|profile| profile.admission.subscribe());
        let mut metrics = raft.metrics();
        loop {
            if let Some(changed) = &mut changed {
                changed.borrow_and_update();
            }
            let proposal = self.proposal_gate.lock().await;
            if let Some(control) = &batch.control {
                let profile = self
                    .profile
                    .as_ref()
                    .ok_or("no trusted member admission registry")?;
                if let FormatControl::Activate(preparation) = control {
                    self.preflight_activation(preparation).await?;
                }
                let owner = {
                    let metrics = metrics.borrow();
                    if metrics.current_leader != Some(self.node_id) {
                        return Err("control enqueue lost leadership".into());
                    }
                    openraft::CommittedLeaderId::new(metrics.current_term, self.node_id)
                };
                match profile.admission.enqueue_control(control, owner)? {
                    ControlAdmission::Enqueue => {}
                    ControlAdmission::Applied(decision) => {
                        drop(proposal);
                        raft.ensure_linearizable()
                            .await
                            .map_err(|e| e.to_string())?;
                        let (sender, receiver) = oneshot::channel();
                        drop(sender.send(Ok(openraft::raft::ClientWriteResponse {
                            log_id: decision.position,
                            data: BatchReply {
                                replies: vec![Reply::Applied],
                            },
                            membership: None,
                        })));
                        return Ok(receiver);
                    }
                    ControlAdmission::Wait => {
                        drop(proposal);
                        if metrics.borrow().current_leader != Some(self.node_id) {
                            return Err("decision wait lost leadership".into());
                        }
                        let changed = changed
                            .as_mut()
                            .ok_or("decision wait lacks admission registry")?;
                        tokio::select! {
                            result = changed.changed() => result.map_err(|e| e.to_string())?,
                            result = metrics.changed() => result.map_err(|e| e.to_string())?,
                        }
                        raft.ensure_linearizable()
                            .await
                            .map_err(|e| e.to_string())?;
                        continue;
                    }
                }
                // OpenRaft 0.9.25 sends to its unbounded API channel before
                // returning this receiver; dropping it cannot undo that send.
                let receiver = raft
                    .client_write_ff(batch)
                    .await
                    .map_err(|e| e.to_string())?;
                drop(proposal);
                return Ok(receiver);
            }
            let active = self.active_format().await.map_err(|e| e.to_string())?;
            let (required, paused) = self.profile.as_ref().map_or_else(
                || Ok((active.required.clone(), false)),
                |profile| profile.admission.proposal_state(),
            )?;
            if paused {
                drop(proposal);
                if metrics.borrow().current_leader != Some(self.node_id) {
                    return Err("preparation wait lost leadership".into());
                }
                let changed = changed
                    .as_mut()
                    .ok_or("paused proposal lacks admission registry")?;
                tokio::select! {
                    result = changed.changed() => result.map_err(|e| e.to_string())?,
                    result = metrics.changed() => result.map_err(|e| e.to_string())?,
                }
                continue;
            }
            if active.required != required || active.freezes_proposals() {
                // SaveCommitted may have resolved the registry before Apply
                // installs its state. Do not stamp the preceding feature epoch.
                drop(proposal);
                raft.ensure_linearizable()
                    .await
                    .map_err(|e| e.to_string())?;
                continue;
            }
            if batch.events.iter().any(|recorded| {
                (recorded.event.requires_receipt_retirement()
                    && !active.required.contains(&RECEIPT_RETIREMENT))
                    || (recorded.event.requires_accepted_history()
                        && !active.required.contains(&ACCEPTED_HISTORY))
            }) {
                return Err("application command requires an inactive feature".into());
            }
            let receiver = raft
                .client_write_ff(batch.at_feature_epoch(active.feature_epoch))
                .await
                .map_err(|e| e.to_string())?;
            drop(proposal);
            return Ok(receiver);
        }
    }

    /// Consistent backend applied state. Callers serving authority reads must
    /// first pass the real Raft read barrier, as `Singleton::read` does.
    pub async fn read_applied(&self) -> Result<(Option<LogId<u64>>, State)> {
        let applied: AppliedState = self
            .read_meta("applied")
            .await?
            .ok_or_else(|| failure(ErrorVerb::Read, "missing applied state"))?;
        Ok((
            applied.last_applied,
            applied.state(self.epoch, self.reader)?,
        ))
    }

    /// Inject one failure in the next write, for explicit storage cutpoint tests.
    #[doc(hidden)]
    pub fn inject_failure_once(&self, fault: JournalFault) {
        self.fault.store(fault as u8, Ordering::SeqCst);
    }

    /// Deterministic qualification cutpoint after Raft commits history
    /// activation but before its state-machine transaction begins. No state or
    /// proposal gate is held. Dropping the release sender fails the apply.
    #[doc(hidden)]
    pub fn pause_history_apply_once(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached, observed) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let mut slot = self
            .history_apply_pause
            .lock()
            .expect("history apply pause poisoned");
        assert!(slot.is_none(), "history apply pause already armed");
        *slot = Some(HistoryApplyPause {
            reached,
            release: released,
        });
        (observed, release)
    }

    /// Qualification cutpoint after a successful preparation read barrier,
    /// before acquiring the enqueue gate. No registry or I/O lock is held.
    #[doc(hidden)]
    pub fn pause_feature_prepare_once(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached, observed) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let mut slot = self
            .feature_prepare_pause
            .lock()
            .expect("prepare pause poisoned");
        assert!(slot.is_none(), "prepare pause already armed");
        *slot = Some(HistoryApplyPause {
            reached,
            release: released,
        });
        (observed, release)
    }

    /// Enable in-memory measurement for this opened journal and its clones.
    /// Counters begin at zero and reset on reopen; bootstrap is not included
    /// when enabled only after `Singleton::open` returns.
    pub fn enable_measurements(&self) {
        self.measurements.enabled.store(true, Ordering::Relaxed);
    }

    /// Diagnostic snapshot only; concurrent operations may advance individual
    /// counters while it is read. Sample after a read barrier for comparisons.
    pub fn measurements(
        &self,
    ) -> std::collections::BTreeMap<&'static str, JournalOperationMeasurement> {
        MEASURED_OPERATION_NAMES
            .iter()
            .enumerate()
            .map(|(index, name)| {
                let counter = &self.measurements.operations[index];
                (
                    *name,
                    JournalOperationMeasurement {
                        calls: counter.calls.load(Ordering::Relaxed),
                        completed_calls: counter.completed_calls.load(Ordering::Relaxed),
                        durable_commits: counter.durable_commits.load(Ordering::Relaxed),
                        volatile_commits: counter.volatile_commits.load(Ordering::Relaxed),
                        batch_items: counter.batch_items.load(Ordering::Relaxed),
                        max_batch_items: counter.max_batch_items.load(Ordering::Relaxed),
                        queue_wait_ns: counter.queue_wait_ns.load(Ordering::Relaxed),
                        io_elapsed_ns: counter.io_elapsed_ns.load(Ordering::Relaxed),
                        commit_elapsed_ns: counter.commit_elapsed_ns.load(Ordering::Relaxed),
                    },
                )
            })
            .collect()
    }
}

impl RaftLogReader<TypeConfig> for Journal {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: R,
    ) -> Result<Vec<Entry<TypeConfig>>> {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        let epoch = self.epoch;
        self.io(MeasuredOperation::ReadLogs, 0, move |database| {
            let tx = database
                .begin_read()
                .map_err(|e| failure(ErrorVerb::Read, e))?;
            let table = tx
                .open_table(LOGS)
                .map_err(|e| failure(ErrorVerb::Read, e))?;
            table
                .range((start, end))
                .map_err(|e| failure(ErrorVerb::Read, e))?
                .map(|entry| {
                    let (key, value) = entry.map_err(|e| failure(ErrorVerb::Read, e))?;
                    decode_log(epoch, key.value(), value.value())
                })
                .collect()
        })
        .await
    }
}

impl RaftLogStorage<TypeConfig> for Journal {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>> {
        let epoch = self.epoch;
        self.io(MeasuredOperation::GetLogState, 0, move |database| {
            let tx = database
                .begin_read()
                .map_err(|e| failure(ErrorVerb::Read, e))?;
            let meta = tx
                .open_table(META)
                .map_err(|e| failure(ErrorVerb::Read, e))?;
            let last_purged_log_id: Option<LogId<u64>> = meta
                .get("purged")
                .map_err(|e| failure(ErrorVerb::Read, e))?
                .map(|value| decode(epoch, value.value()))
                .transpose()?;
            let logs = tx
                .open_table(LOGS)
                .map_err(|e| failure(ErrorVerb::Read, e))?;
            let last_log_id = logs
                .last()
                .map_err(|e| failure(ErrorVerb::Read, e))?
                .map(|(key, value)| {
                    decode_log(epoch, key.value(), value.value()).map(|entry| entry.log_id)
                })
                .transpose()?
                .or(last_purged_log_id);
            Ok(LogState {
                last_purged_log_id,
                last_log_id,
            })
        })
        .await
    }

    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<()> {
        self.save_meta("vote", vote).await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>> {
        if let Some(profile) = &self.profile {
            profile
                .admission
                .recovery_ready()
                .map_err(|e| failure(ErrorVerb::Read, e))?;
        }
        self.read_meta("vote").await
    }

    async fn save_committed(&mut self, committed: Option<LogId<u64>>) -> Result<()> {
        self.save_meta("committed", &committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>> {
        Ok(self
            .read_meta::<Option<LogId<u64>>>("committed")
            .await?
            .flatten())
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> Result<()>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let epoch = self.epoch;
        let entries: Vec<_> = entries.into_iter().collect();
        let profile = self.profile.clone();
        let reader = self.reader;
        let result = async {
            // Validate every command and membership before beginning the write.
            // The completion callback must receive validation failures too.
            for entry in &entries {
                validate_entry(entry, ErrorVerb::Write)?;
            }
            self.write(MeasuredOperation::Append, entries.len(), move |tx| {
                let admitted_features = profile
                    .as_ref()
                    .map(|profile| profile.admission.proposal_state())
                    .transpose()
                    .map_err(|e| failure(ErrorVerb::Write, e))?
                    .is_some_and(|(required, paused)| !required.is_empty() || paused);
                validate_feature_append(tx, epoch, reader, &entries, admitted_features)?;
                for entry in &entries {
                    match &entry.payload {
                        EntryPayload::Normal(batch) if batch.control.is_some() => {
                            profile
                                .as_ref()
                                .ok_or_else(|| {
                                    failure(
                                        ErrorVerb::Write,
                                        "format control requires admitted member evidence",
                                    )
                                })?
                                .admission
                                .authorize(
                                    batch.control.as_ref().expect("checked control"),
                                    entry.log_id,
                                )
                                .map_err(|e| failure(ErrorVerb::Write, e))?;
                        }
                        EntryPayload::Membership(membership) => {
                            if let Some(profile) = &profile {
                                profile
                                    .admission
                                    .membership_allowed(entry.log_id, membership)
                                    .map_err(|e| failure(ErrorVerb::Write, e))?;
                            }
                        }
                        _ => {}
                    }
                }
                let mut table = tx
                    .open_table(LOGS)
                    .map_err(|e| failure(ErrorVerb::Write, e))?;
                for entry in entries {
                    let bytes = encode(epoch, &entry)?;
                    if bytes.len() > MAX_LOG_RECORD_BYTES {
                        return Err(failure(
                            ErrorVerb::Write,
                            "encoded log record exceeds byte bound",
                        ));
                    }
                    table
                        .insert(entry.log_id.index, bytes.as_slice())
                        .map_err(|e| failure(ErrorVerb::Write, e))?;
                }
                Ok(())
            })
            .await
        }
        .await;
        // The callback can precede append's return, but never backend commit.
        // Only JournalPersistence::Durable carries a restart durability claim.
        callback.log_io_completed(match &result {
            Ok(()) => Ok(()),
            Err(error) => Err(io::Error::other(error.to_string())),
        });
        result
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<()> {
        self.write(MeasuredOperation::Truncate, 0, move |tx| {
            let mut table = tx
                .open_table(LOGS)
                .map_err(|e| failure(ErrorVerb::Delete, e))?;
            let keys: Vec<u64> = table
                .range(log_id.index..)
                .map_err(|e| failure(ErrorVerb::Read, e))?
                .map(|entry| {
                    entry
                        .map(|(key, _)| key.value())
                        .map_err(|e| failure(ErrorVerb::Read, e))
                })
                .collect::<Result<_>>()?;
            for key in keys {
                table
                    .remove(key)
                    .map_err(|e| failure(ErrorVerb::Delete, e))?;
            }
            Ok(())
        })
        .await
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<()> {
        let encoded = encode(self.epoch, &log_id)?;
        self.write(MeasuredOperation::Purge, 0, move |tx| {
            let mut table = tx
                .open_table(LOGS)
                .map_err(|e| failure(ErrorVerb::Delete, e))?;
            let keys: Vec<u64> = table
                .range(..=log_id.index)
                .map_err(|e| failure(ErrorVerb::Read, e))?
                .map(|entry| {
                    entry
                        .map(|(key, _)| key.value())
                        .map_err(|e| failure(ErrorVerb::Read, e))
                })
                .collect::<Result<_>>()?;
            for key in keys {
                table
                    .remove(key)
                    .map_err(|e| failure(ErrorVerb::Delete, e))?;
            }
            tx.open_table(META)
                .map_err(|e| failure(ErrorVerb::Write, e))?
                .insert("purged", encoded.as_slice())
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            Ok(())
        })
        .await
    }
}

impl RaftStateMachine<TypeConfig> for Journal {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>)> {
        let applied: AppliedState = self
            .read_meta("applied")
            .await?
            .ok_or_else(|| failure(ErrorVerb::Read, "missing applied state"))?;
        applied.state(self.epoch, self.reader)?;
        Ok((applied.last_applied, applied.membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<BatchReply>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        // Revalidate every batch and membership before applying the first event. This is
        // required at the committed boundary, independently of RPC constructors
        // and deserialization. Malformed later entries cannot partially apply.
        for entry in &entries {
            validate_entry(entry, ErrorVerb::Write)?;
        }
        let pause = if entries.iter().any(|entry| matches!(&entry.payload,
            EntryPayload::Normal(batch) if matches!(&batch.control,
                Some(FormatControl::Activate(preparation)) if preparation.required.contains(&ACCEPTED_HISTORY)))) {
            self.history_apply_pause.lock().expect("history apply pause poisoned").take()
        } else { None };
        if let Some(pause) = pause {
            let _observed = pause.reached.send(());
            pause
                .release
                .await
                .map_err(|e| failure(ErrorVerb::Write, e))?;
        }
        let epoch = self.epoch;
        let reader = self.reader;
        self.write(MeasuredOperation::Apply, entries.len(), move |tx| {
            let mut table = tx
                .open_table(META)
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            let mut applied: AppliedState = {
                let bytes = table
                    .get("applied")
                    .map_err(|e| failure(ErrorVerb::Read, e))?
                    .ok_or_else(|| failure(ErrorVerb::Read, "missing applied state"))?;
                decode(epoch, bytes.value())?
            };
            let mut state = applied.state(epoch, reader)?;
            let mut replies = Vec::with_capacity(entries.len());
            for entry in entries {
                replies.push(replay_state(&mut applied, &mut state, &entry, reader)?);
            }
            applied.scheduler = SchedulerSection::from_state(&state, &applied.format)?;
            applied.state(epoch, reader)?;
            let bytes = encode(epoch, &applied)?;
            table
                .insert("applied", bytes.as_slice())
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            Ok(replies)
        })
        .await
    }

    async fn get_snapshot_builder(&mut self) -> Self {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<SnapshotBuffer>> {
        Ok(Box::default())
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<SnapshotBuffer>,
    ) -> Result<()> {
        validate_snapshot_meta(meta)?;
        let bytes = snapshot
            .into_inner()
            .map_err(|e| failure(ErrorVerb::Read, e))?;
        let saved: SavedSnapshot = decode(self.epoch, &bytes)?;
        saved.validate(self.epoch, self.reader)?;
        if saved.applied.format.domain != self.domain {
            return Err(failure(ErrorVerb::Read, "foreign snapshot namespace"));
        }
        if &saved.meta != meta {
            return Err(failure(
                ErrorVerb::Read,
                "received snapshot metadata mismatch",
            ));
        }
        let applied = encode(self.epoch, &saved.applied)?;
        self.write(MeasuredOperation::InstallSnapshot, 0, move |tx| {
            let mut table = tx
                .open_table(META)
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            table
                .insert("applied", applied.as_slice())
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            table
                .insert("snapshot", bytes.as_slice())
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            Ok(())
        })
        .await
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>> {
        let saved: Option<SavedSnapshot> = self.read_meta("snapshot").await?;
        saved
            .map(|saved| {
                saved.validate(self.epoch, self.reader)?;
                if saved.applied.format.domain != self.domain {
                    return Err(failure(ErrorVerb::Read, "foreign snapshot namespace"));
                }
                let bytes = encode(self.epoch, &saved)?;
                Ok(Snapshot {
                    meta: saved.meta,
                    snapshot: Box::new(
                        SnapshotBuffer::from_bytes(bytes)
                            .map_err(|e| failure(ErrorVerb::Read, e))?,
                    ),
                })
            })
            .transpose()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Journal {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>> {
        let epoch = self.epoch;
        let node_id = self.node_id;
        let reader = self.reader;
        self.write(MeasuredOperation::BuildSnapshot, 0, move |tx| {
            let mut table = tx
                .open_table(META)
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            let applied: AppliedState = {
                let bytes = table
                    .get("applied")
                    .map_err(|e| failure(ErrorVerb::Read, e))?
                    .ok_or_else(|| failure(ErrorVerb::Read, "missing applied state"))?;
                decode(epoch, bytes.value())?
            };
            let sequence = table
                .get("snapshot_sequence")
                .map_err(|e| failure(ErrorVerb::Read, e))?
                .map(|value| decode::<u64>(epoch, value.value()))
                .transpose()?
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| failure(ErrorVerb::Write, "snapshot sequence exhausted"))?;
            let saved = SavedSnapshot {
                meta: SnapshotMeta {
                    last_log_id: applied.last_applied,
                    last_membership: applied.membership.clone(),
                    snapshot_id: format!("{}-{node_id}-{sequence}", epoch.0),
                },
                applied,
            };
            saved.validate(epoch, reader)?;
            let bytes = encode(epoch, &saved)?;
            table
                .insert("snapshot", bytes.as_slice())
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            let sequence = encode(epoch, &sequence)?;
            table
                .insert("snapshot_sequence", sequence.as_slice())
                .map_err(|e| failure(ErrorVerb::Write, e))?;
            Ok(Snapshot {
                meta: saved.meta,
                snapshot: Box::new(
                    SnapshotBuffer::from_bytes(bytes).map_err(|e| failure(ErrorVerb::Write, e))?,
                ),
            })
        })
        .await
    }
}

/// No peer transport is qualified by this singleton adapter.
#[derive(Clone, Copy, Debug)]
pub struct SingletonNetwork;

fn unreachable_peer<E: std::error::Error>() -> RPCError<u64, BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&io::Error::new(
        io::ErrorKind::NotConnected,
        "experimental singleton has no peer transport",
    )))
}

impl RaftNetworkFactory<TypeConfig> for SingletonNetwork {
    type Network = Self;
    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self {
        *self
    }
}

impl RaftNetwork<TypeConfig> for SingletonNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>
    {
        Err(unreachable_peer())
    }
    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        Err(unreachable_peer())
    }
    async fn vote(
        &mut self,
        _rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> std::result::Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(unreachable_peer())
    }
}

/// Experimental one-member real Raft service; no production RPC is exposed.
pub struct Singleton {
    pub raft: Raft<TypeConfig>,
    pub journal: Journal,
}

#[cfg(test)]
struct SingletonStartupGate {
    reached: oneshot::Sender<Raft<TypeConfig>>,
    resume: oneshot::Receiver<()>,
}

impl Debug for Singleton {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Singleton")
            .field("journal", &self.journal)
            .finish_non_exhaustive()
    }
}

impl Singleton {
    /// Submit one event through the same bounded batch and real Raft path.
    pub async fn write(
        &self,
        event: RecordedEvent,
    ) -> std::result::Result<SingleWriteResponse, String> {
        let response = self.write_batch(Batch::new([event])?).await?;
        let data = response
            .data
            .replies
            .into_iter()
            .next()
            .ok_or("single event reply is missing")?;
        Ok(SingleWriteResponse {
            log_id: response.log_id,
            data,
        })
    }

    /// Commit one explicitly bounded group. The caller supplies event order;
    /// this API neither weakens durability nor publishes intermediate effects.
    pub async fn write_batch(
        &self,
        batch: Batch,
    ) -> std::result::Result<openraft::raft::ClientWriteResponse<TypeConfig>, String> {
        self.journal.propose_batch(&self.raft, batch).await
    }

    pub async fn open(
        path: impl AsRef<Path>,
        node_id: u64,
        epoch: ClusterEpoch,
        limits: Limits,
    ) -> std::result::Result<Self, String> {
        let journal = Journal::open_inner(
            path,
            node_id,
            epoch,
            limits,
            None,
            RecoveryExpectation::Singleton,
        )
        .await
        .map_err(|e| e.to_string())?;
        Self::from_journal(
            journal,
            #[cfg(test)]
            None,
        )
        .await
    }

    pub async fn open_with_profile(
        path: impl AsRef<Path>,
        node_id: u64,
        limits: Limits,
        profile: JournalProfile,
    ) -> std::result::Result<Self, String> {
        let journal = Journal::open_inner(
            path,
            node_id,
            profile.domain.epoch,
            limits,
            Some(profile),
            RecoveryExpectation::Singleton,
        )
        .await
        .map_err(|e| e.to_string())?;
        Self::from_journal(
            journal,
            #[cfg(test)]
            None,
        )
        .await
    }

    /// Open through a custody-retaining journal factory and real singleton Raft.
    /// Existing journals must recover exactly this node with no pending
    /// membership change, before profile registration or Raft startup. This
    /// does not authorize canonical bootstrap, fence old members, fix lifetime
    /// membership, or certify clock interpretation/closed-member recovery.
    ///
    /// Once polled, preparation owns startup independently of this caller. If
    /// the caller disappears before accepting the result, preparation shuts down
    /// Raft before releasing its journal custody. Cancellation may leave a valid
    /// initialized journal; it never rolls back initialization or adopts it anew.
    pub async fn open_from_factory(
        binding: JournalBinding,
        node_id: u64,
        limits: Limits,
        profile: JournalProfile,
        factory: impl FnOnce() -> io::Result<JournalDatabase> + Send + 'static,
    ) -> std::result::Result<Self, String> {
        let (prepared, accepted) = Self::prepare_from_factory(
            binding,
            node_id,
            limits,
            profile,
            factory,
            #[cfg(test)]
            None,
        );
        let result = prepared
            .await
            .map_err(|error| format!("Singleton preparation task failed: {error}"))?;
        if result.is_ok() {
            // No await separates receiving ownership, acknowledging the handoff
            // and returning it. A queued result abandoned by cancellation never
            // sends this acknowledgement, so preparation remains responsible.
            let _accepted = accepted.send(());
        }
        result
    }

    fn prepare_from_factory(
        binding: JournalBinding,
        node_id: u64,
        limits: Limits,
        profile: JournalProfile,
        factory: impl FnOnce() -> io::Result<JournalDatabase> + Send + 'static,
        #[cfg(test)] gate: Option<SingletonStartupGate>,
    ) -> (
        oneshot::Receiver<std::result::Result<Self, String>>,
        oneshot::Sender<()>,
    ) {
        let (prepared, receiver) = oneshot::channel();
        let (accepted, acceptance) = oneshot::channel();
        let _prepare = background_spawn!("authority_singleton_prepare", async move {
            let result = async {
                let journal = Journal::open_from_factory_with_expectation(
                    binding,
                    node_id,
                    limits,
                    profile,
                    RecoveryExpectation::Singleton,
                    factory,
                )
                .await
                .map_err(|error| error.to_string())?;
                Self::from_journal(
                    journal,
                    #[cfg(test)]
                    gate,
                )
                .await
            }
            .await;
            // Retain both custody and a shutdown handle until the caller takes
            // ownership. Sending into an unpolled oneshot is not that handoff.
            let retained = result
                .as_ref()
                .ok()
                .map(|authority| (authority.raft.clone(), authority.journal.clone()));
            let delivered = prepared.send(result).is_ok();
            if let Some((raft, _journal)) = retained
                && (!delivered || acceptance.await.is_err())
            {
                let _shutdown = raft.shutdown().await;
            }
        });
        (receiver, accepted)
    }

    async fn from_journal(
        journal: Journal,
        #[cfg(test)] gate: Option<SingletonStartupGate>,
    ) -> std::result::Result<Self, String> {
        let node_id = journal.node_id;
        let config = openraft::Config {
            cluster_name: format!("experimental-authority-{}", journal.epoch.0),
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        }
        .validate()
        .map_err(|e| e.to_string())?;
        let raft = Raft::new(
            node_id,
            Arc::new(config),
            SingletonNetwork,
            journal.clone(),
            journal.clone(),
        )
        .await
        .map_err(|e| e.to_string())?;
        #[cfg(test)]
        if let Some(gate) = gate
            && (gate.reached.send(raft.clone()).is_err() || gate.resume.await.is_err())
        {
            let _shutdown = raft.shutdown().await;
            return Err("singleton startup test gate closed".to_owned());
        }
        let initialize = if journal.created {
            raft.initialize(BTreeSet::from([node_id]))
                .await
                .map_err(|e| e.to_string())
        } else {
            match raft.is_initialized().await {
                Ok(true) => Ok(()),
                Ok(false) => Err("existing journal lacks initialized membership; explicit bootstrap recovery required".to_owned()),
                Err(error) => Err(error.to_string()),
            }
        };
        if let Err(error) = initialize {
            let _shutdown = raft.shutdown().await;
            return Err(error);
        }
        if let Err(error) = raft
            .wait(Some(Duration::from_secs(5)))
            .current_leader(node_id, "singleton leader")
            .await
        {
            let _shutdown = raft.shutdown().await;
            return Err(error.to_string());
        }
        Ok(Self { raft, journal })
    }

    /// Barrier first, then a consistent persisted read at or after its position.
    pub async fn read(&self) -> std::result::Result<State, String> {
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|e| e.to_string())?;
        self.journal
            .read_applied()
            .await
            .map(|(_, state)| state)
            .map_err(|e| e.to_string())
    }

    pub async fn shutdown(self) -> std::result::Result<(), String> {
        self.raft.shutdown().await.map_err(|e| e.to_string())
    }
}

#[cfg(test)]
#[path = "journal_factory_tests.rs"]
mod factory_tests;

#[cfg(test)]
mod batch_validation_tests {
    use super::*;
    use crate::authority::{EffectId, Event, ResultPayload};

    #[test]
    fn fixed_snapshot_metadata_and_nested_escape_budget_fit_receive_bound() {
        const METADATA_MARGIN: usize = 16 * 1024;
        let position = Some(LogId::new(
            openraft::CommittedLeaderId::new(u64::MAX, u64::MAX),
            u64::MAX,
        ));
        let membership = StoredMembership::new(position, Membership::default());
        let saved = SavedSnapshot {
            meta: SnapshotMeta {
                last_log_id: position,
                last_membership: membership.clone(),
                snapshot_id: "\0".repeat(MAX_SNAPSHOT_ID_BYTES),
            },
            applied: AppliedState {
                last_applied: position,
                membership,
                scheduler: SchedulerSection::String(String::new()),
                format: ActiveFormat::base(AuthorityDomain {
                    epoch: ClusterEpoch(u64::MAX),
                    namespace: u64::MAX,
                }),
            },
        };
        // Includes worst scalar widths, all wrappers/checksum/section names,
        // two memberships' fixed framing, and the maximally escaped ID. This
        // shape check does not assert that an empty scheduler is valid state.
        assert!(encode(ClusterEpoch(u64::MAX), &saved).unwrap().len() <= METADATA_MARGIN);
        // Core JSON is nested as a string twice; each of the two canonical
        // memberships is nested once. Their encoded byte limits pay for all
        // variable fields, including escaped node addresses and map keys.
        let budget = 4 * crate::authority::MAX_STATE_BYTES
            + 4 * MAX_MEMBERSHIP_BYTES
            + 2 * MAX_FORMAT_BYTES
            + METADATA_MARGIN;
        assert!(budget <= MAX_SNAPSHOT_BYTES);
    }

    #[nativelink_macro::nativelink_test]
    async fn malformed_later_batch_cannot_partially_apply_an_earlier_entry() {
        let directory =
            std::env::temp_dir().join(format!("authority-invalid-batch-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let epoch = ClusterEpoch(17);
        let mut journal = Journal::open(directory.join("state.redb"), 1, epoch, Limits::default())
            .await
            .unwrap();
        let event = RecordedEvent {
            epoch,
            event: Event::AdvanceTime { now: 50 },
        };
        let valid = Batch::new([event.clone()]).unwrap();
        let oversized = RecordedEvent {
            epoch,
            event: Event::LookupResult {
                effect: EffectId(1),
                result: Some(ResultPayload {
                    digest: "x".repeat(MAX_BATCH_BYTES),
                    exit_code: 0,
                    error: None,
                    response: None,
                }),
            },
        };
        let malformed = [
            Batch {
                control: None,
                feature_epoch: 0,
                version: 99,
                events: vec![event.clone()],
            },
            Batch {
                control: None,
                feature_epoch: 0,
                version: BATCH_VERSION,
                events: vec![],
            },
            Batch {
                control: None,
                feature_epoch: 0,
                version: BATCH_VERSION,
                events: vec![event; MAX_BATCH_EVENTS + 1],
            },
            Batch {
                control: None,
                feature_epoch: 0,
                version: BATCH_VERSION,
                events: vec![oversized],
            },
        ];
        let initial = journal.read_applied().await.unwrap();
        for invalid in malformed {
            let entries = [valid.clone(), invalid]
                .into_iter()
                .enumerate()
                .map(|(index, batch)| Entry {
                    log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index as u64 + 1),
                    payload: EntryPayload::Normal(batch),
                });
            assert!(journal.apply(entries).await.is_err());
            assert_eq!(journal.read_applied().await.unwrap(), initial);
        }
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
