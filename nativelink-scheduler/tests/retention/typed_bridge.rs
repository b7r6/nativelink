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

//! Singleton test composition, not a production authority or publication path.
//! The finite candidate and independent oracle remain unchanged. This wrapper
//! owns a bounded catalogue, preparation tickets, exact-key manifests and one
//! experimental head, and checkpoints their composition before issuing grants.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use nativelink_store::physical_closure::{
    Binding, Failure as ClosureFailure, Limits as ClosureLimits, Preparation, VerifiedReader,
};
use nativelink_store::physical_filesystem::{
    Algorithm, DeleteReceipt, Failure as PhysicalFailure, Incarnation, Limits as PhysicalLimits,
    ObjectId, PhysicalFilesystem, PhysicalKey, WriteReceipt,
};
use serde::{Deserialize, Serialize};

use super::coarse::Coarse;
use super::protocol::*;
use super::reference::Reference;

const CHECKPOINT_MAX: usize = 1024 * 1024;
const HEADER: usize = 48;
const MAGIC: &[u8; 8] = b"TYPEDR01";
const PREPARATIONS: usize = 2;
const PREPARATION_TICKS: u64 = 5;
// The authority protocol's u64 incarnation is a handle, never truncated physical
// identity. One persisted handle maps to this full 128-bit physical incarnation.
const NAMESPACE_HANDLE: u64 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Error {
    Rejected,
    StaleCatalogue,
    Expired,
    Capacity,
    CorruptCheckpoint,
    Storage,
    Ambiguous,
    Poisoned,
    Closure(ClosureFailure),
    Physical(PhysicalFailure),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum Cut {
    #[default]
    None,
    BeforeRename,
    AfterRename,
    AfterDurability,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Descriptor {
    algorithm: u8,
    hash: [u8; 32],
    size: u64,
}

impl From<ObjectId> for Descriptor {
    fn from(object: ObjectId) -> Self {
        Self {
            algorithm: match object.algorithm {
                Algorithm::Sha256 => 1,
                Algorithm::Blake3 => 2,
            },
            hash: object.hash,
            size: object.size,
        }
    }
}

impl Descriptor {
    const fn object(self) -> Result<ObjectId, Error> {
        Ok(ObjectId {
            algorithm: match self.algorithm {
                1 => Algorithm::Sha256,
                2 => Algorithm::Blake3,
                _ => return Err(Error::CorruptCheckpoint),
            },
            hash: self.hash,
            size: self.size,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Row {
    pub(crate) key: Key,
    physical_incarnation: [u8; 16],
    physical_generation: u64,
    descriptor: Descriptor,
    logical_object: u64,
}

impl Row {
    pub(crate) fn binding(self) -> Binding {
        Binding {
            object: self.descriptor.object().unwrap(),
            key: PhysicalKey::new(
                Incarnation::new(self.physical_incarnation).unwrap(),
                self.physical_generation,
            )
            .unwrap(),
        }
    }

    fn validate(self, view: &View, namespace: [u8; 16]) -> Result<(), Error> {
        let generation = view
            .generations
            .get(&self.key)
            .ok_or(Error::CorruptCheckpoint)?;
        if self.key.incarnation != NAMESPACE_HANDLE
            || self.physical_incarnation != namespace
            || self.physical_generation != self.key.generation
            || self.physical_generation == 0
            || self.logical_object == 0
            || self.descriptor.algorithm != 1
            || generation.logical_object != self.logical_object
            || self.descriptor.object()?.size > PhysicalLimits::default().object_bytes
        {
            return Err(Error::CorruptCheckpoint);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    result: Descriptor,
    catalogue_revision: u64,
    objects: Vec<Row>,
}

impl Manifest {
    fn keys(&self) -> Vec<Key> {
        self.objects.iter().map(|row| row.key).collect()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    namespace_handle: u64,
    namespace: [u8; 16],
    sequence: u64,
    revision: u64,
    next_generation: u64,
    next_logical_object: u64,
    catalogue: Vec<Row>,
    // Canonical vectors reject duplicate identities rather than normalizing them.
    manifests: Vec<(RootId, Manifest)>,
    head: Option<RootId>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    version: u32,
    candidate: Vec<u8>,
    oracle: Vec<u8>,
    metadata: Metadata,
}

#[derive(Clone)]
struct State {
    candidate: Coarse,
    oracle: Reference,
    metadata: Metadata,
}

impl State {
    fn apply(&mut self, event: Event) -> Result<Reply, Error> {
        let before = self.candidate.view();
        let actual = self.candidate.apply(event.clone());
        let expected = self.oracle.apply(event);
        assert_eq!(actual, expected, "candidate/oracle transition disagreement");
        self.oracle.check().unwrap();
        assert_eq!(self.candidate.view(), self.oracle.view());
        if actual == Reply::Rejected {
            assert_eq!(self.candidate.view(), before);
            return Err(Error::Rejected);
        }
        Ok(actual)
    }

    fn manifest(&self, root: RootId) -> Result<&Manifest, Error> {
        self.metadata
            .manifests
            .iter()
            .find_map(|(id, manifest)| (*id == root).then_some(manifest))
            .ok_or(Error::Rejected)
    }

    fn fences(&self, keys: &[Key]) -> BTreeMap<CohortId, u64> {
        let view = self.candidate.view();
        keys.iter()
            .map(|key| {
                let cohort = view.generations[key].cohort;
                (cohort, view.cohorts[&cohort].fence)
            })
            .collect()
    }

    fn validate(&self) -> Result<(), Error> {
        let view = self.candidate.view();
        let metadata = &self.metadata;
        if view != self.oracle.view()
            || view.limits != Limits::default()
            || metadata.namespace_handle != NAMESPACE_HANDLE
            || metadata.namespace == [0; 16]
            || metadata.revision == 0
            || metadata.next_generation == 0
            || metadata.next_logical_object == 0
            || metadata.catalogue.len() > view.limits.generations
            || metadata.manifests.len() > view.limits.roots
            || !metadata
                .catalogue
                .windows(2)
                .all(|pair| pair[0].descriptor < pair[1].descriptor)
            || !metadata
                .manifests
                .windows(2)
                .all(|pair| pair[0].0 < pair[1].0)
            || view.generations.keys().any(|key| {
                key.incarnation != NAMESPACE_HANDLE
                    || key.generation == 0
                    || key.generation >= metadata.next_generation
            })
            || view
                .generations
                .values()
                .any(|generation| generation.logical_object >= metadata.next_logical_object)
        {
            return Err(Error::CorruptCheckpoint);
        }
        let mut keys = BTreeSet::new();
        for row in &metadata.catalogue {
            row.validate(&view, metadata.namespace)?;
            if !keys.insert(row.key) {
                return Err(Error::CorruptCheckpoint);
            }
        }
        for (root_id, manifest) in &metadata.manifests {
            let root = view.roots.get(root_id).ok_or(Error::CorruptCheckpoint)?;
            if !matches!(root.kind, RootKind::Publication | RootKind::Serving)
                || manifest.objects.is_empty()
                || manifest.objects.len() > view.limits.closure
                || manifest.catalogue_revision == 0
                || manifest.catalogue_revision > metadata.revision
                || !manifest
                    .objects
                    .windows(2)
                    .all(|pair| pair[0].key < pair[1].key)
                || root.closure != manifest.keys()
                || !manifest
                    .objects
                    .iter()
                    .any(|row| row.descriptor == manifest.result)
            {
                return Err(Error::CorruptCheckpoint);
            }
            for row in &manifest.objects {
                row.validate(&view, metadata.namespace)?;
            }
        }
        for (id, root) in &view.roots {
            if matches!(root.kind, RootKind::Publication | RootKind::Serving) {
                self.manifest(*id).map_err(|_| Error::CorruptCheckpoint)?;
            }
        }
        let mut by_key = BTreeMap::new();
        let mut by_logical = BTreeMap::new();
        let mut by_descriptor = BTreeMap::new();
        for row in metadata.catalogue.iter().chain(
            metadata
                .manifests
                .iter()
                .flat_map(|(_, manifest)| &manifest.objects),
        ) {
            if by_key.insert(row.key, *row).is_some_and(|old| old != *row)
                || by_logical
                    .insert(row.logical_object, row.descriptor)
                    .is_some_and(|old| old != row.descriptor)
                || by_descriptor
                    .insert(row.descriptor, row.logical_object)
                    .is_some_and(|old| old != row.logical_object)
            {
                return Err(Error::CorruptCheckpoint);
            }
        }
        if let Some(head) = metadata.head
            && view
                .roots
                .get(&head)
                .is_none_or(|root| root.kind != RootKind::Publication)
        {
            return Err(Error::CorruptCheckpoint);
        }
        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let payload = serde_json::to_vec(&Checkpoint {
            version: 1,
            candidate: self
                .candidate
                .snapshot()
                .map_err(|_| Error::CorruptCheckpoint)?,
            oracle: self
                .oracle
                .snapshot()
                .map_err(|_| Error::CorruptCheckpoint)?,
            metadata: self.metadata.clone(),
        })
        .map_err(|_| Error::CorruptCheckpoint)?;
        if payload.len() > CHECKPOINT_MAX - HEADER {
            return Err(Error::Capacity);
        }
        let mut bytes = Vec::with_capacity(HEADER + payload.len());
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&ObjectId::for_bytes(Algorithm::Sha256, &payload).hash);
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < HEADER || bytes.len() > CHECKPOINT_MAX || &bytes[..8] != MAGIC {
            return Err(Error::CorruptCheckpoint);
        }
        let length = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let payload = &bytes[HEADER..];
        if length != payload.len() as u64
            || bytes[16..HEADER] != ObjectId::for_bytes(Algorithm::Sha256, payload).hash
        {
            return Err(Error::CorruptCheckpoint);
        }
        let saved: Checkpoint =
            serde_json::from_slice(payload).map_err(|_| Error::CorruptCheckpoint)?;
        if saved.version != 1 {
            return Err(Error::CorruptCheckpoint);
        }
        let state = Self {
            candidate: Coarse::restore(&saved.candidate).map_err(|_| Error::CorruptCheckpoint)?,
            oracle: Reference::restore(&saved.oracle).map_err(|_| Error::CorruptCheckpoint)?,
            metadata: saved.metadata,
        };
        // In addition to invariant checks, reject duplicate/unknown fields and
        // noncanonical encodings rather than silently normalizing a checkpoint.
        if state.encode()? != bytes {
            return Err(Error::CorruptCheckpoint);
        }
        Ok(state)
    }
}

struct Permit(Arc<AtomicUsize>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) struct Pending<B: PhysicalBackend = PhysicalFilesystem> {
    preparation: Preparation<B>,
    revision: u64,
    deadline: u64,
    fences: BTreeMap<CohortId, u64>,
    _permit: Permit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Grant {
    namespace: [u8; 16],
    root: RootId,
    lease: u64,
    until: u64,
}

/// Only physical effects vary; catalogue, authority, oracle and grant checks
/// remain the same test composition for every explicitly supplied backend.
pub(crate) trait PhysicalBackend: VerifiedReader {
    fn write(
        &self,
        key: PhysicalKey,
        write: u64,
        object: ObjectId,
        bytes: &[u8],
    ) -> Result<WriteReceipt, PhysicalFailure>;

    fn delete(&self, key: PhysicalKey, delete: u64) -> Result<DeleteReceipt, PhysicalFailure>;
}

impl PhysicalBackend for PhysicalFilesystem {
    fn write(
        &self,
        key: PhysicalKey,
        write: u64,
        object: ObjectId,
        bytes: &[u8],
    ) -> Result<WriteReceipt, PhysicalFailure> {
        let mut pending = self.begin_write(key, write, object)?;
        pending.append(bytes)?;
        pending.finish()
    }

    fn delete(&self, key: PhysicalKey, delete: u64) -> Result<DeleteReceipt, PhysicalFailure> {
        Self::delete(self, key, delete)
    }
}

pub(crate) struct Bridge<B: PhysicalBackend = PhysicalFilesystem> {
    state: State,
    pub(crate) store: B,
    checkpoint: PathBuf,
    _lock: File,
    pending: Arc<AtomicUsize>,
    cut: Cut,
    poisoned: bool,
}

impl Bridge<PhysicalFilesystem> {
    pub(crate) fn bootstrap(base: &Path) -> Result<Self, Error> {
        Self::bootstrap_with(base, |incarnation| {
            PhysicalFilesystem::bootstrap(
                &base.join("physical"),
                incarnation,
                PhysicalLimits::default(),
            )
        })
    }

    pub(crate) fn reopen(base: &Path) -> Result<Self, Error> {
        Self::reopen_with(base, |incarnation| {
            PhysicalFilesystem::open(&base.join("physical"), incarnation)
        })
    }
}

impl<B: PhysicalBackend> Bridge<B> {
    pub(crate) fn bootstrap_with(
        base: &Path,
        create: impl FnOnce(Incarnation) -> Result<B, PhysicalFailure>,
    ) -> Result<Self, Error> {
        let namespace = *uuid::Uuid::new_v4().as_bytes();
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(base.join("authority/owner"))
            .map_err(|_| Error::Storage)?;
        lock.try_lock().map_err(|_| Error::Storage)?;
        let bridge = Self {
            state: State {
                candidate: Coarse::new(Limits::default()).unwrap(),
                oracle: Reference::new(Limits::default()).unwrap(),
                metadata: Metadata {
                    namespace_handle: NAMESPACE_HANDLE,
                    namespace,
                    sequence: 0,
                    revision: 1,
                    next_generation: 1,
                    next_logical_object: 1,
                    catalogue: Vec::new(),
                    manifests: Vec::new(),
                    head: None,
                },
            },
            store: create(Incarnation::new(namespace).unwrap()).map_err(Error::Physical)?,
            checkpoint: base.join("authority/checkpoint"),
            _lock: lock,
            pending: Arc::new(AtomicUsize::new(0)),
            cut: Cut::None,
            poisoned: false,
        };
        bridge.persist(&bridge.state, Cut::None)?;
        Ok(bridge)
    }

    pub(crate) fn reopen_with(
        base: &Path,
        open: impl FnOnce(Incarnation) -> Result<B, PhysicalFailure>,
    ) -> Result<Self, Error> {
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(base.join("authority/owner"))
            .map_err(|_| Error::Storage)?;
        lock.try_lock().map_err(|_| Error::Storage)?;
        let checkpoint = base.join("authority/checkpoint");
        let file = File::open(&checkpoint).map_err(|_| Error::Storage)?;
        if file.metadata().map_err(|_| Error::Storage)?.len() > CHECKPOINT_MAX as u64 {
            return Err(Error::CorruptCheckpoint);
        }
        // The read cap is enforced independently of the metadata observation.
        let mut bytes = Vec::new();
        file.take(CHECKPOINT_MAX as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::Storage)?;
        let state = State::decode(&bytes)?;
        let store =
            open(Incarnation::new(state.metadata.namespace).unwrap()).map_err(Error::Physical)?;
        Ok(Self {
            state,
            store,
            checkpoint,
            _lock: lock,
            pending: Arc::new(AtomicUsize::new(0)),
            cut: Cut::None,
            poisoned: false,
        })
    }

    const fn live(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn persist(&self, state: &State, cut: Cut) -> Result<(), Error> {
        let bytes = state.encode()?;
        let staging = self.checkpoint.with_extension("next");
        let mut file = File::create(&staging).map_err(|_| Error::Storage)?;
        file.write_all(&bytes).map_err(|_| Error::Storage)?;
        file.sync_all().map_err(|_| Error::Storage)?;
        drop(file);
        if cut == Cut::BeforeRename {
            return Err(Error::Storage);
        }
        fs::rename(staging, &self.checkpoint).map_err(|_| Error::Storage)?;
        if cut == Cut::AfterRename {
            return Err(Error::Ambiguous);
        }
        File::open(self.checkpoint.parent().unwrap())
            .and_then(|file| file.sync_all())
            .map_err(|_| Error::Ambiguous)?;
        if cut == Cut::AfterDurability {
            return Err(Error::Ambiguous);
        }
        Ok(())
    }

    fn commit(&mut self, mut next: State) -> Result<(), Error> {
        self.live()?;
        next.metadata.sequence = next
            .metadata
            .sequence
            .checked_add(1)
            .ok_or(Error::Capacity)?;
        let cut = std::mem::take(&mut self.cut);
        if let Err(error) = self.persist(&next, cut) {
            self.poisoned = true;
            return Err(error);
        }
        self.state = next;
        Ok(())
    }

    pub(crate) fn view(&self) -> View {
        self.state.candidate.view()
    }
    pub(crate) fn head(&self) -> Result<Option<RootId>, Error> {
        self.live()?;
        Ok(self.state.metadata.head)
    }
    pub(crate) const fn inject(&mut self, cut: Cut) {
        self.cut = cut;
    }

    pub(crate) fn event(&mut self, event: Event) -> Result<Reply, Error> {
        self.live()?;
        let mut next = self.state.clone();
        let reply = next.apply(event)?;
        self.commit(next)?;
        Ok(reply)
    }

    pub(crate) fn upload(
        &mut self,
        bytes: &[u8],
        cohort: CohortId,
        replace: bool,
    ) -> Result<Row, Error> {
        self.live()?;
        if bytes.len() as u64 > PhysicalLimits::default().object_bytes {
            return Err(Error::Capacity);
        }
        let descriptor = Descriptor::from(ObjectId::for_bytes(Algorithm::Sha256, bytes));
        let existing = self
            .state
            .metadata
            .catalogue
            .iter()
            .find(|row| row.descriptor == descriptor)
            .copied();
        if !replace && let Some(row) = existing {
            return Ok(row);
        }
        let mut next = self.state.clone();
        let generation = next.metadata.next_generation;
        next.metadata.next_generation = generation.checked_add(1).ok_or(Error::Capacity)?;
        let logical_object = if let Some(row) = existing {
            row.logical_object
        } else {
            let logical = next.metadata.next_logical_object;
            next.metadata.next_logical_object = logical.checked_add(1).ok_or(Error::Capacity)?;
            logical
        };
        let row = Row {
            key: Key {
                incarnation: NAMESPACE_HANDLE,
                generation,
            },
            physical_incarnation: next.metadata.namespace,
            physical_generation: generation,
            descriptor,
            logical_object,
        };
        next.apply(Event::CreateCohort { cohort })?;
        next.apply(Event::Allocate {
            cohort,
            key: row.key,
            logical_object,
        })?;
        let staging = RootId(1000 + generation);
        let until = next
            .candidate
            .view()
            .now
            .checked_add(10)
            .ok_or(Error::Capacity)?;
        next.apply(Event::Acquire {
            root: staging,
            lease: 1,
            kind: RootKind::Staging,
            closure: vec![row.key],
            until,
            fences: next.fences(&[row.key]),
        })?;
        next.apply(Event::StartWrite {
            write: WriteId(generation),
            key: row.key,
            staging,
            lease: 1,
        })?;
        self.commit(next)?;
        let binding = row.binding();
        let receipt = self
            .store
            .write(binding.key, generation, binding.object, bytes)
            .map_err(Error::Physical)?;
        assert_eq!(receipt.key, binding.key);
        assert_eq!(receipt.object, binding.object);
        assert_eq!(receipt.write, generation);
        let mut next = self.state.clone();
        next.apply(Event::WriteDone {
            write: WriteId(generation),
            success: true,
        })?;
        next.apply(Event::Release {
            root: staging,
            lease: 1,
        })?;
        next.metadata
            .catalogue
            .retain(|other| other.descriptor != descriptor);
        next.metadata.catalogue.push(row);
        next.metadata.catalogue.sort_by_key(|row| row.descriptor);
        next.metadata.revision = next
            .metadata
            .revision
            .checked_add(1)
            .ok_or(Error::Capacity)?;
        self.commit(next)?;
        Ok(row)
    }

    pub(crate) fn begin(&self, result: Row) -> Result<Pending<B>, Error> {
        self.live()?;
        self.pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < PREPARATIONS).then_some(count + 1)
            })
            .map_err(|_| Error::Capacity)?;
        let permit = Permit(self.pending.clone());
        let view = self.view();
        let preparation = Preparation::new(
            self.store.clone(),
            self.state.metadata.revision,
            &self
                .state
                .metadata
                .catalogue
                .iter()
                .map(|row| row.binding())
                .collect::<Vec<_>>(),
            result.binding().object,
            &[],
            ClosureLimits {
                catalogue_objects: view.limits.generations,
                physical_objects: view.limits.closure,
                ..ClosureLimits::default()
            },
        )
        .map_err(Error::Closure)?;
        Ok(Pending {
            preparation,
            revision: self.state.metadata.revision,
            deadline: view
                .now
                .checked_add(PREPARATION_TICKS)
                .ok_or(Error::Capacity)?,
            fences: view
                .cohorts
                .iter()
                .map(|(id, cohort)| (*id, cohort.fence))
                .collect(),
            _permit: permit,
        })
    }

    pub(crate) fn advance(&self, pending: &mut Pending<B>, steps: usize) -> Result<bool, Error> {
        self.live()?;
        if self.view().now >= pending.deadline {
            return Err(Error::Expired);
        }
        pending.preparation.advance(steps).map_err(Error::Closure)
    }

    pub(crate) fn publish(
        &mut self,
        pending: Pending<B>,
        root: RootId,
        lease: u64,
        until: u64,
    ) -> Result<Grant, Error> {
        self.live()?;
        if self.view().now >= pending.deadline {
            return Err(Error::Expired);
        }
        if pending.revision != self.state.metadata.revision {
            return Err(Error::StaleCatalogue);
        }
        let prepared = pending.preparation.finish().map_err(Error::Closure)?;
        if prepared.catalogue_revision != pending.revision {
            return Err(Error::StaleCatalogue);
        }
        let mut rows = Vec::new();
        for object in prepared.objects {
            let row = self
                .state
                .metadata
                .catalogue
                .iter()
                .find(|row| row.binding() == object.binding)
                .ok_or(Error::StaleCatalogue)?;
            rows.push(*row);
        }
        rows.sort_by_key(|row| row.key);
        let manifest = Manifest {
            result: prepared.action_result.into(),
            catalogue_revision: pending.revision,
            objects: rows,
        };
        let keys = manifest.keys();
        let view = self.view();
        let touched: BTreeSet<_> = keys
            .iter()
            .map(|key| view.generations[key].cohort)
            .collect();
        let fences = pending
            .fences
            .into_iter()
            .filter(|(cohort, _)| touched.contains(cohort))
            .collect();
        let mut next = self.state.clone();
        next.apply(Event::Acquire {
            root,
            lease,
            kind: RootKind::Publication,
            closure: keys,
            until,
            fences,
        })?;
        if let Some(previous) = next.metadata.head
            && previous != root
        {
            let old = next.candidate.view().roots[&previous].clone();
            next.apply(Event::Release {
                root: previous,
                lease: old.lease,
            })?;
        }
        if let Ok(previous) = next.manifest(root) {
            if *previous != manifest {
                return Err(Error::Rejected);
            }
        } else {
            next.metadata.manifests.push((root, manifest));
            next.metadata.manifests.sort_by_key(|(root, _)| *root);
        }
        next.metadata.head = Some(root);
        self.commit(next)?;
        Ok(Grant {
            namespace: self.state.metadata.namespace,
            root,
            lease,
            until,
        })
    }

    pub(crate) fn serve(&mut self, root: RootId, until: u64) -> Result<Grant, Error> {
        self.live()?;
        let head = self.state.metadata.head.ok_or(Error::Rejected)?;
        let view = self.view();
        if view.roots[&head].until <= view.now {
            return Err(Error::Expired);
        }
        let manifest = self.state.manifest(head)?.clone();
        let mut next = self.state.clone();
        next.apply(Event::Acquire {
            root,
            lease: 1,
            kind: RootKind::Serving,
            closure: manifest.keys(),
            until,
            fences: next.fences(&manifest.keys()),
        })?;
        if next.metadata.manifests.iter().any(|(id, _)| *id == root) {
            return Err(Error::Rejected);
        }
        next.metadata.manifests.push((root, manifest));
        next.metadata.manifests.sort_by_key(|(root, _)| *root);
        self.commit(next)?;
        Ok(Grant {
            namespace: self.state.metadata.namespace,
            root,
            lease: 1,
            until,
        })
    }

    pub(crate) fn remove_head(&mut self) -> Result<(), Error> {
        self.live()?;
        let mut next = self.state.clone();
        if let Some(root) = next.metadata.head.take() {
            let lease = next.candidate.view().roots[&root].lease;
            next.apply(Event::Release { root, lease })?;
        }
        // Serving roots are independent of the derived head's lifetime.
        self.commit(next)
    }

    pub(crate) fn read(&self, grant: Grant, object: ObjectId) -> Result<Vec<u8>, Error> {
        self.live()?;
        if grant.namespace != self.state.metadata.namespace {
            return Err(Error::Rejected);
        }
        let view = self.view();
        let root = view.roots.get(&grant.root).ok_or(Error::Rejected)?;
        if root.lease != grant.lease || root.until < grant.until {
            return Err(Error::Expired);
        }
        if view.now >= grant.until {
            return Err(Error::Expired);
        }
        let manifest = self.state.manifest(grant.root)?;
        let row = manifest
            .objects
            .iter()
            .find(|row| row.descriptor == object.into())
            .ok_or(Error::Rejected)?;
        let binding = row.binding();
        self.store
            .read(binding.key, binding.object, 0, None)
            .map(|read| read.bytes)
            .map_err(Error::Physical)
    }

    pub(crate) fn close(&mut self, cohort: CohortId) -> Result<u64, Error> {
        if self.view().cohorts[&cohort].phase == Phase::Allocating {
            self.event(Event::Seal { cohort })?;
        }
        self.event(Event::BeginClose { cohort })?;
        let fence = self.view().cohorts[&cohort].fence;
        self.event(Event::FinishClose { cohort, fence })?;
        let captured = self.view().cohorts[&cohort].captured.clone();
        for (batch, roots) in captured.chunks(Limits::default().mark_batch).enumerate() {
            self.event(Event::Mark {
                cohort,
                fence,
                at: batch * Limits::default().mark_batch,
                roots: roots.to_vec(),
            })?;
        }
        Ok(fence)
    }

    pub(crate) fn delete_current(&mut self, row: Row) -> Result<(), Error> {
        self.live()?;
        let view = self.view();
        let generation = &view.generations[&row.key];
        assert!(generation.retired);
        assert!(!view.protected.contains(&row.key));
        let delete = generation.current_delete.ok_or(Error::Rejected)?;
        let binding = row.binding();
        let receipt = self
            .store
            .delete(binding.key, delete.0)
            .map_err(Error::Physical)?;
        assert_eq!(receipt.key, binding.key);
        assert_eq!(receipt.delete, delete.0);
        self.event(Event::DeleteDone {
            delete,
            success: true,
        })?;
        Ok(())
    }
}
