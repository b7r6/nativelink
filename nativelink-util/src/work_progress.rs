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

//! Bounded observations of actual work, independent of transport traffic.
//!
//! A scope only carries an observer through nested futures. Shared operations
//! must separately relay observations to every interested caller; inheriting
//! the initiating caller's scope cannot establish progress for its followers.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::store_trait::StoreKey;

/// Fixed-size keys and values, retained without eviction until the attempt ends.
/// Exhaustion conservatively disables renewal, including already known keys.
pub const MAX_PROGRESS_OBJECTS: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ProgressPhase {
    Preparing,
    Running,
    Uploading,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhaseProgress {
    pub phase: ProgressPhase,
    pub progress: u64,
    pub exhausted: bool,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WorkKind {
    Read,
    Populate,
    Write,
    Hash,
    Materialize,
    Process,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ObjectKey {
    phase: ProgressPhase,
    kind: WorkKind,
    identity: [u8; 32],
}

#[derive(Debug, Default)]
struct ObjectState {
    offset: u64,
    complete: bool,
}

#[derive(Debug)]
struct State {
    latest: PhaseProgress,
    objects: BTreeMap<ObjectKey, ObjectState>,
}

#[derive(Clone, Debug)]
pub struct ProgressObserver {
    state: Arc<Mutex<State>>,
    changed: watch::Sender<PhaseProgress>,
}

#[derive(Clone, Debug)]
pub struct ObjectProgress {
    observer: ProgressObserver,
    key: ObjectKey,
}

tokio::task_local! {
    static CURRENT: Option<ProgressObserver>;
}

/// An explicit observed entry point scopes its nested helpers. Passing `None`
/// preserves the legacy path and prevents accidental inheritance by that path.
pub async fn scope<F: Future>(observer: Option<ProgressObserver>, future: F) -> F::Output {
    CURRENT.scope(observer, future).await
}

impl Default for ProgressObserver {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressObserver {
    pub fn new() -> Self {
        let latest = PhaseProgress {
            phase: ProgressPhase::Preparing,
            progress: 0,
            exhausted: false,
        };
        Self {
            state: Arc::new(Mutex::new(State {
                latest,
                objects: BTreeMap::new(),
            })),
            changed: watch::channel(latest).0,
        }
    }

    pub fn current() -> Option<Self> {
        CURRENT.try_with(Clone::clone).ok().flatten()
    }

    pub fn subscribe(&self) -> watch::Receiver<PhaseProgress> {
        self.changed.subscribe()
    }

    pub fn set_phase(&self, phase: ProgressPhase) {
        let mut state = self.state.lock();
        if phase <= state.latest.phase || state.latest.exhausted {
            return;
        }
        state.latest.phase = phase;
        state.latest.progress = 0;
        self.changed.send_replace(state.latest);
    }

    pub fn object(&self, kind: WorkKind, identity: &[u8]) -> ObjectProgress {
        ObjectProgress {
            observer: self.clone(),
            key: ObjectKey {
                phase: self.state.lock().latest.phase,
                kind,
                identity: Sha256::digest(identity).into(),
            },
        }
    }

    pub fn store_object(kind: WorkKind, key: &StoreKey<'_>) -> Option<ObjectProgress> {
        Self::current().map(|observer| observer.object(kind, key.as_str().as_bytes()))
    }

    pub fn path_object(kind: WorkKind, path: &Path) -> Option<ObjectProgress> {
        Self::current().map(|observer| observer.object(kind, path.as_os_str().as_encoded_bytes()))
    }
}

impl ObjectProgress {
    /// An absolute offset, never a transfer-local count added to previous work.
    pub fn offset(&self, offset: u64) {
        self.record(offset, false);
    }

    /// Successful settlement, counted once even across retries and restaging.
    pub fn complete(&self) {
        self.record(0, true);
    }

    fn record(&self, offset: u64, complete: bool) {
        let mut state = self.observer.state.lock();
        if self.key.phase != state.latest.phase || state.latest.exhausted {
            return;
        }
        if !state.objects.contains_key(&self.key) && state.objects.len() >= MAX_PROGRESS_OBJECTS {
            Self::exhaust(&self.observer, &mut state);
            return;
        }
        let object = state.objects.entry(self.key).or_default();
        if object.complete {
            return;
        }
        let advanced = offset > object.offset || (complete && !object.complete);
        object.offset = object.offset.max(offset);
        object.complete |= complete;
        if advanced {
            // Counts meaningful observations, not bytes or elapsed time. A
            // replayed prefix cannot advance beyond its retained maximum.
            let Some(next) = state.latest.progress.checked_add(1) else {
                Self::exhaust(&self.observer, &mut state);
                return;
            };
            state.latest.progress = next;
            self.observer.changed.send_replace(state.latest);
        }
    }

    fn exhaust(observer: &ProgressObserver, state: &mut State) {
        state.latest.exhausted = true;
        observer.changed.send_replace(state.latest);
        tracing::warn!("Attempt progress registry or counter exhausted; renewal disabled");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_prefix_completion_and_old_phase_cannot_mint_progress() {
        let observer = ProgressObserver::new();
        let view = observer.subscribe();
        let input = observer.object(WorkKind::Read, b"input");
        input.offset(100);
        input.offset(50);
        input.offset(100);
        assert_eq!(view.borrow().progress, 1);
        input.complete();
        input.complete();
        input.offset(200);
        assert_eq!(view.borrow().progress, 2);
        let complete_first = observer.object(WorkKind::Read, b"cache-hit");
        complete_first.complete();
        complete_first.offset(100);
        assert_eq!(view.borrow().progress, 3);
        observer.set_phase(ProgressPhase::Uploading);
        input.offset(200);
        assert_eq!(view.borrow().progress, 0);
        observer.object(WorkKind::Write, b"output").offset(50);
        assert_eq!(view.borrow().progress, 1);
        observer.set_phase(ProgressPhase::Preparing);
        assert_eq!(view.borrow().phase, ProgressPhase::Uploading);
    }

    #[test]
    fn saturation_never_evicts_and_disables_all_further_renewal() {
        let observer = ProgressObserver::new();
        let view = observer.subscribe();
        for index in 0..MAX_PROGRESS_OBJECTS {
            observer
                .object(WorkKind::Read, &index.to_be_bytes())
                .complete();
        }
        assert!(!view.borrow().exhausted);
        observer.object(WorkKind::Read, b"overflow").complete();
        assert!(view.borrow().exhausted);
        let progress = view.borrow().progress;
        observer
            .object(WorkKind::Read, &0_usize.to_be_bytes())
            .offset(99);
        observer.set_phase(ProgressPhase::Running);
        assert_eq!(view.borrow().progress, progress);
        assert_eq!(observer.state.lock().objects.len(), MAX_PROGRESS_OBJECTS);
    }
}
