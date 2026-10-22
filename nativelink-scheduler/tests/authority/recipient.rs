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

//! Independent bounded worker recipient simulation for M1 races.
//!
//! This is not the M2 worker protocol or a process manager. It explicitly keeps
//! termination uncertain until `confirm_stopped`; a closed attempt is a durable
//! tombstone within this simulator's lifetime. Tests keep copied network effects
//! outside both the reducer outbox and this recipient to exercise late delivery.

use std::collections::{BTreeMap, BTreeSet};

use nativelink_scheduler::authority::{
    AttemptId, ClusterEpoch, Effect, EffectId, EffectKind, Event, IncarnationId, WorkerId,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Reception {
    Started,
    Duplicate,
    Terminating,
    Closed,
    WrongDestination,
    ConflictingIdentity,
}

#[derive(Clone, Debug)]
pub(super) struct Recipient {
    epoch: ClusterEpoch,
    worker: WorkerId,
    incarnation: IncarnationId,
    effects: BTreeMap<EffectId, Effect>,
    running: BTreeMap<AttemptId, Effect>,
    stopping: BTreeSet<AttemptId>,
    closed: BTreeSet<AttemptId>,
    launches: usize,
}

impl Recipient {
    pub(super) const fn new(
        epoch: ClusterEpoch,
        worker: WorkerId,
        incarnation: IncarnationId,
    ) -> Self {
        Self {
            epoch,
            worker,
            incarnation,
            effects: BTreeMap::new(),
            running: BTreeMap::new(),
            stopping: BTreeSet::new(),
            closed: BTreeSet::new(),
            launches: 0,
        }
    }

    pub(super) fn receive(&mut self, effect: Effect) -> Reception {
        let (attempt, worker, incarnation) = match &effect.kind {
            EffectKind::Dispatch {
                attempt,
                worker,
                incarnation,
                ..
            }
            | EffectKind::Cancel {
                attempt,
                worker,
                incarnation,
                ..
            } => (*attempt, *worker, *incarnation),
            _ => return Reception::WrongDestination,
        };
        if effect.epoch != self.epoch || worker != self.worker || incarnation != self.incarnation {
            return Reception::WrongDestination;
        }
        if self
            .effects
            .get(&effect.id)
            .is_some_and(|original| original != &effect)
        {
            return Reception::ConflictingIdentity;
        }
        self.effects.insert(effect.id, effect.clone());
        if self.closed.contains(&attempt) {
            return Reception::Closed;
        }
        match &effect.kind {
            EffectKind::Dispatch { .. } => {
                if let Some(original) = self.running.get(&attempt) {
                    if original != &effect {
                        return Reception::ConflictingIdentity;
                    }
                    return if self.stopping.contains(&attempt) {
                        Reception::Terminating
                    } else {
                        Reception::Duplicate
                    };
                }
                self.running.insert(attempt, effect);
                self.launches += 1;
                Reception::Started
            }
            EffectKind::Cancel { .. } => {
                if self.running.contains_key(&attempt) {
                    self.stopping.insert(attempt);
                    Reception::Terminating
                } else {
                    // Cancel-before-dispatch closes the identity before ack.
                    self.closed.insert(attempt);
                    Reception::Closed
                }
            }
            _ => unreachable!("destination checked above"),
        }
    }

    pub(super) fn acknowledge(&self, attempt: AttemptId) -> Option<Event> {
        self.running
            .contains_key(&attempt)
            .then_some(Event::WorkerAccepted {
                attempt,
                worker: self.worker,
                incarnation: self.incarnation,
            })
    }

    pub(super) fn confirm_stopped(&mut self, attempt: AttemptId) -> Option<Event> {
        if !self.stopping.remove(&attempt) && !self.closed.contains(&attempt) {
            return None;
        }
        self.running.remove(&attempt);
        self.closed.insert(attempt);
        Some(Event::Cleanup {
            attempt,
            worker: self.worker,
            incarnation: self.incarnation,
        })
    }

    pub(super) fn is_running(&self, attempt: AttemptId) -> bool {
        self.running.contains_key(&attempt)
    }

    pub(super) const fn launches(&self) -> usize {
        self.launches
    }
}
