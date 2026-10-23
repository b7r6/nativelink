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

//! A private clock seeded only after a real application barrier and a complete
//! consistent audit. Pair provenance belongs to the owning recovery supervisor.

use std::time::Instant;

use openraft::{LogId, Raft};

use super::super::{CanonicalBootstrap, Journal, TypeConfig};
use super::temporal::{self, MAX_CLOCK};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClockError {
    Read,
    Range,
    Backward,
}

struct ReadFacts {
    applied: LogId<u64>,
    tick: u64,
}

impl ReadFacts {
    fn after_barrier(
        barrier: Option<LogId<u64>>,
        applied: temporal::AppliedFacts,
    ) -> Result<Self, ClockError> {
        let barrier = barrier.ok_or(ClockError::Read)?;
        let position = applied.last_applied.ok_or(ClockError::Read)?;
        if position.index < barrier.index
            || (position.index == barrier.index && position != barrier)
        {
            return Err(ClockError::Read);
        }
        Ok(Self {
            applied: position,
            tick: applied.now,
        })
    }
}

/// Copied bounded facts; no clock origin or mutation capability escapes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Diagnostic {
    pub(super) applied: LogId<u64>,
    pub(super) base_tick: u64,
    pub(super) first_tick: u64,
}

pub(super) struct Clock {
    facts: ReadFacts,
    origin: Instant,
    last: Instant,
    first_tick: u64,
}

impl Clock {
    fn capture(facts: ReadFacts) -> Result<Self, ClockError> {
        let origin = Instant::now();
        Self::finish_capture(facts, origin, Instant::now())
    }

    fn finish_capture(
        facts: ReadFacts,
        origin: Instant,
        first: Instant,
    ) -> Result<Self, ClockError> {
        if facts.tick > MAX_CLOCK {
            return Err(ClockError::Range);
        }
        let mut clock = Self {
            first_tick: facts.tick,
            facts,
            origin,
            last: origin,
        };
        clock.first_tick = clock.observe_at(first)?;
        Ok(clock)
    }

    fn observe_at(&mut self, now: Instant) -> Result<u64, ClockError> {
        if now < self.last {
            return Err(ClockError::Backward);
        }
        let elapsed = now
            .checked_duration_since(self.origin)
            .ok_or(ClockError::Backward)?;
        let tick = self
            .facts
            .tick
            .checked_add(elapsed.as_secs())
            .filter(|tick| *tick <= MAX_CLOCK)
            .ok_or(ClockError::Range)?;
        self.last = now;
        Ok(tick)
    }

    pub(super) const fn diagnostic(&self) -> Diagnostic {
        Diagnostic {
            applied: self.facts.applied,
            base_tick: self.facts.tick,
            first_tick: self.first_tick,
        }
    }

    #[cfg(test)]
    pub(super) const fn origin_for_test(&self) -> Instant {
        self.origin
    }
}

pub(super) async fn recover(
    raft: &Raft<TypeConfig>,
    journal: &Journal,
) -> Result<Clock, ClockError> {
    recover_inner(
        raft,
        journal,
        #[cfg(test)]
        None,
    )
    .await
}

async fn recover_inner(
    raft: &Raft<TypeConfig>,
    journal: &Journal,
    #[cfg(test)] during_audit: Option<Box<dyn FnOnce() + Send>>,
) -> Result<Clock, ClockError> {
    let barrier = raft
        .ensure_linearizable()
        .await
        .map_err(|_| ClockError::Read)?;
    let applied = temporal::audit(
        journal,
        journal.domain,
        journal.canonical_contract.ok_or(ClockError::Read)?,
        CanonicalBootstrap::Sealed,
        #[cfg(test)]
        during_audit,
    )
    .await
    .map_err(|_| ClockError::Read)?;
    let facts = ReadFacts::after_barrier(barrier, applied)?;
    // The origin follows the completed validated read. Downtime and the old
    // clock's fractional remainder are deliberately absent from recovery.
    Clock::capture(facts)
}

#[cfg(test)]
pub(super) async fn recover_with_audit_hook(
    raft: &Raft<TypeConfig>,
    journal: &Journal,
    hook: Box<dyn FnOnce() + Send>,
) -> Result<Clock, ClockError> {
    recover_inner(raft, journal, Some(hook)).await
}

#[cfg(test)]
#[path = "canonical_clock_tests.rs"]
mod tests;
