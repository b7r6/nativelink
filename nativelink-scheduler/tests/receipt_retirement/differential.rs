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

//! Boundary adapter for separately authored real-State tests. The implementation
//! of Candidate lives outside the independent model. Scenarios supply expected
//! baseline facts and expected replies explicitly; observations never become
//! reference facts or allocation cursors.

use nativelink_scheduler::authority::{
    ClusterEpoch, ObservableState, ReceiptRetirement, RecordedEvent, Reply,
};

use super::reference::{Facts, Reference};

pub(crate) trait Candidate {
    fn enable_receipts(&mut self) -> Result<(), String>;
    fn apply(&mut self, event: RecordedEvent) -> Reply;
    fn receipt_projection(&self) -> Option<ReceiptRetirement>;
    fn lifecycle_projection(&self) -> ObservableState;
    fn checkpoint_roundtrip(&mut self) -> Result<(), String>;
}

pub(crate) struct Differential<C> {
    candidate: C,
    reference: Reference,
    facts: Facts,
}

impl<C: Candidate> Differential<C> {
    pub(crate) fn new(candidate: C, epoch: ClusterEpoch, facts: Facts) -> Self {
        facts
            .validate()
            .expect("scenario facts must be independently valid");
        let driver = Self {
            candidate,
            reference: Reference::new(epoch),
            facts,
        };
        driver.check();
        driver
    }

    pub(crate) fn activate(&mut self) {
        let lifecycle = self.candidate.lifecycle_projection();
        self.reference.activate(&self.facts);
        self.candidate
            .enable_receipts()
            .expect("candidate feature activation");
        assert_eq!(
            self.candidate.lifecycle_projection(),
            lifecycle,
            "activation cannot change lifecycle facts"
        );
        self.check();
    }

    pub(crate) fn receipt(&mut self, event: RecordedEvent) -> Reply {
        let lifecycle = self.candidate.lifecycle_projection();
        let expected = self.reference.apply(event.clone(), &mut self.facts);
        assert_eq!(
            self.candidate.apply(event),
            expected,
            "receipt transition reply differs"
        );
        assert_eq!(
            self.candidate.lifecycle_projection(),
            lifecycle,
            "receipt evidence changed work, charges, or effects"
        );
        self.check();
        expected
    }

    /// Updates baseline facts from the scenario, after comparing the candidate
    /// with that scenario's independent expected reply. The closure receives no
    /// candidate observations. It must also invoke the reference registration
    /// or release notification when the accepted baseline event requires it.
    pub(crate) fn baseline(
        &mut self,
        event: RecordedEvent,
        expected: &Reply,
        update: impl FnOnce(&mut Facts, &mut Reference),
    ) {
        assert_eq!(
            &self.candidate.apply(event),
            expected,
            "baseline scenario reply differs"
        );
        update(&mut self.facts, &mut self.reference);
        self.check();
    }

    pub(crate) fn checkpoint(&mut self) {
        let lifecycle = self.candidate.lifecycle_projection();
        self.reference = Reference::restore(&self.reference.snapshot(), &self.facts)
            .expect("independent snapshot and facts must agree");
        self.candidate
            .checkpoint_roundtrip()
            .expect("candidate checkpoint recovery");
        assert_eq!(
            self.candidate.lifecycle_projection(),
            lifecycle,
            "checkpoint changed lifecycle facts"
        );
        self.check();
    }

    pub(crate) const fn expected(&self) -> &Reference {
        &self.reference
    }

    pub(crate) const fn facts(&self) -> &Facts {
        &self.facts
    }

    fn check(&self) {
        self.reference
            .validate(&self.facts)
            .expect("independent receipt invariants");
        assert_eq!(
            self.candidate.receipt_projection().as_ref(),
            self.reference.projection(),
            "receipt metadata differs from independent model"
        );
    }
}
