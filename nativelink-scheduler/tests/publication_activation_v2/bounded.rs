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

//! Additive byte-admission guard for the frozen activation-v2 reference.

use super::reference;
use super::types::{Event, Facts, Legacy, Outcome, Reject};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Model {
    inner: reference::Model,
}

impl Model {
    pub(crate) fn new(legacy: Legacy) -> Result<Self, Reject> {
        let inner = reference::Model::new(legacy)?;
        inner.snapshot()?;
        Ok(Self { inner })
    }

    pub(crate) fn apply(&mut self, event: &Event, facts: &Facts) -> Result<Outcome, Reject> {
        let mut next = self.inner.clone();
        let outcome = next.apply(event, facts)?;
        next.snapshot()?;
        self.inner = next;
        Ok(outcome)
    }

    pub(crate) const fn view(&self) -> &reference::Model {
        &self.inner
    }

    pub(crate) fn snapshot(&self) -> Result<Vec<u8>, Reject> {
        self.inner.snapshot()
    }

    pub(crate) fn restore(bytes: &[u8], original: &Legacy) -> Result<Self, Reject> {
        let inner = reference::Model::restore(bytes, original)?;
        inner.snapshot()?;
        Ok(Self { inner })
    }
}
