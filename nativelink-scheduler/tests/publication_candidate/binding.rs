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

//! Real request admission keeps one explicit trusted context per authority.

use super::*;

impl PublicationAuthority {
    #[allow(
        dead_code,
        reason = "Frozen synthetic comparison fixtures use the original constructor."
    )]
    pub(crate) fn new_real(
        context: TrustedContext,
        actions: BTreeMap<String, ActionBinding>,
    ) -> Result<Self, Error> {
        let policies = actions
            .iter()
            .map(|(label, binding)| (label.clone(), binding.do_not_cache))
            .collect();
        let mut state = Self::new(policies)?;
        state.metadata.real = Some(RealBindings { context, actions });
        state.validate()?;
        Ok(state)
    }

    #[allow(
        dead_code,
        reason = "Frozen synthetic comparison fixtures use the original admission route."
    )]
    pub(crate) fn admit_request(
        &mut self,
        operation: u64,
        label: String,
        context: &TrustedContext,
        request: a::Request,
        until: u64,
    ) -> Result<a::ExecutionId, Error> {
        let real = self.metadata.real.as_ref().ok_or(Error::Unauthorized)?;
        if context != &real.context || !self.request_matches_action(&label, &request) {
            return Err(Error::Unauthorized);
        }
        self.admit_bound(operation, label, request, until)
    }

    pub(super) fn request_matches_action(&self, label: &str, request: &a::Request) -> bool {
        if let Some(real) = &self.metadata.real {
            real.actions.get(label).is_some_and(|expected| {
                ActionBinding::capture(&real.context, request)
                    .is_ok_and(|actual| &actual == expected)
            })
        } else {
            request.payload.is_none()
                && request.action.instance == label
                && self.metadata.actions.get(label) == Some(&request.do_not_cache)
        }
    }

    pub(super) fn validate_bindings(&self) -> Result<(), Error> {
        let Some(real) = &self.metadata.real else {
            return Ok(());
        };
        real.context.validate()?;
        if !self.metadata.clients.is_empty() || real.actions.len() != self.metadata.actions.len() {
            return Err(Error::Corrupt);
        }
        let mut keys = BTreeSet::new();
        for (label, binding) in &real.actions {
            binding.validate()?;
            if binding.context != real.context
                || self.metadata.actions.get(label) != Some(&binding.do_not_cache)
                || !keys.insert(&binding.action)
            {
                return Err(Error::Corrupt);
            }
        }
        Ok(())
    }
}
