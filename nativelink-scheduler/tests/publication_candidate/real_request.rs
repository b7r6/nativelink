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

//! Explicit trusted context and complete action binding for real request fixtures.

use std::collections::BTreeMap;

use nativelink_proto::build::bazel::remote::execution::v2::Platform;
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_scheduler::authority as a;
use nativelink_scheduler::authority::request::normalize_platform;
use serde::{Deserialize, Serialize};

use super::{Error, MAX_PAYLOAD};

/// One authority fixture accepts one trusted context. Production multi-context
/// routing and authorization remain outside this constructor's qualification.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrustedContext {
    pub(crate) principal: String,
    /// Supplied by the trusted caller; never taken from `RequestMetadata` or a label.
    pub(crate) execution_config: String,
}
impl TrustedContext {
    pub(crate) const fn validate(&self) -> Result<(), Error> {
        if self.principal.is_empty()
            || self.principal.len() > 256
            || self.execution_config.is_empty()
            || self.execution_config.len() > 256
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActionBinding {
    pub(crate) context: TrustedContext,
    pub(crate) action: a::ActionKey,
    /// Canonically ordered complete selected platform, including resource fields.
    pub(crate) platform: BTreeMap<String, String>,
    pub(crate) do_not_cache: bool,
}
impl ActionBinding {
    pub(crate) fn capture(context: &TrustedContext, request: &a::Request) -> Result<Self, Error> {
        context.validate()?;
        let payload = request.require_payload().map_err(|_| Error::Invalid)?;
        if payload.principal() != context.principal {
            return Err(Error::Unauthorized);
        }
        let facts = payload.dispatch_facts().map_err(|_| Error::Invalid)?;
        let binding = Self {
            context: context.clone(),
            action: request.action.clone(),
            platform: facts
                .platform
                .properties
                .into_iter()
                .map(|property| (property.name, property.value))
                .collect(),
            do_not_cache: request.do_not_cache,
        };
        binding.validate()?;
        Ok(binding)
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.context.validate()?;
        if !matches!(self.action.digest_function.as_str(), "sha256" | "blake3")
            || self.action.digest.len() != 64
            || !self
                .action
                .digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || self.action.digest_size_bytes > MAX_PAYLOAD as u64
            || serde_json::to_vec(self).map_err(|_| Error::Invalid)?.len() > MAX_PAYLOAD
        {
            return Err(Error::Invalid);
        }
        normalize_platform(&Platform {
            properties: self
                .platform
                .iter()
                .map(|(name, value)| Property {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
        })
        .map_err(|_| Error::Invalid)?;
        Ok(())
    }
    pub(crate) fn result_algorithm(&self) -> u8 {
        match self.action.digest_function.as_str() {
            "sha256" => 1,
            "blake3" => 9,
            _ => unreachable!("validated action binding"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RealBindings {
    pub(crate) context: TrustedContext,
    pub(crate) actions: BTreeMap<String, ActionBinding>,
}
