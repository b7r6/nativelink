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

//! Private storage prerequisite for a future fixed-clock constructor.
//! No public constructor requests this contract or completes its local seal.

use openraft::ErrorVerb;
use redb::ReadableTable;
use serde::{Deserialize, Serialize};

use super::{ClusterEpoch, Result, decode, failure};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) enum CanonicalProfile {
    #[serde(rename = "singleton_seconds_v1")]
    SingletonSecondsV1,
}

/// One compiled semantic profile and a commitment to the owned namespace facts.
/// Equality is storage compatibility, not proof of filesystem custody or time.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CanonicalContract {
    pub(super) profile: CanonicalProfile,
    pub(super) namespace_commitment: [u8; 32],
}

impl CanonicalContract {
    pub(super) fn validate(self) -> Result<()> {
        if self.namespace_commitment == [0; 32] {
            return Err(failure(
                ErrorVerb::Read,
                "unbound canonical namespace commitment",
            ));
        }
        Ok(())
    }
}

/// Local bootstrap progress never travels as snapshot authority. The seal can
/// only be synthesized by tests until the owned bootstrap protocol is built.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum CanonicalBootstrap {
    Preparing,
    Sealed,
}

pub(super) fn validate_metadata(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    epoch: ClusterEpoch,
    expected: Option<CanonicalContract>,
    require_sealed: bool,
) -> Result<()> {
    let contract: Option<CanonicalContract> = table
        .get("canonical_contract")
        .map_err(|error| failure(ErrorVerb::Read, error))?
        .map(|value| decode(epoch, value.value()))
        .transpose()?;
    let phase: Option<CanonicalBootstrap> = table
        .get("canonical_bootstrap")
        .map_err(|error| failure(ErrorVerb::Read, error))?
        .map(|value| decode(epoch, value.value()))
        .transpose()?;
    if let Some(contract) = contract {
        contract.validate()?;
    }
    if contract != expected || phase.is_some() != expected.is_some() {
        return Err(failure(
            ErrorVerb::Read,
            "canonical journal metadata mismatch",
        ));
    }
    if require_sealed && expected.is_some() && phase != Some(CanonicalBootstrap::Sealed) {
        return Err(failure(
            ErrorVerb::Read,
            "canonical bootstrap is incomplete",
        ));
    }
    Ok(())
}
