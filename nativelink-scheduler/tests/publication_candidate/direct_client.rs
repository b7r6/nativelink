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

//! Trusted fixture grants for direct updates; these are not RPC authentication.

use super::*;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UpdateAuthorization {
    label: String,
    binding: ActionBinding,
    issued_at: u64,
    revoked_at: Option<u64>,
}

impl Metadata {
    pub(super) fn snapshot_version(&self) -> u32 {
        if !self.update_authorizations.is_empty() {
            3
        } else if self.real.is_some() {
            2
        } else {
            1
        }
    }
}

impl PublicationAuthority {
    /// The caller supplies the trusted context and full pre-registered binding.
    /// A grant ID is an immutable authority record, not an incoming credential.
    #[allow(
        dead_code,
        reason = "Synthetic physical and differential fixtures do not exercise trusted direct updates."
    )]
    pub(crate) fn authorize_update(
        &mut self,
        authorization: u64,
        context: &TrustedContext,
        label: &str,
        binding: &ActionBinding,
    ) -> Result<(), Error> {
        self.transaction(|next| {
            let real = next.metadata.real.as_ref().ok_or(Error::Unauthorized)?;
            if context != &real.context
                || &binding.context != context
                || real.actions.get(label) != Some(binding)
            {
                return Err(Error::Unauthorized);
            }
            if binding.do_not_cache || authorization == 0 {
                return Err(Error::Invalid);
            }
            if next
                .metadata
                .update_authorizations
                .contains_key(&authorization)
            {
                return Err(Error::Conflict);
            }
            if next.metadata.update_authorizations.len() >= MAX_STAGES {
                return Err(Error::Bounds);
            }
            next.metadata.update_authorizations.insert(
                authorization,
                UpdateAuthorization {
                    label: label.to_owned(),
                    binding: binding.clone(),
                    issued_at: next.metadata.commit,
                    revoked_at: None,
                },
            );
            Ok(())
        })
    }

    #[allow(
        dead_code,
        reason = "Synthetic physical and differential fixtures do not exercise trusted direct updates."
    )]
    pub(crate) fn revoke_update(
        &mut self,
        authorization: u64,
        context: &TrustedContext,
    ) -> Result<(), Error> {
        self.transaction(|next| {
            let real = next.metadata.real.as_ref().ok_or(Error::Unauthorized)?;
            if context != &real.context {
                return Err(Error::Unauthorized);
            }
            let grant = next
                .metadata
                .update_authorizations
                .get_mut(&authorization)
                .ok_or(Error::Unauthorized)?;
            if grant.revoked_at.is_some() {
                return Err(Error::Stale);
            }
            grant.revoked_at = Some(next.metadata.commit);
            Ok(())
        })
    }

    /// Constructs only the canonical successful `UpdateActionResult` projection.
    /// Trusted typed closure evidence still enters the existing staging path.
    #[allow(
        dead_code,
        reason = "Synthetic physical and differential fixtures do not exercise trusted direct updates."
    )]
    pub(crate) fn direct_update_candidate(
        &self,
        authorization: u64,
        context: &TrustedContext,
        binding: &ActionBinding,
        result: ActionResult,
        closure: BTreeSet<Member>,
    ) -> Result<Candidate, Error> {
        let grant = self
            .metadata
            .update_authorizations
            .get(&authorization)
            .ok_or(Error::Unauthorized)?;
        if &grant.binding != binding || &binding.context != context || grant.revoked_at.is_some() {
            return Err(Error::Unauthorized);
        }
        if result.encoded_len() > MAX_PAYLOAD {
            return Err(Error::Bounds);
        }
        let result_bytes = result.encode_to_vec();
        let value = Candidate {
            action: grant.label.clone(),
            publisher: Publisher::AuthorizedClient { authorization },
            result: Object::for_bytes(binding.result_algorithm(), &result_bytes)?,
            result_bytes,
            response: a::ResultPayload::from_response(
                &ExecuteResponse {
                    result: Some(result),
                    ..ExecuteResponse::default()
                },
                MAX_PAYLOAD,
            )
            .map_err(|_| Error::Invalid)?,
            closure,
        };
        self.validate_candidate(&value)?;
        self.validate_current_update(&value)?;
        Ok(value)
    }

    fn update_grant(&self, candidate: &Candidate) -> Result<Option<&UpdateAuthorization>, Error> {
        let Publisher::AuthorizedClient { authorization } = candidate.publisher else {
            return Ok(None);
        };
        let grant = self
            .metadata
            .update_authorizations
            .get(&authorization)
            .ok_or(Error::Unauthorized)?;
        let real = self.metadata.real.as_ref().ok_or(Error::Unauthorized)?;
        if candidate.action != grant.label
            || grant.binding.context != real.context
            || real.actions.get(&grant.label) != Some(&grant.binding)
            || grant.binding.do_not_cache
        {
            return Err(Error::Unauthorized);
        }
        Ok(Some(grant))
    }

    pub(super) fn validate_current_update(&self, candidate: &Candidate) -> Result<(), Error> {
        if self
            .update_grant(candidate)?
            .is_some_and(|grant| grant.revoked_at.is_some())
        {
            return Err(Error::Unauthorized);
        }
        Ok(())
    }

    pub(super) fn validate_direct_candidate(
        &self,
        candidate: &Candidate,
        result: &ActionResult,
        response: &ExecuteResponse,
    ) -> Result<(), Error> {
        if self.update_grant(candidate)?.is_some() {
            let expected = ExecuteResponse {
                result: Some(result.clone()),
                ..ExecuteResponse::default()
            };
            if result.exit_code != 0
                || response != &expected
                || candidate
                    .response
                    .require_response()
                    .map_err(|_| Error::Invalid)?
                    .encoded()
                    != expected.encode_to_vec()
            {
                return Err(Error::Invalid);
            }
        }
        Ok(())
    }

    pub(super) fn validate_update_authorizations(&self) -> Result<(), Error> {
        if self.metadata.update_authorizations.is_empty() {
            return Ok(());
        }
        let real = self.metadata.real.as_ref().ok_or(Error::Corrupt)?;
        if self.metadata.update_authorizations.len() > MAX_STAGES {
            return Err(Error::Corrupt);
        }
        let mut positions = BTreeSet::new();
        for (id, grant) in &self.metadata.update_authorizations {
            if *id == 0
                || grant.binding.context != real.context
                || real.actions.get(&grant.label) != Some(&grant.binding)
                || grant.binding.do_not_cache
                || grant.issued_at == 0
                || grant.issued_at > self.metadata.commit
                || !positions.insert(grant.issued_at)
            {
                return Err(Error::Corrupt);
            }
            if let Some(revoked) = grant.revoked_at
                && (revoked <= grant.issued_at
                    || revoked > self.metadata.commit
                    || !positions.insert(revoked))
            {
                return Err(Error::Corrupt);
            }
        }
        // Each retained event was a distinct outer transaction. These relations
        // do not authenticate a coherently rewritten snapshot or recover events
        // that the bounded state does not retain.
        for at in self
            .metadata
            .stages
            .values()
            .filter_map(|stage| stage.authorized_at)
            .chain(
                self.metadata
                    .accepted
                    .values()
                    .map(|accepted| accepted.commit),
            )
        {
            if !positions.insert(at) {
                return Err(Error::Corrupt);
            }
        }
        Ok(())
    }

    pub(super) fn validate_update_stage(&self, stage: &Stage) -> Result<(), Error> {
        match (self.update_grant(&stage.candidate)?, stage.authorized_at) {
            (Some(grant), Some(at))
                if at > grant.issued_at
                    && at <= self.metadata.commit
                    && grant.revoked_at.is_none_or(|revoked| at < revoked) =>
            {
                Ok(())
            }
            (None, None) => Ok(()),
            _ => Err(Error::Corrupt),
        }
    }

    pub(super) fn validate_update_acceptance(
        &self,
        stage: &Stage,
        accepted: &Accepted,
    ) -> Result<(), Error> {
        let grant = self.update_grant(&stage.candidate)?.ok_or(Error::Corrupt)?;
        if stage.authorized_at.is_none_or(|at| at >= accepted.commit)
            || grant
                .revoked_at
                .is_some_and(|revoked| accepted.commit >= revoked)
        {
            return Err(Error::Corrupt);
        }
        Ok(())
    }
}
