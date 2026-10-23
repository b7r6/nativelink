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

//! Inert preparation of a namespace-bound journal. No runtime is started.

use std::fmt;
use std::sync::{Arc, OnceLock};

use nativelink_canonical_filesystem::SyncedNamespace;
use nativelink_util::spawn_blocking;
use redb::{Database, Durability, ReadableTable};

use super::canonical_binding::CanonicalProfile;
use super::{
    AuthorityDomain, CanonicalBootstrap, CanonicalContract, ClusterEpoch, ErrorVerb, Journal,
    JournalDatabase, Limits, META, RecoveryExpectation, decode, failure, validate_metadata,
};

#[path = "canonical_cleanup.rs"]
mod canonical_cleanup;
#[path = "canonical_temporal.rs"]
mod temporal;

// Versioned finite preparation policy; independent of changing generic defaults.
const LIMITS: Limits = Limits {
    max_executions: 64,
    max_operations: 256,
    max_workers: 32,
    max_effects: 512,
    max_payload_bytes: 65_536,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnerPhase {
    Preparing,
    Sealed,
}

/// Copied storage facts, not permission to execute or serve.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnerDiagnostic {
    pub node_id: u64,
    pub epoch: u64,
    pub namespace: u64,
    pub namespace_commitment: [u8; 32],
    pub phase: OwnerPhase,
    /// Redb invoked its repair observer during this successful, synchronized open.
    pub repair_observed: bool,
}

/// Bounded construction errors; an error never promises rollback of storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnerError {
    Namespace,
    Journal,
    CurrentSync,
    Task,
}

impl fmt::Display for OwnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Namespace => "invalid canonical namespace",
            Self::Journal => "canonical journal opening or validation failed",
            Self::CurrentSync => "canonical journal current synchronization failed",
            Self::Task => "canonical journal ownership task failed",
        })
    }
}
impl std::error::Error for OwnerError {}

/// An inert, privately bound journal retaining all four namespace locks.
///
/// This is not a running authority: no Raft, clock, store adapter, worker or
/// listener exists. A cold preparation remains Preparing, including on release.
/// Existing experimental ownership aliases are outside this closed API.
pub struct PreparedCanonicalOwner {
    journal: Journal,
    diagnostic: OwnerDiagnostic,
}

impl fmt::Debug for PreparedCanonicalOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("PreparedCanonicalOwner")
            .field(&self.diagnostic)
            .finish()
    }
}

impl PreparedCanonicalOwner {
    /// Exclusively create a bound Preparing journal; never adopt prior artifacts.
    pub async fn prepare_cold(namespace: SyncedNamespace) -> Result<Self, OwnerError> {
        Self::open(
            namespace,
            true,
            #[cfg(test)]
            tests::Hooks::default(),
        )
        .await
    }

    /// Validate and currently synchronize an existing Sealed journal. This does
    /// not replay Raft or confer serving readiness. Preparing always refuses.
    pub async fn reopen_sealed(namespace: SyncedNamespace) -> Result<Self, OwnerError> {
        Self::open(
            namespace,
            false,
            #[cfg(test)]
            tests::Hooks::default(),
        )
        .await
    }

    pub const fn diagnostic(&self) -> OwnerDiagnostic {
        self.diagnostic
    }

    /// Wait for blocking destruction and custody release. Redb Drop can suppress
    /// flush errors; completion here is not a new durability acknowledgement.
    pub async fn release(self) -> Result<(), OwnerError> {
        let Self {
            journal,
            diagnostic: _,
        } = self;
        spawn_blocking!("canonical_owner_release", move || drop(journal))
            .await
            .map_err(|_| OwnerError::Task)
    }

    async fn open(
        namespace: SyncedNamespace,
        create: bool,
        #[cfg(test)] hooks: tests::Hooks,
    ) -> Result<Self, OwnerError> {
        let binding = namespace.journal_binding();
        if binding.node_id == 0 || binding.epoch == 0 || binding.namespace == 0 {
            return Err(OwnerError::Namespace);
        }
        let domain = AuthorityDomain {
            epoch: ClusterEpoch(binding.epoch),
            namespace: binding.namespace,
        };
        let contract = CanonicalContract {
            profile: CanonicalProfile::SingletonSecondsV1,
            namespace_commitment: namespace.commitment(),
        };
        contract.validate().map_err(|_| OwnerError::Namespace)?;
        let phase = if create {
            CanonicalBootstrap::Preparing
        } else {
            CanonicalBootstrap::Sealed
        };
        #[cfg(test)]
        let tests::Hooks {
            after_open,
            during_temporal,
            before_current,
            after_commit,
            after_current,
        } = hooks;
        // The factory owns this slot during blocking work. Cancellation can drop
        // its waiter, but neither the slot nor backend releases custody early.
        let parent = Arc::new(OnceLock::new());
        let transport = parent.clone();
        let journal = Journal::open_with_factory_inner(
            binding.node_id,
            domain.epoch,
            domain,
            LIMITS,
            None,
            RecoveryExpectation::Singleton,
            Some(contract),
            move || {
                let opened = if create {
                    namespace.create_journal()
                } else {
                    namespace.reopen_journal()
                }
                .map_err(|error| failure(ErrorVerb::Read, error))?;
                transport
                    .set((opened.current_parent(), opened.repair_observed()))
                    .map_err(|_| failure(ErrorVerb::Read, "duplicate current parent"))?;
                #[cfg(test)]
                if let Some(hook) = after_open {
                    hook();
                }
                let (database, created_parent) = opened.into_parts();
                Ok(match created_parent {
                    Some(parent) => JournalDatabase::created(database, move || parent.sync_all()),
                    None => JournalDatabase::existing(database),
                })
            },
        )
        .await
        .map_err(|_| OwnerError::Journal)?;
        temporal::audit(
            &journal,
            domain,
            contract,
            phase,
            #[cfg(test)]
            during_temporal,
        )
        .await
        .map_err(|_| OwnerError::Journal)?;
        let (parent, repair_observed) = Arc::try_unwrap(parent)
            .map_err(|_| OwnerError::Task)?
            .into_inner()
            .ok_or(OwnerError::Task)?;
        let database = journal.database.clone();
        spawn_blocking!("canonical_owner_current_sync", move || {
            #[cfg(test)]
            if let Some(hook) = before_current {
                hook(&database);
            }
            parent
                .verify_current()
                .map_err(|_| OwnerError::CurrentSync)?;
            current_transaction(&database, domain.epoch, contract, phase)?;
            #[cfg(test)]
            if let Some(hook) = after_commit {
                hook();
            }
            parent.sync_current().map_err(|_| OwnerError::CurrentSync)?;
            #[cfg(test)]
            if let Some(hook) = after_current {
                hook();
            }
            Ok::<_, OwnerError>(())
        })
        .await
        .map_err(|_| OwnerError::Task)??;
        Ok(Self {
            journal,
            diagnostic: OwnerDiagnostic {
                node_id: binding.node_id,
                epoch: binding.epoch,
                namespace: binding.namespace,
                namespace_commitment: contract.namespace_commitment,
                repair_observed,
                phase: if create {
                    OwnerPhase::Preparing
                } else {
                    OwnerPhase::Sealed
                },
            },
        })
    }
}

fn current_transaction(
    database: &Database,
    epoch: ClusterEpoch,
    contract: CanonicalContract,
    phase: CanonicalBootstrap,
) -> Result<(), OwnerError> {
    let mut transaction = database
        .begin_write()
        .map_err(|_| OwnerError::CurrentSync)?;
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| OwnerError::CurrentSync)?;
    {
        let table = transaction
            .open_table(META)
            .map_err(|_| OwnerError::CurrentSync)?;
        validate_metadata(
            &table,
            epoch,
            Some(contract),
            phase == CanonicalBootstrap::Sealed,
        )
        .map_err(|_| OwnerError::CurrentSync)?;
        let value = table
            .get("canonical_bootstrap")
            .map_err(|_| OwnerError::CurrentSync)?
            .ok_or(OwnerError::CurrentSync)?;
        let actual: CanonicalBootstrap =
            decode(epoch, value.value()).map_err(|_| OwnerError::CurrentSync)?;
        if actual != phase {
            return Err(OwnerError::CurrentSync);
        }
    }
    // No authority record changes. Qualification must observe this exact
    // Immediate commit reaching backend sync before the owner is returned;
    // a later Database Drop is not the current barrier.
    transaction.commit().map_err(|_| OwnerError::CurrentSync)
}

#[cfg(test)]
#[path = "canonical_owner_tests.rs"]
mod tests;
