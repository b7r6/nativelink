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

use std::fmt;
use std::sync::Arc;

use rustix::fs::fsync;
use sha2::{Digest, Sha256};

use super::{Binding, Error, Ownership, Role, failure};

const COMMITMENT_DOMAIN: &[u8] = b"nativelink.canonical.namespace.v1\0";
const ROLES: [Role; 4] = [
    Role::Journal,
    Role::Cas,
    Role::TemporaryActionCache,
    Role::Worker,
];

/// Current ownership-record synchronization with retained namespace custody.
///
/// This object retains all four cooperative locks. It proves no historical
/// acknowledgement, physical-directory continuity, journal or object durability,
/// worker admission, fencing, or execution authority. Existing ownership aliases
/// may remain; this is not linear ownership or protection from other writers.
/// No authority constructor consumes this prerequisite yet.
pub struct SyncedNamespace {
    commitment: [u8; 32],
    _ownership: Arc<Ownership>,
}

impl SyncedNamespace {
    /// Fixed SHA-256 compatibility commitment minted from retained namespace
    /// facts. These bytes alone do not grant custody or execution authority.
    pub const fn commitment(&self) -> [u8; 32] {
        self.commitment
    }
}

impl fmt::Debug for SyncedNamespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SyncedNamespace")
            .field("commitment", &self.commitment)
            .finish_non_exhaustive()
    }
}

impl Ownership {
    /// Validate all four current records, then synchronize their files and
    /// pinned parent directories, retaining all locks throughout. Record
    /// acceptance is exactly the same as `reopen`; no records are rewritten.
    ///
    /// Attachment checks precede and follow synchronization. They detect a
    /// replacement only at those instants, not future or hostile mutations.
    /// Failure returns no token and preserves every artifact. A later explicit
    /// successful retry does not establish a past acknowledgement.
    ///
    /// This is blocking I/O. An async caller must move ownership into its
    /// blocking task so a dropped waiter cannot release custody during I/O.
    pub fn resynchronize(self: Arc<Self>) -> Result<SyncedNamespace, Error> {
        self.resynchronize_inner(
            #[cfg(test)]
            || {},
        )
    }

    pub(super) fn resynchronize_inner(
        self: Arc<Self>,
        #[cfg(test)] mut before_sync: impl FnMut(),
    ) -> Result<SyncedNamespace, Error> {
        self.verify_attached()?;
        // Check even the last record before the first sync, and keep every
        // opened file pinned until the complete operation has finished.
        let records = self
            .roots
            .iter()
            .map(|root| self.open_record(root))
            .collect::<Result<Vec<_>, _>>()?;
        let commitment = self.binding.namespace_commitment()?;
        for (root, record) in self.roots.iter().zip(&records) {
            #[cfg(test)]
            before_sync();
            record.sync_all().map_err(|_| failure("sync_record"))?;
            #[cfg(test)]
            before_sync();
            fsync(&root.descriptor).map_err(|_| failure("sync_directory"))?;
        }
        self.verify_attached()?;
        Ok(SyncedNamespace {
            commitment,
            _ownership: self,
        })
    }
}

impl Binding {
    pub(super) fn namespace_commitment(&self) -> Result<[u8; 32], Error> {
        if self.roots.len() != ROLES.len()
            || self
                .roots
                .iter()
                .zip(ROLES)
                .any(|(root, role)| root.role != role)
        {
            return Err(failure("invalid_namespace_roots"));
        }
        let mut digest = Sha256::new();
        digest.update(COMMITMENT_DOMAIN);
        digest.update(self.version.to_le_bytes());
        text(&mut digest, &self.instance)?;
        digest.update(self.node.to_le_bytes());
        digest.update(self.epoch.to_le_bytes());
        digest.update(self.authority_namespace.to_le_bytes());
        text(&mut digest, &self.principal)?;
        text(&mut digest, &self.cas_namespace)?;
        text(&mut digest, &self.sink_namespace)?;
        digest.update(self.worker.to_le_bytes());
        text(&mut digest, &self.execution_configuration)?;
        for root in &self.roots {
            digest.update([match root.role {
                Role::Journal => 1,
                Role::Cas => 2,
                Role::TemporaryActionCache => 3,
                Role::Worker => 4,
            }]);
            digest.update(root.identity.device.to_le_bytes());
            digest.update(root.identity.inode.to_le_bytes());
        }
        nonzero_commitment(digest.finalize().into())
    }
}

fn text(digest: &mut Sha256, value: &str) -> Result<(), Error> {
    let length = u32::try_from(value.len()).map_err(|_| failure("namespace_text_too_large"))?;
    digest.update(length.to_le_bytes());
    digest.update(value.as_bytes());
    Ok(())
}

#[expect(
    clippy::redundant_pub_crate,
    reason = "The sibling private tests qualify the reserved digest without exposing this helper."
)]
pub(super) fn nonzero_commitment(commitment: [u8; 32]) -> Result<[u8; 32], Error> {
    if commitment == [0; 32] {
        return Err(failure("unbound_namespace_commitment"));
    }
    Ok(commitment)
}
