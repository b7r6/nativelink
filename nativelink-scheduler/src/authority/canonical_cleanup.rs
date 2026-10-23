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

//! Staged final storage custody for a future supervised recovery owner.

use std::sync::Arc;
use std::time::Duration;

use nativelink_util::spawn_blocking;
use redb::Database;

use super::OwnerError;

#[cfg(test)]
#[derive(Default)]
pub(super) struct Hooks {
    pub(super) before_drop: Option<DropGate>,
}

#[cfg(test)]
pub(super) struct DropGate {
    pub(super) reached: tokio::sync::oneshot::Sender<()>,
    pub(super) resume: std::sync::mpsc::Receiver<()>,
}

/// Wait for every other database holder, then await actual blocking destruction.
///
/// The caller must already have stopped the Raft core and tick and dropped all
/// its local Journal/Raft handles. It must own this future in a supervisor that
/// public waiter cancellation cannot drop. This helper alone does not establish
/// that supervision or handle supervisor panic/runtime teardown.
///
/// No I/O/proposal queue is held while waiting: surviving workers must be free
/// to finish. Atomic transfer also excludes a late clone racing the last holder.
/// A blocked worker may keep cleanup pending. Completion is not a durability
/// acknowledgement and does not release unrelated external Ownership aliases.
#[allow(
    dead_code,
    reason = "Sealed recovery supervision must be qualified before adding a production caller"
)]
pub(super) async fn drain_database(
    mut database: Arc<Database>,
    #[cfg(test)] hooks: Hooks,
) -> Result<(), OwnerError> {
    let database = loop {
        match Arc::try_unwrap(database) {
            Ok(database) => break database,
            Err(retained) => database = retained,
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    spawn_blocking!("canonical_final_database_drop", move || {
        #[cfg(test)]
        if let Some(gate) = hooks.before_drop {
            gate.reached.send(()).map_err(|()| OwnerError::Task)?;
            gate.resume
                .recv_timeout(Duration::from_secs(10))
                .map_err(|_| OwnerError::Task)?;
        }
        drop(database);
        Ok(())
    })
    .await
    .map_err(|_| OwnerError::Task)?
}
