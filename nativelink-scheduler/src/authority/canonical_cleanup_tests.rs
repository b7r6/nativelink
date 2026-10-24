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

//! The staged private drain under actual task and controlled storage holders.
//! This does not construct a recovered canonical owner or prove supervision.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use nativelink_macro::nativelink_test;
use nativelink_util::background_spawn;
use redb::{Database, ReadableDatabase};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::super::super::canonical_cleanup::{DropGate, Hooks, drain_database};
use super::super::super::{OwnerError, PreparedCanonicalOwner};
use super::{Fixture, WAIT, applied, existing_singleton};
use crate::authority::ClusterEpoch;
use crate::authority::format::{ACCEPTED_HISTORY, FormatControl, ReaderCapabilities};
use crate::authority::journal::{Batch, META, MeasuredOperation};

type Cleanup = JoinHandle<Result<(), OwnerError>>;

fn cleanup(database: Arc<Database>) -> Cleanup {
    background_spawn!("qualify_private_cleanup", async move {
        drain_database(database, Hooks::default()).await
    })
}

async fn still_pending(task: &mut Cleanup) {
    // A returned result, including an error, is the failing observation here.
    // A stage timeout elsewhere cannot substitute for reaching the held gate.
    assert!(
        tokio::time::timeout(Duration::from_millis(25), task)
            .await
            .is_err(),
        "private cleanup returned before the witnessed holder or drop gate was released"
    );
}

async fn completed(task: Cleanup) {
    tokio::time::timeout(WAIT, task)
        .await
        .expect("private cleanup failed to finish after the witnessed release")
        .unwrap()
        .unwrap();
}

#[nativelink_test]
async fn cleanup_waits_for_real_paused_state_machine() {
    let fixture = Fixture::new();
    let node = existing_singleton(&fixture, ReaderCapabilities::AcceptedHistory).await;
    let preparation = node
        .journal
        .prepare_features(&node.raft, BTreeSet::from([ACCEPTED_HISTORY]))
        .await
        .unwrap();
    node.journal
        .propose_batch(
            &node.raft,
            Batch::format(FormatControl::Prepare(preparation.clone())).unwrap(),
        )
        .await
        .unwrap();
    let (reached, release) = node.journal.pause_history_apply_once();
    let journal = node.journal.clone();
    let raft = node.raft.clone();
    let activation = background_spawn!("cleanup_real_apply", async move {
        journal
            .propose_batch(
                &raft,
                Batch::format(FormatControl::Activate(preparation)).unwrap(),
            )
            .await
    });
    tokio::time::timeout(WAIT, reached).await.unwrap().unwrap();
    let database = node.journal.database.clone();
    let metrics = node.raft.metrics();
    tokio::time::timeout(WAIT, node.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metrics.borrow().state, openraft::ServerState::Shutdown);
    assert!(
        tokio::time::timeout(WAIT, activation)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    let mut task = cleanup(database);
    still_pending(&mut task).await;
    fixture.locked();
    release.send(()).unwrap();
    completed(task).await;
    fixture.released().await;

    let owned = fixture.owned(false);
    let (database, _) = fixture.directory(&owned).reopen().unwrap().into_parts();
    assert!(
        applied(
            &database,
            ClusterEpoch(fixture.manifest.authority.cluster_epoch)
        )
        .format
        .required
        .contains(&ACCEPTED_HISTORY)
    );
}

#[nativelink_test]
async fn cleanup_waits_for_real_spawned_snapshot() {
    let fixture = Fixture::new();
    let node = existing_singleton(&fixture, ReaderCapabilities::Base).await;
    node.read().await.unwrap();
    node.journal.enable_measurements();
    let measurements = node.journal.measurements.clone();
    let counter = &measurements.operations[MeasuredOperation::BuildSnapshot as usize];
    assert_eq!(counter.calls.load(Ordering::Relaxed), 0);
    let queue = node.journal.queue.clone().lock_owned().await;
    node.raft.trigger().snapshot().await.unwrap();
    tokio::time::timeout(WAIT, async {
        while counter.calls.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(counter.calls.load(Ordering::Relaxed), 1);
    assert_eq!(counter.completed_calls.load(Ordering::Relaxed), 0);
    let database = node.journal.database.clone();
    let metrics = node.raft.metrics();
    tokio::time::timeout(WAIT, node.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metrics.borrow().state, openraft::ServerState::Shutdown);
    let mut task = cleanup(database);
    still_pending(&mut task).await;
    assert_eq!(counter.completed_calls.load(Ordering::Relaxed), 0);
    fixture.locked();
    drop(queue);
    completed(task).await;
    fixture.released().await;
    assert_eq!(counter.completed_calls.load(Ordering::Relaxed), 1);

    let owned = fixture.owned(false);
    let (database, _) = fixture.directory(&owned).reopen().unwrap().into_parts();
    let transaction = database.begin_read().unwrap();
    assert!(
        transaction
            .open_table(META)
            .unwrap()
            .get("snapshot")
            .unwrap()
            .is_some()
    );
}

#[nativelink_test]
async fn cleanup_waits_for_private_late_clone() {
    let fixture = Fixture::new();
    let owner = PreparedCanonicalOwner::prepare_cold(fixture.token(true))
        .await
        .unwrap();
    let database = owner.journal.database.clone();
    let first = owner.journal.clone();
    owner.release().await.unwrap();
    let mut task = cleanup(database);
    still_pending(&mut task).await;
    let late = first.clone();
    drop(first);
    still_pending(&mut task).await;
    fixture.locked();
    drop(late);
    completed(task).await;
    fixture.released().await;
}

#[nativelink_test]
async fn cleanup_waits_for_cancelled_blocking_io() {
    let fixture = Fixture::new();
    let owner = PreparedCanonicalOwner::prepare_cold(fixture.token(true))
        .await
        .unwrap();
    let database = owner.journal.database.clone();
    let journal = owner.journal.clone();
    let queue = journal.queue.clone();
    let (reached, waiting) = oneshot::channel();
    let (release, resumed) = std::sync::mpsc::channel();
    let (finished, complete) = oneshot::channel();
    let writer = background_spawn!("cleanup_cancelled_io", async move {
        journal
            .io(MeasuredOperation::ReadApplied, 0, move |_| {
                reached.send(()).unwrap();
                resumed.recv_timeout(WAIT).unwrap();
                finished.send(()).unwrap();
                Ok(())
            })
            .await
    });
    tokio::time::timeout(WAIT, waiting).await.unwrap().unwrap();
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    owner.release().await.unwrap();
    let mut task = cleanup(database);
    still_pending(&mut task).await;
    assert!(queue.try_lock().is_err());
    fixture.locked();
    release.send(()).unwrap();
    tokio::time::timeout(WAIT, complete).await.unwrap().unwrap();
    completed(task).await;
    fixture.released().await;
}

#[nativelink_test]
async fn cleanup_waits_for_final_database_destruction() {
    let fixture = Fixture::new();
    let owner = PreparedCanonicalOwner::prepare_cold(fixture.token(true))
        .await
        .unwrap();
    let database = owner.journal.database.clone();
    owner.release().await.unwrap();
    let (reached, waiting) = oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let mut task = background_spawn!("cleanup_final_drop", async move {
        drain_database(
            database,
            Hooks {
                before_drop: Some(DropGate { reached, resume }),
            },
        )
        .await
    });
    tokio::time::timeout(WAIT, waiting).await.unwrap().unwrap();
    still_pending(&mut task).await;
    fixture.locked();
    release.send(()).unwrap();
    completed(task).await;
    fixture.released().await;
}

#[nativelink_test]
async fn cleanup_waits_for_private_cleanup_journal() {
    let fixture = Fixture::new();
    let owner = PreparedCanonicalOwner::prepare_cold(fixture.token(true))
        .await
        .unwrap();
    let database = owner.journal.database.clone();
    let cleanup_journal = owner.journal.clone();
    owner.release().await.unwrap();
    let mut task = cleanup(database);
    still_pending(&mut task).await;
    fixture.locked();
    drop(cleanup_journal);
    completed(task).await;
    fixture.released().await;
}
