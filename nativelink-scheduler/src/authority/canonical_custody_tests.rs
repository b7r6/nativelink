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

//! Existing shutdown/storage boundaries, before a canonical recovery runtime.
//! The first two fixtures drive actual OpenRaft tasks. Remaining fixtures are
//! explicitly private-reference and blocking-I/O controls, not Raft task proof.

use std::collections::BTreeSet;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use nativelink_macro::nativelink_test;
use nativelink_util::{background_spawn, spawn_blocking};
use redb::{Database, ReadableDatabase};
use tokio::sync::oneshot;

use super::super::{LIMITS, PreparedCanonicalOwner};
use super::{Fixture, applied};
use crate::authority::format::{
    ACCEPTED_HISTORY, AdmissionRegistry, AuthorityDomain, FormatControl, JournalProfile,
    ReaderCapabilities,
};
use crate::authority::journal::{
    Batch, JournalBinding as AuthorityJournalBinding, JournalDatabase, MeasuredOperation, Singleton,
};
use crate::authority::{ClusterEpoch, Limits};

const WAIT: Duration = Duration::from_secs(10);

async fn existing_singleton(fixture: &Fixture, reader: ReaderCapabilities) -> Singleton {
    let token = fixture.token(true);
    let facts = token.journal_binding();
    let domain = AuthorityDomain {
        epoch: ClusterEpoch(facts.epoch),
        namespace: facts.namespace,
    };
    // This is the existing experimental factory, not a canonical constructor.
    // Its history profile is used only to reach the existing real apply gate.
    Singleton::open_from_factory(
        AuthorityJournalBinding {
            node_id: facts.node_id,
            domain,
        },
        facts.node_id,
        Limits {
            max_effects: 1024,
            ..LIMITS
        },
        JournalProfile {
            domain,
            incarnation: 1,
            reader,
            admission: AdmissionRegistry::new(domain),
        },
        move || {
            let (database, parent) = token.create_journal()?.into_parts();
            let parent = parent.ok_or_else(|| io::Error::other("missing creation parent"))?;
            Ok(JournalDatabase::created(database, move || {
                parent.sync_all()
            }))
        },
    )
    .await
    .unwrap()
}

async fn gone(database: &Weak<Database>) {
    tokio::time::timeout(WAIT, async {
        while database.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("database references did not finish after the explicit release gate");
}

fn another_reference(database: Arc<Database>) -> Arc<Database> {
    match Arc::try_unwrap(database) {
        Ok(_) => panic!("transferred the database while the witnessed holder was alive"),
        Err(database) => database,
    }
}

fn last_reference(database: Arc<Database>) -> Database {
    Arc::try_unwrap(database).expect("a completed private holder retained the database")
}

async fn last_reference_after_work(mut database: Arc<Database>) -> Database {
    // Test cleanup only, after the blocked-state assertions. A closure can
    // release its queue guard just before dropping its captured database Arc.
    // This helper is not a production finalizer or a new recovery API stub.
    tokio::time::timeout(WAIT, async move {
        loop {
            match Arc::try_unwrap(database) {
                Ok(database) => return database,
                Err(retained) => database = retained,
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("completed blocking work retained the final database reference")
}

#[nativelink_test]
async fn custody_real_state_machine_outlives_existing_core_shutdown() {
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
    let activation = background_spawn!("custody_real_apply", async move {
        journal
            .propose_batch(
                &raft,
                Batch::format(FormatControl::Activate(preparation)).unwrap(),
            )
            .await
    });
    tokio::time::timeout(WAIT, reached)
        .await
        .expect("real state-machine worker never reached its existing apply gate")
        .unwrap();
    let database = Arc::downgrade(&node.journal.database);
    let metrics = node.raft.metrics();
    tokio::time::timeout(WAIT, node.shutdown())
        .await
        .expect("core shutdown did not finish while state-machine apply was paused")
        .unwrap();
    assert_eq!(metrics.borrow().state, openraft::ServerState::Shutdown);
    // Remove the proposal task's Journal/Raft clones before observing custody.
    assert!(
        tokio::time::timeout(WAIT, activation)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(database.upgrade().is_some());
    fixture.locked();

    release.send(()).unwrap();
    gone(&database).await;
    fixture.released().await;
    // The actual state-machine transaction ran after the core shutdown above.
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
async fn custody_real_spawned_snapshot_outlives_existing_core_shutdown() {
    let fixture = Fixture::new();
    let node = existing_singleton(&fixture, ReaderCapabilities::Base).await;
    node.read().await.unwrap();
    node.journal.enable_measurements();
    let measurements = node.journal.measurements.clone();
    let counter = &measurements.operations[MeasuredOperation::BuildSnapshot as usize];
    assert_eq!(counter.calls.load(Ordering::Relaxed), 0);
    let queue = node.journal.queue.clone().lock_owned().await;
    // Only this real Raft trigger requests a snapshot. The observed call is
    // made by OpenRaft's separately spawned snapshot builder, not this fixture.
    node.raft.trigger().snapshot().await.unwrap();
    tokio::time::timeout(WAIT, async {
        while counter.calls.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual spawned snapshot builder never entered Journal.io");
    assert_eq!(counter.calls.load(Ordering::Relaxed), 1);
    assert_eq!(counter.completed_calls.load(Ordering::Relaxed), 0);
    let database = Arc::downgrade(&node.journal.database);
    let metrics = node.raft.metrics();
    tokio::time::timeout(WAIT, node.shutdown())
        .await
        .expect("core shutdown did not finish while its snapshot builder was queued")
        .unwrap();
    assert_eq!(metrics.borrow().state, openraft::ServerState::Shutdown);
    assert!(database.upgrade().is_some());
    assert_eq!(counter.completed_calls.load(Ordering::Relaxed), 0);
    fixture.locked();

    drop(queue);
    gone(&database).await;
    fixture.released().await;
    assert_eq!(counter.completed_calls.load(Ordering::Relaxed), 1);
    let owned = fixture.owned(false);
    let (database, _) = fixture.directory(&owned).reopen().unwrap().into_parts();
    let transaction = database.begin_read().unwrap();
    assert!(
        transaction
            .open_table(super::super::META)
            .unwrap()
            .get("snapshot")
            .unwrap()
            .is_some()
    );
}

#[nativelink_test]
async fn custody_private_late_clone_prevents_last_database_transfer() {
    let fixture = Fixture::new();
    let owner = PreparedCanonicalOwner::prepare_cold(fixture.token(true))
        .await
        .unwrap();
    let database = owner.journal.database.clone();
    let first = owner.journal.clone();
    owner.release().await.unwrap();
    let database = another_reference(database);
    fixture.locked();
    // This is a controlled private clone, not a claim about a queued Raft task.
    let late = first.clone();
    drop(first);
    let database = another_reference(database);
    fixture.locked();
    drop(late);
    let database = last_reference(database);
    spawn_blocking!("custody_private_clone_drop", move || drop(database))
        .await
        .unwrap();
    fixture.released().await;
}

#[nativelink_test]
async fn custody_cancelled_blocking_io_prevents_last_database_transfer() {
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
    let task = background_spawn!("custody_cancelled_io", async move {
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
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    owner.release().await.unwrap();
    let database = another_reference(database);
    assert!(queue.try_lock().is_err());
    fixture.locked();
    release.send(()).unwrap();
    tokio::time::timeout(WAIT, complete).await.unwrap().unwrap();
    // Completion sent inside the closure precedes its capture destruction.
    // Wait for its queue guard to drop, then use atomic ownership transfer.
    let guard = tokio::time::timeout(WAIT, queue.lock_owned())
        .await
        .unwrap();
    drop(guard);
    let database = last_reference_after_work(database).await;
    spawn_blocking!("custody_cancelled_io_drop", move || drop(database))
        .await
        .unwrap();
    fixture.released().await;
}

#[nativelink_test]
async fn custody_private_final_database_destruction_has_a_separate_completion() {
    let fixture = Fixture::new();
    let owner = PreparedCanonicalOwner::prepare_cold(fixture.token(true))
        .await
        .unwrap();
    let database = owner.journal.database.clone();
    owner.release().await.unwrap();
    let database = last_reference(database);
    let (reached, waiting) = oneshot::channel();
    let (release, resumed) = std::sync::mpsc::channel();
    let (finished, complete) = oneshot::channel();
    let task = spawn_blocking!("custody_private_final_drop", move || {
        reached.send(()).unwrap();
        resumed.recv_timeout(WAIT).unwrap();
        drop(database);
        finished.send(()).unwrap();
    });
    tokio::time::timeout(WAIT, waiting).await.unwrap().unwrap();
    fixture.locked();
    let mut complete = complete;
    assert!(matches!(
        complete.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap();
    complete.await.unwrap();
    fixture.released().await;
}

#[nativelink_test]
async fn custody_private_cleanup_journal_itself_prevents_last_transfer() {
    let fixture = Fixture::new();
    let owner = PreparedCanonicalOwner::prepare_cold(fixture.token(true))
        .await
        .unwrap();
    let database = owner.journal.database.clone();
    let cleanup_journal = owner.journal.clone();
    owner.release().await.unwrap();
    // A finalizer retaining this Journal while waiting on its database would
    // retain itself forever. This fixture identifies that local reference.
    let database = another_reference(database);
    fixture.locked();
    drop(cleanup_journal);
    let database = last_reference(database);
    spawn_blocking!("custody_private_cleanup_drop", move || drop(database))
        .await
        .unwrap();
    fixture.released().await;
}
