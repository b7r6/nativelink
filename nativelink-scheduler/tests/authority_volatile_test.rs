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

//! Low-level RAM backend lifetime, atomicity and format checks. Cluster tests
//! separately establish learner replacement and fail-closed quorum loss; these
//! direct storage calls confer no permission to serve authority or reuse an ID.

use std::collections::{BTreeMap, BTreeSet};

use nativelink_macro::nativelink_test;
use nativelink_scheduler::authority::journal::{
    Batch, Journal, JournalFault, JournalPersistence, MAX_MEMBERSHIP_BYTES, MAX_SNAPSHOT_ID_BYTES,
    SnapshotBuffer, TypeConfig,
};
use nativelink_scheduler::authority::{
    ActionKey, ClusterEpoch, Event, Limits, MAX_PAYLOAD_BYTES, OperationId, RecordedEvent,
    Rejection, Reply, Request, Resources, State,
};
use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::testing::{StoreBuilder, Suite};
use openraft::{
    BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership, RaftLogReader,
    RaftSnapshotBuilder, StorageError, StoredMembership, Vote,
};

const EPOCH: ClusterEpoch = ClusterEpoch(89);
const NODE: u64 = 5;

fn event(operation: u64) -> RecordedEvent {
    RecordedEvent {
        epoch: EPOCH,
        event: Event::Submit {
            operation: OperationId(operation),
            request: Request {
                action: ActionKey {
                    instance: "volatile-journal-test".to_owned(),
                    digest_function: "sha256".to_owned(),
                    digest: format!("{operation:064x}"),
                    digest_size_bytes: 1,
                },
                execution_scope: None,
                do_not_cache: false,
                skip_cache_lookup: false,
                priority: 0,
                properties: BTreeMap::new(),
                resources: Resources {
                    cpu_millis: 100,
                    memory_bytes: 1024,
                    slots: 1,
                },
                execution_timeout: 100,
                payload: None,
            },
        },
    }
}

fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, NODE), index)
}

fn entry(index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(index),
        payload: EntryPayload::Normal(Batch::new([event(index + 1)]).unwrap()),
    }
}

async fn fresh(node_id: u64) -> Journal {
    Journal::fresh_volatile(node_id, EPOCH, Limits::default())
        .await
        .unwrap()
}

async fn assert_pristine(journal: &mut Journal) {
    assert_eq!(journal.persistence(), JournalPersistence::Volatile);
    assert_eq!(journal.read_vote().await.unwrap(), None);
    assert_eq!(journal.read_committed().await.unwrap(), None);
    let logs = journal.get_log_state().await.unwrap();
    assert_eq!(logs.last_log_id, None);
    assert_eq!(logs.last_purged_log_id, None);
    assert_eq!(
        journal.try_get_log_entries(..).await.unwrap(),
        Vec::<Entry<TypeConfig>>::new()
    );
    assert_eq!(
        journal.applied_state().await.unwrap(),
        (None, StoredMembership::default())
    );
    assert_eq!(
        journal.read_applied().await.unwrap(),
        (None, State::new(EPOCH, Limits::default()).unwrap())
    );
    assert!(journal.get_current_snapshot().await.unwrap().is_none());
}

#[nativelink_test]
async fn clones_share_history_but_a_fresh_backend_retains_no_authority() {
    let mut journal = fresh(NODE).await;
    assert_pristine(&mut journal).await;
    let mut surviving_handle = journal.clone();
    let vote = Vote::new_committed(3, NODE);
    journal.save_vote(&vote).await.unwrap();
    let membership = Entry {
        log_id: log_id(0),
        payload: EntryPayload::Membership(Membership::new(vec![BTreeSet::from([NODE])], None)),
    };
    journal
        .blocking_append([membership.clone(), entry(1)])
        .await
        .unwrap();
    journal.save_committed(Some(log_id(1))).await.unwrap();
    journal.apply([membership, entry(1)]).await.unwrap();
    let snapshot = journal.build_snapshot().await.unwrap();
    journal.purge(log_id(0)).await.unwrap();
    let expected = journal.read_applied().await.unwrap();
    assert_eq!(expected.1.view().operations.len(), 1);
    assert_eq!(expected.1.view().effects.len(), 1);
    drop(journal);

    assert_eq!(surviving_handle.read_vote().await.unwrap(), Some(vote));
    assert_eq!(
        surviving_handle.read_committed().await.unwrap(),
        Some(log_id(1))
    );
    assert_eq!(surviving_handle.read_applied().await.unwrap(), expected);
    assert_eq!(
        surviving_handle
            .try_get_log_entries(..)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        surviving_handle
            .get_log_state()
            .await
            .unwrap()
            .last_purged_log_id,
        Some(log_id(0))
    );
    assert_eq!(
        surviving_handle
            .get_current_snapshot()
            .await
            .unwrap()
            .unwrap()
            .meta,
        snapshot.meta
    );
    drop(surviving_handle);

    // A replacement uses a new member identity. No constructor recovers the
    // old member's vote, acknowledged operations, outbox, or membership.
    assert_pristine(&mut fresh(NODE + 1).await).await;
}

#[nativelink_test]
async fn volatile_commit_cutpoints_keep_log_and_applied_outbox_atomic() {
    let mut journal = fresh(NODE).await;
    journal.enable_measurements();
    journal.inject_failure_once(JournalFault::BeforeCommit);
    assert!(journal.blocking_append([entry(0)]).await.is_err());
    assert_eq!(
        journal.try_get_log_entries(..).await.unwrap(),
        Vec::<Entry<TypeConfig>>::new()
    );
    journal.inject_failure_once(JournalFault::AfterCommit);
    assert!(journal.blocking_append([entry(0)]).await.is_err());
    assert_eq!(journal.try_get_log_entries(..).await.unwrap().len(), 1);

    journal.inject_failure_once(JournalFault::BeforeCommit);
    assert!(journal.apply([entry(0)]).await.is_err());
    assert_eq!(
        journal.read_applied().await.unwrap(),
        (None, State::new(EPOCH, Limits::default()).unwrap())
    );
    journal.inject_failure_once(JournalFault::AfterCommit);
    assert!(journal.apply([entry(0)]).await.is_err());
    let committed = journal.read_applied().await.unwrap();
    assert_eq!(committed.0, Some(log_id(0)));
    assert_eq!(committed.1.view().operations.len(), 1);
    assert_eq!(committed.1.view().effects.len(), 1);
    journal.apply([entry(0)]).await.unwrap();
    assert_eq!(journal.read_applied().await.unwrap(), committed);

    let measured = journal.measurements();
    assert!(measured.values().all(|value| value.durable_commits == 0));
    assert_eq!(measured["append"].volatile_commits, 1);
    assert_eq!(measured["apply"].volatile_commits, 2);
}

#[nativelink_test]
async fn volatile_initialization_and_admission_use_the_same_payload_and_count_bounds() {
    for max_payload_bytes in [MAX_PAYLOAD_BYTES + 1, usize::MAX] {
        assert!(
            Journal::fresh_volatile(
                NODE,
                EPOCH,
                Limits {
                    max_payload_bytes,
                    ..Limits::default()
                }
            )
            .await
            .is_err()
        );
    }
    let mut zero = Journal::fresh_volatile(
        NODE,
        EPOCH,
        Limits {
            max_payload_bytes: 0,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        zero.apply([entry(0)]).await.unwrap()[0].replies,
        [Reply::Rejected(Rejection::Invalid)]
    );
    assert_eq!(
        zero.read_applied().await.unwrap().1.view().effects,
        Vec::<nativelink_scheduler::authority::Effect>::new()
    );

    let limits = Limits {
        max_operations: 1,
        ..Limits::default()
    };
    let mut journal = Journal::fresh_volatile(NODE + 1, EPOCH, limits)
        .await
        .unwrap();
    assert!(matches!(
        journal.apply([entry(0)]).await.unwrap()[0].replies[0],
        Reply::Submitted { .. }
    ));
    let before = journal.read_applied().await.unwrap().1;
    assert_eq!(
        journal.apply([entry(1)]).await.unwrap()[0].replies,
        [Reply::Rejected(Rejection::Overloaded)]
    );
    assert_eq!(journal.read_applied().await.unwrap().1, before);

    let mut wrong_epoch = event(1);
    wrong_epoch.epoch = ClusterEpoch(EPOCH.0 + 1);
    let wrong_epoch = Entry {
        log_id: log_id(2),
        payload: EntryPayload::Normal(Batch::new([wrong_epoch]).unwrap()),
    };
    assert_eq!(
        journal.apply([wrong_epoch]).await.unwrap()[0].replies,
        [Reply::Rejected(Rejection::WrongEpoch)]
    );
    assert_eq!(journal.read_applied().await.unwrap().1, before);
}

#[nativelink_test]
async fn volatile_membership_and_snapshot_validation_cannot_partially_install_state() {
    let mut journal = fresh(NODE).await;
    let oversized = Entry {
        log_id: log_id(1),
        payload: EntryPayload::Membership(Membership::new(
            vec![BTreeSet::from([NODE])],
            BTreeMap::from([(NODE, BasicNode::new("\0".repeat(MAX_MEMBERSHIP_BYTES / 6)))]),
        )),
    };
    assert!(
        journal
            .blocking_append([entry(0), oversized.clone()])
            .await
            .is_err()
    );
    assert!(journal.apply([entry(0), oversized]).await.is_err());
    assert_pristine(&mut journal).await;

    let mut source = fresh(NODE + 1).await;
    source.apply([entry(0)]).await.unwrap();
    let snapshot = source.build_snapshot().await.unwrap();
    for (field, replacement) in [
        ("version", serde_json::json!(99)),
        (
            "required_sections",
            serde_json::json!(["unknown-authority-section"]),
        ),
        ("epoch", serde_json::json!(EPOCH.0 + 1)),
        ("checksum", serde_json::json!("corrupt")),
    ] {
        let mut envelope: serde_json::Value =
            serde_json::from_slice(snapshot.snapshot.get_ref()).unwrap();
        envelope[field] = replacement;
        assert!(
            journal
                .install_snapshot(
                    &snapshot.meta,
                    Box::new(
                        SnapshotBuffer::from_bytes(serde_json::to_vec(&envelope).unwrap()).unwrap()
                    ),
                )
                .await
                .is_err()
        );
        assert_pristine(&mut journal).await;
    }
    let mut oversized_meta = snapshot.meta.clone();
    oversized_meta.snapshot_id = "x".repeat(MAX_SNAPSHOT_ID_BYTES + 1);
    assert!(
        journal
            .install_snapshot(&oversized_meta, snapshot.snapshot.clone())
            .await
            .is_err()
    );
    assert_pristine(&mut journal).await;

    journal
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert_eq!(
        journal.read_applied().await.unwrap(),
        source.read_applied().await.unwrap()
    );
    assert_eq!(journal.read_vote().await.unwrap(), None);
}

#[derive(Clone, Copy, Debug)]
struct Builder;

impl StoreBuilder<TypeConfig, Journal, Journal, ()> for Builder {
    async fn build(&self) -> Result<((), Journal, Journal), StorageError<u64>> {
        let journal = Journal::fresh_volatile(NODE, EPOCH, Limits::default()).await?;
        Ok(((), journal.clone(), journal))
    }
}

#[test]
fn pinned_openraft_split_storage_suite_uses_the_volatile_transaction_path() {
    Suite::test_all(Builder).unwrap();
}
