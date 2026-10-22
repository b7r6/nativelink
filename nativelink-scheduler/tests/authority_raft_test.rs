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

//! Experimental real singleton Raft and durable storage evidence. The direct
//! storage tests are cutpoint tests, not a substitute for consensus integration.

use std::collections::BTreeMap;
use std::io::{BufRead, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use nativelink_macro::nativelink_test;
use nativelink_scheduler::authority::journal::{
    Batch, Journal, JournalFault, MAX_BATCH_BYTES, MAX_BATCH_EVENTS, MAX_MEMBERSHIP_BYTES,
    MAX_SNAPSHOT_BYTES, MAX_SNAPSHOT_ID_BYTES, Singleton, SnapshotBuffer, TypeConfig,
};
use nativelink_scheduler::authority::{
    ActionKey, ClusterEpoch, Event, IncarnationId, Limits, MAX_PAYLOAD_BYTES, OperationId,
    RecordedEvent, Rejection, Reply, Request, Resources, ResultPayload, SessionId, State, WorkerId,
};
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::spawn_blocking;
use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::testing::{StoreBuilder, Suite};
use openraft::{
    CommittedLeaderId, Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder,
    StorageError, Vote,
};

const EPOCH: ClusterEpoch = ClusterEpoch(41);
const NODE: u64 = 7;

#[derive(Debug)]
struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("nativelink-authority-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn database(&self) -> PathBuf {
        self.0.join("journal.redb")
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _cleanup = std::fs::remove_dir_all(&self.0);
    }
}

fn event(operation: u64) -> RecordedEvent {
    RecordedEvent {
        epoch: EPOCH,
        event: Event::Submit {
            operation: OperationId(operation),
            request: Request {
                action: ActionKey {
                    instance: "journal-test".to_owned(),
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
        payload: EntryPayload::Normal(Batch::new([event(index)]).unwrap()),
    }
}

fn rewrite_payload(bytes: &[u8], edit: impl FnOnce(&mut serde_json::Value)) -> Vec<u8> {
    let mut envelope: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    let mut payload: serde_json::Value =
        serde_json::from_str(envelope["payload"].as_str().unwrap()).unwrap();
    edit(&mut payload);
    let payload = serde_json::to_string(&payload).unwrap();
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(payload.as_bytes());
    envelope["length"] = serde_json::json!(payload.len());
    envelope["checksum"] = serde_json::json!(hasher.finalize_digest().packed_hash().to_string());
    envelope["payload"] = serde_json::json!(payload);
    serde_json::to_vec(&envelope).unwrap()
}

async fn open(path: &Path) -> Journal {
    Journal::open(path, NODE, EPOCH, Limits::default())
        .await
        .unwrap()
}

#[nativelink_test]
async fn real_singleton_commits_then_barrier_reads_and_recovers_outbox() {
    let directory = Directory::new();
    let node = Singleton::open(directory.database(), NODE, EPOCH, Limits::default())
        .await
        .unwrap();
    let response = node.write(event(1)).await.unwrap();
    assert!(matches!(response.data, Reply::Submitted { .. }));
    let state = node.read().await.unwrap();
    assert_eq!(state.view().operations.len(), 1);
    assert_eq!(state.view().effects.len(), 1);
    assert!(node.journal.read_applied().await.unwrap().0.unwrap().index >= response.log_id.index);
    let bytes = state.snapshot().unwrap();
    node.shutdown().await.unwrap();

    let node = Singleton::open(directory.database(), NODE, EPOCH, Limits::default())
        .await
        .unwrap();
    assert_eq!(node.read().await.unwrap().snapshot().unwrap(), bytes);
    // Duplicate submission through Raft must not allocate another operation or effect.
    node.write(event(1)).await.unwrap();
    assert_eq!(node.read().await.unwrap().snapshot().unwrap(), bytes);
    node.shutdown().await.unwrap();
}

#[nativelink_test]
async fn real_snapshot_and_later_applied_state_survive_restart() {
    let directory = Directory::new();
    let node = Singleton::open(directory.database(), NODE, EPOCH, Limits::default())
        .await
        .unwrap();
    node.write(event(1)).await.unwrap();
    let snapshot = node.journal.clone().build_snapshot().await.unwrap();
    node.write(event(2)).await.unwrap();
    let expected = node.read().await.unwrap().snapshot().unwrap();
    node.shutdown().await.unwrap();

    let node = Singleton::open(directory.database(), NODE, EPOCH, Limits::default())
        .await
        .unwrap();
    assert_eq!(node.read().await.unwrap().snapshot().unwrap(), expected);
    let current = node
        .journal
        .clone()
        .get_current_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.meta, snapshot.meta);
    assert_eq!(*current.snapshot, *snapshot.snapshot);
    node.shutdown().await.unwrap();
}

#[nativelink_test]
async fn oversized_snapshot_stream_cannot_install_an_earlier_valid_prefix() {
    let directory = Directory::new();
    let node = Singleton::open(directory.database(), NODE, EPOCH, Limits::default())
        .await
        .unwrap();
    node.write(event(1)).await.unwrap();
    let mut journal = node.journal.clone();
    let snapshot = journal.build_snapshot().await.unwrap();
    let before = journal.read_applied().await.unwrap();
    let mut receiving = journal.begin_receiving_snapshot().await.unwrap();
    tokio::io::AsyncWriteExt::write_all(receiving.as_mut(), snapshot.snapshot.get_ref())
        .await
        .unwrap();
    let retained = receiving.get_ref().len();
    assert!(
        tokio::io::AsyncSeekExt::seek(
            receiving.as_mut(),
            SeekFrom::Start(MAX_SNAPSHOT_BYTES as u64 + 1)
        )
        .await
        .is_err()
    );
    assert_eq!(
        receiving.get_ref().len(),
        retained,
        "oversized sparse seek must not allocate"
    );
    assert!(
        journal
            .install_snapshot(&snapshot.meta, receiving)
            .await
            .is_err()
    );
    assert_eq!(journal.read_applied().await.unwrap(), before);
    // A legal sparse seek followed by an overflowing async write must poison
    // the same prefix, even when the transport ignores the write error.
    let mut receiving = journal.begin_receiving_snapshot().await.unwrap();
    tokio::io::AsyncWriteExt::write_all(receiving.as_mut(), snapshot.snapshot.get_ref())
        .await
        .unwrap();
    tokio::io::AsyncSeekExt::seek(
        receiving.as_mut(),
        SeekFrom::Start(u64::try_from(MAX_SNAPSHOT_BYTES).unwrap()),
    )
    .await
    .unwrap();
    assert!(
        tokio::io::AsyncWriteExt::write_all(receiving.as_mut(), b"overflow")
            .await
            .is_err()
    );
    assert_eq!(receiving.get_ref(), snapshot.snapshot.get_ref());
    assert!(
        tokio::io::AsyncWriteExt::flush(receiving.as_mut())
            .await
            .is_err()
    );
    assert!(
        tokio::io::AsyncSeekExt::seek(receiving.as_mut(), SeekFrom::Start(0))
            .await
            .is_err()
    );
    let mut prefix = [0u8; 1];
    assert!(
        tokio::io::AsyncReadExt::read(receiving.as_mut(), &mut prefix)
            .await
            .is_err()
    );
    assert!(
        journal
            .install_snapshot(&snapshot.meta, receiving)
            .await
            .is_err()
    );
    assert_eq!(journal.read_applied().await.unwrap(), before);
    let mut receiving = journal.begin_receiving_snapshot().await.unwrap();
    tokio::io::AsyncWriteExt::write_all(receiving.as_mut(), snapshot.snapshot.get_ref())
        .await
        .unwrap();
    journal
        .install_snapshot(&snapshot.meta, receiving)
        .await
        .unwrap();
    assert_eq!(journal.read_applied().await.unwrap(), before);
    node.shutdown().await.unwrap();
}

#[nativelink_test]
async fn committed_suffix_is_reapplied_before_restarted_authority_serves() {
    let directory = Directory::new();
    let node = Singleton::open(directory.database(), NODE, EPOCH, Limits::default())
        .await
        .unwrap();
    node.write(event(1)).await.unwrap();
    let snapshot = node.journal.clone().build_snapshot().await.unwrap();
    node.shutdown().await.unwrap();

    // Model the storage cutpoint after committed-log persistence but before
    // state-machine application. This is not an alternate consensus path.
    let mut journal = open(&directory.database()).await;
    let last = journal.get_log_state().await.unwrap().last_log_id.unwrap();
    let suffix_id = LogId::new(last.leader_id, last.index + 1);
    journal
        .blocking_append([Entry {
            log_id: suffix_id,
            payload: EntryPayload::Normal(Batch::new([event(2), event(3), event(4)]).unwrap()),
        }])
        .await
        .unwrap();
    journal.save_committed(Some(suffix_id)).await.unwrap();
    journal
        .purge(snapshot.meta.last_log_id.unwrap())
        .await
        .unwrap();
    assert_eq!(
        journal
            .read_applied()
            .await
            .unwrap()
            .1
            .view()
            .operations
            .len(),
        1
    );
    drop(journal);

    let node = Singleton::open(directory.database(), NODE, EPOCH, Limits::default())
        .await
        .unwrap();
    let state = node.read().await.unwrap();
    assert_eq!(state.view().operations.len(), 4);
    assert_eq!(state.view().effects.len(), 4);
    node.shutdown().await.unwrap();
}

#[nativelink_test]
async fn durable_log_callback_failure_preserves_known_uncertainty() {
    let directory = Directory::new();
    let mut journal = open(&directory.database()).await;
    let vote = Vote::new(3, NODE);
    journal.save_vote(&vote).await.unwrap();
    journal.inject_failure_once(JournalFault::BeforeCommit);
    assert!(journal.blocking_append([entry(0), entry(1)]).await.is_err());
    assert!(journal.try_get_log_entries(..).await.unwrap().is_empty());
    journal.inject_failure_once(JournalFault::AfterCommit);
    assert!(journal.blocking_append([entry(0), entry(1)]).await.is_err());
    drop(journal);

    let mut journal = open(&directory.database()).await;
    assert_eq!(journal.read_vote().await.unwrap(), Some(vote));
    assert_eq!(journal.try_get_log_entries(..).await.unwrap().len(), 2);
    assert!(
        journal
            .read_applied()
            .await
            .unwrap()
            .1
            .view()
            .operations
            .is_empty()
    );
    // A persisted log alone is not applied authority or a publication intent.
    let reply = journal.apply([entry(1)]).await.unwrap();
    assert!(matches!(reply[0].replies[0], Reply::Submitted { .. }));
    assert_eq!(
        journal.read_applied().await.unwrap().1.view().effects.len(),
        1
    );
}

#[nativelink_test]
async fn applied_state_and_outbox_commit_atomically_at_failure_cutpoints() {
    let directory = Directory::new();
    let mut journal = open(&directory.database()).await;
    journal.inject_failure_once(JournalFault::BeforeCommit);
    assert!(journal.apply([entry(1)]).await.is_err());
    let (position, state) = journal.read_applied().await.unwrap();
    assert_eq!(position, None);
    assert!(state.view().operations.is_empty());
    assert!(state.view().effects.is_empty());

    journal.inject_failure_once(JournalFault::AfterCommit);
    assert!(journal.apply([entry(1)]).await.is_err());
    drop(journal);
    let journal = open(&directory.database()).await;
    let (position, state) = journal.read_applied().await.unwrap();
    assert_eq!(position, Some(log_id(1)));
    assert_eq!(state.view().operations.len(), 1);
    assert_eq!(state.view().effects.len(), 1);
}

#[test]
fn application_batch_count_bytes_and_schema_are_bounded() {
    let tiny = RecordedEvent {
        epoch: EPOCH,
        event: Event::AdvanceTime { now: 1 },
    };
    assert!(Batch::new(std::iter::repeat_n(tiny.clone(), MAX_BATCH_EVENTS)).is_ok());
    assert!(Batch::new(std::iter::repeat_n(tiny.clone(), MAX_BATCH_EVENTS + 1)).is_err());
    assert!(Batch::new([]).is_err());
    let valid = Batch::new([tiny]).unwrap();
    let wire = serde_json::to_value(&valid).unwrap();
    for malformed in [
        serde_json::json!({ "version": 99, "events": wire["events"] }),
        serde_json::json!({ "version": 1, "events": wire["events"], "authority": true }),
        serde_json::json!({ "version": 1, "events": [] }),
        serde_json::json!({ "version": 1, "events": vec![wire["events"][0].clone(); MAX_BATCH_EVENTS + 1] }),
    ] {
        assert!(Batch::decode(&serde_json::to_vec(&malformed).unwrap()).is_err());
    }
    let duplicate = format!(
        "{{\"version\":1,\"version\":1,\"events\":{}}}",
        wire["events"]
    );
    assert!(Batch::decode(duplicate.as_bytes()).is_err());

    let mut padded = event(1);
    let Event::Submit { request, .. } = &mut padded.event else {
        unreachable!()
    };
    request
        .properties
        .insert("padding".to_owned(), "\\\n\"".to_owned());
    let framing = Batch::new([padded.clone()])
        .unwrap()
        .encoded_bytes()
        .unwrap();
    let Event::Submit { request, .. } = &mut padded.event else {
        unreachable!()
    };
    request.properties.insert(
        "padding".to_owned(),
        format!("\\\n\"{}", "x".repeat(MAX_BATCH_BYTES - framing)),
    );
    let exact = Batch::new([padded.clone()]).unwrap();
    assert_eq!(exact.encoded_bytes().unwrap(), MAX_BATCH_BYTES);
    assert_eq!(
        Batch::decode(&serde_json::to_vec(&exact).unwrap()).unwrap(),
        exact
    );
    let Event::Submit { request, .. } = &mut padded.event else {
        unreachable!()
    };
    request.properties.get_mut("padding").unwrap().push('x');
    assert!(Batch::new([padded]).is_err());
    let mut excess_raw = serde_json::to_vec(&exact).unwrap();
    excess_raw.push(b' ');
    assert!(Batch::decode(&excess_raw).is_err());
}

#[nativelink_test]
async fn real_batch_refines_ordered_events_and_replies_through_restart() {
    let directory = Directory::new();
    let mut wrong_epoch = event(2);
    wrong_epoch.epoch = ClusterEpoch(EPOCH.0 + 1);
    let events = vec![event(1), wrong_epoch, event(2), event(1), event(3)];
    let mut reference = State::new(EPOCH, Limits::default()).unwrap();
    let expected: Vec<_> = events
        .iter()
        .cloned()
        .map(|event| reference.apply(event))
        .collect();
    assert!(matches!(expected[1], Reply::Rejected(_)));
    let node = Singleton::open(directory.database(), NODE, EPOCH, Limits::default())
        .await
        .unwrap();
    let batch = Batch::new(events).unwrap();
    let response = node.write_batch(batch.clone()).await.unwrap();
    assert_eq!(response.data.replies, expected);
    assert_eq!(node.read().await.unwrap(), reference);
    assert_eq!(
        node.journal.read_applied().await.unwrap().0,
        Some(response.log_id)
    );
    let bytes = reference.snapshot().unwrap();
    // A lost reply followed by the same submitted identities must not allocate
    // duplicate execution/effect records, even when some events were rejected.
    node.write_batch(batch.clone()).await.unwrap();
    assert_eq!(node.read().await.unwrap().snapshot().unwrap(), bytes);
    node.shutdown().await.unwrap();
    let node = Singleton::open(directory.database(), NODE, EPOCH, Limits::default())
        .await
        .unwrap();
    assert_eq!(node.read().await.unwrap().snapshot().unwrap(), bytes);
    node.write_batch(batch).await.unwrap();
    assert_eq!(node.read().await.unwrap().snapshot().unwrap(), bytes);
    node.shutdown().await.unwrap();
}

#[nativelink_test]
async fn entire_batch_and_outbox_are_atomic_at_commit_cutpoints() {
    let directory = Directory::new();
    let mut journal = open(&directory.database()).await;
    let batch_entry = Entry {
        log_id: log_id(1),
        payload: EntryPayload::Normal(Batch::new([event(1), event(2), event(3)]).unwrap()),
    };
    journal.inject_failure_once(JournalFault::BeforeCommit);
    assert!(journal.apply([batch_entry.clone()]).await.is_err());
    let (position, state) = journal.read_applied().await.unwrap();
    assert_eq!(position, None);
    assert_eq!(state, State::new(EPOCH, Limits::default()).unwrap());
    journal.inject_failure_once(JournalFault::AfterCommit);
    assert!(journal.apply([batch_entry]).await.is_err());
    drop(journal);
    let journal = open(&directory.database()).await;
    let (position, state) = journal.read_applied().await.unwrap();
    assert_eq!(position, Some(log_id(1)));
    assert_eq!(state.view().operations.len(), 3);
    assert_eq!(state.view().effects.len(), 3);
    let mut expected = State::new(EPOCH, Limits::default()).unwrap();
    for operation in 1..=3 {
        expected.apply(event(operation));
    }
    assert_eq!(state, expected);
}

#[nativelink_test]
async fn oversized_membership_is_rejected_before_durable_log_acknowledgment() {
    // Both values exceed canonical membership bytes. The control-character
    // address is much smaller in memory, so counting raw string bytes is wrong.
    for address in [
        "x".repeat(MAX_MEMBERSHIP_BYTES),
        "\0".repeat(MAX_MEMBERSHIP_BYTES / 6),
    ] {
        let directory = Directory::new();
        let mut journal = open(&directory.database()).await;
        let before = journal.read_applied().await.unwrap();
        let membership = membership_with_address(address);
        assert!(serde_json::to_vec(&membership).unwrap().len() > MAX_MEMBERSHIP_BYTES);
        let oversized = Entry {
            log_id: log_id(1),
            payload: EntryPayload::Membership(membership),
        };
        let error = journal
            .blocking_append([entry(0), oversized.clone()])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("membership exceeds byte bound"));
        assert!(journal.try_get_log_entries(..).await.unwrap().is_empty());
        // A malformed later membership cannot commit the preceding command
        // through the state-machine interface either.
        let error = journal.apply([entry(0), oversized]).await.unwrap_err();
        assert!(error.to_string().contains("membership exceeds byte bound"));
        assert_eq!(journal.read_applied().await.unwrap(), before);
        drop(journal);
        let mut journal = open(&directory.database()).await;
        assert!(journal.try_get_log_entries(..).await.unwrap().is_empty());
        assert_eq!(journal.read_applied().await.unwrap(), before);
    }
}

fn membership_with_address(address: String) -> openraft::Membership<u64, openraft::BasicNode> {
    openraft::Membership::new(
        vec![std::collections::BTreeSet::from([NODE])],
        BTreeMap::from([(NODE, openraft::BasicNode { addr: address })]),
    )
}

#[nativelink_test]
async fn incoming_snapshot_metadata_limits_cannot_be_bypassed_by_valid_checksums() {
    let directory = Directory::new();
    let mut journal = open(&directory.database()).await;
    journal.apply([entry(1)]).await.unwrap();
    let snapshot = journal.build_snapshot().await.unwrap();
    let before = journal.read_applied().await.unwrap();
    let oversized_membership = openraft::StoredMembership::new(
        Some(log_id(1)),
        membership_with_address("x".repeat(MAX_MEMBERSHIP_BYTES)),
    );
    for oversize_id in [false, true] {
        let mut meta = snapshot.meta.clone();
        if oversize_id {
            // Check UTF-8 bytes, not the number of characters.
            meta.snapshot_id = "é".repeat(MAX_SNAPSHOT_ID_BYTES / 2 + 1);
        } else {
            meta.last_membership = oversized_membership.clone();
        }
        let bytes = rewrite_payload(snapshot.snapshot.get_ref(), |payload| {
            payload["meta"] = serde_json::to_value(&meta).unwrap();
            payload["applied"]["membership"] = serde_json::to_value(&meta.last_membership).unwrap();
        });
        // Test both the caller's metadata and an otherwise valid caller whose
        // payload metadata alone violates the bound. Digest checks still pass.
        for supplied_meta in [&meta, &snapshot.meta] {
            let error = journal
                .install_snapshot(
                    supplied_meta,
                    Box::new(SnapshotBuffer::from_bytes(bytes.clone()).unwrap()),
                )
                .await
                .unwrap_err();
            assert!(error.to_string().contains(if oversize_id {
                "snapshot ID exceeds byte bound"
            } else {
                "membership exceeds byte bound"
            }));
            assert_eq!(journal.read_applied().await.unwrap(), before);
        }
    }
}

#[nativelink_test]
async fn maximum_membership_and_escaped_snapshot_id_round_trip() {
    let source_dir = Directory::new();
    let target_dir = Directory::new();
    let mut source = open(&source_dir.database()).await;
    let overhead = serde_json::to_vec(&membership_with_address(String::new()))
        .unwrap()
        .len();
    let membership = membership_with_address("x".repeat(MAX_MEMBERSHIP_BYTES - overhead));
    assert_eq!(
        serde_json::to_vec(&membership).unwrap().len(),
        MAX_MEMBERSHIP_BYTES
    );
    let membership_entry = Entry {
        log_id: log_id(0),
        payload: EntryPayload::Membership(membership),
    };
    source
        .blocking_append([membership_entry.clone()])
        .await
        .unwrap();
    source.apply([membership_entry, entry(1)]).await.unwrap();
    let mut snapshot = source.build_snapshot().await.unwrap();
    snapshot.meta.snapshot_id = "\0".repeat(MAX_SNAPSHOT_ID_BYTES);
    let bytes = rewrite_payload(snapshot.snapshot.get_ref(), |payload| {
        payload["meta"] = serde_json::to_value(&snapshot.meta).unwrap();
    });
    assert!(bytes.len() < MAX_SNAPSHOT_BYTES);
    let mut target = open(&target_dir.database()).await;
    target
        .install_snapshot(
            &snapshot.meta,
            Box::new(SnapshotBuffer::from_bytes(bytes).unwrap()),
        )
        .await
        .unwrap();
    drop(target);
    let mut target = open(&target_dir.database()).await;
    assert_eq!(
        target.get_current_snapshot().await.unwrap().unwrap().meta,
        snapshot.meta
    );
    assert_eq!(
        target.read_applied().await.unwrap(),
        source.read_applied().await.unwrap()
    );
    target.apply([entry(2)]).await.unwrap();
    assert_eq!(
        target
            .read_applied()
            .await
            .unwrap()
            .1
            .view()
            .operations
            .len(),
        2
    );
}

#[nativelink_test]
async fn persisted_oversized_membership_fails_closed_in_logs_and_applied_state() {
    for mutate_log in [false, true] {
        let directory = Directory::new();
        let mut journal = open(&directory.database()).await;
        journal.blocking_append([entry(0)]).await.unwrap();
        journal.apply([entry(0)]).await.unwrap();
        drop(journal);
        let database = redb::Database::open(directory.database()).unwrap();
        let tx = database.begin_write().unwrap();
        let oversized = membership_with_address("x".repeat(MAX_MEMBERSHIP_BYTES));
        if mutate_log {
            let mut logs = tx
                .open_table(redb::TableDefinition::<u64, &[u8]>::new("authority_logs"))
                .unwrap();
            let original = logs.remove(0).unwrap().unwrap().value().to_vec();
            let bytes = rewrite_payload(&original, |payload| {
                payload["payload"] =
                    serde_json::to_value(EntryPayload::<TypeConfig>::Membership(oversized))
                        .unwrap();
            });
            // This fits the earlier log-record bound; membership's separate
            // metadata budget must reject it on recovery.
            assert!(bytes.len() < MAX_BATCH_BYTES * 2);
            logs.insert(0, bytes.as_slice()).unwrap();
        } else {
            let mut meta = tx
                .open_table(redb::TableDefinition::<&str, &[u8]>::new("authority_meta"))
                .unwrap();
            let original = meta.remove("applied").unwrap().unwrap().value().to_vec();
            let bytes = rewrite_payload(&original, |payload| {
                payload["membership"] = serde_json::to_value(openraft::StoredMembership::new(
                    Some(log_id(0)),
                    oversized,
                ))
                .unwrap();
            });
            meta.insert("applied", bytes.as_slice()).unwrap();
        }
        tx.commit().unwrap();
        drop(database);
        let error = Journal::open(directory.database(), NODE, EPOCH, Limits::default())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("membership exceeds byte bound"));
    }
}

#[nativelink_test]
async fn invalid_admission_limits_do_not_create_a_journal_file() {
    let directory = Directory::new();
    let limits = Limits {
        max_payload_bytes: MAX_PAYLOAD_BYTES + 1,
        ..Limits::default()
    };
    assert!(
        Journal::open(directory.database(), NODE, EPOCH, limits)
            .await
            .is_err()
    );
    assert!(!directory.database().exists());
}

#[nativelink_test]
async fn saturated_admission_can_finalize_and_snapshot_escaping_heavy_results() {
    let directory = Directory::new();
    let limits = Limits {
        max_payload_bytes: MAX_PAYLOAD_BYTES,
        ..Limits::default()
    };
    let node = Singleton::open(directory.database(), NODE, EPOCH, limits)
        .await
        .unwrap();
    let worker = WorkerId(1);
    let incarnation = IncarnationId(1);
    let capacity = Resources {
        cpu_millis: 100,
        memory_bytes: 1024,
        slots: 1,
    };
    let recorded = |event| RecordedEvent {
        epoch: EPOCH,
        event,
    };
    assert_eq!(
        node.write(recorded(Event::Register {
            worker,
            incarnation,
            session: SessionId(1),
            properties: BTreeMap::new(),
            capacity,
            available: capacity,
        }))
        .await
        .unwrap()
        .data,
        Reply::Applied
    );
    let Event::Submit { mut request, .. } = event(1).event else {
        unreachable!()
    };
    request.skip_cache_lookup = true;
    let mut admitted = 0;
    for id in 1..=limits.max_operations as u64 {
        let before = node.read().await.unwrap();
        let reply = node
            .write(recorded(Event::Submit {
                operation: OperationId(id),
                request: request.clone(),
            }))
            .await
            .unwrap()
            .data;
        if reply == Reply::Rejected(Rejection::Overloaded) {
            assert_eq!(node.read().await.unwrap(), before);
            break;
        }
        assert!(matches!(reply, Reply::Submitted { .. }));
        admitted += 1;
    }
    assert!(admitted > 1 && admitted < limits.max_operations);
    assert_eq!(node.read().await.unwrap().view().executions.len(), 1);
    // Restart while every admitted operation still owes a maximum-size result.
    node.shutdown().await.unwrap();
    let node = Singleton::open(directory.database(), NODE, EPOCH, limits)
        .await
        .unwrap();
    let Reply::Matched { attempt, .. } = node.write(recorded(Event::Match)).await.unwrap().data
    else {
        panic!("admitted execution must be placeable")
    };
    assert_eq!(
        node.write(recorded(Event::WorkerAccepted {
            attempt,
            worker,
            incarnation
        }))
        .await
        .unwrap()
        .data,
        Reply::Applied
    );

    // Synthetic normalized result deliberately exercises the worst JSON escape
    // shape. Real REAPI capture has additional validation covered separately.
    let mut result = ResultPayload {
        digest: String::new(),
        exit_code: 0,
        error: None,
        response: None,
    };
    let overhead = serde_json::to_vec(&result).unwrap().len();
    let content = MAX_PAYLOAD_BYTES - overhead;
    result.digest = "\\\"".repeat(content / 4);
    result.digest.push_str(&"x".repeat(content % 4));
    assert_eq!(
        serde_json::to_vec(&result).unwrap().len(),
        MAX_PAYLOAD_BYTES
    );
    let completion = recorded(Event::Complete {
        attempt,
        worker,
        incarnation,
        result,
    });
    assert!(
        Batch::new([completion.clone()])
            .unwrap()
            .encoded_bytes()
            .unwrap()
            < MAX_BATCH_BYTES
    );
    assert_eq!(node.write(completion).await.unwrap().data, Reply::Applied);
    let accepted = node.read().await.unwrap();
    assert_eq!(accepted.view().operations.len(), admitted);
    assert!(
        accepted
            .view()
            .operations
            .iter()
            .all(|operation| operation.terminal.is_some())
    );
    let mut journal = node.journal.clone();
    let snapshot = journal.build_snapshot().await.unwrap();
    // This is a substantive nested-escaping test, not an almost-empty snapshot.
    assert!(snapshot.snapshot.get_ref().len() > 40 * 1024 * 1024);
    assert!(snapshot.snapshot.get_ref().len() <= MAX_SNAPSHOT_BYTES);
    drop(snapshot);
    drop(journal);
    node.shutdown().await.unwrap();
    let node = Singleton::open(directory.database(), NODE, EPOCH, limits)
        .await
        .unwrap();
    assert_eq!(node.read().await.unwrap(), accepted);
    assert_eq!(
        node.read()
            .await
            .unwrap()
            .view()
            .workers
            .iter()
            .find(|candidate| candidate.id == worker)
            .unwrap()
            .charged,
        Resources::default()
    );
    node.shutdown().await.unwrap();
}

#[nativelink_test]
async fn snapshot_install_is_atomic_and_preserves_outbox_through_suffix() {
    let source_dir = Directory::new();
    let target_dir = Directory::new();
    let mut source = open(&source_dir.database()).await;
    source.apply([entry(1)]).await.unwrap();
    let snapshot = source.build_snapshot().await.unwrap();
    source.apply([entry(2)]).await.unwrap();
    let mut target = open(&target_dir.database()).await;
    target.inject_failure_once(JournalFault::BeforeCommit);
    assert!(
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot.clone())
            .await
            .is_err()
    );
    assert!(target.get_current_snapshot().await.unwrap().is_none());
    assert!(
        target
            .read_applied()
            .await
            .unwrap()
            .1
            .view()
            .effects
            .is_empty()
    );
    target.inject_failure_once(JournalFault::AfterCommit);
    assert!(
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .is_err()
    );
    drop(target);
    let mut target = open(&target_dir.database()).await;
    assert_eq!(
        target.get_current_snapshot().await.unwrap().unwrap().meta,
        snapshot.meta
    );
    assert_eq!(
        target.read_applied().await.unwrap().1.view().effects.len(),
        1
    );
    target.apply([entry(2)]).await.unwrap();
    assert_eq!(
        target.read_applied().await.unwrap().1,
        source.read_applied().await.unwrap().1
    );
}

#[nativelink_test]
async fn unsupported_snapshot_versions_sections_and_corruption_fail_closed() {
    let directory = Directory::new();
    let mut journal = open(&directory.database()).await;
    journal.apply([entry(1)]).await.unwrap();
    let snapshot = journal.build_snapshot().await.unwrap();
    let initial = journal.read_applied().await.unwrap().1;
    for (field, replacement) in [
        ("version", serde_json::json!(1)),
        ("version", serde_json::json!(99)),
        (
            "required_sections",
            serde_json::json!(["unknown-required-section"]),
        ),
        ("epoch", serde_json::json!(42)),
        ("checksum", serde_json::json!("corrupt")),
    ] {
        let mut envelope: serde_json::Value =
            serde_json::from_slice(snapshot.snapshot.get_ref()).unwrap();
        envelope[field] = replacement;
        let bytes = serde_json::to_vec(&envelope).unwrap();
        assert!(
            journal
                .install_snapshot(
                    &snapshot.meta,
                    Box::new(SnapshotBuffer::from_bytes(bytes).unwrap())
                )
                .await
                .is_err()
        );
        assert_eq!(journal.read_applied().await.unwrap().1, initial);
    }
}

#[nativelink_test]
async fn nonempty_snapshot_without_applied_position_is_rejected() {
    let directory = Directory::new();
    let mut journal = open(&directory.database()).await;
    journal.apply([entry(1)]).await.unwrap();
    let mut snapshot = journal.build_snapshot().await.unwrap();
    let before = journal.read_applied().await.unwrap();
    let bytes = rewrite_payload(snapshot.snapshot.get_ref(), |payload| {
        payload["meta"]["last_log_id"] = serde_json::Value::Null;
        payload["applied"]["last_applied"] = serde_json::Value::Null;
    });
    snapshot.meta.last_log_id = None;
    assert!(
        journal
            .install_snapshot(
                &snapshot.meta,
                Box::new(SnapshotBuffer::from_bytes(bytes).unwrap())
            )
            .await
            .is_err()
    );
    assert_eq!(journal.read_applied().await.unwrap(), before);
}

#[nativelink_test]
async fn persisted_log_holes_and_key_index_mismatches_fail_closed() {
    for corrupt_index in [false, true] {
        let directory = Directory::new();
        let mut journal = open(&directory.database()).await;
        journal
            .blocking_append([entry(0), entry(1), entry(2), entry(3)])
            .await
            .unwrap();
        journal.save_committed(Some(log_id(3))).await.unwrap();
        drop(journal);
        let database = redb::Database::open(directory.database()).unwrap();
        let tx = database.begin_write().unwrap();
        {
            let mut logs = tx
                .open_table(redb::TableDefinition::<u64, &[u8]>::new("authority_logs"))
                .unwrap();
            let bytes = logs.remove(2).unwrap().unwrap().value().to_vec();
            if corrupt_index {
                // The entry remains checksummed and all keys remain present.
                // Only the authoritative payload/index binding is malformed.
                let bytes = rewrite_payload(&bytes, |payload| {
                    payload["log_id"]["index"] = serde_json::json!(9);
                });
                logs.insert(2, bytes.as_slice()).unwrap();
            }
        }
        tx.commit().unwrap();
        drop(database);
        assert!(
            Journal::open(directory.database(), NODE, EPOCH, Limits::default())
                .await
                .is_err()
        );
    }
}

#[nativelink_test]
async fn persisted_state_without_applied_position_fails_closed() {
    let directory = Directory::new();
    let mut journal = open(&directory.database()).await;
    journal.apply([entry(1)]).await.unwrap();
    drop(journal);
    let database = redb::Database::open(directory.database()).unwrap();
    let tx = database.begin_write().unwrap();
    {
        let mut meta = tx
            .open_table(redb::TableDefinition::<&str, &[u8]>::new("authority_meta"))
            .unwrap();
        let bytes = meta.remove("applied").unwrap().unwrap().value().to_vec();
        let bytes = rewrite_payload(&bytes, |payload| {
            payload["last_applied"] = serde_json::Value::Null;
        });
        meta.insert("applied", bytes.as_slice()).unwrap();
    }
    tx.commit().unwrap();
    drop(database);
    assert!(
        Journal::open(directory.database(), NODE, EPOCH, Limits::default())
            .await
            .is_err()
    );
}

#[nativelink_test]
async fn wrong_identity_and_existing_empty_database_never_bootstrap() {
    let directory = Directory::new();
    let journal = open(&directory.database()).await;
    drop(journal);
    assert!(
        Journal::open(directory.database(), NODE + 1, EPOCH, Limits::default())
            .await
            .is_err()
    );
    assert!(
        Journal::open(
            directory.database(),
            NODE,
            ClusterEpoch(42),
            Limits::default()
        )
        .await
        .is_err()
    );
    assert!(
        Singleton::open(directory.database(), NODE, EPOCH, Limits::default())
            .await
            .is_err()
    );
    let empty = directory.0.join("empty.redb");
    std::fs::write(&empty, []).unwrap();
    assert!(
        Journal::open(empty, NODE, EPOCH, Limits::default())
            .await
            .is_err()
    );
}

#[nativelink_test]
async fn unknown_persisted_metadata_section_is_not_ignored_on_restart() {
    let directory = Directory::new();
    let journal = open(&directory.database()).await;
    drop(journal);
    let database = redb::Database::open(directory.database()).unwrap();
    let tx = database.begin_write().unwrap();
    {
        let mut table = tx
            .open_table(redb::TableDefinition::<&str, &[u8]>::new("authority_meta"))
            .unwrap();
        table
            .insert("future_retention_section", b"{}".as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    drop(database);
    assert!(
        Journal::open(directory.database(), NODE, EPOCH, Limits::default())
            .await
            .is_err()
    );
}

#[derive(Clone, Copy, Debug)]
struct Builder;

impl StoreBuilder<TypeConfig, Journal, Journal, Directory> for Builder {
    async fn build(&self) -> Result<(Directory, Journal, Journal), StorageError<u64>> {
        let directory = Directory::new();
        let journal = Journal::open(directory.database(), NODE, EPOCH, Limits::default()).await?;
        Ok((directory, journal.clone(), journal))
    }
}

#[test]
fn pinned_openraft_split_storage_suite() {
    Suite::test_all(Builder).unwrap();
}

/// Runs only in a subprocess launched by the process-loss test below. Killing
/// this process bypasses Raft shutdown and all redb/Rust destructors.
#[nativelink_test]
async fn abrupt_crash_child() {
    let Ok(path) = std::env::var("NATIVELINK_JOURNAL_CRASH_PATH") else {
        return;
    };
    let node = Singleton::open(path, NODE, EPOCH, Limits::default())
        .await
        .unwrap();
    node.write_batch(Batch::new([event(1), event(2), event(3)]).unwrap())
        .await
        .unwrap();
    std::io::stdout()
        .write_all(b"JOURNAL_READY_FOR_PROCESS_KILL\n")
        .unwrap();
    std::io::stdout().flush().unwrap();
    std::future::pending::<()>().await;
}

#[nativelink_test]
async fn process_loss_after_accepted_reply_preserves_state_and_effects() {
    let directory = Directory::new();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "abrupt_crash_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("NATIVELINK_JOURNAL_CRASH_PATH", directory.database())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let reader = spawn_blocking!("authority_process_ready", move || {
        std::io::BufReader::new(output)
            .lines()
            .map_while(Result::ok)
            .any(|line| line.contains("JOURNAL_READY_FOR_PROCESS_KILL"))
    });
    let ready = tokio::time::timeout(Duration::from_secs(20), reader).await;
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(
        ready.unwrap().unwrap(),
        "child failed before accepted reply"
    );
    assert!(!status.success());
    let node = Singleton::open(directory.database(), NODE, EPOCH, Limits::default())
        .await
        .unwrap();
    let state = node.read().await.unwrap();
    assert_eq!(state.view().operations.len(), 3);
    assert_eq!(state.view().effects.len(), 3);
    node.shutdown().await.unwrap();
}
