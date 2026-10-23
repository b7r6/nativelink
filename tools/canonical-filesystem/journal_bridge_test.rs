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

//! Manual Linux Bazel integration; standalone Cargo tests do not run this file.
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::{fs, io};

use nativelink_canonical_config::{Manifest, parse_canonical};
use nativelink_canonical_filesystem::{
    JOURNAL_FILE, JournalBinding as FilesystemBinding, JournalDirectory, OpenedJournal, Ownership,
};
use nativelink_macro::nativelink_test;
use nativelink_scheduler::authority::format::{
    AdmissionRegistry, AuthorityDomain, JournalProfile, ReaderCapabilities,
};
use nativelink_scheduler::authority::journal::{
    Journal, JournalBinding, JournalDatabase, Singleton,
};
use nativelink_scheduler::authority::{ClusterEpoch, Event, Limits, RecordedEvent};
use nativelink_util::background_spawn;
use openraft::Vote;
use openraft::storage::RaftLogStorage;
use rustix::fs::{FlockOperation, flock};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    root: PathBuf,
    manifest: Manifest,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "nativelink-journal-bridge-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let mut manifest = parse_canonical(include_bytes!(
            "../canonical-config/fixtures/canonical.json"
        ))
        .unwrap();
        let paths = ["journal", "cas", "sink", "worker"].map(|name| {
            let path = root.join(name);
            fs::create_dir(&path).unwrap();
            path.to_str().unwrap().to_owned()
        });
        manifest.authority.journal_root.clone_from(&paths[0]);
        manifest.cas.root.clone_from(&paths[1]);
        manifest.temporary_action_cache.root.clone_from(&paths[2]);
        manifest.worker.root.clone_from(&paths[3]);
        Self { root, manifest }
    }

    const fn binding(&self) -> JournalBinding {
        JournalBinding {
            node_id: self.manifest.authority.node_id,
            domain: AuthorityDomain {
                epoch: ClusterEpoch(self.manifest.authority.cluster_epoch),
                namespace: self.manifest.authority.namespace,
            },
        }
    }

    fn profile(&self) -> JournalProfile {
        let domain = self.binding().domain;
        JournalProfile {
            domain,
            incarnation: 1,
            reader: ReaderCapabilities::Base,
            admission: AdmissionRegistry::new(domain),
        }
    }

    fn directory(&self, owner: &Arc<Ownership>) -> JournalDirectory {
        let binding = self.binding();
        JournalDirectory::new(
            owner,
            FilesystemBinding {
                node_id: binding.node_id,
                epoch: binding.domain.epoch.0,
                namespace: binding.domain.namespace,
            },
        )
        .unwrap()
    }

    fn file(&self) -> PathBuf {
        Path::new(&self.manifest.authority.journal_root).join(JOURNAL_FILE)
    }

    fn assert_all_locked(&self) {
        for root in [
            &self.manifest.authority.journal_root,
            &self.manifest.cas.root,
            &self.manifest.temporary_action_cache.root,
            &self.manifest.worker.root,
        ] {
            let file = fs::File::open(root).unwrap();
            assert_eq!(
                flock(&file, FlockOperation::NonBlockingLockExclusive),
                Err(rustix::io::Errno::WOULDBLOCK)
            );
        }
    }

    async fn released_owner(&self) -> Arc<Ownership> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(owner) = Ownership::reopen(&self.manifest) {
                    return Arc::new(owner);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

fn carrier(opened: OpenedJournal) -> JournalDatabase {
    let (database, parent) = opened.into_parts();
    match parent {
        Some(parent) => JournalDatabase::created(database, move || parent.sync_all()),
        None => JournalDatabase::existing(database),
    }
}

async fn open(fixture: &Fixture, directory: JournalDirectory, create: bool) -> Journal {
    Journal::open_from_factory(
        fixture.binding(),
        fixture.binding().node_id,
        Limits::default(),
        fixture.profile(),
        move || {
            if create {
                directory.create()
            } else {
                directory.reopen()
            }
            .map(carrier)
        },
    )
    .await
    .unwrap()
}

#[nativelink_test]
async fn real_journal_round_trip_retains_all_locks_after_parent_sync() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let directory = fixture.directory(&owner);
    drop(owner);
    let mut journal = open(&fixture, directory, true).await;
    fixture.assert_all_locked();
    let before = journal.read_applied().await.unwrap();
    assert!(before.0.is_none());
    let vote = Vote::new_committed(4, fixture.binding().node_id);
    journal.save_vote(&vote).await.unwrap();
    let clone = journal.clone();
    drop(journal);
    fixture.assert_all_locked();
    drop(clone);
    let owner = fixture.released_owner().await;
    let mut journal = open(&fixture, fixture.directory(&owner), false).await;
    assert_eq!(journal.read_vote().await.unwrap(), Some(vote));
    assert_eq!(
        journal.read_applied().await.unwrap().1.snapshot().unwrap(),
        before.1.snapshot().unwrap()
    );
}

#[nativelink_test]
async fn missing_and_empty_reopen_cannot_bootstrap() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    for existing in [false, true] {
        if existing {
            fs::write(fixture.file(), []).unwrap();
        }
        let directory = fixture.directory(&owner);
        assert!(
            Journal::open_from_factory(
                fixture.binding(),
                fixture.binding().node_id,
                Limits::default(),
                fixture.profile(),
                move || directory.reopen().map(carrier),
            )
            .await
            .is_err()
        );
        if existing {
            assert_eq!(fs::metadata(fixture.file()).unwrap().len(), 0);
        } else {
            assert!(!fixture.file().exists());
        }
    }
}

#[nativelink_test]
async fn canceled_constructor_retains_custody_before_file_open() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let directory = fixture.directory(&owner);
    drop(owner);
    let binding = fixture.binding();
    let profile = fixture.profile();
    let (started, reached) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let task = background_spawn!("journal_bridge_cancel_open", async move {
        Journal::open_from_factory(
            binding,
            binding.node_id,
            Limits::default(),
            profile,
            move || {
                started.send(()).unwrap();
                resume.recv().map_err(io::Error::other)?;
                directory.create().map(carrier)
            },
        )
        .await
    });
    reached.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!fixture.file().exists());
    fixture.assert_all_locked();
    release.send(()).unwrap();
    let owner = fixture.released_owner().await;
    let journal = open(&fixture, fixture.directory(&owner), false).await;
    assert!(journal.read_applied().await.unwrap().0.is_none());
}

#[nativelink_test]
async fn canceled_parent_sync_retains_database_and_every_role() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let directory = fixture.directory(&owner);
    drop(owner);
    let binding = fixture.binding();
    let profile = fixture.profile();
    let (started, reached) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let task = background_spawn!("journal_bridge_cancel_sync", async move {
        Journal::open_from_factory(
            binding,
            binding.node_id,
            Limits::default(),
            profile,
            move || {
                let (database, parent) = directory.create()?.into_parts();
                Ok(JournalDatabase::created(database, move || {
                    started.send(()).unwrap();
                    resume.recv().map_err(io::Error::other)?;
                    parent.unwrap().sync_all()
                }))
            },
        )
        .await
    });
    reached.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    fixture.assert_all_locked();
    assert!(redb::Database::open(fixture.file()).is_err());
    release.send(()).unwrap();
    let owner = fixture.released_owner().await;
    let journal = open(&fixture, fixture.directory(&owner), false).await;
    assert!(journal.read_applied().await.unwrap().0.is_none());
}

#[nativelink_test]
async fn failed_parent_sync_leaves_initialized_artifact_and_releases_custody() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let directory = fixture.directory(&owner);
    drop(owner);
    assert!(
        Journal::open_from_factory(
            fixture.binding(),
            fixture.binding().node_id,
            Limits::default(),
            fixture.profile(),
            move || {
                let (database, parent) = directory.create()?.into_parts();
                Ok(JournalDatabase::created(database, move || {
                    drop(parent);
                    Err(io::Error::from_raw_os_error(5))
                }))
            },
        )
        .await
        .is_err()
    );
    assert!(fs::metadata(fixture.file()).unwrap().len() > 0);
    let owner = fixture.released_owner().await;
    let journal = open(&fixture, fixture.directory(&owner), false).await;
    assert!(journal.read_applied().await.unwrap().0.is_none());
}

#[nativelink_test]
async fn foreign_persisted_identity_is_rejected_by_common_validation() {
    for mismatch in 0..3 {
        let fixture = Fixture::new();
        let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
        let mut profile = fixture.profile();
        let mut node = fixture.binding().node_id;
        match mismatch {
            0 => node += 1,
            1 => profile.domain.epoch.0 += 1,
            2 => profile.domain.namespace += 1,
            _ => unreachable!(),
        }
        profile.admission = AdmissionRegistry::new(profile.domain);
        drop(
            Journal::open_with_profile(fixture.file(), node, Limits::default(), profile.clone())
                .await
                .unwrap(),
        );
        let directory = fixture.directory(&owner);
        assert!(
            Journal::open_from_factory(
                fixture.binding(),
                fixture.binding().node_id,
                Limits::default(),
                fixture.profile(),
                move || directory.reopen().map(carrier),
            )
            .await
            .is_err()
        );
        // Refusal did not initialize over the foreign authority metadata.
        Journal::open_with_profile(
            fixture.file(),
            node,
            Limits::default(),
            JournalProfile {
                admission: AdmissionRegistry::new(profile.domain),
                ..profile
            },
        )
        .await
        .unwrap();
    }
}

#[nativelink_test]
async fn replaced_ambient_root_cannot_redirect_initialization_or_sync() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let directory = fixture.directory(&owner);
    let original = fixture.root.join("detached");
    fs::rename(&fixture.manifest.authority.journal_root, &original).unwrap();
    fs::create_dir(&fixture.manifest.authority.journal_root).unwrap();
    fs::write(fixture.file(), b"replacement untouched").unwrap();
    let journal = open(&fixture, directory, true).await;
    assert!(journal.read_applied().await.unwrap().0.is_none());
    assert_eq!(fs::read(fixture.file()).unwrap(), b"replacement untouched");
    assert!(original.join(JOURNAL_FILE).is_file());
}

async fn open_singleton(fixture: &Fixture, directory: JournalDirectory, create: bool) -> Singleton {
    Singleton::open_from_factory(
        fixture.binding(),
        fixture.binding().node_id,
        Limits::default(),
        fixture.profile(),
        move || {
            if create {
                directory.create()
            } else {
                directory.reopen()
            }
            .map(carrier)
        },
    )
    .await
    .unwrap()
}

#[nativelink_test]
async fn singleton_commits_and_reopens_through_renamed_root_with_all_locks() {
    let mut fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let directory = fixture.directory(&owner);
    drop(owner);
    let ambient = fixture.file();
    let original = fixture.root.join("detached");
    fs::rename(&fixture.manifest.authority.journal_root, &original).unwrap();
    fs::create_dir(&fixture.manifest.authority.journal_root).unwrap();
    fs::write(&ambient, b"replacement untouched").unwrap();
    fixture.manifest.authority.journal_root = original.to_str().unwrap().to_owned();

    let singleton = open_singleton(&fixture, directory, true).await;
    fixture.assert_all_locked();
    singleton
        .write(RecordedEvent {
            epoch: fixture.binding().domain.epoch,
            event: Event::AdvanceTime { now: 17 },
        })
        .await
        .unwrap();
    assert_eq!(singleton.read().await.unwrap().view().now, 17);
    assert_eq!(fs::read(&ambient).unwrap(), b"replacement untouched");
    singleton.shutdown().await.unwrap();

    let owner = fixture.released_owner().await;
    let directory = fixture.directory(&owner);
    drop(owner);
    let singleton = open_singleton(&fixture, directory, false).await;
    fixture.assert_all_locked();
    assert_eq!(singleton.read().await.unwrap().view().now, 17);
    assert_eq!(fs::read(&ambient).unwrap(), b"replacement untouched");
    singleton.shutdown().await.unwrap();
    drop(fixture.released_owner().await);
}

async fn cancel_singleton_creation(hold_parent_sync: bool) {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let directory = fixture.directory(&owner);
    drop(owner);
    let binding = fixture.binding();
    let profile = fixture.profile();
    let (started, reached) = tokio::sync::oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let task = background_spawn!("singleton_bridge_cancel_creation", async move {
        Singleton::open_from_factory(
            binding,
            binding.node_id,
            Limits::default(),
            profile,
            move || {
                if hold_parent_sync {
                    let (database, parent) = directory.create()?.into_parts();
                    Ok(JournalDatabase::created(database, move || {
                        started.send(()).unwrap();
                        resume.recv().map_err(io::Error::other)?;
                        parent.unwrap().sync_all()
                    }))
                } else {
                    started.send(()).unwrap();
                    resume.recv().map_err(io::Error::other)?;
                    directory.create().map(carrier)
                }
            },
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(10), reached)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
    fixture.assert_all_locked();
    if hold_parent_sync {
        assert!(redb::Database::open(fixture.file()).is_err());
    } else {
        assert!(!fixture.file().exists());
    }
    release.send(()).unwrap();
    let owner = fixture.released_owner().await;
    let directory = fixture.directory(&owner);
    drop(owner);
    // Owned preparation must finish membership initialization and shut Raft
    // down. An orphan core retains locks; canceled initialization cannot reopen
    // as an initialized Singleton through the existing strict reopen path.
    let singleton = open_singleton(&fixture, directory, false).await;
    fixture.assert_all_locked();
    assert_eq!(singleton.read().await.unwrap().view().now, 0);
    singleton.shutdown().await.unwrap();
    drop(fixture.released_owner().await);
}

#[nativelink_test]
async fn singleton_cancellation_before_file_open_retains_all_custody() {
    cancel_singleton_creation(false).await;
}

#[nativelink_test]
async fn singleton_cancellation_during_parent_sync_retains_all_custody() {
    cancel_singleton_creation(true).await;
}

#[nativelink_test]
async fn singleton_existing_uninitialized_membership_is_not_adopted() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let directory = fixture.directory(&owner);
    drop(owner);
    drop(open(&fixture, directory, true).await);
    let owner = fixture.released_owner().await;
    let directory = fixture.directory(&owner);
    drop(owner);
    let error = Singleton::open_from_factory(
        fixture.binding(),
        fixture.binding().node_id,
        Limits::default(),
        fixture.profile(),
        move || directory.reopen().map(carrier),
    )
    .await
    .unwrap_err();
    assert!(error.contains("existing journal lacks initialized membership"));
    let owner = fixture.released_owner().await;
    let journal = open(&fixture, fixture.directory(&owner), false).await;
    assert!(journal.read_applied().await.unwrap().0.is_none());
}

#[nativelink_test]
async fn persisted_limit_refusal_releases_custody_without_replacing_state() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let directory = fixture.directory(&owner);
    drop(owner);
    let mut journal = open(&fixture, directory, true).await;
    let vote = Vote::new_committed(4, fixture.binding().node_id);
    journal.save_vote(&vote).await.unwrap();
    let state = journal.read_applied().await.unwrap();
    drop(journal);
    let owner = fixture.released_owner().await;
    let directory = fixture.directory(&owner);
    drop(owner);
    let profile = fixture.profile();
    let result = Journal::open_from_factory(
        fixture.binding(),
        fixture.binding().node_id,
        Limits {
            max_workers: Limits::default().max_workers - 1,
            ..Limits::default()
        },
        profile.clone(),
        move || directory.reopen().map(carrier),
    )
    .await;
    assert!(result.is_err(), "descriptor reopen accepted changed limits");
    // Refusal releases the database's four ownership locks. Reusing the exact
    // registry/incarnation also shows it was refused before registration.
    let owner = fixture.released_owner().await;
    let directory = fixture.directory(&owner);
    drop(owner);
    let mut journal = Journal::open_from_factory(
        fixture.binding(),
        fixture.binding().node_id,
        Limits::default(),
        profile,
        move || directory.reopen().map(carrier),
    )
    .await
    .unwrap();
    fixture.assert_all_locked();
    assert_eq!(journal.read_vote().await.unwrap(), Some(vote));
    assert_eq!(journal.read_applied().await.unwrap(), state);
}
