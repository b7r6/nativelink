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

use nativelink_macro::nativelink_test;
use nativelink_util::background_spawn;

use super::*;
use crate::authority::Event;

const DOMAIN: AuthorityDomain = AuthorityDomain {
    epoch: ClusterEpoch(93),
    namespace: 94,
};
const NODE: u64 = 95;

fn profile() -> JournalProfile {
    JournalProfile {
        domain: DOMAIN,
        incarnation: 1,
        reader: ReaderCapabilities::Base,
        admission: AdmissionRegistry::new(DOMAIN),
    }
}

#[nativelink_test]
async fn factory_binding_and_limits_are_checked_before_invocation() {
    let binding = JournalBinding {
        node_id: NODE,
        domain: DOMAIN,
    };
    for mismatch in [
        JournalBinding {
            node_id: NODE + 1,
            ..binding
        },
        JournalBinding {
            domain: AuthorityDomain {
                epoch: ClusterEpoch(0),
                ..DOMAIN
            },
            ..binding
        },
        JournalBinding {
            domain: AuthorityDomain {
                namespace: 0,
                ..DOMAIN
            },
            ..binding
        },
    ] {
        assert!(
            Journal::open_from_factory(mismatch, NODE, Limits::default(), profile(), || {
                panic!("binding refusal must precede factory invocation")
            })
            .await
            .is_err()
        );
    }
    assert!(
        Journal::open_from_factory(
            binding,
            NODE,
            Limits {
                max_payload_bytes: usize::MAX,
                ..Limits::default()
            },
            profile(),
            || panic!("limit refusal must precede factory invocation"),
        )
        .await
        .is_err()
    );
}

#[nativelink_test]
async fn existing_factory_cannot_adopt_a_pristine_database() {
    assert!(
        Journal::open_from_factory(
            JournalBinding {
                node_id: NODE,
                domain: DOMAIN
            },
            NODE,
            Limits::default(),
            profile(),
            || {
                Database::builder()
                    .create_with_backend(redb::backends::InMemoryBackend::new())
                    .map(JournalDatabase::existing)
                    .map_err(io::Error::other)
            },
        )
        .await
        .is_err()
    );
}

#[nativelink_test]
async fn canceled_blocking_io_keeps_the_database_and_queue_until_completion() {
    let journal = Journal::fresh_volatile(NODE, DOMAIN.epoch, Limits::default())
        .await
        .unwrap();
    let database = Arc::downgrade(&journal.database);
    let queue = Arc::downgrade(&journal.queue);
    let (started, reached) = oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let task = background_spawn!("journal_canceled_io_fixture", async move {
        journal
            .io(MeasuredOperation::ReadApplied, 0, move |database| {
                started.send(()).unwrap();
                resume.recv().unwrap();
                let transaction = database.begin_read().unwrap();
                assert!(
                    transaction
                        .open_table(META)
                        .unwrap()
                        .get("applied")
                        .unwrap()
                        .is_some()
                );
                Ok(())
            })
            .await
    });
    reached.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(database.upgrade().is_some());
    assert!(queue.upgrade().unwrap().try_lock().is_err());
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while database.upgrade().is_some() || queue.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

fn memory_database() -> io::Result<JournalDatabase> {
    Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .map(|database| JournalDatabase::created(database, || Ok(())))
        .map_err(io::Error::other)
}

async fn await_shutdown(raft: &Raft<TypeConfig>) {
    let mut metrics = raft.metrics();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if metrics.borrow().state == openraft::ServerState::Shutdown {
                break;
            }
            metrics.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[nativelink_test]
async fn singleton_binding_and_limits_refuse_before_factory() {
    let invoked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let binding = JournalBinding {
        node_id: NODE,
        domain: DOMAIN,
    };
    for mismatch in [
        JournalBinding {
            node_id: NODE + 1,
            ..binding
        },
        JournalBinding {
            domain: AuthorityDomain {
                epoch: ClusterEpoch(0),
                ..DOMAIN
            },
            ..binding
        },
        JournalBinding {
            domain: AuthorityDomain {
                namespace: 0,
                ..DOMAIN
            },
            ..binding
        },
    ] {
        let calls = invoked.clone();
        assert!(
            Singleton::open_from_factory(mismatch, NODE, Limits::default(), profile(), move || {
                calls.fetch_add(1, Ordering::Relaxed);
                Err(io::Error::other("factory must not be invoked"))
            },)
            .await
            .is_err()
        );
        assert_eq!(invoked.load(Ordering::Relaxed), 0);
    }
    let calls = invoked.clone();
    assert!(
        Singleton::open_from_factory(
            binding,
            NODE,
            Limits {
                max_payload_bytes: usize::MAX,
                ..Limits::default()
            },
            profile(),
            move || {
                calls.fetch_add(1, Ordering::Relaxed);
                Err(io::Error::other("factory must not be invoked"))
            },
        )
        .await
        .is_err()
    );
    assert_eq!(invoked.load(Ordering::Relaxed), 0);
}

#[nativelink_test]
async fn canceled_singleton_startup_explicitly_stops_raft_after_gate() {
    let (reached, observer) = oneshot::channel();
    let (release, resume) = oneshot::channel();
    let (prepared, accepted) = Singleton::prepare_from_factory(
        JournalBinding {
            node_id: NODE,
            domain: DOMAIN,
        },
        NODE,
        Limits::default(),
        profile(),
        memory_database,
        Some(SingletonStartupGate { reached, resume }),
    );
    let raft = tokio::time::timeout(Duration::from_secs(10), observer)
        .await
        .unwrap()
        .unwrap();
    drop(prepared);
    drop(accepted);
    // Holding another Raft handle makes this an explicit shutdown witness:
    // dropping the returned Singleton alone cannot stop this core.
    assert_ne!(
        raft.metrics().borrow().state,
        openraft::ServerState::Shutdown
    );
    release.send(()).unwrap();
    await_shutdown(&raft).await;
}

#[nativelink_test]
async fn unacknowledged_singleton_handoff_explicitly_stops_raft() {
    let (prepared, accepted) = Singleton::prepare_from_factory(
        JournalBinding {
            node_id: NODE,
            domain: DOMAIN,
        },
        NODE,
        Limits::default(),
        profile(),
        memory_database,
        None,
    );
    // Receive while deliberately withholding the acknowledgement. The public
    // constructor has no await in this interval. This tests the retained
    // owner's responsibility even when the result still holds a Raft handle.
    let singleton = tokio::time::timeout(Duration::from_secs(10), prepared)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_ne!(
        singleton.raft.metrics().borrow().state,
        openraft::ServerState::Shutdown
    );
    drop(accepted);
    await_shutdown(&singleton.raft).await;
}

#[nativelink_test]
async fn accepted_singleton_handoff_keeps_the_authority_live() {
    let singleton = Singleton::open_from_factory(
        JournalBinding {
            node_id: NODE,
            domain: DOMAIN,
        },
        NODE,
        Limits::default(),
        profile(),
        memory_database,
    )
    .await
    .unwrap();
    singleton
        .write(RecordedEvent {
            epoch: DOMAIN.epoch,
            event: Event::AdvanceTime { now: 17 },
        })
        .await
        .unwrap();
    assert_eq!(singleton.read().await.unwrap().view().now, 17);
    singleton.shutdown().await.unwrap();
}

struct LimitsFixture {
    root: PathBuf,
}

impl LimitsFixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "authority-persisted-limits-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&root).unwrap();
        Self { root }
    }

    fn path(&self) -> PathBuf {
        self.root.join("authority.redb")
    }

    async fn reopen(
        &self,
        limits: Limits,
        profile: JournalProfile,
        factory: bool,
    ) -> Result<Journal> {
        if factory {
            let path = self.path();
            Journal::open_from_factory(
                JournalBinding {
                    node_id: NODE,
                    domain: DOMAIN,
                },
                NODE,
                limits,
                profile,
                move || {
                    Database::open(path)
                        .map(JournalDatabase::existing)
                        .map_err(io::Error::other)
                },
            )
            .await
        } else {
            Journal::open_with_profile(self.path(), NODE, limits, profile).await
        }
    }
}

impl Drop for LimitsFixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

fn different_limits() -> Vec<(&'static str, Limits)> {
    let mut cases = Vec::new();
    for increase in [false, true] {
        for field in ["executions", "operations", "workers", "effects", "payload"] {
            let mut limits = Limits::default();
            let value = match field {
                "executions" => &mut limits.max_executions,
                "operations" => &mut limits.max_operations,
                "workers" => &mut limits.max_workers,
                "effects" => &mut limits.max_effects,
                "payload" => &mut limits.max_payload_bytes,
                _ => unreachable!(),
            };
            if increase {
                *value += 1;
            } else {
                *value -= 1;
            }
            State::new(DOMAIN.epoch, limits).unwrap();
            cases.push((field, limits));
        }
    }
    cases
}

#[derive(Debug, Eq, PartialEq)]
struct LogicalJournalContents {
    metadata: Vec<(String, Vec<u8>)>,
    logs: Vec<(u64, Vec<u8>)>,
}

fn logical_contents(database: &Database) -> LogicalJournalContents {
    let transaction = database.begin_read().unwrap();
    let metadata = transaction
        .open_table(META)
        .unwrap()
        .iter()
        .unwrap()
        .map(|record| {
            let (key, value) = record.unwrap();
            (key.value().to_owned(), value.value().to_vec())
        })
        .collect();
    let logs = transaction
        .open_table(LOGS)
        .unwrap()
        .iter()
        .unwrap()
        .map(|record| {
            let (key, value) = record.unwrap();
            (key.value(), value.value().to_vec())
        })
        .collect();
    LogicalJournalContents { metadata, logs }
}

#[nativelink_test]
async fn ambient_reopen_refuses_every_changed_persisted_limit() {
    let fixture = LimitsFixture::new();
    let mut journal = Journal::open(fixture.path(), NODE, DOMAIN.epoch, Limits::default())
        .await
        .unwrap();
    journal.build_snapshot().await.unwrap();
    let before = logical_contents(&journal.database);
    drop(journal);
    for (field, changed) in different_limits() {
        let result = Journal::open(fixture.path(), NODE, DOMAIN.epoch, changed).await;
        assert!(result.is_err(), "accepted changed {field}: {changed:?}");
        let journal = Journal::open(fixture.path(), NODE, DOMAIN.epoch, Limits::default())
            .await
            .unwrap();
        assert_eq!(logical_contents(&journal.database), before);
        assert_eq!(
            journal.read_applied().await.unwrap().1.limits,
            Limits::default()
        );
    }
}

#[nativelink_test]
async fn factory_limit_refusal_precedes_registration_and_preserves_contents() {
    let fixture = LimitsFixture::new();
    let mut journal = fixture
        .reopen(Limits::default(), profile(), false)
        .await
        .unwrap();
    journal.build_snapshot().await.unwrap();
    let before = logical_contents(&journal.database);
    drop(journal);
    for (field, changed) in different_limits() {
        let profile = profile();
        let result = fixture.reopen(changed, profile.clone(), true).await;
        assert!(result.is_err(), "accepted changed {field}: {changed:?}");
        // The exact same registry and incarnation must remain usable. Merely
        // dropping a registered lease does not undo its used incarnation.
        let journal = fixture
            .reopen(Limits::default(), profile, true)
            .await
            .unwrap();
        assert_eq!(logical_contents(&journal.database), before);
        assert_eq!(
            journal.read_applied().await.unwrap().1.limits,
            Limits::default()
        );
    }
}

#[nativelink_test]
async fn reopen_refuses_limits_hidden_only_in_saved_snapshot() {
    let fixture = LimitsFixture::new();
    let mut journal = fixture
        .reopen(Limits::default(), profile(), false)
        .await
        .unwrap();
    journal.build_snapshot().await.unwrap();
    drop(journal);
    for factory in [false, true] {
        for (field, changed) in different_limits() {
            let database = Database::open(fixture.path()).unwrap();
            let original = {
                let transaction = database.begin_read().unwrap();
                let table = transaction.open_table(META).unwrap();
                table.get("snapshot").unwrap().unwrap().value().to_vec()
            };
            let mut saved: SavedSnapshot = decode(DOMAIN.epoch, &original).unwrap();
            saved.applied.scheduler = SchedulerSection::from_state(
                &State::new(DOMAIN.epoch, changed).unwrap(),
                &saved.applied.format,
            )
            .unwrap();
            // This is a valid snapshot and envelope. Only its Limits disagree
            // with the caller and the unchanged applied state.
            saved
                .validate(DOMAIN.epoch, ReaderCapabilities::Base)
                .unwrap();
            let bytes = encode(DOMAIN.epoch, &saved).unwrap();
            let transaction = database.begin_write().unwrap();
            transaction
                .open_table(META)
                .unwrap()
                .insert("snapshot", bytes.as_slice())
                .unwrap();
            transaction.commit().unwrap();
            let before = logical_contents(&database);
            drop(database);
            let profile = profile();
            let result = fixture
                .reopen(Limits::default(), profile.clone(), factory)
                .await;
            assert!(
                result.is_err(),
                "accepted snapshot {field}: {changed:?}; factory={factory}"
            );
            let database = Database::open(fixture.path()).unwrap();
            assert_eq!(logical_contents(&database), before);
            let transaction = database.begin_write().unwrap();
            transaction
                .open_table(META)
                .unwrap()
                .insert("snapshot", original.as_slice())
                .unwrap();
            transaction.commit().unwrap();
            drop(database);
            // Restore only the fixture discrepancy, then reuse the refused
            // profile. Both matching persisted states must reopen normally.
            fixture
                .reopen(Limits::default(), profile, factory)
                .await
                .unwrap();
        }
    }
}

#[nativelink_test]
async fn singleton_reopen_refuses_changed_limits_before_startup() {
    let fixture = LimitsFixture::new();
    let singleton =
        Singleton::open_with_profile(fixture.path(), NODE, Limits::default(), profile())
            .await
            .unwrap();
    singleton
        .write(RecordedEvent {
            epoch: DOMAIN.epoch,
            event: Event::AdvanceTime { now: 17 },
        })
        .await
        .unwrap();
    singleton.shutdown().await.unwrap();
    let database = Database::open(fixture.path()).unwrap();
    let before = logical_contents(&database);
    drop(database);
    let profile = profile();
    let path = fixture.path();
    let result = Singleton::open_from_factory(
        JournalBinding {
            node_id: NODE,
            domain: DOMAIN,
        },
        NODE,
        Limits {
            max_payload_bytes: Limits::default().max_payload_bytes + 1,
            ..Limits::default()
        },
        profile.clone(),
        move || {
            Database::open(path)
                .map(JournalDatabase::existing)
                .map_err(io::Error::other)
        },
    )
    .await;
    if let Ok(singleton) = result {
        singleton.shutdown().await.unwrap();
        panic!("singleton accepted changed persisted limits");
    }
    let journal = fixture
        .reopen(Limits::default(), profile, true)
        .await
        .unwrap();
    assert_eq!(logical_contents(&journal.database), before);
    assert_eq!(journal.read_applied().await.unwrap().1.view().now, 17);
}
