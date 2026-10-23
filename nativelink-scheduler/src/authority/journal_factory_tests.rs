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
