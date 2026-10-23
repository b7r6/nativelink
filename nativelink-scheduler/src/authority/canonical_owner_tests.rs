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

//! Closed journal preparation fixtures; sealed histories here are synthetic.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use nativelink_canonical_config::{Manifest, parse_canonical};
use nativelink_canonical_filesystem::{JOURNAL_FILE, JournalBinding, JournalDirectory, Ownership};
use nativelink_macro::nativelink_test;
use nativelink_util::background_spawn;
use redb::ReadableDatabase;
use rustix::fs::{FlockOperation, flock};

use super::super::{AppliedState, RecordedEvent, Singleton, decode, encode};
use super::*;
use crate::authority::Event;

#[path = "canonical_custody_tests.rs"]
mod custody;
#[path = "canonical_temporal_tests.rs"]
mod temporal;

static NEXT: AtomicU64 = AtomicU64::new(0);

type BeforeCurrent = Box<dyn FnOnce(&Database) + Send>;

#[derive(Default)]
pub(super) struct Hooks {
    pub after_open: Option<Box<dyn FnOnce() + Send>>,
    pub during_temporal: Option<Box<dyn FnOnce() + Send>>,
    pub before_current: Option<BeforeCurrent>,
    pub after_commit: Option<Box<dyn FnOnce() + Send>>,
    pub after_current: Option<Box<dyn FnOnce() + Send>>,
}

struct Fixture {
    root: PathBuf,
    manifest: Manifest,
}

impl Fixture {
    fn new() -> Self {
        Self::at(
            &std::env::temp_dir().join(format!(
                "nativelink-canonical-owner-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )),
            true,
        )
    }

    fn at(root: &Path, fresh: bool) -> Self {
        if fresh {
            fs::create_dir(root).unwrap();
        }
        let root = root.canonicalize().unwrap();
        let mut manifest = parse_canonical(include_bytes!(
            "../../../tools/canonical-config/fixtures/canonical.json"
        ))
        .unwrap();
        let paths = ["journal", "cas", "sink", "worker"].map(|name| {
            let path = root.join(name);
            if fresh {
                fs::create_dir(&path).unwrap();
            }
            path.to_str().unwrap().to_owned()
        });
        manifest.authority.journal_root.clone_from(&paths[0]);
        manifest.cas.root.clone_from(&paths[1]);
        manifest.temporary_action_cache.root.clone_from(&paths[2]);
        manifest.worker.root.clone_from(&paths[3]);
        Self { root, manifest }
    }

    fn owned(&self, create: bool) -> Arc<Ownership> {
        Arc::new(
            if create {
                Ownership::claim_cold(&self.manifest)
            } else {
                Ownership::reopen(&self.manifest)
            }
            .unwrap(),
        )
    }

    fn token(&self, create: bool) -> SyncedNamespace {
        self.owned(create).resynchronize().unwrap()
    }

    fn file(&self) -> PathBuf {
        Path::new(&self.manifest.authority.journal_root).join(JOURNAL_FILE)
    }

    fn directory(&self, owned: &Arc<Ownership>) -> JournalDirectory {
        JournalDirectory::new(
            owned,
            JournalBinding {
                node_id: self.manifest.authority.node_id,
                epoch: self.manifest.authority.cluster_epoch,
                namespace: self.manifest.authority.namespace,
            },
        )
        .unwrap()
    }

    fn locked(&self) {
        for root in [
            &self.manifest.authority.journal_root,
            &self.manifest.cas.root,
            &self.manifest.temporary_action_cache.root,
            &self.manifest.worker.root,
        ] {
            let file = fs::File::open(root).unwrap();
            assert_eq!(
                flock(&file, FlockOperation::NonBlockingLockExclusive),
                Err(rustix::io::Errno::WOULDBLOCK),
                "{root}"
            );
        }
    }

    async fn released(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if Ownership::reopen(&self.manifest).is_ok() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    fn edit(&self, change: impl FnOnce(&mut redb::Table<&str, &[u8]>)) {
        let owned = self.owned(false);
        let (database, _) = self.directory(&owned).reopen().unwrap().into_parts();
        edit(&database, change);
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

fn edit(database: &Database, change: impl FnOnce(&mut redb::Table<&str, &[u8]>)) {
    let transaction = database.begin_write().unwrap();
    {
        change(&mut transaction.open_table(META).unwrap());
    }
    transaction.commit().unwrap();
}

fn applied(database: &Database, epoch: ClusterEpoch) -> AppliedState {
    let transaction = database.begin_read().unwrap();
    let table = transaction.open_table(META).unwrap();
    decode(epoch, table.get("applied").unwrap().unwrap().value()).unwrap()
}

async fn sealed(fixture: &Fixture) {
    let owner = PreparedCanonicalOwner::prepare_cold(fixture.token(true))
        .await
        .unwrap();
    // Tests alone bypass the closed API to construct initialized singleton history.
    let authority = Singleton::from_journal(owner.journal, None).await.unwrap();
    authority
        .write(RecordedEvent {
            epoch: ClusterEpoch(fixture.manifest.authority.cluster_epoch),
            event: Event::AdvanceTime { now: 17 },
        })
        .await
        .unwrap();
    edit(&authority.journal.database, |table| {
        let bytes = encode(
            ClusterEpoch(fixture.manifest.authority.cluster_epoch),
            &CanonicalBootstrap::Sealed,
        )
        .unwrap();
        table
            .insert("canonical_bootstrap", bytes.as_slice())
            .unwrap();
    });
    authority.shutdown().await.unwrap();
    fixture.released().await;
}

#[nativelink_test]
async fn cold_derives_exact_binding_and_fixed_limits_without_runtime() {
    let fixture = Fixture::new();
    let token = fixture.token(true);
    let commitment = token.commitment();
    let owner = PreparedCanonicalOwner::prepare_cold(token).await.unwrap();
    let diagnostic = owner.diagnostic();
    assert_eq!(diagnostic.node_id, fixture.manifest.authority.node_id);
    assert_eq!(diagnostic.epoch, fixture.manifest.authority.cluster_epoch);
    assert_eq!(diagnostic.namespace, fixture.manifest.authority.namespace);
    assert_ne!(diagnostic.namespace, 0);
    assert_eq!(diagnostic.namespace_commitment, commitment);
    assert_eq!(diagnostic.phase, OwnerPhase::Preparing);
    let stored = applied(&owner.journal.database, ClusterEpoch(diagnostic.epoch));
    assert_eq!(
        stored.canonical_contract.unwrap().namespace_commitment,
        commitment
    );
    assert_eq!(stored.format.domain.namespace, diagnostic.namespace);
    let (_, state) = owner.journal.read_applied().await.unwrap();
    assert_eq!(state.now, 0);
    assert_eq!(
        state.limits,
        Limits {
            max_executions: 64,
            max_operations: 256,
            max_workers: 32,
            max_effects: 512,
            max_payload_bytes: 65536
        }
    );
    assert!(stored.last_applied.is_none());
    assert!(stored.membership.log_id().is_none());
    fixture.locked();
    owner.release().await.unwrap();
    fixture.released().await;
    assert!(
        Journal::open(
            fixture.file(),
            diagnostic.node_id,
            ClusterEpoch(diagnostic.epoch),
            Limits::default()
        )
        .await
        .is_err()
    );
}

#[nativelink_test]
async fn preparing_is_preserved_and_never_adopted() {
    let fixture = Fixture::new();
    let owner = PreparedCanonicalOwner::prepare_cold(fixture.token(true))
        .await
        .unwrap();
    let expected = owner.diagnostic();
    owner.release().await.unwrap();
    assert!(
        PreparedCanonicalOwner::prepare_cold(fixture.token(false))
            .await
            .is_err()
    );
    assert!(
        PreparedCanonicalOwner::reopen_sealed(fixture.token(false))
            .await
            .is_err()
    );
    fixture.edit(|table| {
        let phase: CanonicalBootstrap = decode(
            ClusterEpoch(expected.epoch),
            table.get("canonical_bootstrap").unwrap().unwrap().value(),
        )
        .unwrap();
        assert_eq!(phase, CanonicalBootstrap::Preparing);
    });
}

#[nativelink_test]
async fn sealed_reopen_preserves_history_and_creation_provenance() {
    let fixture = Fixture::new();
    sealed(&fixture).await;
    let owner = PreparedCanonicalOwner::reopen_sealed(fixture.token(false))
        .await
        .unwrap();
    assert_eq!(owner.diagnostic().phase, OwnerPhase::Sealed);
    assert!(!owner.journal.created);
    let (position, state) = owner.journal.read_applied().await.unwrap();
    assert!(position.is_some());
    assert_eq!(state.now, 17);
    fixture.locked();
    owner.release().await.unwrap();
    fixture.released().await;
}

#[nativelink_test]
async fn sealed_reopen_refuses_partial_foreign_limits_and_membership() {
    for case in 0..6 {
        let fixture = Fixture::new();
        sealed(&fixture).await;
        let epoch = ClusterEpoch(fixture.manifest.authority.cluster_epoch);
        fixture.edit(|table| match case {
            0 => {
                table.remove("canonical_contract").unwrap();
            }
            1 => {
                table.remove("canonical_bootstrap").unwrap();
            }
            2 => {
                let mut contract: CanonicalContract = decode(
                    epoch,
                    table.get("canonical_contract").unwrap().unwrap().value(),
                )
                .unwrap();
                contract.namespace_commitment[0] ^= 1;
                let bytes = encode(epoch, &contract).unwrap();
                table
                    .insert("canonical_contract", bytes.as_slice())
                    .unwrap();
            }
            3 => {
                let bytes = encode(epoch, &CanonicalBootstrap::Preparing).unwrap();
                table
                    .insert("canonical_bootstrap", bytes.as_slice())
                    .unwrap();
            }
            _ => {
                let mut value: AppliedState =
                    decode(epoch, table.get("applied").unwrap().unwrap().value()).unwrap();
                if case == 4 {
                    let super::super::SchedulerSection::String(json) = &mut value.scheduler else {
                        panic!("base fixture")
                    };
                    let mut state = super::super::State::restore(json.as_bytes()).unwrap();
                    state.limits.max_operations += 1;
                    *json = String::from_utf8(state.snapshot().unwrap()).unwrap();
                } else {
                    value.membership = openraft::StoredMembership::default();
                }
                let bytes = encode(epoch, &value).unwrap();
                table.insert("applied", bytes.as_slice()).unwrap();
            }
        });
        assert!(
            PreparedCanonicalOwner::reopen_sealed(fixture.token(false))
                .await
                .is_err(),
            "case {case}"
        );
    }
}

#[nativelink_test]
async fn typed_reopen_refuses_legacy_snapshots_pending_membership_and_controls() {
    use super::super::{
        Batch, Entry, EntryPayload, FormatControl, LOGS, LogId, SavedSnapshot, SnapshotMeta,
    };
    for case in 0..6 {
        let fixture = Fixture::new();
        sealed(&fixture).await;
        let owned = fixture.owned(false);
        let (database, _) = fixture.directory(&owned).reopen().unwrap().into_parts();
        let epoch = ClusterEpoch(fixture.manifest.authority.cluster_epoch);
        let mut value = applied(&database, epoch);
        let transaction = database.begin_write().unwrap();
        {
            let mut table = transaction.open_table(META).unwrap();
            if case < 2 {
                if case == 0 {
                    value
                        .canonical_contract
                        .as_mut()
                        .unwrap()
                        .namespace_commitment[1] ^= 1;
                } else {
                    let super::super::SchedulerSection::String(json) = &mut value.scheduler else {
                        panic!("base fixture")
                    };
                    let mut state = super::super::State::restore(json.as_bytes()).unwrap();
                    state.limits.max_operations += 1;
                    *json = String::from_utf8(state.snapshot().unwrap()).unwrap();
                }
                let saved = SavedSnapshot {
                    meta: SnapshotMeta {
                        last_log_id: value.last_applied,
                        last_membership: value.membership.clone(),
                        snapshot_id: "synthetic-owner-snapshot".to_owned(),
                    },
                    applied: value,
                };
                let bytes = encode(epoch, &saved).unwrap();
                table.insert("snapshot", bytes.as_slice()).unwrap();
            } else if case == 2 || case == 3 {
                let mut logs = transaction.open_table(LOGS).unwrap();
                let index = logs.last().unwrap().unwrap().0.value() + 1;
                let payload: EntryPayload<super::super::TypeConfig> = if case == 2 {
                    EntryPayload::Membership(value.membership.membership().clone())
                } else {
                    EntryPayload::Normal(
                        Batch::format(FormatControl::Abort {
                            domain: value.format.domain,
                            permit: 123,
                        })
                        .unwrap(),
                    )
                };
                let entry = Entry {
                    log_id: LogId::new(value.last_applied.unwrap().leader_id, index),
                    payload,
                };
                let bytes = encode(epoch, &entry).unwrap();
                logs.insert(index, bytes.as_slice()).unwrap();
            } else {
                if case == 4 {
                    value.format.feature_epoch = 1;
                    value.format.required.insert(1);
                    value.format.activated_at = value.last_applied;
                } else {
                    value.canonical_contract = None;
                    table.remove("canonical_contract").unwrap();
                    table.remove("canonical_bootstrap").unwrap();
                }
                let bytes = encode(epoch, &value).unwrap();
                table.insert("applied", bytes.as_slice()).unwrap();
            }
        }
        transaction.commit().unwrap();
        drop(database);
        let token = owned.resynchronize().unwrap();
        assert!(
            PreparedCanonicalOwner::reopen_sealed(token).await.is_err(),
            "case {case}"
        );
    }
}

#[nativelink_test]
async fn changed_record_after_token_refuses_before_creating_journal() {
    let fixture = Fixture::new();
    let token = fixture.token(true);
    fs::write(
        Path::new(&fixture.manifest.worker.root)
            .join(nativelink_canonical_filesystem::OWNERSHIP_RECORD),
        b"{}",
    )
    .unwrap();
    assert!(PreparedCanonicalOwner::prepare_cold(token).await.is_err());
    assert!(!fixture.file().exists());
}

#[nativelink_test]
async fn attachment_replacement_before_and_after_current_sync_refuses() {
    for after_commit in [false, true] {
        let fixture = Fixture::new();
        let token = fixture.token(true);
        let original = PathBuf::from(&fixture.manifest.authority.journal_root);
        let moved = fixture.root.join("moved-journal");
        let replace = Box::new(move || {
            fs::rename(&original, &moved).unwrap();
            fs::create_dir(&original).unwrap();
        });
        let hooks = if after_commit {
            Hooks {
                after_commit: Some(replace),
                ..Hooks::default()
            }
        } else {
            Hooks {
                before_current: Some(Box::new(move |_| replace())),
                ..Hooks::default()
            }
        };
        assert!(
            PreparedCanonicalOwner::open(token, true, hooks)
                .await
                .is_err()
        );
        assert!(
            fixture
                .root
                .join("moved-journal")
                .join(JOURNAL_FILE)
                .exists()
        );
        assert!(!fixture.file().exists());
    }
}

#[nativelink_test]
async fn phase_change_before_current_barrier_refuses_without_sealing() {
    let fixture = Fixture::new();
    let token = fixture.token(true);
    let epoch = ClusterEpoch(fixture.manifest.authority.cluster_epoch);
    let hooks = Hooks {
        before_current: Some(Box::new(move |database| {
            edit(database, |table| {
                let bytes = encode(epoch, &CanonicalBootstrap::Sealed).unwrap();
                table
                    .insert("canonical_bootstrap", bytes.as_slice())
                    .unwrap();
            });
        })),
        ..Hooks::default()
    };
    assert!(
        PreparedCanonicalOwner::open(token, true, hooks)
            .await
            .is_err()
    );
}

#[nativelink_test]
async fn cancellation_retains_all_roots_until_current_work_finishes() {
    for after_open in [false, true] {
        let fixture = Fixture::new();
        let token = fixture.token(true);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let held = gate.clone();
        let (reached, wait) = tokio::sync::oneshot::channel();
        let pause: Box<dyn FnOnce() + Send> = Box::new(move || {
            reached.send(()).unwrap();
            let (lock, ready) = &*held;
            let guard = lock.lock().unwrap();
            drop(
                ready
                    .wait_timeout_while(guard, Duration::from_secs(10), |released| !*released)
                    .unwrap(),
            );
        });
        let hooks = if after_open {
            Hooks {
                after_open: Some(pause),
                ..Hooks::default()
            }
        } else {
            Hooks {
                before_current: Some(Box::new(move |_| pause())),
                ..Hooks::default()
            }
        };
        let task = background_spawn!(
            "canonical_owner_cancel_test",
            PreparedCanonicalOwner::open(token, true, hooks)
        );
        wait.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        fixture.locked();
        *gate.0.lock().unwrap() = true;
        gate.1.notify_one();
        fixture.released().await;
        assert!(
            PreparedCanonicalOwner::reopen_sealed(fixture.token(false))
                .await
                .is_err()
        );
    }
}

#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "The isolated syscall witness requires exactly one blocking thread for injection ordinals."
)]
#[ignore = "requires tools/test-canonical-owner-sync.py and an isolated subprocess"]
fn syscall_witness() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(syscall_witness_async());
}

async fn syscall_witness_async() {
    let mode = std::env::var("NL_OWNER_MODE").unwrap();
    let root = PathBuf::from(std::env::var("NL_OWNER_ROOT").unwrap());
    let fixture = Fixture::at(&root, matches!(mode.as_str(), "cold" | "seed"));
    if mode == "seed" {
        sealed(&fixture).await;
        eprintln!("NL_OWNER_SEEDED");
        std::mem::forget(fixture);
        return;
    }
    if mode == "crash" {
        let owner = PreparedCanonicalOwner::reopen_sealed(fixture.token(false))
            .await
            .unwrap();
        eprintln!("NL_OWNER_ABRUPT_EXIT");
        std::mem::forget(owner);
        std::process::exit(0);
    }
    let create = mode == "cold";
    // Namespace fsyncs and both owner tasks use the single blocking worker, so
    // per-tracee strace injection ordinals cannot hit a second thread's token.
    let manifest = fixture.manifest.clone();
    let token = spawn_blocking!("canonical_namespace_witness", move || {
        Arc::new(
            if create {
                Ownership::claim_cold(&manifest)
            } else {
                Ownership::reopen(&manifest)
            }
            .unwrap(),
        )
        .resynchronize()
        .unwrap()
    })
    .await
    .unwrap();
    let hooks = Hooks {
        before_current: Some(Box::new(|_| eprintln!("NL_OWNER_CURRENT_BEGIN"))),
        after_commit: Some(Box::new(|| eprintln!("NL_OWNER_COMMIT_ACK"))),
        after_current: Some(Box::new(|| eprintln!("NL_OWNER_CURRENT_ACK"))),
        ..Hooks::default()
    };
    let result = PreparedCanonicalOwner::open(token, create, hooks).await;
    if std::env::var_os("NL_OWNER_EXPECT_REFUSAL").is_some() {
        assert!(result.is_err(), "failed sync must not return an owner");
        eprintln!("NL_OWNER_REFUSED");
    } else {
        let owner = result.unwrap();
        assert_eq!(owner.diagnostic().repair_observed, mode == "repair");
        if owner.diagnostic().repair_observed {
            eprintln!("NL_OWNER_REPAIR_OBSERVED");
        }
        eprintln!("NL_OWNER_SUCCESS_ALIVE");
        fixture.locked();
        owner.release().await.unwrap();
        eprintln!("NL_OWNER_RELEASED");
    }
    std::mem::forget(fixture);
}
