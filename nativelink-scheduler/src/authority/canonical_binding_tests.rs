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

//! Storage-only synthetic contract fixtures. No canonical serving constructor,
//! filesystem identity attestation, bootstrap sealing or clock lifetime proof.

use openraft::storage::RaftLogStorageExt;

use super::super::canonical_binding::CanonicalProfile;
use super::*;

#[derive(Serialize)]
struct LegacyApplied<'a> {
    last_applied: Option<LogId<u64>>,
    membership: &'a StoredMembership<u64, BasicNode>,
    scheduler: &'a SchedulerSection,
    format: &'a ActiveFormat,
}

const CONTRACT: CanonicalContract = CanonicalContract {
    profile: CanonicalProfile::SingletonSecondsV1,
    namespace_commitment: [7; 32],
};
const FOREIGN: CanonicalContract = CanonicalContract {
    namespace_commitment: [8; 32],
    ..CONTRACT
};

async fn open(
    fixture: &LimitsFixture,
    expected: Option<CanonicalContract>,
    create: bool,
    admission: Option<JournalProfile>,
    fail_parent: bool,
) -> Result<Journal> {
    let path = fixture.path();
    Journal::open_with_factory_inner(
        NODE,
        DOMAIN.epoch,
        admission.as_ref().map_or(
            AuthorityDomain {
                epoch: DOMAIN.epoch,
                namespace: 0,
            },
            |p| p.domain,
        ),
        Limits::default(),
        admission,
        RecoveryExpectation::Any,
        expected,
        move || {
            if create {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .map_err(|error| failure(ErrorVerb::Write, error))?;
                let database = Database::builder()
                    .create_file(file)
                    .map_err(|error| failure(ErrorVerb::Write, error))?;
                Ok(JournalDatabase::created(database, move || {
                    if fail_parent {
                        Err(io::Error::other("synthetic parent sync failure"))
                    } else {
                        File::open(path.parent().unwrap())?.sync_all()
                    }
                }))
            } else {
                Database::open(path)
                    .map(JournalDatabase::existing)
                    .map_err(|error| failure(ErrorVerb::Read, error))
            }
        },
    )
    .await
}

fn edit(database: &Database, mutation: impl FnOnce(&mut redb::Table<&str, &[u8]>)) {
    let transaction = database.begin_write().unwrap();
    {
        let mut table = transaction.open_table(META).unwrap();
        mutation(&mut table);
    }
    transaction.commit().unwrap();
}

fn set_binding(database: &Database, mask: u8) {
    edit(database, |table| {
        if mask & 1 == 0 {
            table.remove("canonical_contract").unwrap();
        } else {
            let bytes = encode(DOMAIN.epoch, &CONTRACT).unwrap();
            table
                .insert("canonical_contract", bytes.as_slice())
                .unwrap();
        }
        if mask & 2 == 0 {
            table.remove("canonical_bootstrap").unwrap();
        } else {
            // Deliberately synthetic: this test cannot complete bootstrap.
            let bytes = encode(DOMAIN.epoch, &CanonicalBootstrap::Sealed).unwrap();
            table
                .insert("canonical_bootstrap", bytes.as_slice())
                .unwrap();
        }
        let mut applied: AppliedState =
            decode(DOMAIN.epoch, table.get("applied").unwrap().unwrap().value()).unwrap();
        applied.canonical_contract = (mask & 4 != 0).then_some(CONTRACT);
        let bytes = encode(DOMAIN.epoch, &applied).unwrap();
        table.insert("applied", bytes.as_slice()).unwrap();
    });
}

fn seal_synthetic(database: &Database) {
    edit(database, |table| {
        let bytes = encode(DOMAIN.epoch, &CanonicalBootstrap::Sealed).unwrap();
        table
            .insert("canonical_bootstrap", bytes.as_slice())
            .unwrap();
    });
}

fn stored_applied(database: &Database) -> AppliedState {
    let transaction = database.begin_read().unwrap();
    decode(
        DOMAIN.epoch,
        transaction
            .open_table(META)
            .unwrap()
            .get("applied")
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap()
}

fn stored_phase(database: &Database) -> CanonicalBootstrap {
    let transaction = database.begin_read().unwrap();
    decode(
        DOMAIN.epoch,
        transaction
            .open_table(META)
            .unwrap()
            .get("canonical_bootstrap")
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap()
}

fn event(index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(1, NODE), index),
        payload: EntryPayload::Normal(
            Batch::new([RecordedEvent {
                epoch: DOMAIN.epoch,
                event: Event::AdvanceTime { now: index },
            }])
            .unwrap(),
        ),
    }
}

#[test]
fn contract_codec_has_one_profile_exact_commitment_and_legacy_encoding() {
    CONTRACT.validate().unwrap();
    let json = serde_json::to_value(CONTRACT).unwrap();
    assert_eq!(json["profile"], "singleton_seconds_v1");
    assert_eq!(
        serde_json::from_value::<CanonicalContract>(json.clone()).unwrap(),
        CONTRACT
    );
    for length in [0, 31, 33] {
        let mut invalid = json.clone();
        invalid["namespace_commitment"] = vec![7; length].into();
        assert!(serde_json::from_value::<CanonicalContract>(invalid).is_err());
    }
    for field in ["profile", "clock_seconds"] {
        let mut invalid = json.clone();
        invalid[field] = "operator_selected_clock".into();
        assert!(serde_json::from_value::<CanonicalContract>(invalid).is_err());
    }
    assert!(
        CanonicalContract {
            namespace_commitment: [0; 32],
            ..CONTRACT
        }
        .validate()
        .is_err()
    );

    let database = Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .unwrap();
    Journal::initialize_database(
        &database,
        NODE,
        DOMAIN.epoch,
        &State::new(DOMAIN.epoch, Limits::default()).unwrap(),
        DOMAIN,
        None,
    )
    .unwrap();
    let applied = stored_applied(&database);
    assert_eq!(applied.canonical_contract, None);
    let legacy = LegacyApplied {
        last_applied: applied.last_applied,
        membership: &applied.membership,
        scheduler: &applied.scheduler,
        format: &applied.format,
    };
    assert_eq!(
        encode(DOMAIN.epoch, &applied).unwrap(),
        encode(DOMAIN.epoch, &legacy).unwrap()
    );
    assert!(
        !logical_contents(&database)
            .metadata
            .iter()
            .any(|(key, _)| key.starts_with("canonical_"))
    );
}

#[nativelink_test]
async fn invalid_commitment_refuses_before_factory_and_existing_empty_is_not_new() {
    let invalid = CanonicalContract {
        namespace_commitment: [0; 32],
        ..CONTRACT
    };
    assert!(
        Journal::open_with_factory_inner(
            NODE,
            DOMAIN.epoch,
            DOMAIN,
            Limits::default(),
            Some(profile()),
            RecoveryExpectation::Any,
            Some(invalid),
            || panic!("invalid contract must precede factory"),
        )
        .await
        .is_err()
    );
    let fixture = LimitsFixture::new();
    drop(Database::create(fixture.path()).unwrap());
    assert!(
        open(&fixture, Some(CONTRACT), false, Some(profile()), false)
            .await
            .is_err()
    );
    let database = Database::open(fixture.path()).unwrap();
    assert!(database.begin_read().unwrap().open_table(META).is_err());
}

#[nativelink_test]
async fn cold_binding_is_atomic_and_parent_failure_never_seals_or_adopts() {
    for fail_parent in [false, true] {
        let fixture = LimitsFixture::new();
        let result = open(&fixture, Some(CONTRACT), true, Some(profile()), fail_parent).await;
        assert_eq!(result.is_err(), fail_parent);
        drop(result);
        let database = Database::open(fixture.path()).unwrap();
        assert_eq!(stored_applied(&database).canonical_contract, Some(CONTRACT));
        assert_eq!(stored_phase(&database), CanonicalBootstrap::Preparing);
        let before = logical_contents(&database);
        assert!(
            before
                .metadata
                .iter()
                .any(|(key, bytes)| key == "canonical_contract"
                    && decode::<CanonicalContract>(DOMAIN.epoch, bytes).unwrap() == CONTRACT)
        );
        assert!(before.metadata.iter().any(|(key, _)| key == "identity"));
        drop(database);
        assert!(
            open(&fixture, Some(CONTRACT), false, Some(profile()), false)
                .await
                .is_err()
        );
        assert!(
            open(&fixture, Some(CONTRACT), true, Some(profile()), false)
                .await
                .is_err()
        );
        let database = Database::open(fixture.path()).unwrap();
        assert_eq!(logical_contents(&database), before);
        drop(database);
    }
}

#[nativelink_test]
async fn generic_paths_refuse_every_partial_binding_before_admission_or_mutation() {
    for mask in 1..8 {
        for ambient in [false, true] {
            let fixture = LimitsFixture::new();
            let journal = open(&fixture, None, true, (!ambient).then(profile), false)
                .await
                .unwrap();
            set_binding(&journal.database, mask);
            let before = logical_contents(&journal.database);
            drop(journal);
            let refused_profile = profile();
            if ambient {
                assert!(
                    Journal::open(fixture.path(), NODE, DOMAIN.epoch, Limits::default())
                        .await
                        .is_err()
                );
            } else {
                assert!(
                    Journal::open_with_profile(
                        fixture.path(),
                        NODE,
                        Limits::default(),
                        refused_profile.clone()
                    )
                    .await
                    .is_err()
                );
                assert!(
                    fixture
                        .reopen(Limits::default(), refused_profile.clone(), true)
                        .await
                        .is_err()
                );
            }
            let domain = if ambient {
                AuthorityDomain {
                    namespace: 0,
                    ..DOMAIN
                }
            } else {
                DOMAIN
            };
            assert!(
                Journal::recover_admission(
                    domain,
                    ReaderCapabilities::Base,
                    vec![(NODE, fixture.path())]
                )
                .await
                .is_err()
            );
            let database = Database::open(fixture.path()).unwrap();
            assert_eq!(logical_contents(&database), before);
            set_binding(&database, 0);
            drop(database);
            // Reusing exactly the rejected profile proves no admission registration.
            if ambient {
                Journal::open(fixture.path(), NODE, DOMAIN.epoch, Limits::default())
                    .await
                    .unwrap();
            } else {
                fixture
                    .reopen(Limits::default(), refused_profile, true)
                    .await
                    .unwrap();
            }
        }
    }
}

#[nativelink_test]
async fn synthetic_sealed_reopen_requires_every_binding_half_and_exact_expected_identity() {
    for mask in 0..8 {
        let fixture = LimitsFixture::new();
        let journal = open(&fixture, Some(CONTRACT), true, Some(profile()), false)
            .await
            .unwrap();
        set_binding(&journal.database, mask);
        let before = logical_contents(&journal.database);
        drop(journal);
        let p = profile();
        let result = open(&fixture, Some(CONTRACT), false, Some(p.clone()), false).await;
        assert_eq!(result.is_ok(), mask == 7, "mask={mask}");
        drop(result);
        let database = Database::open(fixture.path()).unwrap();
        assert_eq!(logical_contents(&database), before);
        set_binding(&database, 7);
        drop(database);
        if mask != 7 {
            open(&fixture, Some(CONTRACT), false, Some(p), false)
                .await
                .unwrap();
        }
    }
    let fixture = LimitsFixture::new();
    let journal = open(&fixture, Some(CONTRACT), true, Some(profile()), false)
        .await
        .unwrap();
    seal_synthetic(&journal.database);
    let before = logical_contents(&journal.database);
    drop(journal);
    let p = profile();
    assert!(
        open(&fixture, Some(FOREIGN), false, Some(p.clone()), false)
            .await
            .is_err()
    );
    let journal = open(&fixture, Some(CONTRACT), false, Some(p), false)
        .await
        .unwrap();
    assert_eq!(logical_contents(&journal.database), before);
}

#[nativelink_test]
async fn bound_apply_and_snapshot_roundtrip_preserve_contract_but_not_local_phase() {
    let fixture = LimitsFixture::new();
    let mut journal = open(&fixture, Some(CONTRACT), true, Some(profile()), false)
        .await
        .unwrap();
    let entry = event(1);
    journal.blocking_append([entry.clone()]).await.unwrap();
    journal.apply([entry]).await.unwrap();
    assert_eq!(journal.read_applied().await.unwrap().1.view().now, 1);
    assert_eq!(
        stored_applied(&journal.database).canonical_contract,
        Some(CONTRACT)
    );
    let snapshot = journal.build_snapshot().await.unwrap();
    let snapshot_bytes = snapshot.snapshot.into_inner().unwrap();
    let saved: SavedSnapshot = decode(DOMAIN.epoch, &snapshot_bytes).unwrap();
    assert_eq!(saved.applied.canonical_contract, Some(CONTRACT));
    let before_phase = stored_phase(&journal.database);
    journal
        .install_snapshot(
            &snapshot.meta,
            Box::new(SnapshotBuffer::from_bytes(snapshot_bytes).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(stored_phase(&journal.database), before_phase);
    seal_synthetic(&journal.database);
    let rebuilt = journal.get_current_snapshot().await.unwrap().unwrap();
    let saved_again: SavedSnapshot =
        decode(DOMAIN.epoch, &rebuilt.snapshot.into_inner().unwrap()).unwrap();
    assert_eq!(saved_again.applied.canonical_contract, Some(CONTRACT));
    assert!(
        !serde_json::to_value(&saved_again)
            .unwrap()
            .to_string()
            .contains("canonical_bootstrap")
    );
    drop(journal);
    let journal = open(&fixture, Some(CONTRACT), false, Some(profile()), false)
        .await
        .unwrap();
    assert_eq!(journal.read_applied().await.unwrap().1.view().now, 1);
}

#[nativelink_test]
async fn snapshot_import_and_retained_snapshot_reject_legacy_or_foreign_binding_before_writes() {
    for target in [None, Some(CONTRACT)] {
        for source in [None, Some(CONTRACT), Some(FOREIGN)] {
            if target == source {
                continue;
            }
            let fixture = LimitsFixture::new();
            let mut journal = open(&fixture, target, true, Some(profile()), false)
                .await
                .unwrap();
            let snapshot = journal.build_snapshot().await.unwrap();
            let mut saved: SavedSnapshot =
                decode(DOMAIN.epoch, &snapshot.snapshot.into_inner().unwrap()).unwrap();
            saved.applied.canonical_contract = source;
            let bytes = encode(DOMAIN.epoch, &saved).unwrap();
            let before = logical_contents(&journal.database);
            assert!(
                journal
                    .install_snapshot(
                        &saved.meta,
                        Box::new(SnapshotBuffer::from_bytes(bytes.clone()).unwrap())
                    )
                    .await
                    .is_err()
            );
            assert_eq!(logical_contents(&journal.database), before);
            edit(&journal.database, |table| {
                table.insert("snapshot", bytes.as_slice()).unwrap();
            });
            assert!(journal.get_current_snapshot().await.is_err());
            if target.is_some() {
                seal_synthetic(&journal.database);
            }
            let before = logical_contents(&journal.database);
            drop(journal);
            if target.is_none() {
                assert!(
                    Journal::recover_admission(
                        DOMAIN,
                        ReaderCapabilities::Base,
                        vec![(NODE, fixture.path())],
                    )
                    .await
                    .is_err()
                );
            }
            let p = profile();
            assert!(
                open(&fixture, target, false, Some(p.clone()), false)
                    .await
                    .is_err()
            );
            let database = Database::open(fixture.path()).unwrap();
            assert_eq!(logical_contents(&database), before);
            edit(&database, |table| {
                table.remove("snapshot").unwrap();
            });
            drop(database);
            open(&fixture, target, false, Some(p), false).await.unwrap();
        }
    }
}

#[nativelink_test]
async fn live_applied_binding_mismatch_refuses_reads_apply_and_snapshot_build() {
    for expected in [None, Some(CONTRACT)] {
        let fixture = LimitsFixture::new();
        let mut journal = open(&fixture, expected, true, Some(profile()), false)
            .await
            .unwrap();
        edit(&journal.database, |table| {
            let mut applied: AppliedState =
                decode(DOMAIN.epoch, table.get("applied").unwrap().unwrap().value()).unwrap();
            applied.canonical_contract = Some(FOREIGN);
            let bytes = encode(DOMAIN.epoch, &applied).unwrap();
            table.insert("applied", bytes.as_slice()).unwrap();
        });
        let before = logical_contents(&journal.database);
        assert!(journal.read_applied().await.is_err());
        assert!(journal.applied_state().await.is_err());
        assert!(journal.active_format().await.is_err());
        assert!(journal.apply([event(1)]).await.is_err());
        assert!(journal.build_snapshot().await.is_err());
        assert_eq!(logical_contents(&journal.database), before);
    }
}

#[nativelink_test]
async fn live_required_metadata_drift_refuses_reads_and_durable_mutation() {
    for key in ["canonical_contract", "canonical_bootstrap"] {
        let fixture = LimitsFixture::new();
        let mut journal = open(&fixture, Some(CONTRACT), true, Some(profile()), false)
            .await
            .unwrap();
        edit(&journal.database, |table| {
            table.remove(key).unwrap();
        });
        let before = logical_contents(&journal.database);
        assert!(journal.read_applied().await.is_err());
        assert!(journal.get_log_state().await.is_err());
        assert!(journal.try_get_log_entries(0..10).await.is_err());
        assert!(journal.save_committed(Some(event(1).log_id)).await.is_err());
        assert!(journal.blocking_append([event(1)]).await.is_err());
        assert_eq!(logical_contents(&journal.database), before);
    }
}
