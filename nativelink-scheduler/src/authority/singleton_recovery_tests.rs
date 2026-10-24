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

//! Synthetic durable recovery fixtures, not a live multi-member migration.

use std::collections::BTreeMap;

use super::*;

#[derive(Clone, Copy, Debug)]
enum Constructor {
    Ambient,
    Profile,
    Factory,
}

impl Constructor {
    const ALL: [Self; 3] = [Self::Ambient, Self::Profile, Self::Factory];

    const fn domain(self) -> AuthorityDomain {
        AuthorityDomain {
            namespace: if matches!(self, Self::Ambient) {
                0
            } else {
                DOMAIN.namespace
            },
            ..DOMAIN
        }
    }

    async fn open(
        self,
        fixture: &LimitsFixture,
        profile: JournalProfile,
    ) -> std::result::Result<Singleton, String> {
        match self {
            Self::Ambient => {
                Singleton::open(fixture.path(), NODE, DOMAIN.epoch, Limits::default()).await
            }
            Self::Profile => {
                Singleton::open_with_profile(fixture.path(), NODE, Limits::default(), profile).await
            }
            Self::Factory => {
                let path = fixture.path();
                Singleton::open_from_factory(
                    JournalBinding {
                        node_id: NODE,
                        domain: DOMAIN,
                    },
                    NODE,
                    Limits::default(),
                    profile,
                    move || {
                        Database::open(path)
                            .map(JournalDatabase::existing)
                            .map_err(io::Error::other)
                    },
                )
                .await
            }
        }
    }
}

fn position(index: u64) -> LogId<u64> {
    LogId::new(openraft::CommittedLeaderId::new(1, NODE), index)
}

fn uniform(voters: &[u64], learners: &[u64]) -> Membership<u64, BasicNode> {
    Membership::new(
        vec![voters.iter().copied().collect()],
        learners.iter().copied().collect::<BTreeSet<_>>(),
    )
}

fn member_entry(index: u64, membership: Membership<u64, BasicNode>) -> Entry<TypeConfig> {
    Entry {
        log_id: position(index),
        payload: EntryPayload::Membership(membership),
    }
}

fn blank(index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: position(index),
        payload: EntryPayload::Blank,
    }
}

fn applied(
    domain: AuthorityDomain,
    last: u64,
    membership: StoredMembership<u64, BasicNode>,
) -> AppliedState {
    let format = ActiveFormat::base(domain);
    AppliedState {
        canonical_contract: None,
        last_applied: Some(position(last)),
        membership,
        scheduler: SchedulerSection::from_state(
            &State::new(domain.epoch, Limits::default()).unwrap(),
            &format,
        )
        .unwrap(),
        format,
    }
}

fn snapshot(applied: AppliedState) -> SavedSnapshot {
    SavedSnapshot {
        meta: SnapshotMeta {
            last_log_id: applied.last_applied,
            last_membership: applied.membership.clone(),
            snapshot_id: "synthetic-membership-recovery".to_owned(),
        },
        applied,
    }
}

async fn seed(
    constructor: Constructor,
    applied: AppliedState,
    committed: Option<LogId<u64>>,
    logs: Vec<Entry<TypeConfig>>,
    saved: Option<SavedSnapshot>,
) -> LimitsFixture {
    applied
        .state(DOMAIN.epoch, ReaderCapabilities::Base, None)
        .unwrap();
    if let Some(saved) = &saved {
        saved
            .validate(DOMAIN.epoch, ReaderCapabilities::Base, None)
            .unwrap();
    }
    let fixture = LimitsFixture::new();
    let journal = if matches!(constructor, Constructor::Ambient) {
        Journal::open(fixture.path(), NODE, DOMAIN.epoch, Limits::default())
            .await
            .unwrap()
    } else {
        fixture
            .reopen(Limits::default(), profile(), false)
            .await
            .unwrap()
    };
    let transaction = journal.database.begin_write().unwrap();
    {
        let mut meta = transaction.open_table(META).unwrap();
        for (key, value) in [
            ("applied", encode(DOMAIN.epoch, &applied).unwrap()),
            ("committed", encode(DOMAIN.epoch, &committed).unwrap()),
        ] {
            meta.insert(key, value.as_slice()).unwrap();
        }
        if let Some(saved) = saved {
            let bytes = encode(DOMAIN.epoch, &saved).unwrap();
            meta.insert("snapshot", bytes.as_slice()).unwrap();
        }
        let mut table = transaction.open_table(LOGS).unwrap();
        for entry in logs {
            let bytes = encode(DOMAIN.epoch, &entry).unwrap();
            table.insert(entry.log_id.index, bytes.as_slice()).unwrap();
        }
    }
    transaction.commit().unwrap();
    drop(journal);
    fixture
}

async fn refuses_without_mutation_or_registration(
    constructor: Constructor,
    fixture: &LimitsFixture,
) {
    let database = Database::open(fixture.path()).unwrap();
    let before = logical_contents(&database);
    drop(database);
    let profile = profile();
    let error = match constructor.open(fixture, profile.clone()).await {
        Ok(singleton) => {
            singleton.shutdown().await.unwrap();
            panic!("{constructor:?} accepted unsupported singleton recovery");
        }
        Err(error) => error,
    };
    let database = Database::open(fixture.path()).unwrap();
    assert_eq!(
        logical_contents(&database),
        before,
        "refusal followed durable Raft mutation: {error}"
    );
    drop(database);
    assert!(
        error.contains("singleton recovery"),
        "wrong refusal: {error}"
    );
    // Generic journals still support replicated membership. A failed singleton
    // preflight must leave this same registry/incarnation unused.
    let journal = if matches!(constructor, Constructor::Ambient) {
        Journal::open(fixture.path(), NODE, DOMAIN.epoch, Limits::default())
            .await
            .unwrap()
    } else {
        fixture
            .reopen(
                Limits::default(),
                profile,
                matches!(constructor, Constructor::Factory),
            )
            .await
            .unwrap()
    };
    assert_eq!(logical_contents(&journal.database), before);
}

#[nativelink_test]
async fn singleton_refuses_foreign_joint_learner_and_unsupported_applied_membership() {
    let local = uniform(&[NODE], &[]);
    let missing_node: Membership<u64, BasicNode> = serde_json::from_value(serde_json::json!({
        "configs": [[NODE]], "nodes": {}
    }))
    .unwrap();
    let cases = [
        StoredMembership::new(Some(position(0)), uniform(&[NODE + 1], &[])),
        StoredMembership::new(Some(position(0)), uniform(&[NODE, NODE + 1], &[])),
        StoredMembership::new(
            Some(position(0)),
            Membership::new(vec![BTreeSet::from([NODE]), BTreeSet::from([NODE])], ()),
        ),
        StoredMembership::new(Some(position(0)), uniform(&[NODE], &[NODE + 1])),
        StoredMembership::new(Some(position(0)), missing_node),
        StoredMembership::new(None, local),
        StoredMembership::default(),
    ];
    for constructor in Constructor::ALL {
        for membership in &cases {
            let logs = vec![member_entry(0, membership.membership().clone()), blank(1)];
            let fixture = seed(
                constructor,
                applied(constructor.domain(), 1, membership.clone()),
                Some(position(1)),
                logs,
                None,
            )
            .await;
            refuses_without_mutation_or_registration(constructor, &fixture).await;
        }
    }
}

#[nativelink_test]
async fn singleton_refuses_pending_membership_even_when_it_still_names_only_self() {
    for constructor in Constructor::ALL {
        for pending in [uniform(&[NODE], &[]), uniform(&[NODE, NODE + 1], &[])] {
            let local = uniform(&[NODE], &[]);
            let fixture = seed(
                constructor,
                applied(
                    constructor.domain(),
                    1,
                    StoredMembership::new(Some(position(0)), local.clone()),
                ),
                Some(position(1)),
                vec![member_entry(0, local), blank(1), member_entry(2, pending)],
                None,
            )
            .await;
            refuses_without_mutation_or_registration(constructor, &fixture).await;
        }
    }
}

#[nativelink_test]
async fn singleton_checks_committed_membership_before_replaying_scheduler_events() {
    for constructor in Constructor::ALL {
        let local = uniform(&[NODE], &[]);
        let fixture = seed(
            constructor,
            applied(
                constructor.domain(),
                0,
                StoredMembership::new(Some(position(0)), local.clone()),
            ),
            Some(position(2)),
            vec![
                member_entry(0, local),
                Entry {
                    log_id: position(1),
                    payload: EntryPayload::Normal(
                        Batch::new([RecordedEvent {
                            epoch: DOMAIN.epoch,
                            event: Event::AdvanceTime { now: 17 },
                        }])
                        .unwrap(),
                    ),
                },
                member_entry(2, uniform(&[NODE + 1], &[])),
            ],
            None,
        )
        .await;
        refuses_without_mutation_or_registration(constructor, &fixture).await;
    }
}

#[nativelink_test]
async fn singleton_committed_projection_uses_raft_index_interval() {
    for constructor in Constructor::ALL {
        let local = uniform(&[NODE], &[]);
        let high = |index| LogId::new(openraft::CommittedLeaderId::new(5, NODE), index);
        let mut state = applied(
            constructor.domain(),
            0,
            StoredMembership::new(Some(high(0)), local.clone()),
        );
        state.last_applied = Some(high(0));
        // The lower term makes the foreign membership compare below applied,
        // despite lying inside OpenRaft's committed replay index interval.
        let fixture = seed(
            constructor,
            state,
            Some(high(2)),
            vec![
                Entry {
                    log_id: high(0),
                    payload: EntryPayload::Membership(local),
                },
                member_entry(1, uniform(&[NODE + 1], &[])),
                Entry {
                    log_id: high(2),
                    payload: EntryPayload::Blank,
                },
            ],
            None,
        )
        .await;
        refuses_without_mutation_or_registration(constructor, &fixture).await;
    }
}

#[nativelink_test]
async fn singleton_refuses_future_or_contradictory_current_snapshot_membership() {
    for constructor in Constructor::ALL {
        for future in [false, true] {
            let local = uniform(&[NODE], &[]);
            let member = StoredMembership::new(Some(position(0)), local.clone());
            let saved = snapshot(applied(
                constructor.domain(),
                if future { 2 } else { 1 },
                if future {
                    member.clone()
                } else {
                    StoredMembership::new(Some(position(0)), uniform(&[NODE + 1], &[]))
                },
            ));
            let fixture = seed(
                constructor,
                applied(constructor.domain(), 1, member),
                Some(position(1)),
                vec![member_entry(0, local), blank(1)],
                Some(saved),
            )
            .await;
            refuses_without_mutation_or_registration(constructor, &fixture).await;
        }
    }
}

#[nativelink_test]
async fn singleton_recovers_committed_three_to_one_with_historical_snapshot() {
    for constructor in Constructor::ALL {
        for final_unapplied in [false, true] {
            let old = uniform(&[NODE, NODE + 1, NODE + 2], &[]);
            let joint = Membership::new(
                vec![
                    BTreeSet::from([NODE, NODE + 1, NODE + 2]),
                    BTreeSet::from([NODE]),
                ],
                (),
            );
            let local = Membership::new(
                vec![BTreeSet::from([NODE])],
                BTreeMap::from([(NODE, BasicNode::new("historic-address:17"))]),
            );
            let old_state = applied(
                constructor.domain(),
                0,
                StoredMembership::new(Some(position(0)), old.clone()),
            );
            let current = if final_unapplied {
                old_state.clone()
            } else {
                applied(
                    constructor.domain(),
                    2,
                    StoredMembership::new(Some(position(2)), local.clone()),
                )
            };
            let fixture = seed(
                constructor,
                current,
                Some(position(2)),
                vec![
                    member_entry(0, old),
                    member_entry(1, joint),
                    member_entry(2, local.clone()),
                ],
                Some(snapshot(old_state)),
            )
            .await;
            let singleton = constructor.open(&fixture, profile()).await.unwrap();
            let (_, membership) = singleton.journal.clone().applied_state().await.unwrap();
            assert_eq!(membership, StoredMembership::new(Some(position(2)), local));
            singleton.shutdown().await.unwrap();
        }
    }
}

#[nativelink_test]
async fn singleton_allows_non_membership_suffix_and_applied_ahead_of_commit_marker() {
    for constructor in Constructor::ALL {
        let local = uniform(&[NODE], &[]);
        let member = StoredMembership::new(Some(position(0)), local.clone());
        let fixture = seed(
            constructor,
            applied(constructor.domain(), 1, member.clone()),
            Some(position(0)),
            vec![member_entry(0, local.clone()), blank(1), blank(2)],
            Some(snapshot(applied(constructor.domain(), 1, member))),
        )
        .await;
        let singleton = constructor.open(&fixture, profile()).await.unwrap();
        assert_eq!(
            singleton
                .journal
                .clone()
                .applied_state()
                .await
                .unwrap()
                .1
                .membership(),
            &local
        );
        singleton.shutdown().await.unwrap();
    }
}

#[nativelink_test]
async fn singleton_metadata_only_refusal_precedes_registration() {
    for constructor in Constructor::ALL {
        let fixture = LimitsFixture::new();
        let journal = if matches!(constructor, Constructor::Ambient) {
            Journal::open(fixture.path(), NODE, DOMAIN.epoch, Limits::default())
                .await
                .unwrap()
        } else {
            fixture
                .reopen(Limits::default(), profile(), false)
                .await
                .unwrap()
        };
        drop(journal);
        refuses_without_mutation_or_registration(constructor, &fixture).await;
    }
}

#[nativelink_test]
async fn singleton_rejects_membership_position_beyond_recovered_index() {
    for constructor in Constructor::ALL {
        let high = LogId::new(openraft::CommittedLeaderId::new(5, NODE), 0);
        let mut state = applied(
            constructor.domain(),
            0,
            StoredMembership::new(Some(position(5)), uniform(&[NODE], &[])),
        );
        state.last_applied = Some(high);
        let fixture = seed(
            constructor,
            state,
            Some(high),
            vec![Entry {
                log_id: high,
                payload: EntryPayload::Blank,
            }],
            None,
        )
        .await;
        refuses_without_mutation_or_registration(constructor, &fixture).await;
    }
}
