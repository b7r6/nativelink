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

//! Bounded delivery qualification with reducer-produced accepted terminals.
//! The owned singleton only supplies the driver's unused authority handle;
//! this fixture controls adapter outcomes without proposing journal events.

use nativelink_config::stores::MemorySpec;
use nativelink_macro::nativelink_test;
use nativelink_scheduler::authority::{ClusterEpoch, Limits, State};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::fs;

use super::*;

const EPOCH: ClusterEpoch = ClusterEpoch(27);
const NAMESPACE: [u8; 32] = [91; 32];

fn apply(state: &mut State, event: Event) -> Reply {
    let reply = state.apply(RecordedEvent {
        epoch: EPOCH,
        event,
    });
    state.check_invariants().unwrap();
    reply
}

fn accepted_terminals() -> State {
    let mut state = State::new(EPOCH, Limits::default()).unwrap();
    state.enable_accepted_history(NAMESPACE).unwrap();
    let capacity = Resources {
        cpu_millis: 1,
        memory_bytes: 16,
        slots: 1,
    };
    assert_eq!(
        apply(
            &mut state,
            Event::Register {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(1),
                properties: BTreeMap::new(),
                capacity,
                available: capacity,
            },
        ),
        Reply::Applied
    );
    for index in 1_u8..=5 {
        assert!(matches!(
            apply(
                &mut state,
                Event::Submit {
                    operation: OperationId(u64::from(index)),
                    request: AuthorityRequest {
                        action: ActionKey {
                            instance: "delivery-fixture".to_owned(),
                            digest_function: "sha256".to_owned(),
                            digest: format!("{index:064x}"),
                            digest_size_bytes: 1,
                        },
                        execution_scope: None,
                        do_not_cache: index != 5,
                        skip_cache_lookup: true,
                        priority: 0,
                        properties: BTreeMap::new(),
                        resources: Resources {
                            cpu_millis: 1,
                            memory_bytes: 1,
                            slots: 1,
                        },
                        execution_timeout: 1000,
                        payload: None,
                    },
                },
            ),
            Reply::Submitted { .. }
        ));
        let Reply::Matched { attempt, .. } = apply(&mut state, Event::Match) else {
            panic!("fixture execution must obtain its one available slot");
        };
        let result = ResultPayload::from_response(
            &ExecuteResponse {
                result: Some(ActionResult {
                    stdout_raw: vec![index].into(),
                    ..Default::default()
                }),
                message: "accepted terminal remains unchanged".to_owned(),
                ..Default::default()
            },
            FRAME_BYTES,
        )
        .unwrap();
        assert_eq!(
            apply(
                &mut state,
                Event::Complete {
                    attempt,
                    worker: WorkerId(1),
                    incarnation: IncarnationId(1),
                    result,
                },
            ),
            Reply::Applied
        );
    }
    state
}

/// Consume one finished adapter call but deliberately lose its acknowledgment.
/// The accepted outbox remains pending, including when the sink stored bytes.
async fn lose_outcome(driver: &mut Driver) -> (EffectId, Event) {
    let (effect, outcome) =
        tokio::time::timeout(Duration::from_secs(2), driver.effects.join_next())
            .await
            .expect("bounded memory-store delivery must finish")
            .expect("one delivery task must remain")
            .expect("delivery task must not panic");
    assert!(driver.in_flight.remove(&effect));
    // All retries are already eligible. This models processing that outlasted
    // retry backoff; the fixture makes no claim about real journal latency.
    driver.retry_at.insert(effect, Instant::now());
    (
        effect,
        outcome.expect("the memory store must accept the write"),
    )
}

#[nativelink_test]
async fn due_history_retries_share_four_slots_and_cannot_starve_fresh_ac() {
    let accepted = accepted_terminals();
    let expected_view = accepted.view();
    let expected_history = accepted.accepted_history().cloned();
    let history = Arc::new(expected_history.clone());
    let histories = &history.as_ref().as_ref().unwrap().associations;
    assert_eq!(histories.len(), 5);
    let older: BTreeSet<_> = histories.values().take(4).map(|row| row.effect).collect();
    let newest_history = histories.values().last().unwrap().effect;
    let [publication] = expected_view.effects.as_slice() else {
        panic!("only the fifth terminal must have an ordinary publication");
    };
    let EffectKind::PublishResult { action, result, .. } = &publication.kind else {
        panic!("fixture must expose a real accepted AC write");
    };
    let publication_id = publication.id;
    assert!(older.iter().all(|effect| *effect < publication_id));
    assert!(publication_id < newest_history);
    let publication_digest = digest(action).unwrap();
    let expected_result = result
        .require_response()
        .unwrap()
        .decode()
        .unwrap()
        .result
        .unwrap();

    let directory = std::env::temp_dir().join(format!("history-delivery-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&directory).await.unwrap();
    let authority = Singleton::open(
        directory.join("authority.redb"),
        1,
        EPOCH,
        Limits::default(),
    )
    .await
    .unwrap();
    let cas = MemoryStore::new(&MemorySpec::default());
    let ac = MemoryStore::new(&MemorySpec::default());
    let state = Arc::new(expected_view.clone());
    let (committed, _observations) = watch::channel(state.clone());
    let receipts = Arc::new(None);
    let (committed_receipts, _receipt_observations) = watch::channel(receipts.clone());
    let (committed_history, _history_observations) = watch::channel(history.clone());
    let mut driver = Driver {
        authority,
        state,
        watermark: accepted.ingress_watermark(),
        committed,
        config: QualificationConfig {
            instance: "delivery-fixture".to_owned(),
            principal: "local-test".to_owned(),
            cas: Store::new(cas.clone()),
            cas_namespace: NAMESPACE,
            publication_sink: Store::new(ac.clone()),
            now: Arc::new(|| Ok(0)),
            faults: Arc::new(QualificationFaults::default()),
        },
        workers: BTreeMap::new(),
        sent: BTreeMap::new(),
        effects: JoinSet::new(),
        in_flight: BTreeSet::new(),
        retry_at: BTreeMap::new(),
        storage_cursor: EffectId(0),
        restore_deadlines: false,
        receipts_enabled: false,
        receipts,
        committed_receipts,
        receipt_issue_due: false,
        receipt_sent: BTreeMap::new(),
        history,
        committed_history,
    };

    driver.pump_effects().unwrap();
    assert_eq!(driver.in_flight, older);
    assert_eq!(driver.effects.len(), 4);
    driver.pump_effects().unwrap();
    assert_eq!(driver.in_flight.len(), 4);
    assert_eq!(driver.effects.len(), 4);
    assert_eq!(ac.len_for_test(), 0);

    let (uncertain, outcome) = lose_outcome(&mut driver).await;
    assert!(older.contains(&uncertain));
    assert!(matches!(outcome, Event::HistoryPublication { effect, .. } if effect == uncertain));
    assert_eq!(driver.in_flight.len(), 3);
    for effect in &older {
        driver.retry_at.insert(*effect, Instant::now());
    }
    driver.pump_effects().unwrap();
    assert_eq!(driver.in_flight.len(), 4, "both outboxes share four slots");
    assert_eq!(driver.effects.len(), 4);
    assert!(
        driver.in_flight.contains(&publication_id),
        "fresh AC delivery must advance despite every old history retry being due"
    );
    assert!(!driver.in_flight.contains(&uncertain));
    let mut delivered_ac = false;
    for _ in 0..4 {
        let (effect, outcome) = lose_outcome(&mut driver).await;
        assert!(driver.in_flight.len() < 4);
        if effect == publication_id {
            assert!(
                matches!(outcome, Event::EffectOutcome { effect, success: true } if effect == publication_id)
            );
            delivered_ac = true;
        } else {
            assert!(older.contains(&effect));
            assert!(matches!(outcome, Event::HistoryPublication { .. }));
        }
    }
    assert!(delivered_ac);
    assert_eq!(
        ActionResult::decode(
            bounded_blob(&driver.config.publication_sink, publication_digest)
                .await
                .unwrap()
        )
        .unwrap(),
        expected_result
    );
    assert_eq!(cas.len_for_test(), 4);

    // With all four slots free, the newer history must also advance among old
    // uncertain writes. No acknowledgment has changed either accepted outbox.
    driver.pump_effects().unwrap();
    assert_eq!(driver.in_flight.len(), 4);
    assert_eq!(driver.effects.len(), 4);
    assert!(driver.in_flight.contains(&newest_history));
    for _ in 0..4 {
        let (_, outcome) = lose_outcome(&mut driver).await;
        assert!(matches!(outcome, Event::HistoryPublication { .. }));
    }
    assert_eq!(cas.len_for_test(), 5);
    assert!(driver.in_flight.is_empty());
    assert!(driver.effects.is_empty());
    assert_eq!(driver.state.as_ref(), &expected_view);
    assert_eq!(driver.history.as_ref(), &expected_history);
    assert!(
        driver
            .history
            .as_ref()
            .as_ref()
            .unwrap()
            .associations
            .values()
            .all(|row| row.status == HistoryStatus::Pending)
    );
    driver.authority.shutdown().await.unwrap();
    fs::remove_dir_all(directory).await.unwrap();
}
