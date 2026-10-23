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

//! Private applied-facts and real sealed recovery clock oracles.

use std::time::Instant;

use super::super::super::canonical_clock::{self, ClockError};
use super::*;

#[nativelink_test]
async fn applied_facts_exclude_both_committed_and_uncommitted_staged_time() {
    for committed in [false, true] {
        let fixture = seeded(|epoch| empty(epoch, BASE)).await;
        suffix(
            &fixture,
            vec![vec![Event::AdvanceTime { now: BASE + 9 }]],
            committed,
        );
        let before = stored(&fixture);
        let owner = PreparedCanonicalOwner::reopen_sealed(fixture.token(false))
            .await
            .unwrap();
        let facts = super::super::super::temporal::audit(
            &owner.journal,
            owner.journal.domain,
            owner.journal.canonical_contract.unwrap(),
            CanonicalBootstrap::Sealed,
            None,
        )
        .await
        .unwrap();
        assert_eq!(facts.now, BASE);
        assert_eq!(facts.last_applied.unwrap().index, 0);
        owner.release().await.unwrap();
        assert_eq!(stored(&fixture), before);
    }
}

#[nativelink_test]
async fn real_recovery_clock_uses_replayed_time_not_preflight_time() {
    for committed in [false, true] {
        let fixture = seeded(|epoch| empty(epoch, BASE)).await;
        suffix(
            &fixture,
            vec![vec![Event::AdvanceTime { now: BASE + 9 }]],
            committed,
        );
        let owner = PreparedCanonicalOwner::reopen_sealed(fixture.token(false))
            .await
            .unwrap();
        let node = Singleton::from_journal(owner.journal, None).await.unwrap();
        let before = Instant::now();
        let clock = canonical_clock::recover(&node.raft, &node.journal)
            .await
            .unwrap();
        let facts = clock.diagnostic();
        assert_eq!(facts.base_tick, BASE + 9);
        assert!(facts.applied.index > 0);
        assert!(clock.origin_for_test() >= before);
        node.shutdown().await.unwrap();
        fixture.released().await;
    }
}

#[nativelink_test]
async fn real_base_barrier_waits_for_apply_while_journal_reads_are_available() {
    let fixture = seeded(|epoch| empty(epoch, BASE)).await;
    let owner = PreparedCanonicalOwner::reopen_sealed(fixture.token(false))
        .await
        .unwrap();
    let node = Singleton::from_journal(owner.journal, None).await.unwrap();
    node.read().await.unwrap();
    let (reached, release) = node.journal.pause_canonical_base_apply_once();
    let journal = node.journal.clone();
    let raft = node.raft.clone();
    let proposal = background_spawn!("canonical_clock_real_apply", async move {
        journal
            .propose_batch(
                &raft,
                Batch::new([RecordedEvent {
                    epoch: journal.epoch,
                    event: Event::AdvanceTime { now: BASE + 9 },
                }])
                .unwrap(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reached)
        .await
        .unwrap()
        .unwrap();
    // The real committed entry is not applied; no Journal.io guard is held by
    // the test gate. Both the old persisted state and read watermark are visible.
    let (watermark, applied) =
        tokio::time::timeout(Duration::from_secs(5), node.raft.get_read_log_id())
            .await
            .expect("real read watermark prerequisite stalled")
            .unwrap();
    assert!(watermark.unwrap().index > applied.unwrap().index);
    let observed = tokio::time::timeout(
        Duration::from_secs(5),
        super::super::super::temporal::audit(
            &node.journal,
            node.journal.domain,
            node.journal.canonical_contract.unwrap(),
            CanonicalBootstrap::Sealed,
            None,
        ),
    )
    .await
    .expect("Base gate must leave the real journal read queue available")
    .unwrap();
    assert_eq!(observed.now, BASE);
    assert!(observed.last_applied.unwrap().index < watermark.unwrap().index);
    let journal = node.journal.clone();
    let raft = node.raft.clone();
    let mut recovering = background_spawn!("canonical_clock_real_barrier", async move {
        canonical_clock::recover(&raft, &journal).await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut recovering)
            .await
            .is_err(),
        "clock recovery returned before the real Base application barrier"
    );
    let released_at = Instant::now();
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), proposal)
        .await
        .expect("released Base application prerequisite stalled")
        .unwrap()
        .unwrap();
    let clock = tokio::time::timeout(Duration::from_secs(5), recovering)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(clock.diagnostic().base_tick, BASE + 9);
    assert!(clock.diagnostic().applied.index >= watermark.unwrap().index);
    assert!(clock.origin_for_test() >= released_at);
    node.shutdown().await.unwrap();
    fixture.released().await;
}

#[nativelink_test]
async fn clock_origin_follows_completion_of_the_validated_read() {
    let fixture = seeded(|epoch| empty(epoch, BASE)).await;
    let owner = PreparedCanonicalOwner::reopen_sealed(fixture.token(false))
        .await
        .unwrap();
    let node = Singleton::from_journal(owner.journal, None).await.unwrap();
    let (hook, reached, gate, done) = temporal_pause();
    let journal = node.journal.clone();
    let raft = node.raft.clone();
    let mut recovering = background_spawn!("canonical_clock_after_read", async move {
        canonical_clock::recover_with_audit_hook(&raft, &journal, hook).await
    });
    tokio::time::timeout(Duration::from_secs(5), reached)
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut recovering)
            .await
            .is_err(),
        "clock escaped while the validated transaction was paused"
    );
    let released_at = Instant::now();
    *gate.0.lock().unwrap() = true;
    gate.1.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), done)
            .await
            .unwrap()
            .unwrap()
    );
    let clock = tokio::time::timeout(Duration::from_secs(5), recovering)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(clock.origin_for_test() >= released_at);
    assert_eq!(clock.diagnostic().base_tick, BASE);
    node.shutdown().await.unwrap();
    fixture.released().await;
}

#[nativelink_test]
async fn recovery_read_revalidates_sealed_metadata_after_initial_open() {
    let fixture = seeded(|epoch| empty(epoch, BASE)).await;
    let owner = PreparedCanonicalOwner::reopen_sealed(fixture.token(false))
        .await
        .unwrap();
    let node = Singleton::from_journal(owner.journal, None).await.unwrap();
    node.read().await.unwrap();
    edit(&node.journal.database, |table| {
        table
            .insert(
                "canonical_bootstrap",
                encode(node.journal.epoch, &CanonicalBootstrap::Preparing)
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
    });
    assert!(matches!(
        canonical_clock::recover(&node.raft, &node.journal).await,
        Err(ClockError::Read)
    ));
    node.shutdown().await.unwrap();
    fixture.released().await;
}
