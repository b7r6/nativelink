// Copyright 2024 The NativeLink Authors. All rights reserved.
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

//! The raft backend survives a restart: whatever it acknowledged before the
//! process went away is there after it comes back, whether the state is
//! rebuilt from the log alone, from a snapshot plus the log after it, or from
//! a log whose last write was torn by the crash.

use core::time::Duration;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_scheduler::awaited_action_db::{AwaitedActionDb, AwaitedActionSubscriber};
use nativelink_scheduler::raft_awaited_action_db::state::AppliedState;
use nativelink_scheduler::raft_awaited_action_db::{Command, RaftAwaitedActionDb, RaftDbConfig};
use nativelink_util::action_messages::{
    ActionInfo, ActionStage, ActionUniqueKey, ActionUniqueQualifier, OperationId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use tokio::sync::Notify;

type Db = RaftAwaitedActionDb<SystemTime, fn() -> SystemTime>;

fn action_info(byte: u8) -> Arc<ActionInfo> {
    Arc::new(ActionInfo {
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::MAX,
        platform_properties: HashMap::new(),
        priority: 0,
        load_timestamp: UNIX_EPOCH,
        insert_timestamp: UNIX_EPOCH,
        unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
            execution_scope: None,
            instance_name: "durability".to_string(),
            digest_function: DigestHasherFunc::Sha256,
            digest: DigestInfo::new([byte; 32], u64::from(byte)),
        }),
    })
}

fn config(dir: &Path) -> RaftDbConfig {
    RaftDbConfig {
        data_dir: Some(dir.to_path_buf()),
        retain_client: Duration::from_secs(3600),
        ..RaftDbConfig::default()
    }
}

async fn open(dir: &Path) -> Result<Db, Error> {
    let now_fn: fn() -> SystemTime = SystemTime::now;
    Db::open(config(dir), Arc::new(Notify::new()), now_fn).await
}

/// Add `n` actions and move the first one to `first_stage`, so the state has
/// more than one stage and a bumped revision in it.
async fn populate_with(
    db: &Db,
    first: u8,
    n: u8,
    first_stage: &ActionStage,
) -> Result<Vec<OperationId>, Error> {
    let mut clients = Vec::new();
    for i in first..first + n {
        let client = OperationId::default();
        let sub = db
            .add_action(client.clone(), action_info(i), Duration::from_secs(60))
            .await?;
        if i == first {
            let mut action = sub.borrow().await?;
            let mut state = action.state().as_ref().clone();
            state.stage = first_stage.clone();
            action.worker_set_state(Arc::new(state), SystemTime::now());
            db.update_awaited_action(action).await?;
        }
        clients.push(client);
    }
    Ok(clients)
}

/// [`populate_with`], the first action completed: a restart must give back
/// exactly this state (nothing in it is executing, which a restart requeues).
async fn populate(db: &Db, first: u8, n: u8) -> Result<Vec<OperationId>, Error> {
    populate_with(
        db,
        first,
        n,
        &ActionStage::Completed(nativelink_util::action_messages::ActionResult::default()),
    )
    .await
}

async fn state_of(db: &Db) -> AppliedState {
    db.applied_state_for_test().await
}

#[nativelink_test]
async fn restart_rebuilds_state_from_the_log_alone() -> Result<(), Error> {
    let dir = tempfile::tempdir()
        .map_err(|e| Error::from_std_err(nativelink_error::Code::Internal, &e))?;
    let (before, clients) = {
        let db = open(dir.path()).await?;
        let clients = populate(&db, 1, 3).await?;
        let before = state_of(&db).await;
        db.shutdown().await?;
        (before, clients)
    };
    let db = open(dir.path()).await?;
    let after = state_of(&db).await;
    assert_eq!(before.canonical_json(), after.canonical_json());
    assert_eq!(after.operations.len(), 3);
    // Clients can find their operations again after the restart.
    for client in &clients {
        assert!(db.get_awaited_action_by_id(client).await?.is_some());
    }
    // And the restarted node keeps accepting writes.
    populate(&db, 10, 1).await?;
    assert_eq!(state_of(&db).await.operations.len(), 4);
    Ok(())
}

#[nativelink_test]
async fn restart_rebuilds_state_from_snapshot_plus_log_tail() -> Result<(), Error> {
    let dir = tempfile::tempdir()
        .map_err(|e| Error::from_std_err(nativelink_error::Code::Internal, &e))?;
    let before = {
        let db = open(dir.path()).await?;
        populate(&db, 1, 2).await?;
        db.trigger_snapshot().await?;
        // Entries after the snapshot must be replayed on top of it.
        populate(&db, 20, 2).await?;
        let before = state_of(&db).await;
        db.shutdown().await?;
        before
    };
    assert!(dir.path().join("snapshot").join("snapshot.json").exists());
    let db = open(dir.path()).await?;
    let after = state_of(&db).await;
    assert_eq!(before.canonical_json(), after.canonical_json());
    assert_eq!(after.operations.len(), 4);
    Ok(())
}

#[nativelink_test]
async fn restart_after_a_torn_final_write_keeps_every_acknowledged_entry() -> Result<(), Error> {
    let dir = tempfile::tempdir()
        .map_err(|e| Error::from_std_err(nativelink_error::Code::Internal, &e))?;
    let before = {
        let db = open(dir.path()).await?;
        populate(&db, 1, 3).await?;
        let before = state_of(&db).await;
        db.shutdown().await?;
        before
    };
    // A crash in the middle of the next append leaves a partial frame.
    let mut log = OpenOptions::new()
        .append(true)
        .open(dir.path().join("log").join("raft.log"))
        .map_err(|e| Error::from_std_err(nativelink_error::Code::Internal, &e))?;
    log.write_all(&[0x40, 0, 0, 0, 0xde, 0xad])
        .map_err(|e| Error::from_std_err(nativelink_error::Code::Internal, &e))?;
    drop(log);

    let db = open(dir.path()).await?;
    assert_eq!(
        before.canonical_json(),
        state_of(&db).await.canonical_json()
    );
    populate(&db, 30, 1).await?;
    assert_eq!(state_of(&db).await.operations.len(), 4);
    Ok(())
}

/// Client keepalives are this replica's own observations and are not in the
/// log, so a restart must not judge restored clients by the keepalive they
/// had before it: the scheduler would time out every operation whose client
/// is reconnecting (found by rechaos restart-chaos: `WaitExecution` after a
/// restart got `NOT_FOUND` because its own lookup timed the operation out).
#[nativelink_test]
async fn restart_gives_restored_clients_a_fresh_keepalive() -> Result<(), Error> {
    let dir = tempfile::tempdir()
        .map_err(|e| Error::from_std_err(nativelink_error::Code::Internal, &e))?;
    let client = OperationId::default();
    let before = {
        let db = open(dir.path()).await?;
        let sub = db
            .add_action(client.clone(), action_info(1), Duration::from_secs(60))
            .await?;
        let before = sub.borrow().await?.last_client_keepalive_timestamp();
        db.shutdown().await?;
        before
    };
    // The process is down for a while.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let reopened_at = SystemTime::now();
    let db = open(dir.path()).await?;
    let sub = db
        .get_awaited_action_by_id(&client)
        .await?
        .ok_or_else(|| nativelink_error::make_err!(nativelink_error::Code::NotFound, "lost"))?;
    let after = sub.borrow().await?.last_client_keepalive_timestamp();
    assert!(
        after >= reopened_at && after > before,
        "restored client keepalive {after:?} must be from after the restart ({reopened_at:?})",
    );
    Ok(())
}

/// Worker streams die with the scheduler process, so a restart requeues what
/// was executing at once instead of waiting out the executing timeout (found
/// by rechaos restart-chaos: 48 actions each sat 60s after restarts).
#[nativelink_test]
async fn restart_requeues_what_was_executing() -> Result<(), Error> {
    let dir = tempfile::tempdir()
        .map_err(|e| Error::from_std_err(nativelink_error::Code::Internal, &e))?;
    let clients = {
        let db = open(dir.path()).await?;
        let clients = populate_with(&db, 1, 2, &ActionStage::Executing).await?;
        assert_eq!(
            state_of(&db).await.sorted.executing.len(),
            1,
            "one action executing before the restart"
        );
        db.shutdown().await?;
        clients
    };
    let db = open(dir.path()).await?;
    let state = state_of(&db).await;
    assert!(state.sorted.executing.is_empty(), "nothing left executing");
    assert_eq!(state.sorted.queued.len(), 2, "the executing action is queued again");
    let executing_client = &clients[0];
    let action = db
        .get_awaited_action_by_id(executing_client)
        .await?
        .ok_or_else(|| nativelink_error::make_err!(nativelink_error::Code::NotFound, "lost"))?
        .borrow()
        .await?;
    assert!(matches!(action.state().stage, ActionStage::Queued));
    assert!(
        action.worker_id_public().is_none(),
        "no worker left assigned"
    );
    assert_eq!(
        action.attempts, 0,
        "a scheduler restart is not the action's failure"
    );
    Ok(())
}

/// A leader whose clock is behind the log's time stamps commands in the past.
/// Apply clamps them, so log time never moves backwards and nothing derived
/// from it (leases, deadlines) regresses (#2900 class).
#[test]
fn log_time_never_moves_backwards() {
    let mut state = AppliedState::default();
    let client = OperationId::default();
    let operation_id = OperationId::default();
    state.apply(Command::AddAction {
        client_operation_id: client,
        operation_id: operation_id.clone(),
        action_info: action_info(1),
        now_unix_nanos: 1_000,
        origin_metadata: None,
    });
    assert_eq!(state.log_now, 1_000);
    // A new leader, 900ns behind, expires leases older than 50ns.
    state.apply(Command::Expire {
        now_unix_nanos: 100,
        lease_timeout_nanos: 50,
    });
    assert_eq!(state.log_now, 1_000, "log time must not regress");
    assert_eq!(state.leases.get(&operation_id), Some(&1_000));
    // A later, correct timestamp advances it again.
    state.apply(Command::Expire {
        now_unix_nanos: 2_000,
        lease_timeout_nanos: 5_000,
    });
    assert_eq!(state.log_now, 2_000);
}

/// Not a pass/fail test: prints add+update throughput for the raft backend
/// durable, the raft backend in memory, and the memory backend, so the cost
/// of durability is measured rather than guessed.
/// `cargo test --release -p nativelink-scheduler --test raft_durability_test -- --ignored --nocapture`
#[nativelink_test]
#[ignore = "benchmark"]
async fn throughput_by_backend() -> Result<(), Error> {
    use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
    const N: u32 = 2000;
    async fn one(db: &impl AwaitedActionDb, i: u32) -> Result<(), Error> {
        let mut info = action_info(0).as_ref().clone();
        info.unique_qualifier = ActionUniqueQualifier::Uncacheable(ActionUniqueKey {
            execution_scope: None,
            instance_name: "bench".to_string(),
            digest_function: DigestHasherFunc::Sha256,
            digest: DigestInfo::new([1; 32], u64::from(i)),
        });
        let sub = db
            .add_action(
                OperationId::default(),
                Arc::new(info),
                Duration::from_secs(60),
            )
            .await?;
        let mut action = sub.borrow().await?;
        let mut state = action.state().as_ref().clone();
        state.stage = ActionStage::Executing;
        action.worker_set_state(Arc::new(state), SystemTime::now());
        db.update_awaited_action(action).await
    }
    async fn drive_concurrent(db: &impl AwaitedActionDb, label: &str) -> Result<(), Error> {
        const WRITERS: u32 = 64;
        let start = std::time::Instant::now();
        let writers = (0..WRITERS).map(|w| async move {
            for i in 0..N / WRITERS {
                one(db, 1_000_000 + w * N + i).await?;
            }
            Ok::<(), Error>(())
        });
        for result in futures::future::join_all(writers).await {
            result?;
        }
        let secs = start.elapsed().as_secs_f64();
        eprintln!(
            "{label:>14}: {N} add+update by {WRITERS} writers in {secs:.2}s = {:.0} ops/s",
            f64::from(2 * N) / secs
        );
        Ok(())
    }
    async fn drive(db: &impl AwaitedActionDb, label: &str) -> Result<(), Error> {
        drive_concurrent(db, label).await?;
        let start = std::time::Instant::now();
        for i in 0..N {
            let mut info = action_info(0).as_ref().clone();
            info.unique_qualifier = ActionUniqueQualifier::Uncacheable(ActionUniqueKey {
                execution_scope: None,
                instance_name: "bench".to_string(),
                digest_function: DigestHasherFunc::Sha256,
                digest: DigestInfo::new([0; 32], u64::from(i)),
            });
            let sub = db
                .add_action(
                    OperationId::default(),
                    Arc::new(info),
                    Duration::from_secs(60),
                )
                .await?;
            let mut action = sub.borrow().await?;
            let mut state = action.state().as_ref().clone();
            state.stage = ActionStage::Executing;
            action.worker_set_state(Arc::new(state), SystemTime::now());
            db.update_awaited_action(action).await?;
        }
        let secs = start.elapsed().as_secs_f64();
        eprintln!(
            "{label:>14}: {N} add+update in {secs:.2}s = {:.0} ops/s",
            f64::from(2 * N) / secs
        );
        Ok(())
    }
    let now_fn: fn() -> SystemTime = SystemTime::now;
    let dir = tempfile::tempdir()
        .map_err(|e| Error::from_std_err(nativelink_error::Code::Internal, &e))?;
    drive(&open(dir.path()).await?, "raft durable").await?;
    drive(
        &Db::open(RaftDbConfig::default(), Arc::new(Notify::new()), now_fn).await?,
        "raft in-memory",
    )
    .await?;
    drive(
        &memory_awaited_action_db_factory(3600, &Arc::new(Notify::new()), now_fn),
        "memory",
    )
    .await?;
    Ok(())
}
