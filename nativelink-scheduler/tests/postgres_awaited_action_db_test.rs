// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
//
// SPIKE: integration tests proving the Postgres-backed AwaitedActionDb
// makes whole race classes structurally impossible. Each test stands up a
// throwaway Postgres (initdb + pg_ctl in a tempdir, random port) and tears
// it down on drop.
#![cfg(feature = "postgres")]

mod utils {
    pub(crate) mod scheduler_utils;
}

use core::time::Duration;
use std::collections::HashSet;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_scheduler::awaited_action_db::{
    AwaitedAction, AwaitedActionDb, AwaitedActionSubscriber,
};
use nativelink_scheduler::postgres_awaited_action_db::PostgresAwaitedActionDb;
use nativelink_util::action_messages::{ActionStage, ActionState, OperationId, WorkerId};
use nativelink_util::common::DigestInfo;
use pretty_assertions::assert_eq;
use utils::scheduler_utils::make_base_action_info;

const NO_EVENT_TIMEOUT: Duration = Duration::from_secs(60);

/// Throwaway Postgres instance in a tempdir. Killed and deleted on drop.
struct TempPostgres {
    dir: PathBuf,
    pub conn_str: String,
}

impl TempPostgres {
    fn start() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "nl-pg-spike-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let data = dir.join("data");
        std::fs::create_dir_all(&data).unwrap();
        let out = Command::new("initdb")
            .args(["-D"])
            .arg(&data)
            .args(["-U", "postgres", "-A", "trust", "--no-sync"])
            .output()
            .expect("initdb must be on PATH");
        assert!(
            out.status.success(),
            "initdb failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        // Grab a free port.
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let out = Command::new("pg_ctl")
            .args(["-D"])
            .arg(&data)
            .args(["-w", "-l"])
            .arg(dir.join("pg.log"))
            .arg("-o")
            .arg(format!(
                "-p {port} -c listen_addresses=127.0.0.1 -c unix_socket_directories='{}' -c fsync=off",
                dir.display()
            ))
            .arg("start")
            .output()
            .expect("pg_ctl must be on PATH");
        assert!(
            out.status.success(),
            "pg_ctl start failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        Self {
            dir: dir.clone(),
            conn_str: format!("host=127.0.0.1 port={port} user=postgres dbname=postgres"),
        }
    }
}

impl Drop for TempPostgres {
    fn drop(&mut self) {
        drop(
            Command::new("pg_ctl")
                .args(["-D"])
                .arg(self.dir.join("data"))
                .args(["-m", "immediate", "stop"])
                .output(),
        );
        drop(std::fs::remove_dir_all(&self.dir));
    }
}

fn make_action(digest_byte: u8, priority: i32) -> Arc<nativelink_util::action_messages::ActionInfo> {
    let mut info =
        (*make_base_action_info(SystemTime::now(), DigestInfo::new([digest_byte; 32], 123)))
            .clone();
    info.priority = priority;
    Arc::new(info)
}

async fn new_db(pg: &TempPostgres) -> PostgresAwaitedActionDb {
    PostgresAwaitedActionDb::new(&pg.conn_str)
        .await
        .expect("db connects and migrates")
}

/// (a) Two concurrent add_action of the same cache key produce exactly one
/// row, and both subscribers observe the same operation. The partial unique
/// index decides; there is no window to fork duplicates.
#[nativelink_test]
async fn concurrent_add_same_cache_key_dedups() -> Result<(), Error> {
    let pg = TempPostgres::start();
    let db = Arc::new(new_db(&pg).await);
    let info = make_action(1, 0);

    let mut handles = Vec::new();
    for i in 0..8 {
        let db = db.clone();
        let info = info.clone();
        handles.push(tokio::spawn(async move {
            db.add_action(
                OperationId::from(format!("client-{i}").as_str()),
                info,
                NO_EVENT_TIMEOUT,
            )
            .await
        }));
    }
    let mut op_ids = HashSet::new();
    for h in handles {
        let sub = h.await.unwrap()?;
        op_ids.insert(sub.borrow().await?.operation_id().clone());
    }
    assert_eq!(op_ids.len(), 1, "all adders must converge on one operation");

    // And exactly one row exists.
    let mut count = 0;
    let mut stream = core::pin::pin!(db.get_all_awaited_actions().await?);
    while let Some(item) = futures::StreamExt::next(&mut stream).await {
        item?;
        count += 1;
    }
    assert_eq!(count, 1, "exactly one awaited_actions row");
    Ok(())
}

/// (b) A stale-version update is rejected: version CAS in one UPDATE.
#[nativelink_test]
async fn stale_version_update_rejected() -> Result<(), Error> {
    let pg = TempPostgres::start();
    let db = new_db(&pg).await;
    let sub = db
        .add_action(OperationId::from("client-b"), make_action(2, 0), NO_EVENT_TIMEOUT)
        .await?;
    let action_v0 = sub.borrow().await?;
    let stale_copy = action_v0.clone();

    // First writer wins.
    db.update_awaited_action(action_v0).await?;
    // Second writer, holding the stale version, must be rejected.
    let err = db.update_awaited_action(stale_copy).await.unwrap_err();
    assert_eq!(err.code, Code::Aborted, "stale CAS must abort: {err:?}");
    Ok(())
}

/// (c) THE keepalive class: a client keepalive concurrent with a state
/// read-modify-write can never be lost, because keepalives are their own
/// columns and the state UPDATE never writes them. Run both orders and a
/// racing batch; the keepalive always survives.
#[nativelink_test]
async fn keepalive_survives_concurrent_state_update() -> Result<(), Error> {
    let pg = TempPostgres::start();
    let db = Arc::new(new_db(&pg).await);
    let sub = db
        .add_action(OperationId::from("client-c"), make_action(3, 0), NO_EVENT_TIMEOUT)
        .await?;
    let op_id = sub.borrow().await?.operation_id().clone();

    for round in 0..20 {
        // A distinctive keepalive instant for this round.
        let keepalive_time = UNIX_EPOCH + Duration::from_secs(1_000_000 + round);
        let action = sub.borrow().await?;
        let db_ka = db.clone();
        let op_ka = op_id.clone();
        let db_up = db.clone();
        // Race: versioned state RMW vs column-only keepalive.
        let (up_res, ka_res) = tokio::join!(
            async move { db_up.update_awaited_action(action).await },
            async move { db_ka.client_keep_alive(&op_ka, keepalive_time).await },
        );
        up_res?;
        ka_res?;
        let (client_ka, _worker_ka) = db.get_keepalives(&op_id).await?;
        assert_eq!(
            client_ka, keepalive_time,
            "round {round}: keepalive was clobbered by the concurrent state update"
        );
    }
    Ok(())
}

/// (d) Two concurrent matchers over N queued actions never double-claim:
/// FOR UPDATE SKIP LOCKED is the canonical no-double-dispatch queue.
#[nativelink_test]
async fn concurrent_matchers_never_double_claim() -> Result<(), Error> {
    let pg = TempPostgres::start();
    let db = Arc::new(new_db(&pg).await);
    const N: usize = 12;
    for i in 0..N {
        db.add_action(
            OperationId::from(format!("client-d-{i}").as_str()),
            make_action(10 + u8::try_from(i).unwrap(), i32::try_from(i % 3).unwrap()),
            NO_EVENT_TIMEOUT,
        )
        .await?;
    }

    let matcher = |worker: WorkerId, db: Arc<PostgresAwaitedActionDb>| async move {
        let mut claimed = Vec::new();
        loop {
            match db.claim_next_queued(&worker, SystemTime::now()).await.unwrap() {
                Some(action) => claimed.push(action.operation_id().clone()),
                None => return claimed,
            }
        }
    };
    let (a, b) = tokio::join!(
        tokio::spawn(matcher(WorkerId("matcher-a".into()), db.clone())),
        tokio::spawn(matcher(WorkerId("matcher-b".into()), db.clone())),
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    let set_a: HashSet<_> = a.iter().cloned().collect();
    let set_b: HashSet<_> = b.iter().cloned().collect();
    assert!(
        set_a.is_disjoint(&set_b),
        "double-claim detected: {:?}",
        set_a.intersection(&set_b).collect::<Vec<_>>()
    );
    assert_eq!(set_a.len() + set_b.len(), N, "every action claimed exactly once");
    Ok(())
}

/// (e) The subscriber observes updates: LISTEN/NOTIFY wake with a poll
/// fallback maps onto the trait's changed().
#[nativelink_test]
async fn subscriber_changed_observes_update() -> Result<(), Error> {
    let pg = TempPostgres::start();
    let db = Arc::new(new_db(&pg).await);
    let mut sub = db
        .add_action(OperationId::from("client-e"), make_action(40, 0), NO_EVENT_TIMEOUT)
        .await?;
    let action = sub.borrow().await?;

    let waiter = tokio::spawn(async move {
        let changed = sub.changed().await.unwrap();
        (changed.state().stage.clone(), changed)
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut updated: AwaitedAction = action.clone();
    updated.worker_set_state(
        Arc::new(ActionState {
            stage: ActionStage::Executing,
            client_operation_id: action.state().client_operation_id.clone(),
            action_digest: action.state().action_digest,
            last_transition_timestamp: SystemTime::now(),
        }),
        SystemTime::now(),
    );
    db.update_awaited_action(updated).await?;

    let (stage, changed) =
        tokio::time::timeout(Duration::from_secs(5), waiter).await.unwrap().unwrap();
    assert_eq!(stage, ActionStage::Executing);
    assert_eq!(changed.operation_id(), action.operation_id());
    Ok(())
}
