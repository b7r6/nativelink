// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
//
// SPIKE PROTOTYPE: a Postgres-backed `AwaitedActionDb`.
//
// Thesis: relocate the scheduler's consistency-bearing state into Postgres
// so the unversioned-write / check-then-act-across-await race classes are
// structurally unrepresentable:
//
// - Duplicate actions for one cache key: impossible — a partial UNIQUE
//   index on `unique_qualifier WHERE cacheable AND stage non-terminal`
//   plus `INSERT ... ON CONFLICT DO NOTHING` makes concurrent add_action
//   converge on one row, decided by the database, not by a racy
//   read-then-insert.
// - Lost keepalives: impossible — keepalive timestamps live in their own
//   columns, updated by column-only UPDATEs that never touch (and are
//   never touched by) the versioned state write.
// - Stale-state clobber: impossible — every state write is
//   `UPDATE ... WHERE version = $expected`, a real CAS in one statement.
// - Double dispatch: impossible — matching claims rows with
//   `FOR UPDATE SKIP LOCKED` inside a transaction.
//
// Change notification is LISTEN/NOTIFY (row trigger) with a short-poll
// fallback so a dropped notification can only add latency, not stall.

use core::ops::Bound;
use core::time::Duration;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::stream::StreamExt;
use futures::{Stream, stream};
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use nativelink_util::action_messages::{
    ActionInfo, ActionStage, ActionState, ActionUniqueQualifier, OperationId, WorkerId,
};
use tokio::sync::broadcast;
use tokio_postgres::{AsyncMessage, Client, NoTls};

use crate::awaited_action_db::{
    AwaitedAction, AwaitedActionDb, AwaitedActionSubscriber, SortedAwaitedAction,
    SortedAwaitedActionState,
};

/// How long `changed()` waits on the NOTIFY channel before re-checking the
/// row anyway. Purely a liveness backstop; NOTIFY provides the latency.
const CHANGED_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The whole schema, idempotent. In production this would be a numbered
/// migration; for the spike it is applied on construction.
pub const MIGRATION: &str = r#"
CREATE TABLE IF NOT EXISTS awaited_actions (
    operation_id text PRIMARY KEY,
    unique_qualifier text NOT NULL,
    cacheable boolean NOT NULL,
    stage text NOT NULL,
    priority integer NOT NULL,
    insert_ts_nanos bigint NOT NULL,
    version bigint NOT NULL DEFAULT 0,
    worker_id text,
    -- The serialized AwaitedAction. The keepalive timestamps inside this
    -- blob are inert: the columns below are the source of truth and are
    -- overlaid on every read.
    state jsonb NOT NULL,
    -- CRUCIAL: keepalives are separate columns, NOT part of the versioned
    -- blob. A keepalive is a column-only UPDATE with no version bump, so a
    -- concurrent state read-modify-write can never clobber it.
    last_client_keepalive bigint NOT NULL,
    last_worker_update bigint NOT NULL
);

-- One live row per cache key: concurrent add_action of the same cacheable
-- qualifier cannot fork duplicates, by construction.
CREATE UNIQUE INDEX IF NOT EXISTS awaited_actions_active_cache_key
    ON awaited_actions (unique_qualifier)
    WHERE cacheable AND stage IN ('cache_check', 'queued', 'executing');

-- Priority-ordered matching scan.
CREATE INDEX IF NOT EXISTS awaited_actions_queue
    ON awaited_actions (priority DESC, insert_ts_nanos ASC)
    WHERE stage = 'queued';

CREATE TABLE IF NOT EXISTS client_operation_ids (
    client_operation_id text PRIMARY KEY,
    operation_id text NOT NULL REFERENCES awaited_actions (operation_id)
);

CREATE OR REPLACE FUNCTION notify_awaited_action() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('awaited_actions', NEW.operation_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS awaited_actions_notify ON awaited_actions;
CREATE TRIGGER awaited_actions_notify
    AFTER INSERT OR UPDATE ON awaited_actions
    FOR EACH ROW EXECUTE FUNCTION notify_awaited_action();
"#;

fn stage_class(stage: &ActionStage) -> &'static str {
    match stage {
        ActionStage::Unknown => "unknown",
        ActionStage::CacheCheck => "cache_check",
        ActionStage::Queued => "queued",
        ActionStage::Executing => "executing",
        ActionStage::Completed(_) | ActionStage::CompletedFromCache(_) => "completed",
    }
}

fn micros(t: SystemTime) -> i64 {
    i64::try_from(
        t.duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros(),
    )
    .unwrap_or(i64::MAX)
}

fn from_micros(us: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(u64::try_from(us).unwrap_or(0))
}

fn pg_err(e: tokio_postgres::Error) -> Error {
    make_err!(Code::Internal, "postgres error: {e}")
}

struct Inner {
    /// Shared client used for all pipelined statements (tokio-postgres
    /// clients take `&self` for queries).
    client: Client,
    /// Connection string, kept so transactional paths (claim) can open
    /// their own short-lived connection. A real impl would pool.
    conn_str: String,
    /// Fan-out of NOTIFY payloads (operation ids).
    notify_tx: broadcast::Sender<String>,
}

impl Inner {
    /// Fetch a row and rebuild the AwaitedAction with version and the
    /// keepalive COLUMNS overlaid (columns win over the blob, always).
    async fn fetch(&self, operation_id: &OperationId) -> Result<Option<AwaitedAction>, Error> {
        let rows = self
            .client
            .query(
                "SELECT state::text, version, last_client_keepalive, last_worker_update \
                 FROM awaited_actions WHERE operation_id = $1",
                &[&operation_id.to_string()],
            )
            .await
            .map_err(pg_err)?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let state_json: String = row.get(0);
        let version: i64 = row.get(1);
        let ck: i64 = row.get(2);
        let wk: i64 = row.get(3);
        let mut action: AwaitedAction = serde_json::from_str(&state_json)
            .map_err(|e| make_input_err!("decoding awaited action: {e}"))?;
        action.set_version(version);
        action.update_client_keep_alive(from_micros(ck));
        action.worker_keep_alive(from_micros(wk));
        Ok(Some(action))
    }
}

pub struct PostgresAwaitedActionDb {
    inner: Arc<Inner>,
    // Keep the listener client alive; dropping it closes the LISTEN
    // connection.
    _listen_client: Client,
}

impl core::fmt::Debug for PostgresAwaitedActionDb {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PostgresAwaitedActionDb").finish()
    }
}

impl MetricsComponent for PostgresAwaitedActionDb {
    fn publish(
        &self,
        _kind: MetricKind,
        _field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(MetricPublishKnownKindData::Component)
    }
}

impl PostgresAwaitedActionDb {
    pub async fn new(conn_str: &str) -> Result<Self, Error> {
        // Main client.
        let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
            .await
            .map_err(pg_err)?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("postgres connection error: {e}");
            }
        });
        client.batch_execute(MIGRATION).await.map_err(pg_err)?;

        // Dedicated LISTEN connection; its poll loop feeds the broadcast.
        let (listen_client, mut listen_conn) = tokio_postgres::connect(conn_str, NoTls)
            .await
            .map_err(pg_err)?;
        let (notify_tx, _) = broadcast::channel(4096);
        let tx = notify_tx.clone();
        tokio::spawn(async move {
            let mut messages =
                stream::poll_fn(move |cx| listen_conn.poll_message(cx));
            while let Some(msg) = messages.next().await {
                match msg {
                    Ok(AsyncMessage::Notification(n)) => {
                        drop(tx.send(n.payload().to_string()));
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!("postgres listen connection error: {e}");
                        break;
                    }
                }
            }
        });
        listen_client
            .batch_execute("LISTEN awaited_actions;")
            .await
            .map_err(pg_err)?;

        Ok(Self {
            inner: Arc::new(Inner {
                client,
                conn_str: conn_str.to_string(),
                notify_tx,
            }),
            _listen_client: listen_client,
        })
    }

    fn subscriber(&self, operation_id: OperationId) -> PostgresAwaitedActionSubscriber {
        PostgresAwaitedActionSubscriber {
            inner: self.inner.clone(),
            operation_id,
            notify_rx: self.inner.notify_tx.subscribe(),
            last_seen: None,
        }
    }

    /// Column-only client keepalive: never bumps `version`, never touches
    /// `state`. A concurrent versioned state write cannot clobber it and it
    /// cannot clobber a state write — they are different columns.
    pub async fn client_keep_alive(
        &self,
        operation_id: &OperationId,
        now: SystemTime,
    ) -> Result<(), Error> {
        self.inner
            .client
            .execute(
                "UPDATE awaited_actions SET last_client_keepalive = $2 WHERE operation_id = $1",
                &[&operation_id.to_string(), &micros(now)],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    /// Column-only worker keepalive; same guarantee as `client_keep_alive`.
    pub async fn worker_keep_alive(
        &self,
        operation_id: &OperationId,
        now: SystemTime,
    ) -> Result<(), Error> {
        self.inner
            .client
            .execute(
                "UPDATE awaited_actions SET last_worker_update = $2 WHERE operation_id = $1",
                &[&operation_id.to_string(), &micros(now)],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    /// Ground truth for the keepalive columns (test observability).
    pub async fn get_keepalives(
        &self,
        operation_id: &OperationId,
    ) -> Result<(SystemTime, SystemTime), Error> {
        let row = self
            .inner
            .client
            .query_one(
                "SELECT last_client_keepalive, last_worker_update \
                 FROM awaited_actions WHERE operation_id = $1",
                &[&operation_id.to_string()],
            )
            .await
            .map_err(pg_err)?;
        Ok((from_micros(row.get(0)), from_micros(row.get(1))))
    }

    /// The canonical no-double-dispatch job queue: claim the best queued
    /// action for `worker_id` with `FOR UPDATE SKIP LOCKED` in one
    /// transaction. Two concurrent matchers can never claim the same row.
    ///
    /// This is beyond the current trait surface (the trait makes matchers
    /// scan `get_range_of_actions` and CAS one by one); it is the shape the
    /// matching path wants once the DB is transactional.
    pub async fn claim_next_queued(
        &self,
        worker_id: &WorkerId,
        now: SystemTime,
    ) -> Result<Option<AwaitedAction>, Error> {
        // Transactions need an exclusive client; open a short-lived
        // connection (a real impl pools these).
        let (mut client, connection) = tokio_postgres::connect(&self.inner.conn_str, NoTls)
            .await
            .map_err(pg_err)?;
        let conn_task = tokio::spawn(connection);
        let result = async {
            let txn = client.transaction().await.map_err(pg_err)?;
            let rows = txn
                .query(
                    "SELECT operation_id, state::text, version, \
                            last_client_keepalive, last_worker_update \
                     FROM awaited_actions WHERE stage = 'queued' \
                     ORDER BY priority DESC, insert_ts_nanos ASC \
                     LIMIT 1 FOR UPDATE SKIP LOCKED",
                    &[],
                )
                .await
                .map_err(pg_err)?;
            let Some(row) = rows.first() else {
                txn.commit().await.map_err(pg_err)?;
                return Ok(None);
            };
            let op_id_str: String = row.get(0);
            let state_json: String = row.get(1);
            let version: i64 = row.get(2);
            let mut action: AwaitedAction = serde_json::from_str(&state_json)
                .map_err(|e| make_input_err!("decoding awaited action: {e}"))?;
            action.set_version(version + 1);
            action.set_worker_id(Some(worker_id.clone()), now);
            action.worker_set_state(
                Arc::new(ActionState {
                    stage: ActionStage::Executing,
                    client_operation_id: action.state().client_operation_id.clone(),
                    action_digest: action.state().action_digest,
                    last_transition_timestamp: now,
                }),
                now,
            );
            let json = serde_json::to_string(&action)
                .map_err(|e| make_input_err!("encoding awaited action: {e}"))?;
            txn.execute(
                "UPDATE awaited_actions SET stage = 'executing', worker_id = $2, \
                        version = version + 1, state = $3::text::jsonb, last_worker_update = $4 \
                 WHERE operation_id = $1",
                &[&op_id_str, &worker_id.to_string(), &json, &micros(now)],
            )
            .await
            .map_err(pg_err)?;
            txn.commit().await.map_err(pg_err)?;
            Ok(Some(action))
        }
        .await;
        drop(client);
        conn_task.abort();
        result
    }
}

pub struct PostgresAwaitedActionSubscriber {
    inner: Arc<Inner>,
    operation_id: OperationId,
    notify_rx: broadcast::Receiver<String>,
    /// (version, client_keepalive_micros, worker_update_micros) last
    /// returned; `changed()` resolves when any of the three moves.
    last_seen: Option<(i64, i64, i64)>,
}

impl core::fmt::Debug for PostgresAwaitedActionSubscriber {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PostgresAwaitedActionSubscriber")
            .field("operation_id", &self.operation_id)
            .field("last_seen", &self.last_seen)
            .finish()
    }
}

fn snapshot_of(action: &AwaitedAction) -> (i64, i64, i64) {
    (
        action.version(),
        micros(action.last_client_keepalive_timestamp()),
        micros(action.last_worker_updated_timestamp()),
    )
}

impl AwaitedActionSubscriber for PostgresAwaitedActionSubscriber {
    async fn changed(&mut self) -> Result<AwaitedAction, Error> {
        // Establish the baseline on first call.
        if self.last_seen.is_none() {
            let action = self
                .inner
                .fetch(&self.operation_id)
                .await?
                .err_tip(|| "Action disappeared in changed()")?;
            self.last_seen = Some(snapshot_of(&action));
        }
        loop {
            let action = self
                .inner
                .fetch(&self.operation_id)
                .await?
                .err_tip(|| "Action disappeared in changed()")?;
            let snap = snapshot_of(&action);
            if Some(snap) != self.last_seen {
                self.last_seen = Some(snap);
                return Ok(action);
            }
            // Wait for a NOTIFY about this operation, or fall back to the
            // short poll. A missed/foreign NOTIFY only costs one re-check.
            let wanted = self.operation_id.to_string();
            let wait = async {
                loop {
                    match self.notify_rx.recv().await {
                        Ok(id) if id == wanted => return,
                        Ok(_) => {}
                        // Lagged or closed: degrade to polling.
                        Err(_) => return,
                    }
                }
            };
            tokio::select! {
                () = wait => {}
                () = tokio::time::sleep(CHANGED_POLL_INTERVAL) => {}
            }
        }
    }

    async fn borrow(&self) -> Result<AwaitedAction, Error> {
        self.inner
            .fetch(&self.operation_id)
            .await?
            .err_tip(|| format!("Action {} not found in borrow()", self.operation_id))
    }
}

impl AwaitedActionDb for PostgresAwaitedActionDb {
    type Subscriber = PostgresAwaitedActionSubscriber;

    async fn get_awaited_action_by_id(
        &self,
        client_operation_id: &OperationId,
    ) -> Result<Option<Self::Subscriber>, Error> {
        let rows = self
            .inner
            .client
            .query(
                "SELECT operation_id FROM client_operation_ids WHERE client_operation_id = $1",
                &[&client_operation_id.to_string()],
            )
            .await
            .map_err(pg_err)?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let op_id: String = row.get(0);
        Ok(Some(self.subscriber(OperationId::from(op_id.as_str()))))
    }

    async fn get_all_awaited_actions(
        &self,
    ) -> Result<impl Stream<Item = Result<Self::Subscriber, Error>> + Send, Error> {
        let rows = self
            .inner
            .client
            .query("SELECT operation_id FROM awaited_actions", &[])
            .await
            .map_err(pg_err)?;
        let subscribers: Vec<Result<Self::Subscriber, Error>> = rows
            .iter()
            .map(|row| {
                let op_id: String = row.get(0);
                Ok(self.subscriber(OperationId::from(op_id.as_str())))
            })
            .collect();
        Ok(stream::iter(subscribers))
    }

    async fn get_by_operation_id(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<Self::Subscriber>, Error> {
        Ok(self
            .inner
            .fetch(operation_id)
            .await?
            .map(|_| self.subscriber(operation_id.clone())))
    }

    async fn get_range_of_actions(
        &self,
        state: SortedAwaitedActionState,
        start: Bound<SortedAwaitedAction>,
        end: Bound<SortedAwaitedAction>,
        desc: bool,
    ) -> Result<impl Stream<Item = Result<Self::Subscriber, Error>> + Send, Error> {
        let stage = match state {
            SortedAwaitedActionState::CacheCheck => "cache_check",
            SortedAwaitedActionState::Queued => "queued",
            SortedAwaitedActionState::Executing => "executing",
            SortedAwaitedActionState::Completed => "completed",
        };
        // Spike simplification: pull the stage's rows ordered by the queue
        // index and apply the sort-key bounds in memory. Production would
        // translate the bounds to (priority, insert_ts_nanos, operation_id)
        // keyset predicates.
        let rows = self
            .inner
            .client
            .query(
                "SELECT operation_id, state::text FROM awaited_actions WHERE stage = $1 \
                 ORDER BY priority DESC, insert_ts_nanos ASC, operation_id ASC",
                &[&stage],
            )
            .await
            .map_err(pg_err)?;
        let mut sorted: Vec<SortedAwaitedAction> = Vec::with_capacity(rows.len());
        for row in &rows {
            let state_json: String = row.get(1);
            let action: AwaitedAction = serde_json::from_str(&state_json)
                .map_err(|e| make_input_err!("decoding awaited action: {e}"))?;
            sorted.push(SortedAwaitedAction::from(&action));
        }
        sorted.sort();
        let in_bounds = |item: &SortedAwaitedAction| {
            (match &start {
                Bound::Included(s) => item >= s,
                Bound::Excluded(s) => item > s,
                Bound::Unbounded => true,
            }) && (match &end {
                Bound::Included(e) => item <= e,
                Bound::Excluded(e) => item < e,
                Bound::Unbounded => true,
            })
        };
        let mut filtered: Vec<SortedAwaitedAction> =
            sorted.into_iter().filter(in_bounds).collect();
        if desc {
            filtered.reverse();
        }
        let subscribers: Vec<Result<Self::Subscriber, Error>> = filtered
            .into_iter()
            .map(|item| Ok(self.subscriber(item.operation_id)))
            .collect();
        Ok(stream::iter(subscribers))
    }

    /// Optimistic CAS: the incoming action carries the version the caller
    /// read; the write is `WHERE version = $expected` and stores
    /// `$expected + 1`. Zero rows updated means someone else won — a real
    /// transactional guard, not a hand-rolled Redis version dance.
    ///
    /// Deliberately never writes the keepalive columns: those belong to
    /// the keepalive paths and cannot be clobbered from here.
    async fn update_awaited_action(
        &self,
        mut new_awaited_action: AwaitedAction,
    ) -> Result<(), Error> {
        let expected = new_awaited_action.version();
        new_awaited_action.set_version(expected + 1);
        let json = serde_json::to_string(&new_awaited_action)
            .map_err(|e| make_input_err!("encoding awaited action: {e}"))?;
        let updated = self
            .inner
            .client
            .execute(
                "UPDATE awaited_actions SET version = $2, stage = $3, worker_id = $4, \
                        state = $5::text::jsonb \
                 WHERE operation_id = $1 AND version = $6",
                &[
                    &new_awaited_action.operation_id().to_string(),
                    &(expected + 1),
                    &stage_class(&new_awaited_action.state().stage),
                    &new_awaited_action.worker_id().map(ToString::to_string),
                    &json,
                    &expected,
                ],
            )
            .await
            .map_err(pg_err)?;
        if updated == 0 {
            return Err(make_err!(
                Code::Aborted,
                "Version conflict updating {}: expected version {expected}",
                new_awaited_action.operation_id(),
            ));
        }
        Ok(())
    }

    /// Insert-or-join, decided atomically by the partial unique index.
    async fn add_action(
        &self,
        client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
        _no_event_action_timeout: Duration,
    ) -> Result<Self::Subscriber, Error> {
        let qualifier = action_info.unique_qualifier.to_string();
        let cacheable = matches!(
            action_info.unique_qualifier,
            ActionUniqueQualifier::Cacheable(_)
        );
        let now = SystemTime::now();
        let insert_nanos = i64::try_from(
            action_info
                .insert_timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);

        // Bounded retry: the joined row can complete between our failed
        // insert and the join lookup; on the next pass the insert wins.
        for _ in 0..8 {
            let operation_id = OperationId::default();
            let action =
                AwaitedAction::new(operation_id.clone(), action_info.clone(), now);
            let json = serde_json::to_string(&action)
                .map_err(|e| make_input_err!("encoding awaited action: {e}"))?;
            let rows = self
                .inner
                .client
                .query(
                    "INSERT INTO awaited_actions \
                       (operation_id, unique_qualifier, cacheable, stage, priority, \
                        insert_ts_nanos, version, worker_id, state, \
                        last_client_keepalive, last_worker_update) \
                     VALUES ($1, $2, $3, 'queued', $4, $5, 0, NULL, $6::text::jsonb, $7, $7) \
                     ON CONFLICT (unique_qualifier) \
                       WHERE cacheable AND stage IN ('cache_check', 'queued', 'executing') \
                       DO NOTHING \
                     RETURNING operation_id",
                    &[
                        &operation_id.to_string(),
                        &qualifier,
                        &cacheable,
                        &action_info.priority,
                        &insert_nanos,
                        &json,
                        &micros(now),
                    ],
                )
                .await
                .map_err(pg_err)?;
            let resolved_op_id = if rows.is_empty() {
                // Lost the insert race (or a live action already exists):
                // join the winner.
                let existing = self
                    .inner
                    .client
                    .query(
                        "SELECT operation_id FROM awaited_actions \
                         WHERE unique_qualifier = $1 AND cacheable \
                           AND stage IN ('cache_check', 'queued', 'executing')",
                        &[&qualifier],
                    )
                    .await
                    .map_err(pg_err)?;
                match existing.first() {
                    Some(row) => {
                        let id: String = row.get(0);
                        OperationId::from(id.as_str())
                    }
                    // Winner finished in the window; try inserting again.
                    None => continue,
                }
            } else {
                operation_id
            };
            self.inner
                .client
                .execute(
                    "INSERT INTO client_operation_ids (client_operation_id, operation_id) \
                     VALUES ($1, $2) \
                     ON CONFLICT (client_operation_id) \
                       DO UPDATE SET operation_id = EXCLUDED.operation_id",
                    &[&client_operation_id.to_string(), &resolved_op_id.to_string()],
                )
                .await
                .map_err(pg_err)?;
            return Ok(self.subscriber(resolved_op_id));
        }
        Err(make_err!(
            Code::Aborted,
            "add_action retry budget exhausted for {qualifier}"
        ))
    }
}
