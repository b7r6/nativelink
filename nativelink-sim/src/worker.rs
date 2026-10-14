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

//! A simulated worker that speaks the worker API directly, so the harness
//! controls every behavior: slow, stalled, crashed, or a zombie that ignores
//! kills and reports revoked attempts late.
//!
//! The protocol follows `nativelink-worker`'s `LocalWorker`: connect,
//! `ConnectionResult`, a keepalive every interval, `StartExecute` answered
//! with `ExecuteAccepted` (or `ExecuteDeclined` when full, or a re-ack for a
//! resend), then `ExecuteComplete` and `ExecuteResult` when the action ends.
//! A kill stops the attempt and reports it as an internal error, as the real
//! worker does when its process is killed. A lost connection drops every
//! running attempt (the real worker's `kill_all`) and reconnects as a new
//! session.

use core::time::Duration;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use futures::channel::mpsc;
use nativelink_proto::build::bazel::remote::execution::v2::ExecuteResponse;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::worker_api_client::WorkerApiClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ConnectWorkerRequest, ExecuteAccepted, ExecuteComplete, ExecuteDeclined, ExecuteResult,
    KeepAliveRequest, StartExecute, UpdateForScheduler, execute_declined, execute_result,
    UpdateForWorker, update_for_scheduler, update_for_worker,
};
use nativelink_proto::google::rpc::Status as RpcStatus;
use nativelink_util::action_messages::{ActionResult, ActionStage, ExecutionMetadata};
use tokio::time::Instant;

use crate::history::{Event, History, ResultId};
use crate::net::{ChannelOptions, SCHEDULER_PORT, connect};
use crate::scheduler::SCHEDULER_HOST;
use crate::workload::{INSTANCE_NAME, Workload};

/// How a worker misbehaves. Lives outside the host, so it survives the
/// worker's crashes and the fault injector can change it between steps.
#[derive(Clone, Copy, Debug, Default)]
pub struct WorkerControl {
    /// Frozen: reads and sends nothing (the connection itself stays up),
    /// as a process stopped or starved would.
    pub stalled: bool,
    /// Ignores kills and reports every attempt it started, even across
    /// reconnects.
    pub zombie: bool,
    /// Runs every action this many times longer than its duration.
    pub slow_factor: u32,
}

/// The prefix that marks a result's `execution_metadata.worker` as written by
/// a simulated worker.
const RESULT_TAG: &str = "simw";

/// What the worker writes into a result, so a client can tell which attempt
/// produced it.
pub fn encode_result_id(id: &ResultId) -> String {
    format!(
        "{RESULT_TAG};w={};s={};op={};a={}",
        id.worker, id.session, id.operation, id.attempt
    )
}

/// The inverse of [`encode_result_id`].
pub fn decode_result_id(tag: &str) -> Option<ResultId> {
    let mut parts = tag.split(';');
    if parts.next()? != RESULT_TAG {
        return None;
    }
    let mut worker = None;
    let mut session = None;
    let mut operation = None;
    let mut attempt = None;
    for part in parts {
        let (key, value) = part.split_once('=')?;
        match key {
            "w" => worker = Some(value.to_string()),
            "s" => session = value.parse().ok(),
            "op" => operation = Some(value.to_string()),
            "a" => attempt = value.parse().ok(),
            _ => return None,
        }
    }
    Some(ResultId {
        operation: operation?,
        attempt: attempt?,
        worker: worker?,
        session: session?,
    })
}

/// One simulated worker host.
#[derive(Debug)]
pub struct WorkerHost {
    pub name: String,
    pub slots: usize,
    pub keepalive: Duration,
    pub workload: Arc<Workload>,
    pub history: History,
    pub control: Arc<Mutex<WorkerControl>>,
    /// Session numbers across boots of this host.
    pub sessions: AtomicU32,
}

#[derive(Clone, Debug)]
struct Attempt {
    /// The session that started it (a zombie may report it on a later one).
    session: u32,
    ends_at: Instant,
    /// A kill for it arrived and was ignored.
    killed: bool,
}

type AttemptKey = (String, u64);

/// How one connection attempt ended.
enum SessionEnd {
    /// A session was established and then lost.
    Ended { session: u32, why: String },
    /// No session was established.
    NotConnected(String),
}

/// The outbound half of a session's stream.
struct Outbound {
    tx: mpsc::UnboundedSender<UpdateForScheduler>,
}

impl Outbound {
    fn send(&self, update: update_for_scheduler::Update) -> bool {
        self.tx
            .unbounded_send(UpdateForScheduler {
                update: Some(update),
            })
            .is_ok()
    }
}

impl WorkerHost {
    fn control(&self) -> WorkerControl {
        *self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn record(&self, event: Event) {
        self.history.record(&self.name, event);
    }

    /// One boot of the worker process: connect, serve, reconnect forever.
    pub async fn run(self: Arc<Self>) -> turmoil::Result {
        // Attempts a zombie keeps across reconnects within this boot.
        let mut carried: BTreeMap<AttemptKey, Attempt> = BTreeMap::new();
        let mut backoff = Duration::from_secs(1);
        let mut failing = false;
        loop {
            match self.session(&mut carried).await {
                SessionEnd::Ended { session, why } => {
                    let dropped = if self.control().zombie {
                        0
                    } else {
                        core::mem::take(&mut carried).len()
                    };
                    self.record(Event::WorkerDisconnected {
                        session,
                        why,
                        dropped,
                    });
                    failing = false;
                    backoff = Duration::from_secs(1);
                }
                SessionEnd::NotConnected(why) => {
                    // One event per streak of failed connection attempts.
                    if !failing {
                        self.record(Event::WorkerConnectFailed(why));
                        failing = true;
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(8));
                }
            }
            tokio::time::sleep(backoff).await;
        }
    }

    async fn wait_while_stalled(&self) {
        while self.control().stalled {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// One connection. Returns how it ended; `running` holds the attempts
    /// still running when it did.
    async fn session(&self, running: &mut BTreeMap<AttemptKey, Attempt>) -> SessionEnd {
        let session = match self.connect_session().await {
            Ok(session) => session,
            Err(why) => return SessionEnd::NotConnected(why),
        };
        let (session, outbound, mut inbound, dispatch_ack) = session;
        let why = self
            .serve(session, dispatch_ack, &outbound, &mut inbound, running)
            .await;
        SessionEnd::Ended { session, why }
    }

    async fn connect_session(
        &self,
    ) -> Result<(u32, Outbound, tonic::Streaming<UpdateForWorker>, bool), String> {
        self.wait_while_stalled().await;
        let channel = match connect(
            SCHEDULER_HOST,
            SCHEDULER_PORT,
            ChannelOptions {
                connect_timeout: Duration::from_secs(5),
                keepalive: Some((Duration::from_secs(5), Duration::from_secs(10))),
            },
        )
        .await
        {
            Ok(channel) => channel,
            Err(err) => return Err(format!("connect failed: {err}")),
        };
        let mut client = WorkerApiClient::new(channel);
        let (tx, rx) = mpsc::unbounded();
        let outbound = Outbound { tx };
        outbound.send(update_for_scheduler::Update::ConnectWorkerRequest(
            ConnectWorkerRequest {
                properties: Vec::new(),
                worker_id_prefix: format!("{}-", self.name),
                max_inflight_tasks: self.slots as u64,
                admits_when_idle: false,
            },
        ));
        let mut inbound = match client.connect_worker(rx).await {
            Ok(response) => response.into_inner(),
            Err(status) => return Err(format!("connect_worker failed: {:?}", status.code())),
        };
        let (worker_id, dispatch_ack) = match inbound.message().await {
            Ok(Some(update)) => match update.update {
                Some(update_for_worker::Update::ConnectionResult(result)) => {
                    (result.worker_id, result.dispatch_ack)
                }
                other => return Err(format!("first message was not ConnectionResult: {other:?}")),
            },
            Ok(None) => return Err("closed before ConnectionResult".to_string()),
            Err(status) => return Err(format!("error before ConnectionResult: {:?}", status.code())),
        };
        let session = self.sessions.fetch_add(1, Ordering::SeqCst) + 1;
        self.record(Event::WorkerConnected {
            session,
            worker_id,
        });
        Ok((session, outbound, inbound, dispatch_ack))
    }

    async fn serve(
        &self,
        session: u32,
        dispatch_ack: bool,
        outbound: &Outbound,
        inbound: &mut tonic::Streaming<UpdateForWorker>,
        running: &mut BTreeMap<AttemptKey, Attempt>,
    ) -> String {

        let mut keepalive = tokio::time::interval(self.keepalive);
        loop {
            if self.control().stalled {
                self.wait_while_stalled().await;
            }
            // Report whatever finished, including during a stall.
            if let Err(why) = self.finish_due(outbound, running) {
                return why;
            }
            let next_end = running.values().map(|attempt| attempt.ends_at).min();
            let wake = next_end.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
            tokio::select! {
                message = inbound.message() => match message {
                    Ok(Some(update)) => {
                        if let Some(update) = update.update
                            && let Err(why) = self.handle(session, dispatch_ack, update, outbound, running)
                        {
                            return why;
                        }
                    }
                    Ok(None) => return "stream closed".to_string(),
                    Err(status) => return format!("stream error: {:?}", status.code()),
                },
                _ = keepalive.tick() => {
                    if !self.control().stalled
                        && !outbound.send(update_for_scheduler::Update::KeepAliveRequest(
                            KeepAliveRequest { load: None },
                        ))
                    {
                        return "send failed".to_string();
                    }
                }
                () = tokio::time::sleep_until(wake) => {}
            }
        }
    }

    fn handle(
        &self,
        session: u32,
        dispatch_ack: bool,
        update: update_for_worker::Update,
        outbound: &Outbound,
        running: &mut BTreeMap<AttemptKey, Attempt>,
    ) -> Result<(), String> {
        match update {
            update_for_worker::Update::StartAction(start) => {
                self.start(session, dispatch_ack, &start, outbound, running)
            }
            update_for_worker::Update::KillOperationRequest(kill) => {
                self.kill(session, &kill.operation_id, kill.attempt, outbound, running)
            }
            update_for_worker::Update::Disconnect(()) => Err("scheduler said disconnect".into()),
            update_for_worker::Update::ConnectionResult(_)
            | update_for_worker::Update::KeepAlive(()) => Ok(()),
        }
    }

    fn start(
        &self,
        session: u32,
        dispatch_ack: bool,
        start: &StartExecute,
        outbound: &Outbound,
        running: &mut BTreeMap<AttemptKey, Attempt>,
    ) -> Result<(), String> {
        let operation = start.operation_id.clone();
        let attempt = start.attempt;
        let key = (operation.clone(), attempt);
        let accept = || {
            if dispatch_ack {
                outbound.send(update_for_scheduler::Update::ExecuteAccepted(
                    ExecuteAccepted {
                        operation_id: operation.clone(),
                        attempt,
                    },
                ))
            } else {
                true
            }
        };
        if running.contains_key(&key) {
            self.record(Event::DuplicateStart {
                session,
                operation: operation.clone(),
                attempt,
            });
            return if accept() {
                Ok(())
            } else {
                Err("send failed".into())
            };
        }
        let live = running.len();
        if live >= self.slots {
            let here = running.values().filter(|a| a.session == session);
            let unacked_kills = here.clone().filter(|a| a.killed).count();
            let live_here = here.count() - unacked_kills;
            self.record(Event::OverCapacity {
                session,
                operation: operation.clone(),
                attempt,
                running: live,
                slots: self.slots,
                live_here,
                unacked_kills,
            });
            let sent = outbound.send(update_for_scheduler::Update::ExecuteDeclined(
                ExecuteDeclined {
                    operation_id: operation,
                    reason: execute_declined::Reason::AtCapacity.into(),
                    detail: "simulated worker is full".to_string(),
                    needed_kb: 0,
                    free_kb: 0,
                    attempt,
                },
            ));
            return if sent {
                Ok(())
            } else {
                Err("send failed".into())
            };
        }
        let action = start
            .execute_request
            .as_ref()
            .and_then(|request| request.action_digest.as_ref())
            .and_then(|digest| self.workload.by_digest.get(&digest.hash))
            .map(|index| &self.workload.actions[*index]);
        let duration = action.map_or(Duration::from_secs(5), |action| action.duration);
        let label = action.map_or_else(|| "?".to_string(), |action| action.label.clone());
        let factor = self.control().slow_factor.max(1);
        self.record(Event::StartExecute {
            session,
            operation: operation.clone(),
            attempt,
            running: live,
            action: label,
        });
        if !accept() {
            return Err("send failed".into());
        }
        running.insert(
            key,
            Attempt {
                session,
                ends_at: Instant::now() + duration * factor,
                killed: false,
            },
        );
        Ok(())
    }

    fn kill(
        &self,
        session: u32,
        operation: &str,
        attempt: u64,
        outbound: &Outbound,
        running: &mut BTreeMap<AttemptKey, Attempt>,
    ) -> Result<(), String> {
        // Attempt 0 is a legacy kill of whatever runs the operation.
        let keys: Vec<AttemptKey> = running
            .keys()
            .filter(|(op, a)| op == operation && (attempt == 0 || *a == attempt))
            .cloned()
            .collect();
        let zombie = self.control().zombie;
        self.record(Event::Kill {
            session,
            operation: operation.to_string(),
            attempt,
            was_running: !keys.is_empty(),
            obeyed: !zombie,
        });
        for key in keys {
            if zombie {
                if let Some(attempt) = running.get_mut(&key) {
                    attempt.killed = true;
                }
                continue;
            }
            running.remove(&key);
            self.record(Event::ResultSent {
                session,
                operation: key.0.clone(),
                attempt: key.1,
                killed_before: true,
                ok: false,
            });
            let sent = outbound.send(update_for_scheduler::Update::ExecuteResult(ExecuteResult {
                instance_name: INSTANCE_NAME.to_string(),
                operation_id: key.0,
                result: Some(execute_result::Result::InternalError(RpcStatus {
                    code: tonic::Code::Aborted as i32,
                    message: "simulated worker: action killed".to_string(),
                    details: Vec::new(),
                })),
                resource_usage: None,
                attempt: key.1,
            }));
            if !sent {
                return Err("send failed".into());
            }
        }
        Ok(())
    }

    /// Report every attempt whose time is up.
    fn finish_due(
        &self,
        outbound: &Outbound,
        running: &mut BTreeMap<AttemptKey, Attempt>,
    ) -> Result<(), String> {
        let now = Instant::now();
        let due: Vec<AttemptKey> = running
            .iter()
            .filter(|(_, attempt)| attempt.ends_at <= now)
            .map(|(key, _)| key.clone())
            .collect();
        for key in due {
            let Some(attempt) = running.remove(&key) else {
                continue;
            };
            let (operation, attempt_no) = key;
            let id = ResultId {
                operation: operation.clone(),
                attempt: attempt_no,
                worker: self.name.clone(),
                session: attempt.session,
            };
            self.record(Event::ResultSent {
                session: attempt.session,
                operation: operation.clone(),
                attempt: attempt_no,
                killed_before: attempt.killed,
                ok: true,
            });
            let response: ExecuteResponse = ActionStage::Completed(ActionResult {
                exit_code: 0,
                execution_metadata: ExecutionMetadata {
                    worker: encode_result_id(&id),
                    ..ExecutionMetadata::default()
                },
                ..ActionResult::default()
            })
            .into();
            let complete = outbound.send(update_for_scheduler::Update::ExecuteComplete(
                ExecuteComplete {
                    operation_id: operation.clone(),
                    attempt: attempt_no,
                },
            ));
            let result = outbound.send(update_for_scheduler::Update::ExecuteResult(ExecuteResult {
                instance_name: INSTANCE_NAME.to_string(),
                operation_id: operation,
                result: Some(execute_result::Result::ExecuteResponse(response)),
                resource_usage: None,
                attempt: attempt_no,
            }));
            if !complete || !result {
                return Err("send failed".into());
            }
        }
        Ok(())
    }
}
