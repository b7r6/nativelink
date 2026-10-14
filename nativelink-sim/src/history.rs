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

//! The history of a run: every event a client or a worker can observe, plus
//! the faults the injector applied, in the order they happened.

use core::fmt;
use core::time::Duration;
use std::sync::{Arc, Mutex};

/// Which result an operation carried, as the simulated worker wrote it into
/// the `ActionResult` and the client read it back.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ResultId {
    /// The scheduler's operation id, as the worker saw it in `StartExecute`.
    pub operation: String,
    /// `StartExecute.attempt`.
    pub attempt: u64,
    /// The worker host that ran it.
    pub worker: String,
    /// The worker's connection (session) number on that host.
    pub session: u32,
}

impl fmt::Display for ResultId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "op={} attempt={} worker={}#{}",
            self.operation, self.attempt, self.worker, self.session
        )
    }
}

/// How an operation ended, as a client saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// `done` with an `ExecuteResponse` whose status is OK and whose result
    /// names the attempt that produced it.
    Ok(ResultId),
    /// `done` with an OK status but a result the harness cannot attribute.
    OkUnattributed(String),
    /// `done` with an error, in the operation or in the `ExecuteResponse`.
    Failed { code: i32, message: String },
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok(id) => write!(f, "ok({id})"),
            Self::OkUnattributed(what) => write!(f, "ok-unattributed({what})"),
            Self::Failed { code, message } => write!(f, "failed(code={code} \"{message}\")"),
        }
    }
}

/// One observable event.
#[derive(Clone, Debug)]
pub enum Event {
    /// The injector applied a fault, or healed one.
    Fault(String),
    /// The scheduler process started (on boot and after every restart).
    SchedulerBoot { boot: u32 },
    /// The scheduler process failed to start or stopped serving.
    SchedulerError(String),
    /// A client sent `Execute` for a job.
    Submit { job: String, digest: String },
    /// The first `Operation` of an `Execute` stream: the job now has a name.
    Acked { job: String, name: String },
    /// A client sent `WaitExecution` for a job it had a name for.
    Reattach { job: String, name: String, after: Option<Duration> },
    /// A non-final `Operation` message.
    Stage { job: String, name: String, stage: String },
    /// The final `Operation` message.
    Done { job: String, name: String, outcome: Outcome },
    /// A stream ended without a final message.
    StreamEnded { job: String, why: String },
    /// The client let go of the stream on purpose, for `for_ms`.
    Detach { job: String, for_ms: u64 },
    /// `WaitExecution` answered `NOT_FOUND`.
    NotFound { job: String, name: String, detached_for: Option<Duration> },
    /// An RPC failed with a status other than `NOT_FOUND`.
    RpcError { job: String, code: i32, message: String },
    /// The client gave up at the end of the run without a final message.
    GaveUp { job: String, last_stage: String },
    /// After `Done`, the client read the operation again.
    Confirm { job: String, name: String, seen: String },
    /// A worker connected and was given an id.
    WorkerConnected { session: u32, worker_id: String },
    /// A worker could not connect (first failure of a streak).
    WorkerConnectFailed(String),
    /// A worker's connection ended.
    WorkerDisconnected { session: u32, why: String, dropped: usize },
    /// `StartExecute` arrived.
    StartExecute {
        session: u32,
        operation: String,
        attempt: u64,
        running: usize,
        /// The workload label of the action (`a<index>`).
        action: String,
    },
    /// `StartExecute` for an attempt already running here (a resend).
    DuplicateStart { session: u32, operation: String, attempt: u64 },
    /// The worker declined because it was full. `live_here` attempts were
    /// started on this session and not killed (the scheduler believes them
    /// running), `unacked_kills` were killed but kept running (a zombie), and
    /// the rest of `running` were carried over from an earlier session.
    OverCapacity {
        session: u32,
        operation: String,
        attempt: u64,
        running: usize,
        slots: usize,
        live_here: usize,
        unacked_kills: usize,
    },
    /// `KillOperationRequest` arrived.
    Kill { session: u32, operation: String, attempt: u64, was_running: bool, obeyed: bool },
    /// The worker sent a result for an attempt.
    ResultSent { session: u32, operation: String, attempt: u64, killed_before: bool, ok: bool },
    /// The worker misbehavior controller changed this worker's behavior.
    Behavior(String),
    /// A panic anywhere in the process.
    Panic(String),
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fault(what) => write!(f, "fault {what}"),
            Self::SchedulerBoot { boot } => write!(f, "scheduler-boot {boot}"),
            Self::SchedulerError(err) => write!(f, "scheduler-error {err}"),
            Self::Submit { job, digest } => write!(f, "submit {job} digest={digest}"),
            Self::Acked { job, name } => write!(f, "acked {job} name={name}"),
            Self::Reattach { job, name, after } => {
                write!(f, "reattach {job} name={name} after={}", fmt_opt_dur(*after))
            }
            Self::Stage { job, name, stage } => write!(f, "stage {job} name={name} {stage}"),
            Self::Done { job, name, outcome } => write!(f, "done {job} name={name} {outcome}"),
            Self::StreamEnded { job, why } => write!(f, "stream-ended {job} {why}"),
            Self::Detach { job, for_ms } => write!(f, "detach {job} for={for_ms}ms"),
            Self::NotFound {
                job,
                name,
                detached_for,
            } => write!(
                f,
                "not-found {job} name={name} detached_for={}",
                fmt_opt_dur(*detached_for)
            ),
            Self::RpcError { job, code, message } => {
                write!(f, "rpc-error {job} code={code} \"{message}\"")
            }
            Self::GaveUp { job, last_stage } => write!(f, "gave-up {job} last={last_stage}"),
            Self::Confirm { job, name, seen } => write!(f, "confirm {job} name={name} {seen}"),
            Self::WorkerConnected { session, worker_id } => {
                write!(f, "w-connected #{session} id={worker_id}")
            }
            Self::WorkerConnectFailed(why) => write!(f, "w-connect-failed {why}"),
            Self::WorkerDisconnected {
                session,
                why,
                dropped,
            } => write!(f, "w-disconnected #{session} dropped={dropped} {why}"),
            Self::StartExecute {
                session,
                operation,
                attempt,
                running,
                action,
            } => write!(
                f,
                "w-start #{session} op={operation} attempt={attempt} running={running} digest={action}"
            ),
            Self::DuplicateStart {
                session,
                operation,
                attempt,
            } => write!(f, "w-dup-start #{session} op={operation} attempt={attempt}"),
            Self::OverCapacity {
                session,
                operation,
                attempt,
                running,
                slots,
                live_here,
                unacked_kills,
            } => write!(
                f,
                "w-over-capacity #{session} op={operation} attempt={attempt} running={running} slots={slots} live_here={live_here} unacked_kills={unacked_kills}"
            ),
            Self::Kill {
                session,
                operation,
                attempt,
                was_running,
                obeyed,
            } => write!(
                f,
                "w-kill #{session} op={operation} attempt={attempt} was_running={was_running} obeyed={obeyed}"
            ),
            Self::ResultSent {
                session,
                operation,
                attempt,
                killed_before,
                ok,
            } => write!(
                f,
                "w-result #{session} op={operation} attempt={attempt} ok={ok} killed_before={killed_before}"
            ),
            Self::Behavior(what) => write!(f, "w-behavior {what}"),
            Self::Panic(what) => write!(f, "panic {what}"),
        }
    }
}

fn fmt_opt_dur(d: Option<Duration>) -> String {
    d.map_or_else(|| "-".to_string(), |d| format!("{}ms", d.as_millis()))
}

/// One entry of the history.
#[derive(Clone, Debug)]
pub struct Record {
    /// Position in the history.
    pub seq: usize,
    /// Simulated time since the start of the run.
    pub at: Duration,
    /// The host the event happened on (or `sim` for the injector).
    pub host: String,
    /// What happened.
    pub event: Event,
}

impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:>6} {:>10.3}s {:<8} {}",
            self.seq,
            self.at.as_secs_f64(),
            self.host,
            self.event
        )
    }
}

/// The shared, append-only history. Cheap to clone; every host holds one.
#[derive(Clone, Debug, Default)]
pub struct History {
    inner: Arc<Mutex<Vec<Record>>>,
}

impl History {
    /// Append an event on `host`, stamped with the current simulated time.
    pub fn record(&self, host: &str, event: Event) {
        let at = turmoil::sim_elapsed().unwrap_or_else(runner_elapsed);
        self.record_at(at, host, event);
    }

    /// Append an event with an explicit timestamp (the runner's own events).
    pub fn record_at(&self, at: Duration, host: &str, event: Event) {
        let mut records = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let seq = records.len();
        records.push(Record {
            seq,
            at,
            host: host.to_string(),
            event,
        });
    }

    /// A copy of every record so far.
    pub fn snapshot(&self) -> Vec<Record> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The whole history as text, one record per line.
    pub fn render(&self) -> String {
        use core::fmt::Write as _;
        let mut out = String::new();
        for record in self.snapshot() {
            let _ = writeln!(out, "{record}");
        }
        out
    }
}

thread_local! {
    static RUNNER_ELAPSED: core::cell::Cell<Duration> = const { core::cell::Cell::new(Duration::ZERO) };
}

/// The runner's notion of simulated time, for events recorded outside a host.
pub fn runner_elapsed() -> Duration {
    RUNNER_ELAPSED.with(core::cell::Cell::get)
}

/// Set by the runner after every step.
pub fn set_runner_elapsed(elapsed: Duration) {
    RUNNER_ELAPSED.with(|cell| cell.set(elapsed));
}
