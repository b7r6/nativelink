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

//! Experimental, test-constructed RPC coordinator. No production factory mounts
//! this service. The injected identity, stores, clock and fault gates are local
//! qualification fixtures, not a deployment or authentication interface.

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::{Stream, stream};
use nativelink_error::{Error, make_err};
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_proto::build::bazel::remote::execution::v2::execution_server::Execution;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, Digest, ExecuteOperationMetadata, ExecuteRequest, ExecuteResponse,
    RequestMetadata, WaitExecutionRequest,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_server::AuthorityWorkerApi;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    AuthorityAttemptIdentity, AuthorityAttemptPhase, AuthorityAttemptReport, AuthorityCapability,
    AuthorityCommand, AuthorityCoordinatorUpdate, AuthorityDispatch, AuthorityRegistration,
    AuthorityRegistrationResult, AuthorityResources, AuthoritySession, AuthorityWorkerIdentity,
    AuthorityWorkerUpdate, authority_attempt_report, authority_command,
    authority_coordinator_update, authority_worker_update,
};
use nativelink_proto::google::longrunning::{Operation, operation};
use nativelink_scheduler::authority::journal::Singleton;
use nativelink_scheduler::authority::request::RequestPayload;
use nativelink_scheduler::authority::{
    ActionKey, Effect, EffectId, EffectKind, Event, IncarnationId, ObservableState,
    OperationId, Phase, RecordedEvent, Reply, Request as AuthorityRequest, Resources,
    ResultPayload, SessionId, INTEREST_TIMEOUT, TERMINAL_RETENTION, TerminalResult, WorkerId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::background_spawn;
use nativelink_util::store_trait::{Store, StoreLike};
use prost::Message;
use prost_types::{Any, Timestamp};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, interval};
use tonic::{Request, Response, Status, Streaming};

const FRAME_BYTES: usize = 64 * 1024;
const OPERATION_BYTES: usize = 2 * FRAME_BYTES;
const INTEREST_RENEWAL_TICKS: u64 = INTEREST_TIMEOUT / 3;
const QUEUE: usize = 64;
const VERSION: u32 = 1;
const CAPABILITIES: [i32; 5] = [
    AuthorityCapability::AttemptEffectIdentities as i32,
    AuthorityCapability::ReconnectReconciliation as i32,
    AuthorityCapability::CleanupTombstones as i32,
    AuthorityCapability::CandidateResults as i32,
    AuthorityCapability::MeaningfulProgress as i32,
];
const REQUEST_METADATA: &str = "build.bazel.remote.execution.v2.requestmetadata-bin";

type OperationStream = Pin<Box<dyn Stream<Item = Result<Operation, Status>> + Send>>;
type WorkerStream = Pin<Box<dyn Stream<Item = Result<AuthorityCoordinatorUpdate, Status>> + Send>>;
type ReplyTo<T> = oneshot::Sender<Result<T, Status>>;
type WorkerSender = mpsc::Sender<Result<AuthorityCoordinatorUpdate, Status>>;

/// Explicit local cutpoints. These gates never manufacture accepted evidence.
#[derive(Debug, Default)]
pub struct QualificationFaults {
    pub hold_completions: AtomicBool,
    pub hold_dispatches: AtomicBool,
    pub hold_publications: AtomicBool,
    pub candidate_reports: AtomicU64,
}

/// Fixed test construction facts; not a runtime configuration selector.
pub struct QualificationConfig {
    pub instance: String,
    pub principal: String,
    pub cas: Store,
    pub publication_sink: Store,
    /// Monotonic experimental ticks. This is not a calibrated deployment clock.
    pub now: Arc<dyn Fn() -> u64 + Send + Sync>,
    pub faults: Arc<QualificationFaults>,
}

#[derive(Clone, Debug)]
pub struct AuthorityService {
    commands: mpsc::Sender<Input>,
    committed: watch::Receiver<Arc<ObservableState>>,
    instance: Arc<str>,
    principal: Arc<str>,
    cas: Store,
    permits: Arc<Semaphore>,
}

#[derive(Debug)]
pub struct CoordinatorTask {
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<(), String>>,
}

impl CoordinatorTask {
    pub async fn shutdown(self) -> Result<(), String> {
        let _ignored = self.stop.send(());
        self.task.await.map_err(|error| error.to_string())?
    }
}

enum Input {
    Submit(Box<AuthorityRequest>, ReplyTo<OperationId>),
    Observe(OperationId, ReplyTo<()>),
    RenewInterest(OperationId, ReplyTo<()>),
    Register(
        Box<AuthorityRegistration>,
        WorkerSender,
        oneshot::Sender<()>,
        ReplyTo<()>,
    ),
    Worker(AuthoritySession, Box<AuthorityWorkerUpdate>),
    Disconnected(AuthoritySession),
}

struct Observation {
    receiver: watch::Receiver<Arc<ObservableState>>,
    _permit: OwnedSemaphorePermit,
    previous: Option<Operation>,
    heartbeat_at: u64,
}

struct Connection {
    session: AuthoritySession,
    sender: WorkerSender,
    capacity_sequence: u64,
    _stop: oneshot::Sender<()>,
}

struct Driver {
    authority: Singleton,
    state: Arc<ObservableState>,
    watermark: OperationId,
    committed: watch::Sender<Arc<ObservableState>>,
    config: QualificationConfig,
    workers: BTreeMap<WorkerId, Connection>,
    sent: BTreeMap<EffectId, u64>,
    effects: JoinSet<(EffectId, Result<Event, String>)>,
    in_flight: BTreeSet<EffectId>,
    retry_at: BTreeMap<EffectId, Instant>,
    restore_deadlines: bool,
}

impl AuthorityService {
    pub async fn start(
        authority: Singleton,
        config: QualificationConfig,
    ) -> Result<(Self, CoordinatorTask), String> {
        let initial = match authority.read().await {
            Ok(initial) => initial,
            Err(error) => {
                let _shutdown = authority.shutdown().await;
                return Err(error);
            }
        };
        if initial.payload_limit_bytes() < FRAME_BYTES {
            authority.shutdown().await?;
            return Err(
                "Experimental RPC requires at least 64 KiB committed payload capacity".to_owned(),
            );
        }
        let watermark = initial.ingress_watermark();
        let state = Arc::new(initial.view());
        let (committed, receiver) = watch::channel(state.clone());
        let (commands, inputs) = mpsc::channel(QUEUE);
        let (stop, stopped) = oneshot::channel();
        let service = Self {
            commands,
            committed: receiver,
            instance: Arc::from(config.instance.as_str()),
            principal: Arc::from(config.principal.as_str()),
            cas: config.cas.clone(),
            permits: Arc::new(Semaphore::new(QUEUE)),
        };
        let driver = Driver {
            authority,
            state,
            watermark,
            committed,
            config,
            workers: BTreeMap::new(),
            sent: BTreeMap::new(),
            effects: JoinSet::new(),
            in_flight: BTreeSet::new(),
            retry_at: BTreeMap::new(),
            restore_deadlines: true,
        };
        let task = background_spawn!(
            "experimental_authority_coordinator",
            driver.run(inputs, stopped)
        );
        Ok((service, CoordinatorTask { stop, task }))
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<ObservableState>> {
        self.committed.clone()
    }

    fn name(&self, operation: OperationId) -> String {
        format!(
            "{}/authority/{}/{}",
            self.instance,
            self.committed.borrow().epoch.0,
            operation.0
        )
    }

    fn parse_name(&self, name: &str) -> Result<OperationId, Status> {
        let prefix = format!(
            "{}/authority/{}/",
            self.instance,
            self.committed.borrow().epoch.0
        );
        name.strip_prefix(&prefix)
            .and_then(|suffix| suffix.parse::<u64>().ok())
            .filter(|id| *id != 0)
            .map(OperationId)
            .ok_or_else(|| Status::not_found("Unknown operation name"))
    }

    fn permit(&self) -> Result<OwnedSemaphorePermit, Status> {
        self.permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("Experimental RPC concurrency exhausted"))
    }

    fn stream(&self, operation: OperationId, permit: OwnedSemaphorePermit) -> OperationStream {
        let name = self.name(operation);
        let commands = self.commands.clone();
        Box::pin(stream::unfold(
            Some(Observation {
                receiver: self.committed.clone(),
                _permit: permit,
                previous: None,
                heartbeat_at: 0,
            }),
            move |state| {
                let name = name.clone();
                let commands = commands.clone();
                async move {
                    let mut observation = state?;
                    loop {
                        if commands.is_closed() || observation.receiver.has_changed().is_err() {
                            return Some((Err(Status::unavailable("Coordinator stopped")), None));
                        }
                        let mut view = observation.receiver.borrow_and_update().clone();
                        let mut response = match operation_response(&view, operation, name.clone())
                        {
                            Ok(response) => response,
                            Err(error) => return Some((Err(error), None)),
                        };
                        let first = observation.previous.is_none();
                        let heartbeat = !first && view.now >= observation.heartbeat_at;
                        if first {
                            observation.heartbeat_at = view
                                .operations
                                .iter()
                                .find(|value| value.id == operation)
                                .map_or(view.now, |value| renewal_at(value.interest_deadline.at));
                        } else if heartbeat && !response.done {
                            // This future is polled by the response body. There is no
                            // detached renewal task when downstream capacity is absent.
                            let (reply, renewed) = oneshot::channel();
                            if commands
                                .send(Input::RenewInterest(operation, reply))
                                .await
                                .is_err()
                            {
                                return Some((
                                    Err(Status::unavailable("Coordinator stopped")),
                                    None,
                                ));
                            }
                            match renewed.await {
                                Ok(Ok(())) => {}
                                Ok(Err(error)) => return Some((Err(error), None)),
                                Err(_) => {
                                    return Some((
                                        Err(Status::unavailable("Renewal interrupted")),
                                        None,
                                    ));
                                }
                            }
                            view = observation.receiver.borrow_and_update().clone();
                            response = match operation_response(&view, operation, name.clone()) {
                                Ok(response) => response,
                                Err(error) => return Some((Err(error), None)),
                            };
                            observation.heartbeat_at =
                                view.now.saturating_add(INTEREST_RENEWAL_TICKS);
                        }
                        if first || heartbeat || observation.previous.as_ref() != Some(&response) {
                            if response.done {
                                return Some((Ok(response), None));
                            }
                            observation.previous = Some(response.clone());
                            return Some((Ok(response), Some(observation)));
                        }
                        if observation.receiver.changed().await.is_err() {
                            return Some((Err(Status::unavailable("Coordinator stopped")), None));
                        }
                    }
                }
            },
        ))
    }

    async fn capture(&self, grpc: Request<ExecuteRequest>) -> Result<AuthorityRequest, Status> {
        let metadata = grpc
            .metadata()
            .get_bin(REQUEST_METADATA)
            .map(|value| {
                let bytes = value
                    .to_bytes()
                    .map_err(|_| Status::invalid_argument("Invalid metadata"))?;
                if bytes.len() > FRAME_BYTES {
                    return Err(Status::resource_exhausted("Metadata exceeds capture bound"));
                }
                RequestMetadata::decode(bytes)
                    .map_err(|_| Status::invalid_argument("Invalid metadata"))
            })
            .transpose()?;
        let execute = grpc.into_inner();
        if execute.instance_name != self.instance.as_ref() || execute.encoded_len() > FRAME_BYTES {
            return Err(Status::invalid_argument(
                "Unknown instance or oversized request",
            ));
        }
        let queued = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::internal("Clock precedes epoch"))?;
        let queued = Timestamp {
            seconds: i64::try_from(queued.as_secs())
                .map_err(|_| Status::internal("Clock overflow"))?,
            nanos: i32::try_from(queued.subsec_nanos())
                .map_err(|_| Status::internal("Clock overflow"))?,
        };
        let digest = execute
            .action_digest
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing Action digest"))?;
        let action_bytes = bounded_blob(
            &self.cas,
            DigestInfo::try_from(digest).map_err(Status::from)?,
        )
        .await
        .map_err(Status::from)?;
        if action_bytes.len() > FRAME_BYTES {
            return Err(Status::resource_exhausted("Action exceeds capture bound"));
        }
        let action = Action::decode(action_bytes.clone())
            .map_err(|_| Status::invalid_argument("Invalid Action"))?;
        let command_digest = action
            .command_digest
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing Command digest"))?;
        let command_bytes = bounded_blob(
            &self.cas,
            DigestInfo::try_from(command_digest).map_err(Status::from)?,
        )
        .await
        .map_err(Status::from)?;
        let payload = RequestPayload::capture(
            &execute,
            metadata.as_ref(),
            &queued,
            &self.principal,
            &action_bytes,
            &command_bytes,
            FRAME_BYTES,
        )
        .map_err(Status::invalid_argument)?;
        AuthorityRequest::from_payload(payload).map_err(Status::invalid_argument)
    }
}

#[tonic::async_trait]
impl Execution for AuthorityService {
    type ExecuteStream = OperationStream;
    type WaitExecutionStream = OperationStream;

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        let permit = self.permit()?;
        let request = self.capture(request).await?;
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Input::Submit(Box::new(request), reply))
            .await
            .map_err(|_| Status::unavailable("Coordinator stopped"))?;
        let operation = response
            .await
            .map_err(|_| Status::unavailable("Admission interrupted"))??;
        Ok(Response::new(self.stream(operation, permit)))
    }

    async fn wait_execution(
        &self,
        request: Request<WaitExecutionRequest>,
    ) -> Result<Response<Self::WaitExecutionStream>, Status> {
        let permit = self.permit()?;
        let operation = self.parse_name(&request.into_inner().name)?;
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Input::Observe(operation, reply))
            .await
            .map_err(|_| Status::unavailable("Coordinator stopped"))?;
        response
            .await
            .map_err(|_| Status::unavailable("Observation interrupted"))??;
        Ok(Response::new(self.stream(operation, permit)))
    }
}

#[tonic::async_trait]
impl AuthorityWorkerApi for AuthorityService {
    type ConnectStream = WorkerStream;

    async fn connect(
        &self,
        request: Request<Streaming<AuthorityWorkerUpdate>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        let permit = self.permit()?;
        let mut incoming = request.into_inner();
        let first = incoming
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("Missing registration"))?;
        let Some(authority_worker_update::Update::Registration(registration)) = first.update else {
            return Err(Status::failed_precondition("First update must register"));
        };
        let session = registration
            .session
            .ok_or_else(|| Status::invalid_argument("Missing session"))?;
        let (sender, receiver) = mpsc::channel(QUEUE);
        let (reply, response) = oneshot::channel();
        let (stop, mut stopped) = oneshot::channel();
        self.commands
            .send(Input::Register(Box::new(registration), sender, stop, reply))
            .await
            .map_err(|_| Status::unavailable("Coordinator stopped"))?;
        response
            .await
            .map_err(|_| Status::unavailable("Registration interrupted"))??;
        let commands = self.commands.clone();
        background_spawn!("experimental_authority_worker_ingress", async move {
            let _permit = permit;
            loop {
                let next =
                    tokio::select! { _ = &mut stopped => break, next = incoming.message() => next };
                let Ok(Some(update)) = next else {
                    break;
                };
                if commands
                    .send(Input::Worker(session, Box::new(update)))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            let _ignored = commands.send(Input::Disconnected(session)).await;
        });
        Ok(Response::new(Box::pin(stream::unfold(
            receiver,
            |mut receiver| async move { receiver.recv().await.map(|update| (update, receiver)) },
        ))))
    }
}

impl Driver {
    async fn commit(&mut self, event: Event) -> Result<Reply, String> {
        let reply = self
            .authority
            .write(RecordedEvent {
                epoch: self.state.epoch,
                event,
            })
            .await?
            .data;
        let state = self.authority.read().await?;
        self.watermark = state.ingress_watermark();
        self.state = Arc::new(state.view());
        self.committed.send_replace(self.state.clone());
        Ok(reply)
    }

    async fn run(
        mut self,
        inputs: mpsc::Receiver<Input>,
        stop: oneshot::Receiver<()>,
    ) -> Result<(), String> {
        let result = self.run_loop(inputs, stop).await;
        self.effects.abort_all();
        while self.effects.join_next().await.is_some() {}
        self.workers.clear();
        let shutdown = self.authority.shutdown().await;
        match (result, shutdown) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), shutdown) => Err(format!("{error}; authority shutdown: {shutdown:?}")),
            (Ok(()), Err(error)) => Err(error),
        }
    }

    async fn run_loop(
        &mut self,
        mut inputs: mpsc::Receiver<Input>,
        mut stop: oneshot::Receiver<()>,
    ) -> Result<(), String> {
        let mut ticker = interval(Duration::from_millis(20));
        loop {
            let changed = tokio::select! {
                _ = &mut stop => break,
                input = inputs.recv() => {
                    let Some(input) = input else { break; };
                    self.input(input).await?;
                    true
                }
                completion = self.effects.join_next(), if !self.effects.is_empty() => {
                    let (effect, outcome) = completion.ok_or("Missing effect task")?.map_err(|error| error.to_string())?;
                    self.in_flight.remove(&effect);
                    match outcome {
                        Ok(event) => { self.commit(event).await?; }
                        Err(_) => { self.retry_at.insert(effect, Instant::now() + Duration::from_millis(100)); }
                    }
                    true
                }
                _ = ticker.tick() => self.time().await?,
            };
            if changed {
                for _ in 0..QUEUE {
                    let reply = self.commit(Event::Match).await?;
                    if !matches!(reply, Reply::Matched { .. } | Reply::Applied) {
                        break;
                    }
                }
            }
            self.pump_effects()?;
        }
        Ok(())
    }

    async fn time(&mut self) -> Result<bool, String> {
        let now = (self.config.now)().max(self.state.now);
        let advanced = now > self.state.now;
        if !advanced && !self.restore_deadlines {
            return Ok(false);
        }
        if advanced {
            self.commit(Event::AdvanceTime { now }).await?;
        }
        let mut due = Vec::new();
        for attempt in &self.state.attempts {
            if !attempt.authorized {
                continue;
            }
            if attempt.running_deadline.is_some_and(|at| at <= now) {
                due.push(Event::ExpireExecution {
                    attempt: attempt.id,
                });
            } else if attempt.deadline.at <= now {
                due.push(Event::ExpireAttempt {
                    attempt: attempt.id,
                    generation: attempt.deadline.generation,
                });
            }
        }
        for operation in &self.state.operations {
            if operation.interested
                && operation.terminal.is_none()
                && operation.interest_deadline.at <= now
            {
                due.push(Event::ExpireInterest {
                    operation: operation.id,
                    generation: operation.interest_deadline.generation,
                });
            }
        }
        for execution in &self.state.executions {
            if execution
                .terminal_at
                .is_some_and(|at| now >= at.saturating_add(TERMINAL_RETENTION))
            {
                due.push(Event::RetireExecution {
                    execution: execution.id,
                });
            }
        }
        let changed = advanced || !due.is_empty();
        for event in due {
            self.commit(event).await?;
        }
        // A restored journal may end after AdvanceTime but before its expiry
        // proposals. Sweep it once even if the local clock has not advanced.
        // Rejected retirement proposals do not spin at an unchanged clock.
        self.restore_deadlines = false;
        Ok(changed)
    }

    async fn input(&mut self, input: Input) -> Result<(), String> {
        match input {
            Input::Submit(request, reply) => {
                let Some(next) = self.watermark.0.checked_add(1) else {
                    let _ignored = reply.send(Err(Status::resource_exhausted(
                        "Operation identity exhausted",
                    )));
                    return Ok(());
                };
                let operation = OperationId(next);
                let outcome = self
                    .commit(Event::Submit {
                        operation,
                        request: *request,
                    })
                    .await?;
                let result = if matches!(outcome, Reply::Submitted { .. }) {
                    Ok(operation)
                } else {
                    Err(Status::resource_exhausted(format!(
                        "Admission rejected: {outcome:?}"
                    )))
                };
                let _ignored = reply.send(result);
            }
            Input::Observe(operation, reply) => {
                let outcome = self.commit(Event::Observe { operation }).await?;
                let result = if matches!(outcome, Reply::Observed(_)) {
                    Ok(())
                } else {
                    Err(Status::not_found("Operation is not retained"))
                };
                let _ignored = reply.send(result);
            }
            Input::RenewInterest(operation, reply) => {
                if reply.is_closed() {
                    return Ok(());
                }
                // Account for the current clock before considering renewal. A
                // queued delivery opportunity cannot revive an expired interest.
                self.time().await?;
                let due = self.state.operations.iter().any(|value| {
                    value.id == operation
                        && value.interested
                        && value.terminal.is_none()
                        && self.state.now < value.interest_deadline.at
                        && self.state.now >= renewal_at(value.interest_deadline.at)
                });
                if due && !reply.is_closed() {
                    self.commit(Event::RenewInterest { operation }).await?;
                }
                let _ignored = reply.send(Ok(()));
            }
            Input::Register(registration, sender, stop, reply) => {
                let result = self.register(*registration, sender, stop).await;
                match result {
                    Ok(result) => {
                        let _ignored = reply.send(result);
                    }
                    Err(error) => {
                        let _ignored =
                            reply.send(Err(Status::unavailable("Registration commit failed")));
                        return Err(error);
                    }
                }
            }
            Input::Worker(session, update) => {
                if self.current(session) {
                    self.update(session, *update).await?;
                }
            }
            Input::Disconnected(session) => {
                if self.current(session)
                    && let Some(identity) = session.worker
                {
                    self.workers.remove(&WorkerId(identity.worker_id));
                }
                // Transport loss is not quiescence or an authority revocation.
            }
        }
        Ok(())
    }

    fn current(&self, session: AuthoritySession) -> bool {
        session
            .worker
            .and_then(|worker| self.workers.get(&WorkerId(worker.worker_id)))
            .is_some_and(|connection| connection.session == session)
    }

    async fn register(
        &mut self,
        registration: AuthorityRegistration,
        sender: WorkerSender,
        stop: oneshot::Sender<()>,
    ) -> Result<Result<(), Status>, String> {
        let validated = validate_registration(&registration, self.state.epoch.0);
        let (session, identity, properties, capacity, available) = match validated {
            Ok(value) => value,
            Err(error) => return Ok(Err(error)),
        };
        let outcome = self
            .commit(Event::Register {
                worker: WorkerId(identity.worker_id),
                incarnation: IncarnationId(identity.incarnation_id),
                session: SessionId(session.session_id),
                properties,
                capacity,
                available,
            })
            .await?;
        if outcome != Reply::Applied {
            return Ok(Err(Status::failed_precondition("Registration rejected")));
        }
        self.workers.insert(
            WorkerId(identity.worker_id),
            Connection {
                session,
                sender: sender.clone(),
                capacity_sequence: 0,
                _stop: stop,
            },
        );
        // Inventory is processed before new placement; missing rows establish nothing.
        if let Some(inventory) = registration.reconciliation {
            for report in inventory.attempts {
                self.report(session, report).await?;
            }
        }
        if sender
            .try_send(Ok(AuthorityCoordinatorUpdate {
                update: Some(authority_coordinator_update::Update::Registration(
                    AuthorityRegistrationResult {
                        session: Some(session),
                        protocol_version: VERSION,
                        required_capabilities: CAPABILITIES.to_vec(),
                    },
                )),
            }))
            .is_err()
        {
            self.workers.remove(&WorkerId(identity.worker_id));
            return Ok(Err(Status::unavailable("Registration connection ended")));
        }
        Ok(Ok(()))
    }

    async fn update(
        &mut self,
        session: AuthoritySession,
        update: AuthorityWorkerUpdate,
    ) -> Result<(), String> {
        if update.encoded_len() > 1024 * 1024 {
            return Ok(());
        }
        match update.update {
            Some(authority_worker_update::Update::Reconciliation(inventory))
                if inventory.session == Some(session) =>
            {
                for report in inventory.attempts {
                    self.report(session, report).await?;
                }
            }
            Some(authority_worker_update::Update::Report(report)) => {
                self.report(session, report).await?;
            }
            Some(authority_worker_update::Update::Capacity(capacity))
                if capacity.session == Some(session) =>
            {
                let identity = session.worker.ok_or("Session lost identity")?;
                let connection = self
                    .workers
                    .get_mut(&WorkerId(identity.worker_id))
                    .ok_or("Session lost connection")?;
                if capacity.observation_sequence <= connection.capacity_sequence {
                    return Ok(());
                }
                let (Some(total), Some(ceiling)) = (capacity.capacity, capacity.admission_ceiling)
                else {
                    return Ok(());
                };
                connection.capacity_sequence = capacity.observation_sequence;
                self.commit(Event::Capacity {
                    worker: WorkerId(identity.worker_id),
                    incarnation: IncarnationId(identity.incarnation_id),
                    session: SessionId(session.session_id),
                    capacity: resources(total),
                    available: resources(ceiling),
                    draining: capacity.draining,
                })
                .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn report(
        &mut self,
        session: AuthoritySession,
        report: AuthorityAttemptReport,
    ) -> Result<(), String> {
        let Ok(report_phase) = AuthorityAttemptPhase::try_from(report.phase) else {
            return Ok(());
        };
        if report_phase == AuthorityAttemptPhase::Unspecified
            || report.observation_sequence == 0
            || report.encoded_len() > FRAME_BYTES + 1024
        {
            return Ok(());
        }
        let Some(target) = report.attempt else {
            return Ok(());
        };
        let Some(identity) = target.worker else {
            return Ok(());
        };
        if session.worker != Some(identity) || identity.cluster_epoch != self.state.epoch.0 {
            return Ok(());
        }
        let Some(attempt) = self
            .state
            .attempts
            .iter()
            .find(|attempt| {
                attempt.id.0 == target.attempt_id
                    && attempt.execution.0 == target.execution_id
                    && attempt.worker.0 == identity.worker_id
                    && attempt.incarnation.0 == identity.incarnation_id
            })
            .copied()
        else {
            return Ok(());
        };
        let quiescent = report_phase == AuthorityAttemptPhase::Quiescent;
        let dispatch_matches = attempt.dispatch_effect.0 == report.dispatch_effect_id;
        let cancel_matches = !attempt.authorized && quiescent && report.cancel_effect_id != 0 && self.state.effects.iter().any(|effect| {
            effect.id.0 == report.cancel_effect_id && matches!(effect.kind, EffectKind::Cancel { execution, attempt: target, worker, incarnation } if execution == attempt.execution && target == attempt.id && worker == attempt.worker && incarnation == attempt.incarnation)
        });
        if !dispatch_matches && !cancel_matches {
            return Ok(());
        }
        let worker = attempt.worker;
        let incarnation = attempt.incarnation;
        if !attempt.authorized {
            if quiescent {
                self.commit(Event::Cleanup {
                    attempt: attempt.id,
                    worker,
                    incarnation,
                })
                .await?;
            }
            return Ok(());
        }
        if matches!(
            report.result,
            Some(authority_attempt_report::Result::Decline(_))
        ) {
            if quiescent {
                self.commit(Event::Decline {
                    attempt: attempt.id,
                    worker,
                    incarnation,
                })
                .await?;
            }
            return Ok(());
        }
        self.commit(Event::WorkerAccepted {
            attempt: attempt.id,
            worker,
            incarnation,
        })
        .await?;
        let phase = match AuthorityAttemptPhase::try_from(report.phase).ok() {
            Some(AuthorityAttemptPhase::Preparing) => Some(Phase::Preparing),
            Some(AuthorityAttemptPhase::Running) => Some(Phase::Running),
            Some(AuthorityAttemptPhase::Uploading) => Some(Phase::Uploading),
            _ => None,
        };
        if let Some(phase) = phase {
            self.commit(Event::Progress {
                attempt: attempt.id,
                worker,
                incarnation,
                phase,
                progress: report.phase_progress,
            })
            .await?;
        }
        let candidate = match report.result {
            Some(authority_attempt_report::Result::ExecuteResponse(response)) => Some(response),
            Some(authority_attempt_report::Result::InternalError(status)) if status.code != 0 => {
                Some(ExecuteResponse {
                    status: Some(status),
                    ..ExecuteResponse::default()
                })
            }
            _ => None,
        };
        if let Some(response) = candidate {
            self.config
                .faults
                .candidate_reports
                .fetch_add(1, Ordering::Relaxed);
            if quiescent && !self.config.faults.hold_completions.load(Ordering::Relaxed) {
                let Some(result) = completion_payload(&response) else {
                    return Ok(());
                };
                self.commit(Event::Complete {
                    attempt: attempt.id,
                    worker,
                    incarnation,
                    result,
                })
                .await?;
            }
        }
        Ok(())
    }

    fn pump_effects(&mut self) -> Result<(), String> {
        let live: BTreeSet<_> = self.state.effects.iter().map(|effect| effect.id).collect();
        self.sent.retain(|effect, _| live.contains(effect));
        self.retry_at.retain(|effect, _| live.contains(effect));
        for effect in &self.state.effects {
            match &effect.kind {
                EffectKind::Dispatch {
                    worker,
                    incarnation,
                    ..
                }
                | EffectKind::Cancel {
                    worker,
                    incarnation,
                    ..
                } => {
                    if matches!(effect.kind, EffectKind::Dispatch { .. })
                        && self.config.faults.hold_dispatches.load(Ordering::Relaxed)
                    {
                        continue;
                    }
                    let Some(connection) = self.workers.get(worker) else {
                        continue;
                    };
                    if connection
                        .session
                        .worker
                        .is_none_or(|identity| identity.incarnation_id != incarnation.0)
                        || self
                            .sent
                            .get(&effect.id)
                            .is_some_and(|session| *session == connection.session.session_id)
                    {
                        continue;
                    }
                    let command = effect_command(effect)?;
                    if connection
                        .sender
                        .try_send(Ok(AuthorityCoordinatorUpdate {
                            update: Some(authority_coordinator_update::Update::Command(command)),
                        }))
                        .is_ok()
                    {
                        self.sent.insert(effect.id, connection.session.session_id);
                    }
                }
                EffectKind::Lookup { .. } | EffectKind::PublishResult { .. } => {
                    if self.in_flight.contains(&effect.id)
                        || self.in_flight.len() >= 4
                        || self
                            .retry_at
                            .get(&effect.id)
                            .is_some_and(|at| *at > Instant::now())
                        || (matches!(effect.kind, EffectKind::PublishResult { .. })
                            && self.config.faults.hold_publications.load(Ordering::Relaxed))
                    {
                        continue;
                    }
                    self.in_flight.insert(effect.id);
                    let effect = effect.clone();
                    let sink = self.config.publication_sink.clone();
                    self.effects.spawn(async move {
                        let outcome = tokio::time::timeout(
                            Duration::from_secs(5),
                            storage_effect(&sink, &effect),
                        )
                        .await
                        .map_err(|_| "Experimental sink timeout".to_owned())
                        .and_then(|result| result);
                        (effect.id, outcome)
                    });
                }
            }
        }
        Ok(())
    }
}

const fn resources(value: AuthorityResources) -> Resources {
    Resources {
        cpu_millis: value.cpu_millis,
        memory_bytes: value.memory_bytes,
        slots: value.slots,
    }
}

type RegistrationFacts = (
    AuthoritySession,
    AuthorityWorkerIdentity,
    BTreeMap<String, String>,
    Resources,
    Resources,
);

fn validate_registration(
    registration: &AuthorityRegistration,
    epoch: u64,
) -> Result<RegistrationFacts, Status> {
    let session = registration
        .session
        .ok_or_else(|| Status::invalid_argument("Missing session"))?;
    let identity = session
        .worker
        .ok_or_else(|| Status::invalid_argument("Missing worker identity"))?;
    if identity.cluster_epoch != epoch
        || identity.worker_id == 0
        || identity.incarnation_id == 0
        || session.session_id == 0
        || registration.protocol_version != VERSION
        || !CAPABILITIES
            .iter()
            .all(|capability| registration.capabilities.contains(capability))
        || registration.encoded_len() > 1024 * 1024
    {
        return Err(Status::failed_precondition(
            "Unsupported authority identity or protocol",
        ));
    }
    let inventory = registration
        .reconciliation
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("Complete inventory required"))?;
    if inventory.session != Some(session) {
        return Err(Status::failed_precondition("Inventory session mismatch"));
    }
    let capacity = resources(
        registration
            .capacity
            .ok_or_else(|| Status::invalid_argument("Missing capacity"))?,
    );
    let available = resources(
        registration
            .admission_ceiling
            .ok_or_else(|| Status::invalid_argument("Missing admission ceiling"))?,
    );
    if inventory.single_use_spent && available != Resources::default() {
        return Err(Status::failed_precondition(
            "Spent worker must close admission",
        ));
    }
    let mut properties = BTreeMap::new();
    for property in &registration.properties {
        if properties
            .insert(property.name.clone(), property.value.clone())
            .is_some()
        {
            return Err(Status::invalid_argument("Duplicate worker property"));
        }
    }
    Ok((session, identity, properties, capacity, available))
}

fn effect_command(effect: &Effect) -> Result<AuthorityCommand, String> {
    let (execution, attempt, worker, incarnation, command) = match &effect.kind {
        EffectKind::Dispatch {
            execution,
            attempt,
            worker,
            incarnation,
            reservation,
            charged,
            request,
        } => {
            let facts = request.require_payload()?.dispatch_facts()?;
            (
                *execution,
                *attempt,
                *worker,
                *incarnation,
                authority_command::Command::Dispatch(AuthorityDispatch {
                    reservation_id: reservation.0,
                    reserved_resources: Some(AuthorityResources {
                        cpu_millis: charged.cpu_millis,
                        memory_bytes: charged.memory_bytes,
                        slots: charged.slots,
                    }),
                    execute_request: Some(facts.execute_request),
                    queued_timestamp: Some(facts.queued_timestamp),
                    platform: Some(facts.platform),
                    request_metadata: facts.request_metadata,
                }),
            )
        }
        EffectKind::Cancel {
            execution,
            attempt,
            worker,
            incarnation,
        } => (
            *execution,
            *attempt,
            *worker,
            *incarnation,
            authority_command::Command::Cancel(()),
        ),
        _ => return Err("Not a recipient command".to_owned()),
    };
    Ok(AuthorityCommand {
        attempt: Some(AuthorityAttemptIdentity {
            worker: Some(AuthorityWorkerIdentity {
                cluster_epoch: effect.epoch.0,
                worker_id: worker.0,
                incarnation_id: incarnation.0,
            }),
            execution_id: execution.0,
            attempt_id: attempt.0,
        }),
        effect_id: effect.id.0,
        command: Some(command),
    })
}

fn digest(action: &ActionKey) -> Result<DigestInfo, String> {
    DigestInfo::try_from(Digest {
        hash: action.digest.clone(),
        size_bytes: i64::try_from(action.digest_size_bytes).map_err(|error| error.to_string())?,
    })
    .map_err(|error| error.to_string())
}

async fn storage_effect(sink: &Store, effect: &Effect) -> Result<Event, String> {
    match &effect.kind {
        EffectKind::Lookup { action, .. } => {
            let result = match bounded_blob(sink, digest(action)?).await {
                Ok(bytes) if bytes.len() <= FRAME_BYTES => {
                    let result = ActionResult::decode(bytes).map_err(|error| error.to_string())?;
                    Some(ResultPayload::from_response(
                        &ExecuteResponse {
                            result: Some(result),
                            cached_result: true,
                            ..ExecuteResponse::default()
                        },
                        FRAME_BYTES,
                    )?)
                }
                Err(error) if error.code == tonic::Code::NotFound => None,
                _ => return Err("Lookup failed or exceeded bound".to_owned()),
            };
            Ok(Event::LookupResult {
                effect: effect.id,
                result,
            })
        }
        EffectKind::PublishResult { action, result, .. } => {
            let response = result.require_response()?.decode()?;
            let bytes = response
                .result
                .ok_or("Missing publication result")?
                .encode_to_vec();
            sink.update_oneshot(digest(action)?, bytes.into())
                .await
                .map_err(|error| error.to_string())?;
            Ok(Event::EffectOutcome {
                effect: effect.id,
                success: true,
            })
        }
        _ => Err("Not a storage effect".to_owned()),
    }
}

fn operation_response(
    view: &ObservableState,
    id: OperationId,
    name: String,
) -> Result<Operation, Status> {
    let operation = view
        .operations
        .iter()
        .find(|operation| operation.id == id)
        .ok_or_else(|| Status::not_found("Operation is not retained"))?;
    let execution = view
        .executions
        .iter()
        .find(|execution| execution.id == operation.execution)
        .ok_or_else(|| Status::internal("Missing operation execution"))?;
    let stage = if operation.terminal.is_some() {
        4
    } else {
        match execution.phase {
            Phase::Lookup => 1,
            Phase::Queued => 2,
            Phase::Terminal => 4,
            _ => 3,
        }
    };
    let metadata = ExecuteOperationMetadata {
        stage,
        action_digest: Some(Digest {
            hash: execution.request.action.digest.clone(),
            size_bytes: i64::try_from(execution.request.action.digest_size_bytes)
                .map_err(|_| Status::internal("Digest size overflow"))?,
        }),
        ..ExecuteOperationMetadata::default()
    };
    let result = match &operation.terminal {
        Some(TerminalResult::Completed(result) | TerminalResult::ResourceFailed { result, .. }) => {
            let response = result.require_response().map_err(Status::internal)?;
            Some(operation::Result::Response(Any {
                type_url: "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteResponse"
                    .to_owned(),
                value: response.encoded().to_vec(),
            }))
        }
        Some(TerminalResult::Cancelled) => Some(operation::Result::Error(
            nativelink_proto::google::rpc::Status {
                code: tonic::Code::Cancelled as i32,
                message: "Operation cancelled".to_owned(),
                details: Vec::new(),
            },
        )),
        Some(TerminalResult::Exhausted) => Some(operation::Result::Error(
            nativelink_proto::google::rpc::Status {
                code: tonic::Code::ResourceExhausted as i32,
                message: "Execution attempt budget exhausted".to_owned(),
                details: Vec::new(),
            },
        )),
        None => None,
    };
    let response = Operation {
        name,
        metadata: Some(Any {
            type_url:
                "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteOperationMetadata"
                    .to_owned(),
            value: metadata.encode_to_vec(),
        }),
        done: operation.terminal.is_some(),
        result,
    };
    if response.encoded_len() > OPERATION_BYTES {
        return Err(Status::resource_exhausted(
            "Operation exceeds delivery bound",
        ));
    }
    Ok(response)
}

const fn renewal_at(deadline: u64) -> u64 {
    deadline
        .saturating_sub(INTEREST_TIMEOUT)
        .saturating_add(INTEREST_RENEWAL_TICKS)
}

impl std::fmt::Debug for QualificationConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QualificationConfig")
            .field("instance", &self.instance)
            .finish_non_exhaustive()
    }
}

async fn bounded_blob(store: &Store, digest: DigestInfo) -> Result<Bytes, Error> {
    let (mut writer, mut reader) = make_buf_channel_pair();
    let download = store.get_part(digest, &mut writer, 0, Some(65_537));
    let collect = async move {
        let mut bytes = Vec::new();
        loop {
            let chunk = reader.recv().await?;
            if chunk.is_empty() {
                break;
            }
            if chunk.len() > FRAME_BYTES.saturating_sub(bytes.len()) {
                return Err(make_err!(
                    tonic::Code::ResourceExhausted,
                    "Blob exceeds capture bound"
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(Bytes::from(bytes))
    };
    let ((), bytes) = futures::try_join!(download, collect)?;
    Ok(bytes)
}

// The worker bounds protobuf bytes; the committed representation also includes
// JSON byte arrays and duplicated error text. Check that representation itself.
// Quiescent oversized output has one explicit non-cacheable terminal outcome.
fn completion_payload(response: &ExecuteResponse) -> Option<ResultPayload> {
    let captured = ResultPayload::from_response(response, FRAME_BYTES).ok()?;
    if serde_json::to_vec(&captured).ok()?.len() <= FRAME_BYTES {
        return Some(captured);
    }
    let overflow = ExecuteResponse {
        status: Some(nativelink_proto::google::rpc::Status {
            code: tonic::Code::ResourceExhausted as i32,
            message: "Candidate response exceeds the experimental committed payload bound"
                .to_owned(),
            details: Vec::new(),
        }),
        ..ExecuteResponse::default()
    };
    let result = ResultPayload::from_response(&overflow, FRAME_BYTES).ok()?;
    (serde_json::to_vec(&result).ok()?.len() <= FRAME_BYTES).then_some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_checks_committed_encoding_and_preserves_supported_response() {
        let response = ExecuteResponse {
            result: Some(ActionResult {
                stdout_raw: vec![255; 100].into(),
                ..ActionResult::default()
            }),
            ..ExecuteResponse::default()
        };
        assert_eq!(
            completion_payload(&response)
                .unwrap()
                .require_response()
                .unwrap()
                .decode()
                .unwrap(),
            response
        );
        for response in [
            ExecuteResponse {
                result: Some(ActionResult {
                    stdout_raw: vec![255; FRAME_BYTES - 128].into(),
                    ..ActionResult::default()
                }),
                ..ExecuteResponse::default()
            },
            ExecuteResponse {
                status: Some(nativelink_proto::google::rpc::Status {
                    code: tonic::Code::Internal as i32,
                    message: "\u{0001}".repeat(9000),
                    details: Vec::new(),
                }),
                ..ExecuteResponse::default()
            },
        ] {
            assert!(response.encoded_len() < FRAME_BYTES);
            let original = ResultPayload::from_response(&response, FRAME_BYTES).unwrap();
            assert!(serde_json::to_vec(&original).unwrap().len() > FRAME_BYTES);
            let result = completion_payload(&response).unwrap();
            assert!(serde_json::to_vec(&result).unwrap().len() < 1024);
            let terminal = result.require_response().unwrap().decode().unwrap();
            assert_eq!(
                terminal.status.unwrap().code,
                tonic::Code::ResourceExhausted as i32
            );
            assert!(terminal.result.is_none());
        }
        assert!(completion_payload(&ExecuteResponse::default()).is_none());
    }
}
