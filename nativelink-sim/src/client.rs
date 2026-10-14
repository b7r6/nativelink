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

//! A simulated REAPI client: `Execute`, then follow the operation to its end,
//! detaching and reattaching with `WaitExecution`, retrying on `UNAVAILABLE`
//! and resubmitting on `NOT_FOUND`, as Bazel does.

use core::time::Duration;
use std::sync::Arc;

use nativelink_proto::build::bazel::remote::execution::v2::execution_client::ExecutionClient;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ExecuteOperationMetadata, ExecuteRequest, ExecuteResponse, WaitExecutionRequest,
    digest_function, execution_stage,
};
use nativelink_proto::google::longrunning::{Operation, operation};
use prost::Message;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::time::Instant;
use tonic::Code;

use crate::config::{Scenario, SimConfig};
use crate::history::{Event, History, Outcome};
use crate::net::{ChannelOptions, SCHEDULER_PORT, connect};
use crate::scheduler::SCHEDULER_HOST;
use crate::worker::decode_result_id;
use crate::workload::{INSTANCE_NAME, JobDef, Workload};

/// One simulated client host.
#[derive(Debug)]
pub struct ClientHost {
    pub name: String,
    pub seed: u64,
    pub jobs: Vec<JobDef>,
    pub workload: Arc<Workload>,
    pub config: SimConfig,
    pub history: History,
}

/// What one `Operation` message says.
#[derive(Debug)]
struct Observed {
    name: String,
    stage: String,
    outcome: Option<Outcome>,
}

fn stage_name(stage: i32) -> String {
    execution_stage::Value::try_from(stage)
        .map_or_else(|_| format!("STAGE_{stage}"), |v| v.as_str_name().to_string())
}

fn observe(operation: Operation) -> Observed {
    let stage = operation
        .metadata
        .as_ref()
        .and_then(|any| ExecuteOperationMetadata::decode(any.value.as_slice()).ok())
        .map_or_else(|| "NO_METADATA".to_string(), |m| stage_name(m.stage));
    let outcome = if operation.done {
        Some(match operation.result {
            Some(operation::Result::Error(status)) => Outcome::Failed {
                code: status.code,
                message: status.message,
            },
            Some(operation::Result::Response(any)) => {
                match ExecuteResponse::decode(any.value.as_slice()) {
                    Ok(response) => match response.status {
                        Some(status) if status.code != 0 => Outcome::Failed {
                            code: status.code,
                            message: status.message,
                        },
                        _ => {
                            let tag = response
                                .result
                                .and_then(|result| result.execution_metadata)
                                .map(|metadata| metadata.worker)
                                .unwrap_or_default();
                            decode_result_id(&tag)
                                .map_or(Outcome::OkUnattributed(tag), Outcome::Ok)
                        }
                    },
                    Err(err) => Outcome::OkUnattributed(format!("undecodable response: {err}")),
                }
            }
            None => Outcome::OkUnattributed("done without result".to_string()),
        })
    } else {
        None
    };
    Observed {
        name: operation.name,
        stage,
        outcome,
    }
}

/// How an attachment ended.
enum Attachment {
    Done,
    Detached,
    Broken,
    GaveUp,
}

impl ClientHost {
    fn record(&self, event: Event) {
        self.history.record(&self.name, event);
    }

    /// The client's software: run every job concurrently, then exit.
    pub async fn run(self: Arc<Self>) -> turmoil::Result {
        let start = Instant::now();
        let jobs = self
            .jobs
            .iter()
            .enumerate()
            .map(|(index, job)| self.run_job(start, index, job));
        futures::future::join_all(jobs).await;
        Ok(())
    }

    const fn options() -> ChannelOptions {
        ChannelOptions {
            connect_timeout: Duration::from_secs(5),
            keepalive: Some((Duration::from_secs(10), Duration::from_secs(10))),
        }
    }

    fn detach_duration(&self, rng: &mut StdRng) -> Duration {
        let window = self.config.timeouts.interest_window();
        let long = self.config.scenario == Scenario::LongDetach
            || self.config.scenario == Scenario::Chaos;
        if long && rng.random_bool(0.3) {
            // Past retention: the operation may be gone when the client is back.
            window + Duration::from_secs(rng.random_range(10..60))
        } else {
            let short_max = (window / 2).max(Duration::from_secs(2));
            Duration::from_millis(rng.random_range(500..=u64::try_from(short_max.as_millis()).unwrap_or(u64::MAX)))
        }
    }

    async fn run_job(&self, start: Instant, index: usize, job: &JobDef) {
        let mut rng = StdRng::seed_from_u64(self.seed ^ ((index as u64 + 1) << 32) ^ 0xc11e);
        let deadline = start + self.config.active + self.config.quiet;
        tokio::time::sleep_until(start + job.start_at).await;
        let action = &self.workload.actions[job.action];
        let mut name: Option<String> = None;
        // When the client last stopped being attached (`None` while attached
        // or before the first attachment).
        let mut detached_at: Option<Instant> = None;
        let mut last_stage = "none".to_string();
        let mut backoff = Duration::from_millis(500);
        loop {
            if Instant::now() >= deadline {
                self.record(Event::GaveUp {
                    job: job.name.clone(),
                    last_stage,
                });
                return;
            }
            let channel = match connect(SCHEDULER_HOST, SCHEDULER_PORT, Self::options()).await {
                Ok(channel) => channel,
                Err(err) => {
                    self.record(Event::RpcError {
                        job: job.name.clone(),
                        code: Code::Unavailable as i32,
                        message: format!("connect: {err}"),
                    });
                    detached_at.get_or_insert_with(Instant::now);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(8));
                    continue;
                }
            };
            let mut client = ExecutionClient::new(channel);
            let response = if let Some(known) = &name {
                self.record(Event::Reattach {
                    job: job.name.clone(),
                    name: known.clone(),
                    after: detached_at.map(|at| at.elapsed()),
                });
                client
                    .wait_execution(WaitExecutionRequest { name: known.clone() })
                    .await
            } else {
                self.record(Event::Submit {
                    job: job.name.clone(),
                    digest: action.label.clone(),
                });
                client
                    .execute(ExecuteRequest {
                        instance_name: INSTANCE_NAME.to_string(),
                        skip_cache_lookup: action.uncacheable,
                        action_digest: Some(action.digest.into()),
                        digest_function: digest_function::Value::Sha256.into(),
                        ..Default::default()
                    })
                    .await
            };
            let mut stream = match response {
                Ok(response) => response.into_inner(),
                Err(status) if status.code() == Code::NotFound && name.is_some() => {
                    self.record(Event::NotFound {
                        job: job.name.clone(),
                        name: name.take().unwrap_or_default(),
                        detached_for: detached_at.map(|at| at.elapsed()),
                    });
                    detached_at = None;
                    continue;
                }
                Err(status) => {
                    self.record(Event::RpcError {
                        job: job.name.clone(),
                        code: status.code() as i32,
                        message: status.message().to_string(),
                    });
                    detached_at.get_or_insert_with(Instant::now);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(8));
                    continue;
                }
            };
            backoff = Duration::from_millis(500);
            let detach_after = rng
                .random_bool(0.3)
                .then(|| Duration::from_millis(rng.random_range(500..20_000)));
            let detach_sleep = tokio::time::sleep(detach_after.unwrap_or(Duration::from_secs(1 << 30)));
            tokio::pin!(detach_sleep);
            let mut stream_stage: Option<String> = None;
            let ended = loop {
                tokio::select! {
                    message = stream.message() => match message {
                        Ok(Some(operation)) => {
                            let observed = observe(operation);
                            if name.is_none() {
                                self.record(Event::Acked { job: job.name.clone(), name: observed.name.clone() });
                                name = Some(observed.name.clone());
                            }
                            if let Some(outcome) = observed.outcome {
                                self.record(Event::Done { job: job.name.clone(), name: observed.name.clone(), outcome });
                                break Attachment::Done;
                            }
                            if stream_stage.as_deref() != Some(observed.stage.as_str()) {
                                self.record(Event::Stage { job: job.name.clone(), name: observed.name.clone(), stage: observed.stage.clone() });
                                stream_stage = Some(observed.stage.clone());
                                last_stage.clone_from(&observed.stage);
                            }
                        }
                        Ok(None) => {
                            self.record(Event::StreamEnded { job: job.name.clone(), why: "closed".to_string() });
                            break Attachment::Broken;
                        }
                        Err(status) => {
                            self.record(Event::StreamEnded { job: job.name.clone(), why: format!("{:?}: {}", status.code(), status.message()) });
                            break Attachment::Broken;
                        }
                    },
                    () = &mut detach_sleep, if name.is_some() => {
                        break Attachment::Detached;
                    }
                    () = tokio::time::sleep_until(deadline) => {
                        break Attachment::GaveUp;
                    }
                }
            };
            drop(stream);
            drop(client);
            match ended {
                Attachment::Done => {
                    self.confirm(&mut rng, job, name.as_deref().unwrap_or_default(), deadline)
                        .await;
                    return;
                }
                Attachment::Detached => {
                    let away = self.detach_duration(&mut rng);
                    self.record(Event::Detach {
                        job: job.name.clone(),
                        for_ms: u64::try_from(away.as_millis()).unwrap_or(u64::MAX),
                    });
                    detached_at = Some(Instant::now());
                    tokio::time::sleep(away).await;
                }
                Attachment::Broken => {
                    detached_at = Some(Instant::now());
                    tokio::time::sleep(backoff).await;
                }
                Attachment::GaveUp => {
                    self.record(Event::GaveUp {
                        job: job.name.clone(),
                        last_stage,
                    });
                    return;
                }
            }
        }
    }

    /// Read a finished operation once more, within retention: it must still
    /// be there and still say the same thing.
    async fn confirm(&self, rng: &mut StdRng, job: &JobDef, name: &str, deadline: Instant) {
        if name.is_empty() || !rng.random_bool(0.5) {
            return;
        }
        let window = self.config.timeouts.interest_window();
        let wait_ms = u64::try_from((window / 3).as_millis()).unwrap_or(1).max(1);
        tokio::time::sleep(Duration::from_millis(rng.random_range(0..wait_ms))).await;
        if Instant::now() >= deadline {
            return;
        }
        let seen = match connect(SCHEDULER_HOST, SCHEDULER_PORT, Self::options()).await {
            Err(err) => format!("unavailable(connect: {err})"),
            Ok(channel) => {
                let mut client = ExecutionClient::new(channel);
                match client
                    .wait_execution(WaitExecutionRequest {
                        name: name.to_string(),
                    })
                    .await
                {
                    Err(status) if status.code() == Code::NotFound => "not-found".to_string(),
                    Err(status) => format!("unavailable({:?})", status.code()),
                    Ok(response) => {
                        let mut stream = response.into_inner();
                        let first = tokio::time::timeout(Duration::from_secs(5), stream.message()).await;
                        match first {
                            Ok(Ok(Some(operation))) => {
                                let observed = observe(operation);
                                observed.outcome.map_or_else(
                                    || format!("stage {}", observed.stage),
                                    |outcome| format!("done {outcome}"),
                                )
                            }
                            Ok(Ok(None)) => "unavailable(closed)".to_string(),
                            Ok(Err(status)) => format!("unavailable({:?})", status.code()),
                            Err(_) => "unavailable(no answer)".to_string(),
                        }
                    }
                }
            }
        };
        self.record(Event::Confirm {
            job: job.name.clone(),
            name: name.to_string(),
            seen,
        });
    }
}
