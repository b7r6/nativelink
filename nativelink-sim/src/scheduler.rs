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

//! The scheduler host: the real scheduler (built by the production factory,
//! raft backend on a real directory) behind the real Execution and `WorkerApi`
//! services, served over the simulated network.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use nativelink_config::cas_server::{ExecutionConfig, WithInstanceName, WorkerApiConfig};
use nativelink_config::schedulers::{
    ExperimentalRaftSchedulerBackend, ExperimentalSimpleSchedulerBackend, SchedulerSpec,
    SimpleSpec,
};
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::{Error, ResultExt};
use nativelink_scheduler::default_scheduler_factory::scheduler_factory;
use nativelink_scheduler::known_platform_property_provider::KnownPlatformPropertyProvider;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::execution_server::ExecutionServer;
use nativelink_service::worker_api_server::WorkerApiServer;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::store_trait::StoreLike;
use tonic::transport::Server;

use crate::config::SimConfig;
use crate::history::{Event, History};
use crate::net::{SCHEDULER_PORT, incoming};
use crate::workload::{INSTANCE_NAME, Workload};

/// The scheduler host's name on the simulated network.
pub const SCHEDULER_HOST: &str = "sched";

const SCHEDULER_NAME: &str = "main";
const CAS_NAME: &str = "cas";

/// What every boot of the scheduler host needs.
#[derive(Debug)]
pub struct SchedulerHost {
    pub config: SimConfig,
    pub workload: Arc<Workload>,
    /// The raft log and snapshots: a real directory, kept across restarts.
    pub data_dir: PathBuf,
    pub history: History,
    pub boots: AtomicU32,
}

impl SchedulerHost {
    fn simple_spec(&self) -> SimpleSpec {
        let timeouts = &self.config.timeouts;
        SimpleSpec {
            retain_completed_for_s: timeouts.retain_completed_for_s,
            client_action_timeout_s: timeouts.client_action_timeout_s,
            worker_timeout_s: timeouts.worker_timeout_s,
            max_action_executing_timeout_s: timeouts.max_action_executing_timeout_s,
            unacknowledged_kill_timeout_s: timeouts.unacknowledged_kill_timeout_s,
            dispatch_ack_timeout_s: timeouts.dispatch_ack_timeout_s,
            report_executing_on_accept: self.config.report_executing_on_accept,
            experimental_backend: Some(ExperimentalSimpleSchedulerBackend::Raft(
                ExperimentalRaftSchedulerBackend {
                    node_id: 1,
                    data_dir: Some(self.data_dir.display().to_string()),
                    // No snapshot within a run: the snapshot is written on a
                    // blocking thread (see the blocker list in the README).
                    snapshot_every: 10_000_000,
                },
            )),
            ..Default::default()
        }
    }

    async fn seed_cas(&self, store_manager: &StoreManager) -> Result<(), Error> {
        let cas = store_manager
            .get_store(CAS_NAME)
            .err_tip(|| "simulated CAS missing")?;
        for action in &self.workload.actions {
            for (digest, bytes) in &action.blobs {
                cas.update_oneshot(*digest, bytes.clone())
                    .await
                    .err_tip(|| format!("seeding {} into the CAS", action.label))?;
            }
        }
        Ok(())
    }

    /// One boot of the scheduler process: build everything from the durable
    /// state and serve until the host is crashed.
    pub async fn run(self: Arc<Self>) -> turmoil::Result {
        let boot = self.boots.fetch_add(1, Ordering::SeqCst) + 1;
        self.history
            .record(SCHEDULER_HOST, Event::SchedulerBoot { boot });
        match self.serve().await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.history.record(
                    SCHEDULER_HOST,
                    Event::SchedulerError(err.message_string()),
                );
                // A failed boot is a finding, not a harness error: stay down
                // (like a process stuck in a crash loop) until the next bounce.
                core::future::pending::<()>().await;
                Ok(())
            }
        }
    }

    async fn serve(&self) -> Result<(), Error> {
        // CAS: an in-memory store seeded with every action the clients may
        // execute. It stands in for the separate storage service of a real
        // deployment, so it is re-seeded on every boot.
        let store_manager = Arc::new(StoreManager::new());
        store_manager.add_store(
            CAS_NAME,
            store_factory(
                &StoreSpec::Memory(MemorySpec::default()),
                &store_manager,
                None,
            )
            .await?,
        )?;
        self.seed_cas(&store_manager).await?;

        let spec = SchedulerSpec::Simple(self.simple_spec());
        let (action_scheduler, worker_scheduler) =
            scheduler_factory(&spec, &store_manager, None).await?;
        let action_scheduler: Arc<dyn KnownPlatformPropertyProvider> =
            action_scheduler.err_tip(|| "simple scheduler has an action scheduler")?;
        let worker_scheduler: Arc<dyn WorkerScheduler> =
            worker_scheduler.err_tip(|| "simple scheduler has a worker scheduler")?;

        let action_schedulers = HashMap::from([(SCHEDULER_NAME.to_string(), action_scheduler)]);
        let worker_schedulers = HashMap::from([(SCHEDULER_NAME.to_string(), worker_scheduler)]);
        let execution_server = ExecutionServer::new(
            &[WithInstanceName {
                instance_name: INSTANCE_NAME.to_string(),
                config: ExecutionConfig {
                    experimental_buck2_invocation_isolation: false,
                    cas_store: CAS_NAME.to_string(),
                    scheduler: SCHEDULER_NAME.to_string(),
                },
            }],
            &action_schedulers,
            &store_manager,
        )?;
        let worker_api_server = WorkerApiServer::new(
            &WorkerApiConfig {
                scheduler: SCHEDULER_NAME.to_string(),
                disable_kill_revoked_operations: false,
                kill_revoked_operations_interval_s: 0,
            },
            &worker_schedulers,
        )?;

        let listener = turmoil::net::TcpListener::bind(("0.0.0.0", SCHEDULER_PORT))
            .await
            .map_err(|e| Error::from_std_err(nativelink_error::Code::Internal, &e))?;
        Server::builder()
            .add_service(execution_server.into_service())
            .add_service(worker_api_server.into_service())
            .serve_with_incoming(incoming(listener))
            .await
            .map_err(|e| Error::from_std_err(nativelink_error::Code::Internal, &e))
    }
}
