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

//! Private supervised resource lifecycle. This does not establish a recovered
//! clock or permission to serve; composition with that boundary remains staged.

#![allow(
    dead_code,
    reason = "qualify private supervision before exposing a recovered authority"
)]

use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use nativelink_util::background_spawn;
use openraft::error::Fatal;
use openraft::{Raft, ServerState};
use redb::Database;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::super::{Journal, SingletonNetwork, TypeConfig};
#[cfg(test)]
use super::tests::recovery::{Hooks, Stage};
use super::{
    AuthorityDomain, CanonicalBootstrap, CanonicalContract, CanonicalProfile, ClusterEpoch,
    OwnerDiagnostic, OwnerPhase, PreparedCanonicalOwner, canonical_cleanup, temporal,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RecoveryError {
    Phase,
    Storage,
    Runtime,
    Panic,
    Task,
    Handoff,
}

// This record lives outside the caught startup/session future. In particular,
// neither a handled panic nor cancellation of the consumer loses the only
// designated cleanup reference or a Raft handle that has already been returned.
struct Resources {
    journal: Option<Journal>,
    raft: Option<Raft<TypeConfig>>,
    database: Arc<Database>,
    diagnostic: OwnerDiagnostic,
}

struct Control {
    stop: Option<oneshot::Sender<()>>,
    supervisor: JoinHandle<Result<(), RecoveryError>>,
}

impl Drop for Control {
    fn drop(&mut self) {
        // A raw JoinHandle detaches on Drop. The permanent supervisor must never
        // be owned by an abort-on-drop guard belonging to its public waiter.
        drop(self.stop.take());
    }
}

impl Control {
    async fn stop_and_join(&mut self) -> Result<(), RecoveryError> {
        drop(self.stop.take());
        (&mut self.supervisor)
            .await
            .map_err(|_| RecoveryError::Task)?
    }
}

pub(super) struct LifecycleOwner {
    diagnostic: OwnerDiagnostic,
    control: Control,
}

impl fmt::Debug for LifecycleOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("LifecycleOwner")
            .field(&self.diagnostic)
            .finish()
    }
}

impl LifecycleOwner {
    pub(super) const fn diagnostic(&self) -> OwnerDiagnostic {
        self.diagnostic
    }

    pub(super) async fn shutdown(mut self) -> Result<(), RecoveryError> {
        self.control.stop_and_join().await
    }
}

pub(super) async fn start(
    owner: PreparedCanonicalOwner,
    #[cfg(test)] hooks: Hooks,
) -> Result<LifecycleOwner, RecoveryError> {
    let PreparedCanonicalOwner {
        journal,
        diagnostic,
    } = owner;
    let resources = Resources {
        database: journal.database.clone(),
        journal: Some(journal),
        raft: None,
        diagnostic,
    };
    let (stop, stopped) = oneshot::channel();
    let (offer, offered) = oneshot::channel();
    let (accept, accepted) = oneshot::channel();
    let supervisor = background_spawn!(
        "canonical_recovery_supervisor",
        supervise(
            resources,
            offer,
            accepted,
            stopped,
            #[cfg(test)]
            hooks.clone(),
        )
    );
    // Transfer resources and arm stop ownership synchronously in the first
    // poll, before any consumer suspension point.
    let mut control = Control {
        stop: Some(stop),
        supervisor,
    };
    #[cfg(test)]
    if let Err(error) = hooks.at(Stage::ConsumerWaiting).await {
        control.stop_and_join().await?;
        return Err(error);
    }
    let Ok(diagnostic) = offered.await else {
        // Even a startup failure cannot be returned before real cleanup.
        control.stop_and_join().await?;
        return Err(RecoveryError::Handoff);
    };
    #[cfg(test)]
    if let Err(error) = hooks.at(Stage::ConsumerOffered).await {
        control.stop_and_join().await?;
        return Err(error);
    }
    let owner = LifecycleOwner {
        diagnostic,
        control,
    };
    #[cfg(test)]
    if let Err(error) = hooks.at(Stage::ConsumerArmed).await {
        owner.shutdown().await?;
        return Err(error);
    }
    if accept.send(()).is_err() {
        owner.shutdown().await?;
        return Err(RecoveryError::Handoff);
    }
    // The owner is already armed. No await may separate successful ACK from
    // returning it; even a synchronous test panic drops its stop ownership.
    #[cfg(test)]
    hooks.accepted_in_poll();
    Ok(owner)
}

async fn supervise(
    mut resources: Resources,
    offer: oneshot::Sender<OwnerDiagnostic>,
    accepted: oneshot::Receiver<()>,
    mut stop: oneshot::Receiver<()>,
    #[cfg(test)] hooks: Hooks,
) -> Result<(), RecoveryError> {
    let outcome = AssertUnwindSafe(run(
        &mut resources,
        offer,
        accepted,
        &mut stop,
        #[cfg(test)]
        &hooks,
    ))
    .catch_unwind()
    .await
    .unwrap_or(Err(RecoveryError::Panic));
    // Cleanup is outside the fallible session and is never selected against
    // consumer cancellation. Runtime/process destruction is not an ack.
    #[cfg(test)]
    hooks.observe(Stage::StopObserved);
    let cleanup = cleanup(
        resources,
        #[cfg(test)]
        &hooks,
    )
    .await;
    #[cfg(test)]
    hooks.observe(Stage::SupervisorDone);
    cleanup.and(outcome)
}

fn stop_requested(stop: &mut oneshot::Receiver<()>) -> bool {
    !matches!(stop.try_recv(), Err(oneshot::error::TryRecvError::Empty))
}

async fn run(
    resources: &mut Resources,
    offer: oneshot::Sender<OwnerDiagnostic>,
    mut accepted: oneshot::Receiver<()>,
    stop: &mut oneshot::Receiver<()>,
    #[cfg(test)] hooks: &Hooks,
) -> Result<(), RecoveryError> {
    #[cfg(test)]
    hooks.at(Stage::SupervisorStarted).await?;
    let journal = resources.journal.as_ref().ok_or(RecoveryError::Task)?;
    let diagnostic = resources.diagnostic;
    if diagnostic.phase != OwnerPhase::Sealed || journal.created {
        return Err(RecoveryError::Phase);
    }
    #[cfg(test)]
    hooks.at(Stage::BeforePreflight).await?;
    if stop_requested(stop) {
        return Ok(());
    }
    temporal::audit(
        journal,
        AuthorityDomain {
            epoch: ClusterEpoch(diagnostic.epoch),
            namespace: diagnostic.namespace,
        },
        CanonicalContract {
            profile: CanonicalProfile::SingletonSecondsV1,
            namespace_commitment: diagnostic.namespace_commitment,
        },
        CanonicalBootstrap::Sealed,
        #[cfg(test)]
        None,
    )
    .await
    .map_err(|_| RecoveryError::Storage)?;
    #[cfg(test)]
    hooks.at(Stage::BeforeRaft).await?;
    if stop_requested(stop) {
        return Ok(());
    }
    let config = openraft::Config {
        cluster_name: format!("canonical-authority-{}", diagnostic.epoch),
        heartbeat_interval: 50,
        election_timeout_min: 150,
        election_timeout_max: 300,
        ..Default::default()
    }
    .validate()
    .map_err(|_| RecoveryError::Runtime)?;
    // Do not cancel an in-progress Raft::new when stop arrives. Store a returned
    // handle before any later hook/await, then converge on supervised cleanup.
    resources.raft = Some(
        Raft::new(
            diagnostic.node_id,
            Arc::new(config),
            SingletonNetwork,
            journal.clone(),
            journal.clone(),
        )
        .await
        .map_err(|_| RecoveryError::Runtime)?,
    );
    #[cfg(test)]
    hooks.at(Stage::RaftStored).await?;
    if stop_requested(stop) {
        return Ok(());
    }
    let raft = resources.raft.as_ref().ok_or(RecoveryError::Task)?;
    // This path can never initialize a new member or turn Preparing into Sealed.
    if !raft
        .is_initialized()
        .await
        .map_err(|_| RecoveryError::Runtime)?
    {
        return Err(RecoveryError::Runtime);
    }
    #[cfg(test)]
    hooks.at(Stage::Initialized).await?;
    if stop_requested(stop) {
        return Ok(());
    }
    raft.wait(Some(Duration::from_secs(5)))
        .current_leader(diagnostic.node_id, "canonical singleton leader")
        .await
        .map_err(|_| RecoveryError::Runtime)?;
    #[cfg(test)]
    hooks.at(Stage::Leader).await?;
    if stop_requested(stop) {
        return Ok(());
    }
    #[cfg(test)]
    hooks.at(Stage::BeforeOffer).await?;
    if stop_requested(stop) {
        return Ok(());
    }
    #[cfg(test)]
    {
        hooks.publish_raft(raft);
        if hooks.reject_acceptance() {
            // Close the actual channel: the consumer must observe send failure.
            accepted.close();
        }
    }
    offer.send(diagnostic).map_err(|_| RecoveryError::Handoff)?;
    #[cfg(test)]
    hooks.observe(Stage::Offered);
    tokio::select! {
        result = &mut accepted => result.map_err(|_| RecoveryError::Handoff)?,
        _ = &mut *stop => return Ok(()),
    }
    #[cfg(test)]
    hooks.at(Stage::Accepted).await?;
    let _stopped = stop.await;
    Ok(())
}

async fn cleanup(
    mut resources: Resources,
    #[cfg(test)] hooks: &Hooks,
) -> Result<(), RecoveryError> {
    let outcome = if let Some(raft) = resources.raft.as_ref() {
        let joined = AssertUnwindSafe(raft.shutdown()).catch_unwind().await;
        match joined {
            Ok(Ok(())) => {
                #[cfg(test)]
                hooks.observe(Stage::CoreJoined);
                // Pinned OpenRaft discards core/tick join outcomes in shutdown.
                // Accept only its normal core terminal metrics. This does not
                // assert tick health or successful storage flush on Drop.
                let metrics = raft.metrics().borrow().clone();
                if metrics.state == ServerState::Shutdown
                    && matches!(metrics.running_state, Err(Fatal::Stopped))
                {
                    Ok(())
                } else {
                    Err(RecoveryError::Runtime)
                }
            }
            Ok(Err(_)) => Err(RecoveryError::Runtime),
            Err(_) => Err(RecoveryError::Panic),
        }
    } else {
        Ok(())
    };
    drop(resources.raft.take());
    drop(resources.journal.take());
    #[cfg(test)]
    hooks.observe(Stage::BeforeDrain);
    // Early Raft::new storage failure has no returned core/SM handle. Its tick
    // Drop requests stop but cannot be joined here; it retains no database.
    // Drain proves backend quiescence, including surviving actual I/O holders.
    canonical_cleanup::drain_database(
        resources.database,
        #[cfg(test)]
        canonical_cleanup::Hooks {
            before_drop: hooks.take_drop_gate(),
        },
    )
    .await
    .map_err(|_| RecoveryError::Task)?;
    #[cfg(test)]
    hooks.observe(Stage::DatabaseDropped);
    outcome
}
