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

//! One real gRPC session for the experimental identity-bearing recipient.
//!
//! Construction is test-only until the canonical server/cutover gates land.
//! A transport failure leaves process receipts, candidates and resource charges
//! in the recipient. The caller reconnects with a newer session of the same
//! process incarnation; this function neither revokes nor kills its work.

use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, stream};
use nativelink_error::{Code, Error, make_err};
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_client::AuthorityWorkerApiClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    AuthorityCapacityObservation, AuthorityReconciliation, AuthorityWorkerUpdate,
    authority_coordinator_update, authority_worker_update,
};
use tokio::sync::watch;
use tonic::transport::{Channel, Endpoint};

use crate::authority_worker::AuthorityWorker;
use crate::running_actions_manager::RunningActionsManager;

// Complete inventories retain every receipt/candidate/tombstone. Coalescing
// successive inventories therefore loses no unresolved obligation, while a slow
// transport retains only bounded snapshots instead of an ever-growing queue.
type Inventory = (AuthorityReconciliation, AuthorityCapacityObservation);

/// Reconnect the same process recipient after transient transport failures.
/// Sessions advance even when negotiation was interrupted. This never replaces
/// the incarnation, forgets a receipt, kills work, or refunds a reservation.
/// The caller owns shutdown and the first session of this supervisor lifetime.
pub async fn run_reconnecting<M: RunningActionsManager>(
    worker: Arc<AuthorityWorker<M>>,
    endpoint: Endpoint,
    first_session: u64,
    properties: Vec<Property>,
    mut stopped: watch::Receiver<bool>,
) -> Result<(), Error> {
    let mut session = first_session;
    let mut backoff = Duration::from_millis(100);
    loop {
        if *stopped.borrow() {
            return Ok(());
        }
        let result = tokio::select! {
            result = async {
                let channel = endpoint.connect().await.map_err(|error| Error::from_std_err(Code::Unavailable, &error))?;
                run_session(worker.clone(), AuthorityWorkerApiClient::new(channel), session, properties.clone()).await
            } => result,
            () = stop_requested(&mut stopped) => return Ok(()),
        };
        if let Err(error) = result
            && !matches!(
                error.code,
                Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled | Code::Unknown
            )
        {
            // Protocol/capability/identity rejection cannot be repaired by
            // silently downgrading or starting another worker incarnation.
            return Err(error);
        }
        session = session.checked_add(1).ok_or_else(|| {
            make_err!(
                Code::ResourceExhausted,
                "Authority session identity exhausted"
            )
        })?;
        tokio::select! {
            () = tokio::time::sleep(backoff) => {}
            () = stop_requested(&mut stopped) => return Ok(()),
        }
        backoff = (backoff * 2).min(Duration::from_secs(2));
    }
}

async fn stop_requested(stopped: &mut watch::Receiver<bool>) {
    while !*stopped.borrow_and_update() {
        if stopped.changed().await.is_err() {
            return;
        }
    }
}

fn publish_inventory<M: RunningActionsManager>(
    worker: &AuthorityWorker<M>,
    sender: &watch::Sender<Option<Inventory>>,
    session: u64,
    sequence: &mut u64,
) -> Result<(), Error> {
    let inventory = worker.reconcile();
    if inventory
        .session
        .as_ref()
        .map(|identity| identity.session_id)
        != Some(session)
    {
        return Err(make_err!(
            Code::FailedPrecondition,
            "Authority session was replaced"
        ));
    }
    *sequence = sequence.checked_add(1).ok_or_else(|| {
        make_err!(
            Code::ResourceExhausted,
            "Capacity observation sequence exhausted"
        )
    })?;
    let capacity = worker.make_capacity_observation(session, *sequence)?;
    sender.send_replace(Some((inventory, capacity)));
    Ok(())
}

/// Negotiate one mandatory-capability session and exchange identity-bearing
/// commands/reports over tonic. No scheduler acceptance or publication is
/// inferred from receiving a candidate report.
pub async fn run_session<M: RunningActionsManager>(
    worker: Arc<AuthorityWorker<M>>,
    mut client: AuthorityWorkerApiClient<Channel>,
    session: u64,
    properties: Vec<Property>,
) -> Result<(), Error> {
    let mut changed = worker.subscribe();
    let registration = worker.make_registration(session, properties)?;
    let (sender, receiver) = watch::channel::<Option<Inventory>>(None);
    let updates = stream::unfold((receiver, None), |(mut receiver, pending)| async move {
        let (update, next_capacity) = if let Some(capacity) = pending {
            (authority_worker_update::Update::Capacity(capacity), None)
        } else {
            receiver.changed().await.ok()?;
            let (inventory, capacity) = receiver.borrow_and_update().clone()?;
            (
                authority_worker_update::Update::Reconciliation(inventory),
                Some(capacity),
            )
        };
        Some((
            AuthorityWorkerUpdate {
                update: Some(update),
            },
            (receiver, next_capacity),
        ))
    });
    // The first registration is not coalesced or replaced by observations that
    // arrive while the HTTP/2 stream is being established.
    let outgoing = stream::once(async move {
        AuthorityWorkerUpdate {
            update: Some(authority_worker_update::Update::Registration(registration)),
        }
    })
    .chain(updates);
    let mut incoming = client
        .connect(outgoing)
        .await
        .map_err(Error::from)?
        .into_inner();
    let first = incoming
        .message()
        .await
        .map_err(Error::from)?
        .ok_or_else(|| make_err!(Code::Unavailable, "Authority closed before negotiation"))?;
    let Some(authority_coordinator_update::Update::Registration(accepted)) = first.update else {
        return Err(make_err!(
            Code::FailedPrecondition,
            "Authority must negotiate before sending commands"
        ));
    };
    // Never accept a different session than the one this call proposed. The
    // recipient also checks process identity, version and mandatory capabilities.
    if accepted
        .session
        .as_ref()
        .map(|identity| identity.session_id)
        != Some(session)
    {
        return Err(make_err!(
            Code::FailedPrecondition,
            "Authority changed the proposed session"
        ));
    }
    worker.negotiate(&accepted)?;
    let _disconnect = scopeguard::guard(worker.clone(), |worker| worker.disconnect(session));
    let mut sequence = 0;
    publish_inventory(&worker, &sender, session, &mut sequence)?;
    loop {
        tokio::select! {
            message = incoming.message() => {
                let message = message.map_err(Error::from)?.ok_or_else(|| {
                    make_err!(Code::Unavailable, "Authority command stream ended")
                })?;
                let outcome = match message.update {
                    Some(authority_coordinator_update::Update::Command(command)) => worker.command(session, command).map(|_| ()),
                    Some(authority_coordinator_update::Update::Retirement(certificate)) => worker.retire_receipts(certificate).map(|_| ()),
                    _ => return Err(make_err!(Code::FailedPrecondition, "Unexpected authority negotiation message")),
                };
                if let Err(error) = outcome {
                    // Compacted attempts and older certificates cannot create a
                    // cleanup report or terminate a healthy successor session.
                    if error.code != Code::OutOfRange {
                        return Err(error);
                    }
                }
                publish_inventory(&worker, &sender, session, &mut sequence)?;
            }
            observation = changed.changed() => {
                observation.map_err(|_| make_err!(Code::Unavailable, "Worker observation stream ended"))?;
                publish_inventory(&worker, &sender, session, &mut sequence)?;
            }
        }
    }
}
