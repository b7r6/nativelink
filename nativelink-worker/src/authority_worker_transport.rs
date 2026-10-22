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

use futures::{StreamExt, stream};
use nativelink_error::{Code, Error, make_err};
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_client::AuthorityWorkerApiClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    AuthorityCapacityObservation, AuthorityReconciliation, AuthorityWorkerUpdate,
    authority_coordinator_update, authority_worker_update,
};
use tokio::sync::watch;
use tonic::transport::Channel;

use crate::authority_worker::AuthorityWorker;
use crate::running_actions_manager::RunningActionsManager;

// Complete inventories retain every receipt/candidate/tombstone. Coalescing
// successive inventories therefore loses no unresolved obligation, while a slow
// transport retains only bounded snapshots instead of an ever-growing queue.
type Inventory = (AuthorityReconciliation, AuthorityCapacityObservation);

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
                let Some(authority_coordinator_update::Update::Command(command)) = message.update else {
                    return Err(make_err!(Code::FailedPrecondition, "Unexpected authority negotiation message"));
                };
                worker.command(session, command)?;
                publish_inventory(&worker, &sender, session, &mut sequence)?;
            }
            observation = changed.changed() => {
                observation.map_err(|_| make_err!(Code::Unavailable, "Worker observation stream ended"))?;
                publish_inventory(&worker, &sender, session, &mut sequence)?;
            }
        }
    }
}
