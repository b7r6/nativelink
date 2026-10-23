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

//! Fixed, test-constructed listener roles for the experimental authority.
//!
//! No production binary or configuration selects this construction. The worker
//! listener has exactly Worker API, CAS and `ByteStream`; the client listener has
//! Execution, Operations, `ActionCache`, CAS, `ByteStream` and Capabilities. Their
//! opaque handles cannot accept extra routes.
//! The caller owns transport security and must exclude worker credentials from
//! every writable client route. Separate sockets alone do not enforce that.
//!
//! Construction rejects the same immediate store driver in both roles. Distinct
//! wrappers can still reach the same backend namespace: this local check cannot
//! certify provider prefixes, credentials, or deployment placement.

use std::future::Future;
use std::sync::Arc;

use futures::stream;
use nativelink_config::cas_server::{
    AcStoreConfig, ByteStreamConfig, WithInstanceName,
};
use nativelink_proto::build::bazel::remote::execution::v2::execution_server::ExecutionServer;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_server::AuthorityWorkerApiServer;
use nativelink_proto::google::longrunning::operations_server::OperationsServer;
use nativelink_scheduler::authority::journal::Singleton;
use nativelink_store::store_manager::StoreManager;
use tokio::net::TcpListener;
use tonic::service::Routes;
use tonic::transport::Server;

use crate::ac_server::AcServer;
use crate::authority_server::{AuthorityService, CoordinatorTask, QualificationConfig};
use crate::bytestream_server::ByteStreamServer;
use crate::capabilities_server::CapabilitiesServer;
use crate::cas_server::CasServer;
use crate::wire_compression::RemoteCacheCompressionInstances;

// Per-message wire ceilings, including protobuf envelopes. These do not bound
// concurrent RPCs or allocations made by a service before response encoding.
const CACHE_MESSAGE_BYTES: usize = 1024 * 1024;
const WORKER_MESSAGE_BYTES: usize = 1024 * 1024;
const CONTROL_MESSAGE_BYTES: usize = 64 * 1024;
const OPERATION_MESSAGE_BYTES: usize = 128 * 1024;
const READ_CHUNK_BYTES: usize = 64 * 1024;

/// A fixed worker route set. There is deliberately no route-builder accessor.
#[derive(Debug)]
pub struct WorkerListener {
    routes: Routes,
}

/// A fixed client route set. Its transport must exclude worker credentials.
#[derive(Debug)]
pub struct ClientListener {
    routes: Routes,
}

/// Owned coordinator and its two fixed listener roles, for local qualification.
#[derive(Debug)]
pub struct AuthorityListeners {
    pub service: AuthorityService,
    pub coordinator: CoordinatorTask,
    pub worker: WorkerListener,
    pub client: ClientListener,
}

impl AuthorityListeners {
    /// Bind existing service primitives to the exact stores used by the driver.
    /// On rejection, the consumed authority is shut down before returning.
    pub async fn start(authority: Singleton, config: QualificationConfig) -> Result<Self, String> {
        if Arc::ptr_eq(
            &config.cas.clone().into_inner(),
            &config.publication_sink.clone().into_inner(),
        ) {
            authority.shutdown().await?;
            return Err("Worker CAS and publication sink share the same store driver".to_owned());
        }

        let prepared = (|| {
            let stores = StoreManager::new();
            stores.add_store("authority_objects", config.cas.clone())?;
            stores.add_store("authority_sink", config.publication_sink.clone())?;
            let compression = RemoteCacheCompressionInstances::default();
            let cas = CasServer::new_authority(config.instance.clone(), config.cas.clone());
            let bytestream = ByteStreamServer::new(
                &[WithInstanceName {
                    instance_name: config.instance.clone(),
                    config: ByteStreamConfig {
                        cas_store: "authority_objects".to_owned(),
                        max_bytes_per_stream: READ_CHUNK_BYTES,
                        ..ByteStreamConfig::default()
                    },
                }],
                &stores,
                &compression,
            )?;
            let ac = AcServer::new(
                &[WithInstanceName {
                    instance_name: config.instance.clone(),
                    config: AcStoreConfig {
                        ac_store: "authority_sink".to_owned(),
                        read_only: false,
                    },
                }],
                &stores,
            )?;
            Ok::<_, nativelink_error::Error>((cas, bytestream, ac))
        })();
        let (cas, bytestream, ac) = match prepared {
            Ok(services) => services,
            Err(error) => {
                authority.shutdown().await?;
                return Err(error.to_string());
            }
        };
        let capabilities = CapabilitiesServer::new_authority(config.instance.clone());
        let (service, coordinator) = AuthorityService::start(authority, config).await?;
        let cas = cas
            .into_service()
            .max_decoding_message_size(CACHE_MESSAGE_BYTES)
            .max_encoding_message_size(CACHE_MESSAGE_BYTES);
        let bytestream = bytestream
            .into_service()
            .max_decoding_message_size(CACHE_MESSAGE_BYTES)
            .max_encoding_message_size(CACHE_MESSAGE_BYTES);
        let worker = WorkerListener {
            routes: Routes::new(
                AuthorityWorkerApiServer::new(service.clone())
                    .max_decoding_message_size(WORKER_MESSAGE_BYTES)
                    .max_encoding_message_size(WORKER_MESSAGE_BYTES),
            )
            .add_service(cas.clone())
            .add_service(bytestream.clone()),
        };
        let client = ClientListener {
            routes: Routes::new(
                ExecutionServer::new(service.clone())
                    .max_decoding_message_size(CONTROL_MESSAGE_BYTES)
                    .max_encoding_message_size(OPERATION_MESSAGE_BYTES),
            )
            .add_service(
                OperationsServer::new(service.clone())
                    .max_decoding_message_size(CONTROL_MESSAGE_BYTES)
                    .max_encoding_message_size(OPERATION_MESSAGE_BYTES),
            )
            .add_service(
                ac.into_service()
                    .max_decoding_message_size(CACHE_MESSAGE_BYTES)
                    .max_encoding_message_size(CACHE_MESSAGE_BYTES),
            )
            .add_service(cas)
            .add_service(bytestream)
            .add_service(
                capabilities
                    .into_service()
                    .max_decoding_message_size(CONTROL_MESSAGE_BYTES)
                    .max_encoding_message_size(CONTROL_MESSAGE_BYTES),
            ),
        };
        Ok(Self {
            service,
            coordinator,
            worker,
            client,
        })
    }
}

async fn serve(
    routes: Routes,
    mut transport: Server,
    listener: TcpListener,
    stopped: impl Future<Output = ()> + Send,
) -> Result<(), tonic::transport::Error> {
    let incoming = stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(io, _)| io), listener))
    });
    transport
        .add_routes(routes)
        .serve_with_incoming_shutdown(incoming, stopped)
        .await
}

impl WorkerListener {
    /// Serve the fixed routes using an existing transport/TLS configuration.
    pub async fn serve(
        self,
        transport: Server,
        listener: TcpListener,
        stopped: impl Future<Output = ()> + Send,
    ) -> Result<(), tonic::transport::Error> {
        serve(self.routes, transport, listener, stopped).await
    }
}

impl ClientListener {
    /// Serve the fixed routes using an existing transport/TLS configuration.
    pub async fn serve(
        self,
        transport: Server,
        listener: TcpListener,
        stopped: impl Future<Output = ()> + Send,
    ) -> Result<(), tonic::transport::Error> {
        serve(self.routes, transport, listener, stopped).await
    }
}
