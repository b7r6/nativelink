// Copyright 2025 The NativeLink Authors. All rights reserved.
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

use core::convert::Into;
use std::collections::HashMap;

use nativelink_config::cas_server::{FetchConfig, WithInstanceName};
use nativelink_error::{Error, ResultExt, make_err, make_input_err};
use nativelink_oci::oci_client::{ImportConfig, OciToolchainClient};
use nativelink_oci::projection::ProjectionDigestFunction;
use nativelink_proto::build::bazel::remote::asset::v1::fetch_server::{
    Fetch, FetchServer as Server,
};
use nativelink_proto::build::bazel::remote::asset::v1::{
    FetchBlobRequest, FetchBlobResponse, FetchDirectoryRequest, FetchDirectoryResponse,
};
use nativelink_proto::google::rpc::Status as GoogleStatus;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::digest_hasher::{default_digest_hasher_func, make_ctx_for_hash_func};
use nativelink_util::store_trait::{Store, StoreLike};
use opentelemetry::context::FutureExt;
use prost::Message;
use tonic::{Code, Request, Response, Status};
use tracing::{Instrument, Level, error_span, info, instrument};

use crate::remote_asset_proto::{RemoteAssetArtifact, RemoteAssetQuery};

#[derive(Debug, Clone)]
pub struct FetchStoreInfo {
    store: Store,
    /// Optional CAS store for OCI toolchain imports (may differ from fetch_store).
    oci_cas_store: Option<Store>,
    /// OCI import config (None if OCI is not configured for this instance).
    oci_config: Option<ImportConfig>,
}

#[derive(Debug, Clone)]
pub struct FetchServer {
    stores: HashMap<String, FetchStoreInfo>,
}

impl FetchServer {
    pub fn new(
        configs: &[WithInstanceName<FetchConfig>],
        store_manager: &StoreManager,
    ) -> Result<Self, Error> {
        let mut stores = HashMap::with_capacity(configs.len());
        for config in configs {
            let store = store_manager
                .get_store(&config.fetch_store)
                .ok_or_else(|| {
                    make_input_err!("'fetch_store': '{}' does not exist", config.fetch_store)
                })?;

            // Configure OCI support if present
            let (oci_cas_store, oci_config) = if let Some(ref oci) = config.oci {
                let cas_store = if let Some(ref cas_store_name) = oci.cas_store {
                    store_manager.get_store(cas_store_name).ok_or_else(|| {
                        make_input_err!("'oci.cas_store': '{}' does not exist", cas_store_name)
                    })?
                } else {
                    store.clone()
                };

                let digest_function = match oci.digest_function.to_uppercase().as_str() {
                    "BLAKE3" => ProjectionDigestFunction::Blake3,
                    "SHA256" | "SHA-256" => ProjectionDigestFunction::Sha256,
                    other => {
                        return Err(make_input_err!(
                            "Unsupported OCI digest function: '{}' (expected BLAKE3 or SHA256)",
                            other
                        ));
                    }
                };

                let import_config = ImportConfig {
                    digest_function,
                    dedup_check: oci.dedup_check,
                    strict_hint_verification: false,
                };

                (Some(cas_store), Some(import_config))
            } else {
                (None, None)
            };

            stores.insert(
                config.instance_name.clone(),
                FetchStoreInfo {
                    store,
                    oci_cas_store,
                    oci_config,
                },
            );
        }
        Ok(Self {
            stores: stores.clone(),
        })
    }

    pub fn into_service(self) -> Server<Self> {
        Server::new(self)
    }

    async fn inner_fetch_blob(
        &self,
        request: FetchBlobRequest,
    ) -> Result<Response<FetchBlobResponse>, Error> {
        let instance_name = &request.instance_name;
        let store_info = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?;

        if request.uris.is_empty() {
            return Err(Error::new(
                Code::InvalidArgument,
                "No uris in fetch request".to_owned(),
            ));
        }
        for uri in &request.uris {
            let asset_request = RemoteAssetQuery::new(uri.clone(), request.qualifiers.clone());
            let asset_digest = asset_request.digest();
            let asset_response_possible = store_info
                .store
                .get_part_unchunked(asset_digest, 0, None)
                .await;

            info!(
                uri = uri,
                digest = format!("{}", asset_digest),
                "Looked up fetch asset"
            );

            if let Ok(asset_response_raw) = asset_response_possible {
                let asset_response = RemoteAssetArtifact::decode(asset_response_raw).unwrap();
                return Ok(Response::new(FetchBlobResponse {
                    status: Some(GoogleStatus {
                        code: Code::Ok.into(),
                        message: "Fetch object found".to_owned(),
                        details: vec![],
                    }),
                    uri: asset_response.uri,
                    qualifiers: asset_response.qualifiers,
                    expires_at: asset_response.expire_at,
                    blob_digest: asset_response.blob_digest,
                    digest_function: asset_response.digest_function,
                }));
            }
        }
        Ok(Response::new(FetchBlobResponse {
            status: Some(make_err!(Code::NotFound, "No item found").into()),
            uri: request.uris.first().cloned().unwrap_or(String::new()),
            qualifiers: vec![],
            expires_at: None,
            blob_digest: None,
            digest_function: default_digest_hasher_func().proto_digest_func().into(),
        }))
    }

    async fn inner_fetch_directory(
        &self,
        request: FetchDirectoryRequest,
    ) -> Result<Response<FetchDirectoryResponse>, Error> {
        let instance_name = &request.instance_name;
        let store_info = self
            .stores
            .get(instance_name)
            .err_tip(|| format!("'instance_name' not configured for '{instance_name}'"))?;

        if request.uris.is_empty() {
            return Err(Error::new(
                Code::InvalidArgument,
                "No uris in fetch_directory request".to_owned(),
            ));
        }

        // Check for OCI URIs — these get special handling via the OCI toolchain client
        for uri in &request.uris {
            if uri.starts_with("oci://") || uri.starts_with("docker://") {
                return self
                    .handle_oci_fetch_directory(uri, store_info, request.digest_function)
                    .await;
            }
        }

        // Non-OCI URIs: fall back to the remote asset lookup (same pattern as fetch_blob)
        for uri in &request.uris {
            let asset_request = RemoteAssetQuery::new(uri.clone(), request.qualifiers.clone());
            let asset_digest = asset_request.digest();
            let asset_response_possible = store_info
                .store
                .get_part_unchunked(asset_digest, 0, None)
                .await;

            info!(
                uri = uri,
                digest = format!("{}", asset_digest),
                "Looked up fetch_directory asset"
            );

            if let Ok(asset_response_raw) = asset_response_possible {
                let asset_response = RemoteAssetArtifact::decode(asset_response_raw).unwrap();
                return Ok(Response::new(FetchDirectoryResponse {
                    status: Some(GoogleStatus {
                        code: Code::Ok.into(),
                        message: "FetchDirectory object found".to_owned(),
                        details: vec![],
                    }),
                    uri: asset_response.uri,
                    qualifiers: asset_response.qualifiers,
                    expires_at: asset_response.expire_at,
                    root_directory_digest: asset_response.blob_digest,
                    digest_function: asset_response.digest_function,
                }));
            }
        }

        Ok(Response::new(FetchDirectoryResponse {
            status: Some(make_err!(Code::NotFound, "No item found").into()),
            uri: request.uris.first().cloned().unwrap_or(String::new()),
            qualifiers: vec![],
            expires_at: None,
            root_directory_digest: None,
            digest_function: default_digest_hasher_func().proto_digest_func().into(),
        }))
    }

    /// Handle a FetchDirectory request for an OCI image URI.
    ///
    /// This implements the OCI→REAPI bridge per Standard OCI Toolchain Spec §6:
    /// pulls the image, projects layers into an REAPI Directory tree, uploads
    /// all blobs to CAS, and returns the root Directory digest.
    async fn handle_oci_fetch_directory(
        &self,
        uri: &str,
        store_info: &FetchStoreInfo,
        _digest_function_proto: i32,
    ) -> Result<Response<FetchDirectoryResponse>, Error> {
        // Verify OCI is configured for this instance
        let oci_config = store_info.oci_config.as_ref().ok_or_else(|| {
            make_err!(
                Code::Unimplemented,
                "OCI toolchain support not configured for this instance; \
                 add 'oci' section to FetchConfig"
            )
        })?;

        let cas_store = store_info
            .oci_cas_store
            .as_ref()
            .unwrap_or(&store_info.store);

        info!(uri = uri, "Handling OCI FetchDirectory request");

        // Create the OCI client and run the import
        let client = OciToolchainClient::with_config(*oci_config)
            .err_tip(|| "Creating OCI toolchain client")?;

        let result = client
            .import(uri, cas_store)
            .await
            .err_tip(|| format!("OCI toolchain import for '{uri}'"))?;

        info!(
            uri = uri,
            root_digest = %result.root_digest,
            files_uploaded = result.files_uploaded,
            files_deduped = result.files_deduped,
            directories_uploaded = result.directories_uploaded,
            bytes_uploaded = result.bytes_uploaded,
            "OCI toolchain imported successfully"
        );

        // Map our digest function to the proto enum
        let proto_digest_func: i32 = match oci_config.digest_function {
            ProjectionDigestFunction::Blake3 => {
                // BLAKE3 = 7 in the proto enum
                7
            }
            ProjectionDigestFunction::Sha256 => {
                // SHA256 = 1 in the proto enum
                1
            }
        };

        Ok(Response::new(FetchDirectoryResponse {
            status: Some(GoogleStatus {
                code: Code::Ok.into(),
                message: format!(
                    "OCI toolchain imported: {} files ({} deduped), {} directories",
                    result.files_uploaded, result.files_deduped, result.directories_uploaded,
                ),
                details: vec![],
            }),
            uri: uri.to_string(),
            qualifiers: vec![],
            expires_at: None,
            root_directory_digest: Some(result.root_digest.into()),
            digest_function: proto_digest_func,
        }))
    }
}

#[tonic::async_trait]
impl Fetch for FetchServer {
    #[allow(clippy::blocks_in_conditions)]
    #[instrument(
        err(level = Level::WARN),
        ret(level = Level::INFO),
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn fetch_blob(
        &self,
        grpc_request: Request<FetchBlobRequest>,
    ) -> Result<Response<FetchBlobResponse>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;
        self.inner_fetch_blob(request)
            .instrument(error_span!("fetch_server_fetch_blob"))
            .with_context(
                make_ctx_for_hash_func(digest_function).err_tip(|| "In FetchServer::fetch_blob")?,
            )
            .await
            .err_tip(|| "Failed on fetch_blob() command")
            .map_err(Into::into)
    }

    #[allow(clippy::blocks_in_conditions)]
    #[instrument(
        err(level = Level::WARN),
        ret(level = Level::INFO),
        skip_all,
        fields(request = ?grpc_request.get_ref())
    )]
    async fn fetch_directory(
        &self,
        grpc_request: Request<FetchDirectoryRequest>,
    ) -> Result<Response<FetchDirectoryResponse>, Status> {
        let request = grpc_request.into_inner();
        let digest_function = request.digest_function;
        self.inner_fetch_directory(request)
            .instrument(error_span!("fetch_server_fetch_directory"))
            .with_context(
                make_ctx_for_hash_func(digest_function)
                    .err_tip(|| "In FetchServer::fetch_directory")?,
            )
            .await
            .err_tip(|| "Failed on fetch_directory() command")
            .map_err(Into::into)
    }
}
