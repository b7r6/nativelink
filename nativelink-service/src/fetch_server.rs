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
use std::borrow::Cow;
use std::collections::HashMap;

use nativelink_config::cas_server::{FetchConfig, OciRegistryConfig, WithInstanceName};
use nativelink_error::{Error, ResultExt, make_err, make_input_err};
use nativelink_oci::oci_client::{ImportConfig, OciToolchainClient};
use nativelink_oci::projection::{ProjectionDigestFunction, decompress_gzip};
use nativelink_oci::registry::{
    ImageReference, OciManifest, Reference, RegistryAuth, RegistrySettings,
};
use nativelink_oci_registry::records::{OciDigestAlias, OciTagRecord, alias_key, tag_key};
use nativelink_oci_registry::wire::parse_digest;
use nativelink_proto::build::bazel::remote::asset::v1::fetch_server::{
    Fetch, FetchServer as Server,
};
use nativelink_proto::build::bazel::remote::asset::v1::{
    FetchBlobRequest, FetchBlobResponse, FetchDirectoryRequest, FetchDirectoryResponse,
};
use nativelink_proto::build::bazel::remote::execution::v2::digest_function::Value as ProtoDigestFunction;
use nativelink_proto::google::rpc::Status as GoogleStatus;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{default_digest_hasher_func, make_ctx_for_hash_func};
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use opentelemetry::context::FutureExt;
use prost::Message;
use tonic::{Code, Request, Response, Status};
use tracing::{Instrument, Level, error_span, info, instrument};

use crate::remote_asset_proto::{RemoteAssetArtifact, RemoteAssetQuery};

/// Resolved OCI runtime configuration for one instance: the projection knobs
/// plus the per-registry connection + credential settings.
#[derive(Debug, Clone)]
struct OciRuntimeConfig {
    import: ImportConfig,
    registries: Vec<RegistrySettings>,
}

/// Resolved stores of a colocated `oci_registry` instance, enabling the
/// reserved `oci://self/...` local-projection short-circuit.
#[derive(Debug, Clone)]
struct SelfRegistryStores {
    blobs: Store,
    index: Store,
    refs: Store,
}

#[derive(Debug, Clone)]
pub struct FetchStoreInfo {
    store: Store,
    /// Optional CAS store for OCI toolchain imports (may differ from `fetch_store`).
    oci_cas_store: Option<Store>,
    /// OCI config (None if OCI is not configured for this instance).
    oci_config: Option<OciRuntimeConfig>,
    /// Local `oci_registry` stores for `oci://self/...` (None when not
    /// configured; such URIs then fail with a configuration error rather
    /// than falling back to a network host literally named "self").
    oci_self_stores: Option<SelfRegistryStores>,
}

/// Convert an operator's `OciRegistryConfig` into the runtime `RegistrySettings`,
/// validating the scheme and credential combination.
fn registry_settings_from_config(cfg: &OciRegistryConfig) -> Result<RegistrySettings, Error> {
    if cfg.host.trim().is_empty() {
        return Err(make_input_err!(
            "'oci.registries' entry has an empty 'host'"
        ));
    }
    let scheme = match cfg.scheme.as_deref() {
        None => "https".to_string(),
        Some(s) => {
            let s = s.to_lowercase();
            if s != "http" && s != "https" {
                return Err(make_input_err!(
                    "'oci.registries[{}].scheme' must be 'http' or 'https', got '{s}'",
                    cfg.host
                ));
            }
            s
        }
    };
    if cfg.bearer_token.is_some() && (cfg.username.is_some() || cfg.password.is_some()) {
        return Err(make_input_err!(
            "'oci.registries[{}]': 'bearer_token' is mutually exclusive with 'username'/'password'",
            cfg.host
        ));
    }
    if cfg.username.is_some() != cfg.password.is_some() {
        return Err(make_input_err!(
            "'oci.registries[{}]': 'username' and 'password' must be set together",
            cfg.host
        ));
    }
    Ok(RegistrySettings {
        host: cfg.host.clone(),
        scheme,
        root_certificates: cfg.root_certificates.clone(),
        insecure_skip_verify: cfg.insecure_skip_verify,
        auth: RegistryAuth {
            username: cfg.username.clone(),
            password: cfg.password.clone(),
            bearer_token: cfg.bearer_token.clone(),
        },
    })
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

                let registries = oci
                    .registries
                    .iter()
                    .map(registry_settings_from_config)
                    .collect::<Result<Vec<_>, _>>()?;

                (
                    Some(cas_store),
                    Some(OciRuntimeConfig {
                        import: import_config,
                        registries,
                    }),
                )
            } else {
                (None, None)
            };

            let oci_self_stores = if let Some(ref oci) = config.oci
                && let Some(ref self_registry) = oci.self_registry
            {
                let resolve = |name: &str, what: &str| {
                    store_manager.get_store(name).ok_or_else(|| {
                        make_input_err!("'oci.self_registry.{what}': '{name}' does not exist")
                    })
                };
                Some(SelfRegistryStores {
                    blobs: resolve(&self_registry.blob_store, "blob_store")?,
                    index: resolve(&self_registry.index_store, "index_store")?,
                    refs: resolve(&self_registry.ref_store, "ref_store")?,
                })
            } else {
                None
            };

            stores.insert(
                config.instance_name.clone(),
                FetchStoreInfo {
                    store,
                    oci_cas_store,
                    oci_config,
                    oci_self_stores,
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
                let asset_response = RemoteAssetArtifact::decode(asset_response_raw)
                    .err_tip(|| "Failed to decode stored RemoteAssetArtifact")?;
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
            // The reserved `oci://self/...` form: the local projection over
            // blobs already resident in the colocated oci_registry's
            // stores — no network client, nothing fetched twice.
            if uri.starts_with("oci://self/") {
                return self.handle_oci_self_fetch_directory(uri, store_info).await;
            }
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

    /// Reads one blob from the local registry stores by its OCI wire
    /// digest: alias record -> canonical `DigestInfo` -> blob bytes.
    async fn read_self_blob(
        self_stores: &SelfRegistryStores,
        sha256_hex: &str,
    ) -> Result<Vec<u8>, Error> {
        let raw = self_stores
            .index
            .get_part_unchunked(StoreKey::Str(Cow::Owned(alias_key(sha256_hex))), 0, None)
            .await
            .err_tip(|| format!("Looking up local OCI alias for sha256:{sha256_hex}"))?;
        if raw.is_empty() {
            return Err(make_err!(
                Code::NotFound,
                "local OCI registry has no blob sha256:{sha256_hex} (deleted alias)"
            ));
        }
        let alias =
            OciDigestAlias::decode_record(&raw).err_tip(|| "Decoding local OCI alias record")?;
        let digest = DigestInfo::try_new(&alias.canonical_hex, alias.size)
            .err_tip(|| "In local OCI alias record")?;
        let blob = self_stores
            .blobs
            .get_part_unchunked(digest, 0, None)
            .await
            .err_tip(|| format!("Reading local OCI blob sha256:{sha256_hex}"))?;
        Ok(blob.to_vec())
    }

    /// Handle a `FetchDirectory` for the reserved `oci://self/<name>:<ref>`
    /// form: resolve the manifest through the colocated `oci_registry`
    /// stores, read layer bytes locally, and run the SAME projection as the
    /// network path — the acquisition differs, the projection does not, so
    /// the root digest must agree with a network pull of the same image
    /// (the `oci-projection-differential` check).
    async fn handle_oci_self_fetch_directory(
        &self,
        uri: &str,
        store_info: &FetchStoreInfo,
    ) -> Result<Response<FetchDirectoryResponse>, Error> {
        let oci = store_info.oci_config.as_ref().ok_or_else(|| {
            make_err!(
                Code::Unimplemented,
                "OCI toolchain support not configured for this instance; \
                 add 'oci' section to FetchConfig"
            )
        })?;
        let self_stores = store_info.oci_self_stores.as_ref().ok_or_else(|| {
            make_err!(
                Code::Unimplemented,
                "'oci://self/...' requires 'oci.self_registry' store references \
                 in FetchConfig (the colocated oci_registry instance's stores)"
            )
        })?;
        let cas_store = store_info
            .oci_cas_store
            .as_ref()
            .unwrap_or(&store_info.store);

        info!(uri = uri, "Handling LOCAL OCI FetchDirectory (oci://self)");
        // Parse `<name>[:<tag>|@<digest>]` ourselves: the generic
        // `ImageReference::parse` host heuristic (dot/colon detection)
        // would fold the literal "self" into the repository name.
        let rest = uri
            .strip_prefix("oci://self/")
            .ok_or_else(|| make_input_err!("'{uri}' is not an oci://self/ URI"))?;
        let image = if let Some((name, digest)) = rest.split_once('@') {
            ImageReference {
                registry: "self".to_string(),
                repository: name.to_string(),
                reference: Reference::Digest(digest.to_string()),
            }
        } else {
            let slash = rest.rfind('/').map_or(0, |i| i + 1);
            match rest[slash..].rsplit_once(':') {
                Some((last, tag)) => ImageReference {
                    registry: "self".to_string(),
                    repository: format!("{}{last}", &rest[..slash]),
                    reference: Reference::Tag(tag.to_string()),
                },
                None => ImageReference {
                    registry: "self".to_string(),
                    repository: rest.to_string(),
                    reference: Reference::Tag("latest".to_string()),
                },
            }
        };
        if image.repository.is_empty() {
            return Err(make_input_err!("'{uri}' has an empty repository name"));
        }

        // Resolve the manifest's wire digest: a digest reference directly,
        // a tag through the ref store's tag record.
        let manifest_hex = match &image.reference {
            Reference::Digest(digest) => parse_digest(digest)
                .err_tip(|| "In oci://self digest reference")?
                .to_string(),
            Reference::Tag(tag) => {
                let raw = self_stores
                    .refs
                    .get_part_unchunked(
                        StoreKey::Str(Cow::Owned(tag_key(&image.repository, tag))),
                        0,
                        None,
                    )
                    .await
                    .err_tip(|| format!("Looking up local OCI tag '{}:{tag}'", image.repository))?;
                if raw.is_empty() {
                    return Err(make_err!(
                        Code::NotFound,
                        "local OCI registry has no tag '{}:{tag}'",
                        image.repository
                    ));
                }
                OciTagRecord::decode_record(&raw)
                    .err_tip(|| "Decoding local OCI tag record")?
                    .manifest_sha256_hex
            }
        };

        let manifest_bytes = Self::read_self_blob(self_stores, &manifest_hex).await?;
        let manifest: OciManifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|e| make_input_err!("local OCI manifest is not valid JSON: {e}"))?;

        // Read + decompress layers from the local blob store, mirroring the
        // network path's media-type handling exactly.
        let mut decompressed_layers = Vec::with_capacity(manifest.layers.len());
        for layer in &manifest.layers {
            let layer_hex =
                parse_digest(&layer.digest).err_tip(|| "In local OCI layer descriptor")?;
            let blob = Self::read_self_blob(self_stores, layer_hex).await?;
            let raw_tar = if layer.media_type.contains("+gzip")
                || layer.media_type.contains(".gzip")
                || layer.media_type == "application/vnd.docker.image.rootfs.diff.tar.gzip"
            {
                decompress_gzip(&blob).err_tip(|| "Decompressing local OCI layer")?
            } else {
                blob
            };
            decompressed_layers.push(raw_tar);
        }
        let layer_refs: Vec<&[u8]> = decompressed_layers.iter().map(Vec::as_slice).collect();

        // The projection and CAS upload are IDENTICAL to the network path.
        let client = OciToolchainClient::with_config(oci.import, Vec::new())
            .err_tip(|| "Creating OCI toolchain client for oci://self")?;
        let result = client
            .import_from_layers(&layer_refs, &manifest.annotations, cas_store)
            .await
            .err_tip(|| format!("Local OCI toolchain import for '{uri}'"))?;

        info!(
            uri = uri,
            root_digest = %result.root_digest,
            files_uploaded = result.files_uploaded,
            files_deduped = result.files_deduped,
            "Local OCI toolchain projected (no network)"
        );

        let proto_digest_func: i32 = match oci.import.digest_function {
            ProjectionDigestFunction::Blake3 => ProtoDigestFunction::Blake3.into(),
            ProjectionDigestFunction::Sha256 => ProtoDigestFunction::Sha256.into(),
        };
        Ok(Response::new(FetchDirectoryResponse {
            status: Some(GoogleStatus {
                code: Code::Ok.into(),
                message: format!(
                    "OCI toolchain projected locally: {} files ({} deduped), {} directories",
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

    /// Handle a `FetchDirectory` request for an OCI image URI.
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
        let oci = store_info.oci_config.as_ref().ok_or_else(|| {
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
        let client = OciToolchainClient::with_config(oci.import, oci.registries.clone())
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

        // Map our digest function to the generated proto enum constants.
        let proto_digest_func: i32 = match oci.import.digest_function {
            ProjectionDigestFunction::Blake3 => ProtoDigestFunction::Blake3.into(),
            ProjectionDigestFunction::Sha256 => ProtoDigestFunction::Sha256.into(),
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
