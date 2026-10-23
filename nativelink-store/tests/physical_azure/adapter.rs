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

//! Local qualification adapter, compiled only into the explicit Azurite test.
//! Identity allocation, retirement authority, and unresolved effects stay with
//! its caller. No cloud constructor, `StoreDriver`, environment auth, or retries.

use std::num::NonZero;
use std::sync::Arc;
use std::time::Duration;

use azure_core::error::ErrorKind;
use azure_core::http::{Etag, HttpClient, RequestContent, RetryOptions, StatusCode, Transport};
use azure_storage_blob::models::{
    BlobClientDeleteOptions, BlobClientDownloadOptions, BlobClientGetPropertiesResultHeaders,
    BlockBlobClientUploadOptions,
};
use azure_storage_blob::{BlobContainerClient, BlobContainerClientOptions};
use futures::TryStreamExt;
use nativelink_store::physical_filesystem::{
    Algorithm, DeleteReceipt, Failure, FailureKind, Incarnation, ObjectId, PhysicalKey,
    VerifiedRead, WriteReceipt,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::{Host, Url};

pub(super) const OBJECT_LIMIT: usize = 16 * 1024 * 1024;
const HEADER_SIZE: usize = 73;
const REQUEST_LIMIT: usize = 4;
const API_VERSION: &str = "2020-12-06";

const fn fail(kind: FailureKind, operation: &'static str) -> Failure {
    Failure {
        kind,
        operation,
        io_kind: None,
    }
}

pub(super) fn provider_error(error: &azure_core::Error, operation: &'static str) -> Failure {
    // A missing account/container, malformed endpoint, denied credential, or
    // unknown 404 is unavailable. Only the provider's exact object miss counts.
    let kind = match error.kind() {
        ErrorKind::HttpResponse {
            status: StatusCode::NotFound,
            error_code: Some(code),
            ..
        } if code == "BlobNotFound" => FailureKind::Missing,
        ErrorKind::HttpResponse {
            status: StatusCode::PreconditionFailed,
            error_code: Some(code),
            ..
        } if code == "ConditionNotMet" => FailureKind::Conflict,
        ErrorKind::HttpResponse {
            status: StatusCode::Conflict,
            error_code: Some(code),
            ..
        } if code == "BlobAlreadyExists" => FailureKind::Conflict,
        _ => FailureKind::Unavailable,
    };
    fail(kind, operation)
}

fn validate_endpoint(endpoint: &str) -> Result<Url, Failure> {
    if endpoint.len() > 8192 {
        return Err(fail(FailureKind::Invalid, "local endpoint bound"));
    }
    let url = Url::parse(endpoint).map_err(|_| fail(FailureKind::Invalid, "local endpoint"))?;
    let loopback = match url.host() {
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    };
    let segments: Vec<_> = url.path_segments().into_iter().flatten().collect();
    if url.scheme() != "http"
        || !loopback
        || url.port().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || segments.len() != 2
        || segments[0] != "devstoreaccount1"
        || segments[1].is_empty()
        || url.query_pairs().any(|(key, _)| {
            !matches!(
                key.as_ref(),
                "sv" | "sr" | "sp" | "se" | "sig" | "st" | "spr" | "sip"
            )
        })
    {
        return Err(fail(
            FailureKind::Invalid,
            "loopback Azurite container required",
        ));
    }
    Ok(url)
}

pub(super) fn network_transport() -> Result<Arc<dyn HttpClient>, Failure> {
    drop(rustls::crypto::ring::default_provider().install_default());
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| fail(FailureKind::Unavailable, "local HTTP client"))?;
    Ok(Arc::new(client))
}

pub(super) fn container_client(
    endpoint: &str,
    transport: Arc<dyn HttpClient>,
) -> Result<BlobContainerClient, Failure> {
    let url = validate_endpoint(endpoint)?;
    let mut options = BlobContainerClientOptions {
        version: API_VERSION.into(),
        ..Default::default()
    };
    options.client_options.retry = RetryOptions::none();
    options.client_options.transport = Some(Transport::new(transport));
    BlobContainerClient::new(url, None, Some(options))
        .map_err(|error| provider_error(&error, "local container client"))
}

pub(super) fn object_name(key: PhysicalKey) -> String {
    format!(
        "physical-v1/{}/objects-v1/{:016x}",
        key.incarnation.directory_name(),
        key.generation()
    )
}

fn manifest_name(incarnation: Incarnation) -> String {
    format!("physical-v1/{}/manifest-v1", incarnation.directory_name())
}

fn manifest(incarnation: Incarnation) -> Vec<u8> {
    let mut bytes = b"AZPHYNS1".to_vec();
    bytes.extend_from_slice(&incarnation.as_bytes());
    bytes.extend_from_slice(&(OBJECT_LIMIT as u64).to_be_bytes());
    bytes
}

// The same physical envelope as the filesystem fixture. A cross-provider test
// compares actual file/blob bytes so an encoding change cannot drift silently.
fn header(key: PhysicalKey, object: ObjectId) -> Vec<u8> {
    let mut bytes = b"PHYSOBJ1".to_vec();
    bytes.extend_from_slice(&key.incarnation.as_bytes());
    bytes.extend_from_slice(&key.generation().to_be_bytes());
    bytes.push(match object.algorithm {
        Algorithm::Sha256 => 1,
        Algorithm::Blake3 => 2,
    });
    bytes.extend_from_slice(&object.size.to_be_bytes());
    bytes.extend_from_slice(&object.hash);
    bytes
}

fn upload_options() -> BlockBlobClientUploadOptions<'static> {
    BlockBlobClientUploadOptions {
        parallel: NonZero::new(1),
        partition_size: NonZero::new((OBJECT_LIMIT + HEADER_SIZE) as u64),
        per_request_timeout: Some(10),
        ..Default::default()
    }
    .if_not_exists()
}

#[derive(Clone)]
pub(super) struct LocalAzurePhysical {
    client: Arc<BlobContainerClient>,
    incarnation: Incarnation,
    permits: Arc<Semaphore>,
}

#[derive(Clone, Debug)]
pub(super) struct PreparedDelete {
    key: PhysicalKey,
    delete: u64,
    etag: Option<Etag>,
}

impl LocalAzurePhysical {
    fn connect(
        endpoint: &str,
        incarnation: Incarnation,
        transport: Arc<dyn HttpClient>,
    ) -> Result<Self, Failure> {
        Ok(Self {
            client: Arc::new(container_client(endpoint, transport)?),
            incarnation,
            permits: Arc::new(Semaphore::new(REQUEST_LIMIT)),
        })
    }

    pub(super) async fn bootstrap(
        endpoint: &str,
        incarnation: Incarnation,
        transport: Arc<dyn HttpClient>,
    ) -> Result<Self, Failure> {
        let store = Self::connect(endpoint, incarnation, transport)?;
        store
            .client
            .blob_client(&manifest_name(incarnation))
            .block_blob_client()
            .upload(
                RequestContent::from(manifest(incarnation)),
                Some(upload_options()),
            )
            .await
            .map_err(|error| provider_error(&error, "bootstrap physical namespace"))?;
        Ok(store)
    }

    pub(super) async fn open(
        endpoint: &str,
        incarnation: Incarnation,
        transport: Arc<dyn HttpClient>,
    ) -> Result<Self, Failure> {
        let store = Self::connect(endpoint, incarnation, transport)?;
        let expected = manifest(incarnation);
        let actual = store
            .download(&manifest_name(incarnation), expected.len())
            .await
            .map_err(|mut failure| {
                if failure.kind == FailureKind::Missing {
                    failure.kind = FailureKind::Unavailable;
                }
                failure
            })?;
        if actual != expected {
            return Err(fail(FailureKind::Corrupt, "physical namespace identity"));
        }
        Ok(store)
    }

    fn enter(
        &self,
        key: PhysicalKey,
        object: Option<ObjectId>,
    ) -> Result<OwnedSemaphorePermit, Failure> {
        if key.incarnation != self.incarnation {
            return Err(fail(FailureKind::Conflict, "foreign incarnation"));
        }
        if object.is_some_and(|id| id.size > OBJECT_LIMIT as u64) {
            return Err(fail(FailureKind::Capacity, "object byte bound"));
        }
        self.permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| fail(FailureKind::Capacity, "physical request bound"))
    }

    async fn download(&self, name: &str, expected_size: usize) -> Result<Vec<u8>, Failure> {
        let blob = self.client.blob_client(name);
        let properties = blob
            .get_properties(None)
            .await
            .map_err(|error| provider_error(&error, "physical properties"))?;
        if properties.content_length().ok().flatten() != Some(expected_size as u64) {
            return Err(fail(FailureKind::Corrupt, "physical object length"));
        }
        let etag = properties
            .etag()
            .ok()
            .flatten()
            .filter(|tag| tag.as_ref().len() <= 256)
            .ok_or_else(|| fail(FailureKind::Corrupt, "physical ETag"))?;
        let response = blob
            .download(Some(BlobClientDownloadOptions {
                if_match: Some(etag),
                range: Some((0..expected_size as u64).into()),
                parallel: NonZero::new(1),
                partition_size: NonZero::new(OBJECT_LIMIT + HEADER_SIZE),
                timeout: Some(10),
                ..Default::default()
            }))
            .await
            .map_err(|error| provider_error(&error, "physical download"))?;
        let mut body = response.body;
        let mut bytes = Vec::with_capacity(expected_size);
        while let Some(chunk) = body
            .try_next()
            .await
            .map_err(|error| provider_error(&error, "physical download body"))?
        {
            if bytes
                .len()
                .checked_add(chunk.len())
                .is_none_or(|size| size > expected_size)
            {
                return Err(fail(FailureKind::Corrupt, "physical object grew"));
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != expected_size {
            return Err(fail(FailureKind::Corrupt, "physical object truncated"));
        }
        Ok(bytes)
    }

    async fn verified(&self, key: PhysicalKey, object: ObjectId) -> Result<Vec<u8>, Failure> {
        let size = usize::try_from(object.size)
            .map_err(|_| fail(FailureKind::Capacity, "object byte bound"))?;
        let bytes = self.download(&object_name(key), size + HEADER_SIZE).await?;
        if bytes[..HEADER_SIZE] != header(key, object) {
            return Err(fail(FailureKind::Corrupt, "physical descriptor"));
        }
        if ObjectId::for_bytes(object.algorithm, &bytes[HEADER_SIZE..]) != object {
            return Err(fail(FailureKind::Corrupt, "physical digest"));
        }
        Ok(bytes)
    }

    pub(super) async fn write(
        &self,
        key: PhysicalKey,
        write: u64,
        object: ObjectId,
        payload: &[u8],
    ) -> Result<WriteReceipt, Failure> {
        let _permit = self.enter(key, Some(object))?;
        if write == 0 {
            return Err(fail(FailureKind::Invalid, "zero write identity"));
        }
        if payload.len() > OBJECT_LIMIT || ObjectId::for_bytes(object.algorithm, payload) != object
        {
            return Err(fail(FailureKind::Corrupt, "upload identity"));
        }
        let mut bytes = header(key, object);
        bytes.extend_from_slice(payload);
        let result = self
            .client
            .blob_client(&object_name(key))
            .block_blob_client()
            .upload(RequestContent::from(bytes), Some(upload_options()))
            .await;
        if let Err(error) = result {
            let failure = provider_error(&error, "physical upload");
            if failure.kind != FailureKind::Conflict {
                return Err(failure);
            }
            // A previous ambiguous request may have committed. No replacement
            // occurs; only whole-object verification can yield a retry receipt.
            self.verified(key, object).await.map_err(|mut failure| {
                if failure.kind == FailureKind::Corrupt
                    && matches!(
                        failure.operation,
                        "physical descriptor" | "physical object length"
                    )
                {
                    failure.kind = FailureKind::Conflict;
                }
                failure
            })?;
        }
        Ok(WriteReceipt { key, write, object })
    }

    pub(super) async fn read(
        &self,
        key: PhysicalKey,
        object: ObjectId,
        offset: u64,
        length: Option<u64>,
    ) -> Result<VerifiedRead, Failure> {
        let _permit = self.enter(key, Some(object))?;
        let length = length.unwrap_or_else(|| object.size.saturating_sub(offset));
        let end = offset
            .checked_add(length)
            .filter(|end| offset <= object.size && *end <= object.size)
            .ok_or_else(|| fail(FailureKind::Invalid, "physical range"))?;
        let bytes = self.verified(key, object).await?;
        let start =
            usize::try_from(offset).map_err(|_| fail(FailureKind::Invalid, "physical range"))?;
        let end = usize::try_from(end).map_err(|_| fail(FailureKind::Invalid, "physical range"))?;
        Ok(VerifiedRead {
            key,
            object,
            offset,
            bytes: bytes[HEADER_SIZE + start..HEADER_SIZE + end].to_vec(),
        })
    }

    pub(super) async fn prepare_delete(
        &self,
        key: PhysicalKey,
        delete: u64,
    ) -> Result<PreparedDelete, Failure> {
        let _permit = self.enter(key, None)?;
        if delete == 0 {
            return Err(fail(FailureKind::Invalid, "zero delete identity"));
        }
        let etag = match self
            .client
            .blob_client(&object_name(key))
            .get_properties(None)
            .await
        {
            Ok(properties) => Some(
                properties
                    .etag()
                    .ok()
                    .flatten()
                    .filter(|tag| tag.as_ref().len() <= 256)
                    .ok_or_else(|| fail(FailureKind::Corrupt, "physical ETag"))?,
            ),
            Err(error)
                if provider_error(&error, "delete properties").kind == FailureKind::Missing =>
            {
                None
            }
            Err(error) => return Err(provider_error(&error, "delete properties")),
        };
        Ok(PreparedDelete { key, delete, etag })
    }

    pub(super) async fn finish_delete(
        &self,
        intent: PreparedDelete,
    ) -> Result<DeleteReceipt, Failure> {
        let _permit = self.enter(intent.key, None)?;
        let blob = self.client.blob_client(&object_name(intent.key));
        let result = if let Some(etag) = intent.etag {
            Some(
                blob.delete(Some(BlobClientDeleteOptions {
                    if_match: Some(etag),
                    timeout: Some(10),
                    ..Default::default()
                }))
                .await,
            )
        } else {
            // Azurite ignores If-None-Match:* for DELETE of an existing blob.
            // Never issue DELETE without an observed exact ETag.
            None
        };
        let already_absent = match result {
            Some(Ok(_)) => false,
            Some(Err(error))
                if provider_error(&error, "physical delete").kind == FailureKind::Missing =>
            {
                true
            }
            Some(Err(error))
                if provider_error(&error, "physical delete").kind != FailureKind::Conflict =>
            {
                return Err(provider_error(&error, "physical delete"));
            }
            _ => {
                // Azurite returns 412 for If-Match against an absent blob.
                // Reconcile the exact locator, without an unconditional delete.
                // Absence does not fence an unresolved old PUT.
                match blob.get_properties(None).await {
                    Err(error)
                        if provider_error(&error, "delete reconciliation").kind
                            == FailureKind::Missing =>
                    {
                        true
                    }
                    Err(error) => {
                        return Err(provider_error(&error, "delete reconciliation"));
                    }
                    Ok(_) => return Err(fail(FailureKind::Conflict, "physical delete")),
                }
            }
        };
        Ok(DeleteReceipt {
            key: intent.key,
            delete: intent.delete,
            already_absent,
        })
    }
}
