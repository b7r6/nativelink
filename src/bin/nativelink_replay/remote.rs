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

//! The gRPC side: connecting, reading blobs with digest verification, and
//! executing an action.

use core::time::Duration;
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_client::ContentAddressableStorageClient;
use nativelink_proto::build::bazel::remote::execution::v2::execution_client::ExecutionClient;
use nativelink_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, ExecuteRequest, ExecuteResponse, WaitExecutionRequest,
};
use nativelink_proto::google::bytestream::ReadRequest;
use nativelink_proto::google::bytestream::byte_stream_client::ByteStreamClient;
use nativelink_proto::google::longrunning::{Operation, operation};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::tls_utils::endpoint_from;
use prost::Message;
use tonic::metadata::{Ascii, MetadataKey, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::{Request, Status};

use crate::manifest::{Problem, digest_of};

/// Largest gRPC message accepted from a server.
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// How many times a dropped `Execute` stream is resumed with `WaitExecution`.
const MAX_WAIT_RESUMES: usize = 20;

/// Attaches the `--header` values to every request.
#[derive(Clone, Debug, Default)]
pub(crate) struct HeaderInterceptor {
    headers: Arc<Vec<(MetadataKey<Ascii>, MetadataValue<Ascii>)>>,
}

impl HeaderInterceptor {
    pub(crate) fn new(headers: &[(String, String)]) -> Result<Self, Error> {
        let mut parsed = Vec::with_capacity(headers.len());
        for (key, value) in headers {
            let key = MetadataKey::<Ascii>::from_bytes(key.to_ascii_lowercase().as_bytes())
                .map_err(|_| {
                    make_input_err!("--header key '{key}' is not a valid ASCII metadata key")
                })?;
            let value = MetadataValue::<Ascii>::try_from(value.as_str()).map_err(|_| {
                make_input_err!("--header value for '{key}' is not valid ASCII metadata")
            })?;
            parsed.push((key, value));
        }
        Ok(Self {
            headers: Arc::new(parsed),
        })
    }
}

impl Interceptor for HeaderInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        for (key, value) in self.headers.iter() {
            request.metadata_mut().insert(key.clone(), value.clone());
        }
        Ok(request)
    }
}

type Svc = InterceptedService<Channel, HeaderInterceptor>;

/// How blobs are fetched: small blobs in `BatchReadBlobs` requests, larger
/// ones over `ByteStream.Read`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FetchLimits {
    /// Blobs up to this size go through `BatchReadBlobs`; `0` sends every
    /// blob through `ByteStream`.
    pub(crate) batch_max_blob: u64,
    /// Upper bound on the summed size of one `BatchReadBlobs` request.
    pub(crate) batch_max_total: u64,
    /// Requests in flight at once.
    pub(crate) concurrency: usize,
}

impl Default for FetchLimits {
    fn default() -> Self {
        Self {
            batch_max_blob: 1024 * 1024,
            batch_max_total: 3 * 1024 * 1024,
            concurrency: 8,
        }
    }
}

/// Where to connect and how.
#[derive(Clone, Debug, Default)]
pub(crate) struct ConnectOptions {
    pub(crate) cas: String,
    pub(crate) execution: Option<String>,
    pub(crate) connect_execution: bool,
    pub(crate) instance_name: String,
    pub(crate) digest_function: Option<DigestHasherFunc>,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) tls_ca_cert: Option<PathBuf>,
}

/// A connected CAS, and optionally an execution service.
#[derive(Clone, Debug)]
pub(crate) struct Remote {
    pub(crate) instance_name: String,
    pub(crate) func: DigestHasherFunc,
    pub(crate) limits: FetchLimits,
    cas: ContentAddressableStorageClient<Svc>,
    bytestream: ByteStreamClient<Svc>,
    execution: Option<ExecutionClient<Svc>>,
}

/// `grpc://` is plaintext and `grpcs://` is TLS, as Bazel spells them.
pub(crate) fn normalize_url(url: &str) -> String {
    url.strip_prefix("grpc://").map_or_else(
        || {
            url.strip_prefix("grpcs://")
                .map_or_else(|| url.to_string(), |rest| format!("https://{rest}"))
        },
        |rest| format!("http://{rest}"),
    )
}

async fn connect_channel(url: &str, tls_ca_cert: Option<&Path>) -> Result<Channel, Error> {
    let url = normalize_url(url);
    let tls = if url.starts_with("https://") {
        let mut config = ClientTlsConfig::new().with_enabled_roots();
        if let Some(path) = tls_ca_cert {
            let pem = std::fs::read(path)
                .err_tip(|| format!("reading --tls-ca-cert {}", path.display()))?;
            config = config.ca_certificate(Certificate::from_pem(pem));
        }
        Some(config)
    } else {
        None
    };
    let endpoint = endpoint_from(&url, tls)?
        .connect_timeout(Duration::from_secs(30))
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_while_idle(true);
    endpoint
        .connect()
        .await
        .map_err(|e| make_err!(Code::Unavailable, "connecting to {url}: {e}"))
}

impl Remote {
    pub(crate) async fn connect(options: &ConnectOptions) -> Result<Self, Error> {
        let interceptor = HeaderInterceptor::new(&options.headers)?;
        let cas_channel = connect_channel(&options.cas, options.tls_ca_cert.as_deref()).await?;
        let execution = if options.connect_execution {
            let channel = match &options.execution {
                Some(url) if url != &options.cas => {
                    connect_channel(url, options.tls_ca_cert.as_deref()).await?
                }
                _ => cas_channel.clone(),
            };
            Some(
                ExecutionClient::with_interceptor(channel, interceptor.clone())
                    .max_decoding_message_size(MAX_MESSAGE_BYTES),
            )
        } else {
            None
        };
        Ok(Self {
            instance_name: options.instance_name.clone(),
            func: options.digest_function.unwrap_or(DigestHasherFunc::Sha256),
            limits: FetchLimits::default(),
            cas: ContentAddressableStorageClient::with_interceptor(
                cas_channel.clone(),
                interceptor.clone(),
            )
            .max_decoding_message_size(MAX_MESSAGE_BYTES),
            bytestream: ByteStreamClient::with_interceptor(cas_channel, interceptor)
                .max_decoding_message_size(MAX_MESSAGE_BYTES),
            execution,
        })
    }

    /// The `ByteStream` resource name of a blob. SHA-256 is left out of the
    /// name, as REAPI specifies for the functions that predate the field.
    pub(crate) fn read_resource_name(&self, digest: &DigestInfo) -> String {
        let prefix = if self.instance_name.is_empty() {
            String::new()
        } else {
            format!("{}/", self.instance_name)
        };
        let function = match self.func {
            DigestHasherFunc::Sha256 => "",
            DigestHasherFunc::Blake3 => "blake3/",
        };
        format!(
            "{prefix}blobs/{function}{}/{}",
            digest.packed_hash(),
            digest.size_bytes()
        )
    }

    fn empty_digest(&self) -> DigestInfo {
        digest_of(self.func, b"")
    }

    fn problem_from_status(status: &Status, what: &str, digest: DigestInfo) -> Problem {
        if status.code() == Code::NotFound {
            Problem::Missing {
                what: what.to_string(),
                digest,
            }
        } else {
            Problem::ReadFailed {
                what: what.to_string(),
                digest,
                message: format!("{}: {}", status.code().description(), status.message()),
            }
        }
    }

    fn verify(
        &self,
        what: &str,
        expected: DigestInfo,
        data: &[u8],
        via: &'static str,
    ) -> Result<(), Problem> {
        let actual = digest_of(self.func, data);
        if actual == expected {
            Ok(())
        } else {
            Err(Problem::Mismatch {
                what: what.to_string(),
                expected,
                actual,
                via,
            })
        }
    }

    /// Reads a whole blob over `ByteStream` into memory and verifies it.
    pub(crate) async fn read_bytestream(
        &self,
        digest: DigestInfo,
        what: &str,
    ) -> Result<Bytes, Problem> {
        if digest == self.empty_digest() {
            return Ok(Bytes::new());
        }
        let mut client = self.bytestream.clone();
        let request = ReadRequest {
            resource_name: self.read_resource_name(&digest),
            read_offset: 0,
            read_limit: 0,
        };
        let mut stream = client
            .read(request)
            .await
            .map_err(|status| Self::problem_from_status(&status, what, digest))?
            .into_inner();
        let mut data = Vec::with_capacity(
            usize::try_from(digest.size_bytes())
                .unwrap_or(0)
                .min(MAX_MESSAGE_BYTES),
        );
        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|status| Self::problem_from_status(&status, what, digest))?
        {
            data.extend_from_slice(&chunk.data);
        }
        self.verify(what, digest, &data, "bytestream")?;
        Ok(Bytes::from(data))
    }

    /// Streams a blob over `ByteStream` into `path` (created, must not exist),
    /// hashing as it goes. On a mismatch the file holds the bytes the server
    /// sent.
    pub(crate) async fn read_bytestream_to_file(
        &self,
        digest: DigestInfo,
        what: &str,
        path: &Path,
    ) -> Result<(), Problem> {
        let io_problem = |e: std::io::Error| Problem::ReadFailed {
            what: what.to_string(),
            digest,
            message: format!("writing {}: {e}", path.display()),
        };
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(io_problem)?;
        if digest == self.empty_digest() {
            return Ok(());
        }
        let mut client = self.bytestream.clone();
        let request = ReadRequest {
            resource_name: self.read_resource_name(&digest),
            read_offset: 0,
            read_limit: 0,
        };
        let mut stream = client
            .read(request)
            .await
            .map_err(|status| Self::problem_from_status(&status, what, digest))?
            .into_inner();
        let mut hasher = self.func.hasher();
        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|status| Self::problem_from_status(&status, what, digest))?
        {
            hasher.update(&chunk.data);
            file.write_all(&chunk.data).map_err(io_problem)?;
        }
        file.flush().map_err(io_problem)?;
        let actual = hasher.finalize_digest();
        if actual == digest {
            Ok(())
        } else {
            Err(Problem::Mismatch {
                what: what.to_string(),
                expected: digest,
                actual,
                via: "bytestream",
            })
        }
    }

    /// One `BatchReadBlobs` request, every blob verified.
    async fn batch_read(
        &self,
        digests: Vec<DigestInfo>,
    ) -> Vec<(DigestInfo, Result<Bytes, Problem>)> {
        let mut client = self.cas.clone();
        let request = BatchReadBlobsRequest {
            instance_name: self.instance_name.clone(),
            digests: digests.iter().map(Into::into).collect(),
            acceptable_compressors: vec![],
            digest_function: self.func.proto_digest_func().into(),
        };
        let response = match client.batch_read_blobs(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                return digests
                    .into_iter()
                    .map(|digest| {
                        (
                            digest,
                            Err(Self::problem_from_status(&status, "blob", digest)),
                        )
                    })
                    .collect();
            }
        };
        let mut by_digest: HashMap<DigestInfo, Result<Bytes, Problem>> = HashMap::new();
        for item in response.responses {
            let Some(Ok(digest)) = item.digest.as_ref().map(DigestInfo::try_from) else {
                continue;
            };
            let result = match item.status {
                Some(status) if status.code != 0 => {
                    let status = Status::new(Code::from(status.code), status.message);
                    Err(Self::problem_from_status(&status, "blob", digest))
                }
                _ => self
                    .verify("blob", digest, &item.data, "batch_read_blobs")
                    .map(|()| item.data),
            };
            by_digest.insert(digest, result);
        }
        digests
            .into_iter()
            .map(|digest| {
                let result = by_digest.remove(&digest).unwrap_or_else(|| {
                    Err(Problem::ReadFailed {
                        what: "blob".to_string(),
                        digest,
                        message: "absent from the BatchReadBlobs response".to_string(),
                    })
                });
                (digest, result)
            })
            .collect()
    }

    /// Whether a blob of this size goes through `BatchReadBlobs`.
    pub(crate) const fn batchable(&self, digest: &DigestInfo) -> bool {
        digest.size_bytes() <= self.limits.batch_max_blob
    }

    /// Reads many blobs into memory: small ones in batches, the rest over
    /// `ByteStream`, every one verified. Each result's `what` is `blob`; the
    /// caller renames it.
    pub(crate) async fn read_many(
        &self,
        digests: Vec<DigestInfo>,
    ) -> HashMap<DigestInfo, Result<Bytes, Problem>> {
        let empty = self.empty_digest();
        let mut results = HashMap::new();
        let mut batches: Vec<Vec<DigestInfo>> = Vec::new();
        let mut current: Vec<DigestInfo> = Vec::new();
        let mut current_bytes = 0u64;
        let mut singles = Vec::new();
        let mut unique = digests;
        unique.sort_unstable();
        unique.dedup();
        for digest in unique {
            if digest == empty {
                results.insert(digest, Ok(Bytes::new()));
            } else if self.batchable(&digest) {
                if !current.is_empty()
                    && (current_bytes + digest.size_bytes() > self.limits.batch_max_total
                        || current.len() >= 1000)
                {
                    batches.push(core::mem::take(&mut current));
                    current_bytes = 0;
                }
                current_bytes += digest.size_bytes();
                current.push(digest);
            } else {
                singles.push(digest);
            }
        }
        if !current.is_empty() {
            batches.push(current);
        }
        let concurrency = self.limits.concurrency.max(1);
        let mut batch_results = futures::stream::iter(batches)
            .map(|batch| self.batch_read(batch))
            .buffer_unordered(concurrency);
        while let Some(items) = batch_results.next().await {
            results.extend(items);
        }
        let mut stream_results = futures::stream::iter(singles)
            .map(|digest| async move { (digest, self.read_bytestream(digest, "blob").await) })
            .buffer_unordered(concurrency);
        while let Some((digest, result)) = stream_results.next().await {
            results.insert(digest, result);
        }
        results
    }

    /// Reads one blob, by batch or by stream depending on its size.
    pub(crate) async fn read_blob(&self, digest: DigestInfo, what: &str) -> Result<Bytes, Problem> {
        let result = if self.batchable(&digest) {
            self.read_many(vec![digest])
                .await
                .remove(&digest)
                .unwrap_or_else(|| {
                    Err(Problem::ReadFailed {
                        what: what.to_string(),
                        digest,
                        message: "no result".to_string(),
                    })
                })
        } else {
            self.read_bytestream(digest, what).await
        };
        result.map_err(|problem| rename_problem(problem, what))
    }

    /// Reads and decodes a proto blob.
    pub(crate) async fn read_proto<T: Message + Default>(
        &self,
        digest: DigestInfo,
        what: &str,
    ) -> Result<T, Problem> {
        let data = self.read_blob(digest, what).await?;
        T::decode(data).map_err(|e| Problem::ReadFailed {
            what: what.to_string(),
            digest,
            message: format!("not a valid proto: {e}"),
        })
    }

    /// Executes the action once with `skip_cache_lookup`, following the
    /// operation to completion. Returns the operation name and the response.
    pub(crate) async fn execute(
        &self,
        action_digest: DigestInfo,
    ) -> Result<(String, ExecuteResponse), Error> {
        let mut client = self
            .execution
            .clone()
            .ok_or_else(|| make_err!(Code::Internal, "no execution client"))?;
        let request = ExecuteRequest {
            instance_name: self.instance_name.clone(),
            skip_cache_lookup: true,
            action_digest: Some(action_digest.into()),
            execution_policy: None,
            results_cache_policy: None,
            digest_function: self.func.proto_digest_func().into(),
        };
        let mut stream = client
            .execute(request)
            .await
            .err_tip(|| "Execution.Execute")?
            .into_inner();
        let mut name = String::new();
        let mut resumes = 0;
        loop {
            match stream.message().await {
                Ok(Some(op)) => {
                    if !op.name.is_empty() {
                        name.clone_from(&op.name);
                    }
                    if let Some(response) = operation_response(&op)? {
                        return Ok((name, response));
                    }
                }
                Ok(None) | Err(_) if !name.is_empty() && resumes < MAX_WAIT_RESUMES => {
                    resumes += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    stream = client
                        .wait_execution(WaitExecutionRequest { name: name.clone() })
                        .await
                        .err_tip(|| format!("Execution.WaitExecution {name}"))?
                        .into_inner();
                }
                Ok(None) => {
                    return Err(make_err!(
                        Code::Unavailable,
                        "the Execute stream ended before the operation '{name}' finished"
                    ));
                }
                Err(status) => {
                    return Err(
                        Error::from(status).append(format!("while waiting for operation '{name}'"))
                    );
                }
            }
        }
    }
}

fn rename_problem(problem: Problem, what: &str) -> Problem {
    let what = what.to_string();
    match problem {
        Problem::Mismatch {
            expected,
            actual,
            via,
            ..
        } => Problem::Mismatch {
            what,
            expected,
            actual,
            via,
        },
        Problem::Missing { digest, .. } => Problem::Missing { what, digest },
        Problem::ReadFailed {
            digest, message, ..
        } => Problem::ReadFailed {
            what,
            digest,
            message,
        },
    }
}

/// Renames a `blob` problem from [`Remote::read_many`] after the file it was for.
pub(crate) fn problem_for(problem: Problem, what: &str) -> Problem {
    rename_problem(problem, what)
}

/// The `ExecuteResponse` of a finished operation, `None` while it runs.
pub(crate) fn operation_response(op: &Operation) -> Result<Option<ExecuteResponse>, Error> {
    if !op.done {
        return Ok(None);
    }
    match &op.result {
        Some(operation::Result::Response(any)) => {
            let response = ExecuteResponse::decode(any.value.as_slice()).err_tip(|| {
                format!(
                    "decoding the ExecuteResponse of '{}' ({})",
                    op.name, any.type_url
                )
            })?;
            Ok(Some(response))
        }
        Some(operation::Result::Error(status)) => Err(make_err!(
            Code::from(status.code),
            "operation '{}' failed: {}",
            op.name,
            status.message
        )),
        None => Err(make_err!(
            Code::Internal,
            "operation '{}' is done with neither a response nor an error",
            op.name
        )),
    }
}

#[cfg(test)]
/// An operation that finished with `response`. `prost_types` is not a
/// dependency of this crate, so the `Any` comes from a field of that type.
pub(crate) fn done_operation(name: &str, response: &ExecuteResponse) -> Operation {
    let mut any = Operation::default().metadata.unwrap_or_default();
    any.type_url =
        "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteResponse".to_string();
    any.value = response.encode_to_vec();
    Operation {
        name: name.to_string(),
        done: true,
        result: Some(operation::Result::Response(any)),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use nativelink_proto::build::bazel::remote::execution::v2::ActionResult;
    use nativelink_proto::google::rpc::Status as RpcStatus;

    use super::*;

    #[test]
    fn normalizes_bazel_style_urls() {
        assert_eq!(normalize_url("grpc://h:1"), "http://h:1");
        assert_eq!(normalize_url("grpcs://h:1"), "https://h:1");
        assert_eq!(normalize_url("http://h:1"), "http://h:1");
        assert_eq!(normalize_url("https://h:1"), "https://h:1");
    }

    #[test]
    fn rejects_bad_headers() {
        assert!(HeaderInterceptor::new(&[("x-api-key".to_string(), "v".to_string())]).is_ok());
        assert!(HeaderInterceptor::new(&[("bad key".to_string(), "v".to_string())]).is_err());
        assert!(HeaderInterceptor::new(&[("k".to_string(), "line\nbreak".to_string())]).is_err());
    }

    #[test]
    fn operation_response_handles_each_state() {
        let running = Operation {
            name: "op".to_string(),
            done: false,
            ..Default::default()
        };
        assert!(operation_response(&running).unwrap().is_none());

        let response = ExecuteResponse {
            result: Some(ActionResult {
                exit_code: 3,
                ..Default::default()
            }),
            ..Default::default()
        };
        let done = done_operation("op", &response);
        assert_eq!(operation_response(&done).unwrap(), Some(response));

        let failed = Operation {
            name: "op".to_string(),
            done: true,
            result: Some(operation::Result::Error(RpcStatus {
                code: Code::FailedPrecondition as i32,
                message: "missing input".to_string(),
                details: vec![],
            })),
            ..Default::default()
        };
        let err = operation_response(&failed).unwrap_err();
        assert_eq!(err.code, Code::FailedPrecondition);

        let empty = Operation {
            name: "op".to_string(),
            done: true,
            ..Default::default()
        };
        assert!(operation_response(&empty).is_err());
    }
}
