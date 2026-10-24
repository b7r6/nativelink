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

use core::any::Any;
use core::pin::Pin;

use async_trait::async_trait;
use nativelink_metric::MetricsComponent;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::digest_hasher::{
    default_digest_hasher_func, digest_hasher_func_from_context, make_ctx_for_hash_func,
};
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{RemoveCallback, Store, StoreDriver, StoreKey, UploadSizeInfo};
use opentelemetry::trace::FutureExt;
use pretty_assertions::assert_eq;

use super::*;

// Deliberately separate the namespaces, even for identical hash/size pairs.
// The service must supply the request's algorithm on every backend operation.
#[derive(Debug, MetricsComponent)]
struct AlgorithmStore {
    sha256: Arc<MemoryStore>,
    blake3: Arc<MemoryStore>,
}

impl AlgorithmStore {
    fn current(&self) -> Pin<&MemoryStore> {
        Pin::new(match digest_hasher_func_from_context() {
            DigestHasherFunc::Sha256 => self.sha256.as_ref(),
            DigestHasherFunc::Blake3 => self.blake3.as_ref(),
        })
    }
}

#[async_trait]
impl StoreDriver for AlgorithmStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        yield_now().await;
        self.current().has_with_results(keys, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<u64, Error> {
        yield_now().await;
        self.current().update(key, reader, size_info).await
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        yield_now().await;
        self.current().get_part(key, writer, offset, length).await
    }

    fn inner_store(&self, _key: Option<StoreKey<'_>>) -> &dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Sync + Send + 'static> {
        self
    }

    fn register_remove_callback(self: Arc<Self>, _callback: RemoveCallback) -> Result<(), Error> {
        Ok(())
    }
}

default_health_status_indicator!(AlgorithmStore);

fn manager_and_store() -> Result<(StoreManager, Store), Error> {
    let manager = StoreManager::new();
    let store = Store::new(Arc::new(AlgorithmStore {
        sha256: MemoryStore::new(&MemorySpec::default()),
        blake3: MemoryStore::new(&MemorySpec::default()),
    }));
    manager.add_store("main_cas", store.clone())?;
    Ok((manager, store))
}

const fn opposite(algorithm: DigestHasherFunc) -> DigestHasherFunc {
    match algorithm {
        DigestHasherFunc::Sha256 => DigestHasherFunc::Blake3,
        DigestHasherFunc::Blake3 => DigestHasherFunc::Sha256,
    }
}

fn digest_cases() -> [(&'static str, DigestHasherFunc); 3] {
    [
        ("sha256/", DigestHasherFunc::Sha256),
        ("blake3/", DigestHasherFunc::Blake3),
        ("", default_digest_hasher_func()),
    ]
}

#[nativelink_test]
async fn completed_upload_status_uses_resource_digest_over_http2()
-> Result<(), Box<dyn core::error::Error>> {
    let payload = Bytes::from_static(b"completed upload in an algorithm-qualified namespace");
    for (prefix, algorithm) in digest_cases() {
        for compressed in [false, true] {
            let (manager, _store) = manager_and_store()?;
            let server =
                make_bytestream_server_with_remote_cache_compression(&manager, None, true)?;
            let (_guard, mut client) =
                server_and_client_stub(server, HttpListener::default()).await;
            let mut hasher = algorithm.hasher();
            hasher.update(&payload);
            let digest = hasher.finalize_digest();
            let hash = digest.packed_hash();
            let kind = if compressed {
                "compressed-blobs/zstd"
            } else {
                "blobs"
            };
            let resource_name = format!(
                "{INSTANCE_NAME}/uploads/4dcec57e-1389-4ab5-b188-4a59f22ceb4b/{kind}/{prefix}{hash}/{}",
                payload.len()
            );
            let absent = QueryWriteStatusResponse {
                committed_size: 0,
                complete: false,
            };
            assert_eq!(
                client
                    .query_write_status(QueryWriteStatusRequest {
                        resource_name: resource_name.clone(),
                    })
                    .await?
                    .into_inner(),
                absent
            );
            let data = if compressed {
                Bytes::from(zstd::stream::encode_all(payload.as_ref(), 0)?)
            } else {
                payload.clone()
            };
            client
                .write(tokio_stream::iter([WriteRequest {
                    resource_name: resource_name.clone(),
                    write_offset: 0,
                    finish_write: true,
                    data,
                }]))
                .await?;
            // Awaiting Write guarantees this query takes the completed-store path.
            assert_eq!(
                client
                    .query_write_status(QueryWriteStatusRequest { resource_name })
                    .await?
                    .into_inner(),
                QueryWriteStatusResponse {
                    committed_size: i64::try_from(payload.len())?,
                    complete: true,
                },
                "prefix={prefix:?}, compressed={compressed}"
            );
            for compressed_read in [false, true] {
                let read_kind = if compressed_read {
                    "compressed-blobs/zstd"
                } else {
                    "blobs"
                };
                let mut read = client
                    .read(ReadRequest {
                        resource_name: format!(
                            "{INSTANCE_NAME}/{read_kind}/{prefix}{hash}/{}",
                            payload.len()
                        ),
                        read_offset: 0,
                        read_limit: 0,
                    })
                    .await?
                    .into_inner();
                let mut actual = Vec::new();
                while let Some(chunk) = read.message().await? {
                    actual.extend_from_slice(&chunk.data);
                }
                if compressed_read {
                    actual = zstd::stream::decode_all(actual.as_slice())?;
                }
                assert_eq!(actual, payload);
            }
            // No fallback to a blob with the same hash/size in another namespace.
            assert_eq!(
                client
                    .query_write_status(QueryWriteStatusRequest {
                        resource_name: format!(
                            "{INSTANCE_NAME}/uploads/other/blobs/{}/{hash}/{}",
                            opposite(algorithm).to_string().to_lowercase(),
                            payload.len()
                        ),
                    })
                    .await?
                    .into_inner(),
                absent
            );
        }
    }
    Ok(())
}

#[nativelink_test]
async fn query_status_overrides_ambient_digest_context() -> Result<(), Box<dyn core::error::Error>>
{
    for (prefix, algorithm) in digest_cases() {
        let (manager, store) = manager_and_store()?;
        let server = make_bytestream_server(&manager, None)?;
        let digest = DigestInfo::try_new(HASH1, 7)?;
        store
            .update_oneshot(digest, Bytes::from_static(b"payload"))
            .with_context(make_ctx_for_hash_func(algorithm)?)
            .await?;
        let response = server
            .query_write_status(Request::new(QueryWriteStatusRequest {
                resource_name: format!("{INSTANCE_NAME}/uploads/id/blobs/{prefix}{HASH1}/7"),
            }))
            .with_context(make_ctx_for_hash_func(opposite(algorithm))?)
            .await?
            .into_inner();
        assert_eq!(
            response,
            QueryWriteStatusResponse {
                committed_size: 7,
                complete: true
            },
            "prefix={prefix:?}"
        );
    }
    Ok(())
}
