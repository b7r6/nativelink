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

#[path = "physical_azure/adapter.rs"]
mod adapter;
mod azurite_runner;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use adapter::{
    LocalAzurePhysical, OBJECT_LIMIT, container_client, network_transport, object_name,
    provider_error,
};
use azure_core::error::ErrorKind;
use azure_core::http::{AsyncRawResponse, HttpClient, Method, Request, RequestContent, StatusCode};
use azure_storage_blob::BlobContainerClient;
use azurite_runner::{
    ACCOUNT_KEY, ACCOUNT_NAME, AzuriteEmbedded, build_container_sas_url, create_container,
};
use nativelink_macro::nativelink_test;
use nativelink_store::physical_filesystem::{
    Algorithm, Failure, FailureKind, Incarnation, Limits, ObjectId, PhysicalFilesystem, PhysicalKey,
};
use tempfile::TempDir;

#[derive(Clone, Debug)]
struct RequestRecord {
    method: Method,
    path: String,
    multipart: bool,
    if_match: Option<String>,
    if_none_match: Option<String>,
    status: StatusCode,
}

#[derive(Debug)]
struct ObservedTransport {
    inner: Arc<dyn HttpClient>,
    lose_next_put_response: AtomicBool,
    records: Mutex<Vec<RequestRecord>>,
}

impl ObservedTransport {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: network_transport().unwrap(),
            lose_next_put_response: AtomicBool::new(false),
            records: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait::async_trait]
impl HttpClient for ObservedTransport {
    async fn execute_request(&self, request: &Request) -> azure_core::Result<AsyncRawResponse> {
        if self.records.lock().unwrap().len() >= 512 {
            return Err(azure_core::Error::with_message(
                ErrorKind::Other,
                "bounded request trace exhausted",
            ));
        }
        let response = self.inner.execute_request(request).await?;
        let status = response.status();
        self.records.lock().unwrap().push(RequestRecord {
            method: request.method(),
            path: request.url().path().into(),
            multipart: request.url().query_pairs().any(|(key, value)| {
                key == "comp" && matches!(value.as_ref(), "block" | "blocklist")
            }),
            if_match: request
                .headers()
                .get_optional_str(&"if-match".into())
                .map(str::to_owned),
            if_none_match: request
                .headers()
                .get_optional_str(&"if-none-match".into())
                .map(str::to_owned),
            status,
        });
        if request.method() == Method::Put
            && status == StatusCode::Created
            && request.url().path().contains("objects-v1")
            && self.lose_next_put_response.swap(false, Ordering::SeqCst)
        {
            // Azurite really committed the request. The SDK/caller gets no
            // success response and must reconcile the same immutable locator.
            return Err(azure_core::Error::with_message(
                ErrorKind::Other,
                "injected lost PUT response",
            ));
        }
        Ok(response)
    }
}

fn incarnation(last_byte: u8) -> Incarnation {
    let mut bytes = [7; 16];
    bytes[15] = last_byte;
    Incarnation::new(bytes).unwrap()
}

fn key(incarnation: Incarnation, generation: u64) -> PhysicalKey {
    PhysicalKey::new(incarnation, generation).unwrap()
}

fn failure<T>(result: Result<T, Failure>, expected: FailureKind) {
    assert_eq!(
        result.err().expect("physical operation must refuse").kind,
        expected
    );
}

async fn publish(
    store: &LocalAzurePhysical,
    key: PhysicalKey,
    write: u64,
    bytes: &[u8],
) -> ObjectId {
    let object = ObjectId::for_bytes(Algorithm::Sha256, bytes);
    let receipt = store.write(key, write, object, bytes).await.unwrap();
    assert_eq!(
        (receipt.key, receipt.write, receipt.object),
        (key, write, object)
    );
    object
}

#[nativelink_test]
async fn physical_azure_constructor_requires_explicit_loopback_container() {
    for endpoint in [
        "https://example.invalid/container?sig=x",
        "http://localhost:10000/devstoreaccount1/c",
        "http://192.0.2.1:10000/devstoreaccount1/c",
        "http://127.0.0.1:10000/devstoreaccount1/c/blob",
        "http://127.0.0.1:10000/devstoreaccount1/c?comp=block",
        "http://127.0.0.1:10000/devstoreaccount1/c?versionid=x",
    ] {
        failure(
            LocalAzurePhysical::bootstrap(endpoint, incarnation(1), network_transport().unwrap())
                .await,
            FailureKind::Invalid,
        );
    }
}

async fn ranges_and_wire_identity(store: &LocalAzurePhysical) {
    let data: Vec<_> = (0..200_000)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();
    let temp = TempDir::new().unwrap();
    let filesystem =
        PhysicalFilesystem::bootstrap(temp.path(), incarnation(1), Limits::default()).unwrap();
    for (index, algorithm) in [Algorithm::Sha256, Algorithm::Blake3]
        .into_iter()
        .enumerate()
    {
        let key = key(incarnation(1), index as u64 + 1);
        let object = ObjectId::for_bytes(algorithm, &data);
        store.write(key, 1, object, &data).await.unwrap();
        assert_eq!(store.read(key, object, 0, None).await.unwrap().bytes, data);
        assert_eq!(
            store
                .read(key, object, 65_000, Some(9000))
                .await
                .unwrap()
                .bytes,
            data[65_000..74_000]
        );
        assert!(
            store
                .read(key, object, object.size, Some(0))
                .await
                .unwrap()
                .bytes
                .is_empty()
        );
        failure(
            store.read(key, object, u64::MAX, Some(2)).await,
            FailureKind::Invalid,
        );
        let mut stage = filesystem.begin_write(key, 1, object).unwrap();
        stage.append(&data).unwrap();
        stage.finish().unwrap();
        let file = std::fs::read(
            filesystem
                .namespace_path()
                .join("objects-v1")
                .join(format!("{:016x}", key.generation())),
        )
        .unwrap();
        // The independent raw client below downloads exactly the same envelope.
        assert_eq!(&file[8..24], &incarnation(1).as_bytes());
        assert_eq!(&file[73..], &data);
    }
}

async fn immutable_and_ambiguous_writes(
    endpoint: &str,
    store: &LocalAzurePhysical,
    transport: &Arc<ObservedTransport>,
) {
    let key = key(incarnation(1), 10);
    let original = publish(store, key, 1, b"original").await;
    assert_eq!(publish(store, key, 1, b"original").await, original);
    failure(
        store
            .write(
                key,
                2,
                ObjectId::for_bytes(Algorithm::Sha256, b"different-size"),
                b"different-size",
            )
            .await,
        FailureKind::Conflict,
    );
    failure(
        store.write(key, 3, original, b"corrupt!").await,
        FailureKind::Corrupt,
    );
    assert_eq!(
        store.read(key, original, 0, None).await.unwrap().bytes,
        b"original"
    );
    let absent = PhysicalKey::new(incarnation(1), 11).unwrap();
    failure(
        store.write(absent, 0, original, b"original").await,
        FailureKind::Invalid,
    );
    failure(
        store.read(absent, original, 0, None).await,
        FailureKind::Missing,
    );
    let mut oversized = original;
    oversized.size = OBJECT_LIMIT as u64 + 1;
    failure(
        store.write(absent, 1, oversized, b"").await,
        FailureKind::Capacity,
    );
    transport
        .lose_next_put_response
        .store(true, Ordering::SeqCst);
    failure(
        store.write(absent, 9, original, b"original").await,
        FailureKind::Unavailable,
    );
    let reopened = LocalAzurePhysical::open(endpoint, incarnation(1), transport.clone())
        .await
        .unwrap();
    let receipt = reopened
        .write(absent, 9, original, b"original")
        .await
        .unwrap();
    assert_eq!(receipt.write, 9);
    let records = transport.records.lock().unwrap();
    let path = container_client(endpoint, network_transport().unwrap())
        .unwrap()
        .blob_client(&object_name(absent))
        .url()
        .path()
        .to_owned();
    let puts: Vec<_> = records
        .iter()
        .filter(|r| r.method == Method::Put && r.path == path)
        .collect();
    assert_eq!(puts.len(), 2, "no automatic SDK replay after lost response");
    assert_eq!(puts[0].status, StatusCode::Created);
    assert!(matches!(
        puts[1].status,
        StatusCode::PreconditionFailed | StatusCode::Conflict
    ));
    assert!(puts.iter().all(|r| r.if_none_match.as_deref() == Some("*")));
    assert!(
        records
            .iter()
            .any(|r| r.method == Method::Get && r.path == path && r.if_match.is_some())
    );
}

async fn corruption_and_conditional_delete(store: &LocalAzurePhysical, raw: &BlobContainerClient) {
    let key = key(incarnation(1), 20);
    let data = vec![b'a'; 100_000];
    let object = publish(store, key, 1, &data).await;
    let temp = TempDir::new().unwrap();
    let filesystem =
        PhysicalFilesystem::bootstrap(temp.path(), incarnation(1), Limits::default()).unwrap();
    let mut stage = filesystem.begin_write(key, 1, object).unwrap();
    stage.append(&data).unwrap();
    stage.finish().unwrap();
    let mut envelope = std::fs::read(
        filesystem
            .namespace_path()
            .join("objects-v1")
            .join(format!("{:016x}", key.generation())),
    )
    .unwrap();
    let mut body = raw
        .blob_client(&object_name(key))
        .download(None)
        .await
        .unwrap()
        .body;
    let mut azure_bytes = Vec::new();
    while let Some(chunk) = futures::TryStreamExt::try_next(&mut body).await.unwrap() {
        azure_bytes.extend_from_slice(&chunk);
    }
    assert_eq!(
        azure_bytes, envelope,
        "filesystem and Azure share the physical envelope"
    );
    envelope[73 + 90_000] ^= 1;
    raw.blob_client(&object_name(key))
        .block_blob_client()
        .upload(RequestContent::from(envelope), None)
        .await
        .unwrap();
    failure(
        store.read(key, object, 0, Some(1)).await,
        FailureKind::Corrupt,
    );
    failure(
        store.read(key, object, 0, Some(0)).await,
        FailureKind::Corrupt,
    );
    failure(
        store.write(key, 2, object, &data).await,
        FailureKind::Corrupt,
    );
    let intent = store.prepare_delete(key, 1).await.unwrap();
    assert!(
        !store
            .finish_delete(intent.clone())
            .await
            .unwrap()
            .already_absent
    );
    assert!(store.finish_delete(intent).await.unwrap().already_absent);
    failure(store.read(key, object, 0, None).await, FailureKind::Missing);

    // An absent preparation must recheck absence and refuse a late recreation.
    let absent_delete = store.prepare_delete(key, 2).await.unwrap();
    assert!(
        store
            .finish_delete(absent_delete.clone())
            .await
            .unwrap()
            .already_absent
    );

    // Same physical name recreated by a late old write has a new provider ETag.
    publish(store, key, 3, &data).await;
    failure(
        store.finish_delete(absent_delete).await,
        FailureKind::Conflict,
    );
    let old_delete = store.prepare_delete(key, 2).await.unwrap();
    store.finish_delete(old_delete.clone()).await.unwrap();
    publish(store, key, 4, &data).await;
    failure(store.finish_delete(old_delete).await, FailureKind::Conflict);
    assert_eq!(
        store.read(key, object, 0, Some(1)).await.unwrap().bytes,
        b"a"
    );
    let fresh_delete = store.prepare_delete(key, 3).await.unwrap();
    assert!(
        !store
            .finish_delete(fresh_delete)
            .await
            .unwrap()
            .already_absent
    );
}

async fn generation_and_incarnation_isolation(
    endpoint: &str,
    old: &LocalAzurePhysical,
    transport: &Arc<ObservedTransport>,
) {
    let old_key = key(incarnation(1), 30);
    let object = publish(old, old_key, 1, b"old").await;
    let old_delete = old.prepare_delete(old_key, 1).await.unwrap();
    let fresh = LocalAzurePhysical::bootstrap(endpoint, incarnation(2), transport.clone())
        .await
        .unwrap();
    let fresh_key = key(incarnation(2), 30);
    let replacement = publish(&fresh, fresh_key, 1, b"new").await;
    let successor_key = key(incarnation(1), 31);
    let successor = publish(old, successor_key, 1, b"successor").await;
    failure(
        old.read(fresh_key, replacement, 0, None).await,
        FailureKind::Conflict,
    );
    failure(
        old.prepare_delete(fresh_key, 2).await,
        FailureKind::Conflict,
    );
    old.finish_delete(old_delete.clone()).await.unwrap();
    assert!(old.finish_delete(old_delete).await.unwrap().already_absent);
    assert_eq!(
        fresh
            .read(fresh_key, replacement, 0, None)
            .await
            .unwrap()
            .bytes,
        b"new"
    );
    assert_eq!(
        old.read(successor_key, successor, 0, None)
            .await
            .unwrap()
            .bytes,
        b"successor"
    );
    // A completed delete does not fence an unresolved old write or restore
    // readiness. The physical orphan needs a new caller-owned delete intent.
    old.write(old_key, 2, object, b"old").await.unwrap();
    let cleanup = old.prepare_delete(old_key, 3).await.unwrap();
    old.finish_delete(cleanup).await.unwrap();
    assert_eq!(
        fresh
            .read(fresh_key, replacement, 0, None)
            .await
            .unwrap()
            .bytes,
        b"new"
    );
    failure(
        LocalAzurePhysical::bootstrap(endpoint, incarnation(2), transport.clone()).await,
        FailureKind::Conflict,
    );
    let emptied = LocalAzurePhysical::bootstrap(endpoint, incarnation(4), transport.clone())
        .await
        .unwrap();
    let emptied_key = key(incarnation(4), 1);
    publish(&emptied, emptied_key, 1, b"retired").await;
    let delete = emptied.prepare_delete(emptied_key, 1).await.unwrap();
    emptied.finish_delete(delete).await.unwrap();
    failure(
        LocalAzurePhysical::bootstrap(endpoint, incarnation(4), transport.clone()).await,
        FailureKind::Conflict,
    );
}

#[nativelink_test]
#[ignore = "Explicit local qualification: install tests/azurite_runner dependencies, then select local_azurite_exact_generation_contract with --ignored; missing Azurite is a hard failure"]
async fn local_azurite_exact_generation_contract() {
    let emulator = AzuriteEmbedded::new();
    let process = emulator
        .start()
        .await
        .expect("local Azurite3.37 prerequisite; install tests/azurite_runner dependencies");
    create_container(&process.blob_endpoint, "physical")
        .await
        .unwrap();
    let endpoint = emulator.sas_url(&process, "physical").unwrap();
    let transport = ObservedTransport::new();
    let raw = container_client(&endpoint, network_transport().unwrap()).unwrap();
    // Azurite evaluates If-Match before absence, so adapter replay observes
    // 412 then exact HEAD404. Separately verify its genuine DELETE404 code on
    // this disposable, never-written probe using the independent raw SDK client.
    let delete_404 = raw
        .blob_client("absent-delete-probe")
        .delete(None)
        .await
        .unwrap_err();
    assert_eq!(
        provider_error(&delete_404, "raw DELETE404 probe").kind,
        FailureKind::Missing
    );
    let store = LocalAzurePhysical::bootstrap(&endpoint, incarnation(1), transport.clone())
        .await
        .unwrap();
    ranges_and_wire_identity(&store).await;
    immutable_and_ambiguous_writes(&endpoint, &store, &transport).await;
    corruption_and_conditional_delete(&store, &raw).await;
    generation_and_incarnation_isolation(&endpoint, &store, &transport).await;
    let absent_namespace =
        LocalAzurePhysical::open(&endpoint, incarnation(3), transport.clone()).await;
    failure(absent_namespace, FailureKind::Unavailable);
    let missing_container = build_container_sas_url(
        &process.blob_endpoint,
        ACCOUNT_NAME,
        ACCOUNT_KEY,
        "missing-container",
    )
    .unwrap();
    failure(
        LocalAzurePhysical::open(&missing_container, incarnation(1), transport.clone()).await,
        FailureKind::Unavailable,
    );
    let container_404 = container_client(&missing_container, network_transport().unwrap())
        .unwrap()
        .blob_client("absent-container-probe")
        .get_properties(None)
        .await
        .unwrap_err();
    assert_eq!(
        provider_error(&container_404, "raw container404 probe").kind,
        FailureKind::Unavailable
    );
    let mut denied = url::Url::parse(&endpoint).unwrap();
    denied.set_query(Some(
        "sv=2020-12-06&sr=c&sp=r&se=2099-01-01T00%3A00%3A00Z&sig=invalid",
    ));
    failure(
        LocalAzurePhysical::open(denied.as_str(), incarnation(1), transport.clone()).await,
        FailureKind::Unavailable,
    );
    let records = transport.records.lock().unwrap();
    assert!(
        records.iter().all(|r| !r.multipart),
        "bounded object writes never stage or commit blocks"
    );
    assert!(
        records
            .iter()
            .filter(|r| r.method == Method::Delete)
            .all(|r| r.if_match.is_some())
    );
    assert!(
        records
            .iter()
            .any(|r| r.method == Method::Delete && r.status == StatusCode::PreconditionFailed)
    );
    eprintln!(
        "Qualified {} actual local SDK requests; no cloud endpoint or credentials used",
        records.len()
    );
}
