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

//! Explicit local mTLS qualification, launched by the disposable-certificate
//! runner. No key material is included in the structured evidence.

use std::path::Path;

use serde_json::{Value, json};
use tonic::transport::{Certificate, Identity, ServerTlsConfig};

use super::*;

fn read_certificate(directory: &Path, name: &str) -> Vec<u8> {
    std::fs::read(directory.join(name)).expect("runner must supply disposable certificate material")
}

fn client_tls(directory: &Path, role: &str) -> ClientTlsConfig {
    ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(Certificate::from_pem(read_certificate(
            directory,
            "server-ca.pem",
        )))
        .identity(Identity::from_pem(
            read_certificate(directory, &format!("{role}.pem")),
            read_certificate(directory, &format!("{role}.key")),
        ))
}

fn server_transport(directory: &Path, trust: &str) -> Server {
    Server::builder()
        .tls_config(
            ServerTlsConfig::new()
                .identity(Identity::from_pem(
                    read_certificate(directory, "server.pem"),
                    read_certificate(directory, "server.key"),
                ))
                .client_ca_root(Certificate::from_pem(read_certificate(
                    directory,
                    &format!("{trust}-ca.pem"),
                ))),
        )
        .unwrap()
}

async fn worker_write_client(
    runtime: &Runtime,
    directory: &Path,
    request: UpdateActionResultRequest,
) -> (bool, Value) {
    tokio::time::timeout(Duration::from_secs(5), async {
        match endpoint(runtime.client_address, Some(client_tls(directory, "worker")))
            .connect()
            .await
        {
            Err(error) => (false, json!({"stage": "connect", "error": error.to_string()})),
            Ok(channel) => match ActionCacheClient::new(channel).update_action_result(request).await {
                Ok(response) => (true, json!({"stage": "rpc", "status": "Ok", "exit_code": response.into_inner().exit_code})),
                Err(status) => (false, json!({"stage": "rpc", "code": status.code() as i32, "message": status.message()})),
            },
        }
    })
    .await
    .expect("a timeout is not credential rejection evidence")
}

async fn exercise(directory: &Path, algorithm: DigestHasherFunc, shared_trust: bool) -> Value {
    let runtime = Runtime::start_with_transports(
        server_transport(directory, "worker"),
        server_transport(directory, if shared_trust { "shared" } else { "client" }),
        Some(client_tls(directory, "worker")),
        Some(client_tls(directory, "client")),
    )
    .await;
    let mut client = ActionCacheClient::new(runtime.client.clone());
    let bytes = ActionResult {
        exit_code: 17,
        ..ActionResult::default()
    }
    .encode_to_vec();
    let key = digest(algorithm, &bytes);
    let valid_client_write = cache_update(&key, algorithm, 7);
    assert_eq!(
        client
            .update_action_result(valid_client_write.clone())
            .await
            .unwrap()
            .into_inner()
            .exit_code,
        7
    );

    // A worker certificate is valid on the worker listener; its AC route is
    // absent, even though the same request is valid on the client route.
    let absent = ActionCacheClient::new(runtime.worker.clone())
        .update_action_result(valid_client_write.clone())
        .await
        .unwrap_err();
    assert_eq!(absent.code(), Code::Unimplemented);
    assert_eq!(
        upload(runtime.worker.clone(), algorithm, bytes.clone()).await,
        key
    );
    assert_eq!(
        client
            .get_action_result(cache_get(&key, algorithm))
            .await
            .unwrap()
            .into_inner()
            .exit_code,
        7
    );
    let algorithm_segment = match algorithm {
        DigestHasherFunc::Sha256 => "",
        DigestHasherFunc::Blake3 => "blake3/",
    };
    let resource = format!("blobs/{algorithm_segment}{}/{}", key.hash, key.size_bytes);
    let written = ByteStreamClient::new(runtime.worker.clone())
        .write(stream::iter([WriteRequest {
            resource_name: format!("{INSTANCE}/uploads/tls-boundary/{resource}"),
            write_offset: 0,
            finish_write: true,
            data: bytes.clone().into(),
        }]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(written.committed_size, key.size_bytes);
    assert_eq!(
        client
            .get_action_result(cache_get(&key, algorithm))
            .await
            .unwrap()
            .into_inner()
            .exit_code,
        7
    );
    let mut received = ByteStreamClient::new(runtime.client.clone())
        .read(ReadRequest {
            resource_name: format!("{INSTANCE}/{resource}"),
            read_offset: 0,
            read_limit: 0,
        })
        .await
        .unwrap()
        .into_inner();
    let mut roundtrip = Vec::new();
    while let Some(chunk) = received.message().await.unwrap() {
        roundtrip.extend_from_slice(&chunk.data);
    }
    assert_eq!(roundtrip, bytes);
    drop(received);

    // This helper exposes exactly one client listener. Test every declared
    // writable client listener in this fixture; make no deployment-wide claim.
    let (admitted, observation) =
        worker_write_client(&runtime, directory, cache_update(&key, algorithm, 33)).await;
    let observed = client
        .get_action_result(cache_get(&key, algorithm))
        .await
        .unwrap()
        .into_inner()
        .exit_code;
    let violations = if admitted {
        assert_eq!(observed, 33);
        vec!["worker_credentials_can_write_client_route"]
    } else {
        assert_eq!(
            observed, 7,
            "rejected credentials must leave the sink unchanged"
        );
        Vec::new()
    };
    assert_eq!(
        admitted, shared_trust,
        "shared trust must be detected as unsafe"
    );
    assert_eq!(
        client
            .update_action_result(valid_client_write)
            .await
            .unwrap()
            .into_inner()
            .exit_code,
        7
    );
    assert_eq!(
        client
            .get_action_result(cache_get(&key, algorithm))
            .await
            .unwrap()
            .into_inner()
            .exit_code,
        7
    );

    // Also exercise Execution -> authenticated Worker API -> accepted scheduler
    // publication through the exact fixed service stack for each safe topology.
    if !shared_trust {
        accepted_publication(&runtime, algorithm).await;
    }
    drop(client);
    runtime.shutdown().await;
    json!({
        "topology": if shared_trust { "shared_trust_negative_control" } else { "isolated_trust" },
        "digest_algorithm": algorithm.to_string(),
        "client_listener_count": 1,
        "client_listeners_probed": [0],
        "authorized_client_write_before_and_after": true,
        "worker_listener_ac_code": absent.code() as i32,
        "worker_listener_ac_message": absent.message(),
        "worker_cas_upload_succeeded": true,
        "worker_bytestream_upload_succeeded": true,
        "client_bytestream_roundtrip_succeeded": true,
        "same_key_object_uploads_preserved_sink": true,
        "worker_client_route_attempt": observation,
        "worker_client_route_write_admitted": admitted,
        "sink_exit_code_after_worker_attempt": observed,
        "accepted_publication_and_cache_hit_exercised": !shared_trust,
        "classification": if violations.is_empty() { "isolated_in_this_fixture" } else { "unsafe" },
        "violations": violations,
    })
}

#[nativelink_test]
#[ignore = "requires disposable certificates from tools/test-authority-listener-tls.py"]
async fn fixed_authority_mtls_boundary() {
    let directory = PathBuf::from(
        std::env::var("NL_AUTHORITY_LISTENER_CERT_DIR")
            .expect("explicit TLS qualification requires NL_AUTHORITY_LISTENER_CERT_DIR"),
    );
    let output = PathBuf::from(
        std::env::var("NL_AUTHORITY_LISTENER_TLS_REPORT")
            .expect("explicit TLS qualification requires NL_AUTHORITY_LISTENER_TLS_REPORT"),
    );
    let mut observations = Vec::new();
    for shared_trust in [false, true] {
        for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
            observations.push(exercise(&directory, algorithm, shared_trust).await);
        }
    }
    std::fs::write(output, serde_json::to_vec_pretty(&json!({
        "scope": "Actual fixed authority listeners with disposable local mTLS identities; no deployment claim",
        "passed": true,
        "observations": observations,
    })).unwrap()).unwrap();
}
