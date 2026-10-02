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

//! OCI → REAPI bridge integration test against a real registry.
//!
//! Drives the full server path — `OciFetchConfig` → `OciRegistryConfig` →
//! `RegistrySettings` → `RegistryClient` (WWW-Authenticate challenge) → pull →
//! project (BLAKE3) → CAS upload — and asserts **§7 cross-projection agreement**:
//! the server-projected REAPI root `Directory` digest MUST equal the producer's
//! §6.5 `dev.straylight.toolchain.reapi.root` hint. That is the load-bearing
//! interop property: two independent implementations (the nix producer's
//! `reapi_dir.py` and this rust consumer's `projection.rs`) reduce the same
//! logical content tree to the same BLAKE3 Merkle root.
//!
//! Network- and env-gated so it is inert in CI (no env → early return). Run:
//!
//! ```sh
//! ZOT_TEST_IMAGE=oci://registry.sju1.s4.gl/straylight/prelude-tools-ghc912-libstdcxx-glibc:latest \
//! ZOT_TEST_REAPI_ROOT=a9e765a44bafb8f119a4dffeaa05caf180277aba8f9656fe308355509e2e2787 \
//! cargo test -p nativelink-service --test oci_bridge_integration_test -- --nocapture
//! ```

use std::sync::Arc;

use nativelink_config::cas_server::{
    FetchConfig, OciFetchConfig, OciRegistryConfig, WithInstanceName,
};
use nativelink_config::stores::{MemorySpec, StoreSpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::asset::v1::FetchDirectoryRequest;
use nativelink_proto::build::bazel::remote::asset::v1::fetch_server::Fetch;
use nativelink_service::fetch_server::FetchServer;
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use tonic::Request;

async fn memory_store_manager() -> Result<Arc<StoreManager>, Error> {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store(
        "cas",
        store_factory(
            &StoreSpec::Memory(MemorySpec::default()),
            &store_manager,
            None,
        )
        .await?,
    );
    Ok(store_manager)
}

#[nativelink_test]
async fn oci_bridge_projects_to_producer_hint() -> Result<(), Error> {
    let Ok(image) = std::env::var("ZOT_TEST_IMAGE") else {
        eprintln!("ZOT_TEST_IMAGE unset — skipping OCI bridge integration test");
        return Ok(());
    };
    let want_root = std::env::var("ZOT_TEST_REAPI_ROOT")
        .expect("ZOT_TEST_REAPI_ROOT must accompany ZOT_TEST_IMAGE");
    // The registry host to configure (derived from the image ref, minus the scheme/path).
    let host = image
        .trim_start_matches("oci://")
        .trim_start_matches("docker://")
        .split('/')
        .next()
        .expect("image ref has a registry host")
        .to_string();

    let store_manager = memory_store_manager().await?;
    let fetch_server = FetchServer::new(
        &[WithInstanceName {
            instance_name: "main".to_string(),
            config: FetchConfig {
                fetch_store: "cas".to_string(),
                oci: Some(OciFetchConfig {
                    cas_store: None,
                    dedup_check: true,
                    digest_function: "BLAKE3".to_string(),
                    registries: vec![OciRegistryConfig {
                        host,
                        scheme: Some("https".to_string()),
                        root_certificates: None,
                        insecure_skip_verify: false,
                        username: None,
                        password: None,
                        bearer_token: None,
                    }],
                    self_registry: None,
                }),
            },
        }],
        &store_manager,
    )
    .expect("FetchServer::new");

    let response = fetch_server
        .fetch_directory(Request::new(FetchDirectoryRequest {
            instance_name: "main".to_string(),
            timeout: None,
            oldest_content_accepted: None,
            uris: vec![image.clone()],
            qualifiers: vec![],
            digest_function: 0,
        }))
        .await
        .expect("fetch_directory")
        .into_inner();

    let root = response
        .root_directory_digest
        .expect("bridge returned no root Directory digest");
    eprintln!(
        "image = {image}\n  server REAPI root = {}/{}\n  producer hint    = {want_root}",
        root.hash, root.size_bytes,
    );
    assert_eq!(
        root.hash, want_root,
        "§7 cross-projection agreement: server-projected REAPI root must equal the producer hint",
    );
    Ok(())
}
