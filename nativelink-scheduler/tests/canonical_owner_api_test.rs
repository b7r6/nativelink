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

//! External Linux crate identity and closed public owner bridge.
#![cfg(target_os = "linux")]

use std::sync::Arc;

use nativelink_canonical_config::parse_canonical;
use nativelink_canonical_filesystem::Ownership;
use nativelink_macro::nativelink_test;
use nativelink_scheduler::authority::journal::{OwnerPhase, PreparedCanonicalOwner};
use rustix::fs::{FlockOperation, flock};

#[nativelink_test]
async fn synchronized_namespace_constructs_only_an_inert_preparing_owner() {
    let root = std::env::temp_dir().join(format!(
        "nativelink-owner-external-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let root = root.canonicalize().unwrap();
    let roots = ["journal", "cas", "sink", "worker"].map(|name| {
        let path = root.join(name);
        std::fs::create_dir(&path).unwrap();
        path.to_str().unwrap().to_owned()
    });
    let mut manifest = parse_canonical(include_bytes!(
        "../../tools/canonical-config/fixtures/canonical.json"
    ))
    .unwrap();
    manifest.authority.journal_root.clone_from(&roots[0]);
    manifest.cas.root.clone_from(&roots[1]);
    manifest.temporary_action_cache.root.clone_from(&roots[2]);
    manifest.worker.root.clone_from(&roots[3]);
    let namespace = Arc::new(Ownership::claim_cold(&manifest).unwrap())
        .resynchronize()
        .unwrap();
    let commitment = namespace.commitment();
    let owner = PreparedCanonicalOwner::prepare_cold(namespace)
        .await
        .unwrap();
    let diagnostic = owner.diagnostic();
    assert_eq!(diagnostic.namespace_commitment, commitment);
    assert_eq!(diagnostic.node_id, manifest.authority.node_id);
    assert_eq!(diagnostic.epoch, manifest.authority.cluster_epoch);
    assert_eq!(diagnostic.namespace, manifest.authority.namespace);
    assert_eq!(diagnostic.phase, OwnerPhase::Preparing);
    assert!(!diagnostic.repair_observed);
    for path in &roots {
        let file = std::fs::File::open(path).unwrap();
        assert_eq!(
            flock(&file, FlockOperation::NonBlockingLockExclusive),
            Err(rustix::io::Errno::WOULDBLOCK)
        );
    }
    owner.release().await.unwrap();
    let namespace = Arc::new(Ownership::reopen(&manifest).unwrap())
        .resynchronize()
        .unwrap();
    assert!(
        PreparedCanonicalOwner::reopen_sealed(namespace)
            .await
            .is_err()
    );
    std::fs::remove_dir_all(root).unwrap();
}
