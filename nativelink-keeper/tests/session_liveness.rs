// Copyright 2026 The NativeLink Authors. All rights reserved.
//! THE proof test: sessions-as-liveness works in-process. A worker's
//! registration is an ephemeral node; the worker dying (session lost,
//! no goodbye) removes the registration at the coordination layer.
#![cfg(feature = "embedded")]

use core::time::Duration;
use std::time::Instant;

use nativelink_keeper::{KeeperError, Server};

#[test]
fn ephemeral_node_dies_with_its_session() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path(), Duration::from_millis(50)).unwrap();

    // An observer session that outlives the worker.
    let observer = server.session(Duration::from_secs(10)).unwrap();

    // The "worker": short session timeout, registers ephemerally.
    let worker = server.session(Duration::from_millis(200)).unwrap();
    worker
        .create("/workers/worker-1", b"epoch-1", /* ephemeral= */ true)
        .unwrap();
    assert!(observer.exists("/workers/worker-1").unwrap().is_some(),
        "registration visible to other sessions");

    // Kill the worker without a goodbye — crash semantics.
    worker.abandon();

    // The node must vanish once the server expires the session. Poll with
    // a hard deadline; expiry is 200ms + tick granularity.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match observer.exists("/workers/worker-1").unwrap() {
            None => break, // reaped — sessions-as-liveness holds.
            Some(_) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Some(_) => panic!("ephemeral node outlived its abandoned session"),
        }
    }

    // And CAS still behaves on a normal node.
    observer.create("/ops/op-1", b"v0", false).unwrap();
    let v1 = observer.set_cas("/ops/op-1", b"v1", 0).unwrap();
    assert_eq!(
        observer.set_cas("/ops/op-1", b"clobber", 0),
        Err(KeeperError::BadVersion),
        "stale CAS must be rejected"
    );
    let (data, ver) = observer.get("/ops/op-1").unwrap();
    assert_eq!((data.as_slice(), ver), (b"v1".as_slice(), v1));
}
