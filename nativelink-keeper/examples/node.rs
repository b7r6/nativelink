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
//
// node — 3-box fleet proof driver for nativelink-keeper.
//
//   node --dir DIR --id N --ensemble "1=host:port,2=host:port,3=host:port" \
//        [--register PATH] [--watch PATH] [--session-timeout-ms MS] [--goodbye]
//
// Starts an ensemble member, opens a session, optionally registers an
// ephemeral node (data "epoch-<id>") and/or watches a path (printing
// "EVENT <type> <path>" lines), prints "READY session=<sid>", then runs
// until SIGTERM. On SIGTERM the process exits WITHOUT closing the session
// gracefully — the ephemeral must be reaped by server-side session expiry
// (abandon semantics). Pass --goodbye to instead close the session cleanly
// on SIGTERM, removing ephemerals immediately.

use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use std::io::Write as _;
use std::path::PathBuf;

use nativelink_keeper::{Server, Session, WatchEvent};

static TERM: AtomicBool = AtomicBool::new(false);

extern "C" fn on_sigterm(_sig: i32) {
    TERM.store(true, Ordering::SeqCst);
}

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn print_event(ev: WatchEvent, path: &str) {
    let ty = match ev {
        WatchEvent::Created => "CREATED",
        WatchEvent::Deleted => "DELETED",
        WatchEvent::Changed => "CHANGED",
        WatchEvent::SessionLost => "SESSION_LOST",
    };
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "EVENT {ty} {path}");
    let _ = out.flush();
}

// One-shot ZK watches. Callbacks fire on the keeper dispatcher thread and
// must not block (nativelink-keeper.h:6-7), so re-subscribing there would
// deadlock: hand events to the main loop over a channel and re-arm from it.
fn arm(session: &Session<'_>, path: &str, tx: &std::sync::mpsc::Sender<(WatchEvent, String)>) {
    let tx = tx.clone();
    if let Err(e) = session.watch(path, move |ev, at| {
        let _ = tx.send((ev, at.to_string()));
    }) {
        eprintln!("watch {path} failed: {e:?}");
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = PathBuf::from(arg(&args, "--dir").expect("--dir required"));
    let id: u32 = arg(&args, "--id").expect("--id required").parse().unwrap();
    let ensemble_raw = arg(&args, "--ensemble").expect("--ensemble required");
    let register = arg(&args, "--register");
    let watch = arg(&args, "--watch");
    let timeout_ms: u64 = arg(&args, "--session-timeout-ms")
        .map(|v| v.parse().unwrap())
        .unwrap_or(5000);
    let goodbye = args.iter().any(|a| a == "--goodbye");

    let ensemble: Vec<(u32, String)> = ensemble_raw
        .split(',')
        .map(|e| {
            let (id, hp) = e.split_once('=').expect("ensemble entry id=host:port");
            (id.parse().unwrap(), hp.to_string())
        })
        .collect();
    let ensemble_refs: Vec<(u32, &str)> =
        ensemble.iter().map(|(i, hp)| (*i, hp.as_str())).collect();

    unsafe {
        libc::signal(libc::SIGTERM, on_sigterm as libc::sighandler_t);
    }

    let server = Server::start_ensemble(&dir, Duration::from_millis(500), id, &ensemble_refs)
        .expect("server start");
    let session = server
        .session(Duration::from_millis(timeout_ms))
        .expect("session create");

    if let Some(path) = &register {
        let data = format!("epoch-{id}");
        session
            .create(path, data.as_bytes(), /*ephemeral=*/ true)
            .expect("register ephemeral");
    }
    let (tx, rx) = std::sync::mpsc::channel::<(WatchEvent, String)>();
    if let Some(path) = &watch {
        arm(&session, path, &tx);
    }

    {
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, "READY session={}", session.id());
        let _ = out.flush();
    }

    while !TERM.load(Ordering::SeqCst) {
        if let Ok((ev, at)) = rx.recv_timeout(Duration::from_millis(50)) {
            print_event(ev, &at);
            if ev != WatchEvent::SessionLost
                && let Some(path) = &watch
            {
                arm(&session, path, &tx); // re-arm one-shot watch
            }
        }
    }

    if goodbye {
        drop(session); // graceful Close: ephemerals removed immediately
        drop(server);
        std::process::exit(0);
    }
    // Abandon semantics: exit without running destructors — no Close is sent,
    // so ephemerals persist until the server expires the session.
    std::process::exit(1);
}
