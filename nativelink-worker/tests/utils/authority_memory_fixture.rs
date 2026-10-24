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

//! A real allocation sized by the same process-group PSS the worker charges.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::{Duration, Instant};

use nativelink_util::task::JoinHandleDropGuard;
use nativelink_util::{spawn, spawn_blocking};
use nativelink_worker::running_actions_manager::{parse_kb_field, parse_pgid_from_stat};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::task::JoinSet;

pub(crate) const OBSERVER_ENV: &str = "NATIVELINK_TEST_MEMORY_OBSERVER";
const ALLOCATION_STEPS: u64 = 192;

/// The namespace's allocator cannot read its outer stub's smaps. Keep the
/// observer alongside the test's worker, where both process records are visible.
pub(crate) fn start_observer(socket_path: &Path) -> JoinHandleDropGuard<()> {
    let listener = UnixListener::bind(socket_path).unwrap();
    spawn!("authority_memory_fixture", async move {
        let mut clients = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (mut socket, _) = accepted.unwrap();
                    clients.spawn(async move {
                        let mut identities = [0_u8; 8];
                        if socket.read_exact(&mut identities).await.is_err() {
                            return;
                        }
                        let process_id = u32::from_le_bytes(identities[..4].try_into().unwrap());
                        let group_id = u32::from_le_bytes(identities[4..].try_into().unwrap());
                        // The fixture creates no descendants. A namespaced
                        // action adds exactly its outer process-group leader.
                        let mut members = vec![process_id];
                        if process_id != group_id {
                            members.push(group_id);
                        }
                        let mut request = [0_u8; 1];
                        while socket.read_exact(&mut request).await.is_ok() {
                            let members = members.clone();
                            let observation = spawn_blocking!("fixture_group_memory", move || {
                                members.iter().try_fold(0_u64, |total, member| {
                                    let stat = std::fs::read_to_string(format!("/proc/{member}/stat"))?;
                                    assert_eq!(parse_pgid_from_stat(&stat), Some(group_id));
                                    let rollup = std::fs::read_to_string(format!("/proc/{member}/smaps_rollup"))?;
                                    let memory = parse_kb_field(&rollup, "Pss:")
                                        .expect("Every fixture group member must report PSS");
                                    Ok::<_, std::io::Error>(total + memory)
                                })
                            }).await.unwrap();
                            // An enforced kill can remove either member or
                            // close the socket between the request and reply.
                            let Ok(memory_kb) = observation else { break };
                            if socket.write_all(&memory_kb.to_le_bytes()).await.is_err() {
                                break;
                            }
                        }
                    });
                }
                finished = clients.join_next(), if !clients.is_empty() => {
                    finished.unwrap().unwrap();
                }
            }
        }
    })
}

/// Retain touched chunks until the whole action group reaches the target.
/// Runtime and sanitizer allocations count toward it, rather than being added
/// above an assumed fixed allocation. Neither the grants nor enforcement change.
pub(crate) fn allocate_to_target(target_kb: u64, upper_bound_kb: u64) -> Vec<Vec<u8>> {
    assert!(target_kb < upper_bound_kb);
    // Every observation walks the resident pages of both processes. Use the
    // measured deficit instead of repeatedly scanning after tiny fixed chunks.
    // Keep each chunk within a quarter of the accepted window; actual runtime
    // growth and sharing changes must still pass the final measured upper bound.
    let max_chunk_kb = (upper_bound_kb - target_kb) / 4;
    assert!(max_chunk_kb >= 1024);
    let path = std::env::var_os(OBSERVER_ENV).expect("The memory observer must be configured");
    let mut socket = UnixStream::connect(path).unwrap();
    let timeout = Duration::from_secs(5);
    socket.set_read_timeout(Some(timeout)).unwrap();
    socket.set_write_timeout(Some(timeout)).unwrap();
    // /proc is not remounted in this fixture. Its IDs match the worker's view,
    // unlike getpid/getpgrp inside the nested PID namespace.
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    let process_id: u32 = stat.split_once(' ').unwrap().0.parse().unwrap();
    let group_id = parse_pgid_from_stat(&stat).unwrap();
    assert!(process_id == group_id || std::process::id() == 1);
    socket.write_all(&process_id.to_le_bytes()).unwrap();
    socket.write_all(&group_id.to_le_bytes()).unwrap();
    let started = Instant::now();
    let mut allocations = Vec::new();
    let mut allocated_kb = 0_u64;
    let mut last_observed_kb = None;
    loop {
        assert!(
            started.elapsed() < timeout,
            "Memory calibration exceeded its deadline: elapsed={:?}, chunks={}, allocated={allocated_kb} KiB, last_observed={last_observed_kb:?} KiB, target={target_kb} KiB",
            started.elapsed(),
            allocations.len(),
        );
        socket.write_all(&[1]).unwrap();
        let mut response = [0_u8; 8];
        socket.read_exact(&mut response).unwrap();
        let memory_kb = u64::from_le_bytes(response);
        last_observed_kb = Some(memory_kb);
        assert!(
            !allocations.is_empty() || memory_kb < target_kb,
            "Fixture runtime already exceeds the workload target: {memory_kb} KiB"
        );
        if memory_kb >= target_kb {
            assert!(
                memory_kb < upper_bound_kb,
                "Allocation exceeded the workload window: {memory_kb} KiB"
            );
            eprintln!("MEMORY_FIXTURE_GROUP_KB={memory_kb}");
            return allocations;
        }
        assert!(
            u64::try_from(allocations.len()).unwrap() < ALLOCATION_STEPS
                && allocated_kb < target_kb,
            "Resident allocation did not reach its target: chunks={}, allocated={allocated_kb} KiB, observed={memory_kb} KiB, target={target_kb} KiB",
            allocations.len(),
        );
        let chunk_kb = (target_kb - memory_kb)
            .clamp(1024, max_chunk_kb)
            .min(target_kb - allocated_kb);
        let chunk_bytes = usize::try_from(chunk_kb.checked_mul(1024).unwrap()).unwrap();
        let mut bytes = vec![0_u8; chunk_bytes];
        for byte in bytes.iter_mut().step_by(4096) {
            *byte = 17;
        }
        allocations.push(bytes);
        allocated_kb += chunk_kb;
    }
}
