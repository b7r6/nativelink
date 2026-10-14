// Copyright 2024 The NativeLink Authors. All rights reserved.
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

//! Deterministic simulation of the `NativeLink` scheduler: the real scheduler
//! and services on turmoil's simulated network, simulated workers and
//! clients, injected faults, and a checker over the history. See `README.md`.

pub mod checker;
pub mod client;
pub mod config;
pub mod det;
pub mod faults;
pub mod history;
pub mod net;
pub mod runner;
pub mod scheduler;
pub mod worker;
pub mod workload;

/// FNV-1a over a history's text: two runs of one seed must agree on it.
pub fn history_hash(text: &str) -> u64 {
    history_hash_bytes(text.as_bytes())
}

/// FNV-1a over bytes.
pub fn history_hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// The history with every UUID replaced by an alias in order of first
/// appearance (`u0`, `u1`, ...), so two runs can be compared on behavior
/// alone when entropy is not seeded.
pub fn normalize_uuids(text: &str) -> String {
    let mut aliases: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut out = String::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 36 <= bytes.len() && is_uuid(&bytes[i..i + 36]) {
            let uuid = &text[i..i + 36];
            let next = aliases.len();
            let alias = aliases
                .entry(uuid.to_string())
                .or_insert_with(|| format!("u{next}"));
            out.push_str(alias);
            i += 36;
        } else {
            out.push(char::from(bytes[i]));
            i += 1;
        }
    }
    out
}

fn is_uuid(candidate: &[u8]) -> bool {
    candidate.iter().enumerate().all(|(index, byte)| match index {
        8 | 13 | 18 | 23 => *byte == b'-',
        _ => byte.is_ascii_hexdigit(),
    })
}
