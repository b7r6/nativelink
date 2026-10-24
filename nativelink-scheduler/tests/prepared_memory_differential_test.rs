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

//! Additive comparison written after independent candidate/reference freezes.
#[path = "prepared_memory_differential/adapter.rs"]
mod adapter;
#[path = "prepared_memory_differential/cases.rs"]
mod cases;
#[path = "prepared_memory_differential/fixtures.rs"]
mod fixtures;
#[allow(
    dead_code,
    reason = "Frozen APIs include operations outside the bounded comparison alphabet."
)]
mod publication_candidate {
    #[path = "../retention/coarse.rs"]
    pub(crate) mod coarse;
    pub(crate) mod engine;
    pub(crate) use super::protocol;
    pub(crate) mod types;
}
#[allow(
    dead_code,
    reason = "Frozen graph vocabulary supplies scenario validation."
)]
#[path = "prepared_memory/head.rs"]
mod head;
#[allow(
    dead_code,
    reason = "Frozen relation retains its original standalone APIs."
)]
#[path = "authority/reference.rs"]
mod m1;
#[allow(dead_code, reason = "Only frozen shared retention vocabulary is used.")]
#[path = "retention/protocol.rs"]
pub(crate) mod protocol;
#[path = "prepared_memory/reference.rs"]
mod reference;
#[allow(dead_code, reason = "Only frozen report vocabulary is used.")]
#[path = "terminal_outcome/types.rs"]
mod terminal_types;
#[path = "prepared_memory/types.rs"]
mod types;
