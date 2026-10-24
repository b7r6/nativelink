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

//! Comparison authored after both terminal engines were separately frozen.
//! See design/categorical/terminal-outcome-differential.md for the common alphabet.

#[path = "terminal_outcome_differential/adapter.rs"]
mod adapter;
#[path = "terminal_outcome_differential/cases.rs"]
mod cases;
#[path = "terminal_outcome_differential/fixtures.rs"]
mod fixtures;

#[allow(
    dead_code,
    reason = "Frozen engines retain APIs outside this common alphabet."
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
    reason = "Frozen independent scheduler has its own full suite."
)]
#[path = "authority/reference.rs"]
mod m1;
#[path = "retention/protocol.rs"]
pub(crate) mod protocol;
#[allow(
    dead_code,
    reason = "Frozen reference exposes additional neutral observations."
)]
#[path = "terminal_outcome/reference.rs"]
mod reference;
#[allow(
    dead_code,
    reason = "Frozen independent retention has its own full suite."
)]
#[path = "retention/reference.rs"]
mod retention;
#[path = "terminal_outcome/types.rs"]
mod types;
