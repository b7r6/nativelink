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

#[path = "prepared_memory/cases.rs"]
mod cases;
#[path = "prepared_memory/head.rs"]
#[allow(
    dead_code,
    reason = "The unchanged head oracle supplies graph types and scenario validation only."
)]
mod head;
#[path = "authority/reference.rs"]
mod m1;
#[path = "retention/protocol.rs"]
#[allow(
    dead_code,
    reason = "Only the unchanged terminal vocabulary's retention types are imported."
)]
mod protocol;
#[path = "prepared_memory/reference.rs"]
mod reference;
#[path = "terminal_outcome/types.rs"]
#[allow(
    dead_code,
    reason = "This additive model uses a subset of the frozen terminal vocabulary."
)]
mod terminal_types;
#[path = "prepared_memory/types.rs"]
mod types;
