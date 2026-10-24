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

//! Real filesystem preparation composed with the frozen publication candidate.

#[path = "publication_physical/cases.rs"]
mod cases;
#[path = "retention/coarse.rs"]
mod coarse;
#[path = "publication_candidate/engine.rs"]
mod engine;
#[path = "publication_physical/fixture.rs"]
mod fixture;
#[path = "retention/protocol.rs"]
mod protocol;
#[path = "publication_candidate/types.rs"]
mod types;
