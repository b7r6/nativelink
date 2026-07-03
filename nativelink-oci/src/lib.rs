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

//! # nativelink-oci
//!
//! OCI toolchain bridge for `NativeLink`.
//!
//! Implements the OCI→REAPI projection defined in the Standard OCI Toolchain
//! Specification (§6): pulls an OCI image from a registry, unpacks its layers,
//! re-hashes file content as BLAKE3, constructs REAPI `Directory` trees, and
//! uploads the resulting blobs to a `NativeLink` CAS store.
//!
//! The output is a root `Directory` digest that can be merged into an action's
//! `input_root_digest` for remote execution — making toolchains data in CAS
//! rather than infrastructure on workers.

pub mod oci_client;
pub mod projection;
pub mod registry;
