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

//! # nativelink-oci-registry
//!
//! OCI Distribution Specification protocol primitives for `NativeLink` —
//! the wire vocabulary (repository names, tags, digests, error codes) and
//! the store records (digest-alias index and mutable tag refs) backing the
//! `oci_registry` fork service. Peer of `nativelink-nix`, and built on the
//! same discipline: records are prost messages inside an `ActionResult`
//! envelope so `completeness_checking` can 404 a record whose backing blob
//! was evicted, and every field tag is a wire contract.
//!
//! See `design/oci-registry-over-cas.md` for the full design.

pub mod records;
pub mod wire;
