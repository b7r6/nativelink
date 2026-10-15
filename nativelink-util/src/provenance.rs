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

//! Build-provenance events: which action and output path produced a digest,
//! and what happened when it was uploaded.
//!
//! Every event is a `DEBUG` event on the [`PROVENANCE_TARGET`] tracing target,
//! so the default `info` filter drops them and they cost one callsite check.
//! Turn them on with `RUST_LOG=info,nativelink_provenance=debug`, and set
//! `NL_LOG=json` to get one JSON object per line.
//!
//! Every event carries `event` (its kind) and `schema` ([`PROVENANCE_SCHEMA`]).
//! Events emitted on behalf of a client request also carry the client's
//! `RequestMetadata`, flattened by [`ProvenanceMetadata`]. Emit them with
//! [`provenance_event!`](crate::provenance_event) so the target, schema and
//! metadata fields stay identical across crates.

use nativelink_proto::build::bazel::remote::execution::v2::RequestMetadata;
use opentelemetry::Context;
use opentelemetry::baggage::BaggageExt;

use crate::origin_event::{BAZEL_METADATA_KEY, request_metadata_from_baggage};

/// The tracing target every provenance event is emitted on.
pub const PROVENANCE_TARGET: &str = "nativelink_provenance";

/// Version of the event field layout. Bump it when a field changes meaning or
/// is removed; adding a field does not need a bump.
pub const PROVENANCE_SCHEMA: u32 = 1;

/// Whether provenance events are enabled. Check this before doing any work
/// whose only purpose is a provenance event, such as building a manifest.
#[inline]
#[must_use]
pub fn provenance_enabled() -> bool {
    tracing::enabled!(target: PROVENANCE_TARGET, tracing::Level::DEBUG)
}

/// The client's `RequestMetadata`, flattened to the fields provenance events
/// carry. A field the client did not send is an empty string, so every event
/// of a kind has the same fields.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProvenanceMetadata {
    pub tool_name: String,
    pub tool_version: String,
    pub tool_invocation_id: String,
    pub correlated_invocations_id: String,
    pub action_id: String,
    pub action_mnemonic: String,
    pub target_id: String,
    pub configuration_id: String,
}

impl ProvenanceMetadata {
    /// The metadata of the request the current task is serving, read from the
    /// OpenTelemetry baggage the gRPC middleware fills in from Bazel's
    /// `requestmetadata-bin` header. Empty when there is none.
    #[must_use]
    pub fn from_current_context() -> Self {
        let context = Context::current();
        context
            .baggage()
            .get(BAZEL_METADATA_KEY)
            .and_then(|value| request_metadata_from_baggage(value.as_str()).ok())
            .map_or_else(Self::default, |metadata| Self::from(&metadata))
    }
}

impl From<&RequestMetadata> for ProvenanceMetadata {
    fn from(metadata: &RequestMetadata) -> Self {
        let (tool_name, tool_version) = metadata
            .tool_details
            .as_ref()
            .map(|tool| (tool.tool_name.clone(), tool.tool_version.clone()))
            .unwrap_or_default();
        Self {
            tool_name,
            tool_version,
            tool_invocation_id: metadata.tool_invocation_id.clone(),
            correlated_invocations_id: metadata.correlated_invocations_id.clone(),
            action_id: metadata.action_id.clone(),
            action_mnemonic: metadata.action_mnemonic.clone(),
            target_id: metadata.target_id.clone(),
            configuration_id: metadata.configuration_id.clone(),
        }
    }
}

/// Emits a provenance event.
///
/// ```ignore
/// provenance_event!("cas_write", &metadata; digest = %digest, size_bytes = size);
/// provenance_event!("backend_write"; key = %key, size_bytes = size);
/// ```
///
/// The first form adds the [`ProvenanceMetadata`] fields; the second is for
/// events with no client request behind them. Fields use `tracing` syntax and
/// are separated by commas.
#[macro_export]
macro_rules! provenance_event {
    ($event:literal, $metadata:expr; $($fields:tt)*) => {{
        let metadata: &$crate::provenance::ProvenanceMetadata = $metadata;
        ::tracing::debug!(
            target: "nativelink_provenance",
            event = $event,
            schema = $crate::provenance::PROVENANCE_SCHEMA,
            tool_name = %metadata.tool_name,
            tool_version = %metadata.tool_version,
            tool_invocation_id = %metadata.tool_invocation_id,
            correlated_invocations_id = %metadata.correlated_invocations_id,
            action_id = %metadata.action_id,
            action_mnemonic = %metadata.action_mnemonic,
            target_id = %metadata.target_id,
            configuration_id = %metadata.configuration_id,
            $($fields)*
        );
    }};
    ($event:literal; $($fields:tt)*) => {{
        ::tracing::debug!(
            target: "nativelink_provenance",
            event = $event,
            schema = $crate::provenance::PROVENANCE_SCHEMA,
            $($fields)*
        );
    }};
    ($event:literal) => {
        $crate::provenance_event!($event;)
    };
}

#[cfg(test)]
mod tests {
    use nativelink_proto::build::bazel::remote::execution::v2::ToolDetails;

    use super::*;

    #[test]
    fn metadata_flattens_every_field() {
        let metadata = RequestMetadata {
            tool_details: Some(ToolDetails {
                tool_name: "bazel".to_string(),
                tool_version: "8.4.2".to_string(),
            }),
            action_id: "action".to_string(),
            tool_invocation_id: "invocation".to_string(),
            correlated_invocations_id: "build".to_string(),
            action_mnemonic: "CppLink".to_string(),
            target_id: "//app:bin".to_string(),
            configuration_id: "k8-opt".to_string(),
        };
        assert_eq!(
            ProvenanceMetadata::from(&metadata),
            ProvenanceMetadata {
                tool_name: "bazel".to_string(),
                tool_version: "8.4.2".to_string(),
                tool_invocation_id: "invocation".to_string(),
                correlated_invocations_id: "build".to_string(),
                action_id: "action".to_string(),
                action_mnemonic: "CppLink".to_string(),
                target_id: "//app:bin".to_string(),
                configuration_id: "k8-opt".to_string(),
            }
        );
    }

    #[test]
    fn missing_tool_details_are_empty() {
        let metadata = ProvenanceMetadata::from(&RequestMetadata::default());
        assert_eq!(metadata, ProvenanceMetadata::default());
    }

    #[test]
    fn no_baggage_is_empty_metadata() {
        assert_eq!(
            ProvenanceMetadata::from_current_context(),
            ProvenanceMetadata::default()
        );
    }

    #[test]
    fn macro_compiles_in_both_forms() {
        let metadata = ProvenanceMetadata::default();
        provenance_event!("test_event", &metadata; size_bytes = 1_u64, digest = %"abc-1");
        provenance_event!("test_event"; size_bytes = 1_u64);
        provenance_event!("test_event");
    }
}
