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

//! Candidate request capture and the single experimental v1 resource mapping.
//! Raw CAS bytes are verified and retained without protobuf re-encoding. RPC
//! messages preserve fields known to the pinned schema after tonic decoding.
//! This is a local adapter primitive, not an activated service or converter.

use std::collections::BTreeMap;
use std::fmt;

use nativelink_config::cas_server::CpuUnit;
use nativelink_config::schedulers::PropertyType;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, Digest, ExecuteRequest, Platform, RequestMetadata, digest_function,
};
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use prost::Message;
use prost_types::Timestamp;
use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::{ActionKey, Request, Resources};

const VERSION: u32 = 1;
pub const MAX_REQUEST_BYTES: usize = 256 * 1024;
const MAX_PROTO_SECONDS: i64 = 315_576_000_000;
const RESERVED: &[&str] = &[
    "cpu_millis",
    "cpu_millicores",
    "memory_bytes",
    "memory_mib",
    "memory_mb",
    "slots",
    "disk_kb",
    "network_kbps",
    "disk_read_iops",
    "disk_write_iops",
    "disk_read_bps",
    "disk_write_bps",
    "shm_size",
    "gpu_count",
];

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "RequestWire")]
pub struct RequestPayload {
    version: u32,
    execute: Vec<u8>,
    metadata: Option<Vec<u8>>,
    queued: Vec<u8>,
    principal: String,
    action: Vec<u8>,
    command: Vec<u8>,
    sha256: [u8; 32],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestWire {
    version: u32,
    #[serde(deserialize_with = "bounded_bytes")]
    execute: Vec<u8>,
    #[serde(deserialize_with = "bounded_optional_bytes")]
    metadata: Option<Vec<u8>>,
    #[serde(deserialize_with = "bounded_bytes")]
    queued: Vec<u8>,
    #[serde(deserialize_with = "bounded_principal")]
    principal: String,
    #[serde(deserialize_with = "bounded_bytes")]
    action: Vec<u8>,
    #[serde(deserialize_with = "bounded_bytes")]
    command: Vec<u8>,
    sha256: [u8; 32],
}

fn bounded_bytes<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    struct Bytes;
    impl<'de> Visitor<'de> for Bytes {
        type Value = Vec<u8>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("bounded request bytes")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
            let mut bytes =
                Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX_REQUEST_BYTES));
            while bytes.len() < MAX_REQUEST_BYTES {
                let Some(byte) = sequence.next_element()? else {
                    return Ok(bytes);
                };
                bytes.push(byte);
            }
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom(
                    "request section exceeds byte bound",
                ));
            }
            Ok(bytes)
        }
    }
    deserializer.deserialize_seq(Bytes)
}

fn bounded_principal<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    struct Principal;
    impl Visitor<'_> for Principal {
        type Value = String;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("bounded request principal")
        }
        fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<Self::Value, E> {
            if text.len() > MAX_REQUEST_BYTES {
                return Err(E::custom("request principal exceeds byte bound"));
            }
            Ok(text.to_owned())
        }
    }
    deserializer.deserialize_str(Principal)
}

fn bounded_optional_bytes<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<u8>>, D::Error> {
    #[derive(Deserialize)]
    struct Bytes(#[serde(deserialize_with = "bounded_bytes")] Vec<u8>);
    Option::<Bytes>::deserialize(deserializer).map(|bytes| bytes.map(|bytes| bytes.0))
}

impl TryFrom<RequestWire> for RequestPayload {
    type Error = String;
    fn try_from(wire: RequestWire) -> Result<Self, Self::Error> {
        let payload = Self {
            version: wire.version,
            execute: wire.execute,
            metadata: wire.metadata,
            queued: wire.queued,
            principal: wire.principal,
            action: wire.action,
            command: wire.command,
            sha256: wire.sha256,
        };
        payload.project()?;
        Ok(payload)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestProjection {
    pub action: ActionKey,
    pub execution_scope: Option<String>,
    pub do_not_cache: bool,
    pub skip_cache_lookup: bool,
    pub priority: i32,
    pub properties: BTreeMap<String, String>,
    pub resources: Resources,
    /// Conservative one-second logical ticks; worker execution keeps the exact
    /// protobuf duration. Absent timeout is the explicit unbounded sentinel.
    pub execution_timeout: u64,
}

#[derive(Clone, Debug)]
pub struct DispatchFacts {
    pub execute_request: ExecuteRequest,
    pub request_metadata: Option<RequestMetadata>,
    pub queued_timestamp: Timestamp,
    /// Complete selected platform, including the original resource quantities.
    pub platform: Platform,
}

fn algorithm(value: i32) -> Result<DigestHasherFunc, String> {
    match digest_function::Value::try_from(value).ok() {
        Some(digest_function::Value::Unknown | digest_function::Value::Sha256) => {
            Ok(DigestHasherFunc::Sha256)
        }
        Some(digest_function::Value::Blake3) => Ok(DigestHasherFunc::Blake3),
        _ => Err("v1 supports SHA256 (including the fixed default) or BLAKE3 only".to_owned()),
    }
}

fn validate_digest(digest: &Digest, role: &str) -> Result<(), String> {
    if digest.size_bytes < 0
        || digest.hash.len() != 64
        || !digest
            .hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "{role} requires a nonnegative size and lowercase 64-digit digest"
        ));
    }
    Ok(())
}

fn verify_bytes(
    bytes: &[u8],
    digest: &Digest,
    hash: DigestHasherFunc,
    role: &str,
) -> Result<(), String> {
    validate_digest(digest, role)?;
    let mut hasher = hash.hasher();
    hasher.update(bytes);
    let found = hasher.finalize_digest();
    if found.size_bytes() != u64::try_from(digest.size_bytes).map_err(|error| error.to_string())?
        || found.packed_hash().to_string() != digest.hash
    {
        return Err(format!("{role} bytes do not match their CAS digest"));
    }
    Ok(())
}

fn quantity(name: &str, text: &str, scale: u64) -> Result<u64, String> {
    if text.is_empty()
        || (text.len() > 1 && text.starts_with('0'))
        || !text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(format!("{name} requires canonical unsigned decimal"));
    }
    text.parse::<u64>()
        .ok()
        .and_then(|value| value.checked_mul(scale))
        .ok_or_else(|| format!("{name} value or scaled quantity overflows u64"))
}

fn selected_platform(action: &Action, command: &Command) -> Platform {
    action
        .platform
        .as_ref()
        .filter(|platform| !platform.properties.is_empty())
        .or(command.platform.as_ref())
        .cloned()
        .unwrap_or_default()
}

pub fn normalize_platform(
    platform: &Platform,
) -> Result<(BTreeMap<String, String>, Resources), String> {
    let mut all = BTreeMap::new();
    for property in &platform.properties {
        if all
            .insert(property.name.clone(), property.value.clone())
            .is_some()
        {
            return Err(format!("duplicate platform property: {}", property.name));
        }
        if RESERVED.contains(&property.name.as_str()) {
            return Err(format!(
                "unsupported v1 resource/property spelling: {}",
                property.name
            ));
        }
    }
    let cpu_millis = all
        .remove("cpu_count")
        .map_or(Ok(0), |value| quantity("cpu_count", &value, 1000))?;
    let memory_bytes = all
        .remove("memory_kb")
        .map_or(Ok(0), |value| quantity("memory_kb", &value, 1024))?;
    Ok((
        all,
        Resources {
            cpu_millis,
            memory_bytes,
            slots: 1,
        },
    ))
}

fn validate_timestamp(timestamp: &Timestamp) -> Result<(), String> {
    if !(-62_135_596_800..=253_402_300_799).contains(&timestamp.seconds)
        || !(0..1_000_000_000).contains(&timestamp.nanos)
    {
        return Err("invalid queued Timestamp".to_owned());
    }
    Ok(())
}

fn timeout(action: &Action) -> Result<u64, String> {
    let Some(duration) = action.timeout else {
        return Ok(u64::MAX);
    };
    if !(0..=MAX_PROTO_SECONDS).contains(&duration.seconds)
        || !(0..1_000_000_000).contains(&duration.nanos)
    {
        return Err("invalid nonnegative Action timeout".to_owned());
    }
    Ok(
        (u64::try_from(duration.seconds).map_err(|error| error.to_string())?
            + u64::from(duration.nanos != 0))
        .max(1),
    )
}

fn invocation_scope(metadata: Option<&RequestMetadata>, principal: &str) -> Option<String> {
    let metadata = metadata?;
    if metadata.tool_invocation_id.is_empty()
        || !metadata
            .tool_details
            .as_ref()
            .is_some_and(|tool| tool.tool_name.eq_ignore_ascii_case("buck2"))
    {
        return None;
    }
    let mut hash = DigestHasherFunc::Sha256.hasher();
    hash.update(&u64::try_from(principal.len()).ok()?.to_be_bytes());
    hash.update(principal.as_bytes());
    hash.update(metadata.tool_invocation_id.as_bytes());
    Some(hash.finalize_digest().packed_hash().to_string())
}

impl RequestPayload {
    pub fn capture(
        execute: &ExecuteRequest,
        metadata: Option<&RequestMetadata>,
        queued: &Timestamp,
        principal: &str,
        action: &[u8],
        command: &[u8],
        max_bytes: usize,
    ) -> Result<Self, String> {
        let size = [
            execute.encoded_len(),
            metadata.map_or(0, Message::encoded_len),
            queued.encoded_len(),
            principal.len(),
            action.len(),
            command.len(),
        ]
        .into_iter()
        .try_fold(0usize, usize::checked_add)
        .ok_or("request byte size overflow")?;
        if size > max_bytes.min(MAX_REQUEST_BYTES) {
            return Err("request exceeds aggregate byte bound".to_owned());
        }
        let mut payload = Self {
            version: VERSION,
            execute: execute.encode_to_vec(),
            metadata: metadata.map(Message::encode_to_vec),
            queued: queued.encode_to_vec(),
            principal: principal.to_owned(),
            action: action.to_vec(),
            command: command.to_vec(),
            sha256: [0; 32],
        };
        payload.sha256 = payload.checksum();
        payload.project()?;
        Ok(payload)
    }

    fn checksum(&self) -> [u8; 32] {
        let mut hasher = DigestHasherFunc::Sha256.hasher();
        hasher.update(b"nativelink-request-v1");
        hasher.update(&self.version.to_be_bytes());
        hasher.update(&[u8::from(self.metadata.is_some())]);
        for bytes in [
            self.execute.as_slice(),
            self.metadata.as_deref().unwrap_or_default(),
            self.queued.as_slice(),
            self.principal.as_bytes(),
            self.action.as_slice(),
            self.command.as_slice(),
        ] {
            hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
            hasher.update(bytes);
        }
        **hasher.finalize_digest().packed_hash()
    }

    fn decoded(
        &self,
    ) -> Result<
        (
            ExecuteRequest,
            Option<RequestMetadata>,
            Timestamp,
            Action,
            Command,
        ),
        String,
    > {
        let size = [
            self.execute.len(),
            self.metadata.as_ref().map_or(0, Vec::len),
            self.queued.len(),
            self.principal.len(),
            self.action.len(),
            self.command.len(),
        ]
        .into_iter()
        .try_fold(0usize, usize::checked_add)
        .ok_or("request byte size overflow")?;
        if self.version != VERSION || size > MAX_REQUEST_BYTES || self.sha256 != self.checksum() {
            return Err("unsupported, oversized or corrupt request payload".to_owned());
        }
        let execute =
            ExecuteRequest::decode(self.execute.as_slice()).map_err(|error| error.to_string())?;
        let hash = algorithm(execute.digest_function)?;
        verify_bytes(
            &self.action,
            execute
                .action_digest
                .as_ref()
                .ok_or("missing Action digest")?,
            hash,
            "Action",
        )?;
        let action = Action::decode(self.action.as_slice()).map_err(|error| error.to_string())?;
        verify_bytes(
            &self.command,
            action
                .command_digest
                .as_ref()
                .ok_or("missing Command digest")?,
            hash,
            "Command",
        )?;
        validate_digest(
            action
                .input_root_digest
                .as_ref()
                .ok_or("missing input-root digest")?,
            "input root",
        )?;
        let command =
            Command::decode(self.command.as_slice()).map_err(|error| error.to_string())?;
        let metadata = self
            .metadata
            .as_ref()
            .map(|bytes| {
                RequestMetadata::decode(bytes.as_slice()).map_err(|error| error.to_string())
            })
            .transpose()?;
        let queued =
            Timestamp::decode(self.queued.as_slice()).map_err(|error| error.to_string())?;
        validate_timestamp(&queued)?;
        Ok((execute, metadata, queued, action, command))
    }

    pub fn project(&self) -> Result<RequestProjection, String> {
        let (execute, metadata, _, action, command) = self.decoded()?;
        let (properties, resources) = normalize_platform(&selected_platform(&action, &command))?;
        let hash = algorithm(execute.digest_function)?;
        let action_digest = execute
            .action_digest
            .as_ref()
            .ok_or("missing Action digest")?;
        Ok(RequestProjection {
            action: ActionKey {
                instance: execute.instance_name.clone(),
                digest_function: match hash {
                    DigestHasherFunc::Sha256 => "sha256",
                    DigestHasherFunc::Blake3 => "blake3",
                }
                .to_owned(),
                digest: action_digest.hash.clone(),
                digest_size_bytes: u64::try_from(action_digest.size_bytes)
                    .map_err(|error| error.to_string())?,
            },
            execution_scope: invocation_scope(metadata.as_ref(), &self.principal),
            do_not_cache: action.do_not_cache,
            skip_cache_lookup: execute.skip_cache_lookup,
            priority: execute.execution_policy.map_or(0, |policy| policy.priority),
            properties,
            resources,
            execution_timeout: timeout(&action)?,
        })
    }

    pub fn dispatch_facts(&self) -> Result<DispatchFacts, String> {
        self.project()?;
        let (execute_request, request_metadata, queued_timestamp, action, command) =
            self.decoded()?;
        Ok(DispatchFacts {
            execute_request,
            request_metadata,
            queued_timestamp,
            platform: selected_platform(&action, &command),
        })
    }

    /// Principal supplied by the trusted ingress boundary at capture time.
    /// Request metadata does not authenticate or replace this value.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    pub fn action_bytes(&self) -> &[u8] {
        &self.action
    }
    pub fn command_bytes(&self) -> &[u8] {
        &self.command
    }
}

/// Property/unit blockers only. An empty result is not complete migration or
/// deployment approval: capacity, historical policies and unknown matching
/// still require the separate converter inventory.
pub fn legacy_property_blockers(
    rules: &BTreeMap<String, PropertyType>,
    cpu_unit: CpuUnit,
) -> Vec<String> {
    let mut blockers = Vec::new();
    if cpu_unit != CpuUnit::Cores {
        blockers.push(
            "cpu_count: legacy millicore scale is incompatible with v1 whole cores".to_owned(),
        );
    }
    for (name, rule) in rules {
        let resource = matches!(name.as_str(), "cpu_count" | "memory_kb");
        let supported = matches!(rule, PropertyType::Minimum) && resource
            || matches!(rule, PropertyType::Exact)
                && !resource
                && !RESERVED.contains(&name.as_str());
        if !supported {
            blockers.push(format!(
                "{name}: unsupported legacy property rule {}",
                match rule {
                    PropertyType::Minimum => "Minimum",
                    PropertyType::Exact => "Exact",
                    PropertyType::Priority => "Priority",
                    PropertyType::Ignore => "Ignore",
                }
            ));
        }
    }
    blockers
}

impl Request {
    pub fn from_payload(payload: RequestPayload) -> Result<Self, String> {
        let facts = payload.project()?;
        Ok(Self {
            action: facts.action,
            execution_scope: facts.execution_scope,
            do_not_cache: facts.do_not_cache,
            skip_cache_lookup: facts.skip_cache_lookup,
            priority: facts.priority,
            properties: facts.properties,
            resources: facts.resources,
            execution_timeout: facts.execution_timeout,
            payload: Some(Box::new(payload)),
        })
    }

    pub fn validate_payload(&self) -> Result<(), String> {
        let Some(payload) = &self.payload else {
            return Ok(());
        };
        let expected = payload.project()?;
        if self.action != expected.action
            || self.execution_scope != expected.execution_scope
            || self.do_not_cache != expected.do_not_cache
            || self.skip_cache_lookup != expected.skip_cache_lookup
            || self.priority != expected.priority
            || self.properties != expected.properties
            || self.resources != expected.resources
            || self.execution_timeout != expected.execution_timeout
        {
            return Err("request projection differs from captured ingress facts".to_owned());
        }
        Ok(())
    }

    pub fn require_payload(&self) -> Result<&RequestPayload, String> {
        self.validate_payload()?;
        self.payload
            .as_deref()
            .ok_or_else(|| "real ingress requires captured request facts".to_owned())
    }
}
