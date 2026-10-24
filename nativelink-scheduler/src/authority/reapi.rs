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

//! Bounded candidate responses captured after protobuf decoding, before acceptance.
//!
//! Stored bytes preserve every field known to the pinned generated protobufs.
//! They are one serialization of the decoded message, not the original wire
//! representation: tonic/prost has already discarded unknown protobuf fields.
//! No result-to-internal-ActionResult conversion is involved. The fingerprint
//! detects changed stored bytes; it is not an authenticity signature.

use std::fmt;

use nativelink_proto::build::bazel::remote::execution::v2::ExecuteResponse;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use prost::Message;
use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::ResultPayload;

const RESPONSE_VERSION: u32 = 1;
/// Experimental hard limit; the authority's complete JSON payload limit also
/// applies and may be smaller. This is not a qualified production envelope.
pub const MAX_RESPONSE_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResponsePayload {
    version: u32,
    encoded: Vec<u8>,
    sha256: [u8; 32],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseWire {
    version: u32,
    #[serde(deserialize_with = "bounded_bytes")]
    encoded: Vec<u8>,
    sha256: [u8; 32],
}

fn bounded_bytes<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    struct BytesVisitor;
    impl<'de> Visitor<'de> for BytesVisitor {
        type Value = Vec<u8>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("bounded ExecuteResponse bytes")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
            let mut bytes =
                Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX_RESPONSE_BYTES));
            while bytes.len() < MAX_RESPONSE_BYTES {
                let Some(byte) = sequence.next_element()? else {
                    return Ok(bytes);
                };
                bytes.push(byte);
            }
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom(
                    "response exceeds encoded byte bound",
                ));
            }
            Ok(bytes)
        }
    }
    deserializer.deserialize_seq(BytesVisitor)
}

impl<'de> Deserialize<'de> for ResponsePayload {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = ResponseWire::deserialize(deserializer)?;
        let payload = Self {
            version: wire.version,
            encoded: wire.encoded,
            sha256: wire.sha256,
        };
        payload.decode().map_err(serde::de::Error::custom)?;
        Ok(payload)
    }
}

/// Derived facts only; the immutable protobuf bytes remain the response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResponseFacts {
    pub exit_code: i32,
    pub error: Option<String>,
}

impl ResponseFacts {
    pub const fn cache_eligible(&self, do_not_cache: bool) -> bool {
        !do_not_cache && self.exit_code == 0 && self.error.is_none()
    }
}

fn facts(response: &ExecuteResponse) -> Result<ResponseFacts, String> {
    let status = response.status.as_ref().map_or(0, |status| status.code);
    if status == 0 && response.result.is_none() {
        return Err("OK ExecuteResponse requires an ActionResult".to_owned());
    }
    Ok(ResponseFacts {
        exit_code: response
            .result
            .as_ref()
            .map_or(0, |result| result.exit_code),
        error: (status != 0).then(|| {
            format!(
                "REAPI status {status}: {}",
                response
                    .status
                    .as_ref()
                    .map_or("", |status| status.message.as_str())
            )
        }),
    })
}

fn checksum(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(bytes);
    **hasher.finalize_digest().packed_hash()
}

impl ResponsePayload {
    /// Capture a decoded candidate exactly once, before an acceptance proposal.
    /// Check size before allocating the encoded buffer. Map encoding order is
    /// unspecified, so restoration never decodes and re-encodes these bytes.
    pub fn capture(response: &ExecuteResponse, max_bytes: usize) -> Result<Self, String> {
        if response.encoded_len() > max_bytes.min(MAX_RESPONSE_BYTES) {
            return Err("response exceeds encoded byte bound".to_owned());
        }
        facts(response)?;
        let encoded = response.encode_to_vec();
        Ok(Self {
            version: RESPONSE_VERSION,
            sha256: checksum(&encoded),
            encoded,
        })
    }

    pub fn encoded(&self) -> &[u8] {
        &self.encoded
    }

    /// Identifies the captured `ExecuteResponse` bytes, not a CAS `ActionResult`.
    pub fn fingerprint(&self) -> String {
        DigestInfo::new(self.sha256, 0).packed_hash().to_string()
    }

    pub fn decode(&self) -> Result<ExecuteResponse, String> {
        if self.version != RESPONSE_VERSION || self.encoded.len() > MAX_RESPONSE_BYTES {
            return Err("unsupported response format or encoded byte bound".to_owned());
        }
        if checksum(&self.encoded) != self.sha256 {
            return Err("response checksum mismatch".to_owned());
        }
        let response =
            ExecuteResponse::decode(self.encoded.as_slice()).map_err(|error| error.to_string())?;
        facts(&response)?;
        Ok(response)
    }

    pub fn facts(&self) -> Result<ResponseFacts, String> {
        facts(&self.decode()?)
    }
}

impl ResultPayload {
    /// Capture a complete candidate and derive the reducer's compact facts.
    /// The digest identifies response bytes, not an independently stored CAS blob.
    pub fn from_response(response: &ExecuteResponse, max_bytes: usize) -> Result<Self, String> {
        let response = ResponsePayload::capture(response, max_bytes)?;
        let facts = response.facts()?;
        Ok(Self {
            digest: response.fingerprint(),
            exit_code: facts.exit_code,
            error: facts.error,
            response: Some(response),
        })
    }

    /// Validate on committed application and snapshot restoration. Synthetic
    /// fixtures may omit the full payload; real adapters must require it below.
    pub fn validate_response(&self) -> Result<(), String> {
        if let Some(response) = &self.response {
            let facts = response.facts()?;
            if self.digest != response.fingerprint()
                || self.exit_code != facts.exit_code
                || self.error != facts.error
            {
                return Err("result facts disagree with captured ExecuteResponse".to_owned());
            }
        }
        Ok(())
    }

    /// Real adapter boundary: a synthetic fixture is never a complete response.
    pub fn require_response(&self) -> Result<&ResponsePayload, String> {
        self.validate_response()?;
        self.response
            .as_ref()
            .ok_or_else(|| "complete ExecuteResponse payload required".to_owned())
    }
}
