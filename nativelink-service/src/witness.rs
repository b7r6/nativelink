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

//! Witnessing primitives for the CAS witness.
//!
//! When the CAS witness is configured with a [`WitnessKey`], every cached
//! fetch produces a signed attestation that binds "this body was fetched from
//! this upstream, at this wall-clock time, by this host, and is durably
//! persisted under these CAS keys." The attestation is a DSSE-wrapped in-toto
//! Statement stored in the CAS itself; a compact signed [`Receipt`] is
//! returned inline in response headers so a client can verify the binding
//! without a CAS round-trip.
//!
//! Trust model (v1): the witness is a *trusted attester*. A verifier trusts
//! the witness's public key and believes "the proxy observed X." This is
//! not third-party-non-repudiable proof that the origin served X — TLS 1.3
//! gives no post-hoc non-repudiation. The `notary` field in the predicate is
//! reserved for a future MPC-TLS notarization path.

use std::path::Path;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::{Signature, Signer as _, SigningKey, Verifier as _, VerifyingKey};
use nativelink_error::{Code, Error, ResultExt, make_err};
use rand::RngCore;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Witness signing key
// ---------------------------------------------------------------------------

/// The ed25519 signing key the proxy uses to sign attestations and receipts.
///
/// The key is a raw 32-byte ed25519 seed, stored as base64 in a file (one
/// line, no `name:` prefix — this is not the Nix key format). Generated
/// automatically if the file does not exist, written `0600` on Unix.
/// The `keyid` is `blake3(pubkey)`, identifying the key in DSSE signatures.
#[derive(Clone)]
pub struct WitnessKey {
    signing_key: SigningKey,
    keyid: String,
}

impl core::fmt::Debug for WitnessKey {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WitnessKey")
            .field("keyid", &self.keyid)
            .finish_non_exhaustive()
    }
}

impl WitnessKey {
    /// Loads the ed25519 signing key from `key_file`, or generates a fresh
    /// one and persists it (0600 on Unix) if the file does not exist.
    pub fn load_or_generate(key_file: &str) -> Result<Self, Error> {
        if Path::new(key_file).exists() {
            let contents = std::fs::read_to_string(key_file)
                .err_tip(|| format!("Reading witness key '{key_file}'"))?;
            let seed_b64 = contents.trim();
            let seed = BASE64
                .decode(seed_b64)
                .map_err(|e| make_err!(Code::InvalidArgument, "Invalid witness key base64: {e}"))?;
            let seed_bytes: [u8; 32] = seed.as_slice().try_into().map_err(|_| {
                make_err!(
                    Code::InvalidArgument,
                    "Witness key must be 32 bytes (ed25519 seed), got {}",
                    seed.len()
                )
            })?;
            let signing_key = SigningKey::from_bytes(&seed_bytes);
            Ok(Self::from_signing_key(signing_key))
        } else {
            let mut seed = [0u8; 32];
            rand::rng().fill_bytes(&mut seed);
            let signing_key = SigningKey::from_bytes(&seed);
            persist_witness_key(key_file, &seed)?;
            Ok(Self::from_signing_key(signing_key))
        }
    }

    fn from_signing_key(signing_key: SigningKey) -> Self {
        let pubkey = signing_key.verifying_key();
        let keyid = hex::encode(blake3::hash(pubkey.as_bytes()).as_bytes());
        Self { signing_key, keyid }
    }

    /// The key identifier: `blake3(pubkey)` as lowercase hex. Used in DSSE
    /// signature entries and in receipts so a verifier knows which key to
    /// check.
    #[must_use]
    pub fn keyid(&self) -> &str {
        &self.keyid
    }

    /// The ed25519 verifying key (public key) for this witness.
    #[must_use]
    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    /// Signs `message` with ed25519, returning the 64-byte detached signature.
    fn sign(&self, message: &[u8]) -> Signature {
        self.signing_key.sign(message)
    }
}

/// Writes the witness key seed as base64, creating the parent directory and
/// restricting the file to `0600` on Unix.
fn persist_witness_key(key_file: &str, seed: &[u8; 32]) -> Result<(), Error> {
    if let Some(parent) = Path::new(key_file).parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .err_tip(|| format!("Creating parent dir for witness key '{key_file}'"))?;
    }
    std::fs::write(key_file, BASE64.encode(seed))
        .err_tip(|| format!("Writing witness key '{key_file}'"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(key_file, std::fs::Permissions::from_mode(0o600))
            .err_tip(|| format!("Restricting witness key perms '{key_file}'"))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// in-toto Statement + DSSE envelope
// ---------------------------------------------------------------------------

/// The in-toto Statement subject: the fetched artifact, identified by its
/// upstream URL and content digest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementSubject {
    pub name: String,
    pub digest: ContentDigest,
}

/// A content digest in the attestation: `{"sha256": "<hex>"}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentDigest {
    pub sha256: String,
}

/// The custom fetch-witness predicate. Captures everything the proxy
/// observed about the fetch: the upstream connection, the authentication
/// context, the content, the CAS persistence keys, and the witness host.
///
/// `notary` is reserved for a future MPC-TLS notarization path and is
/// `null` in v1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchWitnessPredicate {
    pub resource: Resource,
    pub upstream: Upstream,
    pub authentication: Authentication,
    pub content: Content,
    pub persistence: Persistence,
    pub witness: WitnessMeta,
    pub notary: Option<Notary>,
}

/// The fetched resource: URL and HTTP method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource {
    pub uri: String,
    pub method: String,
}

/// Upstream connection details observed by the proxy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Upstream {
    pub host: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_addr: Option<String>,
    pub status: u16,
    /// SHA-256 fingerprint of the origin's TLS leaf certificate (DER),
    /// or `null` for plaintext (HTTP) origins.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_cert_sha256: Option<String>,
    /// Selected response headers from the origin (e.g. `content-type`,
    /// `etag`, `last-modified`).
    pub response_headers: Vec<(String, String)>,
}

/// How the client authenticated to the origin (secrets redacted).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Authentication {
    /// e.g. `"none"`, `"basic"`, `"bearer"`, `"tls-client-cert"`.
    pub method: String,
}

/// The fetched content: digest, size, and media type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Content {
    pub sha256: String,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

/// Where the body and attestation are durably persisted in the CAS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Persistence {
    /// The SHA-256 digest of the body in the CAS: `"<hex>-<size>"`.
    pub body_key: String,
    /// The attestation's own CAS key (`blake3(envelope)`) is self-referential
    /// and cannot be signed inside the payload that would contain it, so this
    /// is empty in the signed statement. The real key is conveyed out of band
    /// via the `X-Straylight-Witness` header and the signed receipt.
    pub attestation_key: String,
}

/// The witness host that produced the attestation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WitnessMeta {
    /// RFC 3339 wall-clock timestamp.
    pub timestamp: String,
    pub hostname: String,
    /// Monotonic clock nanoseconds at attestation time (for ordering).
    pub monotonic_ns: u128,
}

/// Reserved for future MPC-TLS notarization. Always `null` in v1.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Notary {
    // Placeholder — the shape will be defined when the notary path is built.
}

/// in-toto Statement v1.
///
/// See <https://github.com/in-toto/attestation/blob/main/spec/v1.0/statement.md>.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statement {
    #[allow(clippy::pub_underscore_fields)] // serde field name is `_type` on the wire
    pub _type: String,
    pub subject: Vec<StatementSubject>,
    pub predicate_type: String,
    pub predicate: FetchWitnessPredicate,
}

/// A DSSE envelope: the signed attestation wrapper.
///
/// See <https://github.com/secure-systems-lab/dsse/blob/master/protocol.md>.
/// The payload is the JSON-serialized [`Statement`], base64-encoded. The
/// PAE (pre-authentication encoding) is `DSSEV1 + payload_type + payload`
/// with length-prefixed fields, signed with ed25519.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DsseEnvelope {
    pub payload: String,
    pub payload_type: String,
    pub signatures: Vec<DsseSignature>,
}

/// One signature in a DSSE envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DsseSignature {
    pub sig: String,
    pub keyid: String,
}

/// The DSSE payload type for in-toto Statements.
const DSSE_PAYLOAD_TYPE: &str = "application/vnd.in-toto+json";

/// The in-toto Statement type URI.
const STATEMENT_TYPE: &str = "https://in-toto.dev/Statement/v1";

/// The fetch-witness predicate type URI.
const PREDICATE_TYPE: &str = "https://straylight.dev/fetch-witness/v0.1";

/// Builds the DSSE PAE (Pre-Authentication Encoding) for signing.
///
/// PAE = `"DSSEV1"` + `len(payload_type)` + `payload_type` + `len(payload)` + `payload`
/// where each length is a 8-byte big-endian u64 and all fields are bytes.
fn dsse_pae(payload_type: &str, payload: &[u8]) -> Vec<u8> {
    let pt = payload_type.as_bytes();
    let mut buf = Vec::with_capacity(7 + 8 + pt.len() + 8 + payload.len());
    buf.extend_from_slice(b"DSSEV1");
    buf.extend_from_slice(&(pt.len() as u64).to_be_bytes());
    buf.extend_from_slice(pt);
    buf.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Creates a DSSE envelope wrapping an in-toto Statement, signed with the
/// witness key.
#[allow(clippy::needless_pass_by_value)] // statement is consumed into the envelope by serialization
pub fn create_attestation(statement: Statement, key: &WitnessKey) -> Result<DsseEnvelope, Error> {
    let payload = serde_json::to_vec(&statement)
        .map_err(|e| make_err!(Code::Internal, "serializing in-toto statement: {e}"))?;
    let pae = dsse_pae(DSSE_PAYLOAD_TYPE, &payload);
    let sig = key.sign(&pae);
    Ok(DsseEnvelope {
        payload: BASE64.encode(&payload),
        payload_type: DSSE_PAYLOAD_TYPE.to_string(),
        signatures: vec![DsseSignature {
            sig: BASE64.encode(sig.to_bytes()),
            keyid: key.keyid().to_string(),
        }],
    })
}

/// Serializes a DSSE envelope to JSON bytes (for CAS storage).
pub fn envelope_to_bytes(envelope: &DsseEnvelope) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(envelope)
        .map_err(|e| make_err!(Code::Internal, "serializing DSSE envelope: {e}"))
}

/// Deserializes a DSSE envelope from JSON bytes (from CAS storage).
pub fn envelope_from_bytes(bytes: &[u8]) -> Result<DsseEnvelope, Error> {
    serde_json::from_slice(bytes)
        .map_err(|e| make_err!(Code::Internal, "deserializing DSSE envelope: {e}"))
}

/// Verifies a DSSE envelope's signature against a public key.
/// Returns `true` if the signature is valid.
pub fn verify_envelope(envelope: &DsseEnvelope, pubkey: &VerifyingKey) -> bool {
    // Bind the payload type: only an in-toto Statement envelope is a valid
    // attestation here. Without this check a signature computed over a
    // different `payload_type` (a type-confusion envelope) could be accepted
    // by a caller that then interprets the payload as an in-toto Statement.
    if envelope.payload_type != DSSE_PAYLOAD_TYPE {
        return false;
    }
    let Ok(payload) = BASE64.decode(&envelope.payload) else {
        return false;
    };
    let pae = dsse_pae(&envelope.payload_type, &payload);
    for sig_entry in &envelope.signatures {
        let Ok(sig_bytes) = BASE64.decode(&sig_entry.sig) else {
            continue;
        };
        let Ok(signature) = Signature::from_slice(&sig_bytes) else {
            continue;
        };
        if pubkey.verify(&pae, &signature).is_ok() {
            return true;
        }
    }
    false
}

/// Extracts the Statement from a DSSE envelope (base64-decodes the payload).
pub fn statement_from_envelope(envelope: &DsseEnvelope) -> Result<Statement, Error> {
    let payload = BASE64
        .decode(&envelope.payload)
        .map_err(|e| make_err!(Code::Internal, "decoding DSSE payload: {e}"))?;
    serde_json::from_slice(&payload)
        .map_err(|e| make_err!(Code::Internal, "deserializing statement: {e}"))
}

// ---------------------------------------------------------------------------
// Receipt — compact inline binding
// ---------------------------------------------------------------------------

/// A compact signed binding that identifies an attestation as belonging to
/// a specific HTTP transaction. Returned inline in the
/// `X-Straylight-Witness-Receipt` header so a client can verify the binding
/// without fetching the full attestation from the CAS.
///
/// The signature is ed25519 over the canonical JSON serialization (struct
/// field order, no whitespace), base64-encoded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Receipt {
    /// BLAKE3 hash of the DSSE envelope in the CAS, lowercase hex.
    pub attestation: String,
    /// SHA-256 hash of the response body, lowercase hex.
    pub subject: String,
    /// The upstream URL that was fetched.
    pub url: String,
    /// RFC 3339 wall-clock timestamp.
    pub ts: String,
    /// The witness key id (blake3 of the public key, hex).
    pub keyid: String,
}

impl Receipt {
    /// Creates a receipt and signs it with the witness key, returning the
    /// base64-encoded signed receipt for use in a response header.
    pub fn create_and_sign(
        attestation_blake3_hex: &str,
        body_sha256_hex: &str,
        url: &str,
        ts: &str,
        key: &WitnessKey,
    ) -> Result<String, Error> {
        let receipt = Self {
            attestation: attestation_blake3_hex.to_string(),
            subject: body_sha256_hex.to_string(),
            url: url.to_string(),
            ts: ts.to_string(),
            keyid: key.keyid().to_string(),
        };
        let payload = serde_json::to_vec(&receipt)
            .map_err(|e| make_err!(Code::Internal, "serializing witness receipt: {e}"))?;
        let sig = key.sign(&payload);
        Ok(format!(
            "{}.{}",
            BASE64.encode(&payload),
            BASE64.encode(sig.to_bytes())
        ))
    }

    /// Parses and verifies a base64-encoded signed receipt.
    /// Returns the receipt if the signature is valid.
    pub fn parse_and_verify(encoded: &str, pubkey: &VerifyingKey) -> Option<Self> {
        let (payload_b64, sig_b64) = encoded.split_once('.')?;
        let payload = BASE64.decode(payload_b64).ok()?;
        let sig_bytes = BASE64.decode(sig_b64).ok()?;
        let signature = Signature::from_slice(&sig_bytes).ok()?;
        pubkey.verify(&payload, &signature).ok()?;
        serde_json::from_slice(&payload).ok()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// SHA-256 of a byte slice, returned as lowercase hex.
pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// BLAKE3 of a byte slice, returned as lowercase hex.
pub fn blake3_hex(data: &[u8]) -> String {
    hex::encode(blake3::hash(data).as_bytes())
}

/// The current RFC 3339 UTC timestamp.
pub fn now_rfc3339() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let days = secs / 86_400;
    let remainder = secs % 86_400;
    let hour = remainder / 3600;
    let min = (remainder % 3600) / 60;
    let sec = remainder % 60;
    let (year, month, day) = days_to_ymd(days.cast_signed());
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

/// The monotonic clock in nanoseconds (for ordering attestations).
pub fn monotonic_ns() -> u128 {
    use std::time::Instant;
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_nanos()
}

/// Converts days since 1970-01-01 to (year, month, day).
/// Algorithm from <https://howardhinnant.github.io/date_algorithms.html>.
#[allow(clippy::cast_possible_truncation)]
const fn days_to_ymd(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m as u32, d as u32)
}

/// The local hostname for the witness metadata, or `"unknown"` if it cannot
/// be determined.
pub fn hostname() -> String {
    // Prefer `$HOSTNAME`, then the kernel-exposed hostname on Linux.
    // Deliberately does NOT shell out to the `hostname` binary: that would
    // inherit `$PATH` and let a planted executable inject an attacker-chosen
    // (but attestation-trusted) hostname into every signed attestation.
    if let Ok(name) = std::env::var("HOSTNAME") {
        let name = name.trim();
        if !name.is_empty() {
            return name.to_string();
        }
    }
    if let Ok(contents) = std::fs::read_to_string("/proc/sys/kernel/hostname") {
        let name = contents.trim();
        if !name.is_empty() {
            return name.to_string();
        }
    }
    "unknown".to_string()
}

// ---------------------------------------------------------------------------
// Statement builder
// ---------------------------------------------------------------------------

/// Builds a complete in-toto Statement for a fetch witness.
#[allow(clippy::too_many_arguments)]
pub fn build_statement(
    url: &str,
    method: &str,
    body_sha256_hex: &str,
    body_size: u64,
    content_type: Option<&str>,
    host: &str,
    peer_addr: Option<&str>,
    status: u16,
    server_cert_sha256: Option<&str>,
    response_headers: Vec<(String, String)>,
    auth_method: &str,
    body_cas_key: &str,
    attestation_cas_key: &str,
    timestamp: &str,
    monotonic_ns: u128,
    witness_hostname: &str,
) -> Statement {
    Statement {
        _type: STATEMENT_TYPE.to_string(),
        subject: vec![StatementSubject {
            name: url.to_string(),
            digest: ContentDigest {
                sha256: body_sha256_hex.to_string(),
            },
        }],
        predicate_type: PREDICATE_TYPE.to_string(),
        predicate: FetchWitnessPredicate {
            resource: Resource {
                uri: url.to_string(),
                method: method.to_string(),
            },
            upstream: Upstream {
                host: host.to_string(),
                peer_addr: peer_addr.map(ToString::to_string),
                status,
                server_cert_sha256: server_cert_sha256.map(ToString::to_string),
                response_headers,
            },
            authentication: Authentication {
                method: auth_method.to_string(),
            },
            content: Content {
                sha256: body_sha256_hex.to_string(),
                size: body_size,
                media_type: content_type.map(ToString::to_string),
            },
            persistence: Persistence {
                body_key: body_cas_key.to_string(),
                attestation_key: attestation_cas_key.to_string(),
            },
            witness: WitnessMeta {
                timestamp: timestamp.to_string(),
                hostname: witness_hostname.to_string(),
                monotonic_ns,
            },
            notary: None,
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_key_path(tag: &str) -> String {
        let dir =
            std::env::temp_dir().join(format!("nl-witness-test-{}-{tag}", std::process::id()));
        drop(std::fs::remove_dir_all(&dir));
        dir.join("witness.key").display().to_string()
    }

    #[test]
    fn generates_persists_and_reloads() {
        let path = tmp_key_path("reload");
        let key = WitnessKey::load_or_generate(&path).expect("generate");
        assert!(Path::new(&path).exists());

        // Reload must produce the same keyid.
        let key2 = WitnessKey::load_or_generate(&path).expect("reload");
        assert_eq!(key.keyid(), key2.keyid());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "witness key must be 0600");
        }
    }

    #[test]
    fn debug_redacts_key_material() {
        let path = tmp_key_path("debug");
        let key = WitnessKey::load_or_generate(&path).expect("generate");
        let rendered = format!("{key:?}");
        assert!(rendered.contains("keyid"));
        // Must not contain the base64 seed.
        let file_contents = std::fs::read_to_string(&path).unwrap();
        let seed_b64 = file_contents.trim();
        assert!(!rendered.contains(seed_b64));
    }

    #[test]
    fn dsse_envelope_round_trips_and_verifies() {
        let path = tmp_key_path("dsse");
        let key = WitnessKey::load_or_generate(&path).expect("generate");

        let statement = build_statement(
            "https://example.com/foo",
            "GET",
            "abc123",
            42,
            Some("text/plain"),
            "example.com",
            Some("1.2.3.4:443"),
            200,
            Some("deadbeef"),
            vec![("content-type".into(), "text/plain".into())],
            "none",
            "abc123-42",
            "blake3hex-100",
            "2026-07-11T12:00:00Z",
            12345,
            "test-host",
        );

        let envelope = create_attestation(statement, &key).expect("create attestation");
        assert_eq!(envelope.payload_type, "application/vnd.in-toto+json");
        assert_eq!(envelope.signatures.len(), 1);
        assert_eq!(envelope.signatures[0].keyid, key.keyid());

        // Verify with the correct public key.
        assert!(verify_envelope(&envelope, &key.verifying_key()));

        // Verify with a wrong public key fails.
        let other_path = tmp_key_path("other");
        let other_key = WitnessKey::load_or_generate(&other_path).expect("other key");
        assert!(!verify_envelope(&envelope, &other_key.verifying_key()));

        // Round-trip through bytes.
        let bytes = envelope_to_bytes(&envelope).expect("envelope to bytes");
        let restored = envelope_from_bytes(&bytes).expect("from bytes");
        assert!(verify_envelope(&restored, &key.verifying_key()));

        // Extract the statement.
        let stmt = statement_from_envelope(&envelope).expect("statement");
        assert_eq!(stmt.predicate.resource.uri, "https://example.com/foo");
        assert_eq!(stmt.predicate.upstream.status, 200);
        assert_eq!(stmt.predicate.content.size, 42);
        assert!(stmt.predicate.notary.is_none());
    }

    #[test]
    fn receipt_round_trips_and_verifies() {
        let path = tmp_key_path("receipt");
        let key = WitnessKey::load_or_generate(&path).expect("generate");

        let encoded = Receipt::create_and_sign(
            "blake3hex",
            "sha256hex",
            "https://example.com/foo",
            "2026-07-11T12:00:00Z",
            &key,
        )
        .expect("sign receipt");

        // Verify with the correct key.
        let receipt =
            Receipt::parse_and_verify(&encoded, &key.verifying_key()).expect("valid receipt");
        assert_eq!(receipt.attestation, "blake3hex");
        assert_eq!(receipt.subject, "sha256hex");
        assert_eq!(receipt.url, "https://example.com/foo");
        assert_eq!(receipt.keyid, key.keyid());

        // Verify with a wrong key fails.
        let other_path = tmp_key_path("receipt-other");
        let other_key = WitnessKey::load_or_generate(&other_path).expect("other key");
        assert!(Receipt::parse_and_verify(&encoded, &other_key.verifying_key()).is_none());

        // Tampered receipt fails.
        let (prefix, _) = encoded.rsplit_once('.').unwrap();
        let tampered = format!("{prefix}.AAAA");
        assert!(Receipt::parse_and_verify(&tampered, &key.verifying_key()).is_none());
    }

    #[test]
    fn sha256_and_blake3_are_deterministic() {
        assert_eq!(
            sha256_hex(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        let h = blake3_hex(b"hello");
        assert_eq!(h.len(), 64, "blake3 hex must be 64 chars");
        assert_eq!(h, blake3_hex(b"hello"), "blake3 must be deterministic");
        assert_ne!(h, blake3_hex(b"world"), "different inputs differ");
    }

    #[test]
    fn days_to_ymd_epoch() {
        // 1970-01-01 is day 0.
        assert_eq!(days_to_ymd(0), (1970, 1, 1));
        // 2026-07-11 is day 20,645 from epoch.
        let (y, m, d) = days_to_ymd(20_645);
        assert_eq!(y, 2026);
        assert_eq!(m, 7);
        assert_eq!(d, 11);
    }

    /// Builds a valid signed envelope over a trivial statement for tamper
    /// tests, returning the envelope and the key that signed it.
    fn signed_envelope(tag: &str) -> (DsseEnvelope, WitnessKey) {
        let key = WitnessKey::load_or_generate(&tmp_key_path(tag)).expect("generate");
        let statement = build_statement(
            "https://example.com/artifact",
            "GET",
            "cafebabe",
            7,
            Some("application/octet-stream"),
            "example.com",
            None,
            200,
            None,
            vec![],
            "none",
            "cafebabe-7",
            "",
            "2026-07-11T12:00:00Z",
            99,
            "test-host",
        );
        let envelope = create_attestation(statement, &key).expect("create attestation");
        (envelope, key)
    }

    /// The payload-type binding: an envelope whose `payload_type` is anything
    /// other than the in-toto Statement type must FAIL verification — even a
    /// SELF-CONSISTENT envelope whose signature is a genuine ed25519 signature
    /// over the PAE of that wrong type. This is the type-confusion attack the
    /// binding closes: without the explicit `payload_type` check, an envelope
    /// legitimately signed over a different payload type would verify and its
    /// payload would then be misinterpreted as an in-toto Statement.
    #[test]
    fn verify_envelope_rejects_wrong_payload_type() {
        let key = WitnessKey::load_or_generate(&tmp_key_path("payload-type")).expect("key");
        let payload = b"{\"anything\":true}";

        for wrong_type in [
            "",
            "application/vnd.in-toto+json ", // trailing space: not equal
            "application/json",
            "application/vnd.in-toto+jsonx",
            "text/plain",
            "APPLICATION/VND.IN-TOTO+JSON", // case matters
        ] {
            // A fully self-consistent envelope: the signature genuinely covers
            // the PAE of `wrong_type`, so signature verification alone would
            // PASS. Only the payload-type binding rejects it.
            let pae = dsse_pae(wrong_type, payload);
            let sig = key.sign(&pae);
            let forged = DsseEnvelope {
                payload: BASE64.encode(payload),
                payload_type: wrong_type.to_string(),
                signatures: vec![DsseSignature {
                    sig: BASE64.encode(sig.to_bytes()),
                    keyid: key.keyid().to_string(),
                }],
            };
            assert!(
                !verify_envelope(&forged, &key.verifying_key()),
                "a self-consistent envelope with payload_type '{wrong_type}' must be rejected"
            );
        }

        // The correct payload type with a matching signature still verifies —
        // the binding does not break the happy path.
        let good_pae = dsse_pae(DSSE_PAYLOAD_TYPE, payload);
        let good_sig = key.sign(&good_pae);
        let good = DsseEnvelope {
            payload: BASE64.encode(payload),
            payload_type: DSSE_PAYLOAD_TYPE.to_string(),
            signatures: vec![DsseSignature {
                sig: BASE64.encode(good_sig.to_bytes()),
                keyid: key.keyid().to_string(),
            }],
        };
        assert!(
            verify_envelope(&good, &key.verifying_key()),
            "the in-toto payload type with a valid signature must verify"
        );
    }

    /// A single-byte tamper of the base64 payload (decoded content) breaks
    /// verification: the PAE covers the payload, so any change invalidates the
    /// signature.
    #[test]
    fn verify_envelope_rejects_payload_tamper() {
        let (mut envelope, key) = signed_envelope("payload-tamper");
        let mut payload = BASE64.decode(&envelope.payload).expect("decode payload");
        assert!(!payload.is_empty());
        payload[0] ^= 0x01;
        envelope.payload = BASE64.encode(&payload);
        assert!(
            !verify_envelope(&envelope, &key.verifying_key()),
            "a mutated payload must fail verification"
        );
    }

    mod proptests {
        use proptest::prelude::*;
        use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence};

        use super::*;

        fn no_persist() -> ProptestConfig {
            ProptestConfig {
                failure_persistence: Some(Box::new(FileFailurePersistence::Off)),
                ..ProptestConfig::default()
            }
        }

        proptest! {
            #![proptest_config(no_persist())]

            /// `dsse_pae` framing is exact and deterministic: the output is
            /// literally `"DSSEV1" + be64(len(pt)) + pt + be64(len(payload)) +
            /// payload`, and recomputing it yields the identical bytes.
            #[test]
            fn dsse_pae_framing_is_exact(pt in ".*", payload in proptest::collection::vec(any::<u8>(), 0..512)) {
                let pae = dsse_pae(&pt, &payload);
                let mut expected = Vec::new();
                expected.extend_from_slice(b"DSSEV1");
                expected.extend_from_slice(&(pt.len() as u64).to_be_bytes());
                expected.extend_from_slice(pt.as_bytes());
                expected.extend_from_slice(&(payload.len() as u64).to_be_bytes());
                expected.extend_from_slice(&payload);
                prop_assert_eq!(&pae, &expected);
                // Deterministic: same inputs, same bytes.
                prop_assert_eq!(dsse_pae(&pt, &payload), pae);
            }

            /// The length prefixes are unambiguous: `(pt, payload)` and the
            /// "shifted" split that moves the payload's first byte onto the end
            /// of the payload type — keeping the raw `pt_bytes ++ payload`
            /// concatenation byte-for-byte identical — must NOT collide,
            /// because the payload-type length prefix pins the field boundary.
            /// The moved byte is constrained to ASCII so the shifted payload
            /// type stays valid UTF-8 (the framing is what is under test).
            #[test]
            fn dsse_pae_length_prefixes_disambiguate(
                pt in "[a-z]{1,16}",
                payload in proptest::collection::vec(0x20u8..0x7f, 1..64),
            ) {
                let a = dsse_pae(&pt, &payload);
                let mut shifted_pt = pt.into_bytes();
                shifted_pt.push(payload[0]);
                let shifted_pt = String::from_utf8(shifted_pt).expect("ascii by construction");
                let b = dsse_pae(&shifted_pt, &payload[1..]);
                prop_assert_ne!(a, b, "length framing must prevent field-boundary ambiguity");
            }

            /// `create_attestation` -> `verify_envelope` round-trips to `true`,
            /// and any single-byte tamper of the payload or the signature makes
            /// it `false`. Property-checked over arbitrary URL/digest/size.
            #[test]
            fn create_verify_round_trips_and_tamper_fails(
                url in "[ -~]{0,64}",
                digest_hex in "[a-f0-9]{0,64}",
                size in any::<u64>(),
                tamper_at in 0usize..64,
            ) {
                let key = WitnessKey::load_or_generate(&tmp_key_path("prop-roundtrip"))
                    .expect("key");
                let statement = build_statement(
                    &url, "GET", &digest_hex, size, None, "h", None, 200, None, vec![],
                    "none", "k", "", "2026-07-11T12:00:00Z", 1, "host",
                );
                let envelope = create_attestation(statement, &key).expect("create attestation");
                prop_assert!(verify_envelope(&envelope, &key.verifying_key()));

                // Tamper one byte of the decoded payload.
                let mut payload = BASE64.decode(&envelope.payload).expect("payload");
                prop_assert!(!payload.is_empty());
                let idx = tamper_at % payload.len();
                let mut tampered_payload = envelope.clone();
                payload[idx] ^= 0x01;
                tampered_payload.payload = BASE64.encode(&payload);
                prop_assert!(!verify_envelope(&tampered_payload, &key.verifying_key()));

                // Tamper one byte of the signature (mutating the original
                // envelope in place — its last use).
                let mut sig_bytes = BASE64.decode(&envelope.signatures[0].sig).expect("sig");
                prop_assert!(!sig_bytes.is_empty());
                let sidx = tamper_at % sig_bytes.len();
                sig_bytes[sidx] ^= 0x01;
                let mut tampered_sig = envelope;
                tampered_sig.signatures[0].sig = BASE64.encode(&sig_bytes);
                prop_assert!(!verify_envelope(&tampered_sig, &key.verifying_key()));
            }
        }
    }
}
