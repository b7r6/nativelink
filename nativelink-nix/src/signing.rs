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

//! Nix-compatible ed25519 signing: `name:base64` key and signature formats.
//!
//! Nix binary caches sign narinfo fingerprints with ed25519 keys serialized
//! as `<name>:<base64>` strings (see `src/libutil/signature/local-keys.cc`
//! in the Nix source tree):
//!
//! - Secret key: `<name>:<base64 of 64 bytes>` where the 64 bytes are the
//!   libsodium keypair layout `seed(32) || public_key(32)` — exactly the
//!   layout [`ed25519_dalek::SigningKey::from_keypair_bytes`] expects.
//! - Public key: `<name>:<base64 of 32 bytes>`.
//! - Signature: `<name>:<base64 of 64-byte detached ed25519 signature>`.
//!
//! The key name is non-empty and cannot contain `:`; key and signature
//! strings are split at the *first* `:` only. Base64 is standard-alphabet
//! *with* padding (what Nix and libsodium emit).

use core::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::{
    KEYPAIR_LENGTH, PUBLIC_KEY_LENGTH, Signature, Signer as _, SigningKey, Verifier as _,
    VerifyingKey,
};
use nativelink_error::{Error, make_input_err};

/// Splits a `<name>:<base64 payload>` string at the first `:`, validates the
/// name, and decodes the payload.
///
/// Error messages never echo the payload: for secret keys the raw input is
/// key material and must not surface in logs. The base64 decode error is
/// safe to include — it names at most the single *invalid* symbol, which by
/// definition is not part of well-formed key material.
fn split_key_string(s: &str, what: &str) -> Result<(String, Vec<u8>), Error> {
    let (name, payload_b64) = s.split_once(':').ok_or_else(|| {
        make_input_err!("Nix {what} is missing the ':' separator between name and base64 payload")
    })?;
    if name.is_empty() {
        return Err(make_input_err!("Nix {what} has an empty key name"));
    }
    let payload = BASE64
        .decode(payload_b64)
        .map_err(|e| make_input_err!("Nix {what} has an invalid base64 payload: {e}"))?;
    Ok((name.to_owned(), payload))
}

/// An ed25519 signing key in Nix's `<name>:<base64>` format, as produced by
/// `nix key generate-secret --key-name <name>`.
#[derive(Clone)]
pub struct NixSigningKey {
    name: String,
    key: SigningKey,
}

impl NixSigningKey {
    /// Parses a Nix secret key string: `<name>:<base64 of 64 bytes>` where
    /// the payload is the libsodium keypair layout `seed(32) || public(32)`.
    ///
    /// The embedded public key half is validated against the seed (via
    /// [`SigningKey::from_keypair_bytes`]); a mismatch is an input error.
    pub fn from_secret_string(s: &str) -> Result<Self, Error> {
        let (name, payload) = split_key_string(s, "secret key")?;
        let payload_len = payload.len();
        let keypair_bytes: [u8; KEYPAIR_LENGTH] = payload.try_into().map_err(|_| {
            make_input_err!(
                "Nix secret key payload must be {KEYPAIR_LENGTH} bytes (seed || public key), got {payload_len}"
            )
        })?;
        let key = SigningKey::from_keypair_bytes(&keypair_bytes)
            .map_err(|e| make_input_err!("Nix secret key is not a valid ed25519 keypair: {e}"))?;
        Ok(Self { name, key })
    }

    /// The key name (the part before the first `:`).
    #[must_use]
    pub const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// The corresponding public key as `<name>:<base64 of 32 bytes>` — the
    /// same string `nix key convert-secret-to-public` prints.
    #[must_use]
    pub fn public_key_string(&self) -> String {
        format!(
            "{}:{}",
            self.name,
            BASE64.encode(self.key.verifying_key().to_bytes())
        )
    }

    /// Signs a narinfo fingerprint, returning `<name>:<base64 signature>` —
    /// the value of a narinfo `Sig:` line. Ed25519 is deterministic, so
    /// signing the same fingerprint with the same key always reproduces the
    /// same string.
    #[must_use]
    pub fn sign(&self, fingerprint: &str) -> String {
        let signature = self.key.sign(fingerprint.as_bytes());
        format!("{}:{}", self.name, BASE64.encode(signature.to_bytes()))
    }
}

/// Manual [`Debug`] that redacts the secret key material.
impl fmt::Debug for NixSigningKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NixSigningKey")
            .field("name", &self.name)
            .field("key", &"<redacted>")
            .finish()
    }
}

/// An ed25519 verifying key in Nix's `<name>:<base64>` format, as found in
/// `trusted-public-keys` (for example the well-known
/// `cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=`).
#[derive(Clone, Debug)]
pub struct NixPublicKey {
    name: String,
    key: VerifyingKey,
}

impl NixPublicKey {
    /// Parses a Nix public key string: `<name>:<base64 of 32 bytes>`.
    pub fn from_string(s: &str) -> Result<Self, Error> {
        let (name, payload) = split_key_string(s, "public key")?;
        let payload_len = payload.len();
        let key_bytes: [u8; PUBLIC_KEY_LENGTH] = payload.try_into().map_err(|_| {
            make_input_err!(
                "Nix public key payload must be {PUBLIC_KEY_LENGTH} bytes, got {payload_len}"
            )
        })?;
        let key = VerifyingKey::from_bytes(&key_bytes).map_err(|e| {
            make_input_err!("Nix public key is not a valid ed25519 curve point: {e}")
        })?;
        Ok(Self { name, key })
    }

    /// The key name (the part before the first `:`).
    #[must_use]
    pub const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Verifies a `<name>:<base64 signature>` string against a fingerprint.
    ///
    /// Returns `false` — never an error — on key-name mismatch, malformed
    /// signature (missing `:`, bad base64, wrong length), or a signature
    /// that does not verify.
    #[must_use]
    pub fn verify(&self, fingerprint: &str, sig: &str) -> bool {
        let Some((sig_name, sig_b64)) = sig.split_once(':') else {
            return false;
        };
        if sig_name != self.name {
            return false;
        }
        let Ok(sig_bytes) = BASE64.decode(sig_b64) else {
            return false;
        };
        let Ok(signature) = Signature::from_slice(&sig_bytes) else {
            return false;
        };
        self.key.verify(fingerprint.as_bytes(), &signature).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Golden key pair, generated 2026-07-02 with the local nix CLI
    // (nix (Nix) 2.34.7):
    //
    //   $ nix key generate-secret --key-name nl-test-1
    const GOLDEN_SECRET: &str = "nl-test-1:JopCSwJGgB13rkPagDgcbo9/UOQluENPBbkpBHWwt/ErsdQpil4SypWcBm2XcowcywRpwwqBsv+BdkaBMKxm3g==";
    //   $ echo "$GOLDEN_SECRET" | nix key convert-secret-to-public
    const GOLDEN_PUBLIC: &str = "nl-test-1:K7HUKYpeEsqVnAZtl3KMHMsEacMKgbL/gXZGgTCsZt4=";

    // End-to-end fixture: a narinfo written and signed by nix itself
    // (nix (Nix) 2.34.7), generated 2026-07-02 with:
    //
    //   $ SK=/tmp/nl-sk
    //   $ nix key generate-secret --key-name nl-test-1 > $SK   # == GOLDEN_SECRET
    //   $ nix copy --to "file:///tmp/nl-sig-test?secret-key=$SK" \
    //       /nix/store/lw117lsr8d585xs63kx5k233impyrq7q-bash-5.3p3
    //   $ cat /tmp/nl-sig-test/lw117lsr8d585xs63kx5k233impyrq7q.narinfo
    //   StorePath: /nix/store/lw117lsr8d585xs63kx5k233impyrq7q-bash-5.3p3
    //   ...
    //   NarHash: sha256:1b89r1vlfiv6immkhq8aqxhy1jrzh2araqsn6rvwhrjgpy3pd52h
    //   NarSize: 1856888
    //   References: j193mfi0f921y0kfs8vjc1znnr45ispv-glibc-2.40-66 lw117lsr8d585xs63kx5k233impyrq7q-bash-5.3p3
    //   ...
    //   Sig: cache.nixos.org-1:jY7ctGkzGM+K70V4BKcxhSwmSq3xr8zXETZ7z7wupsHjxdjbFxGZ4idhDD6Q6hxDol48Hxk7za5O4A0Salt8CA==
    //   Sig: nl-test-1:fVj4XST/j2Yz1xgPY0pjDjmGMUbMjVRC84PzQJKNfOWth+poDayvMYGa1LX+JlY+TcT0Gdk6U4RCk50sEduTBA==
    //
    // The fingerprint below is constructed BY HAND from those narinfo fields
    // per Nix's fingerprint format:
    //   "1;<store path>;<nar hash sha256:nix32>;<nar size>;<comma-joined full reference paths>"
    // (references are the narinfo basenames prefixed with /nix/store/, in
    // narinfo order).
    const NIX_FINGERPRINT: &str = "1;/nix/store/lw117lsr8d585xs63kx5k233impyrq7q-bash-5.3p3;sha256:1b89r1vlfiv6immkhq8aqxhy1jrzh2araqsn6rvwhrjgpy3pd52h;1856888;/nix/store/j193mfi0f921y0kfs8vjc1znnr45ispv-glibc-2.40-66,/nix/store/lw117lsr8d585xs63kx5k233impyrq7q-bash-5.3p3";
    // The Sig line nix wrote with our key.
    const NIX_SIG: &str = "nl-test-1:fVj4XST/j2Yz1xgPY0pjDjmGMUbMjVRC84PzQJKNfOWth+poDayvMYGa1LX+JlY+TcT0Gdk6U4RCk50sEduTBA==";
    // Bonus vector: `nix copy` preserved the upstream cache.nixos.org
    // signature over the same intrinsic fields, verifiable with the
    // well-known cache.nixos.org public key.
    const CACHE_NIXOS_ORG_SIG: &str = "cache.nixos.org-1:jY7ctGkzGM+K70V4BKcxhSwmSq3xr8zXETZ7z7wupsHjxdjbFxGZ4idhDD6Q6hxDol48Hxk7za5O4A0Salt8CA==";
    const CACHE_NIXOS_ORG_KEY: &str =
        "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=";

    #[test]
    fn test_public_key_string_matches_nix_convert_secret_to_public() {
        let key = NixSigningKey::from_secret_string(GOLDEN_SECRET).unwrap();
        assert_eq!(key.name(), "nl-test-1");
        assert_eq!(key.public_key_string(), GOLDEN_PUBLIC);
    }

    #[test]
    fn test_nix_produced_signature_verifies() {
        let key = NixPublicKey::from_string(GOLDEN_PUBLIC).unwrap();
        assert_eq!(key.name(), "nl-test-1");
        assert!(key.verify(NIX_FINGERPRINT, NIX_SIG));
    }

    #[test]
    fn test_mutated_fingerprint_fails_verification() {
        let key = NixPublicKey::from_string(GOLDEN_PUBLIC).unwrap();
        let mutated = NIX_FINGERPRINT.replace(";1856888;", ";1856889;");
        assert_ne!(mutated, NIX_FINGERPRINT);
        assert!(!key.verify(&mutated, NIX_SIG));
    }

    #[test]
    fn test_our_signature_reproduces_nix_exactly() {
        // Ed25519 is deterministic: signing the same fingerprint with the
        // same key must reproduce nix's Sig line byte-for-byte. This is the
        // strongest compatibility proof for both the keypair layout and the
        // fingerprint fixture.
        let key = NixSigningKey::from_secret_string(GOLDEN_SECRET).unwrap();
        assert_eq!(key.sign(NIX_FINGERPRINT), NIX_SIG);
    }

    #[test]
    fn test_cache_nixos_org_signature_verifies() {
        let key = NixPublicKey::from_string(CACHE_NIXOS_ORG_KEY).unwrap();
        assert!(key.verify(NIX_FINGERPRINT, CACHE_NIXOS_ORG_SIG));
    }

    #[test]
    fn test_sign_round_trips_through_verify() {
        let key = NixSigningKey::from_secret_string(GOLDEN_SECRET).unwrap();
        let public = NixPublicKey::from_string(&key.public_key_string()).unwrap();
        let fingerprint = "1;/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-x;sha256:0000000000000000000000000000000000000000000000000000;1;";
        let sig = key.sign(fingerprint);
        assert!(public.verify(fingerprint, &sig));
        assert!(!public.verify("something else", &sig));
    }

    #[test]
    fn test_verify_rejects_malformed_signatures() {
        let key = NixPublicKey::from_string(GOLDEN_PUBLIC).unwrap();
        // Missing ':' separator.
        assert!(!key.verify(NIX_FINGERPRINT, "no-colon-here"));
        // Key name mismatch (valid signature bytes, wrong name).
        let (_, sig_b64) = NIX_SIG.split_once(':').unwrap();
        assert!(!key.verify(NIX_FINGERPRINT, &format!("other-key:{sig_b64}")));
        // Bad base64.
        assert!(!key.verify(NIX_FINGERPRINT, "nl-test-1:!!!not-base64!!!"));
        // Unpadded base64 (nix emits padded standard base64).
        assert!(!key.verify(
            NIX_FINGERPRINT,
            &format!("nl-test-1:{}", sig_b64.trim_end_matches('=')),
        ));
        // Wrong signature length (32 bytes instead of 64).
        assert!(!key.verify(
            NIX_FINGERPRINT,
            &format!("nl-test-1:{}", BASE64.encode([0u8; 32])),
        ));
        // Empty payload.
        assert!(!key.verify(NIX_FINGERPRINT, "nl-test-1:"));
    }

    #[test]
    fn test_secret_key_malformed_inputs() {
        // Missing ':' separator.
        assert!(NixSigningKey::from_secret_string("nocolon").is_err());
        // Empty name.
        let (_, payload_b64) = GOLDEN_SECRET.split_once(':').unwrap();
        assert!(NixSigningKey::from_secret_string(&format!(":{payload_b64}")).is_err());
        // Colon in name: the string splits at the FIRST ':', so the
        // remainder "test-1:<base64>" is not valid base64 and must fail.
        assert!(NixSigningKey::from_secret_string(&format!("nl:test-1:{payload_b64}")).is_err());
        // Wrong-length key material (32 bytes instead of 64).
        assert!(
            NixSigningKey::from_secret_string(&format!("k:{}", BASE64.encode([7u8; 32]))).is_err()
        );
        // Unpadded base64.
        assert!(
            NixSigningKey::from_secret_string(&format!(
                "nl-test-1:{}",
                payload_b64.trim_end_matches('=')
            ))
            .is_err()
        );
        // Corrupted embedded public key half: from_keypair_bytes validates
        // that the public half matches the seed.
        let mut keypair = BASE64.decode(payload_b64).unwrap();
        keypair[32] ^= 0xff;
        assert!(
            NixSigningKey::from_secret_string(&format!("nl-test-1:{}", BASE64.encode(&keypair)))
                .is_err()
        );
    }

    #[test]
    fn test_public_key_malformed_inputs() {
        // Missing ':' separator.
        assert!(NixPublicKey::from_string("nocolon").is_err());
        // Empty name.
        let (_, payload_b64) = GOLDEN_PUBLIC.split_once(':').unwrap();
        assert!(NixPublicKey::from_string(&format!(":{payload_b64}")).is_err());
        // Colon in name (splits at first ':'; remainder is invalid base64).
        assert!(NixPublicKey::from_string(&format!("nl:test-1:{payload_b64}")).is_err());
        // Wrong-length key material (64 bytes instead of 32).
        assert!(NixPublicKey::from_string(&format!("k:{}", BASE64.encode([7u8; 64]))).is_err());
        // Unpadded base64.
        assert!(
            NixPublicKey::from_string(&format!("nl-test-1:{}", payload_b64.trim_end_matches('=')))
                .is_err()
        );
    }

    #[test]
    fn test_debug_redacts_secret_material() {
        let key = NixSigningKey::from_secret_string(GOLDEN_SECRET).unwrap();
        let rendered = format!("{key:?}");
        assert!(rendered.contains("nl-test-1"));
        assert!(rendered.contains("<redacted>"));
        // Must not leak any part of the base64 key material.
        assert!(!rendered.contains("JopCSw"));
    }
}
