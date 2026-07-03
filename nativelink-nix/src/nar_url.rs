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

//! Pure helpers for NAR URL names: compression-codec selection by file
//! extension, the canonical `{nix32(hash)}-{size}.nar` object name used
//! for CAS-backed NARs, and the alias-key scheme that maps arbitrary
//! upstream NAR URL basenames onto canonical names.

use nativelink_error::{Error, make_input_err};

use crate::nixbase32;

/// Compression codec of a NAR, as determined by its URL name (or, for
/// [`NarCodec::Gzip`], by content sniffing — see [`codec_for_name`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NarCodec {
    /// Uncompressed NAR bytes.
    None,
    /// gzip (only ever detected by sniffing [`GZIP_MAGIC`], never by name).
    Gzip,
    /// xz / LZMA2.
    Xz,
    /// Zstandard.
    Zstd,
    /// bzip2.
    Bzip2,
}

/// The first two bytes of any gzip stream (RFC 1952). Some caches serve
/// gzip-compressed bytes under a bare `.nar` name; callers that get
/// [`NarCodec::None`] from [`codec_for_name`] may sniff for this magic.
pub const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// Maps a NAR file name to its compression codec by extension.
///
/// - `.nar` maps to `Some(NarCodec::None)` — but the bytes may still be
///   gzip on the wire; the caller may sniff [`GZIP_MAGIC`].
/// - `.nar.xz`, `.nar.zst`, and `.nar.bz2` map to their codecs.
/// - Anything else (including `.nar.gz`, which real caches do not emit)
///   is unsupported and maps to `Option::None`.
#[must_use]
pub fn codec_for_name(name: &str) -> Option<NarCodec> {
    // Deliberately case-sensitive: NAR URL names are lowercase on the
    // wire, and `.NAR` is not a name any real cache emits.
    const SUFFIX_CODECS: [(&str, NarCodec); 4] = [
        (".nar.xz", NarCodec::Xz),
        (".nar.zst", NarCodec::Zstd),
        (".nar.bz2", NarCodec::Bzip2),
        (".nar", NarCodec::None),
    ];
    SUFFIX_CODECS
        .iter()
        .find(|(suffix, _)| name.ends_with(suffix))
        .map(|&(_, codec)| codec)
}

/// Formats the canonical name for an uncompressed NAR:
/// `{nix32(nar_sha256)}-{nar_size}.nar` (52 nix32 characters, `-`, the
/// decimal size, `.nar`).
#[must_use]
pub fn canonical_nar_name(nar_sha256: &[u8; 32], nar_size: u64) -> String {
    format!("{}-{nar_size}.nar", nixbase32::encode(nar_sha256))
}

/// Parses a name produced by [`canonical_nar_name`].
///
/// Returns `None` unless `name` is exactly 52 nix32 characters, `-`, a
/// canonical decimal `u64` (no leading zeros, no sign, in range), and
/// `.nar` — i.e. unless `canonical_nar_name` reproduces `name` exactly.
#[must_use]
pub fn parse_canonical_nar_name(name: &str) -> Option<([u8; 32], u64)> {
    let stem = name.strip_suffix(".nar")?;
    let (hash_str, size_part) = stem.split_at_checked(52)?;
    let size_str = size_part.strip_prefix('-')?;
    if size_str.is_empty() || !size_str.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // Reject non-canonical decimals: "042" round-trips to "42".
    if size_str.len() > 1 && size_str.starts_with('0') {
        return None;
    }
    let nar_size = size_str.parse::<u64>().ok()?;
    // `decode` also rejects nonzero padding bits in the leading
    // character, so accepted hashes re-encode to `hash_str` exactly.
    let nar_sha256: [u8; 32] = nixbase32::decode(hash_str).ok()?.try_into().ok()?;
    Some((nar_sha256, nar_size))
}

/// Builds the store key under which the alias for an upstream NAR URL
/// basename lives: `nar-alias:{basename}`.
///
/// Only the final `/`-separated segment of `url_basename` is used, so the
/// returned key never contains a `/`.
#[must_use]
pub fn alias_key(url_basename: &str) -> String {
    let basename = url_basename.rsplit('/').next().unwrap_or(url_basename);
    format!("nar-alias:{basename}")
}

/// Formats the alias value for a NAR: `{lowercase hex(nar_sha256)}-{nar_size}`.
#[must_use]
pub fn format_alias(nar_sha256: &[u8; 32], nar_size: u64) -> String {
    format!("{}-{nar_size}", hex::encode(nar_sha256))
}

/// Parses an alias value produced by [`format_alias`].
///
/// # Errors
///
/// Returns an `InvalidArgument` error unless `s` is exactly 64 lowercase
/// hex characters, `-`, and a canonical decimal `u64` (no leading zeros,
/// no sign, in range).
pub fn parse_alias(s: &str) -> Result<([u8; 32], u64), Error> {
    let (hex_str, size_part) = s
        .split_at_checked(64)
        .ok_or_else(|| make_input_err!("nar alias '{s}' is shorter than a 64-character hash"))?;
    if !hex_str
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(make_input_err!(
            "nar alias hash '{hex_str}' is not lowercase hex"
        ));
    }
    let size_str = size_part.strip_prefix('-').ok_or_else(|| {
        make_input_err!("nar alias '{s}' lacks a '-' after the 64-character hash")
    })?;
    if size_str.is_empty() || !size_str.bytes().all(|b| b.is_ascii_digit()) {
        return Err(make_input_err!(
            "nar alias size '{size_str}' is not a decimal number"
        ));
    }
    if size_str.len() > 1 && size_str.starts_with('0') {
        return Err(make_input_err!(
            "nar alias size '{size_str}' has leading zeros"
        ));
    }
    let nar_size = size_str
        .parse::<u64>()
        .map_err(|e| make_input_err!("nar alias size '{size_str}' does not fit in u64: {e}"))?;
    let mut nar_sha256 = [0_u8; 32];
    hex::decode_to_slice(hex_str, &mut nar_sha256)
        .map_err(|e| make_input_err!("invalid hex in nar alias hash '{hex_str}': {e}"))?;
    Ok((nar_sha256, nar_size))
}

#[cfg(test)]
mod tests {
    use super::{
        GZIP_MAGIC, NarCodec, alias_key, canonical_nar_name, codec_for_name, format_alias,
        parse_alias, parse_canonical_nar_name,
    };

    /// sha256("hello"); its nix32 form is a golden vector from the
    /// `nixbase32` module (generated with nix 2.34.7).
    const HELLO_HEX: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
    const HELLO_NIX32: &str = "094qif9n4cq4fdg459qzbhg1c6wywawwaaivx0k0x8xhbyx4vwic";

    fn hello_hash() -> [u8; 32] {
        let bytes = hex::decode(HELLO_HEX).expect("valid hex");
        bytes.try_into().expect("32 bytes")
    }

    #[test]
    fn codec_for_name_maps_known_extensions() {
        assert_eq!(codec_for_name("x.nar"), Some(NarCodec::None));
        assert_eq!(codec_for_name("x.nar.xz"), Some(NarCodec::Xz));
        assert_eq!(codec_for_name("x.nar.zst"), Some(NarCodec::Zstd));
        assert_eq!(codec_for_name("x.nar.bz2"), Some(NarCodec::Bzip2));
        // Full URL-ish names work too: only the suffix matters.
        assert_eq!(
            codec_for_name("nar/1w1fff338fvdw53sqgamddn1b2xgds473pv6y13gizdbqjv4i5p3.nar.xz"),
            Some(NarCodec::Xz)
        );
        assert_eq!(codec_for_name(".nar"), Some(NarCodec::None));
    }

    #[test]
    fn codec_for_name_rejects_everything_else() {
        // gzip is never selected by name — only by sniffing GZIP_MAGIC.
        for unsupported in [
            "x.nar.gz",
            "x.nar.br",
            "x.narinfo",
            "x.tar",
            "x.nar.xz.bak",
            "nar",
            "",
        ] {
            assert_eq!(codec_for_name(unsupported), None, "for '{unsupported}'");
        }
    }

    #[test]
    fn gzip_magic_is_rfc_1952() {
        assert_eq!(GZIP_MAGIC, [0x1f, 0x8b]);
    }

    #[test]
    fn canonical_nar_name_round_trips() {
        for size in [1_u64, 42, 206_104, u64::MAX] {
            let name = canonical_nar_name(&hello_hash(), size);
            assert_eq!(name, format!("{HELLO_NIX32}-{size}.nar"));
            assert_eq!(parse_canonical_nar_name(&name), Some((hello_hash(), size)));
        }
        // Zero is a canonical decimal with no leading zeros.
        assert_eq!(
            parse_canonical_nar_name(&canonical_nar_name(&hello_hash(), 0)),
            Some((hello_hash(), 0))
        );
    }

    #[test]
    fn parse_canonical_nar_name_rejects_malformed_names() {
        let hash51 = &HELLO_NIX32[..51];
        let hash53 = format!("{HELLO_NIX32}0");
        let bad_alphabet = format!("e{}", &HELLO_NIX32[1..]);
        for bad in [
            // 51- and 53-character hashes.
            format!("{hash51}-42.nar"),
            format!("{hash53}-42.nar"),
            // 'e' is outside the nix32 alphabet.
            format!("{bad_alphabet}-42.nar"),
            // Leading zeros / sign / non-decimal sizes.
            format!("{HELLO_NIX32}-042.nar"),
            format!("{HELLO_NIX32}-00.nar"),
            format!("{HELLO_NIX32}-+42.nar"),
            format!("{HELLO_NIX32}--42.nar"),
            format!("{HELLO_NIX32}-4x2.nar"),
            format!("{HELLO_NIX32}-.nar"),
            // u64::MAX + 1 does not fit.
            format!("{HELLO_NIX32}-18446744073709551616.nar"),
            // Missing '-' or missing/wrong extension.
            format!("{HELLO_NIX32}42.nar"),
            format!("{HELLO_NIX32}-42"),
            format!("{HELLO_NIX32}-42.nar.xz"),
            format!("{HELLO_NIX32}-42.narx"),
            String::new(),
        ] {
            assert_eq!(parse_canonical_nar_name(&bad), None, "for '{bad}'");
        }
        // A 52-character string whose leading character carries nonzero
        // padding bits is not the encoding of any 32-byte digest.
        let bad_padding = format!("z{}", &HELLO_NIX32[1..]);
        assert_eq!(
            parse_canonical_nar_name(&format!("{bad_padding}-42.nar")),
            None
        );
    }

    #[test]
    fn alias_key_is_prefixed_and_slash_free() {
        assert_eq!(alias_key("abc.nar.xz"), "nar-alias:abc.nar.xz");
        // Only the final path segment is used, so keys never contain '/'.
        assert_eq!(alias_key("nar/abc.nar.xz"), "nar-alias:abc.nar.xz");
        assert_eq!(alias_key("a/b/c.nar"), "nar-alias:c.nar");
        assert_eq!(alias_key(""), "nar-alias:");
        assert!(!alias_key("nar/deep/path.nar").contains('/'));
    }

    #[test]
    fn alias_round_trips() {
        for size in [0_u64, 1, 336_128, u64::MAX] {
            let alias = format_alias(&hello_hash(), size);
            assert_eq!(alias, format!("{HELLO_HEX}-{size}"));
            let (hash, parsed_size) = parse_alias(&alias).expect("parse alias");
            assert_eq!(hash, hello_hash());
            assert_eq!(parsed_size, size);
        }
    }

    #[test]
    fn parse_alias_rejects_malformed_values() {
        let upper_hex = HELLO_HEX.to_uppercase();
        for bad in [
            // Too short for the 64-character hash / truncated hash.
            String::new(),
            "abc-42".to_string(),
            HELLO_HEX[..63].to_string(),
            // Uppercase and non-hex hashes.
            format!("{upper_hex}-42"),
            format!("g{}-42", &HELLO_HEX[1..]),
            // Missing '-', missing size, non-decimal sizes.
            HELLO_HEX.to_string(),
            format!("{HELLO_HEX}42"),
            format!("{HELLO_HEX}-"),
            format!("{HELLO_HEX}-042"),
            format!("{HELLO_HEX}-+42"),
            format!("{HELLO_HEX}-4x2"),
            format!("{HELLO_HEX}-18446744073709551616"),
        ] {
            assert!(parse_alias(&bad).is_err(), "expected rejection of '{bad}'");
        }
    }
}
