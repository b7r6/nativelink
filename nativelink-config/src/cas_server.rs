// Copyright 2024 The NativeLink Authors. All rights reserved.
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

use std::collections::{HashMap, HashSet};

use nativelink_error::{Code, Error, ResultExt, make_err};
#[cfg(feature = "dev-schema")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::schedulers::SchedulerSpec;
use crate::serde_utils::{
    convert_data_size_with_shellexpand, convert_duration_with_shellexpand,
    convert_numeric_with_shellexpand, convert_optional_numeric_with_shellexpand,
    convert_optional_string_with_shellexpand, convert_string_with_shellexpand,
    convert_vec_string_with_shellexpand,
};
use crate::stores::{ClientTlsConfig, ConfigDigestHashFunction, StoreRefName, StoreSpec};

/// Name of the scheduler. This type will be used when referencing a
/// scheduler in the `CasConfig::schedulers`'s map key.
pub type SchedulerRefName = String;

/// Used when the config references `instance_name` in the protocol.
pub type InstanceName = String;

#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct WithInstanceName<T> {
    /// Used when the config references `instance_name` in the protocol.
    #[serde(default)]
    pub instance_name: InstanceName,
    #[serde(flatten)]
    pub config: T,
}

impl<T> core::ops::Deref for WithInstanceName<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct NamedConfig<Spec> {
    pub name: String,
    #[serde(flatten)]
    pub spec: Spec,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum HttpCompressionAlgorithm {
    /// No compression.
    #[default]
    None,

    /// Zlib compression.
    Gzip,
}

/// Note: Compressing data in the cloud rarely has a benefit, since most
/// cloud providers have very high bandwidth backplanes. However, for
/// clients not inside the data center, it might be a good idea to
/// compress data to and from the cloud. This will however come at a high
/// CPU and performance cost. If you are making remote execution share the
/// same CAS/AC servers as client's remote cache, you can create multiple
/// services with different compression settings that are served on
/// different ports. Then configure the non-cloud clients to use one port
/// and cloud-clients to use another.
#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct HttpCompressionConfig {
    /// The compression algorithm that the server will use when sending
    /// responses to clients. Enabling this will likely save a lot of
    /// data transfer, but will consume a lot of CPU and add a lot of
    /// latency.
    /// see: <https://github.com/tracemachina/nativelink/issues/109>
    ///
    /// Default: `HttpCompressionAlgorithm::None`
    pub send_compression_algorithm: Option<HttpCompressionAlgorithm>,

    /// The compression algorithm that the server will accept from clients.
    /// The server will broadcast the supported compression algorithms to
    /// clients and the client will choose which compression algorithm to
    /// use. Enabling this will likely save a lot of data transfer, but
    /// will consume a lot of CPU and add a lot of latency.
    /// see: <https://github.com/tracemachina/nativelink/issues/109>
    ///
    /// Default: {no supported compression}
    pub accepted_compression_algorithms: Vec<HttpCompressionAlgorithm>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct AcStoreConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub ac_store: StoreRefName,

    /// Whether the Action Cache store may be written to, this if set to false
    /// it is only possible to read from the Action Cache.
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct CasStoreConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cas_store: StoreRefName,
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct CapabilitiesRemoteExecutionConfig {
    /// Scheduler used to configure the capabilities of remote execution.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub scheduler: SchedulerRefName,
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct CapabilitiesConfig {
    /// Configuration for remote execution capabilities.
    /// If not set the capabilities service will inform the client that remote
    /// execution is not supported.
    pub remote_execution: Option<CapabilitiesRemoteExecutionConfig>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ExecutionConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    /// This value must be a CAS store reference.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cas_store: StoreRefName,

    /// The scheduler name referenced in the `schedulers` map in the main config.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub scheduler: SchedulerRefName,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct FetchConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub fetch_store: StoreRefName,

    /// Optional OCI toolchain configuration. When set, `FetchDirectory`
    /// requests with `oci://` URIs will pull the OCI image, project it into
    /// an REAPI Directory tree, upload blobs to the CAS store, and return the
    /// root Directory digest. See the Standard OCI Toolchain Specification §6.
    #[serde(default)]
    pub oci: Option<OciFetchConfig>,
}

/// Configuration for OCI toolchain image fetching via the Remote Asset API.
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct OciFetchConfig {
    /// The CAS store to upload projected file blobs and Directory protos into.
    /// Typically the same store used by the execution service. If omitted,
    /// uses `fetch_store`.
    #[serde(default)]
    #[serde(deserialize_with = "convert_optional_string_with_shellexpand")]
    pub cas_store: Option<StoreRefName>,

    /// Whether to check for existing blobs before uploading (dedup via
    /// `FindMissingBlobs`-equivalent `has_many()`). Default: true.
    #[serde(default = "default_true")]
    pub dedup_check: bool,

    /// Digest function for the REAPI projection.
    /// Must match what the execution service expects. Default: "BLAKE3".
    #[serde(default = "default_oci_digest_function")]
    pub digest_function: String,
}

const fn default_true() -> bool {
    true
}

fn default_oci_digest_function() -> String {
    "BLAKE3".to_string()
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct PushConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store name referenced here may be reused multiple times.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub push_store: StoreRefName,

    /// Whether the Action Cache store may be written to, this if set to false
    /// it is only possible to read from the Action Cache.
    #[serde(default)]
    pub read_only: bool,
}

/// Configuration for a Nix binary-cache (substituter) service. This serves
/// the Nix HTTP binary-cache protocol (`nix-cache-info`, `*.narinfo` and
/// `nar/*` endpoints) directly from `NativeLink` stores, so `nix` clients
/// can list a `NativeLink` deployment in their `substituters`.
///
/// The service mounts an HTTP router at `path` (default `/nix/<instance>`)
/// on the same listener as the gRPC services, so it coexists with a full
/// remote-execution stack on one port. Its `cas_store` (NAR blobs, keyed
/// by `DigestInfo(sha256(nar), nar_size)`) may reuse the same content-
/// addressed store as the gRPC CAS; `path_info_store` and `alias_store`
/// are string-keyed and must be separate stores (see their field docs).
/// See `examples/basic_cas_with_nix.json5` for a combined deployment.
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct NixCacheConfig {
    /// The store name referenced in the `stores` map in the main config.
    /// This store holds the uncompressed NAR blobs and is digest-keyed:
    /// each NAR is stored under `DigestInfo(sha256(nar), nar_size)`, so any
    /// content-addressed CAS store works here. It is strongly recommended
    /// to wrap this store in `verify` with both `verify_size` and
    /// `verify_hash` enabled so corrupt or truncated NAR uploads are
    /// rejected at write time instead of being served to clients.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cas_store: StoreRefName,

    /// The store name referenced in the `stores` map in the main config.
    /// This store holds one record per Nix store path and is string-keyed
    /// (NOT digest-keyed): records live under the 32-character `nixbase32`
    /// store-path hash — the `<hash>` in `/nix/store/<hash>-<name>`.
    ///
    /// It is recommended to wrap this store in `completeness_checking`
    /// with its `cas_store` referencing the NAR store above, so a
    /// `narinfo` whose NAR has been evicted returns 404 instead of
    /// advertising a NAR that can no longer be served.
    ///
    /// Never wrap this store in `existence_cache`: it drops overwrites
    /// (later uploads of the same store path would be silently ignored)
    /// and it rewrites string keys. Never wrap it in `verify` or
    /// `size_partitioning` either — both reject string keys.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub path_info_store: StoreRefName,

    /// The store name referenced in the `stores` map in the main config.
    /// This is a small string-keyed store mapping client-chosen NAR URL
    /// names (the `url` field a client wrote into an uploaded `narinfo`)
    /// to the `(digest, size)` of the NAR blob in `cas_store`, so uploads
    /// are served back under the exact URL the client chose.
    ///
    /// This store must NOT sit behind the `completeness_checking` wrapper
    /// used for `path_info_store`: alias records are not `narinfo`
    /// records and would fail its decoding.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub alias_store: StoreRefName,

    /// URL prefix under which this instance is mounted on the listener.
    /// If the path is "/nix/main" and your domain is "example.com", the
    /// cache root is <http://example.com/nix/main> and clients probe
    /// <http://example.com/nix/main/nix-cache-info>.
    ///
    /// Default: "/nix/<`instance_name`>"
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub path: Option<String>,

    /// The Nix store directory this cache serves paths for, advertised to
    /// clients via the `StoreDir` field of `nix-cache-info`. Clients
    /// refuse to substitute from a cache whose store dir differs from
    /// their own, so this must match the store dir of the Nix clients —
    /// almost always "/nix/store".
    ///
    /// Default: "/nix/store"
    #[serde(
        default = "default_nix_store_dir",
        deserialize_with = "convert_string_with_shellexpand"
    )]
    pub store_dir: String,

    /// Priority advertised via the `Priority` field of `nix-cache-info`.
    /// When a client has several substituters configured, lower values
    /// sort earlier, so a lower number makes this cache preferred. The
    /// official `cache.nixos.org` uses 40.
    ///
    /// Default: 40
    #[serde(
        default = "default_nix_priority",
        deserialize_with = "convert_numeric_with_shellexpand"
    )]
    pub priority: u32,

    /// Value of the `WantMassQuery` field of `nix-cache-info`. When true,
    /// clients are told it is acceptable to batch-query this cache for
    /// many store paths at once, for example when computing which parts
    /// of a large closure can be substituted.
    ///
    /// Default: true
    #[serde(default = "default_true")]
    pub want_mass_query: bool,

    /// Paths to secret signing keys in the format produced by
    /// `nix key generate-secret --key-name <name>` — a single line of
    /// `<name>:<base64 ed25519 keypair>`. Every served `narinfo` gets one
    /// `Sig` line per key, so listing multiple keys enables key rotation:
    /// sign with both the old and the new key while clients migrate their
    /// `trusted-public-keys`.
    ///
    /// Default: [] (`narinfo` responses are unsigned; clients then need
    /// to trust the cache some other way, such as `require-sigs = false`)
    #[serde(default, deserialize_with = "convert_vec_string_with_shellexpand")]
    pub signing_key_files: Vec<String>,

    /// Staging directory for decompressing compressed NAR uploads
    /// (`.nar.xz`, `.nar.zst`, `.nar.bz2` and gzip-sniffed `.nar`).
    /// Compressed uploads are stream-decompressed into a temporary file
    /// here before being written to `cas_store`, so the filesystem behind
    /// it needs enough space for the largest uncompressed NAR in flight.
    /// The directory is created if missing and pruned of stale files at
    /// startup.
    ///
    /// Default: `<system temp>/nativelink-nix-spool/<instance_name>`
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub spool_path: Option<String>,

    /// When true, uploads are rejected: any `PUT` request returns
    /// `405 Method Not Allowed`. Set this on public-facing listeners so
    /// only internal listeners can populate the cache. A valid write
    /// token (see `write_token_files`) does NOT override this.
    ///
    /// Default: false
    #[serde(default)]
    pub read_only: bool,

    /// Paths to read-token files. Each file holds exactly ONE token
    /// (surrounding whitespace is trimmed); listing multiple files
    /// enables rotation — any listed token is accepted. Files are read
    /// at startup and unreadable files or empty tokens fail fast.
    ///
    /// When non-empty, EVERY request on this instance (including
    /// `nix-cache-info`) requires a valid read or write token, presented
    /// either as `Authorization: Bearer <token>` or as HTTP Basic auth
    /// where the token is the PASSWORD and the username is ignored —
    /// the latter is how stock nix authenticates via `netrc`. Requests
    /// without a valid token get `401` with a `Basic` challenge; nix
    /// treats a `401` on a `narinfo` fetch as a clean miss, so a private
    /// cache stays hidden from unauthenticated clients.
    ///
    /// Default: [] (anonymous reads)
    #[serde(default, deserialize_with = "convert_vec_string_with_shellexpand")]
    pub read_token_files: Vec<String>,

    /// Paths to write-token files, with the same one-token-per-file
    /// semantics as `read_token_files`. When non-empty, every `PUT`
    /// additionally requires a valid write token — a read token alone is
    /// not enough. `read_only` still wins over a valid write token:
    /// uploads then get `405`.
    ///
    /// Default: [] (writes gated only by `read_only`)
    #[serde(default, deserialize_with = "convert_vec_string_with_shellexpand")]
    pub write_token_files: Vec<String>,

    /// Compression this cache serves NARs with: unset or `"none"` serves
    /// uncompressed NARs; `"zstd"` transcodes each NAR once when its
    /// `narinfo` is uploaded — the uncompressed NAR is streamed out of
    /// `cas_store` through a zstd encoder and the compressed result is
    /// stored back into `cas_store` under its own digest, so served
    /// `narinfo` documents advertise a `.nar.zst` URL with real
    /// `FileHash`/`FileSize` lines. If the compressed blob is later
    /// evicted, serving falls back to the uncompressed NAR. Stored
    /// signatures stay valid either way: the signed fingerprint covers
    /// only the uncompressed NAR, never the served URL or compression.
    ///
    /// Default: unset (serve uncompressed NARs)
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub serve_compression: Option<String>,

    /// The zstd compression level used when `serve_compression` is
    /// `"zstd"`; ignored otherwise.
    ///
    /// Default: 3
    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub compression_level: Option<i32>,

    /// Round-trip compression fidelity. When true (the default), a
    /// compressed NAR upload (`.nar.xz`/`.nar.zst`/`.nar.bz2`, the codec
    /// nix's `nix copy` picks — `xz` by default) has its ORIGINAL bytes
    /// stored as a CAS blob and served verbatim under the client's URL,
    /// and the served `narinfo` advertises the original
    /// compression/`FileHash`/`FileSize`. A client that pushed a path can
    /// then pull it straight back within the narinfo TTL (the URL it
    /// cached still resolves), matching attic/harmonia/nix-serve. When
    /// false, compressed uploads are decompressed to the canonical
    /// uncompressed NAR and served as `Compression: none`, saving roughly
    /// the compressed blob's storage (~0.3x of the NAR) at the cost of
    /// that warm-pull round trip.
    ///
    /// Interaction with `serve_compression = "zstd"`: a preserved original
    /// takes precedence over transcoding, so a compressed push is served
    /// back in the CLIENT's original codec (no re-encode, saving CPU) and
    /// only `Compression: none` pushes are transcoded to zstd. If a
    /// preserved compressed blob is later evicted, serving degrades
    /// gracefully to the uncompressed NAR (`Compression: none`); the
    /// signed fingerprint covers only the uncompressed NAR, so stored
    /// signatures stay valid across every rendering.
    ///
    /// Default: true
    #[serde(default = "default_true")]
    pub preserve_upload_compression: bool,

    /// Maximum size in bytes of a single uncompressed NAR the cache will
    /// ingest. Compressed uploads (`.nar.xz`/`.nar.zst`/`.nar.bz2` and
    /// gzip-sniffed `.nar`) are stream-decompressed into the spool
    /// directory; without a cap a small crafted upload could decompress to
    /// arbitrarily many bytes and fill the spool disk (a decompression
    /// bomb). A declared size (the `Content-Length` of a direct upload, or
    /// the size embedded in a canonical NAR name) over this limit is
    /// rejected with `413` before the body is read, and a decompressed
    /// stream that grows past it is aborted with `413` mid-flight (the
    /// partial spool file is deleted).
    ///
    /// The default is deliberately generous so legitimate large closures
    /// still push; size it to the spool filesystem's capacity.
    ///
    /// Default: 34359738368 (32 GiB)
    #[serde(
        default = "default_max_nar_size_bytes",
        deserialize_with = "convert_data_size_with_shellexpand"
    )]
    pub max_nar_size_bytes: u64,

    /// Maximum number of NAR GET response bodies streamed concurrently from
    /// this instance. Each in-flight stream holds a producer task plus a
    /// small buffer, so without a ceiling many slow readers grow aggregate
    /// memory without bound. Requests over the limit get a retryable `503`
    /// rather than committing a `200` and buffering.
    ///
    /// The default is high enough that normal parallel substitution is
    /// never throttled.
    ///
    /// Default: 256
    #[serde(
        default = "default_max_concurrent_nar_streams",
        deserialize_with = "convert_numeric_with_shellexpand"
    )]
    pub max_concurrent_nar_streams: usize,

    /// Maximum number of zstd NAR transcodes running concurrently when
    /// `serve_compression` is `"zstd"`. A transcode streams a whole NAR
    /// through a zstd encoder and spools the output, so unbounded fan-out
    /// of narinfo PUTs would balloon CPU and temp disk. Concurrent PUTs for
    /// the SAME NAR are additionally coalesced into a single transcode.
    ///
    /// Default: 8
    #[serde(
        default = "default_max_concurrent_transcodes",
        deserialize_with = "convert_numeric_with_shellexpand"
    )]
    pub max_concurrent_transcodes: usize,

    /// Idle timeout, in seconds, on a NAR upload body: if no new bytes
    /// arrive within this window the upload is aborted with `408` and its
    /// spool file and file descriptor are released, so a stalled or
    /// slowloris-style client cannot pin resources indefinitely. The timer
    /// resets on every received chunk, so a legitimately slow but steady
    /// upload over a slow link is never killed.
    ///
    /// Default: 60
    #[serde(
        default = "default_nar_upload_idle_timeout_s",
        deserialize_with = "convert_duration_with_shellexpand"
    )]
    pub nar_upload_idle_timeout_s: u64,

    /// Upstream Nix binary caches to read through on a local miss. When
    /// non-empty, a `narinfo` GET that misses the local `path_info_store`
    /// is retried against each listed cache in order; the first cache that
    /// has the path (and whose `narinfo` verifies against one of its
    /// `trusted_public_keys`) is fetched — `narinfo` AND NAR — verified,
    /// and ingested into this instance's stores, so the path is served
    /// locally from then on ("durable on first fetch"). The re-served
    /// `narinfo` is signed with this instance's own `signing_key_files`
    /// in addition to the preserved upstream signatures.
    ///
    /// The NAR is fetched eagerly during the `narinfo` GET so the record
    /// satisfies a `completeness_checking` `path_info_store` immediately;
    /// see the store-composition notes in `examples/nix_cache.json5`.
    /// Read-through populates the stores regardless of `read_only` (which
    /// only gates client PUTs).
    ///
    /// Default: [] (read-through disabled; a local miss is a plain 404)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub upstream_caches: Vec<NixUpstreamCacheConfig>,

    /// How long, in seconds, to remember that a path was NOT found in any
    /// upstream, so a repeated request (Nix issues many `narinfo` probes
    /// while computing a closure) does not re-query every upstream each
    /// time. Only misses are cached; a successful fetch is durable in the
    /// stores. Set to 0 to disable negative caching.
    ///
    /// Ignored when `upstream_caches` is empty.
    ///
    /// Default: 60
    #[serde(
        default = "default_upstream_negative_ttl_s",
        deserialize_with = "convert_duration_with_shellexpand"
    )]
    pub upstream_negative_ttl_s: u64,

    /// Timeout, in seconds, for a single upstream `narinfo` probe. Kept
    /// short because the probe is a small metadata request; the (possibly
    /// large) NAR download that follows a hit is not bound by this timeout
    /// but by `max_nar_size_bytes` during ingest.
    ///
    /// Ignored when `upstream_caches` is empty.
    ///
    /// Default: 30
    #[serde(
        default = "default_upstream_timeout_s",
        deserialize_with = "convert_duration_with_shellexpand"
    )]
    pub upstream_timeout_s: u64,
}

/// One upstream Nix binary cache for the read-through feature (see
/// [`NixCacheConfig::upstream_caches`]).
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct NixUpstreamCacheConfig {
    /// Base URL of the upstream binary cache, without a trailing slash,
    /// e.g. `"https://cache.nixos.org"` or
    /// `"https://nix-community.cachix.org"`. The `narinfo` is fetched from
    /// `<url>/<hash>.narinfo` and the NAR from `<url>/<narinfo URL>`.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub url: String,

    /// Nix public keys (`<name>:<base64>`, the same form listed in a
    /// client's `trusted-public-keys`, e.g.
    /// `cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=`)
    /// that a fetched `narinfo` must be signed by before this cache will
    /// store and serve it. At least one listed key must verify the
    /// `narinfo` fingerprint; an unsigned or unverifiable `narinfo` is
    /// refused and the path is treated as an upstream miss, so
    /// read-through can never be a cache-poisoning vector. Must be
    /// non-empty.
    #[serde(default, deserialize_with = "convert_vec_string_with_shellexpand")]
    pub trusted_public_keys: Vec<String>,
}

fn default_nix_store_dir() -> String {
    "/nix/store".to_string()
}

/// Configuration for the caching HTTP forward proxy
/// (see [`ServicesConfig::http_cache_proxy`]).
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct HttpCacheProxyConfig {
    /// Store name for fetched bodies. Digest-keyed: every body is stored
    /// under `DigestInfo(sha256(body), size)`, so any content-addressed CAS
    /// works and may be shared with the gRPC CAS and the `nix_cache` NAR
    /// store. Wrapping it in `verify` is recommended.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cas_store: StoreRefName,

    /// Store name for the URL index. A small string-keyed map from a fetched
    /// URL to the `(digest, size, content-type)` of its body in `cas_store`,
    /// so a repeat fetch of the same URL is served from the CAS. Must be a
    /// separate, string-keyed store (do not wrap in `verify`,
    /// `size_partitioning`, or `completeness_checking`).
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub alias_store: StoreRefName,

    /// Path to the proxy's CA certificate. Clients must trust this to accept
    /// the intercepted TLS connections (nix: `NIX_SSL_CERT_FILE`). Generated
    /// along with `ca_key_file` if either file is missing.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub ca_cert_file: String,

    /// Path to the proxy's CA private key. This key can impersonate any host
    /// to a client that trusts the CA, so keep it private (it is written
    /// `0600` when generated). Generated with `ca_cert_file` if missing.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub ca_key_file: String,

    /// Maximum size in bytes of a single response body the proxy will cache.
    /// A larger response is streamed through to the client but not stored.
    ///
    /// Default: 2147483648 (2 GiB)
    #[serde(
        default = "default_max_fetch_size_bytes",
        deserialize_with = "convert_data_size_with_shellexpand"
    )]
    pub max_fetch_size_bytes: u64,

    /// Timeout in seconds for fetching a single URL from the origin on a
    /// cache miss.
    ///
    /// Default: 300
    #[serde(
        default = "default_fetch_timeout_s",
        deserialize_with = "convert_duration_with_shellexpand"
    )]
    pub fetch_timeout_s: u64,
}

const fn default_max_fetch_size_bytes() -> u64 {
    2 * 1024 * 1024 * 1024 // 2 GiB
}

const fn default_fetch_timeout_s() -> u64 {
    300
}

const fn default_nix_priority() -> u32 {
    40
}

const fn default_max_nar_size_bytes() -> u64 {
    32 * 1024 * 1024 * 1024 // 32 GiB
}

const fn default_max_concurrent_nar_streams() -> usize {
    256
}

const fn default_max_concurrent_transcodes() -> usize {
    8
}

const fn default_nar_upload_idle_timeout_s() -> u64 {
    60
}

const fn default_upstream_negative_ttl_s() -> u64 {
    60
}

const fn default_upstream_timeout_s() -> u64 {
    30
}

// From https://github.com/serde-rs/serde/issues/818#issuecomment-287438544
fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    *t == Default::default()
}

#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ByteStreamConfig {
    /// Name of the store in the "stores" configuration.
    pub cas_store: StoreRefName,

    /// Max number of bytes to send on each grpc stream chunk.
    /// According to <https://github.com/grpc/grpc.github.io/issues/371>
    /// 16KiB - 64KiB is optimal.
    ///
    ///
    /// Default: 64KiB
    #[serde(
        default,
        deserialize_with = "convert_data_size_with_shellexpand",
        skip_serializing_if = "is_default"
    )]
    pub max_bytes_per_stream: usize,

    /// In the event a client disconnects while uploading a blob, we will hold
    /// the internal stream open for this many seconds before closing it.
    /// This allows clients that disconnect to reconnect and continue uploading
    /// the same blob.
    ///
    /// Default: 10 seconds
    #[serde(
        default,
        deserialize_with = "convert_duration_with_shellexpand",
        skip_serializing_if = "is_default",
        alias = "persist_stream_on_disconnect_timeout"
    )]
    pub persist_stream_on_disconnect_timeout_s: usize,
}

// Older bytestream config. All fields are as per the newer docs, but this requires
// the hashed cas_stores v.s. the WithInstanceName approach. This should _not_ be updated
// with newer fields, and eventually dropped
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct OldByteStreamConfig {
    pub cas_stores: HashMap<InstanceName, StoreRefName>,
    #[serde(
        default,
        deserialize_with = "convert_data_size_with_shellexpand",
        skip_serializing_if = "is_default"
    )]
    pub max_bytes_per_stream: usize,
    #[serde(
        default,
        deserialize_with = "convert_data_size_with_shellexpand",
        skip_serializing_if = "is_default"
    )]
    pub max_decoding_message_size: usize,
    #[serde(
        default,
        deserialize_with = "convert_duration_with_shellexpand",
        skip_serializing_if = "is_default",
        alias = "persist_stream_on_disconnect_timeout"
    )]
    pub persist_stream_on_disconnect_timeout_s: usize,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct WorkerApiConfig {
    /// The scheduler name referenced in the `schedulers` map in the main config.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub scheduler: SchedulerRefName,
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct AdminConfig {
    /// Path to register the admin API. If path is "/admin", and your
    /// domain is "example.com", you can reach the endpoint with:
    /// <http://example.com/admin>.
    ///
    /// Default: "/admin"
    #[serde(default)]
    pub path: String,
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct HealthConfig {
    /// Path to register the health status check. If path is "/status", and your
    /// domain is "example.com", you can reach the endpoint with:
    /// <http://example.com/status>.
    ///
    /// Default: "/status"
    #[serde(default)]
    pub path: String,

    /// Timeout on health checks. Default: 5s.
    #[serde(default)]
    pub timeout_seconds: u64,
}

#[derive(Deserialize, Serialize, Debug)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct BepConfig {
    /// The store to publish build events to.
    /// The store name referenced in the `stores` map in the main config.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub store: StoreRefName,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct IdentityHeaderSpec {
    /// The name of the header to look for the identity in.
    /// Default: "x-identity"
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub header_name: Option<String>,

    /// If the header is required to be set or fail the request.
    #[serde(default)]
    pub required: bool,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct OriginEventsPublisherSpec {
    /// The store to publish nativelink events to.
    /// The store name referenced in the `stores` map in the main config.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub store: StoreRefName,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct OriginEventsSpec {
    /// The publisher configuration for origin events.
    pub publisher: OriginEventsPublisherSpec,

    /// The maximum number of events to queue before applying back pressure.
    /// IMPORTANT: Backpressure causes all clients to slow down significantly.
    /// Zero is default.
    ///
    /// Default: 65536 (zero defaults to this)
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_event_queue_size: usize,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ServicesConfig {
    /// The Content Addressable Storage (CAS) backend config.
    /// The key is the `instance_name` used in the protocol and the
    /// value is the underlying CAS store config.
    #[serde(
        default,
        deserialize_with = "super::backcompat::opt_vec_with_instance_name"
    )]
    pub cas: Option<Vec<WithInstanceName<CasStoreConfig>>>,

    /// The Action Cache (AC) backend config.
    /// The key is the `instance_name` used in the protocol and the
    /// value is the underlying AC store config.
    #[serde(
        default,
        deserialize_with = "super::backcompat::opt_vec_with_instance_name"
    )]
    pub ac: Option<Vec<WithInstanceName<AcStoreConfig>>>,

    /// Capabilities service is required in order to use most of the
    /// bazel protocol. This service is used to provide the supported
    /// features and versions of this bazel GRPC service.
    #[serde(
        default,
        deserialize_with = "super::backcompat::opt_vec_with_instance_name"
    )]
    pub capabilities: Option<Vec<WithInstanceName<CapabilitiesConfig>>>,

    /// The remote execution service configuration.
    /// NOTE: This service is under development and is currently just a
    /// place holder.
    #[serde(
        default,
        deserialize_with = "super::backcompat::opt_vec_with_instance_name"
    )]
    pub execution: Option<Vec<WithInstanceName<ExecutionConfig>>>,

    /// This is the service used to stream data to and from the CAS.
    /// Bazel's protocol strongly encourages users to use this streaming
    /// interface to interact with the CAS when the data is large.
    #[serde(default, deserialize_with = "super::backcompat::opt_bytestream")]
    pub bytestream: Option<Vec<WithInstanceName<ByteStreamConfig>>>,

    /// These two are collectively the Remote Asset protocol, but it's
    /// defined as two separate services
    #[serde(
        default,
        deserialize_with = "super::backcompat::opt_vec_with_instance_name"
    )]
    pub fetch: Option<Vec<WithInstanceName<FetchConfig>>>,

    #[serde(
        default,
        deserialize_with = "super::backcompat::opt_vec_with_instance_name"
    )]
    pub push: Option<Vec<WithInstanceName<PushConfig>>>,

    /// The Nix binary-cache (substituter) services. Each entry mounts a
    /// Nix HTTP binary-cache endpoint on this listener that serves
    /// `nix-cache-info`, `narinfo` records and NAR blobs from the
    /// referenced stores.
    #[serde(
        default,
        deserialize_with = "super::backcompat::opt_vec_with_instance_name"
    )]
    pub nix_cache: Option<Vec<WithInstanceName<NixCacheConfig>>>,

    /// Caching HTTP forward proxy (TLS-intercepting) that stores fetched
    /// bodies in a `NativeLink` CAS. A build client points `HTTPS_PROXY`/
    /// `HTTP_PROXY` at this listener and trusts its generated CA (nix reads
    /// it via `NIX_SSL_CERT_FILE`); the first fetch of a URL is streamed
    /// from the origin into the CAS and every later fetch is served from it.
    /// This listener speaks the HTTP proxy protocol (`CONNECT`), so it must
    /// not share a port with other services.
    pub http_cache_proxy: Option<HttpCacheProxyConfig>,

    /// This is the service used for workers to connect and communicate
    /// through.
    /// NOTE: This service should be served on a different, non-public port.
    /// In other words, `worker_api` configuration should not have any other
    /// services that are served on the same port. Doing so is a security
    /// risk, as workers have a different permission set than a client
    /// that makes the remote execution/cache requests.
    pub worker_api: Option<WorkerApiConfig>,

    /// Experimental - Build Event Protocol (BEP) configuration. This is
    /// the service that will consume build events from the client and
    /// publish them to a store for processing by an external service.
    pub experimental_bep: Option<BepConfig>,

    /// This is the service for any administrative tasks.
    /// It provides a REST API endpoint for administrative purposes.
    pub admin: Option<AdminConfig>,

    /// This is the service for health status check.
    pub health: Option<HealthConfig>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct TlsConfig {
    /// Path to the certificate file.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cert_file: String,

    /// Path to the private key file.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub key_file: String,

    /// Path to the certificate authority for mTLS, if client authentication is
    /// required for this endpoint.
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub client_ca_file: Option<String>,

    /// Path to the certificate revocation list for mTLS, if client
    /// authentication is required for this endpoint.
    #[serde(default, deserialize_with = "convert_optional_string_with_shellexpand")]
    pub client_crl_file: Option<String>,
}

/// Advanced Http configurations. These are generally should not be set.
/// For documentation on what each of these do, see the hyper documentation:
/// See: <https://docs.rs/hyper/latest/hyper/server/conn/struct.Http.html>
///
/// Note: All of these default to hyper's default values unless otherwise
/// specified.
#[derive(Deserialize, Serialize, Debug, Default, Clone, Copy)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct HttpServerConfig {
    /// Interval to send keep-alive pings via HTTP2.
    /// Note: This is in seconds.
    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub http2_keep_alive_interval: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_pending_accept_reset_streams: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_initial_stream_window_size: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_initial_connection_window_size: Option<u32>,

    #[serde(default)]
    pub experimental_http2_adaptive_window: Option<bool>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_frame_size: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_concurrent_streams: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand",
        alias = "experimental_http2_keep_alive_timeout"
    )]
    pub experimental_http2_keep_alive_timeout_s: Option<u32>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_send_buf_size: Option<u32>,

    #[serde(default)]
    pub experimental_http2_enable_connect_protocol: Option<bool>,

    #[serde(
        default,
        deserialize_with = "convert_optional_numeric_with_shellexpand"
    )]
    pub experimental_http2_max_header_list_size: Option<u32>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum ListenerConfig {
    /// Listener for HTTP/HTTPS/HTTP2 sockets.
    Http(HttpListener),
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct HttpListener {
    /// Address to listen on. Example: `127.0.0.1:8080` or `:8080` to listen
    /// to all IPs.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub socket_address: String,

    /// Allow binding `socket_address` before it is assigned locally.
    ///
    /// Default: false
    #[serde(default)]
    pub freebind: bool,

    /// Data transport compression configuration to use for this service.
    #[serde(default)]
    pub compression: HttpCompressionConfig,

    /// Advanced Http server configuration.
    #[serde(default)]
    pub advanced_http: HttpServerConfig,

    /// Maximum number of bytes to decode on each grpc stream chunk.
    /// Default: 4 MiB
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub max_decoding_message_size: usize,

    /// Tls Configuration for this server.
    /// If not set, the server will not use TLS.
    ///
    /// Default: None
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct ServerConfig {
    /// Name of the server. This is used to help identify the service
    /// for telemetry and logs.
    ///
    /// Default: {index of server in config}
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub name: String,

    /// Configuration
    pub listener: ListenerConfig,

    /// Services to attach to server.
    pub services: Option<ServicesConfig>,

    /// The config related to identifying the client.
    /// Default: {see `IdentityHeaderSpec`}
    #[serde(default)]
    pub experimental_identity_header: IdentityHeaderSpec,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum WorkerProperty {
    /// List of static values.
    /// Note: Generally there should only ever be 1 value, but if the platform
    /// property key is `PropertyType::Priority` it may have more than one value.
    #[serde(deserialize_with = "convert_vec_string_with_shellexpand")]
    Values(Vec<String>),

    /// A dynamic configuration. The string will be executed as a command
    /// (not shell) and will be split by "\n" (new line character).
    QueryCmd(String),
}

/// Generic config for an endpoint and associated configs.
#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct EndpointConfig {
    /// URI of the endpoint.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub uri: String,

    /// Timeout in seconds that a request should take.
    /// Default: 5 seconds
    pub timeout: Option<f32>,

    /// The TLS configuration to use to connect to the endpoint.
    pub tls_config: Option<ClientTlsConfig>,
}

#[derive(Copy, Clone, Deserialize, Serialize, Debug, Default)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum UploadCacheResultsStrategy {
    /// Only upload action results with an exit code of 0.
    #[default]
    SuccessOnly,

    /// Don't upload any action results.
    Never,

    /// Upload all action results that complete.
    Everything,

    /// Only upload action results that fail.
    FailuresOnly,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum EnvironmentSource {
    /// The name of the platform property in the action to get the value from.
    Property(String),

    /// The raw value to set.
    Value(#[serde(deserialize_with = "convert_string_with_shellexpand")] String),

    /// Take the value from the local environment corresponding to the name key
    FromEnvironment,

    /// The max amount of time in milliseconds the command is allowed to run
    /// (requested by the client).
    TimeoutMillis,

    /// A special file path will be provided that can be used to communicate
    /// with the parent process about out-of-band information. This file
    /// will be read after the command has finished executing. Based on the
    /// contents of the file, the behavior of the result may be modified.
    ///
    /// The format of the file contents should be json with the following
    /// schema:
    /// {
    ///   // If set the command will be considered a failure.
    ///   // May be one of the following static strings:
    ///   // "timeout": Will Consider this task to be a timeout.
    ///   "failure": "timeout",
    /// }
    ///
    /// All fields are optional, file does not need to be created and may be
    /// empty.
    SideChannelFile,

    /// A "root" directory for the action. This directory can be used to
    /// store temporary files that are not needed after the action has
    /// completed. This directory will be purged after the action has
    /// completed.
    ///
    /// For example:
    /// If an action writes temporary data to a path but nativelink should
    /// clean up this path after the job has executed, you may create any
    /// directory under the path provided in this variable. A common pattern
    /// would be to use `entrypoint` to set a shell script that reads this
    /// variable, `mkdir $ENV_VAR_NAME/tmp` and `export TMPDIR=$ENV_VAR_NAME/tmp`.
    /// Another example might be to bind-mount the `/tmp` path in a container to
    /// this path in `entrypoint`.
    ActionDirectory,
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct UploadActionResultConfig {
    /// Underlying AC store that the worker will use to publish execution results
    /// into. Objects placed in this store should be reachable from the
    /// scheduler/client-cas after they have finished updating.
    /// Default: {No uploading is done}
    pub ac_store: Option<StoreRefName>,

    /// In which situations should the results be published to the `ac_store`,
    /// if set to `SuccessOnly` then only results with an exit code of 0 will be
    /// uploaded, if set to Everything all completed results will be uploaded.
    ///
    /// Default: `SuccessOnly`
    #[serde(default)]
    pub upload_ac_results_strategy: UploadCacheResultsStrategy,

    /// Store to upload historical results to. This should be a CAS store if set.
    ///
    /// Default: {CAS store of parent}
    pub historical_results_store: Option<StoreRefName>,

    /// In which situations should the results be published to the historical CAS.
    /// The historical CAS is where failures are published. These messages conform
    /// to the CAS key-value lookup format and are always a `HistoricalExecuteResponse`
    /// serialized message.
    ///
    /// Default: `FailuresOnly`
    #[serde(default)]
    pub upload_historical_results_strategy: Option<UploadCacheResultsStrategy>,

    /// Template to use for the `ExecuteResponse.message` property. This message
    /// is attached to the response before it is sent to the client. The following
    /// special variables are supported:
    /// - `digest_function`: Digest function used to calculate the action digest.
    /// - `action_digest_hash`: Action digest hash.
    /// - `action_digest_size`: Action digest size.
    /// - `historical_results_hash`: `HistoricalExecuteResponse` digest hash.
    /// - `historical_results_size`: `HistoricalExecuteResponse` digest size.
    ///
    /// A common use case of this is to provide a link to the web page that
    /// contains more useful information for the user.
    ///
    /// An example that is fully compatible with `bb_browser` is:
    /// <https://example.com/my-instance-name-here/blobs/{digest_function}/action/{action_digest_hash}-{action_digest_size}/>
    ///
    /// Default: "" (no message)
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub success_message_template: String,

    /// Same as `success_message_template` but for failure case.
    ///
    /// An example that is fully compatible with `bb_browser` is:
    /// <https://example.com/my-instance-name-here/blobs/{digest_function}/historical_execute_response/{historical_results_hash}-{historical_results_size}/>
    ///
    /// Default: "" (no message)
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub failure_message_template: String,
}

#[derive(Deserialize, Serialize, Debug, Default)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct LocalWorkerConfig {
    /// Name of the worker. This is give a more friendly name to a worker for logging
    /// and metric publishing. This is also the prefix of the worker id
    /// (ie: "{name}{uuidv6}").
    /// Default: {Index position in the workers list}
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub name: String,

    /// Endpoint which the worker will connect to the scheduler's `WorkerApiService`.
    pub worker_api_endpoint: EndpointConfig,

    /// The maximum time an action is allowed to run. If a task requests for a timeout
    /// longer than this time limit, the task will be rejected. Value in seconds.
    ///
    /// Default: 20 minutes
    #[serde(
        default,
        deserialize_with = "convert_duration_with_shellexpand",
        alias = "max_action_timeout"
    )]
    pub max_action_timeout_s: usize,

    /// Maximum time allowed for uploading action results to CAS after execution
    /// completes. If upload takes longer than this, the action fails with
    /// `DeadlineExceeded` and may be retried by the scheduler. Value in seconds.
    ///
    /// Default: 10 minutes
    #[serde(
        default,
        deserialize_with = "convert_duration_with_shellexpand",
        alias = "max_upload_timeout"
    )]
    pub max_upload_timeout_s: usize,

    /// Maximum time to wait for action directory cleanup before timing out.
    /// Value in seconds.
    ///
    /// Default: 30 seconds
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub max_cleanup_wait_s: usize,

    /// Maximum backoff duration for exponential backoff when waiting for cleanup.
    /// Value in milliseconds.
    ///
    /// Default: 500 milliseconds
    #[serde(default, deserialize_with = "convert_duration_with_shellexpand")]
    pub max_cleanup_backoff_ms: usize,

    /// Maximum number of inflight tasks this worker can cope with.
    ///
    /// Default: 0 (infinite tasks)
    #[serde(default, deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_inflight_tasks: u64,

    /// If timeout is handled in `entrypoint` or another wrapper script.
    /// If set to true `NativeLink` will not honor the timeout the action requested
    /// and instead will always force kill the action after `max_action_timeout`
    /// has been reached. If this is set to false, the smaller value of the action's
    /// timeout and `max_action_timeout` will be used to which `NativeLink` will kill
    /// the action.
    ///
    /// The real timeout can be received via an environment variable set in:
    /// `EnvironmentSource::TimeoutMillis`.
    ///
    /// Example on where this is useful: `entrypoint` launches the action inside
    /// a docker container, but the docker container may need to be downloaded. Thus
    /// the timer should not start until the docker container has started executing
    /// the action. In this case, action will likely be wrapped in another program,
    /// like `timeout` and propagate timeouts via `EnvironmentSource::SideChannelFile`.
    ///
    /// Default: false (`NativeLink` fully handles timeouts)
    #[serde(default)]
    pub timeout_handled_externally: bool,

    /// The command to execute on every execution request. This will be parsed as
    /// a command + arguments (not shell).
    /// Example: "run.sh" and a job with command: "sleep 5" will result in a
    /// command like: "run.sh sleep 5".
    /// Default: {Use the command from the job request}.
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub entrypoint: String,

    /// An optional script to run before every action is processed on the worker.
    /// The value should be the full path to the script to execute and will pause
    /// all actions on the worker if it returns an exit code other than 0.
    /// If not set, then the worker will never pause and will continue to accept
    /// jobs according to the scheduler configuration.
    /// This is useful, for example, if the worker should not take any more
    /// actions until there is enough resource available on the machine to
    /// handle them.
    pub experimental_precondition_script: Option<String>,

    /// Underlying CAS store that the worker will use to download CAS artifacts.
    /// This store must be a `FastSlowStore`. The `fast` store must be a
    /// `FileSystemStore` because it will use hardlinks when building out the files
    /// instead of copying the files. The slow store must eventually resolve to the
    /// same store the scheduler/client uses to send job requests.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub cas_fast_slow_store: StoreRefName,

    /// Configuration for uploading action results.
    #[serde(default)]
    pub upload_action_result: UploadActionResultConfig,

    /// The directory work jobs will be executed from. This directory will be fully
    /// managed by the worker service and will be purged on startup.
    /// This directory and the directory referenced in `local_filesystem_store_ref`'s
    /// `stores::FilesystemStore::content_path` must be on the same filesystem.
    /// Hardlinks will be used when placing files that are accessible to the jobs
    /// that are sourced from `local_filesystem_store_ref`'s `content_path`.
    #[serde(deserialize_with = "convert_string_with_shellexpand")]
    pub work_directory: String,

    /// Properties of this worker. This configuration will be sent to the scheduler
    /// and used to tell the scheduler to restrict what should be executed on this
    /// worker.
    pub platform_properties: HashMap<String, WorkerProperty>,

    /// An optional mapping of environment names to set for the execution
    /// as well as those specified in the action itself.  If set, will set each
    /// key as an environment variable before executing the job with the value
    /// of the environment variable being the value of the property of the
    /// action being executed of that name or the fixed value.
    pub additional_environment: Option<HashMap<String, EnvironmentSource>>,

    /// Optional directory cache configuration for improving performance by caching
    /// reconstructed input directories and using hardlinks instead of rebuilding
    /// them from CAS for every action.
    /// Default: None (directory cache disabled)
    pub directory_cache: Option<DirectoryCacheConfig>,

    /// Whether to use namespaces to isolate the execution.  This is only available
    /// on Linux.  It is highly recommended as it avoids a number of issues with
    /// zombie processes and also provides additional hermeticity.  If explicitly set
    /// to true and it is not supported the worker will exit with an error.
    ///
    /// Note: this will fail for non-privileged Dockerised workers, as workers in
    /// Docker don't have permissions to make a new user namespace. Privileged
    /// containers can do this.
    ///
    /// Default: False.
    pub use_namespaces: Option<bool>,

    /// Whether to use a mount namespace to isolate the worker root.  This is only
    /// available on Linux and when `use_namespaces` is true.  It is highly recommended
    /// provides additional hermeticity.  If explicitly set to true and it is not
    /// supported or `use_namespaces` is not set to true the worker will exit with an
    /// error.
    /// Default: False.
    pub use_mount_namespace: Option<bool>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct DirectoryCacheConfig {
    /// Maximum number of cached directories.
    /// Default: 1000
    #[serde(default = "default_directory_cache_max_entries")]
    pub max_entries: usize,

    /// Maximum total size in bytes for all cached directories (0 = unlimited).
    /// Default: 10737418240 (10 GB)
    #[serde(
        default = "default_directory_cache_max_size_bytes",
        deserialize_with = "convert_data_size_with_shellexpand"
    )]
    pub max_size_bytes: u64,

    /// Base directory for cache storage. This directory will be managed by
    /// the worker and should be on the same filesystem as `work_directory`.
    /// Default: `{work_directory}/../directory_cache`
    #[serde(default, deserialize_with = "convert_string_with_shellexpand")]
    pub cache_root: String,
}

const fn default_directory_cache_max_entries() -> usize {
    1000
}

const fn default_directory_cache_max_size_bytes() -> u64 {
    10 * 1024 * 1024 * 1024 // 10 GB
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub enum WorkerConfig {
    /// A worker type that executes jobs locally on this machine.
    Local(LocalWorkerConfig),
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct GlobalConfig {
    /// Maximum number of open files that can be opened at one time.
    /// This value is not strictly enforced, it is a best effort. Some internal libraries
    /// open files or read metadata from a files which do not obey this limit, however
    /// the vast majority of cases will have this limit be honored.
    /// This value must be larger than `ulimit -n` to have any effect.
    /// Any network open file descriptors is not counted in this limit, but is counted
    /// in the kernel limit. It is a good idea to set a very large `ulimit -n`.
    /// Note: This value must be greater than 10.
    ///
    /// Default: 24576 (= 24 * 1024)
    #[serde(deserialize_with = "convert_numeric_with_shellexpand")]
    pub max_open_files: usize,

    /// Default hash function to use while uploading blobs to the CAS when not set
    /// by client.
    ///
    /// Default: `ConfigDigestHashFunction::sha256`
    pub default_digest_hash_function: Option<ConfigDigestHashFunction>,

    /// Default digest size to use for health check when running
    /// diagnostics checks. Health checks are expected to use this
    /// size for filling a buffer that is used for creation of
    /// digest.
    ///
    /// Default: 1024*1024 (1MiB)
    #[serde(default, deserialize_with = "convert_data_size_with_shellexpand")]
    pub default_digest_size_health_check: usize,

    /// When true, reject any request where the digest function is not explicitly
    /// set (i.e. arrives as 0/UNKNOWN) rather than silently defaulting to
    /// `default_digest_hash_function`. This prevents a class of bugs where a
    /// BLAKE3 client omits the field, the server defaults to SHA256, and output
    /// Directory trees are hashed with the wrong algorithm — corrupting results.
    ///
    /// When false (the default for backwards compatibility), unset digest
    /// functions fall through to `default_digest_hash_function` as before.
    ///
    /// Recommended: true for new deployments.
    ///
    /// Default: false
    #[serde(default)]
    pub require_explicit_digest_function: bool,
}

pub type StoreConfig = NamedConfig<StoreSpec>;
pub type SchedulerConfig = NamedConfig<SchedulerSpec>;

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "dev-schema", derive(JsonSchema))]
pub struct CasConfig {
    /// List of stores available to use in this config.
    /// The keys can be used in other configs when needing to reference a store.
    pub stores: Vec<StoreConfig>,

    /// Worker configurations used to execute jobs.
    pub workers: Option<Vec<WorkerConfig>>,

    /// List of schedulers available to use in this config.
    /// The keys can be used in other configs when needing to reference a
    /// scheduler.
    pub schedulers: Option<Vec<SchedulerConfig>>,

    /// Servers to setup for this process.
    pub servers: Vec<ServerConfig>,

    /// Experimental - Origin events configuration. This is the service that will
    /// collect and publish nativelink events to a store for processing by an
    /// external service.
    pub experimental_origin_events: Option<OriginEventsSpec>,

    /// Any global configurations that apply to all modules live here.
    pub global: Option<GlobalConfig>,
}

impl CasConfig {
    /// # Errors
    ///
    /// Will return `Err` if we can't load the file.
    pub fn try_from_json5_file(config_file: &str) -> Result<Self, Error> {
        let json_contents = std::fs::read_to_string(config_file)
            .err_tip(|| format!("Could not open config file {config_file}"))?;
        let config: Self = serde_json5::from_str(&json_contents)?;
        for server in &config.servers {
            if let Some(services) = &server.services {
                Self::check_store_conflict(services)?;
            }
        }
        Ok(config)
    }

    fn check_store_conflict(services: &ServicesConfig) -> Result<(), Error> {
        if let Some(cas_config) = &services.cas
            && let Some(ac_config) = &services.ac
        {
            // Create a hashmap from the CAS configuration for quick lookup
            let cas_store_map: HashMap<_, _> = cas_config
                .iter()
                .map(|with_instance_name| {
                    (
                        &with_instance_name.instance_name,
                        &with_instance_name.cas_store,
                    )
                })
                .collect();

            for with_instance_name in ac_config {
                if let Some(cas_store) = cas_store_map.get(&with_instance_name.instance_name)
                    && cas_store == &&with_instance_name.ac_store
                {
                    return Err(make_err!(
                        Code::InvalidArgument,
                        "CAS and AC use the same store '{}' in the config",
                        cas_store
                    ));
                }
            }
        }
        Ok(())
    }

    /// Offline structural validation of every store/scheduler name reference
    /// in this config. This is a pure, read-only check intended for
    /// `nativelink --check`: it constructs no stores, binds no sockets, opens
    /// no connections, and creates no directories.
    ///
    /// Every store name referenced by a worker or by a server service, and
    /// every scheduler name referenced by a service, must resolve to a store
    /// declared in `stores` / a scheduler declared in `schedulers`. Store
    /// references made *inside* a declared store's spec (the `ref_store`
    /// wrappers such as `fast_slow`, `dedup`, `completeness_checking`, ...)
    /// are also resolved, but only for the transitive closure of stores that
    /// are actually wired to a worker or service. Store definitions that are
    /// never referenced by any consumer (for example a pure store-catalog
    /// sample config) are intentionally not walked, so their illustrative
    /// placeholder references are never flagged.
    ///
    /// All problems are collected and reported together, each with a path
    /// such as `servers[0].services.nix_cache[main].path_info_store references
    /// undefined store 'NIX_PI'`, rather than failing on the first one.
    ///
    /// Duplicate store names and duplicate scheduler names are also reported,
    /// because [`crate::stores`] / the store manager resolve names last-wins
    /// and would otherwise silently drop the earlier definition.
    ///
    /// # Errors
    ///
    /// Returns `Err(Code::InvalidArgument)` if any referenced store/scheduler
    /// name is undeclared, or if a store/scheduler name is declared more than
    /// once.
    pub fn validate_references(&self) -> Result<(), Error> {
        let mut problems: Vec<String> = Vec::new();

        // Declared registries. Collision on insert is a duplicate name.
        let mut store_names: HashSet<&str> = HashSet::new();
        for store in &self.stores {
            if !store_names.insert(store.name.as_str()) {
                problems.push(format!("duplicate store name '{}'", store.name));
            }
        }
        let mut scheduler_names: HashSet<&str> = HashSet::new();
        for scheduler in self.schedulers.iter().flatten() {
            if !scheduler_names.insert(scheduler.name.as_str()) {
                problems.push(format!("duplicate scheduler name '{}'", scheduler.name));
            }
        }

        // Only validate a reference category when its registry is non-empty.
        // A config that declares zero stores (or zero schedulers) is a
        // fragment or legacy-format sample whose backing definitions live
        // elsewhere; every reference would then trivially be "unresolved" and
        // flagging them would be a false positive on an otherwise-valid
        // sample. Real deployments always declare their stores, so genuine
        // typos are still caught.
        let mut checker = ReferenceChecker {
            check_stores: !store_names.is_empty(),
            check_schedulers: !scheduler_names.is_empty(),
            store_names,
            scheduler_names,
            problems,
            used_stores: Vec::new(),
        };

        for (worker_idx, worker) in self.workers.iter().flatten().enumerate() {
            let WorkerConfig::Local(local) = worker;
            checker.store(
                &format!("workers[{worker_idx}].local.cas_fast_slow_store"),
                &local.cas_fast_slow_store,
            );
            if let Some(ac_store) = &local.upload_action_result.ac_store {
                checker.store(
                    &format!("workers[{worker_idx}].local.upload_action_result.ac_store"),
                    ac_store,
                );
            }
            if let Some(historical) = &local.upload_action_result.historical_results_store {
                checker.store(
                    &format!(
                        "workers[{worker_idx}].local.upload_action_result.historical_results_store"
                    ),
                    historical,
                );
            }
        }

        if let Some(origin_events) = &self.experimental_origin_events {
            checker.store(
                "experimental_origin_events.publisher.store",
                &origin_events.publisher.store,
            );
        }

        for (server_idx, server) in self.servers.iter().enumerate() {
            let Some(services) = &server.services else {
                continue;
            };
            let prefix = format!("servers[{server_idx}].services");
            for entry in services.cas.iter().flatten() {
                let name = &entry.instance_name;
                checker.store(&format!("{prefix}.cas[{name}].cas_store"), &entry.cas_store);
            }
            for entry in services.ac.iter().flatten() {
                let name = &entry.instance_name;
                checker.store(&format!("{prefix}.ac[{name}].ac_store"), &entry.ac_store);
            }
            for entry in services.execution.iter().flatten() {
                let name = &entry.instance_name;
                checker.store(
                    &format!("{prefix}.execution[{name}].cas_store"),
                    &entry.cas_store,
                );
                checker.scheduler(
                    &format!("{prefix}.execution[{name}].scheduler"),
                    &entry.scheduler,
                );
            }
            for entry in services.capabilities.iter().flatten() {
                if let Some(remote_execution) = &entry.remote_execution {
                    let name = &entry.instance_name;
                    checker.scheduler(
                        &format!("{prefix}.capabilities[{name}].remote_execution.scheduler"),
                        &remote_execution.scheduler,
                    );
                }
            }
            for entry in services.bytestream.iter().flatten() {
                let name = &entry.instance_name;
                checker.store(
                    &format!("{prefix}.bytestream[{name}].cas_store"),
                    &entry.cas_store,
                );
            }
            for entry in services.fetch.iter().flatten() {
                let name = &entry.instance_name;
                checker.store(
                    &format!("{prefix}.fetch[{name}].fetch_store"),
                    &entry.fetch_store,
                );
                // `oci.cas_store` is optional; when absent it defaults to
                // `fetch_store` (already checked above), so only validate it
                // when explicitly present.
                if let Some(oci) = &entry.oci
                    && let Some(cas_store) = &oci.cas_store
                {
                    checker.store(&format!("{prefix}.fetch[{name}].oci.cas_store"), cas_store);
                }
            }
            for entry in services.push.iter().flatten() {
                let name = &entry.instance_name;
                checker.store(
                    &format!("{prefix}.push[{name}].push_store"),
                    &entry.push_store,
                );
            }
            for entry in services.nix_cache.iter().flatten() {
                let name = &entry.instance_name;
                checker.store(
                    &format!("{prefix}.nix_cache[{name}].cas_store"),
                    &entry.cas_store,
                );
                checker.store(
                    &format!("{prefix}.nix_cache[{name}].path_info_store"),
                    &entry.path_info_store,
                );
                checker.store(
                    &format!("{prefix}.nix_cache[{name}].alias_store"),
                    &entry.alias_store,
                );
            }
            if let Some(worker_api) = &services.worker_api {
                checker.scheduler(
                    &format!("{prefix}.worker_api.scheduler"),
                    &worker_api.scheduler,
                );
            }
            if let Some(bep) = &services.experimental_bep {
                checker.store(&format!("{prefix}.experimental_bep.store"), &bep.store);
            }
        }

        // Resolve `ref_store` references reachable from the wired-up stores.
        if checker.check_stores {
            let store_specs: HashMap<&str, &StoreSpec> = self
                .stores
                .iter()
                .map(|store| (store.name.as_str(), &store.spec))
                .collect();
            checker.walk_used_stores(&store_specs);
        }

        if checker.problems.is_empty() {
            Ok(())
        } else {
            Err(make_err!(
                Code::InvalidArgument,
                "configuration reference validation failed:\n  - {}",
                checker.problems.join("\n  - ")
            ))
        }
    }
}

/// Recursively collects every `ref_store` name that appears anywhere inside a
/// single [`StoreSpec`] tree (the wrapper stores nest other `StoreSpec`s). The
/// match is exhaustive on purpose so a newly added wrapping store forces this
/// to be revisited.
fn collect_ref_store_names<'a>(spec: &'a StoreSpec, out: &mut Vec<&'a StoreRefName>) {
    match spec {
        StoreSpec::RefStore(ref_spec) => out.push(&ref_spec.name),
        StoreSpec::CacheMetrics(inner) => collect_ref_store_names(&inner.backend, out),
        StoreSpec::Verify(inner) => collect_ref_store_names(&inner.backend, out),
        StoreSpec::CompletenessChecking(inner) => {
            collect_ref_store_names(&inner.backend, out);
            collect_ref_store_names(&inner.cas_store, out);
        }
        StoreSpec::Compression(inner) => collect_ref_store_names(&inner.backend, out),
        StoreSpec::Dedup(inner) => {
            collect_ref_store_names(&inner.index_store, out);
            collect_ref_store_names(&inner.content_store, out);
        }
        StoreSpec::ExistenceCache(inner) => collect_ref_store_names(&inner.backend, out),
        StoreSpec::FastSlow(inner) => {
            collect_ref_store_names(&inner.fast, out);
            collect_ref_store_names(&inner.slow, out);
        }
        StoreSpec::Shard(inner) => {
            for shard in &inner.stores {
                collect_ref_store_names(&shard.store, out);
            }
        }
        StoreSpec::SizePartitioning(inner) => {
            collect_ref_store_names(&inner.lower_store, out);
            collect_ref_store_names(&inner.upper_store, out);
        }
        // Leaf stores and stores whose backends are concrete (non-`StoreSpec`)
        // specs carry no nested `ref_store` names.
        StoreSpec::Memory(_)
        | StoreSpec::ExperimentalCloudObjectStore(_)
        | StoreSpec::OntapS3ExistenceCache(_)
        | StoreSpec::Filesystem(_)
        | StoreSpec::Grpc(_)
        | StoreSpec::RedisStore(_)
        | StoreSpec::Noop(_)
        | StoreSpec::ExperimentalMongo(_) => {}
    }
}

/// Accumulates unresolved-reference problems while walking a [`CasConfig`].
struct ReferenceChecker<'a> {
    store_names: HashSet<&'a str>,
    scheduler_names: HashSet<&'a str>,
    check_stores: bool,
    check_schedulers: bool,
    problems: Vec<String>,
    /// Declared store names reached from a worker/service, used to seed the
    /// `ref_store` reachability walk.
    used_stores: Vec<&'a str>,
}

impl<'a> ReferenceChecker<'a> {
    /// Records that `name` (found at `path`) must resolve to a declared store.
    fn store(&mut self, path: &str, name: &'a str) {
        if !self.check_stores {
            return;
        }
        if self.store_names.contains(name) {
            self.used_stores.push(name);
        } else {
            self.problems
                .push(format!("{path} references undefined store '{name}'"));
        }
    }

    /// Records that `name` (found at `path`) must resolve to a declared
    /// scheduler.
    fn scheduler(&mut self, path: &str, name: &str) {
        if !self.check_schedulers {
            return;
        }
        if !self.scheduler_names.contains(name) {
            self.problems
                .push(format!("{path} references undefined scheduler '{name}'"));
        }
    }

    /// Walks the transitive closure of stores reachable from
    /// [`Self::used_stores`], resolving each nested `ref_store` against the
    /// declared store set. A visited set bounds the walk against `ref_store`
    /// cycles.
    fn walk_used_stores(&mut self, store_specs: &HashMap<&'a str, &'a StoreSpec>) {
        let mut visited: HashSet<&'a str> = HashSet::new();
        while let Some(name) = self.used_stores.pop() {
            if !visited.insert(name) {
                continue;
            }
            let Some(&spec) = store_specs.get(name) else {
                continue;
            };
            let mut refs: Vec<&'a StoreRefName> = Vec::new();
            collect_ref_store_names(spec, &mut refs);
            for ref_name in refs {
                if self.store_names.contains(ref_name.as_str()) {
                    self.used_stores.push(ref_name.as_str());
                } else {
                    self.problems.push(format!(
                        "store '{name}' references undefined store '{ref_name}'"
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----------------------------------------------------------------------
    // Golden vectors, generated with the local `nix` CLI (nix (Nix) 2.34.7).
    // Each vector documents a wire/key format that the `NixCacheConfig` doc
    // comments promise, with the exact command used to produce it.
    // ----------------------------------------------------------------------

    /// Deterministic store path (reproducible on any machine), generated
    /// with:
    /// ```sh
    /// nix eval --raw --expr \
    ///   '"${builtins.toFile "example.txt" "nativelink nix-cache golden vector\n"}"'
    /// ```
    const GOLDEN_STORE_PATH: &str = "/nix/store/5b7qqpfva0sj45yail8a5x0052vv3pjm-example.txt";

    /// `sha256(nar)` of the NAR serialization of `GOLDEN_STORE_PATH` (the
    /// hash half of the `DigestInfo` key in `cas_store`), generated with:
    /// ```sh
    /// nix nar pack /nix/store/5b7qqpfva0sj45yail8a5x0052vv3pjm-example.txt | sha256sum
    /// ```
    const GOLDEN_NAR_SHA256_HEX: &str =
        "877a42f22e7f9e476abe1c8db48828dc26b1916d92cedfc15e77324978d7488e";

    /// Same NAR hash in `nixbase32`, as it appears in a `narinfo`
    /// `NarHash: sha256:<...>` line, generated with:
    /// ```sh
    /// nix nar pack /nix/store/5b7qqpfva0sj45yail8a5x0052vv3pjm-example.txt > example.nar
    /// nix hash file --type sha256 --base32 example.nar
    /// ```
    const GOLDEN_NAR_SHA256_NIXBASE32: &str =
        "13j8sxw4jckpbv0xzkljdn8v29nw524b938wprm4g7kz5vr44yl7";

    /// `nar_size` in bytes of the same NAR (the size half of the
    /// `DigestInfo` key in `cas_store`), generated with:
    /// ```sh
    /// nix nar pack /nix/store/5b7qqpfva0sj45yail8a5x0052vv3pjm-example.txt | wc -c
    /// ```
    const GOLDEN_NAR_SIZE: u64 = 152;

    /// Contents of a file suitable for `signing_key_files`. Key generation
    /// is random, so this output was generated once and frozen here, with:
    /// ```sh
    /// nix key generate-secret --key-name nix-cache.example.org-1
    /// ```
    const GOLDEN_SIGNING_SECRET_KEY: &str = "nix-cache.example.org-1:+Qh4p1yd65B2kXerqAgjiK3ioUqruN8poSHRBjH4EvRnz/8zr0GPu1kjKaIeOTbHznDkpLxTsUL1LONeOtGSHQ==";

    /// Matching public key for clients' `trusted-public-keys`, generated
    /// from the frozen secret key above with:
    /// ```sh
    /// nix key convert-secret-to-public < nix-cache.example.org-1.key
    /// ```
    const GOLDEN_SIGNING_PUBLIC_KEY: &str =
        "nix-cache.example.org-1:Z8//M69Bj7tZIymiHjk2x85w5KS8U7FC9SzjXjrRkh0=";

    /// The `nixbase32` alphabet: base32 without `e`, `o`, `u` and `t`.
    const NIXBASE32_ALPHABET: &str = "0123456789abcdfghijklmnpqrsvwxyz";

    fn parse_services(json5: &str) -> ServicesConfig {
        serde_json5::from_str(json5).expect("valid ServicesConfig json5")
    }

    #[test]
    fn nix_cache_minimal_config_applies_defaults() {
        let services = parse_services(
            r#"{
                nix_cache: [{
                    instance_name: "main",
                    cas_store: "NIX_NAR_STORE",
                    path_info_store: "NIX_PATH_INFO_STORE",
                    alias_store: "NIX_ALIAS_STORE",
                }],
            }"#,
        );
        let nix_cache = services.nix_cache.expect("nix_cache service is configured");
        assert_eq!(nix_cache.len(), 1);
        let instance = &nix_cache[0];
        assert_eq!(instance.instance_name, "main");
        assert_eq!(instance.cas_store, "NIX_NAR_STORE");
        assert_eq!(instance.path_info_store, "NIX_PATH_INFO_STORE");
        assert_eq!(instance.alias_store, "NIX_ALIAS_STORE");
        assert_eq!(instance.path, None);
        assert_eq!(instance.store_dir, "/nix/store");
        assert_eq!(instance.priority, 40);
        assert!(instance.want_mass_query);
        assert!(instance.signing_key_files.is_empty());
        assert_eq!(instance.spool_path, None);
        assert!(!instance.read_only);
        assert!(instance.read_token_files.is_empty());
        assert!(instance.write_token_files.is_empty());
        assert_eq!(instance.serve_compression, None);
        assert_eq!(instance.compression_level, None);
        // Hardening limits default generously so real workloads are
        // unaffected.
        assert_eq!(instance.max_nar_size_bytes, 32 * 1024 * 1024 * 1024);
        assert_eq!(instance.max_concurrent_nar_streams, 256);
        assert_eq!(instance.max_concurrent_transcodes, 8);
        assert_eq!(instance.nar_upload_idle_timeout_s, 60);
    }

    #[test]
    fn nix_cache_full_config_round_trips() {
        let services = parse_services(
            r#"{
                nix_cache: [{
                    instance_name: "public",
                    cas_store: "NIX_NAR_STORE",
                    path_info_store: "NIX_PATH_INFO_STORE",
                    alias_store: "NIX_ALIAS_STORE",
                    path: "/nix/public",
                    store_dir: "/nix/store",
                    priority: 30,
                    want_mass_query: false,
                    signing_key_files: ["/etc/nix/keys/nix-cache.example.org-1.key"],
                    read_only: true,
                    read_token_files: ["/etc/nativelink/nix-read.token"],
                    write_token_files: ["/etc/nativelink/nix-write.token"],
                    serve_compression: "zstd",
                    compression_level: 19,
                    max_nar_size_bytes: "8GB",
                    max_concurrent_nar_streams: 512,
                    max_concurrent_transcodes: 4,
                    nar_upload_idle_timeout_s: 120,
                }],
            }"#,
        );
        let serialized =
            serde_json::to_string(&services).expect("ServicesConfig serializes to JSON");
        let reparsed: ServicesConfig =
            serde_json5::from_str(&serialized).expect("serialized ServicesConfig reparses");
        let instance = &reparsed.nix_cache.expect("nix_cache survives round trip")[0];
        assert_eq!(instance.instance_name, "public");
        assert_eq!(instance.cas_store, "NIX_NAR_STORE");
        assert_eq!(instance.path_info_store, "NIX_PATH_INFO_STORE");
        assert_eq!(instance.alias_store, "NIX_ALIAS_STORE");
        assert_eq!(instance.path.as_deref(), Some("/nix/public"));
        assert_eq!(instance.store_dir, "/nix/store");
        assert_eq!(instance.priority, 30);
        assert!(!instance.want_mass_query);
        assert_eq!(
            instance.signing_key_files,
            vec!["/etc/nix/keys/nix-cache.example.org-1.key".to_string()]
        );
        assert!(instance.read_only);
        assert_eq!(
            instance.read_token_files,
            vec!["/etc/nativelink/nix-read.token".to_string()]
        );
        assert_eq!(
            instance.write_token_files,
            vec!["/etc/nativelink/nix-write.token".to_string()]
        );
        assert_eq!(instance.serve_compression.as_deref(), Some("zstd"));
        assert_eq!(instance.compression_level, Some(19));
        // The data-size deserializer accepts "8GB"; the rest are plain
        // numerics that survive the JSON round trip.
        assert_eq!(instance.max_nar_size_bytes, 8_000_000_000);
        assert_eq!(instance.max_concurrent_nar_streams, 512);
        assert_eq!(instance.max_concurrent_transcodes, 4);
        assert_eq!(instance.nar_upload_idle_timeout_s, 120);
    }

    #[test]
    fn nix_cache_rejects_unknown_fields() {
        // Direct deserialization honors `deny_unknown_fields`.
        let result: Result<NixCacheConfig, _> = serde_json5::from_str(
            r#"{
                cas_store: "NIX_NAR_STORE",
                path_info_store: "NIX_PATH_INFO_STORE",
                alias_store: "NIX_ALIAS_STORE",
                narinfo_compression: "xz",
            }"#,
        );
        assert!(result.is_err(), "unknown fields must be rejected");

        // Through `ServicesConfig` the entries pass through
        // `WithInstanceName`'s `#[serde(flatten)]`, which swallows
        // `deny_unknown_fields` (a serde limitation). Pin `nix_cache` to
        // whatever the existing `cas` service does there, so the two never
        // drift apart if serde changes behavior.
        let nix_cache_via_services: Result<ServicesConfig, _> = serde_json5::from_str(
            r#"{
                nix_cache: [{
                    cas_store: "NIX_NAR_STORE",
                    path_info_store: "NIX_PATH_INFO_STORE",
                    alias_store: "NIX_ALIAS_STORE",
                    narinfo_compression: "xz",
                }],
            }"#,
        );
        let cas_via_services: Result<ServicesConfig, _> =
            serde_json5::from_str(r#"{ cas: [{ cas_store: "X", narinfo_compression: "xz" }] }"#);
        assert_eq!(
            nix_cache_via_services.is_err(),
            cas_via_services.is_err(),
            "nix_cache unknown-field handling must match the cas service"
        );
    }

    #[test]
    fn nix_cache_missing_store_ref_is_rejected() {
        let result: Result<ServicesConfig, _> = serde_json5::from_str(
            r#"{
                nix_cache: [{
                    cas_store: "NIX_NAR_STORE",
                    path_info_store: "NIX_PATH_INFO_STORE",
                }],
            }"#,
        );
        assert!(result.is_err(), "alias_store is required");
    }

    #[test]
    fn nix_cache_accepts_legacy_map_format() {
        // The `nix_cache` field uses the same backcompat deserializer as
        // `cas`/`ac`/`fetch`, so the deprecated map-of-instance-name format
        // must keep parsing.
        let services = parse_services(
            r#"{
                nix_cache: {
                    "main": {
                        cas_store: "NIX_NAR_STORE",
                        path_info_store: "NIX_PATH_INFO_STORE",
                        alias_store: "NIX_ALIAS_STORE",
                    },
                },
            }"#,
        );
        let nix_cache = services.nix_cache.expect("nix_cache service is configured");
        assert_eq!(nix_cache.len(), 1);
        assert_eq!(nix_cache[0].instance_name, "main");
        assert_eq!(nix_cache[0].cas_store, "NIX_NAR_STORE");
    }

    #[test]
    fn golden_store_path_matches_default_store_dir() {
        // `store_dir` defaults to the same value `nix eval --raw --expr
        // 'builtins.storeDir'` reports on a stock installation.
        let store_dir = default_nix_store_dir();
        assert_eq!(store_dir, "/nix/store");
        let base_name = GOLDEN_STORE_PATH
            .strip_prefix("/nix/store/")
            .expect("golden store path lives under the default store dir");
        // `path_info_store` keys are the 32-character nixbase32 hash before
        // the first `-` of the store path base name.
        let (hash, name) = base_name
            .split_once('-')
            .expect("store path base name is <hash>-<name>");
        assert_eq!(hash.len(), 32);
        assert!(hash.chars().all(|c| NIXBASE32_ALPHABET.contains(c)));
        assert_eq!(name, "example.txt");
    }

    #[test]
    fn golden_nar_digest_key_shape() {
        // `cas_store` keys are `DigestInfo(sha256(nar), nar_size)`: a
        // 64-hex-character sha256 plus the byte size of the NAR.
        assert_eq!(GOLDEN_NAR_SHA256_HEX.len(), 64);
        assert!(
            GOLDEN_NAR_SHA256_HEX
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        // The `narinfo` rendering of the same 32-byte hash is 52 nixbase32
        // characters (ceil(256 / 5)).
        assert_eq!(GOLDEN_NAR_SHA256_NIXBASE32.len(), 52);
        assert!(
            GOLDEN_NAR_SHA256_NIXBASE32
                .chars()
                .all(|c| NIXBASE32_ALPHABET.contains(c))
        );
        // As the pair appears in the narinfo served for this NAR.
        assert_eq!(
            format!("NarHash: sha256:{GOLDEN_NAR_SHA256_NIXBASE32}"),
            "NarHash: sha256:13j8sxw4jckpbv0xzkljdn8v29nw524b938wprm4g7kz5vr44yl7"
        );
        assert_eq!(format!("NarSize: {GOLDEN_NAR_SIZE}"), "NarSize: 152");
    }

    #[test]
    fn golden_signing_key_file_format() {
        // Files listed in `signing_key_files` hold `<name>:<base64 keypair>`
        // where the keypair is the 64-byte ed25519 secret||public
        // concatenation (88 base64 characters). The derived public key is
        // 32 bytes (44 base64 characters) under the same name.
        let (secret_name, secret_b64) = GOLDEN_SIGNING_SECRET_KEY
            .split_once(':')
            .expect("secret key is <name>:<base64>");
        let (public_name, public_b64) = GOLDEN_SIGNING_PUBLIC_KEY
            .split_once(':')
            .expect("public key is <name>:<base64>");
        assert_eq!(secret_name, "nix-cache.example.org-1");
        assert_eq!(secret_name, public_name);
        assert_eq!(secret_b64.len(), 88);
        assert!(secret_b64.ends_with("=="));
        assert_eq!(public_b64.len(), 44);
        assert!(public_b64.ends_with('='));
        // The public key is embedded in the tail of the secret keypair:
        // base64 of bytes[32..64] re-encodes to the public key's base64.
        // (Byte-level check lands with the service implementation; here we
        // pin the textual formats the config doc comments promise.)
        assert_ne!(secret_b64, public_b64);
    }

    // ----------------------------------------------------------------------
    // `validate_references` (offline `nativelink --check`) tests.
    // ----------------------------------------------------------------------

    fn parse_cas_config(json5: &str) -> CasConfig {
        serde_json5::from_str(json5).expect("valid CasConfig json5")
    }

    /// A fully-resolved config that exercises worker, service and nested
    /// `ref_store` reference sites. `WRAPPED` wraps a `ref_store` to `CAS`,
    /// so the reachability walk must resolve it.
    const VALID_CONFIG: &str = r#"{
        stores: [
            { name: "CAS", memory: {} },
            { name: "AC", memory: {} },
            { name: "WRAPPED", fast_slow: {
                fast: { ref_store: { name: "CAS" } },
                slow: { noop: {} },
            } },
        ],
        schedulers: [{ name: "SCHED", simple: {} }],
        workers: [{
            local: {
                worker_api_endpoint: { uri: "grpc://127.0.0.1:50061" },
                cas_fast_slow_store: "WRAPPED",
                upload_action_result: { ac_store: "AC" },
                work_directory: "/tmp/work",
                platform_properties: {},
            },
        }],
        servers: [{
            listener: { http: { socket_address: "0.0.0.0:50051" } },
            services: {
                cas: [{ instance_name: "main", cas_store: "CAS" }],
                ac: [{ instance_name: "main", ac_store: "AC" }],
                execution: [{ instance_name: "main", cas_store: "CAS", scheduler: "SCHED" }],
                capabilities: [{ instance_name: "main", remote_execution: { scheduler: "SCHED" } }],
                bytestream: [{ instance_name: "main", cas_store: "CAS" }],
                worker_api: { scheduler: "SCHED" },
            },
        }],
    }"#;

    #[test]
    fn validate_references_accepts_resolved_config() {
        parse_cas_config(VALID_CONFIG)
            .validate_references()
            .expect("every reference resolves");
    }

    #[test]
    fn validate_references_flags_undefined_service_store() {
        let cfg = parse_cas_config(
            r#"{
                stores: [{ name: "CAS", memory: {} }],
                servers: [{
                    listener: { http: { socket_address: "0.0.0.0:50051" } },
                    services: { cas: [{ instance_name: "main", cas_store: "CAS_TYPO" }] },
                }],
            }"#,
        );
        let err = cfg.validate_references().unwrap_err();
        assert_eq!(err.code, Code::InvalidArgument);
        let message = err.messages.join("\n");
        assert!(
            message.contains("CAS_TYPO"),
            "error must name the bad store ref: {message}"
        );
        assert!(
            message.contains("servers[0].services.cas[main].cas_store"),
            "error must name the reference site: {message}"
        );
    }

    #[test]
    fn validate_references_flags_undefined_scheduler() {
        let cfg = parse_cas_config(
            r#"{
                stores: [{ name: "CAS", memory: {} }],
                schedulers: [{ name: "SCHED", simple: {} }],
                servers: [{
                    listener: { http: { socket_address: "0.0.0.0:50051" } },
                    services: {
                        execution: [{
                            instance_name: "main",
                            cas_store: "CAS",
                            scheduler: "SCHED_TYPO",
                        }],
                    },
                }],
            }"#,
        );
        let err = cfg.validate_references().unwrap_err();
        assert_eq!(err.code, Code::InvalidArgument);
        let message = err.messages.join("\n");
        assert!(
            message.contains("SCHED_TYPO"),
            "error must name the bad scheduler ref: {message}"
        );
        assert!(
            message.contains("scheduler"),
            "error must name the reference site: {message}"
        );
    }

    #[test]
    fn validate_references_flags_undefined_worker_store() {
        let cfg = parse_cas_config(
            r#"{
                stores: [{ name: "CAS", memory: {} }],
                workers: [{
                    local: {
                        worker_api_endpoint: { uri: "grpc://127.0.0.1:50061" },
                        cas_fast_slow_store: "WORKER_TYPO",
                        work_directory: "/tmp/work",
                        platform_properties: {},
                    },
                }],
                servers: [],
            }"#,
        );
        let err = cfg.validate_references().unwrap_err();
        let message = err.messages.join("\n");
        assert!(
            message.contains("WORKER_TYPO")
                && message.contains("workers[0].local.cas_fast_slow_store"),
            "error must name the bad worker store ref and site: {message}"
        );
    }

    #[test]
    fn validate_references_flags_nested_ref_store_typo() {
        // `WRAPPED` is wired to a service, so its nested `ref_store` typo must
        // be resolved and flagged via the reachability walk.
        let cfg = parse_cas_config(
            r#"{
                stores: [
                    { name: "CAS", memory: {} },
                    { name: "WRAPPED", fast_slow: {
                        fast: { ref_store: { name: "CAS_TYPO" } },
                        slow: { noop: {} },
                    } },
                ],
                servers: [{
                    listener: { http: { socket_address: "0.0.0.0:50051" } },
                    services: { cas: [{ instance_name: "main", cas_store: "WRAPPED" }] },
                }],
            }"#,
        );
        let err = cfg.validate_references().unwrap_err();
        let message = err.messages.join("\n");
        assert!(
            message.contains("CAS_TYPO") && message.contains("store 'WRAPPED'"),
            "error must name the nested bad ref and its owning store: {message}"
        );
    }

    #[test]
    fn validate_references_flags_duplicate_store_name() {
        let cfg = parse_cas_config(
            r#"{
                stores: [
                    { name: "CAS", memory: {} },
                    { name: "CAS", memory: {} },
                ],
                servers: [],
            }"#,
        );
        let err = cfg.validate_references().unwrap_err();
        let message = err.messages.join("\n");
        assert!(
            message.contains("duplicate store name 'CAS'"),
            "error must report the duplicate store: {message}"
        );
    }

    #[test]
    fn validate_references_flags_duplicate_scheduler_name() {
        let cfg = parse_cas_config(
            r#"{
                stores: [{ name: "CAS", memory: {} }],
                schedulers: [
                    { name: "SCHED", simple: {} },
                    { name: "SCHED", simple: {} },
                ],
                servers: [],
            }"#,
        );
        let err = cfg.validate_references().unwrap_err();
        let message = err.messages.join("\n");
        assert!(
            message.contains("duplicate scheduler name 'SCHED'"),
            "error must report the duplicate scheduler: {message}"
        );
    }

    #[test]
    fn validate_references_collects_all_problems() {
        let cfg = parse_cas_config(
            r#"{
                stores: [{ name: "CAS", memory: {} }],
                schedulers: [{ name: "SCHED", simple: {} }],
                servers: [{
                    listener: { http: { socket_address: "0.0.0.0:50051" } },
                    services: {
                        cas: [{ instance_name: "main", cas_store: "BAD_CAS" }],
                        ac: [{ instance_name: "main", ac_store: "BAD_AC" }],
                    },
                }],
            }"#,
        );
        let err = cfg.validate_references().unwrap_err();
        let message = err.messages.join("\n");
        // Reporting is not fail-fast: both bad references appear together.
        assert!(
            message.contains("BAD_CAS") && message.contains("BAD_AC"),
            "error must collect every unresolved reference: {message}"
        );
    }

    #[test]
    fn validate_references_skips_empty_store_registry() {
        // A fragment/legacy sample with no declared stores must not be
        // flagged: every reference would trivially be unresolved, which would
        // be a false positive on an otherwise-valid sample.
        let cfg = parse_cas_config(
            r#"{
                stores: [],
                servers: [{
                    listener: { http: { socket_address: "0.0.0.0:50051" } },
                    services: {
                        cas: [{ instance_name: "", cas_store: "CAS_MAIN_STORE" }],
                        execution: [{
                            instance_name: "",
                            cas_store: "WORKER_STORE",
                            scheduler: "MAIN_SCHEDULER",
                        }],
                    },
                }],
            }"#,
        );
        cfg.validate_references()
            .expect("empty registries defer to boot, not a false positive");
    }

    #[test]
    fn validate_references_ignores_unreferenced_store_internal_refs() {
        // A pure store-catalog config (no servers, no workers) may carry
        // illustrative placeholder `ref_store` names; because nothing wires
        // those stores to a consumer, their internal refs are not walked and
        // must not be flagged.
        let cfg = parse_cas_config(
            r#"{
                stores: [
                    { name: "REAL", memory: {} },
                    { name: "SHOWCASE", ref_store: { name: "PLACEHOLDER_NOT_DECLARED" } },
                ],
                servers: [],
            }"#,
        );
        cfg.validate_references()
            .expect("unreferenced catalog stores are not walked");
    }
}
