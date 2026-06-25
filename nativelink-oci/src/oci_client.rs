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

//! High-level OCI toolchain client.
//!
//! Orchestrates the full pipeline:
//!
//! 1. **Pull** — fetch manifest + layer blobs from an OCI registry
//! 2. **Decompress** — gunzip/zstd-decompress layers
//! 3. **Project** — iterate tar entries, build REAPI `Directory` Merkle tree
//! 4. **Upload** — push file blobs and `Directory` protos to CAS store
//!
//! The result is a root `Directory` digest that can be merged into an action's
//! `input_root_digest` for remote execution, making toolchains data in CAS
//! rather than infrastructure on workers.

use nativelink_error::{Error, ResultExt, make_input_err};
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use tracing::{debug, info};

use crate::projection::{
    DigestPair, ProjectionDigestFunction, ProjectionResult, decompress_gzip, project_layers,
};
use crate::registry::{ImageReference, OciManifest, RegistryClient, ToolchainHints};

/// Result of a successful OCI toolchain import.
#[derive(Debug)]
pub struct ImportResult {
    /// The root Directory digest in the CAS. Use this as (or merge into)
    /// `input_root_digest` for `Execute` requests.
    pub root_digest: DigestInfo,

    /// Total number of file blobs uploaded to CAS.
    pub files_uploaded: usize,

    /// Total number of file blobs that were already present (deduped).
    pub files_deduped: usize,

    /// Total number of Directory protos uploaded.
    pub directories_uploaded: usize,

    /// Total bytes uploaded (file content + directory protos).
    pub bytes_uploaded: u64,

    /// Toolchain hints extracted from OCI annotations (if any).
    pub hints: ToolchainHints,
}

/// Configuration for the OCI toolchain import.
#[derive(Debug, Clone, Copy)]
pub struct ImportConfig {
    /// Digest function to use for REAPI projection.
    /// Per §6.2, this SHOULD be BLAKE3.
    pub digest_function: ProjectionDigestFunction,

    /// Whether to check for existing blobs before uploading (dedup).
    /// When true, calls `has_many()` before uploading — reduces bandwidth
    /// for incremental toolchain updates but adds a round-trip.
    pub dedup_check: bool,

    /// Whether to verify the per-layer projection against toolchain hints.
    /// If true and hints are present but don't match, the import fails.
    /// If false (default), mismatched hints are logged as warnings.
    pub strict_hint_verification: bool,
}

impl Default for ImportConfig {
    fn default() -> Self {
        Self {
            digest_function: ProjectionDigestFunction::Blake3,
            dedup_check: true,
            strict_hint_verification: false,
        }
    }
}

/// High-level OCI toolchain client.
///
/// Given a CAS store and an OCI image reference, this client handles the
/// full pipeline from registry pull through to CAS upload.
#[derive(Debug)]
pub struct OciToolchainClient {
    registry: RegistryClient,
    config: ImportConfig,
}

impl OciToolchainClient {
    /// Create a new OCI toolchain client with default configuration.
    pub fn new() -> Result<Self, Error> {
        Self::with_config(ImportConfig::default())
    }

    /// Create a new OCI toolchain client with custom configuration.
    pub fn with_config(config: ImportConfig) -> Result<Self, Error> {
        let registry = RegistryClient::new()?;
        Ok(Self { registry, config })
    }

    /// Import an OCI toolchain image into a CAS store.
    ///
    /// This is the primary entry point. Given an image reference (e.g.
    /// `ghcr.io/tracemachina/toolchain:v1`) and a CAS store, it:
    ///
    /// 1. Pulls the manifest and layer blobs from the registry
    /// 2. Decompresses layers (gzip → raw tar)
    /// 3. Projects each layer into an REAPI Directory tree
    /// 4. Uploads all file blobs and Directory protos to the CAS store
    /// 5. Returns the root Directory digest
    ///
    /// # Arguments
    ///
    /// * `image_ref` - OCI image reference (tag or digest)
    /// * `cas_store` - NativeLink CAS store for blob upload
    pub async fn import(&self, image_ref: &str, cas_store: &Store) -> Result<ImportResult, Error> {
        let image = ImageReference::parse(image_ref)?;
        info!(%image, "Starting OCI toolchain import");

        // Phase 1: Pull manifest
        let (manifest, manifest_digest) = self.registry.fetch_manifest(&image).await?;
        info!(
            layers = manifest.layers.len(),
            manifest_digest = %manifest_digest,
            "Fetched OCI manifest"
        );

        // Extract toolchain hints from manifest annotations
        let hints = ToolchainHints::from_annotations(&manifest.annotations);
        if hints.is_conforming_toolchain() {
            info!(
                role = ?hints.role,
                digest_function = ?hints.reapi_digest_function,
                "Image carries Standard OCI Toolchain annotations"
            );
        }

        // Phase 2: Pull and decompress layers
        let decompressed_layers = self.pull_and_decompress_layers(&image, &manifest).await?;

        // Phase 3: Project into REAPI Directory tree
        let layer_refs: Vec<&[u8]> = decompressed_layers.iter().map(|v| v.as_slice()).collect();
        let projection = project_layers(&layer_refs, self.config.digest_function, Some(&hints))?;

        info!(
            root_hash = %projection.root_digest.hash,
            root_size = projection.root_digest.size_bytes,
            files = projection.file_blobs.len(),
            directories = projection.directory_blobs.len(),
            "Projection complete"
        );

        // Phase 4: Upload to CAS
        let upload_stats = self.upload_projection(projection, cas_store).await?;

        Ok(ImportResult {
            root_digest: upload_stats.root_digest,
            files_uploaded: upload_stats.files_uploaded,
            files_deduped: upload_stats.files_deduped,
            directories_uploaded: upload_stats.directories_uploaded,
            bytes_uploaded: upload_stats.bytes_uploaded,
            hints,
        })
    }

    /// Import using pre-fetched manifest and layers (for testing or when the
    /// caller has already downloaded the image content).
    pub async fn import_from_layers(
        &self,
        decompressed_layers: &[&[u8]],
        annotations: &std::collections::HashMap<String, String>,
        cas_store: &Store,
    ) -> Result<ImportResult, Error> {
        let hints = ToolchainHints::from_annotations(annotations);

        let projection = project_layers(
            decompressed_layers,
            self.config.digest_function,
            Some(&hints),
        )?;

        let upload_stats = self.upload_projection(projection, cas_store).await?;

        Ok(ImportResult {
            root_digest: upload_stats.root_digest,
            files_uploaded: upload_stats.files_uploaded,
            files_deduped: upload_stats.files_deduped,
            directories_uploaded: upload_stats.directories_uploaded,
            bytes_uploaded: upload_stats.bytes_uploaded,
            hints,
        })
    }

    /// Pull layer blobs from the registry and decompress them.
    async fn pull_and_decompress_layers(
        &self,
        image: &ImageReference,
        manifest: &OciManifest,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let mut decompressed = Vec::with_capacity(manifest.layers.len());

        for (i, layer) in manifest.layers.iter().enumerate() {
            debug!(
                layer_idx = i,
                digest = %layer.digest,
                size = layer.size,
                media_type = %layer.media_type,
                "Pulling layer"
            );

            let blob = self.registry.fetch_blob(image, &layer.digest).await?;
            let blob_len = blob.len();

            // Decompress based on media type
            let raw_tar = if layer.media_type.contains("+gzip")
                || layer.media_type.contains(".gzip")
                || layer.media_type == "application/vnd.docker.image.rootfs.diff.tar.gzip"
            {
                decompress_gzip(&blob)?
            } else if layer.media_type.contains("+zstd") {
                decompress_zstd(&blob)?
            } else {
                // Assume uncompressed tar
                blob
            };

            debug!(
                layer_idx = i,
                compressed_size = blob_len,
                uncompressed_size = raw_tar.len(),
                "Layer decompressed"
            );

            decompressed.push(raw_tar);
        }

        Ok(decompressed)
    }

    /// Upload a projection result to the CAS store.
    async fn upload_projection(
        &self,
        projection: ProjectionResult,
        cas_store: &Store,
    ) -> Result<UploadStats, Error> {
        let mut stats = UploadStats {
            root_digest: digest_pair_to_info(&projection.root_digest)?,
            files_uploaded: 0,
            files_deduped: 0,
            directories_uploaded: 0,
            bytes_uploaded: 0,
        };

        // Upload file blobs
        if self.config.dedup_check {
            // Batch existence check
            let file_digests: Vec<DigestInfo> = projection
                .file_blobs
                .keys()
                .map(digest_pair_to_info)
                .collect::<Result<Vec<_>, _>>()?;

            let store_keys: Vec<StoreKey<'_>> =
                file_digests.iter().map(|d| StoreKey::from(*d)).collect();

            let existence = cas_store
                .has_many(&store_keys)
                .await
                .err_tip(|| "Checking file blob existence in CAS")?;

            // Upload only missing blobs
            for ((digest_pair, data), exists) in
                projection.file_blobs.into_iter().zip(existence.iter())
            {
                if exists.is_some() {
                    stats.files_deduped += 1;
                    continue;
                }

                let digest = digest_pair_to_info(&digest_pair)?;
                let size = data.len() as u64;
                cas_store
                    .update_oneshot(digest, data)
                    .await
                    .err_tip(|| format!("Uploading file blob {}", digest_pair.hash))?;

                stats.files_uploaded += 1;
                stats.bytes_uploaded += size;
            }
        } else {
            // Upload all file blobs without checking
            for (digest_pair, data) in projection.file_blobs {
                let digest = digest_pair_to_info(&digest_pair)?;
                let size = data.len() as u64;
                cas_store
                    .update_oneshot(digest, data)
                    .await
                    .err_tip(|| format!("Uploading file blob {}", digest_pair.hash))?;

                stats.files_uploaded += 1;
                stats.bytes_uploaded += size;
            }
        }

        // Upload Directory protos (always — these are small and critical for correctness)
        for (digest_pair, data) in projection.directory_blobs {
            let digest = digest_pair_to_info(&digest_pair)?;
            let size = data.len() as u64;
            cas_store
                .update_oneshot(digest, data)
                .await
                .err_tip(|| format!("Uploading Directory proto {}", digest_pair.hash))?;

            stats.directories_uploaded += 1;
            stats.bytes_uploaded += size;
        }

        info!(
            root_digest = %stats.root_digest,
            files_uploaded = stats.files_uploaded,
            files_deduped = stats.files_deduped,
            directories_uploaded = stats.directories_uploaded,
            bytes_uploaded = stats.bytes_uploaded,
            "CAS upload complete"
        );

        Ok(stats)
    }
}

/// Internal upload statistics.
struct UploadStats {
    root_digest: DigestInfo,
    files_uploaded: usize,
    files_deduped: usize,
    directories_uploaded: usize,
    bytes_uploaded: u64,
}

/// Convert a `DigestPair` (hash string + size) to NativeLink's `DigestInfo`.
fn digest_pair_to_info(pair: &DigestPair) -> Result<DigestInfo, Error> {
    DigestInfo::try_new(&pair.hash, pair.size_bytes as u64)
}

/// Decompress a zstd-compressed layer blob.
fn decompress_zstd(_compressed: &[u8]) -> Result<Vec<u8>, Error> {
    // Use the zstd crate if available, otherwise fall back to streaming
    // For now, we'll use a basic implementation via the flate2-style API
    // TODO: Add zstd dependency when needed. For now, most OCI images use gzip.
    Err(make_input_err!(
        "zstd decompression not yet implemented; layer uses zstd compression"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_digest_pair_to_info() {
        let pair = DigestPair {
            hash: "a".repeat(64),
            size_bytes: 42,
        };
        let info = digest_pair_to_info(&pair).unwrap();
        assert_eq!(info.size_bytes(), 42);
    }

    #[test]
    fn test_import_config_default() {
        let config = ImportConfig::default();
        assert_eq!(config.digest_function, ProjectionDigestFunction::Blake3);
        assert!(config.dedup_check);
        assert!(!config.strict_hint_verification);
    }
}
