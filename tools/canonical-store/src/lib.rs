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

//! Experimental, finite, capability-backed CAS and mutable AC sink adapters.
//! No production factory, cleanup, disk quota, recovery protocol or worker bridge.

#[cfg(not(target_os = "linux"))]
compile_error!("The experimental canonical store is Linux only.");

use std::io::{Read, Write};
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_canonical_filesystem::{
    CasDirectory, DigestAlgorithm, DigestLeaf, FixedLayout, PendingWrite, ReadHandle,
    SinkDirectory, TemporaryId,
};
use nativelink_error::{Code, Error, make_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use nativelink_util::buf_channel::{
    DropCloserReadHalf, DropCloserWriteHalf, make_buf_channel_pair,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{
    DigestHasher, DigestHasherFunc, DigestHasherImpl, digest_hasher_func_from_context,
};
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::spawn_blocking;
use nativelink_util::store_trait::{RemoveCallback, StoreDriver, StoreKey, UploadSizeInfo};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Experimental finite limits, not a qualified workload or lifetime disk budget.
pub const MAX_OBJECT_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_CHUNK_BYTES: usize = 1024 * 1024;
pub const IO_BUFFER_BYTES: usize = 64 * 1024;
pub const MAX_ACTIVE_OPERATIONS: usize = 4;
pub const MAX_HAS_KEYS: usize = 64;

/// Both adapters share admission permits and retain the fixed layout's locks.
#[derive(Debug)]
pub struct CanonicalStores {
    pub cas: Arc<CanonicalCas>,
    pub sink: Arc<CanonicalSink>,
}

impl CanonicalStores {
    /// Synchronize the fixed layout before making either adapter available.
    /// Construction does not adopt data or reconstruct any execution authority.
    pub async fn from_layout(layout: Arc<FixedLayout>) -> Result<Self, Error> {
        let shared = blocking(move || {
            layout
                .sync_layout()
                .map_err(|error| namespace_error(&error))?;
            Ok(Arc::new(Shared {
                layout,
                permits: Arc::new(Semaphore::new(MAX_ACTIVE_OPERATIONS)),
                #[cfg(test)]
                hooks: tests::Hooks::default(),
            }))
        })
        .await?;
        Ok(Self {
            cas: Arc::new(CanonicalCas(Driver {
                directory: Directory::Cas(shared.layout.cas()),
                shared: shared.clone(),
            })),
            sink: Arc::new(CanonicalSink(Driver {
                directory: Directory::Sink(shared.layout.sink()),
                shared,
            })),
        })
    }
}

#[derive(Debug)]
struct Shared {
    layout: Arc<FixedLayout>,
    permits: Arc<Semaphore>,
    #[cfg(test)]
    hooks: tests::Hooks,
}

#[derive(Clone, Debug)]
enum Directory {
    Cas(CasDirectory),
    Sink(SinkDirectory),
}

impl Directory {
    const fn is_cas(&self) -> bool {
        matches!(self, Self::Cas(_))
    }

    fn read(&self, leaf: &DigestLeaf) -> Result<ReadHandle, Error> {
        match self {
            Self::Cas(directory) => directory.read(leaf),
            Self::Sink(directory) => directory.read(leaf),
        }
        .map_err(|error| namespace_error(&error))
    }

    fn temporary(&self) -> Result<PendingWrite, Error> {
        let id = TemporaryId(uuid::Uuid::new_v4().as_u128());
        match self {
            Self::Cas(directory) => directory.create_exclusive(id),
            Self::Sink(directory) => directory.create_exclusive(id),
        }
        .map_err(|error| namespace_error(&error))
    }

    fn sync(&self) -> Result<(), Error> {
        match self {
            Self::Cas(directory) => directory.sync_directories(),
            Self::Sink(directory) => directory.sync_directories(),
        }
        .map_err(|error| namespace_error(&error))
    }
}

#[derive(Clone, Debug)]
struct Driver {
    directory: Directory,
    shared: Arc<Shared>,
}

#[derive(Debug)]
pub struct CanonicalCas(Driver);
#[derive(Debug)]
pub struct CanonicalSink(Driver);

#[derive(Clone)]
struct Key {
    digest: DigestInfo,
    algorithm: DigestHasherFunc,
    leaf: DigestLeaf,
    virtual_empty: bool,
}

impl Driver {
    fn key(&self, key: &StoreKey<'_>, algorithm: DigestHasherFunc) -> Result<Key, Error> {
        let StoreKey::Digest(digest) = key else {
            return Err(make_err!(
                Code::InvalidArgument,
                "canonical store requires a digest key"
            ));
        };
        if self.directory.is_cas() && digest.size_bytes() > MAX_OBJECT_BYTES {
            return Err(make_err!(Code::ResourceExhausted, "canonical object limit"));
        }
        let virtual_empty = self.directory.is_cas() && digest.size_bytes() == 0;
        if virtual_empty && algorithm.hasher().finalize_digest() != *digest {
            return Err(make_err!(Code::InvalidArgument, "invalid empty CAS digest"));
        }
        let leaf = DigestLeaf::new(
            match algorithm {
                DigestHasherFunc::Sha256 => DigestAlgorithm::Sha256,
                DigestHasherFunc::Blake3 => DigestAlgorithm::Blake3,
            },
            **digest.packed_hash(),
            digest.size_bytes(),
        );
        Ok(Key {
            digest: *digest,
            algorithm,
            leaf,
            virtual_empty,
        })
    }

    fn permit(&self) -> Result<OwnedSemaphorePermit, Error> {
        self.shared
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| make_err!(Code::ResourceExhausted, "canonical active operation limit"))
    }

    async fn has(&self, keys: &[StoreKey<'_>], results: &mut [Option<u64>]) -> Result<(), Error> {
        if keys.len() != results.len() || keys.len() > MAX_HAS_KEYS {
            return Err(make_err!(
                Code::InvalidArgument,
                "canonical has batch arity or limit"
            ));
        }
        let algorithm = digest_hasher_func_from_context();
        let keys = keys
            .iter()
            .map(|key| self.key(key, algorithm))
            .collect::<Result<Vec<_>, _>>()?;
        let permit = self.permit()?;
        let driver = self.clone();
        // Results are installed only after the entire bounded batch succeeds.
        let values = blocking(move || {
            let _permit = permit;
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                match driver.verify(&key, None) {
                    Ok(verified) => {
                        if let Some(file) = verified.file {
                            file.sync_all().map_err(io_error)?;
                            #[cfg(test)]
                            driver.before_sync()?;
                            driver.directory.sync()?;
                        }
                        values.push(Some(verified.size));
                    }
                    Err(error) if error.code == Code::NotFound => values.push(None),
                    Err(error) => return Err(error),
                }
            }
            Ok(values)
        })
        .await?;
        results.copy_from_slice(&values);
        Ok(())
    }

    async fn update(
        &self,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        size: UploadSizeInfo,
    ) -> Result<u64, Error> {
        let key = self.key(&key, digest_hasher_func_from_context())?;
        let permit = self.permit()?;
        let driver = self.clone();
        let mut state = blocking(move || {
            Ok(WriteState {
                pending: if key.virtual_empty {
                    None
                } else {
                    Some(driver.directory.temporary()?)
                },
                hasher: if driver.directory.is_cas() {
                    Some(key.algorithm.hasher())
                } else {
                    None
                },
                bytes: 0,
                key,
                driver,
                _permit: permit,
            })
        })
        .await?;
        loop {
            // Never occupy a blocking thread while waiting for a network chunk.
            let chunk = reader.recv().await?;
            if chunk.is_empty() {
                break;
            }
            if chunk.len() > MAX_CHUNK_BYTES {
                return Err(make_err!(
                    Code::ResourceExhausted,
                    "canonical incoming chunk limit"
                ));
            }
            let total = state
                .bytes
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| make_err!(Code::ResourceExhausted, "canonical object limit"))?;
            if total > MAX_OBJECT_BYTES {
                return Err(make_err!(Code::ResourceExhausted, "canonical object limit"));
            }
            if state.driver.directory.is_cas() && total > state.key.digest.size_bytes() {
                return Err(make_err!(
                    Code::InvalidArgument,
                    "CAS upload exceeds digest size"
                ));
            }
            // The closure owns the staging descriptor, locks and permit even if
            // its awaiting future is cancelled after blocking work starts.
            state = blocking(move || {
                #[cfg(test)]
                state.driver.shared.hooks.hit(tests::HookPoint::Write)?;
                for bytes in chunk.chunks(IO_BUFFER_BYTES) {
                    state
                        .pending
                        .as_mut()
                        .ok_or_else(|| make_err!(Code::InvalidArgument, "nonempty empty object"))?
                        .write_all(bytes)
                        .map_err(io_error)?;
                    if let Some(hasher) = &mut state.hasher {
                        hasher.update(bytes);
                    }
                }
                state.bytes = total;
                Ok(state)
            })
            .await?;
        }
        size.validate_size(state.bytes)?;
        if let Some(hasher) = &mut state.hasher
            && hasher.finalize_digest() != state.key.digest
        {
            return Err(make_err!(
                Code::InvalidArgument,
                "CAS upload digest mismatch"
            ));
        }
        blocking(move || state.commit()).await
    }

    async fn oneshot(&self, key: StoreKey<'_>, data: Bytes) -> Result<(), Error> {
        if data.len() as u64 > MAX_OBJECT_BYTES {
            return Err(make_err!(Code::ResourceExhausted, "canonical object limit"));
        }
        let size = data.len() as u64;
        let (mut tx, rx) = make_buf_channel_pair();
        let send = async move {
            for chunk in data.chunks(IO_BUFFER_BYTES) {
                tx.send(Bytes::copy_from_slice(chunk)).await?;
            }
            tx.send_eof()
        };
        // update is the only permit acquisition in this path.
        futures::try_join!(send, self.update(key, rx, UploadSizeInfo::ExactSize(size)))?;
        Ok(())
    }

    async fn get(
        &self,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let key = self.key(&key, digest_hasher_func_from_context())?;
        let end = length
            .map(|length| {
                offset
                    .checked_add(length)
                    .ok_or_else(|| make_err!(Code::InvalidArgument, "canonical range overflow"))
            })
            .transpose()?
            .unwrap_or(u64::MAX);
        let permit = self.permit()?;
        let driver = self.clone();
        let (verified, permit) =
            blocking(move || Ok((driver.verify(&key, Some((offset, end)))?, permit))).await?;
        for chunk in verified.bytes.chunks(IO_BUFFER_BYTES) {
            writer.send(Bytes::copy_from_slice(chunk)).await?;
        }
        writer.send_eof()?;
        // Retain the pinned read descriptor and permit through the final send.
        drop(verified);
        drop(permit);
        Ok(())
    }

    #[cfg(test)]
    fn before_sync(&self) -> Result<(), Error> {
        self.shared.hooks.hit(tests::HookPoint::DirectorySync)
    }

    fn verify(&self, key: &Key, range: Option<(u64, u64)>) -> Result<Verified, Error> {
        if key.virtual_empty {
            if range.is_some_and(|(offset, _)| offset != 0) {
                return Err(make_err!(Code::OutOfRange, "canonical read offset"));
            }
            return Ok(Verified {
                file: None,
                bytes: Vec::new(),
                size: 0,
            });
        }
        let mut file = self.directory.read(&key.leaf)?;
        let mut hasher = self.directory.is_cas().then(|| key.algorithm.hasher());
        let mut bytes = Vec::new();
        let mut size = 0u64;
        let mut buffer = vec![0; IO_BUFFER_BYTES].into_boxed_slice();
        loop {
            let count = file.read(&mut buffer).map_err(io_error)?;
            if count == 0 {
                break;
            }
            let next = size
                .checked_add(count as u64)
                .ok_or_else(|| make_err!(Code::DataLoss, "canonical stored length overflow"))?;
            // Check the actual bytes, including externally corrupted sink files.
            if next > MAX_OBJECT_BYTES
                || (self.directory.is_cas() && next > key.digest.size_bytes())
            {
                return Err(make_err!(
                    Code::DataLoss,
                    "canonical stored object exceeds limit or digest size"
                ));
            }
            if let Some(hasher) = &mut hasher {
                hasher.update(&buffer[..count]);
            }
            if let Some((start, end)) = range {
                let from = start.max(size);
                let to = end.min(next);
                if from < to {
                    let from = usize::try_from(from - size)
                        .map_err(|_| make_err!(Code::Internal, "canonical range start overflow"))?;
                    let to = usize::try_from(to - size)
                        .map_err(|_| make_err!(Code::Internal, "canonical range end overflow"))?;
                    bytes.extend_from_slice(&buffer[from..to]);
                }
            }
            size = next;
        }
        if let Some(hasher) = &mut hasher
            && hasher.finalize_digest() != key.digest
        {
            return Err(make_err!(
                Code::DataLoss,
                "canonical stored digest mismatch"
            ));
        }
        if range.is_some_and(|(start, _)| start > size) {
            return Err(make_err!(Code::OutOfRange, "canonical read offset"));
        }
        Ok(Verified {
            file: Some(file),
            bytes,
            size,
        })
    }
}

struct Verified {
    file: Option<ReadHandle>,
    bytes: Vec<u8>,
    size: u64,
}

struct WriteState {
    pending: Option<PendingWrite>,
    hasher: Option<DigestHasherImpl>,
    bytes: u64,
    key: Key,
    driver: Driver,
    _permit: OwnedSemaphorePermit,
}

impl WriteState {
    fn commit(mut self) -> Result<u64, Error> {
        let Some(pending) = self.pending.take() else {
            return Ok(self.bytes);
        };
        pending.sync_all().map_err(io_error)?;
        match &self.driver.directory {
            Directory::Cas(directory) => {
                match directory.publish(pending, &self.key.leaf) {
                    Ok(_published) => {}
                    Err(error) if error.code == "slot_exists" => {
                        // NOREPLACE preserves the existing object and the new
                        // duplicate temporary entry. Never unlink by guessed name.
                        let existing = self.driver.verify(&self.key, None)?;
                        existing
                            .file
                            .expect("nonempty CAS has a file")
                            .sync_all()
                            .map_err(io_error)?;
                    }
                    Err(error) => return Err(namespace_error(&error)),
                }
            }
            Directory::Sink(directory) => {
                directory
                    .replace(pending, &self.key.leaf)
                    .map_err(|error| namespace_error(&error))?;
            }
        }
        #[cfg(test)]
        self.driver.before_sync()?;
        self.driver.directory.sync()?;
        Ok(self.bytes)
    }
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, Error> + Send + 'static,
) -> Result<T, Error> {
    spawn_blocking!("canonical_store_io", work)
        .await
        .map_err(|_| make_err!(Code::Internal, "canonical blocking task failed"))?
}

fn io_error(_: std::io::Error) -> Error {
    make_err!(Code::Unavailable, "canonical filesystem IO failed")
}

fn namespace_error(error: &nativelink_canonical_config::Error) -> Error {
    make_err!(
        if error.code == "missing_slot" {
            Code::NotFound
        } else {
            Code::Unavailable
        },
        "canonical namespace: {}",
        error.code
    )
}

macro_rules! driver_impl {
    ($kind:ident) => {
        impl MetricsComponent for $kind {
            fn publish(
                &self,
                _: MetricKind,
                _: MetricFieldData,
            ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
                Ok(MetricPublishKnownKindData::Component)
            }
        }
        default_health_status_indicator!($kind);
        #[async_trait]
        impl StoreDriver for $kind {
            async fn post_init(self: Arc<Self>) -> Result<(), Error> {
                Ok(())
            }
            async fn has_with_results(
                self: Pin<&Self>,
                keys: &[StoreKey<'_>],
                results: &mut [Option<u64>],
            ) -> Result<(), Error> {
                self.0.has(keys, results).await
            }
            async fn update(
                self: Pin<&Self>,
                key: StoreKey<'_>,
                reader: DropCloserReadHalf,
                size: UploadSizeInfo,
            ) -> Result<u64, Error> {
                self.0.update(key, reader, size).await
            }
            async fn update_oneshot(
                self: Pin<&Self>,
                key: StoreKey<'_>,
                data: Bytes,
            ) -> Result<(), Error> {
                self.0.oneshot(key, data).await
            }
            async fn get_part(
                self: Pin<&Self>,
                key: StoreKey<'_>,
                writer: &mut DropCloserWriteHalf,
                offset: u64,
                length: Option<u64>,
            ) -> Result<(), Error> {
                self.0.get(key, writer, offset, length).await
            }
            fn inner_store(&self, _: Option<StoreKey<'_>>) -> &dyn StoreDriver {
                self
            }
            fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
                self
            }
            fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
                self
            }
            fn register_remove_callback(self: Arc<Self>, _: RemoveCallback) -> Result<(), Error> {
                Ok(())
            }
        }
    };
}
driver_impl!(CanonicalCas);
driver_impl!(CanonicalSink);

#[cfg(test)]
mod tests;
