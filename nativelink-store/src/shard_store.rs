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

use core::hash::Hasher;
use core::ops::BitXor;
use core::pin::Pin;
use core::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use core::time::Duration;
use std::hash::DefaultHasher;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::try_join_all;
use futures::stream::{FuturesUnordered, TryStreamExt};
use nativelink_config::stores::ShardSpec;
use nativelink_error::{Code, Error, ResultExt, error_if};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    RemoveCallback, Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use tokio::time::Instant;

/// Number of consecutive connectivity failures after which a shard is
/// considered suspect and skipped as the first read target.
const SUSPECT_FAILURE_THRESHOLD: u32 = 3;

/// How long a suspect shard is skipped on the read path before it is
/// probed again.
const SUSPECT_COOLDOWN: Duration = Duration::from_secs(5);

/// Errors that indicate the shard itself is unreachable (as opposed to a
/// definitive answer like `NotFound`), making read failover safe.
const fn is_connectivity_error(err: &Error) -> bool {
    matches!(err.code, Code::Unavailable | Code::DeadlineExceeded)
}

/// Read-path health state for one shard. Writes never consult this: a
/// blob's home shard is fixed by the ring, so writes always target it.
#[derive(Debug, Default)]
struct ShardHealth {
    /// Consecutive connectivity failures observed on this shard.
    consecutive_failures: AtomicU32,
    /// Nanoseconds (since `ShardStore` creation) of the most recent
    /// connectivity failure. Zero means "never failed".
    last_failure_nanos: AtomicU64,
}

#[derive(Debug, MetricsComponent)]
struct StoreAndWeight {
    #[metric(help = "The weight of the store")]
    weight: u32,
    #[metric(help = "The underlying store")]
    store: Store,
    health: ShardHealth,
}

#[derive(Debug, MetricsComponent)]
pub struct ShardStore {
    // The weights will always be in ascending order a specific store is chosen based on
    // the hash of the key hash that is nearest-binary searched using the u32 as the index.
    #[metric(
        group = "stores",
        help = "The weights and stores that are used to determine which store to use"
    )]
    weights_and_stores: Vec<StoreAndWeight>,
    /// Anchor for `ShardHealth::last_failure_nanos` timestamps.
    created_at: Instant,
}

impl ShardStore {
    pub fn new(spec: &ShardSpec, stores: Vec<Store>) -> Result<Arc<Self>, Error> {
        error_if!(
            spec.stores.len() != stores.len(),
            "Config shards do not match stores length"
        );
        error_if!(
            spec.stores.is_empty(),
            "ShardStore must have at least one store"
        );
        let total_weight: u64 = spec
            .stores
            .iter()
            .map(|shard_config| u64::from(shard_config.weight.unwrap_or(1)))
            .sum();
        let mut weights: Vec<u32> = spec
            .stores
            .iter()
            .map(|shard_config| {
                u32::try_from(
                    u64::from(u32::MAX) * u64::from(shard_config.weight.unwrap_or(1))
                        / total_weight,
                )
                .unwrap_or(u32::MAX)
            })
            .scan(0, |state, weight| {
                *state += weight;
                Some(*state)
            })
            .collect();
        // Our last item should always be the max.
        *weights.last_mut().unwrap() = u32::MAX;
        Ok(Arc::new(Self {
            weights_and_stores: weights
                .into_iter()
                .zip(stores)
                .map(|(weight, store)| StoreAndWeight {
                    weight,
                    store,
                    health: ShardHealth::default(),
                })
                .collect(),
            created_at: Instant::now(),
        }))
    }

    fn now_nanos(&self) -> u64 {
        u64::try_from(self.created_at.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    /// A shard is suspect once it has accumulated enough consecutive
    /// connectivity failures and its cooldown has not yet elapsed. After
    /// the cooldown it is probed again (treated as healthy).
    fn is_suspect(&self, store_idx: usize) -> bool {
        let health = &self.weights_and_stores[store_idx].health;
        if health.consecutive_failures.load(Ordering::Relaxed) < SUSPECT_FAILURE_THRESHOLD {
            return false;
        }
        let last_failure_nanos = health.last_failure_nanos.load(Ordering::Relaxed);
        let cooldown_nanos = u64::try_from(SUSPECT_COOLDOWN.as_nanos()).unwrap_or(u64::MAX);
        self.now_nanos().saturating_sub(last_failure_nanos) < cooldown_nanos
    }

    fn note_success(&self, store_idx: usize) {
        self.weights_and_stores[store_idx]
            .health
            .consecutive_failures
            .store(0, Ordering::Relaxed);
    }

    fn note_connectivity_failure(&self, store_idx: usize) {
        let health = &self.weights_and_stores[store_idx].health;
        health.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        health
            .last_failure_nanos
            .store(self.now_nanos(), Ordering::Relaxed);
    }

    /// Runs `has_with_results` against the ring starting at `primary_idx`,
    /// failing over in ring order on connectivity errors (each shard is
    /// tried at most once). Suspect shards are skipped unless every shard
    /// is suspect, in which case the primary is probed directly.
    async fn has_on_ring(
        &self,
        primary_idx: usize,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        let num_stores = self.weights_and_stores.len();
        let mut last_err = None;
        let mut attempted_any = false;
        for attempt in 0..num_stores {
            let store_idx = (primary_idx + attempt) % num_stores;
            if self.is_suspect(store_idx) {
                continue;
            }
            attempted_any = true;
            let store = &self.weights_and_stores[store_idx].store;
            match store.has_with_results(keys, results).await {
                Ok(()) => {
                    self.note_success(store_idx);
                    return Ok(());
                }
                Err(err) if is_connectivity_error(&err) => {
                    self.note_connectivity_failure(store_idx);
                    results.fill(None);
                    last_err = Some(err);
                }
                Err(err) => {
                    return Err(err)
                        .err_tip(|| "In ShardStore::has_with_results() for store {store_idx}");
                }
            }
        }
        if !attempted_any {
            // Every shard is suspect: probe the primary rather than fail
            // without touching the network.
            let store = &self.weights_and_stores[primary_idx].store;
            match store.has_with_results(keys, results).await {
                Ok(()) => {
                    self.note_success(primary_idx);
                    return Ok(());
                }
                Err(err) => {
                    if is_connectivity_error(&err) {
                        self.note_connectivity_failure(primary_idx);
                    }
                    return Err(err)
                        .err_tip(|| "In ShardStore::has_with_results() for store {store_idx}");
                }
            }
        }
        Err(last_err.unwrap_or_else(|| {
            Error::new(
                Code::Unavailable,
                "All shards unavailable in ShardStore::has_with_results()".to_string(),
            )
        }))
    }

    fn get_store_index(&self, store_key: &StoreKey) -> usize {
        let key = match store_key {
            StoreKey::Digest(digest) => {
                // Quote from std primitive array documentation:
                //     Array’s try_from(slice) implementations (and the corresponding slice.try_into()
                //     array implementations) succeed if the input slice length is the same as the result
                //     array length. They optimize especially well when the optimizer can easily determine
                //     the slice length, e.g. <[u8; 4]>::try_from(&slice[4..8]).unwrap(). Array implements
                //     TryFrom returning.
                let size_bytes = digest.size_bytes().to_le_bytes();
                0.bitxor(u32::from_le_bytes(
                    digest.packed_hash()[0..4].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[4..8].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[8..12].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[12..16].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[16..20].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[20..24].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[24..28].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(
                    digest.packed_hash()[28..32].try_into().unwrap(),
                ))
                .bitxor(u32::from_le_bytes(size_bytes[0..4].try_into().unwrap()))
                .bitxor(u32::from_le_bytes(size_bytes[4..8].try_into().unwrap()))
            }
            StoreKey::Str(s) => {
                let mut hasher = DefaultHasher::new();
                hasher.write(s.as_bytes());
                let key_u64 = hasher.finish();
                (key_u64 >> 32) as u32 // We only need the top 32 bits.
            }
        };
        self.weights_and_stores
            .binary_search_by_key(&key, |item| item.weight)
            .unwrap_or_else(|index| index)
    }

    fn get_store(&self, key: &StoreKey) -> &Store {
        let index = self.get_store_index(key);
        &self.weights_and_stores[index].store
    }
}

#[async_trait]
impl StoreDriver for ShardStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        let mut futures = vec![];
        for store_and_weight in &self.weights_and_stores {
            futures.push(store_and_weight.store.clone().into_inner().post_init());
        }
        try_join_all(futures).await?;
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        type KeyIdxVec = Vec<usize>;
        type KeyVec<'a> = Vec<StoreKey<'a>>;

        if keys.len() == 1 {
            // Hot path: It is very common to lookup only one key.
            let store_idx = self.get_store_index(&keys[0]);
            return self.has_on_ring(store_idx, keys, results).await;
        }
        let mut keys_for_store: Vec<(KeyIdxVec, KeyVec)> = self
            .weights_and_stores
            .iter()
            .map(|_| (Vec::new(), Vec::new()))
            .collect();
        // Bucket each key into the store that it belongs to.
        keys.iter()
            .enumerate()
            .map(|(key_idx, key)| (key, key_idx, self.get_store_index(key)))
            .for_each(|(key, key_idx, store_idx)| {
                keys_for_store[store_idx].0.push(key_idx);
                keys_for_store[store_idx].1.push(key.borrow());
            });

        // Build all our futures for each store.
        let mut future_stream: FuturesUnordered<_> = keys_for_store
            .into_iter()
            .enumerate()
            .map(|(store_idx, (key_idxs, keys))| async move {
                let mut inner_results = vec![None; keys.len()];
                self.has_on_ring(store_idx, &keys, &mut inner_results)
                    .await?;
                Result::<_, Error>::Ok((key_idxs, inner_results))
            })
            .collect();

        // Wait for all the stores to finish and populate our output results.
        while let Some((key_idxs, inner_results)) = future_stream.try_next().await? {
            for (key_idx, inner_result) in key_idxs.into_iter().zip(inner_results) {
                results[key_idx] = inner_result;
            }
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<u64, Error> {
        let store = self.get_store(&key);
        store
            .update(key, reader, size_info)
            .await
            .err_tip(|| "In ShardStore::update()")
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        let primary_idx = self.get_store_index(&key);
        let num_stores = self.weights_and_stores.len();
        let bytes_written_start = writer.get_bytes_written();
        let mut last_connectivity_err = None;
        let mut last_other_err = None;
        let mut attempted_any = false;
        for attempt in 0..num_stores {
            let store_idx = (primary_idx + attempt) % num_stores;
            if self.is_suspect(store_idx) {
                continue;
            }
            attempted_any = true;
            let store = &self.weights_and_stores[store_idx].store;
            match store
                .get_part(key.borrow(), &mut *writer, offset, length)
                .await
            {
                Ok(()) => {
                    self.note_success(store_idx);
                    return Ok(());
                }
                Err(err) if is_connectivity_error(&err) => {
                    self.note_connectivity_failure(store_idx);
                    // Failover is only safe if nothing reached the client.
                    if writer.get_bytes_written() != bytes_written_start {
                        return Err(err).err_tip(|| "In ShardStore::get_part()");
                    }
                    last_connectivity_err = Some(err);
                }
                Err(err) if err.code == Code::NotFound && store_idx != primary_idx => {
                    // A failover shard legitimately may not have the blob;
                    // keep looking, but never let it mask a connectivity
                    // error from the shard that owns the key.
                    self.note_success(store_idx);
                    last_other_err = Some(err);
                }
                Err(err) => return Err(err).err_tip(|| "In ShardStore::get_part()"),
            }
        }
        if !attempted_any {
            // Every shard is suspect: probe the primary rather than fail
            // without touching the network.
            let store = &self.weights_and_stores[primary_idx].store;
            let result = store.get_part(key, writer, offset, length).await;
            match &result {
                Ok(()) => self.note_success(primary_idx),
                Err(err) if is_connectivity_error(err) => {
                    self.note_connectivity_failure(primary_idx);
                }
                Err(_) => {}
            }
            return result.err_tip(|| "In ShardStore::get_part()");
        }
        Err(last_connectivity_err
            .or(last_other_err)
            .unwrap_or_else(|| {
                Error::new(
                    Code::Unavailable,
                    "All shards unavailable in ShardStore::get_part()".to_string(),
                )
            }))
        .err_tip(|| "In ShardStore::get_part()")
    }

    fn inner_store(&self, key: Option<StoreKey>) -> &'_ dyn StoreDriver {
        let Some(key) = key else {
            return self;
        };
        let index = self.get_store_index(&key);
        self.weights_and_stores[index].store.inner_store(Some(key))
    }

    fn as_any<'a>(&'a self) -> &'a (dyn core::any::Any + Sync + Send + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }

    fn register_remove_callback(self: Arc<Self>, callback: RemoveCallback) -> Result<(), Error> {
        for store in &self.weights_and_stores {
            store.store.register_remove_callback(callback.clone())?;
        }
        Ok(())
    }
}

default_health_status_indicator!(ShardStore);
