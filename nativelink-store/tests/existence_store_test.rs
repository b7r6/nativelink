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

use core::time::Duration;

use mock_instant::thread_local::MockClock;
use nativelink_config::stores::{
    EvictionPolicy, ExistenceCacheSpec, MemorySpec, NoopSpec, StoreSpec,
};
use nativelink_error::{Error, ResultExt};
use nativelink_macro::nativelink_test;
use nativelink_store::existence_cache_store::ExistenceCacheStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::store_trait::{Store, StoreLike};
use pretty_assertions::assert_eq;

const VALID_HASH1: &str = "0123456789abcdef000000000000000000010000000000000123456789abcdef";

#[nativelink_test]
async fn simple_exist_cache_test() -> Result<(), Error> {
    const VALUE: &str = "123";
    let spec = ExistenceCacheSpec {
        backend: StoreSpec::Noop(NoopSpec::default()), // Note: Not used.
        eviction_policy: Option::default(),
    };
    let inner_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let store = ExistenceCacheStore::new(&spec, inner_store.clone());

    let digest = DigestInfo::try_new(VALID_HASH1, 3).unwrap();
    store
        .update_oneshot(digest, VALUE.into())
        .await
        .err_tip(|| "Failed to update store")?;
    store.remove_from_cache(&digest).await;

    assert!(
        !store.exists_in_cache(&digest).await,
        "Expected digest to not exist in cache"
    );

    assert_eq!(
        store
            .has(digest)
            .await
            .err_tip(|| "Failed to check store")?,
        Some(VALUE.len() as u64),
        "Expected digest to exist in store"
    );

    assert!(
        store.exists_in_cache(&digest).await,
        "Expected digest to exist in cache in direct check"
    );
    Ok(())
}

#[nativelink_test]
async fn update_flags_existence_cache_test() -> Result<(), Error> {
    const VALUE: &str = "123";
    let spec = ExistenceCacheSpec {
        backend: StoreSpec::Noop(NoopSpec::default()),
        eviction_policy: Option::default(),
    };
    let inner_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let store = ExistenceCacheStore::new(&spec, inner_store.clone());

    let digest = DigestInfo::try_new(VALID_HASH1, 3).unwrap();
    store
        .update_oneshot(digest, VALUE.into())
        .await
        .err_tip(|| "Failed to update store")?;

    assert!(
        store.exists_in_cache(&digest).await,
        "Expected digest to exist in cache"
    );
    Ok(())
}

#[nativelink_test]
async fn get_part_caches_if_exact_size_set() -> Result<(), Error> {
    const VALUE: &str = "123";
    let spec = ExistenceCacheSpec {
        backend: StoreSpec::Noop(NoopSpec::default()),
        eviction_policy: Option::default(),
    };
    let inner_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let digest = DigestInfo::try_new(VALID_HASH1, 3).unwrap();
    inner_store
        .update_oneshot(digest, VALUE.into())
        .await
        .err_tip(|| "Failed to update store")?;
    let store = ExistenceCacheStore::new(&spec, inner_store.clone());

    drop(
        store
            .get_part_unchunked(digest, 0, None)
            .await
            .err_tip(|| "Expected get_part to succeed")?,
    );

    assert!(
        store.exists_in_cache(&digest).await,
        "Expected digest to exist in cache"
    );
    Ok(())
}

// Regression test for: https://github.com/TraceMachina/nativelink/issues/1199.
#[nativelink_test]
async fn ensure_has_requests_do_let_evictions_happen() -> Result<(), Error> {
    const VALUE: &str = "123";
    let inner_store = MemoryStore::new(&MemorySpec::default());
    let digest = DigestInfo::try_new(VALID_HASH1, 3).unwrap();
    inner_store
        .update_oneshot(digest, VALUE.into())
        .await
        .err_tip(|| "Failed to update store")?;
    let store = ExistenceCacheStore::new_with_time(
        &ExistenceCacheSpec {
            backend: StoreSpec::Noop(NoopSpec::default()),
            eviction_policy: Some(EvictionPolicy {
                max_seconds: 0, // Explicitly set this level to "don't timeout"
                ..Default::default()
            }),
        },
        Store::new(inner_store.clone()),
        MockInstantWrapped::default(),
    );

    assert_eq!(store.has(digest).await, Ok(Some(VALUE.len() as u64)));
    MockClock::advance(Duration::from_secs(3));

    // Now that our existence cache has been populated, remove
    // it from the inner store.
    inner_store.remove_entry(digest.into()).await;

    // It should be immediately evicted from the existence cache.
    assert_eq!(store.has(digest).await, Ok(None));

    Ok(())
}

#[nativelink_test]
async fn copes_with_dropped_items() -> Result<(), Error> {
    const VALUE: &str = "123";
    let spec = ExistenceCacheSpec {
        backend: StoreSpec::Noop(NoopSpec::default()), // Note: Not used.
        eviction_policy: Option::default(),
    };
    let inner_store = Store::new(MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 1,
            ..Default::default()
        }),
    }));
    let store = ExistenceCacheStore::new(&spec, inner_store.clone());

    let digest = DigestInfo::try_new(VALID_HASH1, 3).unwrap();
    store
        .update_oneshot(digest, VALUE.into())
        .await
        .err_tip(|| "Failed to update store")?;

    let inner_store_item = inner_store.has(digest).await;
    assert!(
        inner_store_item.is_ok(),
        "Failed inner item: {inner_store_item:#?}",
    );
    let unwrapped_inner = inner_store_item.unwrap();
    assert!(
        unwrapped_inner.is_none(),
        "Failed inner item: {unwrapped_inner:#?}"
    );

    let store_item = store.has(digest).await;
    assert!(store_item.is_ok(), "Failed item: {store_item:#?}");
    let unwrapped_store = store_item.unwrap();
    assert!(
        unwrapped_store.is_none(),
        "Failed item: {unwrapped_store:#?}"
    );

    Ok(())
}

// Reproduces the pause_remove_callbacks race: the pause slot is a single
// non-refcounted Option shared by all concurrent update() calls. The first
// update to finish take()s it, ending the pause for every other in-flight
// update. A remove callback (eviction under pressure) firing inside the
// second update's inner_store.update await then runs immediately, BEFORE
// that update inserts into the existence cache, leaving a permanent stale
// exists-entry for a blob the inner store does not hold. Subsequent uploads
// of that key are drained and silently discarded.
#[nativelink_test]
async fn concurrent_update_pause_refcount_race_test() -> Result<(), Error> {
    use core::pin::Pin;
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use nativelink_metric::MetricsComponent;
    use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
    use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
    use nativelink_util::store_trait::{
        RemoveCallback, StoreDriver, StoreKey, UploadSizeInfo,
    };
    use tokio::sync::Notify;

    const VALID_HASH2: &str = "0223456789abcdef000000000000000000010000000000000123456789abcdef";

    #[derive(MetricsComponent)]
    struct GatedStore {
        inner: Store,
        mem: Arc<MemoryStore>,
        // Captured ExistenceCacheCallback registered by ExistenceCacheStore.
        callback: parking_lot::Mutex<Option<RemoveCallback>>,
        // (entered, release) per digest.
        gates: HashMap<DigestInfo, (Arc<Notify>, Arc<Notify>)>,
        // Key whose insertion should trigger an inline eviction (i.e. the
        // inner store evicts the just-inserted entry under cache pressure
        // and fires the remove callback while still inside update()).
        evict_key: DigestInfo,
    }

    impl core::fmt::Debug for GatedStore {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.write_str("GatedStore")
        }
    }

    #[async_trait]
    impl StoreDriver for GatedStore {
        async fn post_init(self: Arc<Self>) -> Result<(), Error> {
            Ok(())
        }

        async fn has_with_results(
            self: Pin<&Self>,
            digests: &[StoreKey<'_>],
            results: &mut [Option<u64>],
        ) -> Result<(), Error> {
            self.inner.has_with_results(digests, results).await
        }

        async fn update(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            reader: DropCloserReadHalf,
            size_info: UploadSizeInfo,
        ) -> Result<u64, Error> {
            let digest = key.borrow().into_digest();
            if let Some((entered, release)) = self.gates.get(&digest) {
                entered.notify_one();
                release.notified().await;
            }
            let result = self.inner.update(key, reader, size_info).await;
            if digest == self.evict_key {
                // Simulate eviction-under-pressure of the just-inserted
                // entry: the evicting map removes it and fires the remove
                // callback inline, inside this awaited update (see
                // filesystem_store.rs "Insertion can unref this entry
                // immediately under cache pressure").
                self.mem.remove_entry(digest.into()).await;
                let callback = self.callback.lock().clone();
                if let Some(callback) = callback {
                    callback.callback(digest.into()).await;
                }
            }
            result
        }

        async fn get_part(
            self: Pin<&Self>,
            key: StoreKey<'_>,
            writer: &mut DropCloserWriteHalf,
            offset: u64,
            length: Option<u64>,
        ) -> Result<(), Error> {
            self.inner.get_part(key, writer, offset, length).await
        }

        fn inner_store(&self, _digest: Option<StoreKey>) -> &dyn StoreDriver {
            self
        }

        fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
            self
        }

        fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
            self
        }

        fn register_remove_callback(
            self: Arc<Self>,
            callback: RemoveCallback,
        ) -> Result<(), Error> {
            *self.callback.lock() = Some(callback);
            Ok(())
        }
    }
    default_health_status_indicator!(GatedStore);

    let digest_x = DigestInfo::try_new(VALID_HASH1, 2).unwrap();
    let digest_y = DigestInfo::try_new(VALID_HASH2, 2).unwrap();

    let entered_x = Arc::new(Notify::new());
    let release_x = Arc::new(Notify::new());
    let entered_y = Arc::new(Notify::new());
    let release_y = Arc::new(Notify::new());

    let mem = MemoryStore::new(&MemorySpec::default());
    let gated = Arc::new(GatedStore {
        inner: Store::new(mem.clone()),
        mem: mem.clone(),
        callback: parking_lot::Mutex::new(None),
        gates: HashMap::from([
            (digest_x, (entered_x.clone(), release_x.clone())),
            (digest_y, (entered_y.clone(), release_y.clone())),
        ]),
        evict_key: digest_y,
    });

    let spec = ExistenceCacheSpec {
        backend: StoreSpec::Noop(NoopSpec::default()),
        eviction_policy: Option::default(),
    };
    let store = ExistenceCacheStore::new(&spec, Store::new(gated.clone()));

    // Update A (key X): reaches inner update, having installed the pause.
    let store_a = store.clone();
    let task_a = tokio::spawn(async move { store_a.update_oneshot(digest_x, "aa".into()).await });
    entered_x.notified().await;

    // Update B (key Y): sees the pause already installed and relies on it.
    let store_b = store.clone();
    let task_b = tokio::spawn(async move { store_b.update_oneshot(digest_y, "bb".into()).await });
    entered_y.notified().await;

    // A finishes completely: inserts X and take()s the shared pause slot,
    // ending the pause window for B as well.
    release_x.notify_one();
    task_a.await.unwrap()?;

    // B finishes its inner update; the just-inserted Y is evicted under
    // pressure and the remove callback fires inline (during B's await),
    // then B inserts Y into the existence cache.
    release_y.notify_one();
    task_b.await.unwrap()?;

    // The inner store does not hold Y (it was evicted).
    assert_eq!(mem.has(digest_y).await?, None, "Y was evicted from inner");

    // Correct behavior: the eviction notification for Y, delivered during
    // B's update, must not be lost - the existence cache must not claim Y
    // exists. With the non-refcounted pause slot, A's take() ended the
    // pause, the removal ran before B's insert, and the cache is stale.
    assert!(
        !store.exists_in_cache(&digest_y).await,
        "existence cache claims Y exists but inner store does not hold it"
    );

    // Demonstrate the harm: a re-upload of Y is drained and discarded.
    store.update_oneshot(digest_y, "bb".into()).await?;
    assert_eq!(
        mem.has(digest_y).await?,
        Some(2),
        "re-upload of Y was silently discarded due to stale existence cache entry"
    );
    Ok(())
}
