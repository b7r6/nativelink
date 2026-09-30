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

const VALID_HASH2: &str = "aa23456789abcdef000000000000000000010000000000000123456789abcdef";

// Repro for the pause_remove_callbacks race: it is an Option-as-boolean with no
// refcount. Two concurrent update() calls share one pause window; the first
// finisher unconditionally take()s the flag, unpausing the second update while
// its inner-store write is still in flight. If the inner store then fires a
// remove callback for the in-flight key (eviction/oversized skip), the callback
// runs immediately as a no-op (key not yet cached), and the second update then
// inserts a stale "exists" entry for a blob the inner store does not hold.
#[nativelink_test]
async fn concurrent_update_unpause_race_stale_existence_test() -> Result<(), Error> {
    let spec = ExistenceCacheSpec {
        backend: StoreSpec::Noop(NoopSpec::default()),
        eviction_policy: Option::default(),
    };
    // max_bytes small enough that digest_b's write is skipped (and remove
    // callbacks fired) by the inner memory store.
    let inner_store = Store::new(MemoryStore::new(&MemorySpec {
        eviction_policy: Some(EvictionPolicy {
            max_bytes: 150,
            ..Default::default()
        }),
    }));
    let store = ExistenceCacheStore::new_with_time(
        &spec,
        inner_store.clone(),
        MockInstantWrapped::default(),
    );

    let digest_a = DigestInfo::try_new(VALID_HASH1, 10).unwrap();
    let digest_b = DigestInfo::try_new(VALID_HASH2, 200).unwrap();

    // Task B: starts update(), sets the pause flag, then parks inside the inner
    // store's update awaiting stream data.
    let (mut tx_b, rx_b) = nativelink_util::buf_channel::make_buf_channel_pair();
    let store_b = store.clone();
    let task_b = nativelink_util::spawn!("update_b", async move {
        store_b
            .update(
                digest_b,
                rx_b,
                nativelink_util::store_trait::UploadSizeInfo::ExactSize(200),
            )
            .await
    });
    // Single-threaded runtime: let B run until it awaits the reader.
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    // Task A: full update while B is in flight. A finds the pause flag Some
    // (set by B), and at the end unconditionally take()s it -- stealing B's
    // pause window.
    store
        .update_oneshot(digest_a, vec![0u8; 10].into())
        .await
        .err_tip(|| "update A failed")?;

    // Now finish B. The inner memory store drains it, refuses to store it
    // (200 >= max_bytes) and fires remove callbacks for digest_b -- but the
    // pause flag is already None, so the removal runs immediately as a no-op.
    // B then inserts digest_b into the existence cache: stale entry.
    tx_b.send(bytes::Bytes::from(vec![0u8; 200]))
        .await
        .err_tip(|| "send b")?;
    tx_b.send_eof().err_tip(|| "eof b")?;
    task_b.await.expect("join b").err_tip(|| "update B failed")?;

    assert_eq!(
        inner_store.has(digest_b).await?,
        None,
        "precondition: inner store must not hold digest_b"
    );
    assert_eq!(
        store.has(digest_b).await.err_tip(|| "has b")?,
        None,
        "existence cache claims digest_b exists but the inner store evicted it \
         (pause window stolen by concurrent update)"
    );
    Ok(())
}
