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

//! A valid child snapshot need not belong to the outer authority's alphabet.

use super::*;
use crate::coarse::Coarse;

fn published() -> PublicationAuthority {
    let mut state = authority(&[("a", false)]);
    state.authorize_client(9).unwrap();
    let value = candidate(&mut state, "a", Publisher::Client { principal: 9 }, 1, 0, 0);
    stage(&mut state, 1, value);
    state.publish(1, 1, 100).unwrap();
    state.serve("a", 1, state.commit(), 9, 90).unwrap();
    state
}

fn child(snapshot: &[u8]) -> Coarse {
    let outer: serde_json::Value = serde_json::from_slice(snapshot).unwrap();
    let bytes: Vec<u8> = serde_json::from_value(outer["retention"].clone()).unwrap();
    Coarse::restore(&bytes).unwrap()
}

fn replace_child(snapshot: &[u8], retention: &Coarse) -> Vec<u8> {
    let outer: serde_json::Value = serde_json::from_slice(snapshot).unwrap();
    let previous: Vec<u8> = serde_json::from_value(outer["retention"].clone()).unwrap();
    let replacement = retention.snapshot().unwrap();
    // Prove this is a valid, canonical Coarse state before testing outer refusal.
    assert_eq!(
        Coarse::restore(&replacement).unwrap().snapshot().unwrap(),
        replacement
    );
    let original = format!(
        "\"retention\":{}",
        serde_json::to_string(&previous).unwrap()
    );
    let changed = format!(
        "\"retention\":{}",
        serde_json::to_string(&replacement).unwrap()
    );
    let text = std::str::from_utf8(snapshot).unwrap();
    assert_eq!(text.matches(&original).count(), 1);
    // Preserve the outer struct's field order; serializing Value would sort it.
    let encoded = text.replacen(&original, &changed, 1).into_bytes();
    assert!(encoded.len() < MAX_SNAPSHOT);
    encoded
}

#[test]
fn restore_rejects_valid_sealed_child_between_atomic_fence_steps() {
    let state = published();
    let snapshot = state.snapshot().unwrap();
    let mut retention = child(&snapshot);
    assert_eq!(
        retention.apply(r::Event::Seal {
            cohort: r::CohortId(1)
        }),
        r::Reply::Applied
    );
    assert_eq!(
        retention.view().cohorts[&r::CohortId(1)].phase,
        r::Phase::Sealed
    );
    assert_eq!(
        PublicationAuthority::restore(&replace_child(&snapshot, &retention)),
        Err(Error::Corrupt)
    );
    assert_eq!(state.snapshot().unwrap(), snapshot);
}

#[test]
fn restore_rejects_valid_closed_capture_with_or_without_completed_mark() {
    let mut state = published();
    state.fence_namespace(1).unwrap();
    let snapshot = state.snapshot().unwrap();
    let mut retention = child(&snapshot);
    let fence = retention.view().cohorts[&r::CohortId(1)].fence;
    assert_eq!(
        retention.apply(r::Event::FinishClose {
            cohort: r::CohortId(1),
            fence
        }),
        r::Reply::Applied
    );
    let closed = retention.view().cohorts[&r::CohortId(1)].clone();
    assert_eq!(closed.phase, r::Phase::Closed);
    assert_eq!(closed.captured.len(), 2);
    assert!(closed.marked.is_empty());
    let before_mark =
        PublicationAuthority::restore(&replace_child(&snapshot, &retention)).map(|_| ());
    assert_eq!(
        retention.apply(r::Event::Mark {
            cohort: r::CohortId(1),
            fence,
            at: 0,
            roots: closed.captured
        }),
        r::Reply::Applied
    );
    let marked = retention.view().cohorts[&r::CohortId(1)].clone();
    assert_eq!(marked.cursor, 2);
    assert_eq!(marked.marked.len(), 1);
    let after_mark =
        PublicationAuthority::restore(&replace_child(&snapshot, &retention)).map(|_| ());
    assert_eq!(
        (before_mark, after_mark),
        (Err(Error::Corrupt), Err(Error::Corrupt))
    );
    assert_eq!(state.snapshot().unwrap(), snapshot);
}

#[test]
fn restore_rejects_valid_closed_child_after_all_roots_expire() {
    let mut state = published();
    state.advance(100).unwrap();
    state.fence_namespace(1).unwrap();
    let snapshot = state.snapshot().unwrap();
    let mut retention = child(&snapshot);
    let fence = retention.view().cohorts[&r::CohortId(1)].fence;
    assert_eq!(
        retention.apply(r::Event::FinishClose {
            cohort: r::CohortId(1),
            fence
        }),
        r::Reply::Applied
    );
    let closed = retention.view().cohorts[&r::CohortId(1)].clone();
    assert!(closed.captured.is_empty() && closed.marked.is_empty());
    assert_eq!(
        PublicationAuthority::restore(&replace_child(&snapshot, &retention)),
        Err(Error::Corrupt)
    );
}

#[test]
fn exposed_allocating_and_closing_states_restore_and_continue() {
    let mut state = published();
    for expected in [r::Phase::Allocating, r::Phase::Closing] {
        let snapshot = state.snapshot().unwrap();
        let cohort = state.retention_view().cohorts[&r::CohortId(1)].clone();
        assert_eq!(cohort.phase, expected);
        assert!(cohort.captured.is_empty() && cohort.marked.is_empty());
        assert_eq!(cohort.cursor, 0);
        let mut restored = PublicationAuthority::restore(&snapshot).unwrap();
        assert_eq!(restored.snapshot().unwrap(), snapshot);
        state.advance(1).unwrap();
        restored.advance(1).unwrap();
        assert_eq!(restored.snapshot().unwrap(), state.snapshot().unwrap());
        if expected == r::Phase::Allocating {
            state.fence_namespace(1).unwrap();
        }
    }
}
