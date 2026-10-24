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

//! Numeric clock oracles; production construction accepts only private read facts.

use std::time::{Duration, Instant};

use openraft::{CommittedLeaderId, LogId};

use super::*;

const CUTOFF: u64 = 18_446_743_758_133_551_613;

fn facts(tick: u64) -> ReadFacts {
    ReadFacts {
        applied: LogId::new(CommittedLeaderId::new(7, 1), 9),
        tick,
    }
}

fn clock(tick: u64, origin: Instant) -> Clock {
    Clock {
        facts: facts(tick),
        origin,
        last: origin,
        first_tick: tick,
    }
}

#[test]
fn fractional_observations_floor_without_accumulating_rounding() {
    let origin = Instant::now();
    let mut value = clock(17, origin);
    assert_eq!(
        value.observe_at(origin + Duration::from_millis(999)),
        Ok(17)
    );
    assert_eq!(
        value.observe_at(origin + Duration::from_millis(1_001)),
        Ok(18)
    );
    assert_eq!(
        value.observe_at(origin + Duration::from_millis(1_999)),
        Ok(18)
    );
    assert_eq!(value.observe_at(origin + Duration::from_secs(2)), Ok(19));
}

#[test]
fn exact_cutoff_retains_fractional_headroom_and_refuses_the_next_tick() {
    let origin = Instant::now();
    let mut value = clock(CUTOFF, origin);
    assert_eq!(value.observe_at(origin), Ok(CUTOFF));
    assert_eq!(
        value.observe_at(origin + Duration::from_nanos(999_999_999)),
        Ok(CUTOFF)
    );
    assert_eq!(
        value.observe_at(origin + Duration::from_secs(1)),
        Err(ClockError::Range)
    );
    assert_eq!(value.last, origin + Duration::from_nanos(999_999_999));
}

#[test]
fn backward_observation_before_origin_refuses() {
    let origin = Instant::now();
    let mut value = clock(17, origin);
    assert_eq!(
        value.observe_at(origin.checked_sub(Duration::from_nanos(1)).unwrap()),
        Err(ClockError::Backward)
    );
    assert_eq!(value.last, origin);
}

#[test]
fn backward_observation_after_origin_and_within_the_same_tick_refuses() {
    let origin = Instant::now();
    let mut value = clock(17, origin);
    let accepted = origin + Duration::from_millis(2_200);
    assert_eq!(value.observe_at(accepted), Ok(19));
    assert_eq!(
        value.observe_at(origin + Duration::from_millis(2_100)),
        Err(ClockError::Backward)
    );
    assert_eq!(value.last, accepted);
    assert_eq!(value.observe_at(accepted), Ok(19));
}

#[test]
fn invalid_base_is_refused_before_an_observation_is_published() {
    assert!(matches!(
        Clock::capture(facts(CUTOFF + 1)),
        Err(ClockError::Range)
    ));
    assert!(matches!(
        Clock::capture(facts(u64::MAX)),
        Err(ClockError::Range)
    ));
}

#[test]
fn addition_overflow_refuses_without_saturation() {
    let origin = Instant::now();
    let mut value = clock(CUTOFF, origin);
    let future = origin
        .checked_add(Duration::from_secs(315_576_000_003))
        .expect("Linux monotonic instant supports the finite timeout headroom interval");
    assert_eq!(value.observe_at(future), Err(ClockError::Range));
    assert_eq!(value.last, origin);
}

#[test]
fn capture_preserves_the_persisted_base_and_position() {
    let before = Instant::now();
    let value = Clock::capture(facts(17)).unwrap();
    assert!(value.origin >= before);
    assert_eq!(value.facts.tick, 17);
    assert_eq!(value.facts.applied, facts(17).applied);
    assert!(value.first_tick >= 17 && value.first_tick <= CUTOFF);
    assert!(value.last >= value.origin);
}

#[test]
fn fresh_capture_does_not_restore_previous_fraction_or_downtime() {
    let before = Instant::now();
    let old_origin = before.checked_sub(Duration::from_secs(8)).unwrap();
    let mut old = clock(17, old_origin);
    assert_eq!(
        old.observe_at(old_origin + Duration::from_millis(1_900)),
        Ok(18)
    );
    let fresh = Clock::capture(facts(18)).unwrap();
    assert_eq!(fresh.facts.tick, 18);
    assert!(fresh.origin >= before);
    assert!(fresh.origin > old.last);
}

#[test]
fn first_observation_failure_refuses_the_clock() {
    let origin = Instant::now();
    assert!(matches!(
        Clock::finish_capture(facts(CUTOFF), origin, origin + Duration::from_secs(1)),
        Err(ClockError::Range)
    ));
    assert!(matches!(
        Clock::finish_capture(
            facts(17),
            origin,
            origin.checked_sub(Duration::from_nanos(1)).unwrap()
        ),
        Err(ClockError::Backward)
    ));
}

#[test]
fn missing_barrier_or_applied_position_is_refused() {
    let position = facts(17).applied;
    for (barrier, applied) in [(None, Some(position)), (Some(position), None), (None, None)] {
        assert!(matches!(
            ReadFacts::after_barrier(
                barrier,
                temporal::AppliedFacts {
                    last_applied: applied,
                    now: 17
                }
            ),
            Err(ClockError::Read)
        ));
    }
}

#[test]
fn applied_position_must_reach_the_barrier_with_exact_identity_at_equal_index() {
    let barrier = facts(17).applied;
    for applied in [
        LogId::new(barrier.leader_id, barrier.index - 1),
        LogId::new(CommittedLeaderId::new(8, 1), barrier.index),
        LogId::new(CommittedLeaderId::new(7, 2), barrier.index),
    ] {
        assert!(matches!(
            ReadFacts::after_barrier(
                Some(barrier),
                temporal::AppliedFacts {
                    last_applied: Some(applied),
                    now: 17
                }
            ),
            Err(ClockError::Read)
        ));
    }
    for applied in [
        barrier,
        LogId::new(CommittedLeaderId::new(8, 1), barrier.index + 1),
    ] {
        let value = ReadFacts::after_barrier(
            Some(barrier),
            temporal::AppliedFacts {
                last_applied: Some(applied),
                now: 17,
            },
        )
        .unwrap();
        assert_eq!(value.applied, applied);
        assert_eq!(value.tick, 17);
    }
}
