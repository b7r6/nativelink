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

//! Checked elapsed seconds for experimental authority construction.
//!
//! This source alone does not bind a journal's time unit, select expiry
//! comparisons, or establish minimum elapsed deadline windows. Production
//! bootstrap must validate those contracts before interpreting recovered ticks.

use std::time::Instant;

/// A committed base plus whole monotonic seconds in this process lifetime.
///
/// Recreating the clock discards downtime, uncommitted elapsed time and the
/// previous fractional second. Repeated restarts can postpone expiry without a
/// wall-time bound. The clock never infers elapsed time from a wall clock.
#[derive(Clone, Copy, Debug)]
pub struct MonotonicClock {
    base: u64,
    origin: Instant,
}

impl MonotonicClock {
    /// Capture a fresh origin after the authority's recovery read barrier.
    /// The caller must establish the fixed seconds contract independently;
    /// passing an arbitrary journal tick does not validate its provenance.
    pub fn from_recovered_tick(base: u64) -> Self {
        Self {
            base,
            origin: Instant::now(),
        }
    }

    pub fn observe(&self) -> Result<u64, String> {
        self.observe_at(Instant::now())
    }

    fn observe_at(&self, now: Instant) -> Result<u64, String> {
        let elapsed = now
            .checked_duration_since(self.origin)
            .ok_or_else(|| "Monotonic source precedes its captured origin".to_owned())?;
        self.base
            .checked_add(elapsed.as_secs())
            .ok_or_else(|| "Monotonic authority seconds exhausted".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn fractional_seconds_do_not_advance_the_base() {
        let clock = MonotonicClock::from_recovered_tick(73);
        for (elapsed, expected) in [
            (Duration::ZERO, 73),
            (Duration::from_nanos(999_999_999), 73),
            (Duration::from_secs(1), 74),
            (Duration::from_millis(1999), 74),
            (Duration::from_secs(2), 75),
        ] {
            assert_eq!(clock.observe_at(clock.origin + elapsed).unwrap(), expected);
        }
    }

    #[test]
    fn restart_preserves_only_the_committed_base() {
        let clock = MonotonicClock::from_recovered_tick(73);
        let committed = clock
            .observe_at(clock.origin + Duration::from_secs(7))
            .unwrap();
        assert_eq!(committed, 80);
        assert_eq!(
            clock
                .observe_at(clock.origin + Duration::from_millis(9999))
                .unwrap(),
            82
        );
        // Neither the discarded observation nor arbitrary downtime is input
        // to the next lifetime. Repeating this can postpone expiry forever.
        for _ in 0..4 {
            let restarted = MonotonicClock::from_recovered_tick(committed);
            assert_eq!(restarted.observe_at(restarted.origin).unwrap(), 80);
            assert_eq!(
                restarted
                    .observe_at(restarted.origin + Duration::from_millis(999))
                    .unwrap(),
                80
            );
        }
    }

    #[test]
    fn exhaustion_and_a_source_before_origin_are_errors() {
        let clock = MonotonicClock::from_recovered_tick(u64::MAX - 1);
        assert_eq!(
            clock
                .observe_at(clock.origin + Duration::from_secs(1))
                .unwrap(),
            u64::MAX
        );
        assert!(
            clock
                .observe_at(clock.origin + Duration::from_secs(2))
                .unwrap_err()
                .contains("exhausted")
        );
        assert!(
            clock
                .observe_at(clock.origin.checked_sub(Duration::from_nanos(1)).unwrap())
                .unwrap_err()
                .contains("precedes")
        );
    }
}
