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

//! Deterministic clocks and entropy for the simulation thread.
//!
//! `NativeLink` reads the wall clock (`SystemTime::now`), the monotonic clock
//! (`std::time::Instant::now`) and OS entropy (`HashMap` seeds, `Uuid::new_v4`,
//! `rand::rng()`, tokio's `select!` seed) in hundreds of places. Plumbing a
//! clock and an rng through all of them is not on the table, so the simulator
//! does what madsim and `mad-turmoil` do: this binary defines `clock_gettime`,
//! `getrandom` and `getentropy` itself, and the definitions here shadow libc's
//! for every caller linked into the binary (std, uuid, getrandom, rand, tokio).
//!
//! Only a thread that called [`activate`] sees simulated values; every other
//! thread (the test harness, tokio's blocking pool) gets the real syscall. On
//! the simulation thread:
//!
//! - `CLOCK_REALTIME`: turmoil's logical epoch time of the running host
//!   (`turmoil::since_epoch`), plus an offset the fault injector can step
//!   ([`set_realtime_offset_ms`]) to model a clock step on the scheduler.
//! - `CLOCK_MONOTONIC`: the running host's tokio clock, which turmoil keeps
//!   paused and advances itself, so `std::time::Instant` and
//!   `tokio::time::Instant` agree.
//! - entropy: a splitmix64 stream seeded from the run's seed.
//!
//! Outside a host (in the runner, between steps) both clocks hold their last
//! simulated value.

use core::cell::Cell;
use core::time::Duration;
use std::time::Instant;

/// The logical wall clock starts here: 2026-01-01T00:00:00Z. Not the Unix
/// epoch, so no timestamp in the simulation looks like an unset one.
pub const SIM_EPOCH_SECS: u64 = 1_767_225_600;

/// Monotonic readings start here, far from zero for the same reason.
const MONO_BASE: Duration = Duration::from_secs(1_000_000);

thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static REALTIME_ON: Cell<bool> = const { Cell::new(true) };
    static MONOTONIC_ON: Cell<bool> = const { Cell::new(true) };
    static STD_ENTROPY_ON: Cell<bool> = const { Cell::new(true) };
    static CRATE_ENTROPY_ON: Cell<bool> = const { Cell::new(true) };
    static IN_HOOK: Cell<bool> = const { Cell::new(false) };
    static RNG: Cell<u64> = const { Cell::new(0) };
    static REALTIME_OFFSET_MS: Cell<i64> = const { Cell::new(0) };
    static LAST_REALTIME: Cell<Duration> = const { Cell::new(Duration::from_secs(SIM_EPOCH_SECS)) };
    static LAST_MONO: Cell<Duration> = const { Cell::new(MONO_BASE) };
    /// The `std::time::Instant` that corresponds to `MONO_BASE`.
    static ANCHOR: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// Which hooks a simulation thread uses. Turning one off is an ablation: it
/// shows what that source of nondeterminism does to a replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each bool is an independent ablation switch"
)]
pub struct Hooks {
    /// `CLOCK_REALTIME` (`SystemTime::now`).
    pub realtime: bool,
    /// `CLOCK_MONOTONIC` (`std::time::Instant::now`).
    pub monotonic: bool,
    /// std's own `getrandom` calls (flagged `GRND_INSECURE`/`GRND_NONBLOCK`):
    /// `HashMap` seeds, and through them tokio's scheduler and `select!` seed.
    pub std_entropy: bool,
    /// Every other `getrandom` call (unflagged, from the `getrandom` crate):
    /// `Uuid::new_v4`, `rand::rng()` (worker-id node bytes, openraft's
    /// election timeouts, retry jitter).
    pub crate_entropy: bool,
}

impl Hooks {
    pub const ALL: Self = Self {
        realtime: true,
        monotonic: true,
        std_entropy: true,
        crate_entropy: true,
    };

    /// `SIM_DET_OFF` is a comma-separated list of `realtime`, `monotonic`,
    /// `clock` (both), `hashseed` (std's entropy), `ids` (every other
    /// entropy), `entropy` (both) or `all`: those hooks stay off.
    pub fn from_env() -> Self {
        let mut hooks = Self::ALL;
        let off = std::env::var("SIM_DET_OFF").unwrap_or_default();
        for item in off.split(',') {
            match item.trim() {
                "realtime" => hooks.realtime = false,
                "monotonic" => hooks.monotonic = false,
                "clock" | "clocks" => {
                    hooks.realtime = false;
                    hooks.monotonic = false;
                }
                "hashseed" => hooks.std_entropy = false,
                "ids" => hooks.crate_entropy = false,
                "entropy" => {
                    hooks.std_entropy = false;
                    hooks.crate_entropy = false;
                }
                "all" => {
                    hooks = Self {
                        realtime: false,
                        monotonic: false,
                        std_entropy: false,
                        crate_entropy: false,
                    };
                }
                _ => {}
            }
        }
        hooks
    }
}

/// Make this thread a simulation thread: from now on its clocks are the
/// simulation's and its entropy is drawn from `seed` (each as `hooks` says).
pub fn activate(seed: u64, hooks: Hooks) {
    REALTIME_ON.with(|on| on.set(hooks.realtime));
    MONOTONIC_ON.with(|on| on.set(hooks.monotonic));
    STD_ENTROPY_ON.with(|on| on.set(hooks.std_entropy));
    CRATE_ENTROPY_ON.with(|on| on.set(hooks.crate_entropy));
    // Referencing the hooks keeps the linker from dropping the object file
    // that defines them: nothing else in the binary names these symbols, and
    // libc would otherwise satisfy std's references.
    let clock: unsafe extern "C" fn(libc::clockid_t, *mut libc::timespec) -> libc::c_int =
        clock_gettime;
    let random: unsafe extern "C" fn(*mut u8, usize, libc::c_uint) -> isize = getrandom;
    let entropy: unsafe extern "C" fn(*mut u8, usize) -> libc::c_int = getentropy;
    core::hint::black_box((clock, random, entropy));
    RNG.with(|rng| rng.set(seed ^ 0x6a09_e667_f3bc_c908));
    REALTIME_OFFSET_MS.with(|offset| offset.set(0));
    LAST_REALTIME.with(|last| last.set(Duration::from_secs(SIM_EPOCH_SECS)));
    LAST_MONO.with(|last| last.set(MONO_BASE));
    ACTIVE.with(|active| active.set(true));
    // With the hook active and no runtime entered, this reads `MONO_BASE`.
    ANCHOR.with(|anchor| anchor.set(Some(Instant::now())));
}

/// Hand the thread back to the real clocks and entropy.
pub fn deactivate() {
    ACTIVE.with(|active| active.set(false));
}

/// Whether this thread is a simulation thread.
pub fn is_active() -> bool {
    ACTIVE.try_with(Cell::get).unwrap_or(false)
}

/// Step the simulated wall clock by `offset_ms` relative to logical time.
pub fn set_realtime_offset_ms(offset_ms: i64) {
    REALTIME_OFFSET_MS.with(|offset| offset.set(offset_ms));
}

/// The current wall-clock offset, in milliseconds.
pub fn realtime_offset_ms() -> i64 {
    REALTIME_OFFSET_MS.with(Cell::get)
}

/// A draw from the simulation's entropy stream (for harness use, so the
/// harness and the code under test share one seeded source).
pub fn next_u64() -> u64 {
    RNG.with(|rng| {
        let mut state = rng.get();
        let value = splitmix64(&mut state);
        rng.set(state);
        value
    })
}

const fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn simulated_realtime() -> Duration {
    let since_epoch = turmoil::since_epoch().map(|logical| {
        let offset_ms = REALTIME_OFFSET_MS.with(Cell::get);
        let skewed = if offset_ms >= 0 {
            logical.saturating_add(Duration::from_millis(offset_ms.unsigned_abs()))
        } else {
            logical.saturating_sub(Duration::from_millis(offset_ms.unsigned_abs()))
        };
        LAST_REALTIME.with(|last| last.set(skewed));
        skewed
    });
    since_epoch.unwrap_or_else(|| LAST_REALTIME.with(Cell::get))
}

fn simulated_monotonic() -> Duration {
    let anchor = ANCHOR.with(Cell::get);
    let from_tokio = anchor.and_then(|anchor| {
        tokio::runtime::Handle::try_current().ok().map(|_| {
            let now = tokio::time::Instant::now().into_std();
            MONO_BASE.saturating_add(now.saturating_duration_since(anchor))
        })
    });
    match from_tokio {
        Some(mono) => {
            LAST_MONO.with(|last| last.set(mono));
            mono
        }
        None => LAST_MONO.with(Cell::get),
    }
}

fn to_timespec(duration: Duration) -> libc::timespec {
    libc::timespec {
        tv_sec: libc::time_t::try_from(duration.as_secs()).unwrap_or(libc::time_t::MAX),
        tv_nsec: libc::c_long::from(duration.subsec_nanos()),
    }
}

/// Run `f` if this is a simulation thread and not already inside a hook (a
/// clock read can call back into a clock read: tokio's clock on a runtime
/// without time enabled reads `Instant::now`).
fn simulated<R>(f: impl FnOnce() -> R) -> Option<R> {
    if !is_active() || IN_HOOK.with(Cell::get) {
        return None;
    }
    IN_HOOK.with(|in_hook| in_hook.set(true));
    let result = f();
    IN_HOOK.with(|in_hook| in_hook.set(false));
    Some(result)
}

/// # Safety
///
/// `tp` must be valid for a write of one `timespec`, as libc requires.
#[unsafe(no_mangle)]
#[inline(never)]
pub unsafe extern "C" fn clock_gettime(
    clockid: libc::clockid_t,
    tp: *mut libc::timespec,
) -> libc::c_int {
    let realtime_on = REALTIME_ON.try_with(Cell::get).unwrap_or(false);
    let monotonic_on = MONOTONIC_ON.try_with(Cell::get).unwrap_or(false);
    let value = simulated(|| match clockid {
        libc::CLOCK_REALTIME | libc::CLOCK_REALTIME_COARSE if realtime_on => {
            Some(simulated_realtime())
        }
        libc::CLOCK_MONOTONIC
        | libc::CLOCK_MONOTONIC_RAW
        | libc::CLOCK_MONOTONIC_COARSE
        | libc::CLOCK_BOOTTIME
            if monotonic_on =>
        {
            Some(simulated_monotonic())
        }
        _ => None,
    })
    .flatten();
    if let Some(value) = value
        && !tp.is_null()
    {
        // SAFETY: the caller hands us a valid out pointer, as for libc.
        unsafe { tp.write(to_timespec(value)) };
        return 0;
    }
    // SAFETY: the raw syscall has exactly libc's contract.
    let result = unsafe { libc::syscall(libc::SYS_clock_gettime, clockid, tp) };
    libc::c_int::try_from(result).unwrap_or(-1)
}

fn fill_simulated(buf: *mut u8, buflen: usize, flags: libc::c_uint) -> bool {
    let hooked = if flags == 0 {
        &CRATE_ENTROPY_ON
    } else {
        &STD_ENTROPY_ON
    };
    if !hooked.try_with(Cell::get).unwrap_or(false) {
        return false;
    }
    simulated(|| {
        if buf.is_null() {
            return;
        }
        // SAFETY: the caller hands us `buflen` writable bytes, as for libc.
        let dest = unsafe { core::slice::from_raw_parts_mut(buf, buflen) };
        for chunk in dest.chunks_mut(8) {
            let bytes = next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    })
    .is_some()
}

/// # Safety
///
/// `buf` must be valid for `buflen` bytes of writes, as libc requires.
#[unsafe(no_mangle)]
#[inline(never)]
pub unsafe extern "C" fn getrandom(buf: *mut u8, buflen: usize, flags: libc::c_uint) -> isize {
    if fill_simulated(buf, buflen, flags) {
        return isize::try_from(buflen).unwrap_or(isize::MAX);
    }
    // SAFETY: the raw syscall has exactly libc's contract.
    let result = unsafe { libc::syscall(libc::SYS_getrandom, buf, buflen, flags) };
    isize::try_from(result).unwrap_or(-1)
}

/// # Safety
///
/// `buf` must be valid for `buflen` bytes of writes, as libc requires.
#[unsafe(no_mangle)]
#[inline(never)]
pub unsafe extern "C" fn getentropy(buf: *mut u8, buflen: usize) -> libc::c_int {
    if buflen > 256 {
        return -1;
    }
    // SAFETY: forwarded with the caller's own contract.
    let filled = unsafe { getrandom(buf, buflen, 0) };
    if filled < 0 { -1 } else { 0 }
}
