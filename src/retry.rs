//! Shared retry policy: exponential backoff with optional equal jitter.
//!
//! Used by the worker for both Memory and Redis broker paths so nack requeue
//! delays are not reimplemented per broker.
//!
//! # Delay formula
//!
//! After a failed attempt with claim count `attempt` (1-based, as stored on
//! [`crate::Job::attempts`] after claim):
//!
//! ```text
//! raw = min(max_delay, base_delay * 2^(attempt.saturating_sub(1)))
//! ```
//!
//! - When `jitter` is **false**, the delay is exactly `raw`.
//! - When `jitter` is **true** (default), **equal jitter** is applied using
//!   integer nanosecond math: `half = floor(raw/2)`, then
//!   `half + random(0..=half)`. The result lies in
//!   `[floor(raw/2), 2*floor(raw/2)]` ⊆ `[raw/2, raw]`. The upper bound equals
//!   `raw` when `raw` is even in nanoseconds; when odd it is `raw - 1ns`.
//!
//! Attempt `0` is treated like attempt `1` (no panic). Large attempt values
//! saturate rather than overflow or panic; the delay is still capped by
//! `max_delay`.

use rand::Rng;
use std::time::Duration;

/// Default max claim attempts before a failure is terminal (M1 default).
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// Default base delay for exponential backoff (`base_delay * 2^(attempt-1)`).
pub const DEFAULT_BASE_DELAY: Duration = Duration::from_secs(1);
/// Default cap on computed delay (before jitter).
pub const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(15 * 60);

/// Worker retry / nack requeue policy (shared across brokers).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Max claim attempts before a failure is terminal (default 3).
    pub max_attempts: u32,
    /// Base delay for attempt 1 (default 1s).
    pub base_delay: Duration,
    /// Hard cap on the raw exponential delay (default 15 minutes).
    pub max_delay: Duration,
    /// When true, apply equal jitter so delay ∈
    /// `[floor(raw/2), 2*floor(raw/2)]` ⊆ `[raw/2, raw]` (default true).
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            base_delay: DEFAULT_BASE_DELAY,
            max_delay: DEFAULT_MAX_DELAY,
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Compute delay after a failed attempt `attempt` (1-based claim count as
    /// stored on [`crate::Job::attempts`]).
    ///
    /// - `attempt == 1` → `base_delay` (then jitter if enabled)
    /// - `attempt == 2` → `2 * base_delay`, …
    /// - Always capped at `max_delay` before jitter
    /// - Does not panic for `attempt == 0` or very large `attempt`
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let exp = attempt.saturating_sub(1);
        let raw = saturating_mul_pow2(self.base_delay, exp).min(self.max_delay);

        if !self.jitter || raw.is_zero() {
            return raw;
        }

        // Equal jitter: half + U(0..=half) where half = floor(raw/2).
        // Range: [half, 2*half] ⊆ [raw/2, raw] (equals raw only when raw even in ns).
        let half = raw / 2;
        let half_nanos = duration_as_nanos_u64(half);
        let extra = if half_nanos == 0 {
            0
        } else {
            rand::thread_rng().gen_range(0..=half_nanos)
        };
        half + Duration::from_nanos(extra)
    }
}

/// `base * 2^exp`, saturating at [`Duration`] limits (no panic).
fn saturating_mul_pow2(base: Duration, exp: u32) -> Duration {
    if exp == 0 || base.is_zero() {
        return base;
    }
    // Duration is stored as u64 nanoseconds; saturate if product would exceed.
    let nanos = base.as_nanos(); // u128
    if exp >= 128 {
        return Duration::MAX;
    }
    let factor = 1u128 << exp;
    match nanos.checked_mul(factor) {
        Some(n) if n <= u64::MAX as u128 => Duration::from_nanos(n as u64),
        _ => Duration::MAX,
    }
}

fn duration_as_nanos_u64(d: Duration) -> u64 {
    d.as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_jitter_exact_powers_of_two() {
        let p = RetryPolicy {
            max_attempts: 10,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(15 * 60),
            jitter: false,
        };

        assert_eq!(p.delay_for_attempt(1), Duration::from_secs(1));
        assert_eq!(p.delay_for_attempt(2), Duration::from_secs(2));
        assert_eq!(p.delay_for_attempt(3), Duration::from_secs(4));
        assert_eq!(p.delay_for_attempt(4), Duration::from_secs(8));
        assert_eq!(p.delay_for_attempt(5), Duration::from_secs(16));
    }

    #[test]
    fn no_jitter_caps_at_max_delay() {
        let p = RetryPolicy {
            max_attempts: 100,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
            jitter: false,
        };

        // 2^10 = 1024s would exceed 10s cap
        assert_eq!(p.delay_for_attempt(11), Duration::from_secs(10));
        assert_eq!(p.delay_for_attempt(50), Duration::from_secs(10));
    }

    #[test]
    fn jitter_stays_within_half_to_raw() {
        let p = RetryPolicy {
            max_attempts: 10,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            jitter: true,
        };

        for attempt in 1..=8 {
            let exp = attempt - 1;
            let raw = Duration::from_millis(100) * (1u32 << exp);
            let half = raw / 2;
            for _ in 0..32 {
                let d = p.delay_for_attempt(attempt);
                assert!(
                    d >= half && d <= raw,
                    "attempt={attempt}: delay {d:?} not in [{half:?}, {raw:?}]"
                );
            }
        }
    }

    #[test]
    fn jitter_respects_max_delay_cap() {
        let max = Duration::from_millis(200);
        let p = RetryPolicy {
            max_attempts: 10,
            base_delay: Duration::from_millis(100),
            max_delay: max,
            jitter: true,
        };

        // attempt large enough that raw would exceed max → raw = max
        let half = max / 2;
        for _ in 0..32 {
            let d = p.delay_for_attempt(10);
            assert!(
                d >= half && d <= max,
                "delay {d:?} not in [{half:?}, {max:?}]"
            );
        }
    }

    #[test]
    fn attempt_zero_and_large_do_not_panic() {
        let p = RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(15 * 60),
            jitter: false,
        };

        // attempt 0 treated like attempt 1 (exp = 0)
        assert_eq!(p.delay_for_attempt(0), Duration::from_secs(1));

        // Very large attempt: saturates then caps at max_delay
        let d = p.delay_for_attempt(u32::MAX);
        assert_eq!(d, p.max_delay);

        let p_jitter = RetryPolicy { jitter: true, ..p };
        let _ = p_jitter.delay_for_attempt(0);
        let _ = p_jitter.delay_for_attempt(u32::MAX);
    }

    #[test]
    fn default_matches_locked_values() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_attempts, 3);
        assert_eq!(p.base_delay, Duration::from_secs(1));
        assert_eq!(p.max_delay, Duration::from_secs(15 * 60));
        assert!(p.jitter);
    }

    #[test]
    fn zero_base_delay_stays_zero() {
        let p = RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::ZERO,
            max_delay: Duration::from_secs(10),
            jitter: true,
        };
        assert_eq!(p.delay_for_attempt(1), Duration::ZERO);
        assert_eq!(p.delay_for_attempt(5), Duration::ZERO);
    }
}
