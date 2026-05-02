//! Node-local token-bucket limiter for DSQL connection creation.
//!
//! This limiter protects connection establishment, not query execution. DSQL
//! IAM/token paths and cluster connection limits are sensitive to synchronized
//! bursts, so the reservoir refiller asks this limiter before opening a new
//! physical connection.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration as StdDuration, Instant},
};

use crate::metrics;

const SCALE: u64 = 1_000_000;

/// Node-local token-bucket rate limiter for connection creation.
#[derive(Debug)]
pub struct TokenBucketRateLimiter {
    /// Current token count in fixed-point units.
    tokens: AtomicU64,
    /// Maximum token count in fixed-point units.
    capacity: AtomicU64,
    /// Tokens added per second in fixed-point units.
    refill_rate: AtomicU64,
    /// Stable monotonic origin for elapsed-nanosecond calculations.
    ///
    /// We never try to serialize `Instant`; atomics store elapsed nanos since
    /// this per-process base.
    base: Instant,
    /// Last refill timestamp as nanos elapsed since `base`.
    last_refill_nanos: AtomicU64,
}

impl TokenBucketRateLimiter {
    /// Create a limiter with an initially full bucket.
    pub fn new(rate_per_second: f64, capacity: u64) -> Self {
        let capacity_fixed = capacity.saturating_mul(SCALE);
        Self {
            tokens: AtomicU64::new(capacity_fixed),
            capacity: AtomicU64::new(capacity_fixed),
            refill_rate: AtomicU64::new(to_fixed(rate_per_second)),
            base: Instant::now(),
            last_refill_nanos: AtomicU64::new(0),
        }
    }

    /// Wait until one token can be consumed.
    ///
    /// The short async delay avoids a CPU spin while keeping connection
    /// creation responsive under startup pressure.
    pub async fn acquire(&self) {
        loop {
            if self.try_acquire() {
                return;
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    }

    /// Attempt to consume one token without waiting.
    pub fn try_acquire(&self) -> bool {
        self.refill();
        let one = SCALE;
        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current < one {
                return false;
            }
            if self
                .tokens
                .compare_exchange(current, current - one, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                metrics::record_dsql_pool_rate_limiter(
                    self.available_tokens(),
                    self.rate_per_second(),
                );
                return true;
            }
        }
    }

    /// Replace limiter parameters while preserving current token state.
    ///
    /// Tokens are capped to the new capacity so a scale-down takes effect
    /// immediately.
    pub fn reconfigure(&self, rate_per_second: f64, capacity: u64) {
        self.refill();
        let capacity_fixed = capacity.saturating_mul(SCALE);
        self.capacity.store(capacity_fixed, Ordering::Release);
        self.refill_rate
            .store(to_fixed(rate_per_second), Ordering::Release);
        let current = self.tokens.load(Ordering::Acquire);
        if current > capacity_fixed {
            self.tokens.store(capacity_fixed, Ordering::Release);
        }
        metrics::record_dsql_pool_rate_limiter(self.available_tokens(), rate_per_second);
    }

    /// Current token count in human-readable whole-token units.
    pub fn available_tokens(&self) -> f64 {
        self.refill();
        self.tokens.load(Ordering::Acquire) as f64 / SCALE as f64
    }

    /// Current refill rate in whole tokens per second.
    pub fn rate_per_second(&self) -> f64 {
        self.refill_rate.load(Ordering::Acquire) as f64 / SCALE as f64
    }

    /// Refill tokens based on monotonic elapsed time.
    ///
    /// Multiple concurrent callers may race here; compare-exchange on `tokens`
    /// keeps the bucket bounded, and swapping `last_refill_nanos` means a small
    /// amount of refill can be skipped under races but never double-counted.
    fn refill(&self) {
        let now_nanos = nanos_since(self.base);
        let previous = self.last_refill_nanos.swap(now_nanos, Ordering::AcqRel);
        if now_nanos <= previous {
            return;
        }
        let elapsed_nanos = now_nanos - previous;
        let rate = self.refill_rate.load(Ordering::Acquire);
        let add = ((elapsed_nanos as u128 * rate as u128) / 1_000_000_000) as u64;
        if add == 0 {
            return;
        }
        let capacity = self.capacity.load(Ordering::Acquire);
        loop {
            let current = self.tokens.load(Ordering::Acquire);
            let next = current.saturating_add(add).min(capacity);
            if self
                .tokens
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }
}

fn to_fixed(rate: f64) -> u64 {
    if !rate.is_finite() || rate <= 0.0 {
        return 0;
    }
    (rate * SCALE as f64) as u64
}

fn nanos_since(base: Instant) -> u64 {
    let nanos = base.elapsed().as_nanos();
    nanos.min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::TokenBucketRateLimiter;

    #[test]
    fn try_acquire_respects_capacity() {
        let limiter = TokenBucketRateLimiter::new(1.0, 2);
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn reconfigure_caps_tokens() {
        let limiter = TokenBucketRateLimiter::new(100.0, 10);
        limiter.reconfigure(100.0, 1);
        assert!(limiter.available_tokens() <= 1.0);
    }

    proptest! {
        #[test]
        fn burst_never_exceeds_capacity(capacity in 1u64..64) {
            let limiter = TokenBucketRateLimiter::new(1.0, capacity);
            let mut acquired = 0;
            while limiter.try_acquire() {
                acquired += 1;
            }
            prop_assert_eq!(acquired, capacity);
            prop_assert!(limiter.available_tokens() < 1.0);
        }
    }
}
