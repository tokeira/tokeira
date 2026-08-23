//! Process-local DSQL connection-creation admission for embedded engines.
//!
//! This module is deliberately absent from the distributed `tokeirad` path.
//! Embedded mode uses a monotonic token bucket and atomic slot budget without
//! constructing DynamoDB resources or changing the established distributed
//! reservoir, rate limiter, or slot-block manager.

use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use async_trait::async_trait;
use tokio::sync::Notify;

use super::{MonotonicClock, SystemMonotonicClock};

/// Admission boundary used immediately before opening a physical connection.
///
/// Callers must acquire a physical slot before a creation-rate token and must
/// release exactly one slot on every path that discards or fails to create the
/// corresponding connection.
#[async_trait]
pub(crate) trait EmbeddedConnectionCoordinator: fmt::Debug + Send + Sync {
    /// Validate process-local admission state before reservoir warmup.
    async fn validate(&self) -> Result<()>;

    /// Reserve capacity for one physical connection.
    async fn acquire_slot(&self) -> Result<()>;

    /// Admit one physical connection creation attempt.
    async fn acquire_creation_token(&self) -> Result<()>;

    /// Release capacity for exactly one retired or failed connection.
    fn release_slot(&self);

    /// Number of physical slots currently charged to this coordinator.
    fn used_slots(&self) -> usize;

    /// Stop new admissions and verify that every local slot was released.
    async fn shutdown(&self) -> Result<()>;
}

#[derive(Clone, Copy, Debug)]
struct TokenBucketState {
    tokens: f64,
    updated_at: Instant,
}

impl TokenBucketState {
    fn consume(
        &mut self,
        now: Instant,
        rate_per_second: f64,
        burst_capacity: f64,
    ) -> Option<Duration> {
        let elapsed = now.saturating_duration_since(self.updated_at).as_secs_f64();
        self.tokens = (self.tokens + elapsed * rate_per_second).min(burst_capacity);
        self.updated_at = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            return None;
        }
        Some(Duration::from_secs_f64(
            ((1.0 - self.tokens) / rate_per_second).max(0.001),
        ))
    }
}

/// DynamoDB-free coordinator for one exclusive embedded engine process.
pub(crate) struct ProcessLocalConnectionCoordinator {
    max_slots: usize,
    used_slots: AtomicUsize,
    rate_per_second: f64,
    burst_capacity: f64,
    bucket: Mutex<TokenBucketState>,
    clock: Arc<dyn MonotonicClock>,
    slot_changed: Notify,
    rate_changed: Notify,
    closed: AtomicBool,
}

impl fmt::Debug for ProcessLocalConnectionCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcessLocalConnectionCoordinator")
            .field("max_slots", &self.max_slots)
            .field("used_slots", &self.used_slots.load(Ordering::Acquire))
            .field("rate_per_second", &self.rate_per_second)
            .field("burst_capacity", &self.burst_capacity)
            .field("closed", &self.closed.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl ProcessLocalConnectionCoordinator {
    /// Construct production process-local admission using the system monotonic clock.
    pub(crate) fn new(max_slots: usize, rate_per_second: f64, burst_capacity: u64) -> Result<Self> {
        Self::with_clock(
            max_slots,
            rate_per_second,
            burst_capacity,
            Arc::new(SystemMonotonicClock),
        )
    }

    /// Construct process-local admission with an injected monotonic clock.
    pub(crate) fn with_clock(
        max_slots: usize,
        rate_per_second: f64,
        burst_capacity: u64,
        clock: Arc<dyn MonotonicClock>,
    ) -> Result<Self> {
        if max_slots == 0 {
            bail!("process-local DSQL max_slots must be greater than zero");
        }
        if !rate_per_second.is_finite() || rate_per_second <= 0.0 {
            bail!("process-local DSQL connection rate must be finite and positive");
        }
        if burst_capacity == 0 {
            bail!("process-local DSQL burst capacity must be greater than zero");
        }
        let now = clock.now();
        Ok(Self {
            max_slots,
            used_slots: AtomicUsize::new(0),
            rate_per_second,
            burst_capacity: burst_capacity as f64,
            bucket: Mutex::new(TokenBucketState {
                // Embedded startup is deliberately allowed exactly the configured burst.
                tokens: burst_capacity as f64,
                updated_at: now,
            }),
            clock,
            slot_changed: Notify::new(),
            rate_changed: Notify::new(),
            closed: AtomicBool::new(false),
        })
    }

    fn try_acquire_slot(&self) -> Result<bool> {
        if self.closed.load(Ordering::Acquire) {
            bail!("process-local DSQL connection coordinator is closed");
        }
        let mut used = self.used_slots.load(Ordering::Acquire);
        while used < self.max_slots {
            match self.used_slots.compare_exchange(
                used,
                used + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(true),
                Err(actual) => used = actual,
            }
        }
        Ok(false)
    }

    fn token_wait(&self) -> Result<Option<Duration>> {
        if self.closed.load(Ordering::Acquire) {
            bail!("process-local DSQL connection coordinator is closed");
        }
        let mut bucket = self
            .bucket
            .lock()
            .map_err(|_| anyhow::anyhow!("process-local DSQL token bucket is poisoned"))?;
        Ok(bucket.consume(self.clock.now(), self.rate_per_second, self.burst_capacity))
    }
}

#[async_trait]
impl EmbeddedConnectionCoordinator for ProcessLocalConnectionCoordinator {
    async fn validate(&self) -> Result<()> {
        Ok(())
    }

    async fn acquire_slot(&self) -> Result<()> {
        loop {
            let notified = self.slot_changed.notified();
            if self.try_acquire_slot()? {
                return Ok(());
            }
            notified.await;
        }
    }

    async fn acquire_creation_token(&self) -> Result<()> {
        loop {
            let notified = self.rate_changed.notified();
            let Some(wait_for) = self.token_wait()? else {
                return Ok(());
            };
            tokio::select! {
                () = tokio::time::sleep(wait_for) => {}
                () = notified => {}
            }
        }
    }

    fn release_slot(&self) {
        let mut used = self.used_slots.load(Ordering::Acquire);
        while used > 0 {
            match self.used_slots.compare_exchange(
                used,
                used - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.slot_changed.notify_one();
                    return;
                }
                Err(actual) => used = actual,
            }
        }
        tracing::warn!("ignored unmatched process-local DSQL slot release");
    }

    fn used_slots(&self) -> usize {
        self.used_slots.load(Ordering::Acquire)
    }

    async fn shutdown(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        self.slot_changed.notify_waiters();
        self.rate_changed.notify_waiters();
        if self.used_slots() != 0 {
            bail!(
                "process-local DSQL coordinator closed with {} physical slots still in use",
                self.used_slots()
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[derive(Clone, Copy, Debug)]
    enum AccountingEvent {
        Create,
        Checkout,
        Return,
        Retire,
        FailedCreate,
        LeakResolved,
        Shutdown,
    }

    fn accounting_event() -> impl Strategy<Value = AccountingEvent> {
        prop_oneof![
            Just(AccountingEvent::Create),
            Just(AccountingEvent::Checkout),
            Just(AccountingEvent::Return),
            Just(AccountingEvent::Retire),
            Just(AccountingEvent::FailedCreate),
            Just(AccountingEvent::LeakResolved),
            Just(AccountingEvent::Shutdown),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: managed-embedded-dsql, Property 9: process-local creation limiting obeys rate and burst
        #[test]
        fn process_local_creation_limiting_obeys_rate_and_burst(
            rate in 0.1f64..32.0,
            burst in 1u64..16,
            advances_ms in prop::collection::vec(0u64..2_000, 1..128),
        ) {
            let origin = Instant::now();
            let mut actual = TokenBucketState {
                tokens: burst as f64,
                updated_at: origin,
            };
            let mut reference_tokens = burst as f64;
            let mut reference_at = origin;
            let mut now = origin;

            for advance_ms in advances_ms {
                now += Duration::from_millis(advance_ms);
                let actual_wait = actual.consume(now, rate, burst as f64);

                let elapsed = now.saturating_duration_since(reference_at).as_secs_f64();
                reference_tokens = (reference_tokens + elapsed * rate).min(burst as f64);
                reference_at = now;
                let reference_admitted = reference_tokens >= 1.0;
                if reference_admitted {
                    reference_tokens -= 1.0;
                }

                prop_assert_eq!(actual_wait.is_none(), reference_admitted);
                prop_assert!(actual.tokens >= 0.0);
                prop_assert!(actual.tokens <= burst as f64);
                prop_assert!((actual.tokens - reference_tokens).abs() < 1e-9);
            }
        }

        // Feature: managed-embedded-dsql, Property 10: connection slot and class accounting is conserved
        #[test]
        fn connection_slot_and_class_accounting_is_conserved(
            slot_limit in 1usize..32,
            class_limit in 1usize..32,
            events in prop::collection::vec(accounting_event(), 1..256),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("property runtime");
            runtime.block_on(async move {
                use std::collections::HashMap;

                use crate::{DbClass, dsql::connection::ClassBudgets};

                let coordinator = ProcessLocalConnectionCoordinator::new(slot_limit, 8.0, 2)
                    .expect("valid coordinator");
                let mut allocations = HashMap::new();
                allocations.insert(DbClass::Control, 1);
                allocations.insert(DbClass::Commit, class_limit);
                allocations.insert(DbClass::Read, 1);
                allocations.insert(DbClass::Projection, 1);
                allocations.insert(DbClass::Maintenance, 1);
                let budgets = ClassBudgets::new(&allocations).expect("valid class budgets");
                let mut class_permits = Vec::new();
                let mut physical = 0usize;
                let mut closed = false;

                for event in events {
                    match event {
                        AccountingEvent::Create if !closed => {
                            let admitted = coordinator.try_acquire_slot().expect("open coordinator");
                            if physical < slot_limit {
                                prop_assert!(admitted);
                                physical += 1;
                            } else {
                                prop_assert!(!admitted);
                            }
                        }
                        AccountingEvent::Checkout
                            if !closed
                                && class_permits.len() < class_limit
                                && class_permits.len() < physical =>
                        {
                            class_permits.push(
                                budgets.acquire(DbClass::Commit).await.expect("available class"),
                            );
                        }
                        AccountingEvent::Return if !class_permits.is_empty() => {
                            class_permits.pop();
                        }
                        AccountingEvent::Retire if physical > class_permits.len() => {
                            coordinator.release_slot();
                            physical -= 1;
                        }
                        AccountingEvent::FailedCreate if !closed => {
                            if coordinator.try_acquire_slot().expect("open coordinator") {
                                coordinator.release_slot();
                            }
                        }
                        AccountingEvent::LeakResolved if !class_permits.is_empty() => {
                            class_permits.pop();
                            coordinator.release_slot();
                            physical -= 1;
                        }
                        AccountingEvent::Shutdown => {
                            closed = true;
                            while physical > class_permits.len() {
                                coordinator.release_slot();
                                physical -= 1;
                            }
                        }
                        _ => {}
                    }
                    prop_assert_eq!(coordinator.used_slots(), physical);
                    prop_assert!(physical <= slot_limit);
                    prop_assert!(class_permits.len() <= class_limit);
                    prop_assert!(class_permits.len() <= physical);
                    prop_assert_eq!(
                        budgets.class_available(DbClass::Commit).await,
                        Some(class_limit - class_permits.len())
                    );
                }

                while let Some(permit) = class_permits.pop() {
                    drop(permit);
                    coordinator.release_slot();
                }
                while coordinator.used_slots() > 0 {
                    coordinator.release_slot();
                }
                coordinator.shutdown().await.expect("conserved shutdown");
                Ok(())
            })?;
        }
    }

    #[tokio::test]
    async fn process_local_slots_are_bounded_and_shutdown_reaches_zero() {
        let coordinator = ProcessLocalConnectionCoordinator::new(2, 8.0, 2)
            .expect("valid process-local coordinator");
        coordinator.acquire_slot().await.expect("first slot");
        coordinator.acquire_slot().await.expect("second slot");
        assert_eq!(coordinator.used_slots(), 2);

        coordinator.release_slot();
        coordinator.release_slot();
        assert_eq!(coordinator.used_slots(), 0);
        coordinator
            .shutdown()
            .await
            .expect("zero-resource shutdown");
        assert!(coordinator.acquire_slot().await.is_err());
    }

    #[tokio::test]
    async fn process_local_bucket_starts_with_exact_burst() {
        let coordinator = ProcessLocalConnectionCoordinator::new(1, 0.01, 3)
            .expect("valid process-local coordinator");
        for _ in 0..3 {
            coordinator
                .acquire_creation_token()
                .await
                .expect("initial burst token");
        }
        let bucket = coordinator.bucket.lock().expect("token bucket");
        assert!(bucket.tokens < 1.0);
        assert!(bucket.tokens >= 0.0);
    }
}
