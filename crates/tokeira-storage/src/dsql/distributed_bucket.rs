//! The cluster-wide connection-creation rate limiter.
//!
//! DSQL admits 100 new connections a second per cluster, with a burst of
//! 1,000, and refuses the rest with SQLSTATE `53400`. Every refiller in the
//! deployment shares that rate through one DynamoDB item per DSQL endpoint,
//! `dsql_connect_gcra#{endpoint}`, which holds a single time: the arrival time
//! (`tat_us`, microseconds since the Unix epoch).
//!
//! The limiter is the generic cell rate algorithm. Each admission is paid for
//! by one interval of the sustained rate, and the arrival time is the time by
//! which every admission granted so far would have been paid for. An admission
//! at time `t` may run ahead of payment by the burst less one interval, the
//! tolerance:
//!
//! - an arrival time before `t` is an idle bucket: the admission runs now, and
//!   the arrival time becomes `t + interval`;
//! - an arrival time between `t` and `t + tolerance + RATE_MAX_WAIT` is a busy
//!   bucket: the admission is scheduled at `arrival time − tolerance` (now, if
//!   that has passed) and the arrival time moves on by one interval;
//! - a later arrival time would schedule the admission more than
//!   [`RATE_MAX_WAIT`] ahead, and the admission is refused.
//!
//! Each branch is one conditional update, and no read precedes it: the node
//! guesses the branch from the last arrival time it saw, and a wrong guess
//! fails its condition and returns the stored item, which says which branch is
//! right. An admission therefore takes one update, or two when the guess was
//! wrong. Concurrent busy admissions from any number of nodes each succeed
//! first time, because the busy update adds an interval to whatever is stored
//! and DynamoDB serializes the updates on the item.
//!
//! One stored time and the admitting node's own clock decide every admission,
//! so two nodes whose clocks differ by δ disagree about the bucket by at most
//! δ's worth of admissions, a fixed offset that does not grow with traffic.
//!
//! A DynamoDB failure other than a failed condition (after the SDK's own
//! retries) puts the node on a process-local limiter for [`FALLBACK_PERIOD`],
//! at its connection budget directive's rate share, or [`FALLBACK_RATE`]
//! without one. Refills continue while DynamoDB is down, and DSQL refuses
//! whatever exceeds its rate.

use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use aws_sdk_dynamodb::{
    Client,
    operation::update_item::UpdateItemError,
    types::{AttributeValue, ReturnValue, ReturnValuesOnConditionCheckFailure},
};
use thiserror::Error;

use crate::metrics;

/// Longest an admission may be scheduled ahead of the moment it was asked for.
/// A refiller refused for waiting longer backs off and asks again.
pub const RATE_MAX_WAIT: Duration = Duration::from_secs(30);
/// Conditional updates one admission may spend before it is refused. Two
/// suffice unless other nodes keep changing the bucket's branch in between.
pub const MAX_ATTEMPTS: u32 = 4;
/// How long a node admits through its fallback limiter after a DynamoDB error.
pub const FALLBACK_PERIOD: Duration = Duration::from_secs(30);
/// Fallback rate for a node that has received no connection budget directive.
/// Ten nodes falling back together stay within the cluster's 100 a second.
pub const FALLBACK_RATE: f64 = 10.0;

/// An item untouched for an hour expires; a missing item is an idle bucket,
/// which is what an hour without admissions is.
const ITEM_TTL_SECONDS: i64 = 3_600;
const ITEM_KEY_PREFIX: &str = "dsql_connect_gcra#";
const ARRIVAL_TIME: &str = "tat_us";

/// Why an admission was refused. Either way the refiller treats it as a
/// failed refill attempt and backs off.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AdmissionError {
    /// The bucket would schedule the admission more than [`RATE_MAX_WAIT`]
    /// ahead.
    #[error("the DSQL connection rate limiter would schedule this connect more than {0:?} ahead")]
    WaitTooLong(Duration),
    /// Every conditional update failed because other nodes kept changing the
    /// bucket's branch.
    #[error("the DSQL connection rate limiter item changed under each of {0} conditional updates")]
    Contended(u32),
}

/// How a conditional update on the arrival-time item failed.
#[derive(Debug)]
pub(crate) enum UpdateFailure {
    /// The condition did not hold. Carries the item's arrival time, or `None`
    /// when there is no item or it holds no arrival time.
    Condition { old_tat_us: Option<i64> },
    /// The store itself failed, after any retries of its own.
    Store(anyhow::Error),
}

/// The conditional updates the bucket makes, with DynamoDB's semantics: each
/// applies atomically, and a failed condition returns the item as it was.
#[async_trait]
pub(crate) trait ArrivalTimeStore: fmt::Debug + Send + Sync {
    /// Busy update: add `interval_us` to the stored arrival time if it lies in
    /// `[now_us, limit_us]`, and return the new arrival time.
    async fn advance(
        &self,
        key: &str,
        interval_us: i64,
        now_us: i64,
        limit_us: i64,
        ttl_epoch: i64,
    ) -> Result<i64, UpdateFailure>;

    /// Idle update: set the arrival time to `next_us` if there is none or it
    /// is before `now_us`.
    async fn reset(
        &self,
        key: &str,
        next_us: i64,
        now_us: i64,
        ttl_epoch: i64,
    ) -> Result<(), UpdateFailure>;

    /// Check the store can be reached, before the node starts.
    async fn validate(&self) -> Result<()>;
}

/// The wall clock admissions are scheduled against, in microseconds since the
/// Unix epoch. Nodes compare it against one another's through the stored
/// arrival time, so it must be the host's synchronized clock, not a monotonic
/// one.
pub(crate) trait WallClock: fmt::Debug + Send + Sync {
    fn now_us(&self) -> i64;
}

#[derive(Debug)]
struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now_us(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| {
                i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX)
            })
    }
}

/// One reserved admission, before the caller waits for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Reservation {
    /// When the admission may run, on the reserving node's clock.
    pub(crate) scheduled_us: i64,
    /// When it was asked for, on the same clock.
    pub(crate) requested_us: i64,
    /// Conditional updates it took.
    pub(crate) updates: u32,
}

/// The cluster-wide connection-creation limiter, shared by every node through
/// one DynamoDB item per DSQL endpoint.
pub struct DistributedTokenBucket {
    store: Arc<dyn ArrivalTimeStore>,
    clock: Arc<dyn WallClock>,
    key: String,
    rate_per_second: f64,
    interval_us: i64,
    tolerance_us: i64,
    horizon_us: i64,
    /// The latest arrival time this node saw, which guesses the branch of its
    /// next admission. A stale guess costs one failed condition, never a
    /// wrong admission.
    last_seen_tat_us: AtomicI64,
    fallback: FallbackLimiter,
}

impl fmt::Debug for DistributedTokenBucket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DistributedTokenBucket")
            .field("store", &self.store)
            .field("rate_per_second", &self.rate_per_second)
            .field("interval_us", &self.interval_us)
            .field("tolerance_us", &self.tolerance_us)
            .finish_non_exhaustive()
    }
}

impl DistributedTokenBucket {
    /// The production limiter over the DynamoDB table `table_name`, keyed by
    /// the DSQL `endpoint` it guards. `rate_per_second` and `burst_capacity`
    /// are the cluster's connection rate and burst.
    pub fn new(
        client: Client,
        table_name: impl Into<String>,
        endpoint: impl AsRef<str>,
        rate_per_second: f64,
        burst_capacity: u64,
    ) -> Self {
        Self::with_store(
            Arc::new(DynamoDbArrivalTime {
                client,
                table_name: table_name.into(),
            }),
            Arc::new(SystemWallClock),
            endpoint.as_ref(),
            rate_per_second,
            burst_capacity,
        )
    }

    pub(crate) fn with_store(
        store: Arc<dyn ArrivalTimeStore>,
        clock: Arc<dyn WallClock>,
        endpoint: &str,
        rate_per_second: f64,
        burst_capacity: u64,
    ) -> Self {
        // Whole microseconds keep DynamoDB Number attributes integral. At the
        // cluster's 100 a second the interval is exactly 10 ms.
        let interval_us = (1_000_000.0 / rate_per_second.max(f64::MIN_POSITIVE))
            .round()
            .clamp(1.0, i64::MAX as f64) as i64;
        let tolerance_us = interval_us
            .saturating_mul(i64::try_from(burst_capacity.max(1) - 1).unwrap_or(i64::MAX));
        let max_wait_us = i64::try_from(RATE_MAX_WAIT.as_micros()).unwrap_or(i64::MAX);
        metrics::record_dsql_pool_rate_limiter(burst_capacity as f64, rate_per_second);
        metrics::set_dsql_pool_rate_limiter_fallback(false);
        Self {
            store,
            clock,
            key: format!("{ITEM_KEY_PREFIX}{endpoint}"),
            rate_per_second,
            interval_us,
            tolerance_us,
            horizon_us: tolerance_us.saturating_add(max_wait_us),
            last_seen_tat_us: AtomicI64::new(i64::MIN),
            fallback: FallbackLimiter::default(),
        }
    }

    /// A limiter over an item held in this process, on a clock that follows
    /// Tokio's (so paused-time tests advance it), for tests that need no
    /// DynamoDB.
    #[cfg(any(test, feature = "dsql-integration"))]
    pub fn local_for_tests(rate_per_second: f64, burst_capacity: u64) -> Self {
        Self::with_store(
            Arc::new(InProcessArrivalTime::default()),
            Arc::new(VirtualWallClock::new(0)),
            "local-test",
            rate_per_second,
            burst_capacity,
        )
    }

    /// Fail startup unless the rate limiter table can be reached.
    pub async fn validate_table(&self) -> Result<()> {
        self.store.validate().await
    }

    /// The rate the fallback limiter admits at: this node's share of the
    /// cluster's connection rate from its latest connection budget directive.
    /// A rate that is not a positive number is ignored.
    pub fn set_fallback_rate(&self, rate_per_second: f64) {
        self.fallback.set_rate(rate_per_second);
    }

    /// Wait until one connection creation is admitted, cluster-wide.
    ///
    /// A refused admission (scheduled too far ahead, or contended past
    /// [`MAX_ATTEMPTS`]) changes nothing and returns at once. A DynamoDB error
    /// puts the node on the fallback limiter, which admits instead.
    pub async fn admit(&self) -> Result<(), AdmissionError> {
        match self.reserve().await {
            Ok(Some(reservation)) => {
                let wait = reservation
                    .scheduled_us
                    .saturating_sub(reservation.requested_us);
                if wait > 0 {
                    metrics::record_dsql_rate_limiter_throttled();
                    let wait = Duration::from_micros(u64::try_from(wait).unwrap_or(0));
                    tokio::time::sleep(wait).await;
                    metrics::record_dsql_rate_limiter_throttle_duration(wait);
                }
                Ok(())
            }
            Ok(None) => self.fallback.admit().await,
            Err(error) => Err(error),
        }
    }

    /// Reserve one admission without waiting for it. `Ok(None)` means the
    /// node is on its fallback limiter, which admits instead.
    pub(crate) async fn reserve(&self) -> Result<Option<Reservation>, AdmissionError> {
        if self.fallback.active() {
            return Ok(None);
        }
        let mut busy = self.last_seen_tat_us.load(Ordering::Acquire) >= self.clock.now_us();
        for attempt in 1..=MAX_ATTEMPTS {
            let now = self.clock.now_us();
            let ttl_epoch = now / 1_000_000 + ITEM_TTL_SECONDS;
            let outcome = if busy {
                self.store
                    .advance(
                        &self.key,
                        self.interval_us,
                        now,
                        now.saturating_add(self.horizon_us),
                        ttl_epoch,
                    )
                    .await
                    .map(|tat| {
                        let previous = tat - self.interval_us;
                        (tat, now.max(previous - self.tolerance_us))
                    })
            } else {
                let tat = now + self.interval_us;
                self.store
                    .reset(&self.key, tat, now, ttl_epoch)
                    .await
                    .map(|()| (tat, now))
            };
            match outcome {
                Ok((tat, scheduled_us)) => {
                    self.last_seen_tat_us.store(tat, Ordering::Release);
                    self.fallback.store_answered();
                    let tokens = ((now + self.tolerance_us - tat) / self.interval_us + 1).max(0);
                    metrics::record_dsql_pool_rate_limiter(tokens as f64, self.rate_per_second);
                    metrics::set_dsql_rate_limiter_tokens_remaining(tokens as f64);
                    return Ok(Some(Reservation {
                        scheduled_us,
                        requested_us: now,
                        updates: attempt,
                    }));
                }
                // A failed condition is not an error: the item as it was says
                // which branch is right.
                Err(UpdateFailure::Condition { old_tat_us }) => match old_tat_us {
                    Some(old) if old >= now => {
                        if old > now.saturating_add(self.horizon_us) {
                            return Err(AdmissionError::WaitTooLong(RATE_MAX_WAIT));
                        }
                        self.last_seen_tat_us.store(old, Ordering::Release);
                        busy = true;
                    }
                    _ => busy = false,
                },
                Err(UpdateFailure::Store(error)) => {
                    self.fallback.begin(&error);
                    return Ok(None);
                }
            }
        }
        Err(AdmissionError::Contended(MAX_ATTEMPTS))
    }

    #[cfg(test)]
    fn fallback_active(&self) -> bool {
        self.fallback.active()
    }
}

/// The process-local limiter a node admits through while DynamoDB fails.
///
/// The same algorithm over an arrival time held in memory, measured on Tokio's
/// monotonic clock. Its burst is one second's admissions, `max(1, ⌈rate⌉)`.
#[derive(Debug, Default)]
struct FallbackLimiter {
    /// End of the current fallback period.
    until: Mutex<Option<tokio::time::Instant>>,
    /// Set while the fallback gauge reads one, until DynamoDB answers again.
    engaged: AtomicBool,
    /// The directive's rate share as `f64` bits; zero means no directive.
    rate_bits: AtomicU64,
    gcra: Mutex<LocalGcra>,
}

impl FallbackLimiter {
    fn rate(&self) -> f64 {
        match self.rate_bits.load(Ordering::Acquire) {
            0 => FALLBACK_RATE,
            bits => f64::from_bits(bits),
        }
    }

    fn set_rate(&self, rate_per_second: f64) {
        if rate_per_second.is_finite() && rate_per_second > 0.0 {
            self.rate_bits
                .store(rate_per_second.to_bits(), Ordering::Release);
        }
    }

    fn active(&self) -> bool {
        let until = self.until.lock().expect("fallback period lock poisoned");
        until.is_some_and(|until| tokio::time::Instant::now() < until)
    }

    fn begin(&self, error: &anyhow::Error) {
        let rate = self.rate();
        *self.until.lock().expect("fallback period lock poisoned") =
            Some(tokio::time::Instant::now() + FALLBACK_PERIOD);
        metrics::set_dsql_pool_rate_limiter_fallback(true);
        self.engaged.store(true, Ordering::Release);
        tracing::warn!(
            error = format!("{error:#}"),
            fallback_rate_per_second = rate,
            fallback_period_seconds = FALLBACK_PERIOD.as_secs(),
            "the DSQL connection rate limiter could not reach DynamoDB; admitting connection \
             creations through the process-local limiter, then trying DynamoDB again"
        );
    }

    fn store_answered(&self) {
        if self.engaged.swap(false, Ordering::AcqRel) {
            metrics::set_dsql_pool_rate_limiter_fallback(false);
            tracing::info!("the DSQL connection rate limiter reached DynamoDB again");
        }
    }

    async fn admit(&self) -> Result<(), AdmissionError> {
        let now = tokio::time::Instant::now();
        let wait = self
            .gcra
            .lock()
            .expect("fallback limiter lock poisoned")
            .reserve(now, self.rate())?;
        if !wait.is_zero() {
            metrics::record_dsql_rate_limiter_throttled();
            tokio::time::sleep(wait).await;
            metrics::record_dsql_rate_limiter_throttle_duration(wait);
        }
        Ok(())
    }
}

/// The cell rate algorithm over an arrival time held in memory.
#[derive(Debug, Default)]
struct LocalGcra {
    tat: Option<tokio::time::Instant>,
}

impl LocalGcra {
    /// Reserve one admission at `now` at `rate` a second, with a burst of one
    /// second's admissions; returns how long the caller waits for it.
    fn reserve(
        &mut self,
        now: tokio::time::Instant,
        rate: f64,
    ) -> Result<Duration, AdmissionError> {
        let interval = Duration::from_secs_f64(1.0 / rate);
        let burst = rate.ceil().clamp(1.0, f64::from(u32::MAX)) as u32;
        let tolerance = interval.saturating_mul(burst - 1);
        let tat = self.tat.map_or(now, |tat| tat.max(now));
        let scheduled = tat
            .checked_sub(tolerance)
            .map_or(now, |earliest| earliest.max(now));
        let wait = scheduled - now;
        if wait > RATE_MAX_WAIT {
            return Err(AdmissionError::WaitTooLong(RATE_MAX_WAIT));
        }
        self.tat = Some(tat + interval);
        Ok(wait)
    }
}

/// The arrival-time item in DynamoDB.
#[derive(Debug)]
struct DynamoDbArrivalTime {
    client: Client,
    table_name: String,
}

#[async_trait]
impl ArrivalTimeStore for DynamoDbArrivalTime {
    async fn advance(
        &self,
        key: &str,
        interval_us: i64,
        now_us: i64,
        limit_us: i64,
        ttl_epoch: i64,
    ) -> Result<i64, UpdateFailure> {
        let output = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key("pk", AttributeValue::S(key.to_owned()))
            .update_expression("SET tat_us = tat_us + :interval, ttl_epoch = :ttl")
            .condition_expression("tat_us BETWEEN :now AND :limit")
            .expression_attribute_values(":interval", number(interval_us))
            .expression_attribute_values(":ttl", number(ttl_epoch))
            .expression_attribute_values(":now", number(now_us))
            .expression_attribute_values(":limit", number(limit_us))
            .return_values(ReturnValue::UpdatedNew)
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .send()
            .await
            .map_err(|error| self.update_failure(error))?;
        arrival_time(output.attributes()).ok_or_else(|| {
            UpdateFailure::Store(anyhow!(
                "DynamoDB returned no arrival time from the DSQL rate limiter table {}",
                self.table_name
            ))
        })
    }

    async fn reset(
        &self,
        key: &str,
        next_us: i64,
        now_us: i64,
        ttl_epoch: i64,
    ) -> Result<(), UpdateFailure> {
        self.client
            .update_item()
            .table_name(&self.table_name)
            .key("pk", AttributeValue::S(key.to_owned()))
            .update_expression("SET tat_us = :next, ttl_epoch = :ttl")
            .condition_expression("attribute_not_exists(tat_us) OR tat_us < :now")
            .expression_attribute_values(":next", number(next_us))
            .expression_attribute_values(":ttl", number(ttl_epoch))
            .expression_attribute_values(":now", number(now_us))
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .send()
            .await
            .map_err(|error| self.update_failure(error))?;
        Ok(())
    }

    async fn validate(&self) -> Result<()> {
        self.client
            .describe_table()
            .table_name(&self.table_name)
            .send()
            .await
            .with_context(|| {
                format!(
                    "DynamoDB table '{}' is unreachable. DSQL deployments require the DynamoDB \
                     rate limiter table. Run 'tkr infra apply' to provision it",
                    self.table_name
                )
            })?;
        Ok(())
    }
}

impl DynamoDbArrivalTime {
    fn update_failure(
        &self,
        error: aws_sdk_dynamodb::error::SdkError<UpdateItemError>,
    ) -> UpdateFailure {
        if let Some(UpdateItemError::ConditionalCheckFailedException(failed)) =
            error.as_service_error()
        {
            return UpdateFailure::Condition {
                old_tat_us: arrival_time(failed.item()),
            };
        }
        UpdateFailure::Store(anyhow::Error::new(error).context(format!(
            "failed to update the DSQL rate limiter table {}",
            self.table_name
        )))
    }
}

fn number(value: i64) -> AttributeValue {
    AttributeValue::N(value.to_string())
}

fn arrival_time(item: Option<&HashMap<String, AttributeValue>>) -> Option<i64> {
    item?.get(ARRIVAL_TIME)?.as_n().ok()?.parse().ok()
}

/// The arrival-time item held in this process, with DynamoDB's conditional
/// update semantics, so tests run the production algorithm without DynamoDB.
#[cfg(any(test, feature = "dsql-integration"))]
#[derive(Debug, Default)]
pub(crate) struct InProcessArrivalTime {
    items: Mutex<HashMap<String, i64>>,
    updates: std::sync::atomic::AtomicUsize,
    failing: AtomicBool,
}

#[cfg(any(test, feature = "dsql-integration"))]
impl InProcessArrivalTime {
    /// Make every update fail as an unreachable table does, or stop doing so.
    #[cfg(test)]
    pub(crate) fn set_failing(&self, failing: bool) {
        self.failing.store(failing, Ordering::Release);
    }

    /// Conditional updates attempted so far, failed ones included.
    #[cfg(test)]
    pub(crate) fn updates(&self) -> usize {
        self.updates.load(Ordering::Acquire)
    }

    fn update<T>(
        &self,
        key: &str,
        apply: impl FnOnce(Option<i64>) -> Option<(i64, T)>,
    ) -> Result<T, UpdateFailure> {
        self.updates.fetch_add(1, Ordering::AcqRel);
        if self.failing.load(Ordering::Acquire) {
            return Err(UpdateFailure::Store(anyhow!(
                "the in-process rate limiter item is failing"
            )));
        }
        let mut items = self.items.lock().expect("arrival-time items lock poisoned");
        let old = items.get(key).copied();
        let Some((tat, value)) = apply(old) else {
            return Err(UpdateFailure::Condition { old_tat_us: old });
        };
        items.insert(key.to_owned(), tat);
        Ok(value)
    }
}

#[cfg(any(test, feature = "dsql-integration"))]
#[async_trait]
impl ArrivalTimeStore for InProcessArrivalTime {
    async fn advance(
        &self,
        key: &str,
        interval_us: i64,
        now_us: i64,
        limit_us: i64,
        _ttl_epoch: i64,
    ) -> Result<i64, UpdateFailure> {
        self.update(key, |old| {
            let old = old.filter(|old| (now_us..=limit_us).contains(old))?;
            Some((old + interval_us, old + interval_us))
        })
    }

    async fn reset(
        &self,
        key: &str,
        next_us: i64,
        now_us: i64,
        _ttl_epoch: i64,
    ) -> Result<(), UpdateFailure> {
        self.update(key, |old| {
            old.is_none_or(|old| old < now_us).then_some((next_us, ()))
        })
    }

    async fn validate(&self) -> Result<()> {
        if self.failing.load(Ordering::Acquire) {
            anyhow::bail!("the in-process rate limiter item is failing");
        }
        Ok(())
    }
}

/// A wall clock that follows Tokio's clock from a fixed origin, offset by a
/// fixed skew, so paused-time tests can run nodes with differing clocks.
#[cfg(any(test, feature = "dsql-integration"))]
#[derive(Debug)]
pub(crate) struct VirtualWallClock {
    origin_us: i64,
    origin: tokio::time::Instant,
}

#[cfg(any(test, feature = "dsql-integration"))]
impl VirtualWallClock {
    /// A clock reading the system time now, plus `skew_us`.
    pub(crate) fn new(skew_us: i64) -> Self {
        Self {
            origin_us: SystemWallClock.now_us() + skew_us,
            origin: tokio::time::Instant::now(),
        }
    }

    /// A clock reading `origin_us` now.
    #[cfg(test)]
    pub(crate) fn at(origin_us: i64) -> Self {
        Self {
            origin_us,
            origin: tokio::time::Instant::now(),
        }
    }
}

#[cfg(any(test, feature = "dsql-integration"))]
impl WallClock for VirtualWallClock {
    fn now_us(&self) -> i64 {
        self.origin_us + i64::try_from(self.origin.elapsed().as_micros()).unwrap_or(i64::MAX)
    }
}

#[cfg(test)]
mod tests;
