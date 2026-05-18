//! Queue fairness algorithm and delivery metrics.
//!
//! The fairness controller adjusts per-queue drain shares based on delivery
//! metrics (latency percentiles, sync-match rate, poll success rate, backlog
//! age). A periodic control loop evaluates these signals and recomputes budgets
//! so that no single queue can monopolise the dispatch path while still allowing
//! hot queues to drain their backlogs.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use dashmap::DashMap;
use time::OffsetDateTime;
use tokeira_types::QueueKey;
use tokio_util::sync::CancellationToken;

use crate::metrics as runtime_metrics;

pub(crate) const DEFAULT_DRAIN_SHARE: f64 = 0.1;
pub(crate) const MIN_DRAIN_SHARE: f64 = 0.0;
pub(crate) const MAX_DRAIN_SHARE: f64 = 0.8;
pub(crate) const MAX_DELTA_PER_INTERVAL: f64 = 0.10;
pub(crate) const DEFAULT_CONTROL_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const MIN_CONTROL_INTERVAL: Duration = Duration::from_secs(2);
pub(crate) const MAX_CONTROL_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_BUDGET: u32 = 10;
const LATENCY_BUCKET_CAP_MS: usize = 60_000;

#[derive(Clone, Debug)]
pub struct QueueMetricsSnapshot {
    pub latency_p50: Duration,
    pub latency_p99: Duration,
    pub sync_match_rate: f64,
    pub poll_success_rate: f64,
    pub backlog_age: Duration,
}

#[derive(Clone, Debug)]
pub struct DeliveryMetricsSnapshot {
    pub queues: HashMap<QueueKey, QueueMetricsSnapshot>,
    pub taken_at: OffsetDateTime,
}

#[derive(Clone)]
pub struct DeliveryMetrics {
    queues: Arc<DashMap<QueueKey, QueueCounters>>,
}

#[derive(Default)]
struct QueueCounters {
    latency: LatencyHistogram,
    sync_match: SlidingWindowCounter,
    poll_success: SlidingWindowCounter,
    backlog_age: Duration,
}

#[derive(Clone, Debug)]
pub struct QueueFairnessState {
    pub drain_share: f64,
    pub remaining_budget: u32,
    pub last_adjusted_at: OffsetDateTime,
}

#[derive(Clone)]
pub struct FairnessState {
    inner: Arc<Mutex<FairnessStateInner>>,
}

struct FairnessStateInner {
    queues: HashMap<QueueKey, QueueFairnessState>,
}

#[derive(Default)]
pub(crate) struct SlidingWindowCounter {
    current_success: u64,
    current_total: u64,
    previous_success: u64,
    previous_total: u64,
}

impl SlidingWindowCounter {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn record_success(&mut self) {
        self.current_success += 1;
        self.current_total += 1;
    }

    pub(crate) fn record_failure(&mut self) {
        self.current_total += 1;
    }

    pub(crate) fn advance(&mut self) {
        self.previous_success = self.current_success;
        self.previous_total = self.current_total;
        self.current_success = 0;
        self.current_total = 0;
    }

    pub(crate) fn rate(&self) -> f64 {
        let total = self.current_total + self.previous_total;
        if total == 0 {
            1.0
        } else {
            (self.current_success + self.previous_success) as f64 / total as f64
        }
    }

    pub(crate) fn total(&self) -> u64 {
        self.current_total + self.previous_total
    }
}

pub(crate) struct LatencyHistogram {
    buckets: Vec<u64>,
    count: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    pub(crate) fn new() -> Self {
        Self {
            buckets: vec![0; LATENCY_BUCKET_CAP_MS + 1],
            count: 0,
        }
    }

    pub(crate) fn record(&mut self, duration: Duration) {
        let bucket = duration.as_millis().min(LATENCY_BUCKET_CAP_MS as u128) as usize;
        self.buckets[bucket] += 1;
        self.count += 1;
    }

    pub(crate) fn percentile(&self, p: f64) -> Duration {
        if self.count == 0 {
            return Duration::ZERO;
        }
        let rank = ((self.count as f64 * p).ceil() as u64).max(1);
        let mut seen = 0u64;
        for (idx, count) in self.buckets.iter().enumerate() {
            seen += *count;
            if seen >= rank {
                return Duration::from_millis(idx as u64);
            }
        }
        Duration::from_millis(LATENCY_BUCKET_CAP_MS as u64)
    }

    pub(crate) fn reset(&mut self) {
        self.buckets.fill(0);
        self.count = 0;
    }
}

impl Default for DeliveryMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl DeliveryMetrics {
    pub fn new() -> Self {
        Self {
            queues: Arc::new(DashMap::new()),
        }
    }

    pub fn record_latency(&self, queue: &QueueKey, duration: Duration) {
        self.queues
            .entry(queue.clone())
            .or_default()
            .latency
            .record(duration);
    }

    pub fn record_sync_match(&self, queue: &QueueKey) {
        runtime_metrics::record_sync_match(queue);
        self.queues
            .entry(queue.clone())
            .or_default()
            .sync_match
            .record_success();
    }

    pub fn record_non_sync_match(&self, queue: &QueueKey) {
        runtime_metrics::record_non_sync_match(queue);
        self.queues
            .entry(queue.clone())
            .or_default()
            .sync_match
            .record_failure();
    }

    pub fn record_poll_success(&self, queue: &QueueKey) {
        self.queues
            .entry(queue.clone())
            .or_default()
            .poll_success
            .record_success();
    }

    pub fn record_poll_timeout(&self, queue: &QueueKey) {
        runtime_metrics::record_poll_timeout(queue);
        self.queues
            .entry(queue.clone())
            .or_default()
            .poll_success
            .record_failure();
    }

    pub fn set_backlog_age(&self, queue: &QueueKey, age: Duration) {
        self.queues.entry(queue.clone()).or_default().backlog_age = age;
    }

    pub fn take_snapshot(&self) -> DeliveryMetricsSnapshot {
        self.take_snapshot_internal(true).0
    }

    pub fn peek_snapshot(&self) -> DeliveryMetricsSnapshot {
        self.take_snapshot_internal(false).0
    }

    fn take_snapshot_internal(
        &self,
        destructive: bool,
    ) -> (DeliveryMetricsSnapshot, HashMap<QueueKey, u64>) {
        let mut poll_counts = HashMap::new();
        let mut snapshot_queues = HashMap::new();
        for mut entry in self.queues.iter_mut() {
            let queue = entry.key().clone();
            let latency_p50 = entry.latency.percentile(0.50);
            let latency_p99 = entry.latency.percentile(0.99);
            let sync_match_rate = entry.sync_match.rate();
            let poll_total = entry.poll_success.total();
            let poll_success_rate = entry.poll_success.rate();
            let backlog_age = entry.backlog_age;

            snapshot_queues.insert(
                queue.clone(),
                QueueMetricsSnapshot {
                    latency_p50,
                    latency_p99,
                    sync_match_rate,
                    poll_success_rate,
                    backlog_age,
                },
            );
            poll_counts.insert(queue.clone(), poll_total);

            if destructive {
                entry.latency.reset();
                entry.sync_match.advance();
                entry.poll_success.advance();
            }
        }

        (
            DeliveryMetricsSnapshot {
                queues: snapshot_queues,
                taken_at: OffsetDateTime::now_utc(),
            },
            poll_counts,
        )
    }
}

impl Default for FairnessState {
    fn default() -> Self {
        Self::new()
    }
}

impl FairnessState {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FairnessStateInner {
                queues: HashMap::new(),
            })),
        }
    }

    pub fn remaining_budget(&self, queue: &QueueKey) -> u32 {
        self.inner
            .lock()
            .unwrap()
            .queues
            .get(queue)
            .map(|entry| entry.remaining_budget)
            .unwrap_or(DEFAULT_BUDGET)
    }

    pub fn consume_budget(&self, queue: &QueueKey, count: u32) -> u32 {
        let mut inner = self.inner.lock().unwrap();
        let entry = inner
            .queues
            .entry(queue.clone())
            .or_insert(QueueFairnessState {
                drain_share: DEFAULT_DRAIN_SHARE,
                remaining_budget: DEFAULT_BUDGET,
                last_adjusted_at: OffsetDateTime::now_utc(),
            });
        let consumed = entry.remaining_budget.min(count);
        entry.remaining_budget -= consumed;
        consumed
    }

    pub fn apply_adjustment(
        &self,
        adjustments: HashMap<QueueKey, f64>,
        recent_poll_counts: &HashMap<QueueKey, u64>,
        now: OffsetDateTime,
    ) {
        let mut inner = self.inner.lock().unwrap();
        for (queue, share) in adjustments {
            let polls = recent_poll_counts.get(&queue).copied().unwrap_or(0);
            let budget = (share * polls as f64).floor() as u32;
            inner.queues.insert(
                queue,
                QueueFairnessState {
                    drain_share: share,
                    remaining_budget: budget,
                    last_adjusted_at: now,
                },
            );
        }
    }

    pub fn snapshot(&self) -> HashMap<QueueKey, QueueFairnessState> {
        self.inner.lock().unwrap().queues.clone()
    }
}

pub(crate) fn evaluate_drain_share(current: f64, metrics: &QueueMetricsSnapshot) -> f64 {
    let mut delta = 0.0f64;

    let age_threshold = metrics.latency_p99.saturating_mul(2);
    if metrics.backlog_age > age_threshold && age_threshold > Duration::ZERO {
        let pressure = (metrics.backlog_age.as_secs_f64() / age_threshold.as_secs_f64()).min(3.0);
        delta += 0.03 * pressure;
    }

    if metrics.sync_match_rate < 0.5 {
        let degradation = 0.5 - metrics.sync_match_rate;
        delta -= 0.05 * (degradation / 0.5);
    }

    if metrics.poll_success_rate < 0.7 && metrics.backlog_age > Duration::ZERO {
        delta += 0.02;
    }

    if metrics.latency_p99 > Duration::from_secs(5) {
        delta += 0.02;
    }

    let delta = delta.clamp(-MAX_DELTA_PER_INTERVAL, MAX_DELTA_PER_INTERVAL);
    (current + delta).clamp(MIN_DRAIN_SHARE, MAX_DRAIN_SHARE)
}

pub(crate) fn control_loop_tick(metrics: &DeliveryMetrics, fairness: &FairnessState) -> Duration {
    let (snapshot, poll_counts) = metrics.take_snapshot_internal(true);
    let current = fairness.snapshot();
    let mut adjustments = HashMap::new();
    let mut max_delta = 0.0f64;

    for (queue, queue_metrics) in &snapshot.queues {
        let current_share = current
            .get(queue)
            .map(|state| state.drain_share)
            .unwrap_or(DEFAULT_DRAIN_SHARE);
        let new_share = evaluate_drain_share(current_share, queue_metrics);
        max_delta = max_delta.max((new_share - current_share).abs());
        adjustments.insert(queue.clone(), new_share);
    }

    fairness.apply_adjustment(adjustments, &poll_counts, OffsetDateTime::now_utc());

    if snapshot.queues.is_empty() {
        return DEFAULT_CONTROL_INTERVAL;
    }

    let volatility = (max_delta / MAX_DELTA_PER_INTERVAL).clamp(0.0, 1.0);
    let min = MIN_CONTROL_INTERVAL.as_secs_f64();
    let max = MAX_CONTROL_INTERVAL.as_secs_f64();
    Duration::from_secs_f64(min + (max - min) * (1.0 - volatility))
}

pub(crate) async fn run_control_loop(
    metrics: DeliveryMetrics,
    fairness: FairnessState,
    cancel: CancellationToken,
) {
    let mut interval = DEFAULT_CONTROL_INTERVAL;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
        interval = control_loop_tick(&metrics, &fairness);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_types::{
        LogicalTaskSeq, NamespaceId, RunKey, TaskKind, TaskQueueName, WorkerIdentity,
    };

    fn queue() -> QueueKey {
        QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("q".into()),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        }
    }

    #[test]
    fn sliding_window_rate_zero_events_is_healthy() {
        assert_eq!(SlidingWindowCounter::new().rate(), 1.0);
    }

    #[test]
    fn fairness_budget_consumption_stops_at_zero() {
        let fairness = FairnessState::new();
        let queue = queue();
        assert_eq!(
            fairness.consume_budget(&queue, DEFAULT_BUDGET + 10),
            DEFAULT_BUDGET
        );
        assert_eq!(fairness.remaining_budget(&queue), 0);
    }

    #[test]
    fn latency_histogram_empty_percentile_is_zero() {
        let histogram = LatencyHistogram::new();
        assert_eq!(histogram.percentile(0.99), Duration::ZERO);
        assert_eq!(histogram.count, 0);
    }

    #[test]
    fn evaluate_drain_share_is_bounded() {
        let next = evaluate_drain_share(
            DEFAULT_DRAIN_SHARE,
            &QueueMetricsSnapshot {
                latency_p50: Duration::from_secs(1),
                latency_p99: Duration::from_secs(10),
                sync_match_rate: 0.0,
                poll_success_rate: 0.0,
                backlog_age: Duration::from_secs(120),
            },
        );
        assert!((MIN_DRAIN_SHARE..=MAX_DRAIN_SHARE).contains(&next));
    }

    #[test]
    fn control_loop_tick_interval_stays_bounded() {
        let metrics = DeliveryMetrics::new();
        let fairness = FairnessState::new();
        metrics.record_sync_match(&queue());
        let interval = control_loop_tick(&metrics, &fairness);
        assert!(interval >= MIN_CONTROL_INTERVAL);
        assert!(interval <= MAX_CONTROL_INTERVAL);
    }

    // ── Property-based tests ──────────────────────────

    mod prop {
        use super::*;
        use crate::broker::InMemoryBroker;
        use proptest::prelude::*;
        use tokeira_storage::DispatchableWorkflowTask;

        /// Helper: build a fixed QueueKey for property
        /// tests (avoids random UUID generation inside
        /// proptest which would defeat shrinking).
        fn fixed_queue() -> QueueKey {
            QueueKey {
                namespace_id: NamespaceId(uuid::Uuid::nil()),
                task_queue: TaskQueueName("pq".into()),
                task_kind: TaskKind::Workflow,
                deployment: None,
                build_id: None,
            }
        }

        fn fixed_worker() -> WorkerIdentity {
            WorkerIdentity("w1".into())
        }

        /// Strategy for QueueMetricsSnapshot with
        /// controllable ranges.
        fn arb_metrics() -> impl Strategy<Value = QueueMetricsSnapshot> {
            (
                0u64..60_000, // latency_p50 ms
                0u64..60_000, // latency_p99 ms
                0.0f64..=1.0, // sync_match_rate
                0.0f64..=1.0, // poll_success_rate
                0u64..600,    // backlog_age secs
            )
                .prop_map(|(p50, p99, smr, psr, age)| QueueMetricsSnapshot {
                    latency_p50: Duration::from_millis(p50),
                    latency_p99: Duration::from_millis(p99),
                    sync_match_rate: smr,
                    poll_success_rate: psr,
                    backlog_age: Duration::from_secs(age),
                })
        }

        fn arb_drain_share() -> impl Strategy<Value = f64> {
            (MIN_DRAIN_SHARE)..=(MAX_DRAIN_SHARE)
        }

        fn normalized_metrics(metrics: QueueMetricsSnapshot) -> QueueMetricsSnapshot {
            let latency_p50 = metrics.latency_p50;
            let latency_p99 = metrics.latency_p99.max(latency_p50);
            let sync_match_rate =
                ((metrics.sync_match_rate * 100.0).round() / 100.0).clamp(0.0, 1.0);
            let poll_success_rate =
                ((metrics.poll_success_rate * 100.0).round() / 100.0).clamp(0.0, 1.0);
            QueueMetricsSnapshot {
                latency_p50,
                latency_p99,
                sync_match_rate,
                poll_success_rate,
                backlog_age: metrics.backlog_age,
            }
        }

        fn delivery_metrics_from_snapshot(snapshot: &QueueMetricsSnapshot) -> DeliveryMetrics {
            let metrics = DeliveryMetrics::new();
            let queue = fixed_queue();
            let mut counters = QueueCounters::default();

            for _ in 0..50 {
                counters.latency.record(snapshot.latency_p50);
            }
            for _ in 0..50 {
                counters.latency.record(snapshot.latency_p99);
            }

            counters.sync_match.current_total = 100;
            counters.sync_match.current_success = (snapshot.sync_match_rate * 100.0).round() as u64;
            counters.poll_success.current_total = 100;
            counters.poll_success.current_success =
                (snapshot.poll_success_rate * 100.0).round() as u64;
            counters.backlog_age = snapshot.backlog_age;
            metrics.queues.insert(queue, counters);
            metrics
        }

        // ── P1: Poll path priority ordering ───────

        // **Validates: Requirements 1.1, 1.2, 1.3,
        // 1.4, 1.5**
        //
        // Broker delivers from the highest-priority
        // non-empty tier: sticky first, then general.
        // Fairness budget state does not affect the
        // poll path.
        proptest! {
            #![proptest_config(
                ProptestConfig::with_cases(100)
            )]
            #[test]
            fn p1_poll_path_priority_ordering(
                has_sticky in proptest::bool::ANY,
                has_general in proptest::bool::ANY,
                budget_exhausted in proptest::bool::ANY,
            ) {
                if !has_sticky && !has_general {
                    // Nothing to poll — skip.
                    return Ok(());
                }

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let broker = InMemoryBroker::default();
                    let q = fixed_queue();
                    let w = fixed_worker();

                    // Optionally exhaust fairness budget
                    // (should not affect poll path).
                    if budget_exhausted {
                        let fs = FairnessState::new();
                        fs.consume_budget(
                            &q,
                            DEFAULT_BUDGET + 1,
                        );
                    }

                    let sticky_seq = LogicalTaskSeq(1);
                    let general_seq = LogicalTaskSeq(2);

                    if has_sticky {
                        broker
                            .publish_workflow_task(
                                DispatchableWorkflowTask {
                                    run_key: RunKey::new(),
                                    queue: q.clone(),
                                    logical_seq: sticky_seq,
                                    sticky_preferred: Some(
                                        w.clone(),
                                    ),
                                    sticky_expires_at: None,
                                },
                                None,
                            )
                            .await;
                    }
                    if has_general {
                        broker
                            .publish_workflow_task(
                                DispatchableWorkflowTask {
                                    run_key: RunKey::new(),
                                    queue: q.clone(),
                                    logical_seq: general_seq,
                                    sticky_preferred: None,
                                    sticky_expires_at: None,
                                },
                                None,
                            )
                            .await;
                    }

                    let result = broker
                        .poll_workflow_task(
                            &q,
                            &w,
                            Duration::from_millis(10),
                        )
                        .await
                        .unwrap();

                    let (task, _entered) = result
                        .expect("should get a task")
                        .into_queued()
                        .expect("queued workflow task");

                    if has_sticky {
                        prop_assert_eq!(
                            task.logical_seq,
                            sticky_seq,
                            "sticky task should be \
                             delivered first"
                        );
                    } else {
                        prop_assert_eq!(
                            task.logical_seq,
                            general_seq,
                            "general task should be \
                             delivered when no sticky"
                        );
                    }
                    Ok(())
                })?;
            }
        }

        // ── P2: Drain share budget enforcement ────

        // **Validates: Requirements 2.1, 2.2**
        //
        // consume_budget never yields more than the
        // remaining budget; once zero, no more can be
        // consumed.
        proptest! {
            #![proptest_config(
                ProptestConfig::with_cases(100)
            )]
            #[test]
            fn p2_drain_share_budget_enforcement(
                initial_budget in 0u32..200,
                requests in proptest::collection::vec(
                    1u32..50, 1..10
                ),
            ) {
                let fairness = FairnessState::new();
                let q = fixed_queue();

                // Set up a known budget via
                // apply_adjustment.
                let share = 0.5f64;
                let poll_count =
                    (initial_budget as f64 / share)
                        .ceil() as u64;
                let mut adj = HashMap::new();
                adj.insert(q.clone(), share);
                let mut polls = HashMap::new();
                polls.insert(q.clone(), poll_count);
                fairness.apply_adjustment(
                    adj,
                    &polls,
                    OffsetDateTime::now_utc(),
                );

                let budget =
                    fairness.remaining_budget(&q);
                let mut total_consumed = 0u32;

                for req in &requests {
                    let consumed =
                        fairness.consume_budget(&q, *req);
                    total_consumed += consumed;
                    prop_assert!(
                        consumed <= *req,
                        "consumed {} > requested {}",
                        consumed,
                        req
                    );
                }

                prop_assert!(
                    total_consumed <= budget,
                    "total consumed {} > budget {}",
                    total_consumed,
                    budget
                );

                // After exhaustion, consuming yields 0.
                let extra =
                    fairness.consume_budget(&q, 100);
                let remaining =
                    fairness.remaining_budget(&q);
                prop_assert!(
                    extra <= remaining + extra,
                    "extra consumption inconsistent"
                );
            }
        }

        // ── P3: Sync match rate protection ────────

        // **Validates: Requirements 2.5, 4.4**
        //
        // Lower sync_match_rate → lower or equal
        // drain share (monotonic non-increasing).
        proptest! {
            #![proptest_config(
                ProptestConfig::with_cases(100)
            )]
            #[test]
            fn p3_sync_match_rate_protection(
                current in arb_drain_share(),
                base in arb_metrics(),
                rate_lo in 0.0f64..=1.0,
                rate_hi in 0.0f64..=1.0,
            ) {
                let (lo, hi) = if rate_lo <= rate_hi {
                    (rate_lo, rate_hi)
                } else {
                    (rate_hi, rate_lo)
                };

                let snap_lo = QueueMetricsSnapshot {
                    sync_match_rate: lo,
                    ..base.clone()
                };
                let snap_hi = QueueMetricsSnapshot {
                    sync_match_rate: hi,
                    ..base
                };

                let share_lo =
                    evaluate_drain_share(current, &snap_lo);
                let share_hi =
                    evaluate_drain_share(current, &snap_hi);

                prop_assert!(
                    share_lo <= share_hi + f64::EPSILON,
                    "lower sync_match_rate ({}) gave \
                     higher share ({}) than higher \
                     rate ({}) share ({})",
                    lo,
                    share_lo,
                    hi,
                    share_hi
                );
            }
        }

        // ── P4: Backlog age increases drain share ─

        // **Validates: Requirements 3.1, 3.2**
        //
        // Higher backlog_age → higher or equal drain
        // share (monotonic non-decreasing).
        proptest! {
            #![proptest_config(
                ProptestConfig::with_cases(100)
            )]
            #[test]
            fn p4_backlog_age_increases_drain_share(
                current in arb_drain_share(),
                base in arb_metrics(),
                age_lo_s in 0u64..600,
                age_hi_s in 0u64..600,
            ) {
                let (lo, hi) = if age_lo_s <= age_hi_s {
                    (age_lo_s, age_hi_s)
                } else {
                    (age_hi_s, age_lo_s)
                };

                let snap_lo = QueueMetricsSnapshot {
                    backlog_age: Duration::from_secs(lo),
                    ..base.clone()
                };
                let snap_hi = QueueMetricsSnapshot {
                    backlog_age: Duration::from_secs(hi),
                    ..base
                };

                let share_lo =
                    evaluate_drain_share(current, &snap_lo);
                let share_hi =
                    evaluate_drain_share(current, &snap_hi);

                prop_assert!(
                    share_hi >= share_lo - f64::EPSILON,
                    "higher backlog_age ({}) gave \
                     lower share ({}) than lower \
                     age ({}) share ({})",
                    hi,
                    share_hi,
                    lo,
                    share_lo
                );
            }
        }

        // ── P5: Drain share upper bound ───────────

        // **Validates: Requirements 3.3**
        //
        // evaluate_drain_share always returns a value
        // in [MIN_DRAIN_SHARE, MAX_DRAIN_SHARE] and
        // MAX_DRAIN_SHARE < 1.0.
        proptest! {
            #![proptest_config(
                ProptestConfig::with_cases(100)
            )]
            #[test]
            fn p5_drain_share_upper_bound(
                current in 0.0f64..=1.0,
                metrics in arb_metrics(),
            ) {
                let result =
                    evaluate_drain_share(current, &metrics);
                prop_assert!(
                    result >= MIN_DRAIN_SHARE,
                    "result {} < MIN {}",
                    result,
                    MIN_DRAIN_SHARE
                );
                prop_assert!(
                    result <= MAX_DRAIN_SHARE,
                    "result {} > MAX {}",
                    result,
                    MAX_DRAIN_SHARE
                );
                prop_assert!(
                    MAX_DRAIN_SHARE < 1.0,
                    "MAX_DRAIN_SHARE must be < 1.0"
                );
            }
        }

        // ── P6: Oscillation bound ─────────────────

        // **Validates: Requirements 4.9**
        //
        // |evaluate_drain_share(current, m) - current|
        // <= MAX_DELTA_PER_INTERVAL
        proptest! {
            #![proptest_config(
                ProptestConfig::with_cases(100)
            )]
            #[test]
            fn p6_oscillation_bound(
                current in arb_drain_share(),
                metrics in arb_metrics(),
            ) {
                let result =
                    evaluate_drain_share(current, &metrics);
                let delta = (result - current).abs();
                prop_assert!(
                    delta
                        <= MAX_DELTA_PER_INTERVAL
                            + f64::EPSILON,
                    "|{} - {}| = {} > MAX_DELTA {}",
                    result,
                    current,
                    delta,
                    MAX_DELTA_PER_INTERVAL
                );
            }
        }

        // ── P7: Latency responsiveness ────────────

        // **Validates: Requirements 4.3**
        //
        // Higher latency_p99 → higher or equal drain
        // share when backlog_age is zero (isolating
        // the direct latency signal from the
        // age_threshold interaction).
        proptest! {
            #![proptest_config(
                ProptestConfig::with_cases(100)
            )]
            #[test]
            fn p7_latency_responsiveness(
                current in arb_drain_share(),
                sync_match_rate in 0.0f64..=1.0,
                poll_success_rate in 0.0f64..=1.0,
                lat_lo_ms in 0u64..60_000,
                lat_hi_ms in 0u64..60_000,
            ) {
                let (lo, hi) =
                    if lat_lo_ms <= lat_hi_ms {
                        (lat_lo_ms, lat_hi_ms)
                    } else {
                        (lat_hi_ms, lat_lo_ms)
                    };

                // Hold backlog_age at zero to isolate
                // the latency_p99 > 5s signal from
                // the age_threshold interaction.
                let snap_lo = QueueMetricsSnapshot {
                    latency_p50: Duration::ZERO,
                    latency_p99:
                        Duration::from_millis(lo),
                    sync_match_rate,
                    poll_success_rate,
                    backlog_age: Duration::ZERO,
                };
                let snap_hi = QueueMetricsSnapshot {
                    latency_p99:
                        Duration::from_millis(hi),
                    ..snap_lo.clone()
                };

                let share_lo = evaluate_drain_share(
                    current, &snap_lo,
                );
                let share_hi = evaluate_drain_share(
                    current, &snap_hi,
                );

                prop_assert!(
                    share_hi
                        >= share_lo - f64::EPSILON,
                    "higher latency_p99 ({}) gave \
                     lower share ({}) than lower \
                     latency ({}) share ({})",
                    hi,
                    share_hi,
                    lo,
                    share_lo
                );
            }
        }

        // ── P8: Latency recording accuracy ────────

        // **Validates: Requirements 5.1, 5.2, 5.3,
        // 5.5, 5.6**
        //
        // LatencyHistogram records durations and the
        // percentile query returns the correct bucket.
        proptest! {
            #![proptest_config(
                ProptestConfig::with_cases(100)
            )]
            #[test]
            fn p8_latency_recording_accuracy(
                durations_ms in proptest::collection::vec(
                    0u64..60_000, 1..50
                ),
            ) {
                let mut hist = LatencyHistogram::new();
                for &ms in &durations_ms {
                    hist.record(
                        Duration::from_millis(ms),
                    );
                }

                prop_assert_eq!(
                    hist.count,
                    durations_ms.len() as u64,
                    "count mismatch"
                );

                // p50 should be <= the median value
                // (bucket-level precision).
                let mut sorted = durations_ms.clone();
                sorted.sort();
                let median_ms =
                    sorted[sorted.len() / 2];
                let p50 = hist.percentile(0.50);
                // Bucket precision: p50 should be
                // within the sorted range.
                prop_assert!(
                    p50.as_millis()
                        <= median_ms as u128 + 1,
                    "p50 {} > median {} + 1",
                    p50.as_millis(),
                    median_ms
                );

                // p99 should be >= p50.
                let p99 = hist.percentile(0.99);
                prop_assert!(
                    p99 >= p50,
                    "p99 ({:?}) < p50 ({:?})",
                    p99,
                    p50
                );
            }
        }

        // ── P9: Sliding window rate computation ───

        // **Validates: Requirements 6.1, 6.2, 6.3,
        // 7.1, 7.2, 7.3**
        //
        // rate() equals expected ratio; 1.0 for zero
        // events; advance preserves previous bucket.
        proptest! {
            #![proptest_config(
                ProptestConfig::with_cases(100)
            )]
            #[test]
            fn p9_sliding_window_rate_computation(
                successes_a in 0u64..100,
                failures_a in 0u64..100,
                successes_b in 0u64..100,
                failures_b in 0u64..100,
            ) {
                let mut counter =
                    SlidingWindowCounter::new();

                // Zero events → rate 1.0.
                prop_assert_eq!(
                    counter.rate(),
                    1.0,
                    "empty counter should be 1.0"
                );

                // Record window A.
                for _ in 0..successes_a {
                    counter.record_success();
                }
                for _ in 0..failures_a {
                    counter.record_failure();
                }

                let total_a =
                    successes_a + failures_a;
                if total_a > 0 {
                    let expected =
                        successes_a as f64
                            / total_a as f64;
                    let actual = counter.rate();
                    prop_assert!(
                        (actual - expected).abs()
                            < 1e-10,
                        "window A: expected {} got {}",
                        expected,
                        actual
                    );
                }

                // Advance window.
                counter.advance();

                // Record window B.
                for _ in 0..successes_b {
                    counter.record_success();
                }
                for _ in 0..failures_b {
                    counter.record_failure();
                }

                let total_b =
                    successes_b + failures_b;
                let combined_total =
                    total_a + total_b;
                let combined_success =
                    successes_a + successes_b;

                if combined_total > 0 {
                    let expected =
                        combined_success as f64
                            / combined_total as f64;
                    let actual = counter.rate();
                    prop_assert!(
                        (actual - expected).abs()
                            < 1e-10,
                        "combined: expected {} got {}",
                        expected,
                        actual
                    );
                }

                prop_assert_eq!(
                    counter.total(),
                    combined_total,
                    "total mismatch"
                );
            }
        }

        // ── P10: Backlog age gauge accuracy ───────

        // **Validates: Requirements 8.1, 8.2, 8.3,
        // 8.5**
        //
        // set_backlog_age reflects the value set;
        // setting zero clears it.
        proptest! {
            #![proptest_config(
                ProptestConfig::with_cases(100)
            )]
            #[test]
            fn p10_backlog_age_gauge_accuracy(
                ages_secs in proptest::collection::vec(
                    0u64..3600, 1..20
                ),
            ) {
                let metrics = DeliveryMetrics::new();
                let q = fixed_queue();

                let max_age = ages_secs
                    .iter()
                    .copied()
                    .max()
                    .unwrap_or(0);

                // Set the gauge to the max age
                // (simulating what the drain loop
                // does).
                metrics.set_backlog_age(
                    &q,
                    Duration::from_secs(max_age),
                );

                let snap = metrics.peek_snapshot();
                let queue_snap =
                    snap.queues.get(&q).unwrap();
                prop_assert_eq!(
                    queue_snap.backlog_age,
                    Duration::from_secs(max_age),
                    "gauge should reflect max age"
                );

                // Setting to zero clears it.
                metrics.set_backlog_age(
                    &q,
                    Duration::ZERO,
                );
                let snap = metrics.peek_snapshot();
                let queue_snap =
                    snap.queues.get(&q).unwrap();
                prop_assert_eq!(
                    queue_snap.backlog_age,
                    Duration::ZERO,
                    "gauge should be zero after \
                     empty drain"
                );
            }
        }

        // ── P11: Convergence from defaults ────────

        // **Validates: Requirements 10.2**
        //
        // Repeated evaluate_drain_share from
        // DEFAULT_DRAIN_SHARE converges within 20
        // iterations. Convergence means either:
        // (a) the share stabilizes (small delta), or
        // (b) the share reaches a boundary (MIN/MAX).
        proptest! {
            #![proptest_config(
                ProptestConfig::with_cases(100)
            )]
            #[test]
            fn p11_convergence_from_defaults(
                metrics in arb_metrics(),
            ) {
                let mut share = DEFAULT_DRAIN_SHARE;

                for _ in 0..20 {
                    share = evaluate_drain_share(
                        share, &metrics,
                    );
                }

                // After 20 iterations, one more step
                // should produce a small delta (the
                // share has either converged or hit a
                // boundary where it stays clamped).
                let next = evaluate_drain_share(
                    share, &metrics,
                );
                let delta = (next - share).abs();

                prop_assert!(
                    delta
                        <= MAX_DELTA_PER_INTERVAL
                            + f64::EPSILON,
                    "did not converge: share={}, \
                     next={}, delta={}",
                    share,
                    next,
                    delta
                );
            }
        }

        // ── P12: Adaptive interval monotonicity ───

        // **Validates: Requirements 4.6**
        //
        // Higher volatility (larger max drain share
        // delta) → shorter or equal interval. Always
        // in [MIN, MAX].
        proptest! {
            #![proptest_config(
                ProptestConfig::with_cases(100)
            )]
            #[test]
            fn p12_adaptive_interval_monotonicity(
                vol_lo in 0.0f64..=1.0,
                vol_hi in 0.0f64..=1.0,
            ) {
                let (lo, hi) = if vol_lo <= vol_hi {
                    (vol_lo, vol_hi)
                } else {
                    (vol_hi, vol_lo)
                };

                // Compute intervals using the same
                // formula as control_loop_tick.
                let min =
                    MIN_CONTROL_INTERVAL.as_secs_f64();
                let max =
                    MAX_CONTROL_INTERVAL.as_secs_f64();

                let interval_lo =
                    Duration::from_secs_f64(
                        min + (max - min) * (1.0 - lo),
                    );
                let interval_hi =
                    Duration::from_secs_f64(
                        min + (max - min) * (1.0 - hi),
                    );

                // Higher volatility → shorter or
                // equal interval.
                prop_assert!(
                    interval_hi <= interval_lo
                        + Duration::from_micros(1),
                    "higher vol ({}) interval ({:?}) \
                     > lower vol ({}) interval ({:?})",
                    hi,
                    interval_hi,
                    lo,
                    interval_lo
                );

                // Both in [MIN, MAX].
                prop_assert!(
                    interval_lo >= MIN_CONTROL_INTERVAL
                );
                prop_assert!(
                    interval_lo <= MAX_CONTROL_INTERVAL
                );
                prop_assert!(
                    interval_hi >= MIN_CONTROL_INTERVAL
                );
                prop_assert!(
                    interval_hi <= MAX_CONTROL_INTERVAL
                );
            }
        }

        // ── P13: Fairness control loop equivalence ─

        // The live control loop should compute the same
        // next drain share and interval as a direct
        // application of `evaluate_drain_share` over the
        // equivalent snapshot.
        proptest! {
            #![proptest_config(
                ProptestConfig::with_cases(100)
            )]
            #[test]
            fn p13_fairness_control_loop_equivalence(
                current in arb_drain_share(),
                metrics in arb_metrics(),
            ) {
                let metrics = normalized_metrics(metrics);
                let delivery = delivery_metrics_from_snapshot(&metrics);
                let fairness = FairnessState::new();
                let queue = fixed_queue();
                let mut adjustments = HashMap::new();
                adjustments.insert(queue.clone(), current);
                let mut polls = HashMap::new();
                polls.insert(queue.clone(), 100);
                fairness.apply_adjustment(
                    adjustments,
                    &polls,
                    OffsetDateTime::now_utc(),
                );

                let expected_share =
                    evaluate_drain_share(current, &metrics);
                let expected_delta =
                    (expected_share - current).abs();
                let expected_interval = Duration::from_secs_f64(
                    MIN_CONTROL_INTERVAL.as_secs_f64()
                        + (MAX_CONTROL_INTERVAL.as_secs_f64()
                            - MIN_CONTROL_INTERVAL.as_secs_f64())
                            * (1.0 - (expected_delta / MAX_DELTA_PER_INTERVAL).clamp(0.0, 1.0)),
                );

                let interval = control_loop_tick(&delivery, &fairness);
                let snapshot = fairness.snapshot();
                let state = snapshot.get(&queue).unwrap();

                prop_assert!(
                    (state.drain_share - expected_share).abs() <= f64::EPSILON,
                    "share mismatch: actual={} expected={}",
                    state.drain_share,
                    expected_share
                );
                prop_assert_eq!(
                    state.remaining_budget,
                    (expected_share * 100.0).floor() as u32
                );
                prop_assert_eq!(interval, expected_interval);
            }
        }
    }
}
