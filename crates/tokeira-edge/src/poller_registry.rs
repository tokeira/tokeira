//! Per-task-queue recent-poller bookkeeping.
//!
//! `DescribeTaskQueue` reports identities observed polling a queue during the
//! preceding few minutes, rather than only requests whose long poll is still
//! open. Temporal v1.31.0 keys this diagnostic history by worker identity and
//! retains it for the five-minute `PollerHistoryTTL` default
//! (`service/matching/poller_history.go` and
//! `common/dynamicconfig/constants.go:479 @ v1.31.0`). [`PollerRegistry`] keeps
//! the same ephemeral edge-plane view: losing it on restart can only make a
//! Describe response temporarily empty and never affects workflow correctness.
//!
//! Temporal eagerly removes a worker when `CancelOutstandingWorkerPolls`
//! reports shutdown (`service/matching/matching_engine.go:1194-1206 @
//! v1.31.0`). Tokeira has no equivalent matching-internal shutdown signal yet,
//! so a stopped worker may remain visible until the bounded TTL expires.
//! A cancelled poll deliberately does not refresh its completion timestamp, so
//! cancellation cannot extend that stale window or re-add a future eager
//! removal (`service/matching/task_queue_partition_manager.go:617-621 @
//! v1.31.0`).

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use time::{Duration, OffsetDateTime};
use tokeira_types::{QueueKey, WorkerIdentity};

const POLLER_HISTORY_TTL: Duration = Duration::minutes(5);

/// One worker identity recently observed polling a task queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivePoller {
    /// SDK-reported worker identity, used as the registry's deduplication key.
    pub identity: WorkerIdentity,
    /// Latest poll-admission or non-cancelled poll-end observation.
    pub last_accessed_at: OffsetDateTime,
}

#[derive(Debug, Default)]
struct PollerRegistryState {
    pollers: RwLock<HashMap<QueueKey, HashMap<WorkerIdentity, ActivePoller>>>,
}

impl PollerRegistryState {
    fn record(&self, queue: QueueKey, identity: WorkerIdentity, observed_at: OffsetDateTime) {
        self.pollers
            .write()
            .expect("poller registry poisoned")
            .entry(queue)
            .or_default()
            .insert(
                identity.clone(),
                ActivePoller {
                    identity,
                    last_accessed_at: observed_at,
                },
            );
    }

    fn pollers_at(&self, queue: &QueueKey, now: OffsetDateTime) -> Vec<ActivePoller> {
        let cutoff = now - POLLER_HISTORY_TTL;
        let mut pollers = self.pollers.write().expect("poller registry poisoned");
        let remove_queue = match pollers.get_mut(queue) {
            Some(entries) => {
                entries.retain(|_, poller| poller.last_accessed_at > cutoff);
                entries.is_empty()
            }
            None => return Vec::new(),
        };
        if remove_queue {
            pollers.remove(queue);
            return Vec::new();
        }

        let mut recent = pollers
            .get(queue)
            .into_iter()
            .flat_map(|entries| entries.values().cloned())
            .collect::<Vec<_>>();
        // Hash iteration must not leak into public response ordering. Temporal
        // does not promise an order, but stable output keeps tests and operator
        // diffs deterministic.
        recent.sort_by(|left, right| left.identity.0.cmp(&right.identity.0));
        recent
    }
}

/// Ephemeral recent-poller history shared by poll and Describe handlers.
#[derive(Clone, Debug, Default)]
pub struct PollerRegistry {
    state: Arc<PollerRegistryState>,
}

impl PollerRegistry {
    /// Record a poll start and return a finalizer for non-cancelled completion.
    ///
    /// Call [`PollerGuard::completed`] after the poll future resolves, including
    /// the long-poll timeout case. Dropping the guard without completing it is
    /// the cancellation path and intentionally leaves the admission timestamp.
    pub fn register(&self, queue: QueueKey, identity: WorkerIdentity) -> PollerGuard {
        self.register_at(queue, identity, OffsetDateTime::now_utc())
    }

    fn register_at(
        &self,
        queue: QueueKey,
        identity: WorkerIdentity,
        observed_at: OffsetDateTime,
    ) -> PollerGuard {
        self.state
            .record(queue.clone(), identity.clone(), observed_at);

        PollerGuard {
            state: self.state.clone(),
            queue,
            identity,
        }
    }

    /// Return identities observed on `queue` during the five-minute history
    /// window, deduplicated by worker identity.
    pub fn pollers(&self, queue: &QueueKey) -> Vec<ActivePoller> {
        self.state.pollers_at(queue, OffsetDateTime::now_utc())
    }

    #[cfg(test)]
    fn record_at(&self, queue: QueueKey, identity: WorkerIdentity, observed_at: OffsetDateTime) {
        self.state.record(queue, identity, observed_at);
    }

    #[cfg(test)]
    fn pollers_at(&self, queue: &QueueKey, now: OffsetDateTime) -> Vec<ActivePoller> {
        self.state.pollers_at(queue, now)
    }
}

/// Poll-lifetime finalizer that distinguishes completion from cancellation.
///
/// A normally resolved poll consumes the guard through [`Self::completed`]. If
/// tonic cancels the handler by dropping its future, ordinary guard drop has no
/// side effect and therefore cannot refresh the worker's poller-history entry.
#[must_use = "call completed after a non-cancelled poll result"]
#[derive(Debug)]
pub struct PollerGuard {
    state: Arc<PollerRegistryState>,
    queue: QueueKey,
    identity: WorkerIdentity,
}

impl PollerGuard {
    /// Refresh the identity after the poll resolves normally, including a
    /// deadline/long-poll timeout that returns no task.
    pub fn completed(self) {
        self.completed_at(OffsetDateTime::now_utc());
    }

    fn completed_at(self, observed_at: OffsetDateTime) {
        // v1.31.0 performs this second update only when the poll did not end
        // with context.Canceled. Consuming the guard explicitly maps that
        // distinction onto Rust future cancellation: dropped future means this
        // method is never reached.
        self.state.record(self.queue, self.identity, observed_at);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use time::{Duration, OffsetDateTime};
    use tokeira_types::{NamespaceId, QueueKey, TaskKind, TaskQueueName, WorkerIdentity};
    use uuid::Uuid;

    use super::{POLLER_HISTORY_TTL, PollerRegistry};

    fn queue() -> QueueKey {
        QueueKey {
            namespace_id: NamespaceId(Uuid::nil()),
            task_queue: TaskQueueName("queue".to_string()),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        }
    }

    #[test]
    fn cancelled_poll_keeps_admission_observation() {
        let registry = PollerRegistry::default();
        let queue = queue();
        let admission = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);

        let guard = registry.register_at(
            queue.clone(),
            WorkerIdentity("worker".to_string()),
            admission,
        );
        drop(guard);

        let pollers = registry.pollers_at(&queue, admission + Duration::seconds(1));
        assert_eq!(pollers.len(), 1);
        assert_eq!(pollers[0].identity.0, "worker");
        assert_eq!(pollers[0].last_accessed_at, admission);
    }

    #[test]
    fn non_cancelled_completion_advances_observation() {
        let registry = PollerRegistry::default();
        let queue = queue();
        let admission = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let completion = admission + Duration::seconds(30);

        let guard = registry.register_at(
            queue.clone(),
            WorkerIdentity("worker".to_string()),
            admission,
        );
        guard.completed_at(completion);

        let pollers = registry.pollers_at(&queue, completion + Duration::seconds(1));
        assert_eq!(pollers.len(), 1);
        assert_eq!(pollers[0].last_accessed_at, completion);
    }

    #[test]
    fn repeated_identity_is_deduplicated_at_latest_observation() {
        let registry = PollerRegistry::default();
        let queue = queue();
        let first = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let latest = first + Duration::seconds(30);
        let identity = WorkerIdentity("worker".to_string());

        registry.record_at(queue.clone(), identity.clone(), first);
        registry.record_at(queue.clone(), identity, latest);

        let pollers = registry.pollers_at(&queue, latest + Duration::seconds(1));
        assert_eq!(pollers.len(), 1);
        assert_eq!(pollers[0].last_accessed_at, latest);
    }

    #[test]
    fn observations_expire_at_five_minute_boundary() {
        let registry = PollerRegistry::default();
        let queue = queue();
        let now = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);

        registry.record_at(
            queue.clone(),
            WorkerIdentity("expired".to_string()),
            now - POLLER_HISTORY_TTL,
        );
        registry.record_at(
            queue.clone(),
            WorkerIdentity("recent".to_string()),
            now - POLLER_HISTORY_TTL + Duration::nanoseconds(1),
        );

        let pollers = registry.pollers_at(&queue, now);
        assert_eq!(pollers.len(), 1);
        assert_eq!(pollers[0].identity.0, "recent");
    }

    // Feature: temporal-ui-support, Property 6: describe task queue pollers
    // Every recent identity appears exactly once at its latest observation.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn recent_poller_history_is_deduplicated_and_ttl_bounded(
            observations in prop::collection::vec((0u8..8, 0i64..700), 0..64),
        ) {
            let registry = PollerRegistry::default();
            let queue = queue();
            let now = OffsetDateTime::UNIX_EPOCH + Duration::hours(2);
            let origin = now - Duration::seconds(700);
            let mut observations = observations;
            observations.sort_by_key(|(_, offset)| *offset);
            let mut latest = BTreeMap::new();

            for (worker, offset) in observations {
                let identity = format!("worker-{worker}");
                let observed_at = origin + Duration::seconds(offset);
                registry.record_at(
                    queue.clone(),
                    WorkerIdentity(identity.clone()),
                    observed_at,
                );
                latest.insert(identity, observed_at);
            }

            let expected = latest
                .into_iter()
                .filter(|(_, observed_at)| *observed_at > now - POLLER_HISTORY_TTL)
                .collect::<Vec<_>>();
            let actual = registry
                .pollers_at(&queue, now)
                .into_iter()
                .map(|poller| (poller.identity.0, poller.last_accessed_at))
                .collect::<Vec<_>>();

            prop_assert_eq!(actual, expected);
        }
    }
}
