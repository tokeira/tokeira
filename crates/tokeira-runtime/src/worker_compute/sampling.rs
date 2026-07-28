//! Advisory exact-version delivery counters and metrics aggregation.
//!
//! The task-delivery path records only process-local monotonic counters. Periodic
//! sampling owns all durable writes, and controller evaluation consumes only fresh
//! replacement samples. Losing this module's memory can delay or perturb a capacity
//! hint, but cannot lose or complete workflow work.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, TryLockError},
};

use anyhow::Result;
use time::OffsetDateTime;
use tokeira_storage::{RunRepository, WorkerComputeQueueSample, WorkerComputeRepository};
use tokeira_types::{
    ControllerInstanceKey, IncarnationId, QueueKey, ShardId, TaskKind, WorkerComputeQueueKey,
    WorkerComputeTaskType,
};

use crate::{
    EndpointTarget, InMemoryActivityBroker, InMemoryBroker, NexusEndpointRegistry, NexusTaskBroker,
};

use super::{MetricsSnapshot, QUEUE_SAMPLE_TTL, TaskTypeMetrics};

/// Monotonic process-local totals for one exact-version queue.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkerComputeQueueCounters {
    /// Unique task publications observed by this queue home.
    pub adds: u64,
    /// Successful task handouts observed by this queue home.
    pub dispatches: u64,
}

/// Fail-open process-local counters used by the periodic queue sampler.
///
/// Recording uses `try_lock`: capacity advice must never stall task publication or
/// handout behind an observer. A contended or poisoned recorder drops that advisory
/// increment; durable backlog reconstruction remains the recovery path.
#[derive(Clone, Debug, Default)]
pub struct WorkerComputeQueueMetrics {
    counters: Arc<Mutex<BTreeMap<WorkerComputeQueueKey, WorkerComputeQueueCounters>>>,
}

impl WorkerComputeQueueMetrics {
    /// Record one unique exact-version task publication without blocking delivery.
    pub fn record_add(&self, key: WorkerComputeQueueKey) {
        self.record(key, |counters| {
            counters.adds = counters.adds.saturating_add(1);
        });
    }

    /// Record one successful exact-version task handout without blocking delivery.
    pub fn record_dispatch(&self, key: WorkerComputeQueueKey) {
        self.record(key, |counters| {
            counters.dispatches = counters.dispatches.saturating_add(1);
        });
    }

    fn record(
        &self,
        key: WorkerComputeQueueKey,
        update: impl FnOnce(&mut WorkerComputeQueueCounters),
    ) {
        match self.counters.try_lock() {
            Ok(mut counters) => update(counters.entry(key).or_default()),
            Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => {}
        }
    }

    /// Snapshot all known queue totals in deterministic identity order.
    #[must_use]
    pub fn snapshot(&self) -> BTreeMap<WorkerComputeQueueKey, WorkerComputeQueueCounters> {
        self.counters
            .lock()
            .map(|counters| counters.clone())
            .unwrap_or_default()
    }
}

#[derive(Clone, Copy, Debug)]
struct PriorQueueSample {
    counters: WorkerComputeQueueCounters,
    sampled_at: OffsetDateTime,
}

#[derive(Debug, Default)]
struct QueueSamplerState {
    writer_sequence: u64,
    prior: BTreeMap<WorkerComputeQueueKey, PriorQueueSample>,
}

/// Periodic producer of durable advisory queue snapshots.
///
/// Workflow and activity backlog combines disposable ready state with the disjoint
/// durable backlog. Nexus uses live ready state and the maximum of that count and
/// reconstructible authoritative pending deliveries, avoiding obvious double counting
/// while remaining conservative after broker loss.
pub struct WorkerComputeQueueSampler<R> {
    run_repository: Arc<R>,
    sample_repository: Arc<dyn WorkerComputeRepository>,
    workflow_broker: InMemoryBroker,
    activity_broker: InMemoryActivityBroker,
    nexus_broker: NexusTaskBroker,
    nexus_registry: NexusEndpointRegistry,
    queue_metrics: WorkerComputeQueueMetrics,
    writer_id: IncarnationId,
    state: Arc<Mutex<QueueSamplerState>>,
}

impl<R> std::fmt::Debug for WorkerComputeQueueSampler<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerComputeQueueSampler")
            .field("writer_id", &self.writer_id)
            .finish_non_exhaustive()
    }
}

impl<R> WorkerComputeQueueSampler<R>
where
    R: RunRepository + 'static,
{
    /// Construct a sampler over existing delivery and repository ports.
    pub fn new(
        run_repository: Arc<R>,
        sample_repository: Arc<dyn WorkerComputeRepository>,
        workflow_broker: InMemoryBroker,
        activity_broker: InMemoryActivityBroker,
        nexus_broker: NexusTaskBroker,
        nexus_registry: NexusEndpointRegistry,
        queue_metrics: WorkerComputeQueueMetrics,
        writer_id: IncarnationId,
    ) -> Self {
        Self {
            run_repository,
            sample_repository,
            workflow_broker,
            activity_broker,
            nexus_broker,
            nexus_registry,
            queue_metrics,
            writer_id,
            state: Arc::new(Mutex::new(QueueSamplerState::default())),
        }
    }

    /// Produce and persist one bounded-write sample pass at explicit time.
    pub async fn sample_once(
        &self,
        active_shards: &[ShardId],
        now: OffsetDateTime,
    ) -> Result<Vec<WorkerComputeQueueSample>> {
        let counter_snapshot = self.queue_metrics.snapshot();
        let mut backlog = self.workflow_broker.versioned_backlog_counts().await;
        for (key, count) in self.activity_broker.versioned_backlog_counts().await {
            backlog
                .entry(key)
                .and_modify(|current| *current = current.saturating_add(count))
                .or_insert(count);
        }

        for queue in self
            .run_repository
            .list_versioned_backlog_queue_keys()
            .await?
        {
            let Some(key) = sample_key_from_queue(&queue) else {
                continue;
            };
            let durable_count = self
                .run_repository
                .backlog_stats_by_priority(&queue)
                .await?
                .values()
                .fold(0_u64, |total, band| {
                    total.saturating_add(u64::try_from(band.count).unwrap_or(u64::MAX))
                });
            backlog
                .entry(key)
                .and_modify(|current| *current = current.saturating_add(durable_count))
                .or_insert(durable_count);
        }

        let mut nexus_live = self.nexus_broker.versioned_backlog_counts().await;
        let mut nexus_reconstructed = BTreeMap::<WorkerComputeQueueKey, u64>::new();
        for shard_id in active_shards {
            for delivery in self
                .run_repository
                .list_reconstructible_nexus_deliveries_for_shard(*shard_id, now, usize::MAX)
                .await?
            {
                let Some(endpoint) = self.nexus_registry.resolve(&delivery.endpoint) else {
                    continue;
                };
                let EndpointTarget::Worker {
                    namespace_id,
                    task_queue,
                } = endpoint.target
                else {
                    continue;
                };
                let key = WorkerComputeQueueKey {
                    namespace_id,
                    deployment_name: tokeira_types::DeploymentId(delivery.version.deployment_name),
                    build_id: tokeira_types::BuildId(delivery.version.build_id),
                    task_type: WorkerComputeTaskType::Nexus,
                    task_queue,
                };
                nexus_reconstructed
                    .entry(key)
                    .and_modify(|count| *count = count.saturating_add(1))
                    .or_insert(1);
            }
        }
        for (key, reconstructed) in nexus_reconstructed {
            nexus_live
                .entry(key)
                .and_modify(|live| *live = (*live).max(reconstructed))
                .or_insert(reconstructed);
        }
        for (key, count) in nexus_live {
            backlog.insert(key, count);
        }

        let mut keys = counter_snapshot.keys().cloned().collect::<BTreeSet<_>>();
        keys.extend(backlog.keys().cloned());
        let samples = {
            let mut state = self
                .state
                .lock()
                .expect("worker-compute queue-sampler lock poisoned");
            keys.into_iter()
                .map(|key| {
                    let counters = counter_snapshot.get(&key).copied().unwrap_or_default();
                    let (add_rate, dispatch_rate) = state
                        .prior
                        .get(&key)
                        .map_or((0.0, 0.0), |prior| rates_since(*prior, counters, now));
                    state.prior.insert(
                        key.clone(),
                        PriorQueueSample {
                            counters,
                            sampled_at: now,
                        },
                    );
                    state.writer_sequence = state.writer_sequence.saturating_add(1);
                    WorkerComputeQueueSample {
                        key: key.clone(),
                        writer_id: self.writer_id,
                        writer_sequence: state.writer_sequence,
                        backlog_count: backlog.get(&key).copied().unwrap_or(0),
                        add_rate,
                        dispatch_rate,
                        sampled_at: now,
                    }
                })
                .collect::<Vec<_>>()
        };

        for sample in samples.iter().cloned() {
            self.sample_repository.put_queue_sample(sample).await?;
        }
        Ok(samples)
    }
}

fn rates_since(
    prior: PriorQueueSample,
    current: WorkerComputeQueueCounters,
    now: OffsetDateTime,
) -> (f64, f64) {
    let elapsed = (now - prior.sampled_at).as_seconds_f64();
    if elapsed <= 0.0 {
        return (0.0, 0.0);
    }
    (
        current.adds.saturating_sub(prior.counters.adds) as f64 / elapsed,
        current.dispatches.saturating_sub(prior.counters.dispatches) as f64 / elapsed,
    )
}

fn sample_key_from_queue(queue: &QueueKey) -> Option<WorkerComputeQueueKey> {
    Some(WorkerComputeQueueKey {
        namespace_id: queue.namespace_id,
        deployment_name: queue.deployment.clone()?,
        build_id: queue.build_id.clone()?,
        task_type: match queue.task_kind {
            TaskKind::Workflow => WorkerComputeTaskType::Workflow,
            TaskKind::Activity => WorkerComputeTaskType::Activity,
        },
        task_queue: queue.task_queue.clone(),
    })
}

/// Aggregate fresh samples for one exact Deployment Version.
///
/// The returned version snapshot always contains all three task families, including
/// explicit zeros. Group routing is a separate step so one sample can never leak a
/// task family into a scaling group that does not own it.
#[must_use]
pub fn aggregate_queue_samples(
    controller: &ControllerInstanceKey,
    samples: &[WorkerComputeQueueSample],
    now: OffsetDateTime,
) -> MetricsSnapshot {
    let not_before = now
        - time::Duration::try_from(QUEUE_SAMPLE_TTL)
            .expect("queue sample TTL is representable by time::Duration");
    let mut snapshot = MetricsSnapshot {
        workflow: Some(TaskTypeMetrics::default()),
        activity: Some(TaskTypeMetrics::default()),
        nexus: Some(TaskTypeMetrics::default()),
    };

    for sample in samples {
        if sample.sampled_at < not_before
            || sample.key.namespace_id != controller.namespace_id
            || sample.key.deployment_name != controller.deployment_name
            || sample.key.build_id != controller.build_id
        {
            continue;
        }
        let metrics = match sample.key.task_type {
            WorkerComputeTaskType::Workflow => snapshot
                .workflow
                .as_mut()
                .expect("full version snapshot carries workflow metrics"),
            WorkerComputeTaskType::Activity => snapshot
                .activity
                .as_mut()
                .expect("full version snapshot carries activity metrics"),
            WorkerComputeTaskType::Nexus => snapshot
                .nexus
                .as_mut()
                .expect("full version snapshot carries Nexus metrics"),
        };
        metrics.backlog_count = metrics.backlog_count.saturating_add(sample.backlog_count);
        metrics.dispatch_rate += sample.dispatch_rate;
    }

    snapshot
}

/// Restrict a full version snapshot to one scaling group's effective task types.
#[must_use]
pub fn metrics_for_group(
    snapshot: &MetricsSnapshot,
    effective_task_types: &BTreeSet<WorkerComputeTaskType>,
) -> MetricsSnapshot {
    MetricsSnapshot {
        workflow: effective_task_types
            .contains(&WorkerComputeTaskType::Workflow)
            .then_some(snapshot.workflow.unwrap_or_default()),
        activity: effective_task_types
            .contains(&WorkerComputeTaskType::Activity)
            .then_some(snapshot.activity.unwrap_or_default()),
        nexus: effective_task_types
            .contains(&WorkerComputeTaskType::Nexus)
            .then_some(snapshot.nexus.unwrap_or_default()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use proptest::prelude::*;
    use time::{Duration, OffsetDateTime};
    use tokeira_storage::{
        InMemoryStore, InMemoryWorkerComputeRepository, WorkerComputeQueueSample,
        WorkerComputeRepository,
    };
    use tokeira_types::{
        BuildId, ControllerInstanceKey, DeploymentId, IncarnationId, NamespaceId, TaskQueueName,
        WorkerComputeQueueKey, WorkerComputeTaskType,
    };
    use uuid::Uuid;

    use super::*;

    fn controller(namespace: u128, deployment: &str, build_id: &str) -> ControllerInstanceKey {
        ControllerInstanceKey {
            namespace_id: NamespaceId(Uuid::from_u128(namespace)),
            deployment_name: DeploymentId(deployment.to_owned()),
            build_id: BuildId(build_id.to_owned()),
        }
    }

    fn sample(
        controller: &ControllerInstanceKey,
        task_type: WorkerComputeTaskType,
        queue: usize,
        backlog_count: u64,
        dispatch_rate: f64,
        sampled_at: OffsetDateTime,
    ) -> WorkerComputeQueueSample {
        WorkerComputeQueueSample {
            key: WorkerComputeQueueKey {
                namespace_id: controller.namespace_id,
                deployment_name: controller.deployment_name.clone(),
                build_id: controller.build_id.clone(),
                task_type,
                task_queue: TaskQueueName(format!("queue-{queue}")),
            },
            writer_id: IncarnationId(Uuid::from_u128(99)),
            writer_sequence: u64::try_from(queue).expect("test queue index fits u64"),
            backlog_count,
            add_rate: 0.0,
            dispatch_rate,
            sampled_at,
        }
    }

    #[test]
    fn queue_metrics_record_saturating_totals() {
        let metrics = WorkerComputeQueueMetrics::default();
        let owner = controller(1, "deployment", "build");
        let key = WorkerComputeQueueKey {
            namespace_id: owner.namespace_id,
            deployment_name: owner.deployment_name,
            build_id: owner.build_id,
            task_type: WorkerComputeTaskType::Workflow,
            task_queue: TaskQueueName("queue".to_owned()),
        };

        metrics.record_add(key.clone());
        metrics.record_add(key.clone());
        metrics.record_dispatch(key.clone());

        assert_eq!(
            metrics.snapshot().get(&key),
            Some(&WorkerComputeQueueCounters {
                adds: 2,
                dispatches: 1,
            })
        );
    }

    #[tokio::test]
    async fn sampler_persists_periodic_rates_off_the_task_path() {
        let run_repository = Arc::new(InMemoryStore::default());
        let sample_repository = Arc::new(InMemoryWorkerComputeRepository::default());
        let queue_metrics = WorkerComputeQueueMetrics::default();
        let owner = controller(1, "deployment", "build");
        let key = WorkerComputeQueueKey {
            namespace_id: owner.namespace_id,
            deployment_name: owner.deployment_name.clone(),
            build_id: owner.build_id.clone(),
            task_type: WorkerComputeTaskType::Workflow,
            task_queue: TaskQueueName("queue".to_owned()),
        };
        let writer_id = IncarnationId(Uuid::from_u128(44));
        let sampler = WorkerComputeQueueSampler::new(
            run_repository,
            sample_repository.clone(),
            InMemoryBroker::default(),
            InMemoryActivityBroker::default(),
            NexusTaskBroker::default(),
            NexusEndpointRegistry::default(),
            queue_metrics.clone(),
            writer_id,
        );
        let started_at = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);

        queue_metrics.record_add(key.clone());
        let first = sampler.sample_once(&[], started_at).await.expect("sample");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].add_rate, 0.0);

        queue_metrics.record_add(key.clone());
        queue_metrics.record_dispatch(key.clone());
        let second = sampler
            .sample_once(&[], started_at + Duration::seconds(10))
            .await
            .expect("sample");
        assert_eq!(second[0].writer_sequence, 2);
        assert_eq!(second[0].add_rate, 0.1);
        assert_eq!(second[0].dispatch_rate, 0.1);
        assert_eq!(
            sample_repository
                .list_queue_samples(&owner, started_at)
                .await
                .expect("stored samples"),
            second
        );
    }

    // Feature: worker-compute-controller, Property 8: metrics aggregate by version, type, and effective group
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn metrics_aggregate_by_version_type_and_effective_group(
            rows in prop::collection::vec(
                (
                    0usize..32,
                    0u8..3,
                    0u64..10_000,
                    0u32..10_000,
                    any::<bool>(),
                    any::<bool>(),
                ),
                0..128,
            ),
            owns_workflow in any::<bool>(),
            owns_activity in any::<bool>(),
            owns_nexus in any::<bool>(),
        ) {
            let now = OffsetDateTime::UNIX_EPOCH + Duration::hours(10);
            let target = controller(1, "deployment", "build");
            let other = controller(2, "other", "other-build");
            let mut samples = Vec::new();
            let mut expected = [TaskTypeMetrics::default(); 3];

            for (queue, task_type, backlog, rate_basis, stale, wrong_version) in rows {
                let task_type = match task_type {
                    0 => WorkerComputeTaskType::Workflow,
                    1 => WorkerComputeTaskType::Activity,
                    _ => WorkerComputeTaskType::Nexus,
                };
                let sampled_at = if stale {
                    now - Duration::minutes(3)
                } else {
                    now - Duration::seconds(5)
                };
                let rate = f64::from(rate_basis) / 10.0;
                let owner = if wrong_version { &other } else { &target };
                samples.push(sample(
                    owner,
                    task_type,
                    queue,
                    backlog,
                    rate,
                    sampled_at,
                ));
                if !stale && !wrong_version {
                    let index = match task_type {
                        WorkerComputeTaskType::Workflow => 0,
                        WorkerComputeTaskType::Activity => 1,
                        WorkerComputeTaskType::Nexus => 2,
                    };
                    expected[index].backlog_count =
                        expected[index].backlog_count.saturating_add(backlog);
                    expected[index].dispatch_rate += rate;
                }
            }

            let original_samples = samples.clone();
            let snapshot = aggregate_queue_samples(&target, &samples, now);
            prop_assert_eq!(snapshot.workflow, Some(expected[0]));
            prop_assert_eq!(snapshot.activity, Some(expected[1]));
            prop_assert_eq!(snapshot.nexus, Some(expected[2]));

            let effective = [
                (WorkerComputeTaskType::Workflow, owns_workflow),
                (WorkerComputeTaskType::Activity, owns_activity),
                (WorkerComputeTaskType::Nexus, owns_nexus),
            ]
            .into_iter()
            .filter_map(|(task_type, owned)| owned.then_some(task_type))
            .collect::<BTreeSet<_>>();
            let group = metrics_for_group(&snapshot, &effective);
            prop_assert_eq!(group.workflow, owns_workflow.then_some(expected[0]));
            prop_assert_eq!(group.activity, owns_activity.then_some(expected[1]));
            prop_assert_eq!(group.nexus, owns_nexus.then_some(expected[2]));

            // Aggregation is read-only over caller-owned delivery observations.
            prop_assert_eq!(samples, original_samples);
        }
    }
}
