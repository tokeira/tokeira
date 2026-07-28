//! Pure in-memory batching for best-effort demand observations.
//!
//! Batches are isolated by exact Worker Deployment Version. Time is supplied by
//! the caller, so this state machine has no timers, tasks, I/O, or hidden clock.

use std::collections::{BTreeMap, BTreeSet};

use time::OffsetDateTime;
use tokeira_types::{ControllerInstanceKey, WorkerComputeTaskQueueBinding};

use super::{
    DemandMatchKind, DemandObservation, NO_SYNC_BATCH_INTERVAL, ObservationBatch,
    SYNC_ONLY_BATCH_INTERVAL,
};

/// Exact-version observation accumulator with deterministic due ordering.
#[derive(Clone, Debug, Default)]
pub struct ObservationBatcher {
    batches: BTreeMap<ControllerInstanceKey, ObservationBatch>,
}

impl ObservationBatcher {
    /// Record one observation without moving an existing batch deadline later.
    pub fn ingest(&mut self, observation: DemandObservation, now: OffsetDateTime) {
        let key = observation.controller_key();
        let batch = self.batches.entry(key).or_insert_with(|| ObservationBatch {
            first_observed_at: now,
            first_no_sync_at: None,
            sync_count: 0,
            no_sync_count: 0,
            task_types: BTreeSet::new(),
            counts_by_task_type: BTreeMap::new(),
            task_queues: BTreeSet::new(),
        });
        let counts = batch
            .counts_by_task_type
            .entry(observation.task_type)
            .or_default();
        match observation.match_kind {
            DemandMatchKind::Sync => {
                batch.sync_count = batch.sync_count.saturating_add(1);
                counts.sync_count = counts.sync_count.saturating_add(1);
            }
            DemandMatchKind::NoSync => {
                batch.no_sync_count = batch.no_sync_count.saturating_add(1);
                counts.no_sync_count = counts.no_sync_count.saturating_add(1);
                batch.first_no_sync_at.get_or_insert(now);
            }
        }
        batch.task_types.insert(observation.task_type);
        batch.task_queues.insert(WorkerComputeTaskQueueBinding {
            name: observation.task_queue,
            task_type: observation.task_type,
        });
    }

    /// Remove every batch due at `now`, ordered by exact controller identity.
    #[must_use]
    pub fn take_due(
        &mut self,
        now: OffsetDateTime,
    ) -> Vec<(ControllerInstanceKey, ObservationBatch)> {
        let due_keys = self
            .batches
            .iter()
            .filter_map(|(key, batch)| (batch.due_at() <= now).then_some(key.clone()))
            .collect::<Vec<_>>();
        due_keys
            .into_iter()
            .map(|key| {
                let batch = self
                    .batches
                    .remove(&key)
                    .expect("due key was read from this map");
                (key, batch)
            })
            .collect()
    }

    /// Number of exact-version batches awaiting evaluation.
    #[must_use]
    pub fn len(&self) -> usize {
        self.batches.len()
    }

    /// Whether no observations are currently retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Earliest pending deadline, if any.
    #[must_use]
    pub fn next_due_at(&self) -> Option<OffsetDateTime> {
        self.batches.values().map(ObservationBatch::due_at).min()
    }
}

impl ObservationBatch {
    /// Fixed deadline chosen by the earliest observation kind in this batch.
    #[must_use]
    pub fn due_at(&self) -> OffsetDateTime {
        let sync_deadline = self.first_observed_at
            + time::Duration::try_from(SYNC_ONLY_BATCH_INTERVAL)
                .expect("fixed sync batch interval fits time::Duration");
        self.first_no_sync_at
            .map_or(sync_deadline, |first_no_sync_at| {
                sync_deadline.min(
                    first_no_sync_at
                        + time::Duration::try_from(NO_SYNC_BATCH_INTERVAL)
                            .expect("fixed no-sync batch interval fits time::Duration"),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use time::Duration;
    use tokeira_types::{BuildId, DeploymentId, NamespaceId, TaskQueueName, WorkerComputeTaskType};

    use super::*;

    fn observation(
        namespace_id: NamespaceId,
        version: u8,
        match_kind: DemandMatchKind,
    ) -> DemandObservation {
        DemandObservation {
            namespace_id,
            task_queue: TaskQueueName(format!("queue-{version}")),
            task_type: match version % 3 {
                0 => WorkerComputeTaskType::Workflow,
                1 => WorkerComputeTaskType::Activity,
                _ => WorkerComputeTaskType::Nexus,
            },
            deployment_name: DeploymentId(format!("deployment-{version}")),
            build_id: BuildId(format!("build-{version}")),
            match_kind,
        }
    }

    fn reference_due_at(batch: &ObservationBatch) -> OffsetDateTime {
        let sync_deadline = batch.first_observed_at + Duration::seconds(60);
        batch.first_no_sync_at.map_or(sync_deadline, |first| {
            sync_deadline.min(first + Duration::milliseconds(500))
        })
    }

    #[test]
    fn first_no_sync_shortens_but_later_observations_do_not_extend_deadline() {
        let namespace_id = NamespaceId::new();
        let start = OffsetDateTime::UNIX_EPOCH;
        let mut batcher = ObservationBatcher::default();
        batcher.ingest(observation(namespace_id, 0, DemandMatchKind::Sync), start);
        batcher.ingest(
            observation(namespace_id, 0, DemandMatchKind::NoSync),
            start + Duration::seconds(2),
        );
        batcher.ingest(
            observation(namespace_id, 0, DemandMatchKind::NoSync),
            start + Duration::seconds(30),
        );
        assert!(batcher.take_due(start + Duration::seconds(2)).is_empty());
        let due = batcher.take_due(start + Duration::milliseconds(2_500));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].1.sync_count, 1);
        assert_eq!(due[0].1.no_sync_count, 2);
    }

    #[test]
    fn counts_saturate_instead_of_wrapping() {
        let namespace_id = NamespaceId::new();
        let observation = observation(namespace_id, 0, DemandMatchKind::Sync);
        let key = observation.controller_key();
        let mut batcher = ObservationBatcher::default();
        batcher.batches.insert(
            key.clone(),
            ObservationBatch {
                first_observed_at: OffsetDateTime::UNIX_EPOCH,
                first_no_sync_at: None,
                sync_count: u64::MAX,
                no_sync_count: 0,
                task_types: BTreeSet::new(),
                counts_by_task_type: BTreeMap::new(),
                task_queues: BTreeSet::new(),
            },
        );
        batcher.ingest(observation, OffsetDateTime::UNIX_EPOCH);
        assert_eq!(batcher.batches[&key].sync_count, u64::MAX);
    }

    #[test]
    fn late_no_sync_cannot_extend_an_already_due_sync_batch() {
        let namespace_id = NamespaceId::new();
        let start = OffsetDateTime::UNIX_EPOCH;
        let mut batcher = ObservationBatcher::default();
        batcher.ingest(observation(namespace_id, 0, DemandMatchKind::Sync), start);
        batcher.ingest(
            observation(namespace_id, 0, DemandMatchKind::NoSync),
            start + Duration::seconds(61),
        );
        assert_eq!(batcher.take_due(start + Duration::seconds(61)).len(), 1);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: worker-compute-controller, Property 7: batch eligibility matches the reference clock
        #[test]
        fn property_batch_eligibility_matches_reference_clock(
            events in proptest::collection::vec((0u8..5, any::<bool>(), 0u16..2_000), 1..100),
        ) {
            let namespace_id = NamespaceId::new();
            let start = OffsetDateTime::UNIX_EPOCH;
            let mut batcher = ObservationBatcher::default();
            let mut model = BTreeMap::<ControllerInstanceKey, ObservationBatch>::new();
            let mut elapsed_ms = 0i64;
            for (version, no_sync, advance_ms) in events {
                elapsed_ms = elapsed_ms.saturating_add(i64::from(advance_ms));
                let now = start + Duration::milliseconds(elapsed_ms);
                let match_kind = if no_sync {
                    DemandMatchKind::NoSync
                } else {
                    DemandMatchKind::Sync
                };
                let event = observation(namespace_id, version, match_kind);
                let key = event.controller_key();
                batcher.ingest(event.clone(), now);

                let expected = model.entry(key).or_insert_with(|| ObservationBatch {
                    first_observed_at: now,
                    first_no_sync_at: None,
                    sync_count: 0,
                    no_sync_count: 0,
                    task_types: BTreeSet::new(),
                    counts_by_task_type: BTreeMap::new(),
                    task_queues: BTreeSet::new(),
                });
                if no_sync {
                    expected.no_sync_count = expected.no_sync_count.saturating_add(1);
                    if expected.first_no_sync_at.is_none() {
                        expected.first_no_sync_at = Some(now);
                    }
                } else {
                    expected.sync_count = expected.sync_count.saturating_add(1);
                }
                expected.task_types.insert(event.task_type);
                let counts = expected
                    .counts_by_task_type
                    .entry(event.task_type)
                    .or_default();
                if no_sync {
                    counts.no_sync_count = counts.no_sync_count.saturating_add(1);
                } else {
                    counts.sync_count = counts.sync_count.saturating_add(1);
                }
                expected.task_queues.insert(WorkerComputeTaskQueueBinding {
                    name: event.task_queue,
                    task_type: event.task_type,
                });
            }

            let final_now = start + Duration::milliseconds(elapsed_ms);
            let expected_due = model
                .iter()
                .filter_map(|(key, batch)| {
                    (reference_due_at(batch) <= final_now).then_some((key.clone(), batch.clone()))
                })
                .collect::<Vec<_>>();
            for (key, _) in &expected_due {
                model.remove(key);
            }
            prop_assert_eq!(batcher.take_due(final_now), expected_due);
            prop_assert_eq!(batcher.batches, model);
        }
    }
}
