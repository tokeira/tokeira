//! Pure implementation of the pinned WCI `no-sync` decision behavior.
//!
//! This is an original Rust state machine over explicit inputs and time. Behavior
//! is verified against `wci/workflow/scaling_algorithm/no_sync_match.go` at
//! `go.temporal.io/auto-scaled-workers` commit `edd947d743d2`; no WCI workflow,
//! activity, storage, or provider architecture is imported.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use time::OffsetDateTime;
use tokeira_types::{WorkerComputeInvokeReason, WorkerComputeTaskType};

use super::{
    MetricsSnapshot, NoSyncConfig, NoSyncState, ObservationBatch, ScaleUpDecision, ScalerDecision,
    ScalerSuppression,
};

/// Evaluate one exact-version demand batch.
#[must_use]
pub fn evaluate_task_add(
    config: &NoSyncConfig,
    state: &NoSyncState,
    batch: &ObservationBatch,
    now: OffsetDateTime,
) -> ScalerDecision {
    let mut next_state = state.clone();
    let can_scale = cooloff_elapsed(config, state, now);
    let action = (batch.no_sync_count > 0 && can_scale).then(|| {
        next_state.last_scale_up_at = Some(now);
        ScaleUpDecision {
            reason: WorkerComputeInvokeReason::NoSyncMatch,
            count: 1,
        }
    });
    let suppressions = if batch.no_sync_count > 0 && !can_scale {
        batch
            .counts_by_task_type
            .iter()
            .filter_map(|(task_type, counts)| {
                (counts.no_sync_count > 0).then_some((*task_type, ScalerSuppression::Cooloff))
            })
            .collect()
    } else {
        BTreeMap::new()
    };
    ScalerDecision {
        next_state,
        action,
        next_poll_after: None,
        suppressions,
    }
}

/// Evaluate periodic metrics for the task types currently owned by one group.
#[must_use]
pub fn evaluate_metrics(
    config: &NoSyncConfig,
    state: &NoSyncState,
    snapshot: &MetricsSnapshot,
    effective_types: &BTreeSet<WorkerComputeTaskType>,
    now: OffsetDateTime,
) -> ScalerDecision {
    let mut next_state = state.clone();
    let elapsed_ms = elapsed_since_scale_up_ms(state, now);
    let mut backlog_scale_up = false;
    let mut refresh_scale_up = false;
    let mut suppressions = BTreeMap::new();

    for task_type in [
        WorkerComputeTaskType::Workflow,
        WorkerComputeTaskType::Activity,
        WorkerComputeTaskType::Nexus,
    ] {
        if !effective_types.contains(&task_type) {
            continue;
        }
        let metrics = snapshot.get(task_type).unwrap_or_default();
        let backlog_demand = metrics.backlog_count
            > u64::try_from(config.scale_up_backlog_threshold)
                .expect("validated no-sync backlog threshold is non-negative");
        let backlog_branch = backlog_demand && elapsed_ms >= config.scale_up_cooloff_ms;
        let refresh_branch = !backlog_branch
            && config.max_worker_lifetime_ms > 0
            && metrics.backlog_count > 0
            && elapsed_ms >= config.max_worker_lifetime_ms;
        let suppressed = (backlog_branch || refresh_branch)
            && config.scale_up_dispatch_rate_epsilon > 0.0
            && state
                .prior_dispatch_rates
                .get(&task_type)
                .is_some_and(|prior| {
                    (metrics.dispatch_rate - prior).abs() <= config.scale_up_dispatch_rate_epsilon
                });
        if !suppressed {
            backlog_scale_up |= backlog_branch;
            refresh_scale_up |= refresh_branch;
        } else {
            suppressions.insert(task_type, ScalerSuppression::Epsilon);
        }
        if backlog_demand && elapsed_ms < config.scale_up_cooloff_ms {
            suppressions.insert(task_type, ScalerSuppression::Cooloff);
        }
        next_state
            .prior_dispatch_rates
            .insert(task_type, metrics.dispatch_rate);
    }

    let reason = if backlog_scale_up {
        Some(WorkerComputeInvokeReason::Backlog)
    } else if refresh_scale_up {
        Some(WorkerComputeInvokeReason::WorkerRefresh)
    } else {
        None
    };
    let action = reason.map(|reason| {
        next_state.last_scale_up_at = Some(now);
        ScaleUpDecision { reason, count: 1 }
    });
    ScalerDecision {
        next_state,
        action,
        next_poll_after: Some(Duration::from_millis(
            u64::try_from(config.metrics_poll_interval_ms)
                .expect("validated no-sync poll interval is non-negative"),
        )),
        suppressions,
    }
}

fn cooloff_elapsed(config: &NoSyncConfig, state: &NoSyncState, now: OffsetDateTime) -> bool {
    elapsed_since_scale_up_ms(state, now) >= config.scale_up_cooloff_ms
}

fn elapsed_since_scale_up_ms(state: &NoSyncState, now: OffsetDateTime) -> i64 {
    let last_scale_up_at = state.last_scale_up_at.unwrap_or(OffsetDateTime::UNIX_EPOCH);
    (now - last_scale_up_at)
        .whole_milliseconds()
        .clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;
    use time::Duration as TimeDuration;

    use super::*;
    use crate::worker_compute::TaskTypeMetrics;

    fn config() -> NoSyncConfig {
        NoSyncConfig {
            scale_up_cooloff_ms: 100,
            scale_up_backlog_threshold: 0,
            max_worker_lifetime_ms: 600_000,
            scale_up_dispatch_rate_epsilon: 0.0,
            metrics_poll_interval_ms: 60_000,
        }
    }

    fn batch(sync: u64, no_sync: u64) -> ObservationBatch {
        ObservationBatch {
            first_observed_at: OffsetDateTime::UNIX_EPOCH,
            first_no_sync_at: (no_sync > 0).then_some(OffsetDateTime::UNIX_EPOCH),
            sync_count: sync,
            no_sync_count: no_sync,
            task_types: BTreeSet::new(),
            counts_by_task_type: BTreeMap::new(),
            task_queues: BTreeSet::new(),
        }
    }

    #[test]
    fn task_add_and_metrics_cover_pinned_boundaries() {
        let now = OffsetDateTime::UNIX_EPOCH + TimeDuration::seconds(10);
        assert!(
            evaluate_task_add(&config(), &NoSyncState::default(), &batch(1, 0), now)
                .action
                .is_none()
        );
        assert_eq!(
            evaluate_task_add(&config(), &NoSyncState::default(), &batch(0, 1), now)
                .action
                .expect("no-sync after cooloff scales")
                .reason,
            WorkerComputeInvokeReason::NoSyncMatch
        );

        let exact_threshold = MetricsSnapshot {
            workflow: Some(TaskTypeMetrics {
                backlog_count: 5,
                dispatch_rate: 10.0,
            }),
            ..MetricsSnapshot::default()
        };
        let mut threshold_config = config();
        threshold_config.scale_up_backlog_threshold = 5;
        threshold_config.max_worker_lifetime_ms = 0;
        assert!(
            evaluate_metrics(
                &threshold_config,
                &NoSyncState::default(),
                &exact_threshold,
                &BTreeSet::from([WorkerComputeTaskType::Workflow]),
                now,
            )
            .action
            .is_none()
        );

        threshold_config.scale_up_backlog_threshold = 100;
        threshold_config.max_worker_lifetime_ms = 1_000;
        threshold_config.scale_up_cooloff_ms = 30_000;
        assert_eq!(
            evaluate_metrics(
                &threshold_config,
                &NoSyncState::default(),
                &exact_threshold,
                &BTreeSet::from([WorkerComputeTaskType::Workflow]),
                now,
            )
            .action
            .expect("refresh is independent of cooloff")
            .reason,
            WorkerComputeInvokeReason::WorkerRefresh
        );
    }

    fn reference_metrics(
        config: &NoSyncConfig,
        state: &NoSyncState,
        snapshot: &MetricsSnapshot,
        effective_types: &BTreeSet<WorkerComputeTaskType>,
        now: OffsetDateTime,
    ) -> ScalerDecision {
        let mut rates = state.prior_dispatch_rates.clone();
        let last = state.last_scale_up_at.unwrap_or(OffsetDateTime::UNIX_EPOCH);
        let elapsed = (now - last).whole_milliseconds() as i64;
        let mut backlog = false;
        let mut refresh = false;
        let mut suppressions = BTreeMap::new();
        for task_type in [
            WorkerComputeTaskType::Workflow,
            WorkerComputeTaskType::Activity,
            WorkerComputeTaskType::Nexus,
        ] {
            if !effective_types.contains(&task_type) {
                continue;
            }
            let value = snapshot.get(task_type).unwrap_or_default();
            let backlog_demand = value.backlog_count > config.scale_up_backlog_threshold as u64;
            let threshold = backlog_demand && elapsed >= config.scale_up_cooloff_ms;
            let lifetime = !threshold
                && config.max_worker_lifetime_ms > 0
                && value.backlog_count > 0
                && elapsed >= config.max_worker_lifetime_ms;
            let close_rate = rates.get(&task_type).is_some_and(|prior| {
                config.scale_up_dispatch_rate_epsilon > 0.0
                    && (value.dispatch_rate - prior).abs() <= config.scale_up_dispatch_rate_epsilon
            });
            if !close_rate {
                backlog |= threshold;
                refresh |= lifetime;
            } else if threshold || lifetime {
                suppressions.insert(task_type, ScalerSuppression::Epsilon);
            }
            if backlog_demand && elapsed < config.scale_up_cooloff_ms {
                suppressions.insert(task_type, ScalerSuppression::Cooloff);
            }
            rates.insert(task_type, value.dispatch_rate);
        }
        let reason = backlog
            .then_some(WorkerComputeInvokeReason::Backlog)
            .or_else(|| refresh.then_some(WorkerComputeInvokeReason::WorkerRefresh));
        ScalerDecision {
            next_state: NoSyncState {
                last_scale_up_at: reason.map_or(state.last_scale_up_at, |_| Some(now)),
                prior_dispatch_rates: rates,
            },
            action: reason.map(|reason| ScaleUpDecision { reason, count: 1 }),
            next_poll_after: Some(Duration::from_millis(
                config.metrics_poll_interval_ms as u64,
            )),
            suppressions,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: worker-compute-controller, Property 9: no-sync decisions match the pinned reference model
        #[test]
        fn property_no_sync_decisions_match_pinned_reference_model(
            cooloff in 0i64..100_000,
            threshold in 0i64..100,
            lifetime in 0i64..1_000_000,
            epsilon in 0.0f64..100.0,
            elapsed in 0i64..2_000_000,
            workflow in (0u64..200, 0.0f64..1_000.0),
            activity in (0u64..200, 0.0f64..1_000.0),
            nexus in (0u64..200, 0.0f64..1_000.0),
            prior in proptest::collection::btree_map(0u8..3, 0.0f64..1_000.0, 0..=3),
            owned in proptest::collection::btree_set(0u8..3, 0..=3),
        ) {
            let config = NoSyncConfig {
                scale_up_cooloff_ms: cooloff,
                scale_up_backlog_threshold: threshold,
                max_worker_lifetime_ms: lifetime,
                scale_up_dispatch_rate_epsilon: epsilon,
                metrics_poll_interval_ms: 60_000,
            };
            let task_type = |value| match value {
                0 => WorkerComputeTaskType::Workflow,
                1 => WorkerComputeTaskType::Activity,
                _ => WorkerComputeTaskType::Nexus,
            };
            let state = NoSyncState {
                last_scale_up_at: Some(OffsetDateTime::UNIX_EPOCH),
                prior_dispatch_rates: prior
                    .into_iter()
                    .map(|(key, value)| (task_type(key), value))
                    .collect::<BTreeMap<_, _>>(),
            };
            let snapshot = MetricsSnapshot {
                workflow: Some(TaskTypeMetrics {
                    backlog_count: workflow.0,
                    dispatch_rate: workflow.1,
                }),
                activity: Some(TaskTypeMetrics {
                    backlog_count: activity.0,
                    dispatch_rate: activity.1,
                }),
                nexus: Some(TaskTypeMetrics {
                    backlog_count: nexus.0,
                    dispatch_rate: nexus.1,
                }),
            };
            let effective_types = owned.into_iter().map(task_type).collect::<BTreeSet<_>>();
            let now = OffsetDateTime::UNIX_EPOCH + TimeDuration::milliseconds(elapsed);
            prop_assert_eq!(
                evaluate_metrics(&config, &state, &snapshot, &effective_types, now),
                reference_metrics(&config, &state, &snapshot, &effective_types, now)
            );

            let demand = batch(1, workflow.0);
            let expected_action = workflow.0 > 0 && elapsed >= cooloff;
            let decision = evaluate_task_add(&config, &state, &demand, now);
            prop_assert_eq!(decision.action.is_some(), expected_action);
            prop_assert_eq!(
                decision.next_state.last_scale_up_at,
                if expected_action { Some(now) } else { state.last_scale_up_at }
            );
        }
    }
}
