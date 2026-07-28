//! Runtime metric definitions and recording helpers.

use metrics::{counter, gauge, histogram};
use tokeira_observability::{
    NotShardOwnerOperationLabel, OutcomeLabel, QueryBufferOutcomeLabel, QueryDispatchOutcomeLabel,
    QueryDispatchPathLabel, RetryOutcomeLabel,
};
#[cfg(test)]
use tokeira_types::validate_metric_name;
use tokeira_types::{
    MetricType, NamespaceId, QueueKey, TaskKind, WorkerComputeHealth, WorkerComputeInvokeReason,
    WorkerComputeTaskType, WorkerInstanceKey,
};

use crate::worker_compute::{ObserveResult, ScalerSuppression, WorkerComputeProviderTargetKind};

pub const BROKER_PUBLISH_TOTAL: &str = "tokeira_runtime_broker_publish_total";
pub const BROKER_SYNC_MATCH_TOTAL: &str = "tokeira_runtime_broker_sync_match_total";
pub const BROKER_NON_SYNC_MATCH_TOTAL: &str = "tokeira_runtime_broker_non_sync_match_total";
pub const BROKER_POLL_TIMEOUT_TOTAL: &str = "tokeira_runtime_broker_poll_timeout_total";
pub const BROKER_QUEUE_DEPTH: &str = "tokeira_runtime_broker_queue_depth";
pub const LANE_SUBMIT_DURATION_SECONDS: &str = "tokeira_runtime_lane_submit_duration_seconds";
pub const LANE_QUEUE_WAIT_SECONDS: &str = "tokeira_runtime_lane_queue_wait_seconds";
pub const LANE_PROCESSING_DURATION_SECONDS: &str =
    "tokeira_runtime_lane_processing_duration_seconds";
pub const NOT_SHARD_OWNER_TOTAL: &str = "tokeira_runtime_not_shard_owner_total";
pub const QUERY_DISPATCH_TOTAL: &str = "tokeira_runtime_query_dispatch_total";
pub const QUERY_BUFFER_WAIT_SECONDS: &str = "tokeira_runtime_query_buffer_wait_seconds";
pub const WORKFLOW_TASK_STARTED_TOTAL: &str = "tokeira_runtime_workflow_task_started_total";
pub const WORKFLOW_TASK_COMPLETED_TOTAL: &str = "tokeira_runtime_workflow_task_completed_total";
pub const WORKFLOW_TASK_TIMED_OUT_TOTAL: &str = "tokeira_runtime_workflow_task_timed_out_total";
/// Speculative workflow task that MATERIALIZED into history (worker returned
/// commands, heartbeat, interleaved events, or an update acceptance) — the
/// bridge renames this to Temporal's `speculative_workflow_task_commits`
/// (metric_defs.go:940 @ v1.31.0; spec speculative-wft M.1).
pub const SPECULATIVE_WFT_COMMITS_TOTAL: &str =
    "tokeira_runtime_speculative_workflow_task_commits_total";
/// Speculative workflow task that was DISCARDED without a trace (rejection-only
/// completion in the drop window) — renamed to Temporal's
/// `speculative_workflow_task_rollbacks` (metric_defs.go:941 @ v1.31.0).
pub const SPECULATIVE_WFT_ROLLBACKS_TOTAL: &str =
    "tokeira_runtime_speculative_workflow_task_rollbacks_total";
/// One in-memory speculative-WFT timer task processed — renamed to Temporal's
/// generic `task_requests`, tagged `operation=TimerActiveTaskSpeculative
/// WorkflowTaskTimeout` (queues/metrics.go:93-97 @ v1.31.0).
pub const SPECULATIVE_TIMER_TASK_REQUESTS_TOTAL: &str =
    "tokeira_runtime_speculative_timer_task_requests_total";
/// A speculative workflow task that timed out on its START-to-close deadline —
/// renamed to Temporal's `start_to_close_timeout`, same operation tag
/// (metric_defs.go:964 + timer_queue_active_task_executor.go:424-437 @ v1.31.0).
pub const SPECULATIVE_START_TO_CLOSE_TIMEOUT_TOTAL: &str =
    "tokeira_runtime_speculative_start_to_close_timeout_total";
pub const ACTIVITY_TASK_STARTED_TOTAL: &str = "tokeira_runtime_activity_task_started_total";
pub const ACTIVITY_TASK_COMPLETED_TOTAL: &str = "tokeira_runtime_activity_task_completed_total";
pub const ACTIVITY_TASK_FAILED_TOTAL: &str = "tokeira_runtime_activity_task_failed_total";
pub const ACTIVITY_TASK_RETRY_TOTAL: &str = "tokeira_runtime_activity_task_retry_total";
pub const ACTIVITY_TASK_TIMED_OUT_TOTAL: &str = "tokeira_runtime_activity_task_timed_out_total";
pub const SCANNER_TICK_TOTAL: &str = "tokeira_runtime_scanner_tick_total";
pub const SCANNER_DISPATCHED_TOTAL: &str = "tokeira_runtime_scanner_dispatched_total";
pub const OCC_RETRY_TOTAL: &str = "tokeira_runtime_occ_retry_total";
pub const KERNEL_TRANSITION_COMMITTED_TOTAL: &str = "tokeira_kernel_transition_committed_total";
pub const KERNEL_EVENTS_EMITTED_TOTAL: &str = "tokeira_kernel_events_emitted_total";
pub const KERNEL_COMMANDS_PROCESSED_TOTAL: &str = "tokeira_kernel_commands_processed_total";
pub const WORKER_HEARTBEATS_ACCEPTED_TOTAL: &str = "tokeira_worker_heartbeats_accepted_total";
pub const WORKER_HEARTBEATS_REJECTED_TOTAL: &str = "tokeira_worker_heartbeats_rejected_total";
pub const WORKERS_OBSERVED: &str = "tokeira_worker_heartbeat_entries_observed";
pub const WORKERS_TOTAL: &str = "tokeira_worker_heartbeat_entries_total";
pub const WORKER_HEARTBEAT_ACTIVE: &str = "tokeira_worker_heartbeat_active_state";
pub const WORKER_LAST_HEARTBEAT_AGE_SECONDS: &str = "tokeira_worker_last_heartbeat_age_seconds";
/// Best-effort worker-compute observations by bounded channel outcome.
pub const WORKER_COMPUTE_OBSERVATIONS_TOTAL: &str =
    "tokeira_runtime_worker_compute_observations_total";
/// Worker-compute scaler actions by task family and bounded reason.
pub const WORKER_COMPUTE_DECISIONS_TOTAL: &str = "tokeira_runtime_worker_compute_decisions_total";
/// Worker-compute scaler suppressions by task family and bounded policy reason.
pub const WORKER_COMPUTE_SUPPRESSIONS_TOTAL: &str =
    "tokeira_runtime_worker_compute_suppressions_total";
/// Durable provider-attempt finalizations by Nexus target kind and outcome.
pub const WORKER_COMPUTE_ACTIONS_TOTAL: &str = "tokeira_runtime_worker_compute_actions_total";
/// Wall-clock duration of a provider attempt through the selected Nexus target.
pub const WORKER_COMPUTE_ACTION_LATENCY_SECONDS: &str =
    "tokeira_runtime_worker_compute_action_latency_seconds";
/// Current durable scaling-group count by bounded health category.
pub const WORKER_COMPUTE_HEALTH: &str = "tokeira_runtime_worker_compute_health";

/// Canonical bounded labels for the worker-compute metric family.
pub const WORKER_COMPUTE_METRIC_LABEL_KEYS: &[(&str, &[&str])] = &[
    (WORKER_COMPUTE_OBSERVATIONS_TOTAL, &["task_type", "outcome"]),
    (WORKER_COMPUTE_DECISIONS_TOTAL, &["task_type", "reason"]),
    (WORKER_COMPUTE_SUPPRESSIONS_TOTAL, &["task_type", "reason"]),
    (WORKER_COMPUTE_ACTIONS_TOTAL, &["target_kind", "outcome"]),
    (
        WORKER_COMPUTE_ACTION_LATENCY_SECONDS,
        &["target_kind", "outcome"],
    ),
    (WORKER_COMPUTE_HEALTH, &["health"]),
];

// Outbound Nexus requests the runtime (history-service analogue) makes on a caller's
// behalf. v1.31.0 records these in the history service (`OutboundRequestCounter`,
// `components/nexusoperations/executors.go:320-331 @ v1.31.0`); tokeira observes them at two
// sites that both record this one metric — the External-endpoint HTTP `start_operation` in
// the publisher, and the worker-endpoint StartOperation resolved by a worker's
// `RespondNexusTask{Completed,Failed}` at the edge (which calls these helpers across the
// edge→runtime dependency). The conformance metrics bridge renames them to Temporal's
// `nexus_outbound_requests` / `nexus_outbound_latency`.
pub const NEXUS_OUTBOUND_REQUESTS_TOTAL: &str = "tokeira_runtime_nexus_outbound_requests_total";
pub const NEXUS_OUTBOUND_LATENCY_SECONDS: &str = "tokeira_runtime_nexus_outbound_latency_seconds";

pub const METRIC_NAMES: &[(&str, MetricType)] = &[
    (BROKER_PUBLISH_TOTAL, MetricType::Counter),
    (BROKER_SYNC_MATCH_TOTAL, MetricType::Counter),
    (BROKER_NON_SYNC_MATCH_TOTAL, MetricType::Counter),
    (BROKER_POLL_TIMEOUT_TOTAL, MetricType::Counter),
    (BROKER_QUEUE_DEPTH, MetricType::Gauge),
    (LANE_SUBMIT_DURATION_SECONDS, MetricType::DurationHistogram),
    (LANE_QUEUE_WAIT_SECONDS, MetricType::DurationHistogram),
    (
        LANE_PROCESSING_DURATION_SECONDS,
        MetricType::DurationHistogram,
    ),
    (NOT_SHARD_OWNER_TOTAL, MetricType::Counter),
    (QUERY_DISPATCH_TOTAL, MetricType::Counter),
    (QUERY_BUFFER_WAIT_SECONDS, MetricType::DurationHistogram),
    (WORKFLOW_TASK_STARTED_TOTAL, MetricType::Counter),
    (WORKFLOW_TASK_COMPLETED_TOTAL, MetricType::Counter),
    (WORKFLOW_TASK_TIMED_OUT_TOTAL, MetricType::Counter),
    (ACTIVITY_TASK_STARTED_TOTAL, MetricType::Counter),
    (ACTIVITY_TASK_COMPLETED_TOTAL, MetricType::Counter),
    (ACTIVITY_TASK_FAILED_TOTAL, MetricType::Counter),
    (ACTIVITY_TASK_RETRY_TOTAL, MetricType::Counter),
    (ACTIVITY_TASK_TIMED_OUT_TOTAL, MetricType::Counter),
    (SCANNER_TICK_TOTAL, MetricType::Counter),
    (SCANNER_DISPATCHED_TOTAL, MetricType::Counter),
    (OCC_RETRY_TOTAL, MetricType::Counter),
    (KERNEL_TRANSITION_COMMITTED_TOTAL, MetricType::Counter),
    (KERNEL_EVENTS_EMITTED_TOTAL, MetricType::Counter),
    (KERNEL_COMMANDS_PROCESSED_TOTAL, MetricType::Counter),
    (WORKER_HEARTBEATS_ACCEPTED_TOTAL, MetricType::Counter),
    (WORKER_HEARTBEATS_REJECTED_TOTAL, MetricType::Counter),
    (WORKERS_OBSERVED, MetricType::Gauge),
    (WORKERS_TOTAL, MetricType::Gauge),
    (WORKER_HEARTBEAT_ACTIVE, MetricType::Gauge),
    (
        WORKER_LAST_HEARTBEAT_AGE_SECONDS,
        MetricType::DurationHistogram,
    ),
    (WORKER_COMPUTE_OBSERVATIONS_TOTAL, MetricType::Counter),
    (WORKER_COMPUTE_DECISIONS_TOTAL, MetricType::Counter),
    (WORKER_COMPUTE_SUPPRESSIONS_TOTAL, MetricType::Counter),
    (WORKER_COMPUTE_ACTIONS_TOTAL, MetricType::Counter),
    (
        WORKER_COMPUTE_ACTION_LATENCY_SECONDS,
        MetricType::DurationHistogram,
    ),
    (WORKER_COMPUTE_HEALTH, MetricType::Gauge),
    (NEXUS_OUTBOUND_REQUESTS_TOTAL, MetricType::Counter),
    (
        NEXUS_OUTBOUND_LATENCY_SECONDS,
        MetricType::DurationHistogram,
    ),
];

/// Record one bounded worker-compute observation attempt.
pub fn record_worker_compute_observation(task_type: WorkerComputeTaskType, result: ObserveResult) {
    let outcome = match result {
        ObserveResult::Accepted => "accepted",
        ObserveResult::Full => "full",
        ObserveResult::Closed => "closed",
        ObserveResult::Disabled => "disabled",
    };
    counter!(
        WORKER_COMPUTE_OBSERVATIONS_TOTAL,
        "task_type" => worker_compute_task_type_label(task_type),
        "outcome" => outcome,
    )
    .increment(1);
}

/// Record one scaler decision for each task family governed by the group.
pub fn record_worker_compute_decision(
    task_types: impl IntoIterator<Item = WorkerComputeTaskType>,
    reason: WorkerComputeInvokeReason,
) {
    for task_type in task_types {
        counter!(
            WORKER_COMPUTE_DECISIONS_TOTAL,
            "task_type" => worker_compute_task_type_label(task_type),
            "reason" => worker_compute_reason_label(reason),
        )
        .increment(1);
    }
}

/// Record one deliberate policy suppression.
pub fn record_worker_compute_suppression(
    task_type: WorkerComputeTaskType,
    suppression: ScalerSuppression,
) {
    let reason = match suppression {
        ScalerSuppression::Cooloff => "cooloff",
        ScalerSuppression::Epsilon => "epsilon",
    };
    counter!(
        WORKER_COMPUTE_SUPPRESSIONS_TOTAL,
        "task_type" => worker_compute_task_type_label(task_type),
        "reason" => reason,
    )
    .increment(1);
}

/// Record one finalized provider attempt without identity-sized labels.
pub fn record_worker_compute_action(
    target_kind: WorkerComputeProviderTargetKind,
    outcome: &'static str,
    duration: std::time::Duration,
) {
    let target_kind = match target_kind {
        WorkerComputeProviderTargetKind::Unresolved => "unresolved",
        WorkerComputeProviderTargetKind::External => "external",
        WorkerComputeProviderTargetKind::Worker => "worker",
    };
    counter!(
        WORKER_COMPUTE_ACTIONS_TOTAL,
        "target_kind" => target_kind,
        "outcome" => outcome,
    )
    .increment(1);
    histogram!(
        WORKER_COMPUTE_ACTION_LATENCY_SECONDS,
        "target_kind" => target_kind,
        "outcome" => outcome,
    )
    .record(duration.as_secs_f64());
}

/// Publish the current durable group count for every bounded health category.
pub fn record_worker_compute_health(values: impl IntoIterator<Item = WorkerComputeHealth>) {
    let mut counts = [0_u64; 11];
    for value in values {
        counts[worker_compute_health_index(value)] =
            counts[worker_compute_health_index(value)].saturating_add(1);
    }
    for (index, health) in WORKER_COMPUTE_HEALTH_LABELS.iter().enumerate() {
        gauge!(WORKER_COMPUTE_HEALTH, "health" => *health).set(counts[index] as f64);
    }
}

const WORKER_COMPUTE_HEALTH_LABELS: [&str; 11] = [
    "active",
    "disabled",
    "unsupported_provider",
    "unsupported_scaler",
    "invalid_configuration",
    "provider_request_too_large",
    "misconfigured_endpoint",
    "capacity_limited",
    "delivery_retrying",
    "delivery_terminal_failure",
    "inactive",
];

const fn worker_compute_health_index(health: WorkerComputeHealth) -> usize {
    match health {
        WorkerComputeHealth::Active => 0,
        WorkerComputeHealth::Disabled => 1,
        WorkerComputeHealth::UnsupportedProvider => 2,
        WorkerComputeHealth::UnsupportedScaler => 3,
        WorkerComputeHealth::InvalidConfiguration => 4,
        WorkerComputeHealth::ProviderRequestTooLarge => 5,
        WorkerComputeHealth::MisconfiguredEndpoint => 6,
        WorkerComputeHealth::CapacityLimited => 7,
        WorkerComputeHealth::DeliveryRetrying => 8,
        WorkerComputeHealth::DeliveryTerminalFailure => 9,
        WorkerComputeHealth::Inactive => 10,
    }
}

const fn worker_compute_task_type_label(task_type: WorkerComputeTaskType) -> &'static str {
    match task_type {
        WorkerComputeTaskType::Workflow => "workflow",
        WorkerComputeTaskType::Activity => "activity",
        WorkerComputeTaskType::Nexus => "nexus",
    }
}

const fn worker_compute_reason_label(reason: WorkerComputeInvokeReason) -> &'static str {
    match reason {
        WorkerComputeInvokeReason::ConfigurationActivation => "configuration_activation",
        WorkerComputeInvokeReason::NoSyncMatch => "no_sync_match",
        WorkerComputeInvokeReason::Backlog => "backlog",
        WorkerComputeInvokeReason::WorkerRefresh => "worker_refresh",
    }
}

fn task_type_name(kind: TaskKind) -> &'static str {
    match kind {
        TaskKind::Workflow => "workflow",
        TaskKind::Activity => "activity",
    }
}

/// Record a task publication to the broker.
pub fn record_broker_publish(queue: &QueueKey) {
    counter!(
        BROKER_PUBLISH_TOTAL,
        "namespace" => queue.namespace_id.0.to_string(),
        "task_queue" => queue.task_queue.0.clone(),
        "task_type" => task_type_name(queue.task_kind),
    )
    .increment(1);
}

/// Record one outbound Nexus request, tagged by the originator `namespace` (name), the
/// Nexus `method` (`StartOperation` / `CancelOperation`), the `failure_source` (`worker`
/// when the worker reported the failure, else `_unknown_`), and the `outcome`
/// (`startCallOutcomeTag` taxonomy: `successful` / `pending` / `operation-unsuccessful:<state>`
/// / `handler-error:<TYPE>` / `unknown-error`). Mirrors `OutboundRequestCounter`
/// (`components/nexusoperations/executors.go:330 @ v1.31.0`).
pub fn record_nexus_outbound_request(
    namespace: &str,
    method: &str,
    failure_source: &str,
    outcome: &str,
) {
    counter!(
        NEXUS_OUTBOUND_REQUESTS_TOTAL,
        "namespace" => namespace.to_string(),
        "method" => method.to_string(),
        "failure_source" => failure_source.to_string(),
        "outcome" => outcome.to_string(),
    )
    .increment(1);
}

/// Record the wall-clock latency of one outbound Nexus request (`OutboundRequestLatency`,
/// `executors.go:331 @ v1.31.0`).
pub fn record_nexus_outbound_latency(namespace: &str, method: &str, duration: std::time::Duration) {
    histogram!(
        NEXUS_OUTBOUND_LATENCY_SECONDS,
        "namespace" => namespace.to_string(),
        "method" => method.to_string(),
    )
    .record(duration.as_secs_f64());
}

/// Record a sync-match delivery where a waiter was already present.
pub fn record_sync_match(queue: &QueueKey) {
    counter!(
        BROKER_SYNC_MATCH_TOTAL,
        "namespace" => queue.namespace_id.0.to_string(),
        "task_queue" => queue.task_queue.0.clone(),
        "task_type" => task_type_name(queue.task_kind),
    )
    .increment(1);
}

/// Record a non-sync-match publication where work was queued.
pub fn record_non_sync_match(queue: &QueueKey) {
    counter!(
        BROKER_NON_SYNC_MATCH_TOTAL,
        "namespace" => queue.namespace_id.0.to_string(),
        "task_queue" => queue.task_queue.0.clone(),
        "task_type" => task_type_name(queue.task_kind),
    )
    .increment(1);
}

/// Record a long poll timing out without receiving work.
pub fn record_poll_timeout(queue: &QueueKey) {
    counter!(
        BROKER_POLL_TIMEOUT_TOTAL,
        "namespace" => queue.namespace_id.0.to_string(),
        "task_queue" => queue.task_queue.0.clone(),
        "task_type" => task_type_name(queue.task_kind),
    )
    .increment(1);
}

/// Record the current number of ready tasks for a queue/tier.
pub fn set_queue_depth(queue: &QueueKey, tier: &'static str, depth: usize) {
    gauge!(
        BROKER_QUEUE_DEPTH,
        "namespace" => queue.namespace_id.0.to_string(),
        "task_queue" => queue.task_queue.0.clone(),
        "task_type" => task_type_name(queue.task_kind),
        "tier" => tier,
    )
    .set(depth as f64);
}

/// Record how long a lane submission took end-to-end.
pub fn record_lane_submit_duration(lane_id: usize, duration: std::time::Duration) {
    histogram!(LANE_SUBMIT_DURATION_SECONDS, "lane_id" => lane_id.to_string())
        .record(duration.as_secs_f64());
}

/// Record how long a message waited in the lane channel before processing began.
pub fn record_lane_queue_wait(duration: std::time::Duration) {
    histogram!(LANE_QUEUE_WAIT_SECONDS).record(duration.as_secs_f64());
}

/// Record how long the lane spent processing a single command (load + kernel + commit).
pub fn record_lane_processing_duration(command_type: &'static str, duration: std::time::Duration) {
    histogram!(LANE_PROCESSING_DURATION_SECONDS, "command_type" => command_type)
        .record(duration.as_secs_f64());
}

/// Record a runtime ownership rejection.
pub fn record_not_shard_owner(operation: NotShardOwnerOperationLabel) {
    counter!(NOT_SHARD_OWNER_TOTAL, "operation" => operation.as_str()).increment(1);
}

/// Record a query delivery decision without exposing query/workflow identity.
pub fn record_query_dispatch(path: QueryDispatchPathLabel, outcome: QueryDispatchOutcomeLabel) {
    counter!(QUERY_DISPATCH_TOTAL, "path" => path.as_str(), "outcome" => outcome.as_str())
        .increment(1);
}

/// Record how long a query waited behind a workflow-task consistency barrier.
pub fn record_query_buffer_wait(duration: std::time::Duration, outcome: QueryBufferOutcomeLabel) {
    histogram!(QUERY_BUFFER_WAIT_SECONDS, "outcome" => outcome.as_str())
        .record(duration.as_secs_f64());
}

pub fn record_workflow_task_started(outcome: OutcomeLabel) {
    counter!(WORKFLOW_TASK_STARTED_TOTAL, "outcome" => outcome.as_str()).increment(1);
}

pub fn record_workflow_task_completed(outcome: OutcomeLabel) {
    counter!(WORKFLOW_TASK_COMPLETED_TOTAL, "outcome" => outcome.as_str()).increment(1);
}

pub fn record_workflow_task_timed_out(outcome: OutcomeLabel) {
    counter!(WORKFLOW_TASK_TIMED_OUT_TOTAL, "outcome" => outcome.as_str()).increment(1);
}

/// v1.31.0's `metrics.TaskTypeTimerActiveTaskSpeculativeWorkflowTaskTimeout`
/// operation tag (metric_defs.go:597 @ v1.31.0), the value the corpus filters
/// the timer-task metrics on.
const SPECULATIVE_TIMER_OPERATION: &str = "TimerActiveTaskSpeculativeWorkflowTaskTimeout";

/// Record a speculative workflow task's commit-vs-rollback outcome at
/// completion (spec speculative-wft M.1): a materialized task commits, a
/// dropped (discarded) task rolls back. Count-only — v1.31.0's reason tags are
/// not asserted by the corpus (F4). `namespace` is the namespace NAME, which
/// the fork's metric bridge filters on.
pub fn record_speculative_workflow_task_outcome(namespace: &str, committed: bool) {
    if committed {
        counter!(SPECULATIVE_WFT_COMMITS_TOTAL, "namespace" => namespace.to_owned()).increment(1);
    } else {
        counter!(SPECULATIVE_WFT_ROLLBACKS_TOTAL, "namespace" => namespace.to_owned()).increment(1);
    }
}

/// Record that an in-memory speculative-WFT timer fired and applied its timeout
/// (spec speculative-wft M.1). Every firing counts a timer-task request; a
/// start-to-close firing also counts the start-to-close timeout. Both carry the
/// namespace label and the `TimerActiveTaskSpeculativeWorkflowTaskTimeout`
/// operation tag the corpus filters on.
pub fn record_speculative_timer_fired(namespace: &str, start_to_close: bool) {
    counter!(
        SPECULATIVE_TIMER_TASK_REQUESTS_TOTAL,
        "namespace" => namespace.to_owned(),
        "operation" => SPECULATIVE_TIMER_OPERATION,
    )
    .increment(1);
    if start_to_close {
        counter!(
            SPECULATIVE_START_TO_CLOSE_TIMEOUT_TOTAL,
            "namespace" => namespace.to_owned(),
            "operation" => SPECULATIVE_TIMER_OPERATION,
        )
        .increment(1);
    }
}

pub fn record_activity_task_started(outcome: OutcomeLabel) {
    counter!(ACTIVITY_TASK_STARTED_TOTAL, "outcome" => outcome.as_str()).increment(1);
}

pub fn record_activity_task_completed(outcome: OutcomeLabel) {
    counter!(ACTIVITY_TASK_COMPLETED_TOTAL, "outcome" => outcome.as_str()).increment(1);
}

pub fn record_activity_task_failed(outcome: OutcomeLabel) {
    counter!(ACTIVITY_TASK_FAILED_TOTAL, "outcome" => outcome.as_str()).increment(1);
}

pub fn record_activity_task_retry(outcome: OutcomeLabel) {
    counter!(ACTIVITY_TASK_RETRY_TOTAL, "outcome" => outcome.as_str()).increment(1);
}

pub fn record_activity_task_timed_out(outcome: OutcomeLabel) {
    counter!(ACTIVITY_TASK_TIMED_OUT_TOTAL, "outcome" => outcome.as_str()).increment(1);
}

/// Record a scanner tick for one shard.
pub fn record_scanner_tick(scanner_type: &'static str, shard_id: u32) {
    counter!(
        SCANNER_TICK_TOTAL,
        "scanner_type" => scanner_type,
        "shard_id" => shard_id.to_string(),
    )
    .increment(1);
}

/// Record that a scanner found work to dispatch.
pub fn record_scanner_dispatched(scanner_type: &'static str, shard_id: u32) {
    counter!(
        SCANNER_DISPATCHED_TOTAL,
        "scanner_type" => scanner_type,
        "shard_id" => shard_id.to_string(),
    )
    .increment(1);
}

/// Record an OCC retry outcome.
pub fn record_occ_retry(outcome: RetryOutcomeLabel) {
    counter!(OCC_RETRY_TOTAL, "outcome" => outcome.as_str()).increment(1);
}

/// Record a committed kernel transition observed by the runtime.
pub fn record_transition_committed(namespace: &str, command_type: &'static str) {
    counter!(
        KERNEL_TRANSITION_COMMITTED_TOTAL,
        "namespace" => namespace.to_owned(),
        "command_type" => command_type,
    )
    .increment(1);
}

/// Record emitted history events by event type.
pub fn record_events_emitted(event_type: &'static str, count: usize) {
    counter!(KERNEL_EVENTS_EMITTED_TOTAL, "event_type" => event_type).increment(count as u64);
}

/// Record processed commands by command type.
pub fn record_commands_processed(command_type: &'static str) {
    counter!(KERNEL_COMMANDS_PROCESSED_TOTAL, "command_type" => command_type).increment(1);
}

pub fn record_worker_heartbeat_accepted(namespace: NamespaceId, key: &WorkerInstanceKey) {
    counter!(
        WORKER_HEARTBEATS_ACCEPTED_TOTAL,
        "namespace" => namespace.0.to_string(),
        "worker_instance_key" => key.0.clone(),
    )
    .increment(1);
}

pub fn record_worker_heartbeat_rejected(namespace: &str, reason: &'static str) {
    counter!(
        WORKER_HEARTBEATS_REJECTED_TOTAL,
        "namespace" => namespace.to_string(),
        "reason" => reason,
    )
    .increment(1);
}

pub fn record_worker_heartbeat_active(
    namespace: NamespaceId,
    key: &WorkerInstanceKey,
    active: bool,
) {
    gauge!(
        WORKER_HEARTBEAT_ACTIVE,
        "namespace" => namespace.0.to_string(),
        "worker_instance_key" => key.0.clone(),
    )
    .set(if active { 1.0 } else { 0.0 });
}

pub fn record_worker_last_heartbeat_age(namespace: NamespaceId, age: time::Duration) {
    histogram!(
        WORKER_LAST_HEARTBEAT_AGE_SECONDS,
        "namespace" => namespace.0.to_string(),
    )
    .record(age.as_seconds_f64().max(0.0));
}

pub fn set_workers_observed(namespace: NamespaceId, count: usize) {
    gauge!(WORKERS_OBSERVED, "namespace" => namespace.0.to_string()).set(count as f64);
}

pub fn set_workers_total(count: usize) {
    gauge!(WORKERS_TOTAL).set(count as f64);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use proptest::prelude::*;
    use tokeira_storage::WorkerComputeControllerHealthView;
    use tokeira_types::{
        BuildId, ConfigurationFingerprint, ControllerInstanceKey, DeploymentId, NamespaceId,
        ScalingGroupId, TaskQueueName, WorkerComputeFailureCategory,
    };
    use uuid::Uuid;

    fn snapshot_map(
        recorder: &DebuggingRecorder,
    ) -> HashMap<String, (HashMap<String, String>, DebugValue)> {
        recorder
            .snapshotter()
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, _, _, value)| {
                let labels = key
                    .key()
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect::<HashMap<_, _>>();
                (key.key().name().to_string(), (labels, value))
            })
            .collect()
    }

    #[test]
    fn manifest_uses_valid_metric_names() {
        for (name, kind) in METRIC_NAMES {
            validate_metric_name(name, *kind).unwrap();
        }
    }

    #[test]
    fn nexus_outbound_helpers_emit_expected_metrics_and_labels() {
        let recorder = DebuggingRecorder::new();

        with_local_recorder(&recorder, || {
            record_nexus_outbound_request(
                "default",
                "StartOperation",
                "_unknown_",
                "handler-error:BAD_REQUEST",
            );
            record_nexus_outbound_latency(
                "default",
                "StartOperation",
                std::time::Duration::from_millis(4),
            );
        });

        let snapshot = snapshot_map(&recorder);

        let (labels, value) = snapshot.get(NEXUS_OUTBOUND_REQUESTS_TOTAL).unwrap();
        assert_eq!(labels.get("namespace"), Some(&"default".to_string()));
        assert_eq!(labels.get("method"), Some(&"StartOperation".to_string()));
        assert_eq!(labels.get("failure_source"), Some(&"_unknown_".to_string()));
        assert_eq!(
            labels.get("outcome"),
            Some(&"handler-error:BAD_REQUEST".to_string())
        );
        assert!(!labels.contains_key("workflow_id"));
        assert!(!labels.contains_key("run_id"));
        assert_eq!(value, &DebugValue::Counter(1));

        assert!(snapshot.contains_key(NEXUS_OUTBOUND_LATENCY_SECONDS));
    }

    #[test]
    fn worker_compute_helpers_emit_only_bounded_labels() {
        let recorder = DebuggingRecorder::new();
        with_local_recorder(&recorder, || {
            record_worker_compute_observation(
                WorkerComputeTaskType::Workflow,
                ObserveResult::Accepted,
            );
            record_worker_compute_decision(
                [WorkerComputeTaskType::Activity],
                WorkerComputeInvokeReason::Backlog,
            );
            record_worker_compute_suppression(
                WorkerComputeTaskType::Nexus,
                ScalerSuppression::Epsilon,
            );
            record_worker_compute_action(
                WorkerComputeProviderTargetKind::External,
                "retrying",
                std::time::Duration::from_millis(5),
            );
            record_worker_compute_health([WorkerComputeHealth::DeliveryRetrying]);
        });
        let snapshot = snapshot_map(&recorder);
        for (metric, allowed) in WORKER_COMPUTE_METRIC_LABEL_KEYS {
            let (labels, _) = snapshot.get(*metric).expect("worker-compute metric");
            assert!(labels.keys().all(|label| allowed.contains(&label.as_str())));
        }
    }

    #[test]
    fn helpers_emit_expected_metrics_and_labels() {
        let recorder = DebuggingRecorder::new();
        let queue = QueueKey {
            namespace_id: NamespaceId(uuid::Uuid::nil()),
            task_queue: TaskQueueName("queue-a".into()),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };

        with_local_recorder(&recorder, || {
            record_broker_publish(&queue);
            record_sync_match(&queue);
            record_non_sync_match(&queue);
            record_poll_timeout(&queue);
            set_queue_depth(&queue, "ready", 7);
            record_lane_submit_duration(3, std::time::Duration::from_millis(25));
            record_lane_processing_duration(
                "WorkflowTaskCompleted",
                std::time::Duration::from_millis(19),
            );
            record_not_shard_owner(NotShardOwnerOperationLabel::Submit);
            record_query_dispatch(
                QueryDispatchPathLabel::Direct,
                QueryDispatchOutcomeLabel::Published,
            );
            record_query_buffer_wait(
                std::time::Duration::from_millis(13),
                QueryBufferOutcomeLabel::Released,
            );
            record_workflow_task_started(OutcomeLabel::Success);
            record_workflow_task_completed(OutcomeLabel::Failure);
            record_workflow_task_timed_out(OutcomeLabel::Success);
            record_activity_task_started(OutcomeLabel::Success);
            record_activity_task_completed(OutcomeLabel::Success);
            record_activity_task_failed(OutcomeLabel::Failure);
            record_activity_task_retry(OutcomeLabel::Success);
            record_activity_task_timed_out(OutcomeLabel::Failure);
            record_scanner_tick("timer", 4);
            record_scanner_dispatched("timer", 4);
            record_occ_retry(RetryOutcomeLabel::Retry);
            record_transition_committed("default", "Start");
            record_events_emitted("WorkflowExecutionStarted", 2);
            record_commands_processed("Start");
        });

        let snapshot = snapshot_map(&recorder);

        let (labels, value) = snapshot.get(BROKER_PUBLISH_TOTAL).unwrap();
        assert_eq!(
            labels.get("namespace"),
            Some(&uuid::Uuid::nil().to_string())
        );
        assert_eq!(labels.get("task_queue"), Some(&"queue-a".to_string()));
        assert_eq!(labels.get("task_type"), Some(&"workflow".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        assert_eq!(
            snapshot.get(BROKER_SYNC_MATCH_TOTAL).unwrap().1,
            DebugValue::Counter(1)
        );
        assert_eq!(
            snapshot.get(BROKER_NON_SYNC_MATCH_TOTAL).unwrap().1,
            DebugValue::Counter(1)
        );
        assert_eq!(
            snapshot.get(BROKER_POLL_TIMEOUT_TOTAL).unwrap().1,
            DebugValue::Counter(1)
        );
        assert_eq!(
            snapshot.get(BROKER_QUEUE_DEPTH).unwrap().1,
            DebugValue::Gauge(7.0.into())
        );

        let (labels, value) = snapshot.get(LANE_SUBMIT_DURATION_SECONDS).unwrap();
        assert_eq!(labels.get("lane_id"), Some(&"3".to_string()));
        match value {
            DebugValue::Histogram(values) => {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].into_inner(), 0.025f64);
            }
            other => panic!("expected histogram, got {other:?}"),
        }

        let (labels, value) = snapshot.get(LANE_PROCESSING_DURATION_SECONDS).unwrap();
        assert_eq!(
            labels.get("command_type"),
            Some(&"WorkflowTaskCompleted".to_string())
        );
        match value {
            DebugValue::Histogram(values) => {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].into_inner(), 0.019f64);
            }
            other => panic!("expected histogram, got {other:?}"),
        }

        let (labels, value) = snapshot.get(NOT_SHARD_OWNER_TOTAL).unwrap();
        assert_eq!(labels.get("operation"), Some(&"submit".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));
        assert!(!labels.contains_key("workflow_id"));
        assert!(!labels.contains_key("run_id"));

        let (labels, value) = snapshot.get(QUERY_DISPATCH_TOTAL).unwrap();
        assert_eq!(labels.get("path"), Some(&"direct".to_string()));
        assert_eq!(labels.get("outcome"), Some(&"published".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));
        assert!(!labels.contains_key("query_id"));
        assert!(!labels.contains_key("workflow_id"));
        assert!(!labels.contains_key("run_id"));

        let (labels, value) = snapshot.get(QUERY_BUFFER_WAIT_SECONDS).unwrap();
        assert_eq!(labels.get("outcome"), Some(&"released".to_string()));
        match value {
            DebugValue::Histogram(values) => {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].into_inner(), 0.013f64);
            }
            other => panic!("expected histogram, got {other:?}"),
        }

        let (labels, value) = snapshot.get(WORKFLOW_TASK_STARTED_TOTAL).unwrap();
        assert_eq!(labels.get("outcome"), Some(&"success".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(WORKFLOW_TASK_COMPLETED_TOTAL).unwrap();
        assert_eq!(labels.get("outcome"), Some(&"failure".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(WORKFLOW_TASK_TIMED_OUT_TOTAL).unwrap();
        assert_eq!(labels.get("outcome"), Some(&"success".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(ACTIVITY_TASK_STARTED_TOTAL).unwrap();
        assert_eq!(labels.get("outcome"), Some(&"success".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(ACTIVITY_TASK_COMPLETED_TOTAL).unwrap();
        assert_eq!(labels.get("outcome"), Some(&"success".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(ACTIVITY_TASK_FAILED_TOTAL).unwrap();
        assert_eq!(labels.get("outcome"), Some(&"failure".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(ACTIVITY_TASK_RETRY_TOTAL).unwrap();
        assert_eq!(labels.get("outcome"), Some(&"success".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(ACTIVITY_TASK_TIMED_OUT_TOTAL).unwrap();
        assert_eq!(labels.get("outcome"), Some(&"failure".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(SCANNER_TICK_TOTAL).unwrap();
        assert_eq!(labels.get("scanner_type"), Some(&"timer".to_string()));
        assert_eq!(labels.get("shard_id"), Some(&"4".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(SCANNER_DISPATCHED_TOTAL).unwrap();
        assert_eq!(labels.get("scanner_type"), Some(&"timer".to_string()));
        assert_eq!(labels.get("shard_id"), Some(&"4".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(OCC_RETRY_TOTAL).unwrap();
        assert_eq!(labels.get("outcome"), Some(&"retry".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(KERNEL_TRANSITION_COMMITTED_TOTAL).unwrap();
        assert_eq!(labels.get("namespace"), Some(&"default".to_string()));
        assert_eq!(labels.get("command_type"), Some(&"Start".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));

        let (labels, value) = snapshot.get(KERNEL_EVENTS_EMITTED_TOTAL).unwrap();
        assert_eq!(
            labels.get("event_type"),
            Some(&"WorkflowExecutionStarted".to_string())
        );
        assert_eq!(value, &DebugValue::Counter(2));

        let (labels, value) = snapshot.get(KERNEL_COMMANDS_PROCESSED_TOTAL).unwrap();
        assert_eq!(labels.get("command_type"), Some(&"Start".to_string()));
        assert_eq!(value, &DebugValue::Counter(1));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: worker-compute-controller, Property 16: diagnostics and telemetry remain bounded and truthful
        #[test]
        fn property_worker_compute_diagnostics_and_telemetry_are_bounded(
            seed in any::<u128>(),
            health_index in 0_usize..11,
            failure_index in proptest::option::of(0_usize..11),
        ) {
            let health = [
                WorkerComputeHealth::Active,
                WorkerComputeHealth::Disabled,
                WorkerComputeHealth::UnsupportedProvider,
                WorkerComputeHealth::UnsupportedScaler,
                WorkerComputeHealth::InvalidConfiguration,
                WorkerComputeHealth::ProviderRequestTooLarge,
                WorkerComputeHealth::MisconfiguredEndpoint,
                WorkerComputeHealth::CapacityLimited,
                WorkerComputeHealth::DeliveryRetrying,
                WorkerComputeHealth::DeliveryTerminalFailure,
                WorkerComputeHealth::Inactive,
            ][health_index];
            let failures = [
                WorkerComputeFailureCategory::NamespaceUnresolved,
                WorkerComputeFailureCategory::EndpointNotFound,
                WorkerComputeFailureCategory::Transport,
                WorkerComputeFailureCategory::RetryableHandler,
                WorkerComputeFailureCategory::NonRetryableHandler,
                WorkerComputeFailureCategory::OperationUnsuccessful,
                WorkerComputeFailureCategory::AsyncResponse,
                WorkerComputeFailureCategory::RequestTooLarge,
                WorkerComputeFailureCategory::InvalidResponsePayload,
                WorkerComputeFailureCategory::ResponseIdMismatch,
                WorkerComputeFailureCategory::Storage,
            ];
            let failure = failure_index.map(|index| failures[index]);
            let namespace_id = NamespaceId(Uuid::from_u128(seed));
            let view = WorkerComputeControllerHealthView {
                namespace_name: format!("namespace-{seed}"),
                controller_key: ControllerInstanceKey {
                    namespace_id,
                    deployment_name: DeploymentId(format!("deployment-{seed}")),
                    build_id: BuildId(format!("build-{seed}")),
                },
                scaling_group: ScalingGroupId(format!("group-{seed}")),
                fingerprint: ConfigurationFingerprint::from_canonical_bytes(
                    &seed.to_be_bytes(),
                ),
                health,
                last_action_id: Some(Uuid::from_u128(seed.rotate_left(1))),
                last_failure_category: failure,
                next_metrics_poll_at: None,
            };
            let first = serde_json::to_string(&view).expect("diagnostic JSON");
            let second = serde_json::to_string(&view).expect("diagnostic JSON");
            prop_assert_eq!(&first, &second);
            prop_assert_eq!(view.health, health);
            prop_assert_eq!(view.last_failure_category, failure);
            prop_assert!(!first.contains("provider_details"));
            prop_assert!(!first.contains("credential"));

            let forbidden = [
                "task_queue",
                "action_id",
                "deployment",
                "build_id",
                "scaling_group",
            ];
            for (_, labels) in WORKER_COMPUTE_METRIC_LABEL_KEYS {
                prop_assert!(labels.iter().all(|label| !forbidden.contains(label)));
            }
        }
    }
}
