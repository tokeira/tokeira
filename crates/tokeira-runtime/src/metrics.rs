//! Runtime metric definitions and recording helpers.

use metrics::{counter, gauge, histogram};
#[cfg(test)]
use tokeira_types::validate_metric_name;
use tokeira_types::{MetricType, QueueKey, TaskKind};

pub const BROKER_PUBLISH_TOTAL: &str = "tokeira_runtime_broker_publish_total";
pub const BROKER_SYNC_MATCH_TOTAL: &str = "tokeira_runtime_broker_sync_match_total";
pub const BROKER_NON_SYNC_MATCH_TOTAL: &str = "tokeira_runtime_broker_non_sync_match_total";
pub const BROKER_POLL_TIMEOUT_TOTAL: &str = "tokeira_runtime_broker_poll_timeout_total";
pub const BROKER_QUEUE_DEPTH: &str = "tokeira_runtime_broker_queue_depth";
pub const LANE_SUBMIT_DURATION_SECONDS: &str = "tokeira_runtime_lane_submit_duration_seconds";
pub const LANE_QUEUE_WAIT_SECONDS: &str = "tokeira_runtime_lane_queue_wait_seconds";
pub const LANE_PROCESSING_DURATION_SECONDS: &str =
    "tokeira_runtime_lane_processing_duration_seconds";
pub const SCANNER_TICK_TOTAL: &str = "tokeira_runtime_scanner_tick_total";
pub const SCANNER_DISPATCHED_TOTAL: &str = "tokeira_runtime_scanner_dispatched_total";
pub const OCC_RETRY_TOTAL: &str = "tokeira_runtime_occ_retry_total";
pub const KERNEL_TRANSITION_COMMITTED_TOTAL: &str = "tokeira_kernel_transition_committed_total";
pub const KERNEL_EVENTS_EMITTED_TOTAL: &str = "tokeira_kernel_events_emitted_total";
pub const KERNEL_COMMANDS_PROCESSED_TOTAL: &str = "tokeira_kernel_commands_processed_total";

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
    (SCANNER_TICK_TOTAL, MetricType::Counter),
    (SCANNER_DISPATCHED_TOTAL, MetricType::Counter),
    (OCC_RETRY_TOTAL, MetricType::Counter),
    (KERNEL_TRANSITION_COMMITTED_TOTAL, MetricType::Counter),
    (KERNEL_EVENTS_EMITTED_TOTAL, MetricType::Counter),
    (KERNEL_COMMANDS_PROCESSED_TOTAL, MetricType::Counter),
];

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
pub fn record_occ_retry(outcome: &'static str) {
    counter!(OCC_RETRY_TOTAL, "outcome" => outcome).increment(1);
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tokeira_types::{NamespaceId, TaskQueueName};

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
            record_scanner_tick("timer", 4);
            record_scanner_dispatched("timer", 4);
            record_occ_retry("retry");
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
}
