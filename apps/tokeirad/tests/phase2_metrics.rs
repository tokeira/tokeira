use std::collections::HashMap;

use metrics::with_local_recorder;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use proptest::prelude::*;
use tokeira_edge::{
    GRPC_ACTIVE_REQUESTS, GRPC_REQUEST_DURATION_SECONDS, GRPC_REQUEST_TOTAL, record_grpc_request,
    record_grpc_request_duration, set_grpc_active_requests,
};
use tokeira_projection::{
    LAG_RECORDS, RECORDS_PROCESSED_TOTAL, SINK_WRITE_DURATION_SECONDS, record_records_processed,
    record_sink_write_duration, set_projection_lag,
};
use tokeira_runtime::{
    BROKER_PUBLISH_TOTAL, BROKER_QUEUE_DEPTH, LANE_SUBMIT_DURATION_SECONDS, record_broker_publish,
    record_lane_submit_duration, set_queue_depth,
};
use tokeira_types::{NamespaceId, QueueKey, TaskKind, TaskQueueName};

#[derive(Clone, Debug)]
enum MetricOp {
    RuntimePublish,
    RuntimeQueueDepth(u16),
    RuntimeLaneSubmit(u16),
    EdgeRequest,
    EdgeActive(u16),
    EdgeDuration(u16),
    ProjectionProcessed(u8),
    ProjectionLag(u16),
    ProjectionSinkDuration(u16),
}

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

fn fixed_queue() -> QueueKey {
    QueueKey {
        namespace_id: NamespaceId::new(),
        task_queue: TaskQueueName("metrics-q".into()),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}

fn op_strategy() -> impl Strategy<Value = MetricOp> {
    prop_oneof![
        Just(MetricOp::RuntimePublish),
        (0u16..1000).prop_map(MetricOp::RuntimeQueueDepth),
        (1u16..1000).prop_map(MetricOp::RuntimeLaneSubmit),
        Just(MetricOp::EdgeRequest),
        (0u16..1000).prop_map(MetricOp::EdgeActive),
        (1u16..1000).prop_map(MetricOp::EdgeDuration),
        (1u8..20).prop_map(MetricOp::ProjectionProcessed),
        (0u16..1000).prop_map(MetricOp::ProjectionLag),
        (1u16..1000).prop_map(MetricOp::ProjectionSinkDuration),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn property_metric_accounting_accuracy(
        ops in prop::collection::vec(op_strategy(), 1..60)
    ) {
        let recorder = DebuggingRecorder::new();
        let queue = fixed_queue();

        let mut runtime_publish_total = 0u64;
        let mut runtime_queue_depth = None::<f64>;
        let mut runtime_lane_submit_count = 0usize;
        let mut edge_request_total = 0u64;
        let mut edge_active = None::<f64>;
        let mut edge_duration_count = 0usize;
        let mut projection_processed_total = 0u64;
        let mut projection_lag = None::<f64>;
        let mut projection_sink_write_count = 0usize;

        with_local_recorder(&recorder, || {
            for op in &ops {
                match *op {
                    MetricOp::RuntimePublish => {
                        record_broker_publish(&queue);
                        runtime_publish_total += 1;
                    }
                    MetricOp::RuntimeQueueDepth(depth) => {
                        set_queue_depth(&queue, "ready", depth as usize);
                        runtime_queue_depth = Some(depth as f64);
                    }
                    MetricOp::RuntimeLaneSubmit(ms) => {
                        record_lane_submit_duration(
                            1,
                            std::time::Duration::from_millis(ms as u64),
                        );
                        runtime_lane_submit_count += 1;
                    }
                    MetricOp::EdgeRequest => {
                        record_grpc_request(
                            "PollWorkflowTaskQueue",
                            "default",
                            "ok",
                        );
                        edge_request_total += 1;
                    }
                    MetricOp::EdgeActive(value) => {
                        set_grpc_active_requests(
                            "PollWorkflowTaskQueue",
                            value as f64,
                        );
                        edge_active = Some(value as f64);
                    }
                    MetricOp::EdgeDuration(ms) => {
                        record_grpc_request_duration(
                            "PollWorkflowTaskQueue",
                            "default",
                            std::time::Duration::from_millis(ms as u64),
                        );
                        edge_duration_count += 1;
                    }
                    MetricOp::ProjectionProcessed(count) => {
                        record_records_processed(7, count as usize);
                        projection_processed_total += count as u64;
                    }
                    MetricOp::ProjectionLag(value) => {
                        set_projection_lag(7, value as usize);
                        projection_lag = Some(value as f64);
                    }
                    MetricOp::ProjectionSinkDuration(ms) => {
                        record_sink_write_duration(
                            7,
                            std::time::Duration::from_millis(ms as u64),
                        );
                        projection_sink_write_count += 1;
                    }
                }
            }
        });

        let snapshot = snapshot_map(&recorder);

        if runtime_publish_total > 0 {
            assert_eq!(
                snapshot.get(BROKER_PUBLISH_TOTAL).unwrap().1,
                DebugValue::Counter(runtime_publish_total)
            );
        }
        if let Some(expected) = runtime_queue_depth {
            assert_eq!(
                snapshot.get(BROKER_QUEUE_DEPTH).unwrap().1,
                DebugValue::Gauge(expected.into())
            );
        }
        if runtime_lane_submit_count > 0 {
            match &snapshot.get(LANE_SUBMIT_DURATION_SECONDS).unwrap().1 {
                DebugValue::Histogram(values) => {
                    assert_eq!(values.len(), runtime_lane_submit_count);
                }
                other => panic!("expected histogram, got {other:?}"),
            }
        }

        if edge_request_total > 0 {
            assert_eq!(
                snapshot.get(GRPC_REQUEST_TOTAL).unwrap().1,
                DebugValue::Counter(edge_request_total)
            );
        }
        if let Some(expected) = edge_active {
            assert_eq!(
                snapshot.get(GRPC_ACTIVE_REQUESTS).unwrap().1,
                DebugValue::Gauge(expected.into())
            );
        }
        if edge_duration_count > 0 {
            match &snapshot.get(GRPC_REQUEST_DURATION_SECONDS).unwrap().1 {
                DebugValue::Histogram(values) => {
                    assert_eq!(values.len(), edge_duration_count);
                }
                other => panic!("expected histogram, got {other:?}"),
            }
        }

        if projection_processed_total > 0 {
            assert_eq!(
                snapshot.get(RECORDS_PROCESSED_TOTAL).unwrap().1,
                DebugValue::Counter(projection_processed_total)
            );
        }
        if let Some(expected) = projection_lag {
            assert_eq!(
                snapshot.get(LAG_RECORDS).unwrap().1,
                DebugValue::Gauge(expected.into())
            );
        }
        if projection_sink_write_count > 0 {
            match &snapshot.get(SINK_WRITE_DURATION_SECONDS).unwrap().1 {
                DebugValue::Histogram(values) => {
                    assert_eq!(values.len(), projection_sink_write_count);
                }
                other => panic!("expected histogram, got {other:?}"),
            }
        }
    }
}
