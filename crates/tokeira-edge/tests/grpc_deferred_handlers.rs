//! Campaign stubs must reject every request uniformly before consulting runtime state.

use std::sync::Arc;

use proptest::prelude::*;
use tokeira_edge::{
    EdgeInterceptors, EmptyVisibilityApi, InMemoryNamespaceCache, InMemoryOperatorApi,
    LocalOnlyRouter, LongPollConfig, LongPollGate, PendingQueryStore, PollerRegistry,
    WorkflowService,
    grpc::{runtime_adapter::RuntimeAdapter, workflow_service::WorkflowServiceGrpc},
    workflow_service::InMemoryExecutionResolver,
};
use tokeira_proto::{
    common,
    workflowservice::{self, workflow_service_server::WorkflowService as _},
};
use tokeira_runtime::{
    BacklogConfig, LaneConfig, TimerScannerConfig, TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::InMemoryStore;
use tonic::{Code, Request, Status};

fn service() -> WorkflowServiceGrpc {
    let store = Arc::new(InMemoryStore::default());
    let runtime = Arc::new(TokeiraRuntime::new(
        store.clone(),
        1,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    ));
    let broker = runtime.broker();
    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    let interceptors = Arc::new(EdgeInterceptors::permissive(namespaces.clone()));
    WorkflowServiceGrpc::new(WorkflowService::new(
        Arc::new(RuntimeAdapter::new(runtime)),
        Arc::new(InMemoryExecutionResolver::new()),
        Arc::new(EmptyVisibilityApi),
        store,
        Arc::new(InMemoryOperatorApi::new("local", "test")),
        namespaces,
        interceptors,
        PollerRegistry::default(),
        PendingQueryStore::default(),
        broker,
        LongPollGate::new(LongPollConfig::default()),
        Arc::new(LocalOnlyRouter),
    ))
}

fn assert_deferred(status: Status, method: &str, owner: &str) {
    assert_eq!(status.code(), Code::Unimplemented);
    assert_eq!(
        status.message(),
        format!("{method} is not implemented; tracked in spec {owner}")
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: temporal-v1.32-compatibility, Property 6: deferred stubs answer uniformly
    #[test]
    fn deferred_stubs_answer_uniformly(
        text in prop::array::uniform8(any::<String>()),
        flags in prop::array::uniform6(any::<bool>()),
        seconds in any::<i64>(), nanos in any::<i32>(),
        paths in prop::collection::vec(any::<String>(), 0..8),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()
            .expect("test runtime");
        runtime.block_on(async {
            let grpc = service();
            let duration = flags[0].then_some(prost_types::Duration { seconds, nanos });
            let status = grpc.count_workers(Request::new(workflowservice::CountWorkersRequest {
                namespace: text[0].clone(), query: text[1].clone(), include_system_workers: flags[0],
            })).await.expect_err("CountWorkers must remain deferred");
            assert_deferred(status, "count_workers", "v132-batch-operations-and-workers");
            let status = grpc.pause_activity_execution(Request::new(workflowservice::PauseActivityExecutionRequest {
                namespace: text[0].clone(), workflow_id: text[1].clone(), activity_id: text[2].clone(),
                run_id: text[3].clone(), identity: text[4].clone(), reason: text[5].clone(),
                resource_id: text[6].clone(), request_id: text[7].clone(),
            })).await.expect_err("PauseActivityExecution must remain deferred");
            assert_deferred(status, "pause_activity_execution", "v132-standalone-activities");
            let status = grpc.unpause_activity_execution(Request::new(workflowservice::UnpauseActivityExecutionRequest {
                namespace: text[0].clone(), workflow_id: text[1].clone(), activity_id: text[2].clone(),
                run_id: text[3].clone(), identity: text[4].clone(), reason: text[5].clone(),
                resource_id: text[6].clone(), request_id: text[7].clone(), jitter: duration,
            })).await.expect_err("UnpauseActivityExecution must remain deferred");
            assert_deferred(status, "unpause_activity_execution", "v132-standalone-activities");
            let status = grpc.reset_activity_execution(Request::new(workflowservice::ResetActivityExecutionRequest {
                namespace: text[0].clone(), workflow_id: text[1].clone(), activity_id: text[2].clone(),
                run_id: text[3].clone(), identity: text[4].clone(), resource_id: text[6].clone(),
                request_id: text[7].clone(), jitter: duration, keep_paused: flags[1],
                restore_original_options: flags[2], reset_heartbeat: flags[3],
            })).await.expect_err("ResetActivityExecution must remain deferred");
            assert_deferred(status, "reset_activity_execution", "v132-standalone-activities");
            let status = grpc.update_activity_execution_options(Request::new(workflowservice::UpdateActivityExecutionOptionsRequest {
                namespace: text[0].clone(), workflow_id: text[1].clone(), activity_id: text[2].clone(),
                run_id: text[3].clone(), identity: text[4].clone(), resource_id: text[6].clone(),
                request_id: text[7].clone(), restore_original: flags[1],
                activity_options: flags[4].then(|| tokeira_proto::public::temporal::api::activity::v1::ActivityOptions {
                    start_delay: duration,
                    task_queue: Some(tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                        name: text[5].clone(), kind: nanos, normal_name: text[6].clone(),
                    }),
                    schedule_to_close_timeout: duration, schedule_to_start_timeout: duration,
                    start_to_close_timeout: duration, heartbeat_timeout: duration,
                    retry_policy: None, priority: None,
                }),
                update_mask: flags[5].then_some(prost_types::FieldMask { paths }),
            })).await.expect_err("UpdateActivityExecutionOptions must remain deferred");
            assert_deferred(status, "update_activity_execution_options", "v132-standalone-activities");
            let status = grpc.poll_workflow_execution_time_skipping(Request::new(workflowservice::PollWorkflowExecutionTimeSkippingRequest {
                namespace: text[0].clone(), fast_forward_id: text[7].clone(),
                workflow_execution: flags[0].then(|| common::WorkflowExecution {
                    workflow_id: text[1].clone(), run_id: text[3].clone(),
                }),
            })).await.expect_err("PollWorkflowExecutionTimeSkipping must remain deferred");
            assert_deferred(status, "poll_workflow_execution_time_skipping", "v132-gated-surfaces");
        });
    }
}

#[test]
fn deferred_macro_logs_at_debug_only() {
    let source = include_str!("../src/grpc/workflow_service.rs");
    let body = source
        .split("macro_rules! deferred_unary {")
        .nth(1)
        .expect("deferred macro")
        .split("#[tonic::async_trait]")
        .next()
        .expect("macro boundary");
    assert!(body.contains("debug!("));
    for level in ["trace!(", "info!(", "warn!(", "error!("] {
        assert!(!body.contains(level), "deferred macro logs with {level}");
    }
}
