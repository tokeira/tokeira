//! Fixed-clock standalone wire replay, recorded before extension executors land.

use super::*;
use crate::chasm_activity::{ActivityBridge, ActivityDispatchQueue, StartActivity};
use std::{
    fmt::Write as _,
    sync::atomic::{AtomicI64, Ordering},
};
use tokeira_chasm::{BusinessIdPolicy, Library, Registry};
use tokeira_chasm_activity::{ActivityConfig, ActivityExecution, ActivityLibrary};
use tokeira_runtime::chasm::{ChasmEngine, ChasmTimerSweeper, CollectingVisibilitySink};
use tokeira_storage::InMemoryChasmNodeStore;

const SEC: i64 = 1_000_000_000;
const QUEUE: &str = "standalone-golden";

struct Harness {
    grpc: WorkflowServiceGrpc,
    bridge: Arc<ActivityBridge>,
    engine: Arc<ChasmEngine>,
    clock: Arc<AtomicI64>,
    namespace: String,
}

impl Harness {
    fn new() -> Self {
        let cache = Arc::new(StaticNamespaceCache);
        let service = WorkflowService::new_with_buffered_queries_and_history_wait_registry(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(tokeira_storage::InMemoryStore::default()),
            Arc::new(InMemoryOperatorApi::new("tokeira-local", "0.1.0+test0001")),
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            crate::PendingQueryStore::default(),
            tokeira_runtime::BufferedQueryRegistry::default(),
            tokeira_runtime::InMemoryBroker::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
            HistoryWaitRegistry::default(),
        );
        let mut registry = Registry::builder();
        ActivityLibrary::register(&mut registry).unwrap();
        let queue = Arc::new(ActivityDispatchQueue::new());
        let clock = Arc::new(AtomicI64::new(1_000 * SEC));
        let read_clock = clock.clone();
        let engine = Arc::new(
            ChasmEngine::new(
                Arc::new(InMemoryChasmNodeStore::new()),
                Arc::new(registry.build()),
                queue.clone(),
                Arc::new(CollectingVisibilitySink::default()),
            )
            .with_clock(Arc::new(move || read_clock.load(Ordering::SeqCst))),
        );
        let bridge = Arc::new(
            ActivityBridge::new(
                engine.clone(),
                ActivityConfig {
                    enable_standalone: true,
                    ..ActivityConfig::default()
                },
                1000,
            )
            .with_dispatch_queue(queue),
        );
        Self {
            grpc: WorkflowServiceGrpc::new(service).with_chasm_activity(bridge.clone()),
            bridge,
            engine,
            clock,
            namespace: crate::translate::to_internal::namespace_id_for("default")
                .0
                .to_string(),
        }
    }

    fn advance(&self, nanos: i64) {
        self.clock.fetch_add(nanos, Ordering::SeqCst);
    }

    fn start_request(&self, id: &str, run: &str) -> StartActivity {
        StartActivity {
            namespace_id: self.namespace.clone(),
            activity_id: id.into(),
            run_id: run.into(),
            activity_type: "GoldenActivity".into(),
            task_queue: QUEUE.into(),
            input: payloads(b"input").encode_to_vec(),
            schedule_to_start_nanos: 5 * SEC,
            schedule_to_close_nanos: 60 * SEC,
            start_to_close_nanos: 10 * SEC,
            heartbeat_nanos: 4 * SEC,
            run_timeout_nanos: 0,
            request_id: Some(format!("start-{id}")),
            policy: BusinessIdPolicy::default(),
            header: Vec::new(),
            retry_policy: defaulted_retry_policy(None).encode_to_vec(),
            retry_initial_interval_nanos: SEC,
            retry_backoff_coefficient: 2.0,
            retry_maximum_interval_nanos: 100 * SEC,
            maximum_attempts: 2,
            priority: Vec::new(),
            search_attributes: Vec::new(),
            user_metadata: Vec::new(),
        }
    }

    async fn start(&self, id: &str, run: &str, trace: &mut Trace) {
        let result = self
            .bridge
            .start(self.start_request(id, run))
            .await
            .unwrap();
        trace.record(
            &format!("start/{id}"),
            &workflowservice::StartActivityExecutionResponse {
                run_id: result.reference.execution_key.run_id,
                started: result.started,
                link: None,
            },
        );
        self.describe(id, run, trace, "scheduled").await;
    }

    async fn describe(&self, id: &str, run: &str, trace: &mut Trace, label: &str) {
        let response = self
            .grpc
            .describe_activity_execution(Request::new(
                workflowservice::DescribeActivityExecutionRequest {
                    namespace: "default".into(),
                    activity_id: id.into(),
                    run_id: run.into(),
                    include_input: true,
                    include_outcome: true,
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner();
        trace.record(&format!("describe/{id}/{label}"), &response);
    }

    async fn poll(&self, trace: &mut Trace, label: &str) -> Vec<u8> {
        let response = self
            .grpc
            .poll_activity_task_queue(Request::new(
                workflowservice::PollActivityTaskQueueRequest {
                    namespace: "default".into(),
                    task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                        name: QUEUE.into(),
                        ..Default::default()
                    }),
                    identity: "golden-worker".into(),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.task_token.is_empty());
        trace.record(label, &response);
        response.task_token
    }
}

fn payloads(bytes: &[u8]) -> tokeira_proto::common::Payloads {
    tokeira_proto::common::Payloads {
        payloads: vec![tokeira_proto::common::Payload {
            data: bytes.into(),
            ..Default::default()
        }],
    }
}

#[derive(Default)]
struct Trace(String);
impl Trace {
    fn record<M: prost::Message>(&mut self, label: &str, response: &M) {
        self.record_bytes(label, &response.encode_to_vec());
    }

    fn record_normalized<M: prost::Message>(&mut self, label: &str, response: &M, run: &str) {
        const PLACEHOLDER: &[u8] = b"00000000-0000-4000-8000-00000000ffff";
        assert_eq!(run.len(), PLACEHOLDER.len());
        uuid::Uuid::parse_str(run).unwrap();
        let mut bytes = response.encode_to_vec();
        // Equal-length substitution preserves every protobuf and embedded-token
        // length prefix. Only this response's actual minted UUID is eligible.
        let mut offset = 0;
        let mut replacements = 0;
        while offset + run.len() <= bytes.len() {
            if &bytes[offset..offset + run.len()] == run.as_bytes() {
                bytes[offset..offset + run.len()].copy_from_slice(PLACEHOLDER);
                replacements += 1;
                offset += run.len();
            } else {
                offset += 1;
            }
        }
        assert!(replacements > 0, "missing generated run id in {label}");
        self.record_bytes(label, &bytes);
    }

    fn record_bytes(&mut self, label: &str, bytes: &[u8]) {
        // Atomic start corrects the public transition count from two to one.
        // Normalize that field and the opaque token's VT, retaining its execution
        // identity. Separate tests assert count 1 and live long-poll wake behavior.
        let normalized;
        let bytes = if label.starts_with("describe/") {
            let mut description =
                workflowservice::DescribeActivityExecutionResponse::decode(bytes).unwrap();
            description.info.as_mut().unwrap().state_transition_count = 0;
            let reference =
                tokeira_chasm::ComponentRef::decode(&description.long_poll_token).unwrap();
            description.long_poll_token = serde_json::to_vec(&reference.execution_key).unwrap();
            normalized = description.encode_to_vec();
            normalized.as_slice()
        } else {
            bytes
        };
        write!(self.0, "{label}: ").unwrap();
        for byte in bytes {
            write!(self.0, "{byte:02x}").unwrap();
        }
        self.0.push('\n');
    }
    fn error(&mut self, label: &str, error: Status) {
        writeln!(self.0, "{label}: {:?} {}", error.code(), error.message()).unwrap();
    }
}

// Feature: chasm-extension-archetypes, Property 17: gate-off invariance
// Fixed bridge ids keep the lifecycle exact; only fresh wire starts normalize the
// server-minted run UUID, including its occurrence inside the describe token.
#[tokio::test]
async fn standalone_activity_v1_31_0_golden() {
    let h = Harness::new();
    let mut trace = Trace::default();
    let completed = "00000000-0000-4000-8000-000000000001";
    h.start("complete", completed, &mut trace).await;
    for callbacks in [
        Vec::new(),
        vec![
            tokeira_proto::common::Callback {
                variant: Some(tokeira_proto::common::callback::Variant::Nexus(
                    tokeira_proto::common::callback::Nexus {
                        url: "not a URL".into(),
                        header: Default::default(),
                    },
                )),
                ..Default::default()
            },
            tokeira_proto::common::Callback {
                variant: Some(tokeira_proto::common::callback::Variant::Internal(
                    tokeira_proto::common::callback::Internal { data: vec![1, 2] },
                )),
                ..Default::default()
            },
        ],
    ] {
        let label = if callbacks.is_empty() {
            "wire-start/plain"
        } else {
            "wire-start/ignored-callbacks"
        };
        let response = h
            .grpc
            .start_activity_execution(Request::new(
                workflowservice::StartActivityExecutionRequest {
                    namespace: "default".into(),
                    activity_id: "complete".into(),
                    request_id: "start-complete".into(),
                    activity_type: Some(tokeira_proto::common::ActivityType {
                        name: "GoldenActivity".into(),
                    }),
                    task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                        name: QUEUE.into(),
                        ..Default::default()
                    }),
                    start_to_close_timeout: Some(prost_types::Duration {
                        seconds: 10,
                        nanos: 0,
                    }),
                    completion_callbacks: callbacks,
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner();
        trace.record(label, &response);
        h.describe("complete", completed, &mut trace, label).await;
    }
    let token = h.poll(&mut trace, "poll/attempt-1").await;
    h.describe("complete", completed, &mut trace, "started")
        .await;
    h.advance(SEC);
    trace.record(
        "heartbeat",
        &h.grpc
            .record_activity_task_heartbeat(Request::new(
                workflowservice::RecordActivityTaskHeartbeatRequest {
                    namespace: "default".into(),
                    task_token: token.clone(),
                    details: Some(payloads(b"heartbeat")),
                    identity: "golden-worker".into(),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner(),
    );
    h.describe("complete", completed, &mut trace, "heartbeat")
        .await;
    trace.record(
        "fail/retry",
        &h.grpc
            .respond_activity_task_failed(Request::new(
                workflowservice::RespondActivityTaskFailedRequest {
                    namespace: "default".into(),
                    task_token: token,
                    identity: "golden-worker".into(),
                    failure: Some(tokeira_proto::failure::Failure {
                        message: "retry me".into(),
                        failure_info: Some(
                            tokeira_proto::failure::failure::FailureInfo::ApplicationFailureInfo(
                                tokeira_proto::failure::ApplicationFailureInfo::default(),
                            ),
                        ),
                        ..Default::default()
                    }),
                    last_heartbeat_details: Some(payloads(b"heartbeat")),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner(),
    );
    h.describe("complete", completed, &mut trace, "backoff")
        .await;
    assert!(
        h.bridge
            .poll_activity_task(QUEUE, "golden-worker")
            .await
            .unwrap()
            .is_none()
    );
    trace.record(
        "poll/before-retry",
        &workflowservice::PollActivityTaskQueueResponse::default(),
    );
    h.advance(SEC);
    let token = h.poll(&mut trace, "poll/attempt-2").await;
    h.describe("complete", completed, &mut trace, "retry-started")
        .await;
    trace.record(
        "complete",
        &h.grpc
            .respond_activity_task_completed(Request::new(
                workflowservice::RespondActivityTaskCompletedRequest {
                    namespace: "default".into(),
                    task_token: token,
                    identity: "golden-worker".into(),
                    result: Some(payloads(b"result")),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner(),
    );
    h.describe("complete", completed, &mut trace, "completed")
        .await;

    let canceled = "00000000-0000-4000-8000-000000000002";
    h.start("cancel", canceled, &mut trace).await;
    let token = h.poll(&mut trace, "poll/cancel").await;
    trace.record(
        "cancel/request",
        &h.grpc
            .request_cancel_activity_execution(Request::new(
                workflowservice::RequestCancelActivityExecutionRequest {
                    namespace: "default".into(),
                    activity_id: "cancel".into(),
                    run_id: canceled.into(),
                    request_id: "cancel-1".into(),
                    identity: "client".into(),
                    reason: "stop".into(),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner(),
    );
    h.describe("cancel", canceled, &mut trace, "requested")
        .await;
    trace.record(
        "cancel/heartbeat",
        &h.grpc
            .record_activity_task_heartbeat(Request::new(
                workflowservice::RecordActivityTaskHeartbeatRequest {
                    namespace: "default".into(),
                    task_token: token.clone(),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner(),
    );
    trace.record(
        "cancel/ack",
        &h.grpc
            .respond_activity_task_canceled(Request::new(
                workflowservice::RespondActivityTaskCanceledRequest {
                    namespace: "default".into(),
                    task_token: token,
                    details: Some(payloads(b"canceled")),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner(),
    );
    h.describe("cancel", canceled, &mut trace, "canceled").await;

    let terminated = "00000000-0000-4000-8000-000000000003";
    h.start("terminate", terminated, &mut trace).await;
    trace.record(
        "terminate",
        &h.grpc
            .terminate_activity_execution(Request::new(
                workflowservice::TerminateActivityExecutionRequest {
                    namespace: "default".into(),
                    activity_id: "terminate".into(),
                    run_id: terminated.into(),
                    request_id: "terminate-1".into(),
                    reason: "terminated".into(),
                    identity: "client".into(),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner(),
    );
    h.describe("terminate", terminated, &mut trace, "terminated")
        .await;

    let timed_out = "00000000-0000-4000-8000-000000000004";
    h.start("timeout", timed_out, &mut trace).await;
    h.advance(6 * SEC);
    let sweeper = ChasmTimerSweeper::new(h.engine.clone()).with_evaluator(
        tokeira_chasm::archetype_id_for_fqn(<ActivityExecution as tokeira_chasm::Component>::FQN),
        h.bridge.clone(),
    );
    assert!(sweeper.sweep_once().await > 0);
    h.describe("timeout", timed_out, &mut trace, "timed-out")
        .await;
    for (id, run) in [
        ("complete", completed),
        ("cancel", canceled),
        ("terminate", terminated),
        ("timeout", timed_out),
    ] {
        trace.record(
            &format!("outcome/{id}"),
            &h.grpc
                .poll_activity_execution(Request::new(
                    workflowservice::PollActivityExecutionRequest {
                        namespace: "default".into(),
                        activity_id: id.into(),
                        run_id: run.into(),
                        ..Default::default()
                    },
                ))
                .await
                .unwrap()
                .into_inner(),
        );
    }
    trace.record(
        "delete",
        &h.grpc
            .delete_activity_execution(Request::new(
                workflowservice::DeleteActivityExecutionRequest {
                    namespace: "default".into(),
                    activity_id: "complete".into(),
                    run_id: completed.into(),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner(),
    );
    trace.error(
        "describe/deleted",
        h.grpc
            .describe_activity_execution(Request::new(
                workflowservice::DescribeActivityExecutionRequest {
                    namespace: "default".into(),
                    activity_id: "complete".into(),
                    run_id: completed.into(),
                    ..Default::default()
                },
            ))
            .await
            .unwrap_err(),
    );

    for (label, callbacks) in [
        ("fresh/plain", Vec::new()),
        (
            "fresh/ignored-callbacks",
            vec![tokeira_proto::common::Callback {
                variant: Some(tokeira_proto::common::callback::Variant::Internal(
                    tokeira_proto::common::callback::Internal { data: vec![1, 2] },
                )),
                ..Default::default()
            }],
        ),
    ] {
        let fresh = Harness::new();
        let response = fresh
            .grpc
            .start_activity_execution(Request::new(
                workflowservice::StartActivityExecutionRequest {
                    namespace: "default".into(),
                    activity_id: "fresh".into(),
                    request_id: "fresh-request".into(),
                    activity_type: Some(tokeira_proto::common::ActivityType {
                        name: "GoldenActivity".into(),
                    }),
                    task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                        name: QUEUE.into(),
                        ..Default::default()
                    }),
                    start_to_close_timeout: Some(prost_types::Duration {
                        seconds: 10,
                        nanos: 0,
                    }),
                    completion_callbacks: callbacks,
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(response.started);
        trace.record_normalized(label, &response, &response.run_id);
        let description = fresh
            .grpc
            .describe_activity_execution(Request::new(
                workflowservice::DescribeActivityExecutionRequest {
                    namespace: "default".into(),
                    activity_id: "fresh".into(),
                    run_id: response.run_id.clone(),
                    include_input: true,
                    include_outcome: true,
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .into_inner();
        trace.record_normalized(&format!("describe/{label}"), &description, &response.run_id);
    }

    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/standalone_activity_v1_31_0.golden"
    );
    if std::env::var_os("UPDATE_CHASM_GOLDEN").is_some() {
        std::fs::write(path, &trace.0).unwrap();
    } else {
        assert_eq!(
            trace.0,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/standalone_activity_v1_31_0.golden"
            ))
        );
    }
}
