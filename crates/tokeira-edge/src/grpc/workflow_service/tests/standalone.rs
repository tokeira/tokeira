//! Fixed-clock standalone wire replay, recorded before extension executors land.

use super::*;
use crate::{
    chasm_activity::{ActivityBridge, ActivityDispatchQueue, StartActivity},
    chasm_executors::ActivityDispatchExecutor,
};
use proptest::prelude::*;
use std::{
    fmt::Write as _,
    sync::atomic::{AtomicI64, Ordering},
};
use tokeira_chasm::{BusinessIdPolicy, ExecutionKey, Library, Registry};
use tokeira_chasm_activity::{ActivityConfig, ActivityExecution, ActivityLibrary, ActivityState};
use tokeira_runtime::chasm::{
    ChasmEngine, ChasmTimerSweeper, CollectingVisibilitySink, DispatchMultiplexer, Engine,
};
use tokeira_storage::{InMemoryChasmNodeStore, WorkerTaskProvenanceStore};

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
        Self::with_config(ActivityConfig {
            enable_standalone: true,
            ..Default::default()
        })
    }

    fn with_config(config: ActivityConfig) -> Self {
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
        let mux = Arc::new(DispatchMultiplexer::default());
        let clock = Arc::new(AtomicI64::new(1_000 * SEC));
        let read_clock = clock.clone();
        let engine = Arc::new(
            ChasmEngine::new(
                Arc::new(InMemoryChasmNodeStore::new()),
                Arc::new(registry.build()),
                mux.clone(),
                Arc::new(CollectingVisibilitySink::default()),
            )
            .with_clock(Arc::new(move || read_clock.load(Ordering::SeqCst))),
        );
        let executor = Arc::new(ActivityDispatchExecutor::new(
            Arc::downgrade(&engine),
            queue,
        ));
        mux.register(executor.clone()).unwrap();
        let bridge = Arc::new(
            ActivityBridge::new(engine.clone(), config, 1000).with_dispatch_executor(executor),
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
            callbacks: Vec::new(),
            version_target: None,
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
            .poll_activity_task(QUEUE, "golden-worker", None)
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

fn wire_start(
    callbacks: Vec<tokeira_proto::common::Callback>,
    input: Vec<u8>,
) -> workflowservice::StartActivityExecutionRequest {
    workflowservice::StartActivityExecutionRequest {
        namespace: "default".into(),
        activity_id: "generated".into(),
        request_id: "generated-request".into(),
        activity_type: Some(tokeira_proto::common::ActivityType {
            name: "Generated".into(),
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
        input: Some(payloads(&input)),
        ..Default::default()
    }
}

async fn wire_description(
    harness: &Harness,
    run: &str,
) -> workflowservice::DescribeActivityExecutionResponse {
    harness
        .grpc
        .describe_activity_execution(Request::new(
            workflowservice::DescribeActivityExecutionRequest {
                namespace: "default".into(),
                activity_id: "generated".into(),
                run_id: run.into(),
                include_input: true,
                include_outcome: true,
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner()
}

fn generated_callbacks(
    specs: &[(String, String)],
    internal: bool,
) -> Vec<tokeira_proto::common::Callback> {
    let mut callbacks: Vec<_> = specs
        .iter()
        .enumerate()
        .map(|(index, (path, value))| tokeira_proto::common::Callback {
            variant: Some(tokeira_proto::common::callback::Variant::Nexus(
                tokeira_proto::common::callback::Nexus {
                    url: format!("https://callback.example/{path}/{index}"),
                    header: [("MiXeD-Case".into(), value.clone())].into_iter().collect(),
                },
            )),
            links: vec![tokeira_proto::common::Link {
                variant: Some(tokeira_proto::common::link::Variant::BatchJob(
                    tokeira_proto::common::link::BatchJob {
                        job_id: path.clone(),
                    },
                )),
            }],
        })
        .collect();
    if internal {
        callbacks.push(tokeira_proto::common::Callback {
            variant: Some(tokeira_proto::common::callback::Variant::Internal(
                tokeira_proto::common::callback::Internal { data: vec![1, 2] },
            )),
            ..Default::default()
        });
    }
    callbacks
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: chasm-extension-archetypes, Property 17: gate-off invariance
    // Fresh wire starts ignore callback bytes in persisted state and every describe field.
    #[test]
    fn gate_off_generated_starts_ignore_callbacks(specs in prop::collection::vec(("[a-z]{1,12}", "[A-Za-z0-9]{0,15}"), 0..6), internal in any::<bool>(), input in prop::collection::vec(any::<u8>(), 0..24)) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let callbacks = generated_callbacks(&specs, internal);
            let mut states = Vec::new(); let mut descriptions = Vec::new();
            for callbacks in [Vec::new(), callbacks] {
                let h = Harness::new();
                let started = h.grpc.start_activity_execution(Request::new(wire_start(callbacks, input.clone()))).await.unwrap().into_inner();
                prop_assert!(started.started);
                let key = ExecutionKey::new(&h.namespace, "generated", &started.run_id);
                states.push(h.engine.read_component(&key).await.unwrap().data.unwrap());
                let description = wire_description(&h, &started.run_id).await;
                let mut trace = Trace::default(); trace.record_normalized("describe/generated", &description, &started.run_id); descriptions.push(trace.0);
            }
            prop_assert_eq!(&states[0], &states[1]); prop_assert_eq!(&descriptions[0], &descriptions[1]); Ok(())
        })?;
    }

    // Feature: chasm-extension-archetypes, Property 9: callback attachment model
    // Admission, atomic cap rejection and attach-order projection match the model.
    #[test]
    fn callback_attachment_matches_wire_model(specs in prop::collection::vec(("[a-z]{1,12}", "[A-Za-z0-9]{0,15}"), 0..6), internal in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let h = Harness::with_config(ActivityConfig { enable_standalone: true, enable_callbacks: true, max_callbacks_per_execution: 2, ..Default::default() });
            let callbacks = generated_callbacks(&specs, internal);
            let result = h.grpc.start_activity_execution(Request::new(wire_start(callbacks.clone(), vec![1]))).await;
            if internal || specs.len() > 2 {
                let error = result.unwrap_err();
                if internal { prop_assert_eq!(error.code(), tonic::Code::InvalidArgument); prop_assert_eq!(error.message(), "unsupported callback variant: *common.Callback_Internal_"); }
                else { prop_assert_eq!(error.code(), tonic::Code::FailedPrecondition); prop_assert_eq!(error.message(), "cannot attach more than 2 callbacks to an activity (0 callbacks already attached)"); }
                prop_assert!(h.bridge.current_run(&h.namespace, "generated").await.unwrap().is_none());
            } else {
                let started = result.unwrap().into_inner();
                let data = h.engine.read_component(&ExecutionKey::new(&h.namespace, "generated", &started.run_id)).await.unwrap().data.unwrap();
                let state = ActivityState::decode(data.as_slice()).unwrap();
                prop_assert!(state.version_target.is_none(), "public starts never target a release");
                prop_assert_eq!(state.callbacks.len(), specs.len());
                let description = wire_description(&h, &started.run_id).await;
                prop_assert_eq!(description.callbacks.len(), specs.len());
                for (index, callback) in state.callbacks.iter().enumerate() {
                    prop_assert_eq!(&callback.id, &format!("generated-request-{index}"));
                    prop_assert_eq!(callback.state(), tokeira_proto::enums::CallbackState::Standby);
                    let info = description.callbacks[index].info.as_ref().unwrap();
                    prop_assert_eq!(info.callback.as_ref(), Some(&callbacks[index]));
                    prop_assert_eq!(info.attempt, 0); prop_assert!(info.last_attempt_complete_time.is_none() && info.next_attempt_schedule_time.is_none() && info.last_attempt_failure.is_none());
                    prop_assert!(matches!(description.callbacks[index].trigger.as_ref().unwrap().variant, Some(activity_v1::callback_info::trigger::Variant::ActivityClosed(_))));
                }
            }
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn atomic_wire_start_reports_one_transition_and_advances_on_updates() {
    // Both pinned handlers schedule inside StartExecution
    // (`chasm/lib/activity/handler.go @ v1.31.0, v1.32.0`).
    for callbacks in [false, true] {
        let h = Harness::with_config(ActivityConfig {
            enable_standalone: true,
            enable_callbacks: callbacks,
            ..Default::default()
        });
        let request = wire_start(
            generated_callbacks(&[("path".into(), "value".into())], false),
            vec![],
        );
        let started = h
            .grpc
            .start_activity_execution(Request::new(request.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            wire_description(&h, &started.run_id)
                .await
                .info
                .unwrap()
                .state_transition_count,
            1
        );
        let repeated = h
            .grpc
            .start_activity_execution(Request::new(request))
            .await
            .unwrap()
            .into_inner();
        assert!(!repeated.started);
        assert_eq!(repeated.run_id, started.run_id);
        assert_eq!(
            wire_description(&h, &started.run_id)
                .await
                .info
                .unwrap()
                .state_transition_count,
            1
        );
        let token = h.poll(&mut Trace::default(), "poll/count").await;
        assert_eq!(
            wire_description(&h, &started.run_id)
                .await
                .info
                .unwrap()
                .state_transition_count,
            2
        );
        token_action(&h.grpc, token, 0).await.unwrap();
        assert_eq!(
            wire_description(&h, &started.run_id)
                .await
                .info
                .unwrap()
                .state_transition_count,
            3
        );
    }
}

#[tokio::test]
async fn unscoped_poll_fields_do_not_admit_targeted_work_and_internal_callbacks_are_hidden() {
    let h = Harness::with_config(ActivityConfig {
        enable_standalone: true,
        enable_callbacks: true,
        ..Default::default()
    });
    let mut start = h.start_request("generated", "00000000-0000-4000-8000-000000000014");
    start.version_target = Some(DeploymentVersionTarget {
        deployment_name: "d".into(),
        build_id: "b".into(),
    });
    start.callbacks = vec![tokeira_chasm_activity::CallbackSpec {
        target: tokeira_chasm_activity::CallbackTarget::Internal {
            component_ref: vec![1],
            task_type_id: 99,
            task_id: vec![2],
        },
        links: vec![],
    }];
    let run = h
        .bridge
        .start(start)
        .await
        .unwrap()
        .reference
        .execution_key
        .run_id;
    assert!(wire_description(&h, &run).await.callbacks.is_empty());
    let poll = h
        .grpc
        .poll_activity_task_queue(Request::new(
            workflowservice::PollActivityTaskQueueRequest {
                namespace: "default".into(),
                task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
                    name: QUEUE.into(),
                    ..Default::default()
                }),
                deployment_options: Some(WorkerDeploymentOptions {
                    worker_versioning_mode: WorkerVersioningMode::Versioned as i32,
                    deployment_name: "d".into(),
                    build_id: "b".into(),
                    ..Default::default()
                }),
                worker_instance_key: "instance".into(),
                ..Default::default()
            },
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(poll.task_token.is_empty());
    assert_eq!(
        wire_description(&h, &run)
            .await
            .info
            .unwrap()
            .state_transition_count,
        1
    );
}

fn scoped_grpc(
    harness: &Harness,
    version: &DeploymentVersionTarget,
    provenance: Arc<tokeira_storage::InMemoryStore>,
) -> WorkflowServiceGrpc {
    let cache = Arc::new(StaticNamespaceCache);
    let scope = WorkerScope::try_new(
        "default".into(),
        vec![QUEUE.into()],
        version.deployment_name.clone(),
        version.build_id.clone(),
    )
    .unwrap();
    let authenticator = Arc::new(PolicyAuthenticator::new(
        Arc::new(FixedClaimsMapper(Claims {
            subject: "scoped-worker".into(),
            auth_type: "jwt".into(),
            worker_scope: Some(scope),
            ..Default::default()
        })),
        Arc::new(DefaultAuthorizer),
        false,
    ));
    let service = WorkflowService::new_with_buffered_queries_and_history_wait_registry(
        Arc::new(PollNoneRuntime),
        Arc::new(NoopResolver),
        Arc::new(EmptyVisibilityApi),
        Arc::new(tokeira_storage::InMemoryStore::default()),
        Arc::new(InMemoryOperatorApi::new("tokeira-local", "0.1.0+test0001")),
        cache.clone(),
        Arc::new(EdgeInterceptors::configured(cache, authenticator, false)),
        PollerRegistry::default(),
        crate::PendingQueryStore::default(),
        tokeira_runtime::BufferedQueryRegistry::default(),
        tokeira_runtime::InMemoryBroker::default(),
        LongPollGate::new(LongPollConfig::default()),
        Arc::new(LocalOnlyRouter),
        HistoryWaitRegistry::default(),
    )
    .with_worker_task_provenance(provenance);
    WorkflowServiceGrpc::new(service).with_chasm_activity(harness.bridge.clone())
}

async fn token_action(
    service: &WorkflowServiceGrpc,
    token: Vec<u8>,
    action: u8,
) -> Result<(), Status> {
    match action {
        0 => service
            .respond_activity_task_completed(Request::new(
                workflowservice::RespondActivityTaskCompletedRequest {
                    namespace: "default".into(),
                    task_token: token,
                    result: Some(payloads(b"done")),
                    ..Default::default()
                },
            ))
            .await
            .map(|_| ()),
        1 => service
            .respond_activity_task_failed(Request::new(
                workflowservice::RespondActivityTaskFailedRequest {
                    namespace: "default".into(),
                    task_token: token,
                    failure: Some(tokeira_proto::failure::Failure {
                        message: "failed".into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ))
            .await
            .map(|_| ()),
        2 => service
            .respond_activity_task_canceled(Request::new(
                workflowservice::RespondActivityTaskCanceledRequest {
                    namespace: "default".into(),
                    task_token: token,
                    ..Default::default()
                },
            ))
            .await
            .map(|_| ()),
        _ => service
            .record_activity_task_heartbeat(Request::new(
                workflowservice::RecordActivityTaskHeartbeatRequest {
                    namespace: "default".into(),
                    task_token: token,
                    details: Some(payloads(b"beat")),
                    ..Default::default()
                },
            ))
            .await
            .map(|_| ()),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Feature: chasm-extension-archetypes, Property 12: versioned admission
    // A real scoped pickup authors provenance; another release cannot mutate the run.
    #[test]
    fn scoped_wire_tokens_reject_other_releases(version in 0u8..4, action in 0u8..4) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let h = Harness::new();
            h.clock.store(OffsetDateTime::now_utc().unix_timestamp_nanos() as i64, Ordering::SeqCst);
            let target = DeploymentVersionTarget { deployment_name: format!("deployment-{}", version / 2), build_id: format!("build-{version}") };
            let wrong = DeploymentVersionTarget { build_id: format!("build-{}", (version + 1) % 4), ..target.clone() };
            let provenance = Arc::new(tokeira_storage::InMemoryStore::default());
            let own = scoped_grpc(&h, &target, provenance.clone()); let other = scoped_grpc(&h, &wrong, provenance.clone());
            let run = "00000000-0000-4000-8000-000000000012";
            let mut start = h.start_request("scoped", run); start.version_target = Some(target.clone());
            h.bridge.start(start).await.unwrap();
            let poll = own.poll_activity_task_queue(Request::new(workflowservice::PollActivityTaskQueueRequest {
                namespace: "default".into(), task_queue: Some(tokeira_proto::taskqueue::TaskQueue { name: QUEUE.into(), ..Default::default() }),
                identity: "worker".into(), worker_instance_key: "instance".into(),
                deployment_options: Some(WorkerDeploymentOptions { worker_versioning_mode: WorkerVersioningMode::Versioned as i32, deployment_name: target.deployment_name, build_id: target.build_id, ..Default::default() }),
                ..Default::default()
            })).await.unwrap().into_inner();
            prop_assert!(!poll.task_token.is_empty());
            let digest = tokeira_storage::worker_task_token_digest(&poll.task_token);
            prop_assert!(provenance.get(digest).await.unwrap().is_some());
            let key = ExecutionKey::new(&h.namespace, "scoped", run);
            if action == 2 { h.bridge.request_cancel(key.clone(), "client".into(), "cancel".into(), "stop".into()).await.unwrap(); }
            let before = h.engine.read_component(&key).await.unwrap();
            let error = token_action(&other, poll.task_token.clone(), action).await.unwrap_err();
            prop_assert_eq!(error.code(), tonic::Code::PermissionDenied);
            let after = h.engine.read_component(&key).await.unwrap();
            prop_assert_eq!(before.data, after.data); prop_assert_eq!(before.execution_vt, after.execution_vt);
            token_action(&own, poll.task_token, action).await.unwrap();
            prop_assert_eq!(provenance.get(digest).await.unwrap().is_some(), action == 3);
            Ok(())
        })?;
    }
}
