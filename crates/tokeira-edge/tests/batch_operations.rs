use std::{collections::VecDeque, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use http::HeaderMap;
use tokeira_edge::{
    BatchDispatchContext, EdgeContext, EdgeInterceptors, EmptyVisibilityApi,
    InMemoryExecutionResolver, InMemoryNamespaceCache, InMemoryOperatorApi,
    ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse, LocalOnlyRouter, LongPollConfig,
    LongPollGate, NamespaceCache, PendingQueryStore, PollerRegistry, Principal, RequestId,
    ResolvedNamespace, VisibilityApi, WorkflowExecutionSummary, WorkflowMutationOutcome,
    WorkflowRuntimeApi, WorkflowService, batch_engine,
    translate::{
        batch::{
            DescribeBatchOperationRequest, ListBatchOperationsRequest, StartBatchOperationRequest,
            StopBatchOperationRequest,
        },
        to_internal::namespace_id_for,
    },
};
use tokeira_kernel::{
    CancelRequest, NexusResolution, ResetRequest, SignalRequest, StartRequest, TerminateRequest,
    WorkflowIdConflictPolicy, WorkflowIdReusePolicy, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    BacklogConfig, BatchOperationEntry, BatchOperationParams, BatchOperationState,
    BatchOperationStore, BatchOperationType, BatchProgressCounters, BatchResetTarget,
    BufferedQueryRegistry, InMemoryBroker, JobId, LaneConfig, PendingUpdateTransport, QueryResult,
    ResetWorkflowResult, ScheduleStore, SignalWithStartResult, StartWorkflowResult,
    TimerScannerConfig, TokeiraRuntime, UpdateOutcome, UpdateTransportResolution, UpdateWaitPolicy,
    VersioningRuleStore, WorkerRegistry, WorkflowExecutionRef, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::InMemoryStore;
use tokeira_types::{
    ActivityTaskToken, BuildId, ExecutionRef, ExecutionStatus, Memo, Payload, Payloads, QueueKey,
    RequestContext, RequestId as DomainRequestId, RunId, RunKey, SearchAttributes, TaskKind,
    TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};
use tokio::sync::{Mutex, Notify};

#[derive(Clone, Debug, PartialEq)]
enum RecordedCall {
    Terminate {
        run_key: RunKey,
        identity: String,
    },
    Cancel {
        run_key: RunKey,
    },
    Signal {
        run_key: RunKey,
        signal_name: String,
    },
    Reset {
        execution: ExecutionRef,
        fork_event_id: i64,
        reason: String,
    },
}

#[derive(Default)]
struct RecordingRuntime {
    calls: Mutex<Vec<RecordedCall>>,
    block_mutations: bool,
    started: Notify,
    release: Notify,
}

impl RecordingRuntime {
    fn blocking() -> Self {
        Self {
            block_mutations: true,
            ..Self::default()
        }
    }

    async fn calls(&self) -> Vec<RecordedCall> {
        self.calls.lock().await.clone()
    }

    async fn wait_started(&self) {
        self.started.notified().await;
    }

    fn release(&self) {
        self.release.notify_waiters();
    }

    async fn record(&self, call: RecordedCall) {
        self.calls.lock().await.push(call);
        self.started.notify_waiters();
        if self.block_mutations {
            self.release.notified().await;
        }
    }
}

#[async_trait]
impl WorkflowRuntimeApi for RecordingRuntime {
    async fn start_workflow(&self, _req: StartRequest) -> Result<WorkflowMutationOutcome> {
        unreachable!()
    }

    async fn start_workflow_with_policy(&self, _req: StartRequest) -> Result<StartWorkflowResult> {
        unreachable!()
    }

    async fn signal_with_start_workflow(
        &self,
        _req: tokeira_kernel::SignalWithStartRequest,
    ) -> Result<SignalWithStartResult> {
        unreachable!()
    }

    async fn signal_workflow(
        &self,
        run_key: RunKey,
        req: SignalRequest,
    ) -> Result<WorkflowMutationOutcome> {
        self.record(RecordedCall::Signal {
            run_key,
            signal_name: req.signal_name,
        })
        .await;
        Ok(outcome())
    }

    async fn poll_workflow_task(
        &self,
        _queue: QueueKey,
        _worker_identity: WorkerIdentity,
        _timeout: Duration,
    ) -> Result<Option<tokeira_runtime::StartedWorkflowTask>> {
        unreachable!()
    }

    async fn complete_workflow_task(
        &self,
        _req: WorkflowTaskCompletedRequest,
    ) -> Result<WorkflowMutationOutcome> {
        unreachable!()
    }

    async fn poll_activity_task(
        &self,
        _queue: QueueKey,
        _worker_identity: WorkerIdentity,
        _timeout: Duration,
    ) -> Result<Option<tokeira_runtime::StartedActivityTask>> {
        unreachable!()
    }

    async fn complete_activity_task(
        &self,
        _token: ActivityTaskToken,
        _result: Payloads,
    ) -> Result<WorkflowMutationOutcome> {
        unreachable!()
    }

    async fn fail_activity_task(
        &self,
        _token: ActivityTaskToken,
        _failure: Payload,
        _failure_error_type: Option<String>,
        _is_non_retryable: bool,
    ) -> Result<()> {
        unreachable!()
    }

    async fn record_activity_heartbeat(&self, _token: ActivityTaskToken) -> Result<bool> {
        unreachable!()
    }

    async fn terminate_workflow(
        &self,
        run_key: RunKey,
        req: TerminateRequest,
    ) -> Result<WorkflowMutationOutcome> {
        self.record(RecordedCall::Terminate {
            run_key,
            identity: req.identity,
        })
        .await;
        Ok(outcome())
    }

    async fn cancel_workflow(
        &self,
        run_key: RunKey,
        _req: CancelRequest,
    ) -> Result<WorkflowMutationOutcome> {
        self.record(RecordedCall::Cancel { run_key }).await;
        Ok(outcome())
    }

    async fn reset_workflow(
        &self,
        execution: ExecutionRef,
        req: ResetRequest,
    ) -> Result<ResetWorkflowResult> {
        self.record(RecordedCall::Reset {
            execution,
            fork_event_id: req.fork_event_id,
            reason: req.reason,
        })
        .await;
        Ok(ResetWorkflowResult {
            successor_run_key: RunKey::new(),
            successor_run_id: RunId::new(),
        })
    }

    async fn query_workflow(
        &self,
        _execution: ExecutionRef,
        _query_type: String,
        _query_args: Payloads,
        _timeout: Duration,
    ) -> Result<QueryResult> {
        unreachable!()
    }

    async fn update_workflow(
        &self,
        _execution: ExecutionRef,
        _update_id: String,
        _update_name: String,
        _input: Payloads,
        _request: RequestContext,
        _timeout: Duration,
        _wait_policy: UpdateWaitPolicy,
    ) -> Result<UpdateOutcome> {
        unreachable!()
    }

    async fn pending_update_transports(
        &self,
        _run_key: RunKey,
    ) -> Result<Vec<PendingUpdateTransport>> {
        Ok(Vec::new())
    }

    async fn resolve_update_transport(
        &self,
        _run_key: RunKey,
        _update_id: String,
        _resolution: UpdateTransportResolution,
    ) -> Result<bool> {
        Ok(false)
    }

    async fn peek_update_info(
        &self,
        _run_key: RunKey,
        _update_id: String,
    ) -> Result<Option<(String, Payloads)>> {
        Ok(None)
    }

    async fn resolve_nexus_operation(
        &self,
        _run_key: RunKey,
        _operation_id: String,
        _scheduled_event_id: i64,
        _resolution: NexusResolution,
    ) -> Result<bool> {
        Ok(false)
    }
}

struct ScriptedVisibility {
    responses: Mutex<VecDeque<Result<ListWorkflowExecutionsResponse>>>,
    deleted: Mutex<Vec<RunKey>>,
}

impl ScriptedVisibility {
    fn from_responses(responses: Vec<Result<ListWorkflowExecutionsResponse>>) -> Self {
        Self {
            responses: Mutex::new(responses.into()),
            deleted: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl VisibilityApi for ScriptedVisibility {
    async fn list_workflows(
        &self,
        _req: ListWorkflowExecutionsRequest,
    ) -> Result<ListWorkflowExecutionsResponse> {
        self.responses.lock().await.pop_front().unwrap_or_else(|| {
            Ok(ListWorkflowExecutionsResponse {
                executions: Vec::new(),
                next_page_token: None,
            })
        })
    }

    async fn count_workflows(
        &self,
        _req: tokeira_edge::CountWorkflowExecutionsRequest,
    ) -> Result<tokeira_edge::CountWorkflowExecutionsResponse> {
        Ok(tokeira_edge::CountWorkflowExecutionsResponse {
            total_count: 0,
            groups: Vec::new(),
        })
    }

    async fn delete_execution(&self, run_key: RunKey) -> Result<()> {
        self.deleted.lock().await.push(run_key);
        Ok(())
    }
}

fn outcome() -> WorkflowMutationOutcome {
    WorkflowMutationOutcome {
        transition_seq: 1,
        last_event_id: 1,
        was_duplicate: false,
        execution_status: ExecutionStatus::Running,
        new_run_id: None,
    }
}

fn edge_context() -> EdgeContext {
    EdgeContext {
        request_id: RequestId::new("req-batch"),
        principal: Principal::root(),
        namespace: Some(ResolvedNamespace::active("default")),
        received_at: time::OffsetDateTime::UNIX_EPOCH,
        is_long_poll: false,
    }
}

async fn build_service(
    runtime: Arc<dyn WorkflowRuntimeApi>,
    visibility: Arc<dyn VisibilityApi>,
    repo: Arc<InMemoryStore>,
) -> WorkflowService {
    let namespaces = Arc::new(InMemoryNamespaceCache::new());
    namespaces
        .insert(ResolvedNamespace::active("default"))
        .await
        .expect("namespace insert");
    WorkflowService::new_with_versioning_and_buffered_queries_and_history_wait_registry(
        runtime,
        Arc::new(InMemoryExecutionResolver::new()),
        visibility,
        repo,
        Arc::new(InMemoryOperatorApi::new("tokeira-local")),
        namespaces.clone(),
        Arc::new(EdgeInterceptors::permissive(namespaces)),
        PollerRegistry::default(),
        PendingQueryStore::default(),
        BufferedQueryRegistry::default(),
        InMemoryBroker::default(),
        tokeira_runtime::NexusTaskBroker::default(),
        LongPollGate::new(LongPollConfig::default()),
        Arc::new(LocalOnlyRouter),
        tokeira_edge::HistoryWaitRegistry::default(),
        Arc::new(VersioningRuleStore::default()),
        WorkerRegistry::default(),
        Arc::new(ScheduleStore::default()),
        Arc::new(BatchOperationStore::default()),
    )
}

async fn seed_workflow(
    repo: Arc<InMemoryStore>,
    workflow_id: &str,
    build_id: Option<&str>,
    start_first_wft: bool,
) -> WorkflowExecutionRef {
    let runtime = Arc::new(TokeiraRuntime::new(
        repo.clone(),
        4,
        LaneConfig::default(),
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
    ));
    let namespace_id = namespace_id_for("default");
    let run_key = RunKey::new();
    let run_id = RunId::new();
    let now = time::OffsetDateTime::now_utc();
    let queue = TaskQueueName("queue".to_string());
    runtime
        .start_workflow_with_policy(StartRequest {
            run_key,
            namespace_id,
            workflow_id: WorkflowId(workflow_id.to_string()),
            run_id,
            workflow_type: WorkflowType("test".to_string()),
            task_queue: queue.clone(),
            input: Payloads::default(),
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: time::Duration::seconds(10),
            retry_policy: None,
            conflict_policy: WorkflowIdConflictPolicy::Fail,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            deployment: None,
            build_id: build_id.map(|value| BuildId(value.to_string())),
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_initiated_event_id: 0,
            original_execution_run_id: None,
            continued_failure: None,
            last_completion_result: None,
            first_run_started_at: None,
            request: RequestContext {
                request_id: DomainRequestId(format!("seed-{workflow_id}")),
                caller_identity: Some("seed".to_string()),
                received_at: now,
            },
            now,
            cron_schedule: None,
        })
        .await
        .expect("seed workflow");

    if start_first_wft {
        let poll = runtime
            .poll_workflow_task(
                QueueKey {
                    namespace_id,
                    task_queue: queue,
                    task_kind: TaskKind::Workflow,
                    deployment: None,
                    build_id: build_id.map(|value| BuildId(value.to_string())),
                },
                WorkerIdentity("worker".to_string()),
                Duration::from_millis(10),
            )
            .await
            .expect("poll workflow task");
        assert!(
            poll.is_some(),
            "seeded workflow should yield a workflow task"
        );
    }

    WorkflowExecutionRef {
        workflow_id: workflow_id.to_string(),
        run_id: Some(run_id.0.to_string()),
    }
}

async fn wait_for_state(
    store: Arc<BatchOperationStore>,
    namespace_id: tokeira_types::NamespaceId,
    job_id: &JobId,
    expected: BatchOperationState,
) {
    for _ in 0..100 {
        if let Ok(snapshot) = store.describe(namespace_id, job_id) {
            if snapshot.state == expected {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for state {expected:?}");
}

#[tokio::test]
async fn start_batch_operation_creates_running_entry_and_duplicate_rejected() {
    let repo = Arc::new(InMemoryStore::default());
    let workflow = seed_workflow(repo.clone(), "wf-start", None, false).await;
    let runtime = Arc::new(RecordingRuntime::blocking());
    let service = build_service(runtime.clone(), Arc::new(EmptyVisibilityApi), repo).await;
    let headers = HeaderMap::new();

    service
        .start_batch_operation(
            &headers,
            StartBatchOperationRequest {
                namespace: "default".to_string(),
                job_id: JobId("job-running".to_string()),
                reason: "batch reason".to_string(),
                visibility_query: None,
                executions: Some(vec![workflow]),
                max_operations_per_second: 10.0,
                operation_type: BatchOperationType::Cancel,
                operation_params: BatchOperationParams::Cancel {
                    identity: String::new(),
                },
            },
        )
        .await
        .expect("start batch");

    runtime.wait_started().await;

    let snapshot = service
        .describe_batch_operation(
            &headers,
            DescribeBatchOperationRequest {
                namespace: "default".to_string(),
                job_id: JobId("job-running".to_string()),
            },
        )
        .await
        .expect("describe batch");
    assert_eq!(snapshot.state, BatchOperationState::Running);
    assert_eq!(snapshot.reason, "batch reason");
    assert_eq!(snapshot.identity, "root");

    let duplicate = service
        .start_batch_operation(
            &headers,
            StartBatchOperationRequest {
                namespace: "default".to_string(),
                job_id: JobId("job-running".to_string()),
                reason: "other".to_string(),
                visibility_query: Some("WorkflowType = 'test'".to_string()),
                executions: None,
                max_operations_per_second: 1.0,
                operation_type: BatchOperationType::Cancel,
                operation_params: BatchOperationParams::Cancel {
                    identity: "caller".to_string(),
                },
            },
        )
        .await
        .expect_err("duplicate job id must fail");
    assert!(matches!(
        duplicate,
        tokeira_edge::EdgeError::BatchOperationAlreadyExists { .. }
    ));

    runtime.release();
    wait_for_state(
        service.batch_store(),
        namespace_id_for("default"),
        &JobId("job-running".to_string()),
        BatchOperationState::Completed,
    )
    .await;
}

#[tokio::test]
async fn stop_describe_and_list_handlers_work() {
    let repo = Arc::new(InMemoryStore::default());
    let workflow = seed_workflow(repo.clone(), "wf-stop", None, false).await;
    let runtime = Arc::new(RecordingRuntime::blocking());
    let service = build_service(runtime.clone(), Arc::new(EmptyVisibilityApi), repo).await;
    let headers = HeaderMap::new();
    let job_id = JobId("job-stop".to_string());

    service
        .start_batch_operation(
            &headers,
            StartBatchOperationRequest {
                namespace: "default".to_string(),
                job_id: job_id.clone(),
                reason: "reason".to_string(),
                visibility_query: None,
                executions: Some(vec![workflow]),
                max_operations_per_second: 1.0,
                operation_type: BatchOperationType::Cancel,
                operation_params: BatchOperationParams::Cancel {
                    identity: "starter".to_string(),
                },
            },
        )
        .await
        .expect("start batch");

    runtime.wait_started().await;

    service
        .stop_batch_operation(
            &headers,
            StopBatchOperationRequest {
                namespace: "default".to_string(),
                job_id: job_id.clone(),
                reason: "user stop".to_string(),
                identity: "operator".to_string(),
            },
        )
        .await
        .expect("stop batch");

    let described = service
        .describe_batch_operation(
            &headers,
            DescribeBatchOperationRequest {
                namespace: "default".to_string(),
                job_id: job_id.clone(),
            },
        )
        .await
        .expect("describe batch");
    assert_eq!(described.reason, "user stop");

    let (entries, _) = service
        .list_batch_operations(
            &headers,
            ListBatchOperationsRequest {
                namespace: "default".to_string(),
                page_size: 10,
                next_page_token: Vec::new(),
            },
        )
        .await
        .expect("list batches");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].job_id, job_id);

    runtime.release();
    wait_for_state(
        service.batch_store(),
        namespace_id_for("default"),
        &job_id,
        BatchOperationState::Completed,
    )
    .await;
}

#[tokio::test]
async fn run_batch_operation_processes_explicit_executions_and_counts_progress() {
    let repo = Arc::new(InMemoryStore::default());
    let wf1 = seed_workflow(repo.clone(), "wf-explicit-1", None, false).await;
    let wf2 = seed_workflow(repo.clone(), "wf-explicit-2", None, false).await;
    let runtime = Arc::new(RecordingRuntime::default());
    let service = build_service(runtime.clone(), Arc::new(EmptyVisibilityApi), repo).await;
    let store = service.batch_store();
    let namespace_id = namespace_id_for("default");
    let job_id = JobId("job-explicit".to_string());
    let cancellation = tokio_util::sync::CancellationToken::new();

    store
        .create(BatchOperationEntry {
            job_id: job_id.clone(),
            namespace_id,
            operation_type: BatchOperationType::Cancel,
            operation_params: BatchOperationParams::Cancel {
                identity: "starter".to_string(),
            },
            state: BatchOperationState::Running,
            start_time: time::OffsetDateTime::now_utc(),
            close_time: None,
            counters: Arc::new(BatchProgressCounters::default()),
            visibility_query: None,
            executions: Some(vec![wf1, wf2]),
            reason: "reason".to_string(),
            identity: "starter".to_string(),
            max_operations_per_second: 1000.0,
            cancellation_token: cancellation.clone(),
            stop_reason: None,
            stop_identity: None,
        })
        .expect("create batch");

    batch_engine::run_batch_operation(
        store.clone(),
        service,
        BatchDispatchContext {
            namespace_id,
            namespace_name: "default".to_string(),
            identity: "starter".to_string(),
            edge_context: edge_context(),
        },
        namespace_id,
        job_id.clone(),
        cancellation,
    )
    .await;

    let snapshot = store.describe(namespace_id, &job_id).expect("snapshot");
    assert_eq!(snapshot.state, BatchOperationState::Completed);
    assert_eq!(snapshot.total_operation_count, 2);
    assert_eq!(snapshot.complete_operation_count, 2);
    assert_eq!(snapshot.failure_operation_count, 0);
    assert_eq!(runtime.calls().await.len(), 2);
}

#[tokio::test]
async fn run_batch_operation_fails_on_visibility_error() {
    let repo = Arc::new(InMemoryStore::default());
    let visibility = Arc::new(ScriptedVisibility::from_responses(vec![Err(anyhow!(
        "visibility boom"
    ))]));
    let runtime = Arc::new(RecordingRuntime::default());
    let service = build_service(runtime, visibility, repo).await;
    let store = service.batch_store();
    let namespace_id = namespace_id_for("default");
    let job_id = JobId("job-fail".to_string());
    let cancellation = tokio_util::sync::CancellationToken::new();

    store
        .create(BatchOperationEntry {
            job_id: job_id.clone(),
            namespace_id,
            operation_type: BatchOperationType::Signal,
            operation_params: BatchOperationParams::Signal {
                signal_name: "sig".to_string(),
                input: None,
                identity: "starter".to_string(),
            },
            state: BatchOperationState::Running,
            start_time: time::OffsetDateTime::now_utc(),
            close_time: None,
            counters: Arc::new(BatchProgressCounters::default()),
            visibility_query: Some("WorkflowType = 'test'".to_string()),
            executions: None,
            reason: "reason".to_string(),
            identity: "starter".to_string(),
            max_operations_per_second: 1.0,
            cancellation_token: cancellation.clone(),
            stop_reason: None,
            stop_identity: None,
        })
        .expect("create batch");

    batch_engine::run_batch_operation(
        store.clone(),
        service,
        BatchDispatchContext {
            namespace_id,
            namespace_name: "default".to_string(),
            identity: "starter".to_string(),
            edge_context: edge_context(),
        },
        namespace_id,
        job_id.clone(),
        cancellation,
    )
    .await;

    let snapshot = store.describe(namespace_id, &job_id).expect("snapshot");
    assert_eq!(snapshot.state, BatchOperationState::Failed);
}

#[tokio::test]
async fn run_batch_operation_stops_on_cancellation_without_rollback() {
    let repo = Arc::new(InMemoryStore::default());
    let wf1 = seed_workflow(repo.clone(), "wf-cancel-1", None, false).await;
    let wf2 = seed_workflow(repo.clone(), "wf-cancel-2", None, false).await;
    let runtime = Arc::new(RecordingRuntime::blocking());
    let service = build_service(runtime.clone(), Arc::new(EmptyVisibilityApi), repo).await;
    let store = service.batch_store();
    let namespace_id = namespace_id_for("default");
    let job_id = JobId("job-cancel".to_string());
    let cancellation = tokio_util::sync::CancellationToken::new();

    store
        .create(BatchOperationEntry {
            job_id: job_id.clone(),
            namespace_id,
            operation_type: BatchOperationType::Cancel,
            operation_params: BatchOperationParams::Cancel {
                identity: "starter".to_string(),
            },
            state: BatchOperationState::Running,
            start_time: time::OffsetDateTime::now_utc(),
            close_time: None,
            counters: Arc::new(BatchProgressCounters::default()),
            visibility_query: None,
            executions: Some(vec![wf1, wf2]),
            reason: "reason".to_string(),
            identity: "starter".to_string(),
            max_operations_per_second: 1000.0,
            cancellation_token: cancellation.clone(),
            stop_reason: None,
            stop_identity: None,
        })
        .expect("create batch");

    let task = tokio::spawn(batch_engine::run_batch_operation(
        store.clone(),
        service,
        BatchDispatchContext {
            namespace_id,
            namespace_name: "default".to_string(),
            identity: "starter".to_string(),
            edge_context: edge_context(),
        },
        namespace_id,
        job_id.clone(),
        cancellation.clone(),
    ));

    runtime.wait_started().await;
    cancellation.cancel();
    runtime.release();
    task.await.expect("engine join");

    let snapshot = store.describe(namespace_id, &job_id).expect("snapshot");
    assert_eq!(snapshot.state, BatchOperationState::Completed);
    assert_eq!(snapshot.total_operation_count, 2);
    assert!(snapshot.complete_operation_count + snapshot.failure_operation_count <= 1);
}

#[tokio::test]
async fn run_batch_operation_uses_visibility_pagination() {
    let repo = Arc::new(InMemoryStore::default());
    let wf1 = seed_workflow(repo.clone(), "wf-page-1", None, false).await;
    let wf2 = seed_workflow(repo.clone(), "wf-page-2", None, false).await;
    let visibility = Arc::new(ScriptedVisibility::from_responses(vec![
        Ok(ListWorkflowExecutionsResponse {
            executions: vec![WorkflowExecutionSummary {
                namespace: "default".to_string(),
                workflow_id: wf1.workflow_id.clone(),
                run_id: RunId(uuid::Uuid::parse_str(wf1.run_id.as_deref().unwrap()).unwrap()),
                workflow_type: "test".to_string(),
                task_queue: "queue".to_string(),
                status: ExecutionStatus::Running,
                start_time: None,
                close_time: None,
                history_length: 0,
                state_transition_count: 0,
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
            }],
            next_page_token: Some("page-2".to_string()),
        }),
        Ok(ListWorkflowExecutionsResponse {
            executions: vec![WorkflowExecutionSummary {
                namespace: "default".to_string(),
                workflow_id: wf2.workflow_id.clone(),
                run_id: RunId(uuid::Uuid::parse_str(wf2.run_id.as_deref().unwrap()).unwrap()),
                workflow_type: "test".to_string(),
                task_queue: "queue".to_string(),
                status: ExecutionStatus::Running,
                start_time: None,
                close_time: None,
                history_length: 0,
                state_transition_count: 0,
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
            }],
            next_page_token: None,
        }),
    ]));
    let runtime = Arc::new(RecordingRuntime::default());
    let service = build_service(runtime.clone(), visibility, repo).await;
    let store = service.batch_store();
    let namespace_id = namespace_id_for("default");
    let job_id = JobId("job-pages".to_string());
    let cancellation = tokio_util::sync::CancellationToken::new();

    store
        .create(BatchOperationEntry {
            job_id: job_id.clone(),
            namespace_id,
            operation_type: BatchOperationType::Signal,
            operation_params: BatchOperationParams::Signal {
                signal_name: "sig".to_string(),
                input: None,
                identity: "starter".to_string(),
            },
            state: BatchOperationState::Running,
            start_time: time::OffsetDateTime::now_utc(),
            close_time: None,
            counters: Arc::new(BatchProgressCounters::default()),
            visibility_query: Some("WorkflowType = 'test'".to_string()),
            executions: None,
            reason: "reason".to_string(),
            identity: "starter".to_string(),
            max_operations_per_second: 1000.0,
            cancellation_token: cancellation.clone(),
            stop_reason: None,
            stop_identity: None,
        })
        .expect("create batch");

    batch_engine::run_batch_operation(
        store.clone(),
        service,
        BatchDispatchContext {
            namespace_id,
            namespace_name: "default".to_string(),
            identity: "starter".to_string(),
            edge_context: edge_context(),
        },
        namespace_id,
        job_id.clone(),
        cancellation,
    )
    .await;

    let snapshot = store.describe(namespace_id, &job_id).expect("snapshot");
    assert_eq!(snapshot.total_operation_count, 2);
    assert_eq!(runtime.calls().await.len(), 2);
}

#[tokio::test]
async fn run_batch_operation_dispatches_delete_and_reset() {
    let repo = Arc::new(InMemoryStore::default());
    let delete_wf = seed_workflow(repo.clone(), "wf-delete", None, false).await;
    let reset_wf = seed_workflow(repo.clone(), "wf-reset", None, true).await;
    let visibility = Arc::new(EmptyVisibilityApi);
    let runtime = Arc::new(RecordingRuntime::default());
    let service = build_service(runtime.clone(), visibility, repo).await;
    let namespace_id = namespace_id_for("default");

    let delete_job = JobId("job-delete".to_string());
    let delete_store = service.batch_store();
    let delete_token = tokio_util::sync::CancellationToken::new();
    delete_store
        .create(BatchOperationEntry {
            job_id: delete_job.clone(),
            namespace_id,
            operation_type: BatchOperationType::Delete,
            operation_params: BatchOperationParams::Delete {
                identity: "starter".to_string(),
            },
            state: BatchOperationState::Running,
            start_time: time::OffsetDateTime::now_utc(),
            close_time: None,
            counters: Arc::new(BatchProgressCounters::default()),
            visibility_query: None,
            executions: Some(vec![delete_wf]),
            reason: "reason".to_string(),
            identity: "starter".to_string(),
            max_operations_per_second: 1000.0,
            cancellation_token: delete_token.clone(),
            stop_reason: None,
            stop_identity: None,
        })
        .expect("create delete batch");

    batch_engine::run_batch_operation(
        delete_store.clone(),
        service.clone(),
        BatchDispatchContext {
            namespace_id,
            namespace_name: "default".to_string(),
            identity: "starter".to_string(),
            edge_context: edge_context(),
        },
        namespace_id,
        delete_job.clone(),
        delete_token,
    )
    .await;

    let reset_job = JobId("job-reset".to_string());
    let reset_token = tokio_util::sync::CancellationToken::new();
    delete_store
        .create(BatchOperationEntry {
            job_id: reset_job.clone(),
            namespace_id,
            operation_type: BatchOperationType::Reset,
            operation_params: BatchOperationParams::Reset {
                identity: "starter".to_string(),
                target: BatchResetTarget::FirstWorkflowTask,
                reason: "reset reason".to_string(),
            },
            state: BatchOperationState::Running,
            start_time: time::OffsetDateTime::now_utc(),
            close_time: None,
            counters: Arc::new(BatchProgressCounters::default()),
            visibility_query: None,
            executions: Some(vec![reset_wf]),
            reason: "reason".to_string(),
            identity: "starter".to_string(),
            max_operations_per_second: 1000.0,
            cancellation_token: reset_token.clone(),
            stop_reason: None,
            stop_identity: None,
        })
        .expect("create reset batch");

    batch_engine::run_batch_operation(
        delete_store.clone(),
        service.clone(),
        BatchDispatchContext {
            namespace_id,
            namespace_name: "default".to_string(),
            identity: "starter".to_string(),
            edge_context: edge_context(),
        },
        namespace_id,
        reset_job.clone(),
        reset_token,
    )
    .await;

    let calls = runtime.calls().await;
    assert!(
        calls
            .iter()
            .any(|call| matches!(call, RecordedCall::Terminate { .. }))
    );
    assert!(calls.iter().any(|call| matches!(call, RecordedCall::Reset { fork_event_id, reason, .. } if *fork_event_id > 0 && reason == "reset reason")));
}

#[test]
fn compute_sleep_duration_defaults_to_fifty_ops_per_second() {
    assert_eq!(
        batch_engine::compute_sleep_duration(0.0),
        Duration::from_millis(20)
    );
}
