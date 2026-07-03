//! Focused tests for the inbound `/nexus/callback` completion handler (Wave 5, P9).
//!
//! These drive [`handle_nexus_callback`] directly with a scripted [`FakeRuntime`]
//! `WorkflowRuntimeApi` so the transport-agnostic handler can be exercised without an HTTP
//! server: token decode / state selection / content-type + body validation → status, and
//! the `resolve_nexus_operation` result → status mapping (200 / 404 / 503).

use std::sync::Mutex;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokeira_edge::{
    CallbackResponse, WorkflowMutationOutcome, WorkflowRuntimeApi, handle_nexus_callback,
};
use tokeira_kernel::{
    CancelRequest, NexusResolution, ResetRequest, SignalRequest, StartRequest, TerminateRequest,
    WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    NexusCompletionFailureBody, NexusCompletionToken, PendingUpdateTransport, QueryResult,
    ResetWorkflowResult, SignalWithStartResult, StartWorkflowResult, UpdateLifecycleSnapshot,
    UpdateTransportResolution, UpdateWaitPolicy,
};
use tokeira_types::{
    ActivityTaskToken, ExecutionRef, Payload, Payloads, QueueKey, RequestContext, RunKey,
    WorkerIdentity,
};
use uuid::Uuid;

/// What the fake runtime returns from `resolve_nexus_operation`.
#[derive(Clone, Copy)]
enum Behavior {
    /// A new terminal event was recorded.
    Accepted,
    /// The kernel rejected the command (absent / stale / already resolved).
    Rejected,
    /// A transient internal failure.
    Errored,
}

/// A `WorkflowRuntimeApi` whose only live method is `resolve_nexus_operation`: it records
/// the forwarded arguments and returns a scripted outcome. Every other method is
/// unreachable — the callback handler never calls them.
struct FakeRuntime {
    behavior: Behavior,
    calls: Mutex<Vec<(RunKey, String, i64, NexusResolution)>>,
}

impl FakeRuntime {
    fn new(behavior: Behavior) -> Self {
        Self {
            behavior,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<(RunKey, String, i64, NexusResolution)> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl WorkflowRuntimeApi for FakeRuntime {
    async fn resolve_nexus_operation(
        &self,
        run_key: RunKey,
        operation_id: String,
        scheduled_event_id: i64,
        resolution: NexusResolution,
    ) -> Result<bool> {
        self.calls
            .lock()
            .unwrap()
            .push((run_key, operation_id, scheduled_event_id, resolution));
        match self.behavior {
            Behavior::Accepted => Ok(true),
            Behavior::Rejected => Ok(false),
            Behavior::Errored => Err(anyhow!("simulated transient lane failure")),
        }
    }

    async fn start_workflow(&self, _req: StartRequest) -> Result<WorkflowMutationOutcome> {
        unreachable!("callback handler never starts workflows")
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
        _run_key: RunKey,
        _req: SignalRequest,
    ) -> Result<WorkflowMutationOutcome> {
        unreachable!()
    }

    async fn poll_workflow_task(
        &self,
        _queue: QueueKey,
        _worker_identity: WorkerIdentity,
        _timeout: std::time::Duration,
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
        _timeout: std::time::Duration,
    ) -> Result<Option<tokeira_runtime::StartedActivityTask>> {
        unreachable!()
    }

    async fn complete_activity_task(
        &self,
        _token: ActivityTaskToken,
        _result: Payloads,
        _worker_identity: Option<WorkerIdentity>,
    ) -> Result<WorkflowMutationOutcome> {
        unreachable!()
    }

    async fn fail_activity_task(
        &self,
        _token: ActivityTaskToken,
        _failure: Payload,
        _failure_error_type: Option<String>,
        _is_non_retryable: bool,
        _worker_identity: Option<WorkerIdentity>,
    ) -> Result<()> {
        unreachable!()
    }

    async fn record_activity_heartbeat(
        &self,
        _token: ActivityTaskToken,
        _details: Option<Payloads>,
        _identity: Option<tokeira_types::WorkerIdentity>,
    ) -> Result<bool> {
        unreachable!()
    }

    async fn terminate_workflow(
        &self,
        _run_key: RunKey,
        _req: TerminateRequest,
    ) -> Result<WorkflowMutationOutcome> {
        unreachable!()
    }

    async fn cancel_workflow(
        &self,
        _run_key: RunKey,
        _req: CancelRequest,
    ) -> Result<WorkflowMutationOutcome> {
        unreachable!()
    }

    async fn reset_workflow(
        &self,
        _execution: ExecutionRef,
        _req: ResetRequest,
    ) -> Result<ResetWorkflowResult> {
        unreachable!()
    }

    async fn query_workflow(
        &self,
        _execution: ExecutionRef,
        _query_type: String,
        _query_args: Payloads,
        _timeout: std::time::Duration,
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
        _timeout: std::time::Duration,
        _wait_policy: UpdateWaitPolicy,
    ) -> Result<UpdateLifecycleSnapshot> {
        unreachable!()
    }

    async fn pending_update_transports(
        &self,
        _run_key: RunKey,
    ) -> Result<Vec<PendingUpdateTransport>> {
        unreachable!()
    }

    async fn resolve_update_transport(
        &self,
        _run_key: RunKey,
        _update_id: String,
        _resolution: UpdateTransportResolution,
    ) -> Result<bool> {
        unreachable!()
    }

    async fn peek_update_info(
        &self,
        _run_key: RunKey,
        _update_id: String,
    ) -> Result<Option<(String, Payloads)>> {
        unreachable!()
    }
}

/// Build a valid encoded callback token for `(run_key, operation_id, scheduled_event_id)`.
fn encoded_token(run_key: RunKey, operation_id: &str, scheduled_event_id: i64) -> String {
    NexusCompletionToken {
        originator_run_key: run_key,
        operation_id: operation_id.to_string(),
        scheduled_event_id,
        request_id: "req-1".to_string(),
        endpoint: "endpoint".to_string(),
        service: "service".to_string(),
        operation: "operation".to_string(),
    }
    .encode()
    .expect("token encodes")
}

/// A serialized failure body (the shape the firing client sends for failed/canceled).
fn failure_body(message: &str) -> Vec<u8> {
    NexusCompletionFailureBody {
        message: message.to_string(),
        failure: Payload {
            data: b"failure-detail".to_vec(),
            metadata: std::collections::BTreeMap::from([(
                "encoding".to_string(),
                "json/plain".to_string(),
            )]),
        },
    }
    .encode()
}

#[tokio::test]
async fn succeeded_completion_resolves_and_returns_200() {
    let runtime = FakeRuntime::new(Behavior::Accepted);
    let run_key = RunKey::new();
    let token = encoded_token(run_key, "op-1", 7);

    let response =
        handle_nexus_callback(&runtime, Some(&token), Some("succeeded"), None, b"").await;

    assert_eq!(
        response,
        CallbackResponse {
            status: 200,
            body: None
        }
    );
    let calls = runtime.calls();
    assert_eq!(calls.len(), 1, "exactly one resolution submitted");
    let (forwarded_key, forwarded_op, forwarded_event, resolution) = &calls[0];
    assert_eq!(*forwarded_key, run_key);
    assert_eq!(forwarded_op, "op-1");
    assert_eq!(*forwarded_event, 7);
    assert!(
        matches!(resolution, NexusResolution::Completed { .. }),
        "succeeded → Completed, got {resolution:?}"
    );
}

#[tokio::test]
async fn failed_completion_wraps_failure_and_returns_200() {
    let runtime = FakeRuntime::new(Behavior::Accepted);
    let token = encoded_token(RunKey::new(), "op-2", 11);

    let response = handle_nexus_callback(
        &runtime,
        Some(&token),
        Some("failed"),
        Some("application/json"),
        &failure_body("boom"),
    )
    .await;

    assert_eq!(response.status, 200);
    let calls = runtime.calls();
    assert_eq!(calls.len(), 1);
    match &calls[0].3 {
        NexusResolution::Failed { failure } => {
            // Gap A: the handler failure is wrapped in a NexusOperationFailureInfo (so the
            // caller decodes a NexusOperationError) and re-encoded as temporal/failure+proto
            // — NOT forwarded as the raw json/plain completion payload. The wrapper's
            // structure is asserted in translate::nexus tests and end-to-end by the
            // TestNexusAsyncOperationErrorRehydration conformance leaf.
            assert_eq!(
                failure.metadata.get("encoding").map(String::as_str),
                Some("temporal/failure+proto"),
                "completion failure must be wrapped as a temporal failure, got {:?}",
                failure.metadata
            );
            assert_ne!(
                failure.data, b"failure-detail",
                "failure must be wrapped, not the raw forwarded payload"
            );
        }
        other => panic!("failed → Failed, got {other:?}"),
    }
}

#[tokio::test]
async fn canceled_completion_resolves_to_canceled_and_returns_200() {
    let runtime = FakeRuntime::new(Behavior::Accepted);
    let token = encoded_token(RunKey::new(), "op-3", 13);

    let response = handle_nexus_callback(
        &runtime,
        Some(&token),
        Some("canceled"),
        // A charset parameter must not defeat the content-type check.
        Some("application/json; charset=utf-8"),
        &failure_body("user canceled"),
    )
    .await;

    assert_eq!(response.status, 200);
    let calls = runtime.calls();
    assert_eq!(calls.len(), 1);
    assert!(matches!(calls[0].3, NexusResolution::Canceled));
}

#[tokio::test]
async fn already_resolved_or_absent_operation_returns_404() {
    let runtime = FakeRuntime::new(Behavior::Rejected);
    let token = encoded_token(RunKey::new(), "op-4", 17);

    let response =
        handle_nexus_callback(&runtime, Some(&token), Some("succeeded"), None, b"").await;

    assert_eq!(
        response.status, 404,
        "kernel rejection → not-found (idempotent)"
    );
    assert_eq!(runtime.calls().len(), 1, "resolution was attempted");
}

#[tokio::test]
async fn transient_runtime_error_returns_503() {
    let runtime = FakeRuntime::new(Behavior::Errored);
    let token = encoded_token(RunKey::new(), "op-5", 19);

    let response =
        handle_nexus_callback(&runtime, Some(&token), Some("succeeded"), None, b"").await;

    assert_eq!(
        response.status, 503,
        "transient internal failure → retryable 503"
    );
}

#[tokio::test]
async fn missing_token_is_bad_request_and_does_not_resolve() {
    let runtime = FakeRuntime::new(Behavior::Accepted);

    let response = handle_nexus_callback(&runtime, None, Some("succeeded"), None, b"").await;

    assert_eq!(response.status, 400);
    assert!(
        runtime.calls().is_empty(),
        "no resolution for a missing token"
    );
}

#[tokio::test]
async fn undecodable_token_is_bad_request_and_does_not_resolve() {
    let runtime = FakeRuntime::new(Behavior::Accepted);

    let response = handle_nexus_callback(
        &runtime,
        Some("not-a-valid-token"),
        Some("succeeded"),
        None,
        b"",
    )
    .await;

    assert_eq!(response.status, 400);
    assert!(runtime.calls().is_empty());
}

#[tokio::test]
async fn wrong_version_token_is_bad_request() {
    let runtime = FakeRuntime::new(Behavior::Accepted);
    // A well-formed `{v,d}` envelope carrying an unsupported version: the envelope parses
    // but the version check fails (`DecodeCallbackToken` → InvalidArgument @ v1.31.0).
    let valid = encoded_token(RunKey::new(), "op-6", 23);
    let wrong_version = valid.replace("\"v\":1", "\"v\":2");
    assert_ne!(valid, wrong_version, "the version substitution took effect");

    let response =
        handle_nexus_callback(&runtime, Some(&wrong_version), Some("succeeded"), None, b"").await;

    assert_eq!(response.status, 400);
    assert!(runtime.calls().is_empty());
}

#[tokio::test]
async fn missing_state_header_is_bad_request() {
    let runtime = FakeRuntime::new(Behavior::Accepted);
    let token = encoded_token(RunKey::new(), "op-7", 29);

    let response = handle_nexus_callback(&runtime, Some(&token), None, None, b"").await;

    assert_eq!(response.status, 400);
    assert!(runtime.calls().is_empty());
}

#[tokio::test]
async fn unknown_state_is_bad_request() {
    let runtime = FakeRuntime::new(Behavior::Accepted);
    let token = encoded_token(RunKey::new(), "op-8", 31);

    let response = handle_nexus_callback(&runtime, Some(&token), Some("running"), None, b"").await;

    assert_eq!(response.status, 400);
    assert!(runtime.calls().is_empty());
}

#[tokio::test]
async fn failed_without_json_content_type_is_bad_request() {
    let runtime = FakeRuntime::new(Behavior::Accepted);
    let token = encoded_token(RunKey::new(), "op-9", 37);

    let response = handle_nexus_callback(
        &runtime,
        Some(&token),
        Some("failed"),
        // Missing the required `application/json` content type.
        None,
        &failure_body("boom"),
    )
    .await;

    assert_eq!(response.status, 400);
    assert!(runtime.calls().is_empty());
}

#[tokio::test]
async fn failed_with_malformed_body_is_bad_request() {
    let runtime = FakeRuntime::new(Behavior::Accepted);
    let token = encoded_token(RunKey::new(), "op-10", 41);

    let response = handle_nexus_callback(
        &runtime,
        Some(&token),
        Some("failed"),
        Some("application/json"),
        b"{ this is not valid json",
    )
    .await;

    assert_eq!(response.status, 400);
    assert!(runtime.calls().is_empty());
}

/// A token whose argument identifier exercises the `Uuid`-backed run key end to end:
/// the handler must forward the exact run key without mangling it.
#[tokio::test]
async fn forwards_exact_run_key_from_token() {
    let runtime = FakeRuntime::new(Behavior::Accepted);
    let run_key = RunKey(Uuid::from_u128(0x1234_5678_9abc_def0_1122_3344_5566_7788));
    let token = encoded_token(run_key, "op-11", 43);

    let response =
        handle_nexus_callback(&runtime, Some(&token), Some("succeeded"), None, b"").await;

    assert_eq!(response.status, 200);
    assert_eq!(runtime.calls()[0].0, run_key);
}
