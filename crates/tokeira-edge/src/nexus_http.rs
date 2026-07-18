//! Caller-facing Nexus HTTP dispatch for Temporal v1.31.0 compatibility.
//!
//! The same public listener that serves Temporal gRPC forwards the two Nexus
//! HTTP route families here. This module owns outer-route preprocessing, the
//! Nexus RPC request grammar, authorization ordering, HTTP-to-worker request
//! translation, synchronous caller correlation, worker-outcome conversion, and
//! Nexus telemetry. The kernel is intentionally absent: caller-facing dispatch
//! uses the runtime's disposable delivery broker and does not mutate workflow
//! history.
#![allow(deprecated)]

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration as StdDuration, Instant},
};

use async_trait::async_trait;
use base64::Engine as _;
use percent_encoding::percent_decode_str;
use prost::Message;
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use time::{Duration, OffsetDateTime};
use tokeira_auth::{
    Access, Authorizer, AuthzDecision, CallClassification, CallTarget, ClaimMapper, Scope,
};
use tokeira_proto::{
    conversions::common::payload_to_domain,
    public::temporal::api::{
        common::v1 as common_proto, enums::v1 as enums_proto, failure::v1 as failure_proto,
        nexus::v1 as nexus_v1,
    },
};
use tokeira_runtime::{
    NexusHttpTaskRequest, NexusHttpTaskRequestVariant, NexusTaskBroker, NexusTaskLink,
    NexusTaskRequest,
    nexus::{NexusEndpointSpecTarget, NexusEndpointStore, NexusEndpointStoreError},
};
use tokeira_types::{NamespaceId, TaskQueueName};
use tokio::sync::oneshot;
use url::form_urlencoded;

use crate::{
    interceptors::authorizer_errors_exposed,
    metrics::{
        record_nexus_latency, record_nexus_request, record_nexus_request_preprocess_error,
        record_service_request,
    },
    namespace_cache::{NamespaceCache, ResolvedNamespace},
};

/// v1.31.0's global namespace ID limit
/// (`common/dynamicconfig/constants.go:423 @ v1.31.0`).
pub const MAX_NEXUS_ID_LENGTH: usize = 1000;
/// v1.31.0's raw Nexus HTTP body and converted public Payload limit.
pub const MAX_NEXUS_PAYLOAD_BYTES: usize = 2 * 1024 * 1024;

const START_METHOD: &str = "StartNexusOperation";
const CANCEL_METHOD: &str = "CancelNexusOperation";
const UNKNOWN_ENDPOINT: &str = "_unknown_";
const NAMESPACE_API: &str =
    "/temporal.api.nexusservice.v1.NexusService/DispatchByNamespaceAndTaskQueue";
const ENDPOINT_API: &str = "/temporal.api.nexusservice.v1.NexusService/DispatchByEndpoint";
const REQUEST_TIMEOUT_HEADER: &str = "request-timeout";
const OPERATION_TOKEN_HEADER: &str = "nexus-operation-token";
const FAILURE_SUPPORT_HEADER: &str = "temporal-nexus-failure-support";
const FAILURE_SOURCE_HEADER: &str = "Temporal-Nexus-Failure-Source";
const RETRYABLE_HEADER: &str = "Nexus-Request-Retryable";
const OPERATION_STATE_HEADER: &str = "Nexus-Operation-State";
const LINK_HEADER: &str = "Nexus-Link";
const DISPATCH_TIMEOUT: Duration = Duration::seconds(60);
const CALLER_DEADLINE_BUFFER: Duration = Duration::seconds(1);

/// A bounded request collected by the shared-listener transport adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NexusHttpRequest {
    /// HTTP method as received.
    pub method: String,
    /// Percent-encoded URI path.
    pub path: String,
    /// Raw URI query without the leading `?`.
    pub query: Option<String>,
    /// Header pairs as received. When a field repeats, the first value wins.
    pub headers: Vec<(String, String)>,
    /// Raw body bytes collected up to the transport cap.
    pub body: Vec<u8>,
    /// Whether the body exceeded the raw 2 MiB cap while being collected.
    pub body_too_large: bool,
    /// Whether the transport body stream failed while being collected.
    pub body_read_failed: bool,
}

/// A transport-neutral HTTP response returned to the listener adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NexusHttpResponse {
    /// HTTP status code.
    pub status: u16,
    /// Response headers.
    pub headers: Vec<(String, String)>,
    /// Response body.
    pub body: Vec<u8>,
}

/// Edge-owned worker outcome retained only for the lifetime of one caller-facing
/// HTTP request.
///
/// Public protobufs stay in the compatibility plane: runtime correlation carries
/// only the waiter ID and never depends on this wire representation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum NexusHttpWorkerOutcome {
    /// Worker completed the task with a StartOperation or CancelOperation response.
    Completed(nexus_v1::Response),
    /// Worker failed the task with the modern and/or deprecated failure shape.
    Failed {
        /// Deprecated Nexus handler error.
        error: Option<nexus_v1::HandlerError>,
        /// Modern Temporal Nexus handler failure.
        failure: Option<failure_proto::Failure>,
    },
}

/// Process-local HTTP caller waiters keyed by opaque IDs retained in the
/// runtime delivery broker.
///
/// This registry is deliberately edge-owned and ephemeral. Dropping a caller
/// removes only its waiter; it creates no durable workflow state and cannot
/// complete an operation.
#[derive(Clone, Default)]
pub struct NexusHttpWaiterRegistry {
    inner: Arc<Mutex<HashMap<String, oneshot::Sender<NexusHttpWorkerOutcome>>>>,
}

impl std::fmt::Debug for NexusHttpWaiterRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NexusHttpWaiterRegistry")
            .finish_non_exhaustive()
    }
}

impl NexusHttpWaiterRegistry {
    pub(crate) fn register(&self) -> NexusHttpWaiter {
        let waiter_id = uuid::Uuid::new_v4().to_string();
        let (sender, receiver) = oneshot::channel();
        self.lock().insert(waiter_id.clone(), sender);
        NexusHttpWaiter {
            waiter_id,
            registry: self.clone(),
            receiver,
        }
    }

    pub(crate) fn complete(&self, waiter_id: &str, outcome: NexusHttpWorkerOutcome) -> bool {
        self.lock()
            .remove(waiter_id)
            .is_some_and(|sender| sender.send(outcome).is_ok())
    }

    fn remove(&self, waiter_id: &str) {
        self.lock().remove(waiter_id);
    }

    fn lock(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<String, oneshot::Sender<NexusHttpWorkerOutcome>>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Debug)]
pub(crate) struct NexusHttpWaiter {
    waiter_id: String,
    registry: NexusHttpWaiterRegistry,
    receiver: oneshot::Receiver<NexusHttpWorkerOutcome>,
}

impl NexusHttpWaiter {
    pub(crate) fn id(&self) -> &str {
        &self.waiter_id
    }

    pub(crate) async fn outcome(
        &mut self,
    ) -> Result<NexusHttpWorkerOutcome, oneshot::error::RecvError> {
        (&mut self.receiver).await
    }
}

impl Drop for NexusHttpWaiter {
    fn drop(&mut self) {
        self.registry.remove(&self.waiter_id);
    }
}

/// The v1.31.0 authorization target for one resolved Nexus dispatch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NexusAuthorizationTarget {
    /// Temporal frontend API classification name.
    pub api_name: String,
    /// Resolved namespace name.
    pub namespace: String,
    /// Endpoint name for endpoint dispatch, absent for namespace/task-queue dispatch.
    pub nexus_endpoint_name: Option<String>,
}

/// A policy result already adapted to Nexus HTTP semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NexusAuthorizationDecision {
    /// Permit dispatch.
    Allow,
    /// Reject with Temporal's optional denial reason.
    Deny { reason: Option<String> },
    /// Reject authentication before the Nexus operation handler is admitted.
    Unauthenticated,
    /// Surface an authorizer failure when the expose-errors gate permits it.
    ExposedError {
        /// Nexus HandlerError type, for example `UNAVAILABLE`.
        error_type: String,
        /// Public error message.
        message: String,
    },
}

/// Authorization seam shared by production policy and the conformance callback
/// adapter.
#[async_trait]
pub trait NexusHttpAuthorizer: Send + Sync {
    /// Decide whether the resolved Nexus request may be dispatched.
    async fn authorize(
        &self,
        target: &NexusAuthorizationTarget,
        headers: &[(String, String)],
    ) -> NexusAuthorizationDecision;
}

/// Permissive policy used when no production identity source is configured.
#[derive(Clone, Copy, Debug, Default)]
pub struct PermissiveNexusHttpAuthorizer;

#[async_trait]
impl NexusHttpAuthorizer for PermissiveNexusHttpAuthorizer {
    async fn authorize(
        &self,
        _target: &NexusAuthorizationTarget,
        _headers: &[(String, String)],
    ) -> NexusAuthorizationDecision {
        NexusAuthorizationDecision::Allow
    }
}

/// Production Nexus adapter over the shared claims and authorizer foundation.
pub struct PolicyNexusHttpAuthorizer {
    claim_mapper: Arc<dyn ClaimMapper>,
    authorizer: Arc<dyn Authorizer>,
    expose_authorizer_errors: bool,
}

impl std::fmt::Debug for PolicyNexusHttpAuthorizer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PolicyNexusHttpAuthorizer")
            .field("expose_authorizer_errors", &self.expose_authorizer_errors)
            .finish_non_exhaustive()
    }
}

impl PolicyNexusHttpAuthorizer {
    /// Construct a Nexus policy adapter sharing the gRPC identity sources.
    pub fn new(
        claim_mapper: Arc<dyn ClaimMapper>,
        authorizer: Arc<dyn Authorizer>,
        expose_authorizer_errors: bool,
    ) -> Self {
        Self {
            claim_mapper,
            authorizer,
            expose_authorizer_errors,
        }
    }
}

#[async_trait]
impl NexusHttpAuthorizer for PolicyNexusHttpAuthorizer {
    async fn authorize(
        &self,
        target: &NexusAuthorizationTarget,
        headers: &[(String, String)],
    ) -> NexusAuthorizationDecision {
        let token = header_value(headers, "authorization").unwrap_or_default();
        let extras = header_value(headers, "authorization-extras").unwrap_or_default();
        let claims = match self.claim_mapper.get_claims(token, extras).await {
            Ok(claims) => claims,
            Err(error) => {
                // Nexus adapts a claim-mapper failure before operation admission,
                // unlike the gRPC interceptor's permission-denied envelope
                // (`service/frontend/nexus_http_handler.go:128-134,179-185 @
                // v1.31.0`). The diagnostic itself remains private.
                tracing::warn!(%error, api = %target.api_name, "Nexus authentication failed");
                return NexusAuthorizationDecision::Unauthenticated;
            }
        };
        let call_target = CallTarget {
            api_name: &target.api_name,
            namespace: Some(&target.namespace),
            classification: CallClassification {
                scope: Scope::Namespace,
                access: Access::Write,
            },
        };
        match self.authorizer.authorize(Some(&claims), &call_target) {
            Ok(AuthzDecision::Allow { .. }) => NexusAuthorizationDecision::Allow,
            Ok(AuthzDecision::Deny { reason }) => NexusAuthorizationDecision::Deny { reason },
            Err(error) if authorizer_errors_exposed(self.expose_authorizer_errors) => {
                NexusAuthorizationDecision::ExposedError {
                    error_type: "INTERNAL".to_owned(),
                    message: error.to_string(),
                }
            }
            Err(error) => {
                tracing::error!(%error, api = %target.api_name, "Nexus authorizer failed");
                NexusAuthorizationDecision::Deny { reason: None }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedTarget {
    namespace: String,
    namespace_id: NamespaceId,
    task_queue: String,
    endpoint_name: Option<String>,
    api_name: &'static str,
    operation_path: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct UnresolvedNamespaceTarget {
    namespace: String,
    task_queue: String,
    operation_path: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PreprocessedTarget {
    Namespace(UnresolvedNamespaceTarget),
    Endpoint(ResolvedTarget),
}

#[derive(Clone, Debug, PartialEq)]
enum ParsedOperation {
    Start {
        service: String,
        operation: String,
        links: Vec<NexusTaskLink>,
        request_timeout: Option<Duration>,
    },
    Cancel {
        service: String,
        operation: String,
        operation_token: String,
        request_timeout: Option<Duration>,
        token_from_header: bool,
    },
}

impl ParsedOperation {
    fn method(&self) -> &'static str {
        match self {
            Self::Start { .. } => START_METHOD,
            Self::Cancel { .. } => CANCEL_METHOD,
        }
    }

    fn request_timeout(&self) -> Option<Duration> {
        match self {
            Self::Start {
                request_timeout, ..
            }
            | Self::Cancel {
                request_timeout, ..
            } => *request_timeout,
        }
    }

    fn is_start(&self) -> bool {
        matches!(self, Self::Start { .. })
    }
}

/// Thin Nexus HTTP service over the live namespace, endpoint, and Nexus task
/// registries.
#[derive(Clone)]
pub struct NexusHttpHandler {
    namespaces: Arc<dyn NamespaceCache>,
    endpoints: Arc<dyn NexusEndpointStore>,
    broker: NexusTaskBroker,
    waiters: NexusHttpWaiterRegistry,
    authorizer: Arc<dyn NexusHttpAuthorizer>,
}

impl std::fmt::Debug for NexusHttpHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NexusHttpHandler")
            .finish_non_exhaustive()
    }
}

impl NexusHttpHandler {
    /// Construct a handler sharing the same registries and broker used by gRPC
    /// endpoint administration and Nexus worker polling.
    pub fn new(
        namespaces: Arc<dyn NamespaceCache>,
        endpoints: Arc<dyn NexusEndpointStore>,
        broker: NexusTaskBroker,
        waiters: NexusHttpWaiterRegistry,
        authorizer: Arc<dyn NexusHttpAuthorizer>,
    ) -> Self {
        Self {
            namespaces,
            endpoints,
            broker,
            waiters,
            authorizer,
        }
    }

    /// Whether `path` belongs to a caller-facing Nexus route rather than tonic.
    pub fn recognizes_path(path: &str) -> bool {
        path.starts_with("/namespaces/") || path.starts_with("/nexus/endpoints/")
    }

    /// Process one caller-facing Nexus HTTP request through the complete
    /// v1.31.0 admission, dispatch, and response lifecycle.
    pub async fn handle(&self, request: NexusHttpRequest) -> NexusHttpResponse {
        let request_started = OffsetDateTime::now_utc();
        let metric_started = Instant::now();

        let preprocessed = match self.preprocess(&request.path).await {
            Ok(target) => target,
            Err(error) => {
                record_nexus_request_preprocess_error();
                return error.into_response(false);
            }
        };

        let operation_path = match &preprocessed {
            PreprocessedTarget::Namespace(target) => &target.operation_path,
            PreprocessedTarget::Endpoint(target) => &target.operation_path,
        };
        let operation = match parse_operation(&request, operation_path) {
            Ok(operation) => operation,
            Err(error) => return error.into_response(false),
        };

        let target = match preprocessed {
            PreprocessedTarget::Endpoint(target) => target,
            PreprocessedTarget::Namespace(target) => {
                match self.namespaces.get(&target.namespace).await {
                    Ok(Some(namespace)) if !namespace.deleted => ResolvedTarget {
                        namespace_id: namespace_id(&namespace),
                        namespace: namespace.name,
                        task_queue: target.task_queue,
                        endpoint_name: None,
                        api_name: NAMESPACE_API,
                        operation_path: target.operation_path,
                    },
                    Ok(_) => {
                        // getOperationContext records this outcome itself because it
                        // fails before the deferred latency/service metrics are armed
                        // (`nexus_handler.go:333-379 @ v1.31.0`).
                        record_nexus_request(
                            &target.namespace,
                            operation.method(),
                            "namespace_not_found",
                            UNKNOWN_ENDPOINT,
                        );
                        return HandlerError::not_found(format!(
                            "namespace not found: {:?}",
                            target.namespace
                        ))
                        .into_response(false);
                    }
                    Err(_) => {
                        record_nexus_request(
                            &target.namespace,
                            operation.method(),
                            "namespace_not_found",
                            UNKNOWN_ENDPOINT,
                        );
                        return HandlerError::internal("internal error").into_response(false);
                    }
                }
            }
        };

        let endpoint_metric = target.endpoint_name.as_deref().unwrap_or(UNKNOWN_ENDPOINT);
        let caller_failure_support = header_value(&request.headers, FAILURE_SUPPORT_HEADER)
            .is_some_and(|value| value == "true");

        // The neutral request is intentionally constructed without its Start payload
        // before authorization. v1.31.0 authorizes the resolved dispatch target, then
        // consumes the LazyValue and enforces converted Payload size
        // (`nexus_handler.go:397-439 @ v1.31.0`).
        let mut worker_request = build_worker_request(
            &request,
            &operation,
            &target,
            request_started,
            caller_failure_support,
        );
        let authorization_target = NexusAuthorizationTarget {
            api_name: target.api_name.to_owned(),
            namespace: target.namespace.clone(),
            nexus_endpoint_name: target.endpoint_name.clone(),
        };
        match self
            .authorizer
            .authorize(&authorization_target, &request.headers)
            .await
        {
            NexusAuthorizationDecision::Allow => {}
            NexusAuthorizationDecision::Unauthenticated => {
                // Claim mapping happens in the outer Nexus HTTP handler in
                // v1.31.0, so its failure increments only the preprocess counter
                // and returns the protocol's fixed unauthenticated error.
                record_nexus_request_preprocess_error();
                return HandlerError::unauthenticated("unauthorized")
                    .into_response(caller_failure_support);
            }
            NexusAuthorizationDecision::Deny { reason } => {
                let message = reason.filter(|reason| !reason.is_empty()).map_or_else(
                    || "permission denied".to_owned(),
                    |reason| format!("permission denied: {reason}"),
                );
                record_terminal(
                    &target,
                    endpoint_metric,
                    operation.method(),
                    "unauthorized",
                    metric_started.elapsed(),
                );
                return HandlerError::unauthorized(message).into_response(caller_failure_support);
            }
            NexusAuthorizationDecision::ExposedError {
                error_type,
                message,
            } => {
                record_terminal(
                    &target,
                    endpoint_metric,
                    operation.method(),
                    "internal_auth_error",
                    metric_started.elapsed(),
                );
                return HandlerError::new(error_type, message)
                    .into_response(caller_failure_support);
            }
        }

        if operation.is_start() {
            let payload = if request.body_too_large || request.body_read_failed {
                None
            } else {
                http_body_to_payload(&request).ok()
            };
            let Some(payload) = payload else {
                record_terminal(
                    &target,
                    endpoint_metric,
                    operation.method(),
                    "internal_error",
                    metric_started.elapsed(),
                );
                return HandlerError::bad_request("invalid input")
                    .into_response(caller_failure_support);
            };
            if payload.encoded_len() > MAX_NEXUS_PAYLOAD_BYTES {
                record_terminal(
                    &target,
                    endpoint_metric,
                    operation.method(),
                    "internal_error",
                    metric_started.elapsed(),
                );
                return HandlerError::bad_request("input exceeds size limit")
                    .into_response(caller_failure_support);
            }
            if let NexusHttpTaskRequestVariant::StartOperation { payload: slot, .. } =
                &mut worker_request.variant
            {
                *slot = Some(payload_to_domain(&payload));
            }
        }

        let dispatch_deadline = dispatch_deadline(
            request_started,
            operation.request_timeout(),
            OffsetDateTime::now_utc(),
        );
        worker_request.dispatch_deadline = dispatch_deadline;
        let mut waiter = self.waiters.register();
        let delivery_lease = self
            .broker
            .publish_http(
                target.namespace_id,
                TaskQueueName(target.task_queue.clone()),
                waiter.id().to_owned(),
                NexusTaskRequest::Http(worker_request),
            )
            .await;
        let wait_for = (dispatch_deadline - OffsetDateTime::now_utc())
            .max(Duration::ZERO)
            .try_into()
            .unwrap_or(StdDuration::ZERO);
        let outcome = tokio::time::timeout(wait_for, waiter.outcome()).await;
        // A normal response and a timeout both finalize synchronously. If the
        // caller disconnects while awaiting, the lease's Drop fallback performs
        // the same transport-only cleanup from the cancelled handler future.
        delivery_lease.cancel().await;
        let (response, outcome_label) = match outcome {
            Ok(Ok(outcome)) => worker_outcome_to_http(outcome, &operation, caller_failure_support),
            Ok(Err(_)) => (
                HandlerError::internal("internal error").into_response(caller_failure_support),
                "internal_error".to_owned(),
            ),
            Err(_) => (
                HandlerError::upstream_timeout("upstream timeout")
                    .with_worker_source()
                    .into_response(caller_failure_support),
                "handler_timeout".to_owned(),
            ),
        };
        record_terminal(
            &target,
            endpoint_metric,
            operation.method(),
            &outcome_label,
            metric_started.elapsed(),
        );
        response
    }

    async fn preprocess(&self, path: &str) -> Result<PreprocessedTarget, HandlerError> {
        let segments = raw_segments(path);
        match segments.as_slice() {
            [
                namespaces,
                namespace,
                task_queues,
                task_queue,
                nexus_services,
                rest @ ..,
            ] if namespaces == "namespaces"
                && task_queues == "task-queues"
                && nexus_services == "nexus-services" =>
            {
                let namespace = decode_outer_segment(namespace)?;
                let task_queue = decode_outer_segment(task_queue)?;
                if namespace.len() > MAX_NEXUS_ID_LENGTH {
                    return Err(HandlerError::bad_request("Namespace length exceeds limit."));
                }
                Ok(PreprocessedTarget::Namespace(UnresolvedNamespaceTarget {
                    namespace,
                    task_queue,
                    operation_path: rest.to_vec(),
                }))
            }
            [nexus, endpoints, endpoint_id, services, rest @ ..]
                if nexus == "nexus" && endpoints == "endpoints" && services == "services" =>
            {
                let endpoint_id = decode_outer_segment(endpoint_id)?;
                let endpoint = match self.endpoints.get(&endpoint_id) {
                    Ok(endpoint) => endpoint,
                    Err(NexusEndpointStoreError::NotFound(_)) => {
                        return Err(HandlerError::not_found("nexus endpoint not found"));
                    }
                    Err(_) => return Err(HandlerError::internal("internal error")),
                };
                let NexusEndpointSpecTarget::Worker {
                    namespace_id: target_namespace_id,
                    task_queue,
                    ..
                } = endpoint.spec.target
                else {
                    return Err(HandlerError::bad_request("invalid endpoint target"));
                };
                let namespace = self
                    .namespaces
                    .get_by_id(&target_namespace_id)
                    .await
                    .map_err(|_| HandlerError::internal("internal error"))?
                    .filter(|namespace| !namespace.deleted)
                    .ok_or_else(|| {
                        HandlerError::not_found("invalid endpoint target").with_retryable(true)
                    })?;
                Ok(PreprocessedTarget::Endpoint(ResolvedTarget {
                    namespace_id: namespace_id(&namespace),
                    namespace: namespace.name,
                    task_queue,
                    endpoint_name: Some(endpoint.spec.name),
                    api_name: ENDPOINT_API,
                    operation_path: rest.to_vec(),
                }))
            }
            _ => Err(HandlerError::not_found("not found")),
        }
    }
}

fn namespace_id(namespace: &ResolvedNamespace) -> NamespaceId {
    namespace
        .namespace_id
        .as_deref()
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
        .map(NamespaceId)
        .unwrap_or_else(|| crate::translate::to_internal::namespace_id_for(&namespace.name))
}

fn raw_segments(path: &str) -> Vec<String> {
    path.trim_matches('/')
        .split('/')
        .map(str::to_owned)
        .collect()
}

fn decode_outer_segment(segment: &str) -> Result<String, HandlerError> {
    percent_decode_str(segment)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|_| HandlerError::bad_request("invalid URL"))
}

fn decode_operation_segment(segment: &str) -> Result<String, HandlerError> {
    percent_decode_str(segment)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|_| HandlerError::bad_request("failed to parse URL path"))
}

fn parse_operation(
    request: &NexusHttpRequest,
    path: &[String],
) -> Result<ParsedOperation, HandlerError> {
    if request.method != "POST" {
        return Err(HandlerError::bad_request(format!(
            "invalid request method: expected POST, got {:?}",
            request.method
        )));
    }
    if path.len() < 2 {
        return Err(HandlerError::not_found("not found"));
    }
    let service = decode_operation_segment(&path[0])?;
    let operation = decode_operation_segment(&path[1])?;
    let request_timeout = parse_request_timeout(&request.headers)?;

    if path.len() == 2 {
        return Ok(ParsedOperation::Start {
            service,
            operation,
            links: parse_links(&request.headers)?,
            request_timeout,
        });
    }

    // v1.31.0 still accepts the deprecated token-in-path form. It is a
    // separate route, so the path token wins outright rather than participating
    // in the current route's header→query fallback.
    if path.len() == 4 && path[3] == "cancel" {
        return Ok(ParsedOperation::Cancel {
            service,
            operation,
            operation_token: decode_operation_segment(&path[2])?,
            request_timeout,
            token_from_header: false,
        });
    }

    if path.len() != 3 || path[2] != "cancel" {
        return Err(HandlerError::not_found("not found"));
    }
    if let Some(token) = header_value(&request.headers, OPERATION_TOKEN_HEADER) {
        return Ok(ParsedOperation::Cancel {
            service,
            operation,
            operation_token: token.to_owned(),
            request_timeout,
            token_from_header: true,
        });
    }
    let token = query_value(request.query.as_deref(), "token")
        .filter(|token| !token.is_empty())
        .ok_or_else(|| HandlerError::bad_request("missing operation token"))?;
    Ok(ParsedOperation::Cancel {
        service,
        operation,
        operation_token: token,
        request_timeout,
        token_from_header: false,
    })
}

fn build_worker_request(
    request: &NexusHttpRequest,
    operation: &ParsedOperation,
    target: &ResolvedTarget,
    scheduled_time: OffsetDateTime,
    caller_failure_support: bool,
) -> NexusHttpTaskRequest {
    let (header, variant) = match operation {
        ParsedOperation::Start {
            service,
            operation,
            links,
            ..
        } => {
            let header = general_headers(&request.headers, &["content-", "nexus-callback-"], None);
            let start = NexusHttpTaskRequestVariant::StartOperation {
                service: service.clone(),
                operation: operation.clone(),
                request_id: header_value(&request.headers, "nexus-request-id")
                    .unwrap_or_default()
                    .to_owned(),
                callback: query_value(request.query.as_deref(), "callback").unwrap_or_default(),
                payload: None,
                callback_header: prefixed_headers(&request.headers, "nexus-callback-"),
                links: links.clone(),
            };
            (header, start)
        }
        ParsedOperation::Cancel {
            service,
            operation,
            operation_token,
            token_from_header,
            ..
        } => {
            let excluded_header = token_from_header.then_some(OPERATION_TOKEN_HEADER);
            let header = general_headers(&request.headers, &[], excluded_header);
            let cancel = NexusHttpTaskRequestVariant::CancelOperation {
                service: service.clone(),
                operation: operation.clone(),
                operation_id: operation_token.clone(),
                operation_token: operation_token.clone(),
            };
            (header, cancel)
        }
    };
    NexusHttpTaskRequest {
        header,
        scheduled_time,
        temporal_failure_responses: caller_failure_support,
        endpoint: target.endpoint_name.clone().unwrap_or_default(),
        dispatch_deadline: scheduled_time,
        variant,
    }
}

fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn general_headers(
    headers: &[(String, String)],
    excluded_prefixes: &[&str],
    excluded_header: Option<&str>,
) -> BTreeMap<String, String> {
    let mut result = BTreeMap::new();
    for (name, value) in headers {
        let name = name.to_ascii_lowercase();
        // This Temporal-private capability controls the response envelope and
        // is represented in Request.capabilities instead. It is never worker
        // input (`tests/nexus_api_test.go @ v1.31.0`).
        if name == FAILURE_SUPPORT_HEADER
            || excluded_header.is_some_and(|excluded| name == excluded)
            || excluded_prefixes
                .iter()
                .any(|prefix| name.starts_with(prefix))
        {
            continue;
        }
        result.entry(name).or_insert_with(|| value.clone());
    }
    result
}

fn prefixed_headers(headers: &[(String, String)], prefix: &str) -> BTreeMap<String, String> {
    let mut result = BTreeMap::new();
    for (name, value) in headers {
        let name = name.to_ascii_lowercase();
        if let Some(name) = name.strip_prefix(prefix) {
            result
                .entry(name.to_owned())
                .or_insert_with(|| value.clone());
        }
    }
    result
}

fn query_value(query: Option<&str>, name: &str) -> Option<String> {
    form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
}

fn parse_request_timeout(headers: &[(String, String)]) -> Result<Option<Duration>, HandlerError> {
    let Some(value) = header_value(headers, REQUEST_TIMEOUT_HEADER) else {
        return Ok(None);
    };
    let (number, unit) = if let Some(number) = value.strip_suffix("ms") {
        (number, 0.001)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1.0)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60.0)
    } else {
        return Err(HandlerError::bad_request("invalid request timeout header"));
    };
    if number.is_empty()
        || !number
            .chars()
            .enumerate()
            .all(|(index, character)| character.is_ascii_digit() || (character == '.' && index > 0))
        || number.matches('.').count() > 1
        || number.ends_with('.')
    {
        return Err(HandlerError::bad_request("invalid request timeout header"));
    }
    let seconds = number
        .parse::<f64>()
        .map_err(|_| HandlerError::bad_request("invalid request timeout header"))?
        * unit;
    Ok(Some(Duration::seconds_f64(seconds)))
}

fn dispatch_deadline(
    request_started: OffsetDateTime,
    request_timeout: Option<Duration>,
    now: OffsetDateTime,
) -> OffsetDateTime {
    let dispatch_deadline = now + DISPATCH_TIMEOUT;
    request_timeout
        .filter(|timeout| timeout.is_positive())
        .map(|timeout| (request_started + timeout - CALLER_DEADLINE_BUFFER).max(now))
        .map_or(dispatch_deadline, |caller_deadline| {
            caller_deadline.min(dispatch_deadline)
        })
}

fn parse_links(headers: &[(String, String)]) -> Result<Vec<NexusTaskLink>, HandlerError> {
    let values = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("nexus-link"))
        .map(|(_, value)| value.as_str())
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Ok(Vec::new());
    }
    values
        .join(",")
        .split(',')
        .map(parse_link)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| HandlerError::bad_request("invalid \"nexus-link\" header"))
}

fn parse_link(value: &str) -> Result<NexusTaskLink, ()> {
    let value = value.trim();
    if !value.starts_with('<') {
        return Err(());
    }
    let end = value.find('>').ok_or(())?;
    let url = value[1..end].trim();
    validate_link_url(url)?;
    let parameters = value[end + 1..].split(';').collect::<Vec<_>>();
    if parameters.len() < 2 || !parameters[0].trim().is_empty() {
        return Err(());
    }
    let mut link_type = None;
    for parameter in &parameters[1..] {
        let parameter = parameter.trim();
        if parameter.is_empty() {
            return Err(());
        }
        let (key, mut value) = parameter.split_once('=').ok_or(())?;
        let key = key.trim();
        value = value.trim();
        if value.starts_with('"') != value.ends_with('"') {
            return Err(());
        }
        if value.starts_with('"') {
            value = &value[1..value.len() - 1];
        }
        if key == "type" {
            validate_link_type(value)?;
            link_type = Some(value.to_owned());
        }
    }
    Ok(NexusTaskLink {
        url: url.to_owned(),
        link_type: link_type.ok_or(())?,
    })
}

fn validate_link_url(value: &str) -> Result<(), ()> {
    if value.is_empty() {
        return Err(());
    }
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return Err(());
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    Ok(())
}

fn validate_link_type(value: &str) -> Result<(), ()> {
    if value.is_empty()
        || !value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '.' | '/')
        })
    {
        return Err(());
    }
    Ok(())
}

fn encode_links(links: &[nexus_v1::Link]) -> Result<Vec<(String, String)>, ()> {
    links
        .iter()
        .map(|link| {
            validate_link_url(&link.url)?;
            validate_link_type(&link.r#type)?;
            Ok((
                LINK_HEADER.to_owned(),
                format!("<{}>; type=\"{}\"", link.url, link.r#type),
            ))
        })
        .collect()
}

fn http_body_to_payload(request: &NexusHttpRequest) -> Result<common_proto::Payload, ()> {
    let mut content = prefixed_headers(&request.headers, "content-");
    content.remove("encoding");
    content.remove("length");
    let content_type = content.get("type").cloned().unwrap_or_default();
    if content.len() > 1 {
        return Ok(unknown_content_payload(content, &request.body));
    }
    if content_type.is_empty() {
        if content.is_empty() && request.body.is_empty() {
            return Ok(payload_with_encoding("binary/null", Vec::new()));
        }
        return Ok(unknown_content_payload(content, &request.body));
    }
    let Some((media_type, parameters)) = parse_media_type(&content_type) else {
        return Ok(unknown_content_payload(content, &request.body));
    };
    match media_type.as_str() {
        "application/x-temporal-payload" => {
            common_proto::Payload::decode(request.body.as_slice()).map_err(|_| ())
        }
        "application/json" if parameters.is_empty() => {
            Ok(payload_with_encoding("json/plain", request.body.clone()))
        }
        "application/json"
            if parameters.len() == 2
                && parameters
                    .get("format")
                    .is_some_and(|value| value == "protobuf")
                && parameters
                    .get("message-type")
                    .is_some_and(|value| !value.is_empty()) =>
        {
            Ok(payload_with_message_type(
                "json/protobuf",
                parameters["message-type"].clone(),
                request.body.clone(),
            ))
        }
        "application/x-protobuf"
            if parameters.len() == 1
                && parameters
                    .get("message-type")
                    .is_some_and(|value| !value.is_empty()) =>
        {
            Ok(payload_with_message_type(
                "binary/protobuf",
                parameters["message-type"].clone(),
                request.body.clone(),
            ))
        }
        "application/octet-stream" if parameters.is_empty() => {
            Ok(payload_with_encoding("binary/plain", request.body.clone()))
        }
        _ => Ok(unknown_content_payload(content, &request.body)),
    }
}

fn parse_media_type(value: &str) -> Option<(String, BTreeMap<String, String>)> {
    let mut parts = value.split(';');
    let media_type = parts.next()?.trim().to_ascii_lowercase();
    if media_type.is_empty() || !media_type.contains('/') {
        return None;
    }
    let mut parameters = BTreeMap::new();
    for part in parts {
        let (name, mut value) = part.trim().split_once('=')?;
        let name = name.trim().to_ascii_lowercase();
        value = value.trim();
        if value.starts_with('"') != value.ends_with('"') {
            return None;
        }
        if value.starts_with('"') {
            value = &value[1..value.len() - 1];
        }
        parameters.insert(name, value.to_owned());
    }
    Some((media_type, parameters))
}

fn payload_with_encoding(encoding: &str, data: Vec<u8>) -> common_proto::Payload {
    common_proto::Payload {
        metadata: BTreeMap::from([("encoding".to_owned(), encoding.as_bytes().to_vec())]),
        data,
        external_payloads: Vec::new(),
    }
}

fn payload_with_message_type(
    encoding: &str,
    message_type: String,
    data: Vec<u8>,
) -> common_proto::Payload {
    common_proto::Payload {
        metadata: BTreeMap::from([
            ("encoding".to_owned(), encoding.as_bytes().to_vec()),
            ("messageType".to_owned(), message_type.into_bytes()),
        ]),
        data,
        external_payloads: Vec::new(),
    }
}

fn unknown_content_payload(
    headers: BTreeMap<String, String>,
    data: &[u8],
) -> common_proto::Payload {
    let mut metadata = headers
        .into_iter()
        .map(|(key, value)| (key, value.into_bytes()))
        .collect::<BTreeMap<_, _>>();
    metadata.insert("encoding".to_owned(), b"unknown/nexus-content".to_vec());
    common_proto::Payload {
        metadata,
        data: data.to_vec(),
        external_payloads: Vec::new(),
    }
}

fn worker_outcome_to_http(
    outcome: NexusHttpWorkerOutcome,
    operation: &ParsedOperation,
    caller_failure_support: bool,
) -> (NexusHttpResponse, String) {
    match outcome {
        NexusHttpWorkerOutcome::Completed(response) => {
            completed_outcome_to_http(response, operation, caller_failure_support)
        }
        NexusHttpWorkerOutcome::Failed { error, failure } => {
            failed_outcome_to_http(error, failure, caller_failure_support)
        }
    }
}

fn completed_outcome_to_http(
    response: nexus_v1::Response,
    operation: &ParsedOperation,
    caller_failure_support: bool,
) -> (NexusHttpResponse, String) {
    if !operation.is_start() {
        return (
            NexusHttpResponse {
                status: 202,
                headers: Vec::new(),
                body: Vec::new(),
            },
            "success".to_owned(),
        );
    }
    let Some(nexus_v1::response::Variant::StartOperation(start)) = response.variant else {
        return empty_worker_outcome(caller_failure_support);
    };
    match start.variant {
        Some(nexus_v1::start_operation_response::Variant::SyncSuccess(success)) => {
            let links = match encode_links(&success.links) {
                Ok(links) => links,
                Err(()) => {
                    return (
                        NexusHttpResponse {
                            status: 500,
                            headers: Vec::new(),
                            body: Vec::new(),
                        },
                        "sync_success".to_owned(),
                    );
                }
            };
            let mut response = payload_to_http(success.payload.as_ref());
            response.headers.extend(links);
            (response, "sync_success".to_owned())
        }
        Some(nexus_v1::start_operation_response::Variant::AsyncSuccess(success)) => {
            let links = match encode_links(&success.links) {
                Ok(links) => links,
                Err(()) => {
                    return (
                        NexusHttpResponse {
                            status: 500,
                            headers: Vec::new(),
                            body: Vec::new(),
                        },
                        "async_success".to_owned(),
                    );
                }
            };
            let token = if success.operation_token.is_empty() {
                success.operation_id
            } else {
                success.operation_token
            };
            let mut headers = vec![("Content-Type".to_owned(), "application/json".to_owned())];
            headers.extend(links);
            (
                NexusHttpResponse {
                    status: 201,
                    headers,
                    body: serde_json::to_vec(&json!({"token": token, "state": "running"}))
                        .unwrap_or_default(),
                },
                "async_success".to_owned(),
            )
        }
        Some(nexus_v1::start_operation_response::Variant::OperationError(error)) => {
            let state = error.operation_state;
            let cause = error
                .failure
                .as_ref()
                .map(proto_nexus_failure)
                .unwrap_or_default();
            let failure = operation_error_failure(&state, cause);
            (
                operation_error_response(state, failure, caller_failure_support),
                "operation_error".to_owned(),
            )
        }
        Some(nexus_v1::start_operation_response::Variant::Failure(failure)) => {
            let state = if matches!(
                failure.failure_info,
                Some(failure_proto::failure::FailureInfo::CanceledFailureInfo(_))
            ) {
                "canceled"
            } else {
                "failed"
            };
            let cause = temporal_failure_to_nexus(&failure);
            let failure = operation_error_failure(state, cause);
            (
                operation_error_response(state.to_owned(), failure, caller_failure_support),
                "failure".to_owned(),
            )
        }
        None => empty_worker_outcome(caller_failure_support),
    }
}

fn failed_outcome_to_http(
    error: Option<nexus_v1::HandlerError>,
    failure: Option<failure_proto::Failure>,
    caller_failure_support: bool,
) -> (NexusHttpResponse, String) {
    if let Some(failure) = failure {
        let (error_type, retryable) = nexus_handler_info(&failure)
            .map(|info| {
                (
                    info.r#type.clone(),
                    retry_behavior_override(info.retry_behavior),
                )
            })
            .unwrap_or_else(|| ("INTERNAL".to_owned(), None));
        let response =
            HandlerError::from_failure(error_type.clone(), temporal_failure_to_nexus(&failure))
                .with_retryable_override(retryable)
                .with_worker_source()
                .into_response(caller_failure_support);
        return (response, format!("handler_error:{error_type}"));
    }
    if let Some(error) = error {
        let error_type = error.error_type.clone();
        let cause = error
            .failure
            .as_ref()
            .map(proto_nexus_failure)
            .unwrap_or_default();
        let wrapper = NexusFailure {
            message: String::new(),
            stack_trace: String::new(),
            metadata: BTreeMap::from([("type".to_owned(), "nexus.HandlerError".to_owned())]),
            details: Some(handler_error_details(
                &error_type,
                retry_behavior_override(error.retry_behavior),
                None,
            )),
            cause: Some(Box::new(cause)),
        };
        let response = HandlerError::from_failure(error_type.clone(), wrapper)
            .with_retryable_override(retry_behavior_override(error.retry_behavior))
            .with_worker_source()
            .into_response(caller_failure_support);
        return (response, format!("handler_error:{error_type}"));
    }
    empty_worker_outcome(caller_failure_support)
}

fn empty_worker_outcome(caller_failure_support: bool) -> (NexusHttpResponse, String) {
    (
        HandlerError::internal("empty outcome")
            .with_worker_source()
            .into_response(caller_failure_support),
        "handler_error:EMPTY_OUTCOME".to_owned(),
    )
}

fn payload_to_http(payload: Option<&common_proto::Payload>) -> NexusHttpResponse {
    let mut headers = Vec::new();
    let (content_headers, body) = match payload {
        None => (BTreeMap::new(), Vec::new()),
        Some(payload) => serialize_payload_content(payload),
    };
    for (name, value) in content_headers {
        headers.push((format!("Content-{name}"), value));
    }
    headers.push(("Content-length".to_owned(), body.len().to_string()));
    NexusHttpResponse {
        status: 200,
        headers,
        body,
    }
}

fn serialize_payload_content(
    payload: &common_proto::Payload,
) -> (BTreeMap<String, String>, Vec<u8>) {
    if payload.metadata.is_empty() {
        return x_temporal_payload(payload);
    }
    let encoding = payload
        .metadata
        .get("encoding")
        .map(|value| String::from_utf8_lossy(value).into_owned())
        .unwrap_or_default();
    let message_type = payload
        .metadata
        .get("messageType")
        .map(|value| String::from_utf8_lossy(value).into_owned())
        .unwrap_or_default();
    let mut header = BTreeMap::new();
    match encoding.as_str() {
        "unknown/nexus-content" => {
            for (name, value) in &payload.metadata {
                if name != "encoding" {
                    header.insert(name.clone(), String::from_utf8_lossy(value).into_owned());
                }
            }
        }
        "json/protobuf" if payload.metadata.len() == 2 && !message_type.is_empty() => {
            header.insert(
                "type".to_owned(),
                format!("application/json; format=protobuf; message-type=\"{message_type}\""),
            );
        }
        "binary/protobuf" if payload.metadata.len() == 2 && !message_type.is_empty() => {
            header.insert(
                "type".to_owned(),
                format!("application/x-protobuf; message-type=\"{message_type}\""),
            );
        }
        "json/plain" => {
            header.insert("type".to_owned(), "application/json".to_owned());
        }
        "binary/null" if payload.metadata.len() == 1 => {}
        "binary/plain" if payload.metadata.len() == 1 => {
            header.insert("type".to_owned(), "application/octet-stream".to_owned());
        }
        _ => return x_temporal_payload(payload),
    }
    (header, payload.data.clone())
}

fn x_temporal_payload(payload: &common_proto::Payload) -> (BTreeMap<String, String>, Vec<u8>) {
    (
        BTreeMap::from([(
            "type".to_owned(),
            "application/x-temporal-payload".to_owned(),
        )]),
        payload.encode_to_vec(),
    )
}

fn operation_error_failure(state: &str, cause: NexusFailure) -> NexusFailure {
    NexusFailure {
        message: "operation error".to_owned(),
        stack_trace: String::new(),
        metadata: BTreeMap::from([("type".to_owned(), "nexus.OperationError".to_owned())]),
        details: Some(json!({"state": state})),
        cause: Some(Box::new(cause)),
    }
}

fn operation_error_response(
    state: String,
    mut failure: NexusFailure,
    caller_failure_support: bool,
) -> NexusHttpResponse {
    if !caller_failure_support && let Some(cause) = failure.cause.take() {
        failure = *cause;
    }
    NexusHttpResponse {
        status: 424,
        headers: vec![
            ("Content-Type".to_owned(), "application/json".to_owned()),
            (OPERATION_STATE_HEADER.to_owned(), state),
            (FAILURE_SOURCE_HEADER.to_owned(), "worker".to_owned()),
        ],
        body: serde_json::to_vec(&failure).unwrap_or_default(),
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct NexusFailure {
    #[serde(skip_serializing_if = "String::is_empty")]
    message: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    stack_trace: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    metadata: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cause: Option<Box<NexusFailure>>,
}

fn proto_nexus_failure(failure: &nexus_v1::Failure) -> NexusFailure {
    NexusFailure {
        message: failure.message.clone(),
        stack_trace: failure.stack_trace.clone(),
        metadata: failure.metadata.clone(),
        details: (!failure.details.is_empty())
            .then(|| serde_json::from_slice(&failure.details).ok())
            .flatten(),
        cause: failure
            .cause
            .as_deref()
            .map(proto_nexus_failure)
            .map(Box::new),
    }
}

fn temporal_failure_to_nexus(failure: &failure_proto::Failure) -> NexusFailure {
    let cause = failure
        .cause
        .as_deref()
        .map(temporal_failure_to_nexus)
        .map(Box::new);
    if let Some(info) = nexus_handler_info(failure) {
        let encoded_attributes = failure.encoded_attributes.as_ref().map(|payload| {
            let bytes = serde_json::to_vec(&payload_json(payload)).unwrap_or_default();
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
        });
        return NexusFailure {
            message: failure.message.clone(),
            stack_trace: failure.stack_trace.clone(),
            metadata: BTreeMap::from([("type".to_owned(), "nexus.HandlerError".to_owned())]),
            details: Some(handler_error_details(
                &info.r#type,
                retry_behavior_override(info.retry_behavior),
                encoded_attributes,
            )),
            cause,
        };
    }
    NexusFailure {
        message: failure.message.clone(),
        stack_trace: failure.stack_trace.clone(),
        metadata: BTreeMap::from([(
            "type".to_owned(),
            "temporal.api.failure.v1.Failure".to_owned(),
        )]),
        details: Some(temporal_failure_json(failure, true)),
        cause,
    }
}

fn nexus_handler_info(
    failure: &failure_proto::Failure,
) -> Option<&failure_proto::NexusHandlerFailureInfo> {
    match failure.failure_info.as_ref() {
        Some(failure_proto::failure::FailureInfo::NexusHandlerFailureInfo(info)) => Some(info),
        _ => None,
    }
}

fn retry_behavior_override(value: i32) -> Option<bool> {
    match value {
        1 => Some(true),
        2 => Some(false),
        _ => None,
    }
}

fn handler_error_details(
    error_type: &str,
    retryable: Option<bool>,
    encoded_attributes: Option<String>,
) -> JsonValue {
    let mut details = JsonMap::new();
    if !error_type.is_empty() {
        details.insert("type".to_owned(), JsonValue::String(error_type.to_owned()));
    }
    if let Some(retryable) = retryable {
        details.insert("retryableOverride".to_owned(), JsonValue::Bool(retryable));
    }
    if let Some(encoded_attributes) = encoded_attributes.filter(|value| !value.is_empty()) {
        details.insert(
            "encodedAttributes".to_owned(),
            JsonValue::String(encoded_attributes),
        );
    }
    JsonValue::Object(details)
}

fn temporal_failure_json(failure: &failure_proto::Failure, clear_message_stack: bool) -> JsonValue {
    let mut object = JsonMap::new();
    if !clear_message_stack && !failure.message.is_empty() {
        object.insert(
            "message".to_owned(),
            JsonValue::String(failure.message.clone()),
        );
    }
    if !failure.source.is_empty() {
        object.insert(
            "source".to_owned(),
            JsonValue::String(failure.source.clone()),
        );
    }
    if !clear_message_stack && !failure.stack_trace.is_empty() {
        object.insert(
            "stackTrace".to_owned(),
            JsonValue::String(failure.stack_trace.clone()),
        );
    }
    if let Some(attributes) = failure.encoded_attributes.as_ref() {
        object.insert("encodedAttributes".to_owned(), payload_json(attributes));
    }
    if let Some(cause) = failure.cause.as_deref() {
        object.insert("cause".to_owned(), temporal_failure_json(cause, false));
    }
    if let Some(info) = failure.failure_info.as_ref() {
        use failure_proto::failure::FailureInfo;
        let (name, value) = match info {
            FailureInfo::ApplicationFailureInfo(info) => {
                let mut value = JsonMap::new();
                insert_string(&mut value, "type", &info.r#type);
                insert_bool(&mut value, "nonRetryable", info.non_retryable);
                insert_payloads(&mut value, "details", info.details.as_ref());
                insert_duration(&mut value, "nextRetryDelay", info.next_retry_delay.as_ref());
                insert_enum::<enums_proto::ApplicationErrorCategory>(
                    &mut value,
                    "category",
                    info.category,
                );
                ("applicationFailureInfo", JsonValue::Object(value))
            }
            FailureInfo::TimeoutFailureInfo(info) => {
                let mut value = JsonMap::new();
                insert_enum::<enums_proto::TimeoutType>(
                    &mut value,
                    "timeoutType",
                    info.timeout_type,
                );
                insert_payloads(
                    &mut value,
                    "lastHeartbeatDetails",
                    info.last_heartbeat_details.as_ref(),
                );
                ("timeoutFailureInfo", JsonValue::Object(value))
            }
            FailureInfo::CanceledFailureInfo(info) => {
                let mut value = JsonMap::new();
                insert_payloads(&mut value, "details", info.details.as_ref());
                insert_string(&mut value, "identity", &info.identity);
                ("canceledFailureInfo", JsonValue::Object(value))
            }
            FailureInfo::TerminatedFailureInfo(info) => {
                let mut value = JsonMap::new();
                insert_string(&mut value, "identity", &info.identity);
                ("terminatedFailureInfo", JsonValue::Object(value))
            }
            FailureInfo::ServerFailureInfo(info) => {
                let mut value = JsonMap::new();
                insert_bool(&mut value, "nonRetryable", info.non_retryable);
                ("serverFailureInfo", JsonValue::Object(value))
            }
            FailureInfo::ResetWorkflowFailureInfo(info) => {
                let mut value = JsonMap::new();
                insert_payloads(
                    &mut value,
                    "lastHeartbeatDetails",
                    info.last_heartbeat_details.as_ref(),
                );
                ("resetWorkflowFailureInfo", JsonValue::Object(value))
            }
            FailureInfo::ActivityFailureInfo(info) => {
                let mut value = JsonMap::new();
                insert_i64(&mut value, "scheduledEventId", info.scheduled_event_id);
                insert_i64(&mut value, "startedEventId", info.started_event_id);
                insert_string(&mut value, "identity", &info.identity);
                if let Some(activity_type) = info.activity_type.as_ref() {
                    value.insert(
                        "activityType".to_owned(),
                        json!({"name": activity_type.name}),
                    );
                }
                insert_string(&mut value, "activityId", &info.activity_id);
                insert_enum::<enums_proto::RetryState>(&mut value, "retryState", info.retry_state);
                ("activityFailureInfo", JsonValue::Object(value))
            }
            FailureInfo::ChildWorkflowExecutionFailureInfo(info) => {
                let mut value = JsonMap::new();
                insert_string(&mut value, "namespace", &info.namespace);
                if let Some(execution) = info.workflow_execution.as_ref() {
                    value.insert(
                        "workflowExecution".to_owned(),
                        json!({
                            "workflowId": execution.workflow_id,
                            "runId": execution.run_id,
                        }),
                    );
                }
                if let Some(workflow_type) = info.workflow_type.as_ref() {
                    value.insert(
                        "workflowType".to_owned(),
                        json!({"name": workflow_type.name}),
                    );
                }
                insert_i64(&mut value, "initiatedEventId", info.initiated_event_id);
                insert_i64(&mut value, "startedEventId", info.started_event_id);
                insert_enum::<enums_proto::RetryState>(&mut value, "retryState", info.retry_state);
                (
                    "childWorkflowExecutionFailureInfo",
                    JsonValue::Object(value),
                )
            }
            FailureInfo::NexusOperationExecutionFailureInfo(info) => {
                let mut value = JsonMap::new();
                insert_i64(&mut value, "scheduledEventId", info.scheduled_event_id);
                insert_string(&mut value, "endpoint", &info.endpoint);
                insert_string(&mut value, "service", &info.service);
                insert_string(&mut value, "operation", &info.operation);
                insert_string(&mut value, "operationId", &info.operation_id);
                insert_string(&mut value, "operationToken", &info.operation_token);
                (
                    "nexusOperationExecutionFailureInfo",
                    JsonValue::Object(value),
                )
            }
            FailureInfo::NexusHandlerFailureInfo(info) => {
                let mut value = JsonMap::new();
                insert_string(&mut value, "type", &info.r#type);
                insert_enum::<enums_proto::NexusHandlerErrorRetryBehavior>(
                    &mut value,
                    "retryBehavior",
                    info.retry_behavior,
                );
                ("nexusHandlerFailureInfo", JsonValue::Object(value))
            }
        };
        object.insert(name.to_owned(), value);
    }
    JsonValue::Object(object)
}

fn payload_json(payload: &common_proto::Payload) -> JsonValue {
    let mut object = JsonMap::new();
    if !payload.metadata.is_empty() {
        object.insert(
            "metadata".to_owned(),
            JsonValue::Object(
                payload
                    .metadata
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.clone(),
                            JsonValue::String(
                                base64::engine::general_purpose::STANDARD.encode(value),
                            ),
                        )
                    })
                    .collect(),
            ),
        );
    }
    if !payload.data.is_empty() {
        object.insert(
            "data".to_owned(),
            JsonValue::String(base64::engine::general_purpose::STANDARD.encode(&payload.data)),
        );
    }
    if !payload.external_payloads.is_empty() {
        object.insert(
            "externalPayloads".to_owned(),
            JsonValue::Array(
                payload
                    .external_payloads
                    .iter()
                    .map(|details| json!({"sizeBytes": details.size_bytes.to_string()}))
                    .collect(),
            ),
        );
    }
    JsonValue::Object(object)
}

fn insert_payloads(
    object: &mut JsonMap<String, JsonValue>,
    name: &str,
    payloads: Option<&common_proto::Payloads>,
) {
    if let Some(payloads) = payloads {
        object.insert(
            name.to_owned(),
            json!({
                "payloads": payloads.payloads.iter().map(payload_json).collect::<Vec<_>>()
            }),
        );
    }
}

fn insert_duration(
    object: &mut JsonMap<String, JsonValue>,
    name: &str,
    duration: Option<&prost_types::Duration>,
) {
    if let Some(duration) = duration {
        let value = if duration.nanos == 0 {
            format!("{}s", duration.seconds)
        } else {
            let fractional = format!("{:09}", duration.nanos.unsigned_abs());
            let fractional = fractional.trim_end_matches('0');
            if duration.seconds == 0 && duration.nanos < 0 {
                format!("-0.{fractional}s")
            } else {
                format!("{}.{fractional}s", duration.seconds)
            }
        };
        object.insert(name.to_owned(), JsonValue::String(value));
    }
}

fn insert_string(object: &mut JsonMap<String, JsonValue>, name: &str, value: &str) {
    if !value.is_empty() {
        object.insert(name.to_owned(), JsonValue::String(value.to_owned()));
    }
}

fn insert_bool(object: &mut JsonMap<String, JsonValue>, name: &str, value: bool) {
    if value {
        object.insert(name.to_owned(), JsonValue::Bool(value));
    }
}

fn insert_i64(object: &mut JsonMap<String, JsonValue>, name: &str, value: i64) {
    if value != 0 {
        object.insert(name.to_owned(), JsonValue::String(value.to_string()));
    }
}

trait ProtoEnumName: TryFrom<i32> {
    fn proto_name(&self) -> &'static str;
}

impl ProtoEnumName for enums_proto::ApplicationErrorCategory {
    fn proto_name(&self) -> &'static str {
        self.as_str_name()
    }
}

impl ProtoEnumName for enums_proto::TimeoutType {
    fn proto_name(&self) -> &'static str {
        self.as_str_name()
    }
}

impl ProtoEnumName for enums_proto::RetryState {
    fn proto_name(&self) -> &'static str {
        self.as_str_name()
    }
}

impl ProtoEnumName for enums_proto::NexusHandlerErrorRetryBehavior {
    fn proto_name(&self) -> &'static str {
        self.as_str_name()
    }
}

fn insert_enum<E>(object: &mut JsonMap<String, JsonValue>, name: &str, value: i32)
where
    E: ProtoEnumName,
{
    if value != 0
        && let Ok(value) = E::try_from(value)
    {
        object.insert(
            name.to_owned(),
            JsonValue::String(value.proto_name().to_owned()),
        );
    }
}

fn record_terminal(
    target: &ResolvedTarget,
    endpoint: &str,
    method: &str,
    outcome: &str,
    duration: StdDuration,
) {
    record_nexus_request(&target.namespace, method, outcome, endpoint);
    record_nexus_latency(&target.namespace, method, outcome, endpoint, duration);
    record_service_request(&target.namespace, method);
}

#[derive(Clone, Debug, PartialEq)]
struct HandlerError {
    error_type: String,
    message: String,
    failure: Option<NexusFailure>,
    retryable_override: Option<bool>,
    worker_source: bool,
}

impl HandlerError {
    fn new(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error_type: error_type.into(),
            message: message.into(),
            failure: None,
            retryable_override: None,
            worker_source: false,
        }
    }

    fn from_failure(error_type: impl Into<String>, failure: NexusFailure) -> Self {
        Self {
            error_type: error_type.into(),
            message: String::new(),
            failure: Some(failure),
            retryable_override: None,
            worker_source: false,
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new("BAD_REQUEST", message)
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self::new("UNAUTHORIZED", message)
    }

    fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new("UNAUTHENTICATED", message)
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::new("NOT_FOUND", message)
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new("INTERNAL", message)
    }

    fn upstream_timeout(message: impl Into<String>) -> Self {
        Self::new("UPSTREAM_TIMEOUT", message)
    }

    fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable_override = Some(retryable);
        self
    }

    fn with_retryable_override(mut self, retryable: Option<bool>) -> Self {
        self.retryable_override = retryable;
        self
    }

    fn with_worker_source(mut self) -> Self {
        self.worker_source = true;
        self
    }

    fn into_response(self, caller_failure_support: bool) -> NexusHttpResponse {
        let status = match self.error_type.as_str() {
            "BAD_REQUEST" => 400,
            "REQUEST_TIMEOUT" => 408,
            "CONFLICT" => 409,
            "UNAUTHENTICATED" => 401,
            "UNAUTHORIZED" => 403,
            "NOT_FOUND" => 404,
            "RESOURCE_EXHAUSTED" => 429,
            "INTERNAL" => 500,
            "NOT_IMPLEMENTED" => 501,
            "UNAVAILABLE" => 503,
            "UPSTREAM_TIMEOUT" => 520,
            _ => 500,
        };
        let mut failure = self.failure.unwrap_or_else(|| NexusFailure {
            message: self.message,
            stack_trace: String::new(),
            metadata: BTreeMap::from([("type".to_owned(), "nexus.HandlerError".to_owned())]),
            details: Some(handler_error_details(
                &self.error_type,
                self.retryable_override,
                None,
            )),
            cause: None,
        });
        if !caller_failure_support && let Some(cause) = failure.cause.take() {
            failure = *cause;
        }
        let mut headers = vec![("Content-Type".to_owned(), "application/json".to_owned())];
        if let Some(retryable) = self.retryable_override {
            headers.push((RETRYABLE_HEADER.to_owned(), retryable.to_string()));
        }
        if self.worker_source {
            headers.push((FAILURE_SOURCE_HEADER.to_owned(), "worker".to_owned()));
        }
        NexusHttpResponse {
            status,
            headers,
            body: serde_json::to_vec(&failure).unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tokeira_runtime::nexus::InMemoryNexusEndpointStore;

    use crate::namespace_cache::{InMemoryNamespaceCache, ResolvedNamespace};

    use super::*;

    #[derive(Debug)]
    struct RecordingAuthorizer {
        decision: NexusAuthorizationDecision,
        targets: Mutex<Vec<NexusAuthorizationTarget>>,
    }

    #[async_trait]
    impl NexusHttpAuthorizer for RecordingAuthorizer {
        async fn authorize(
            &self,
            target: &NexusAuthorizationTarget,
            _headers: &[(String, String)],
        ) -> NexusAuthorizationDecision {
            self.targets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(target.clone());
            self.decision.clone()
        }
    }

    fn start_request(path: String, body: Vec<u8>) -> NexusHttpRequest {
        NexusHttpRequest {
            method: "POST".to_owned(),
            path,
            query: None,
            headers: vec![("Content-Type".to_owned(), "application/json".to_owned())],
            body,
            body_too_large: false,
            body_read_failed: false,
        }
    }

    #[test]
    fn handler_error_is_nexusrpc_failure() {
        let response = HandlerError::bad_request("bad input").into_response(false);
        assert_eq!(response.status, 400);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response.body)
                .expect("failure must be JSON"),
            serde_json::json!({
                "message": "bad input",
                "metadata": {"type": "nexus.HandlerError"},
                "details": {"type": "BAD_REQUEST"}
            })
        );
    }

    #[test]
    fn route_recognition_does_not_capture_grpc() {
        assert!(NexusHttpHandler::recognizes_path(
            "/namespaces/default/task-queues/tq/nexus-services/svc/op"
        ));
        assert!(NexusHttpHandler::recognizes_path(
            "/nexus/endpoints/id/services/svc/op"
        ));
        assert!(!NexusHttpHandler::recognizes_path(
            "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"
        ));
    }

    #[test]
    fn request_timeout_parser_matches_nexus_duration_grammar() {
        for (value, milliseconds) in [("1ms", 1), ("1.5s", 1_500), ("2m", 120_000)] {
            let headers = vec![("Request-Timeout".to_owned(), value.to_owned())];
            assert_eq!(
                parse_request_timeout(&headers)
                    .expect("valid timeout")
                    .expect("timeout present")
                    .whole_milliseconds(),
                milliseconds
            );
        }
        let headers = vec![("Request-Timeout".to_owned(), "1h".to_owned())];
        assert!(parse_request_timeout(&headers).is_err());
    }

    #[test]
    fn deprecated_cancel_path_uses_its_path_token() {
        let request = NexusHttpRequest {
            method: "POST".to_owned(),
            path: String::new(),
            query: Some("token=query".to_owned()),
            headers: vec![(OPERATION_TOKEN_HEADER.to_owned(), "header".to_owned())],
            body: Vec::new(),
            body_too_large: false,
            body_read_failed: false,
        };
        let operation = parse_operation(
            &request,
            &[
                "service".to_owned(),
                "operation".to_owned(),
                "path".to_owned(),
                "cancel".to_owned(),
            ],
        )
        .expect("deprecated route must parse");
        assert!(matches!(
            operation,
            ParsedOperation::Cancel {
                operation_token,
                token_from_header: false,
                ..
            } if operation_token == "path"
        ));
    }

    // Feature: edge-nexus-http-dispatch, Property 1: route recognition and
    // operation parsing preserve every valid caller-selected segment.
    proptest! {
        #[test]
        fn property_route_resolution_and_delegation(
            namespace in "[a-z0-9_-]{1,24}",
            task_queue in "[a-z0-9_-]{1,24}",
            service in "[a-z0-9_-]{1,24}",
            operation in "[a-z0-9_-]{1,24}",
        ) {
            let path = format!(
                "/namespaces/{namespace}/task-queues/{task_queue}/nexus-services/{service}/{operation}"
            );
            prop_assert!(NexusHttpHandler::recognizes_path(&path));
            let segments = raw_segments(&path);
            prop_assert_eq!(decode_outer_segment(&segments[1]).expect("namespace"), namespace);
            prop_assert_eq!(decode_outer_segment(&segments[3]).expect("task queue"), task_queue);

            let request = start_request(path, Vec::new());
            let parsed = parse_operation(&request, &segments[5..]).expect("operation route");
            prop_assert!(
                matches!(
                    parsed,
                    ParsedOperation::Start {
                        service: parsed_service,
                        operation: parsed_operation,
                        ..
                    } if parsed_service == service && parsed_operation == operation
                ),
                "start route fields changed during parsing"
            );
            prop_assert!(!NexusHttpHandler::recognizes_path(
                "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"
            ));
        }
    }

    // Feature: edge-nexus-http-dispatch, Property 2: HTTP Start translation
    // preserves Nexus metadata while keeping content and callback headers scoped.
    proptest! {
        #[test]
        fn property_start_request_translation_preserves_fields(
            service in "[a-z]{1,12}",
            operation in "[a-z]{1,12}",
            request_id in "[a-z0-9_-]{1,16}",
            body in proptest::collection::vec(any::<u8>(), 0..128),
        ) {
            let request = NexusHttpRequest {
                method: "POST".to_owned(),
                path: String::new(),
                query: Some("callback=https%3A%2F%2Fcallback.example%2Fcomplete".to_owned()),
                headers: vec![
                    ("Content-Type".to_owned(), "application/json".to_owned()),
                    ("Nexus-Request-Id".to_owned(), request_id.clone()),
                    ("Nexus-Callback-Token".to_owned(), "callback-secret".to_owned()),
                    (
                        "Temporal-Nexus-Failure-Support".to_owned(),
                        "true".to_owned(),
                    ),
                    ("X-General".to_owned(), "general-value".to_owned()),
                ],
                body: body.clone(),
                body_too_large: false,
                body_read_failed: false,
            };
            let parsed_operation = ParsedOperation::Start {
                service: service.clone(),
                operation: operation.clone(),
                links: vec![NexusTaskLink {
                    url: "temporal://namespace/workflow/id".to_owned(),
                    link_type: "type.example/link".to_owned(),
                }],
                request_timeout: Some(Duration::seconds(10)),
            };
            let target = ResolvedTarget {
                namespace: "default".to_owned(),
                namespace_id: crate::translate::to_internal::namespace_id_for("default"),
                task_queue: "queue".to_owned(),
                endpoint_name: None,
                api_name: NAMESPACE_API,
                operation_path: Vec::new(),
            };
            let mut translated = build_worker_request(
                &request,
                &parsed_operation,
                &target,
                OffsetDateTime::UNIX_EPOCH,
                true,
            );
            let payload = http_body_to_payload(&request).expect("payload");
            if let NexusHttpTaskRequestVariant::StartOperation { payload: slot, .. } =
                &mut translated.variant
            {
                *slot = Some(payload_to_domain(&payload));
            }

            prop_assert_eq!(translated.header.get("x-general"), Some(&"general-value".to_owned()));
            prop_assert!(!translated.header.contains_key("content-type"));
            prop_assert!(!translated.header.contains_key("nexus-callback-token"));
            prop_assert!(!translated.header.contains_key(FAILURE_SUPPORT_HEADER));
            prop_assert!(translated.temporal_failure_responses);
            match translated.variant {
                NexusHttpTaskRequestVariant::StartOperation {
                    service: actual_service,
                    operation: actual_operation,
                    request_id: actual_request_id,
                    callback,
                    payload,
                    callback_header,
                    links,
                } => {
                    prop_assert_eq!(actual_service, service);
                    prop_assert_eq!(actual_operation, operation);
                    prop_assert_eq!(actual_request_id, request_id);
                    prop_assert_eq!(callback, "https://callback.example/complete");
                    prop_assert_eq!(payload.expect("payload").data, body);
                    prop_assert_eq!(callback_header.get("token"), Some(&"callback-secret".to_owned()));
                    prop_assert_eq!(links.len(), 1);
                }
                other => panic!("unexpected request variant: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn authentication_failure_uses_nexus_unauthenticated_shape() {
        let namespaces = Arc::new(InMemoryNamespaceCache::new());
        namespaces
            .insert(ResolvedNamespace::active("default"))
            .await
            .expect("namespace");
        let handler = NexusHttpHandler::new(
            namespaces,
            Arc::new(InMemoryNexusEndpointStore::new()),
            NexusTaskBroker::default(),
            NexusHttpWaiterRegistry::default(),
            Arc::new(RecordingAuthorizer {
                decision: NexusAuthorizationDecision::Unauthenticated,
                targets: Mutex::new(Vec::new()),
            }),
        );
        let response = handler
            .handle(start_request(
                "/namespaces/default/task-queues/queue/nexus-services/service/operation".to_owned(),
                b"\"input\"".to_vec(),
            ))
            .await;

        assert_eq!(response.status, 401);
        let failure: serde_json::Value =
            serde_json::from_slice(&response.body).expect("failure JSON");
        assert_eq!(failure["message"], "unauthorized");
        assert_eq!(failure["details"]["type"], "UNAUTHENTICATED");
    }

    #[test]
    fn converted_payload_limit_counts_public_proto_metadata() {
        let request = start_request(String::new(), vec![b'a'; MAX_NEXUS_PAYLOAD_BYTES - 10]);
        let payload = http_body_to_payload(&request).expect("payload");
        assert!(
            payload.encoded_len() > MAX_NEXUS_PAYLOAD_BYTES,
            "the public Payload representation, not the raw HTTP body, owns the limit"
        );
    }

    // Feature: edge-nexus-http-dispatch, Property 3: current-route cancellation
    // uses header then query, while the deprecated route owns its path token.
    proptest! {
        #[test]
        fn property_cancel_operation_token_precedence(
            header_token in "[a-z0-9_-]{1,16}",
            query_token in "[a-z0-9_-]{1,16}",
            path_token in "[a-z0-9_-]{1,16}",
        ) {
            let mut request = NexusHttpRequest {
                method: "POST".to_owned(),
                path: String::new(),
                query: Some(format!("token={query_token}")),
                headers: vec![(OPERATION_TOKEN_HEADER.to_owned(), header_token.clone())],
                body: Vec::new(),
                body_too_large: false,
                body_read_failed: false,
            };
            let current = parse_operation(
                &request,
                &["service".to_owned(), "operation".to_owned(), "cancel".to_owned()],
            )
            .expect("current cancel route");
            prop_assert!(
                matches!(
                    current,
                    ParsedOperation::Cancel { operation_token, token_from_header: true, .. }
                        if operation_token == header_token
                ),
                "header token did not win on the current route"
            );

            request.headers.clear();
            let query = parse_operation(
                &request,
                &["service".to_owned(), "operation".to_owned(), "cancel".to_owned()],
            )
            .expect("query cancel route");
            prop_assert!(
                matches!(
                    query,
                    ParsedOperation::Cancel { operation_token, token_from_header: false, .. }
                        if operation_token == query_token
                ),
                "query token was not used when the header was absent"
            );

            request.headers.push((OPERATION_TOKEN_HEADER.to_owned(), header_token));
            let deprecated = parse_operation(
                &request,
                &[
                    "service".to_owned(),
                    "operation".to_owned(),
                    path_token.clone(),
                    "cancel".to_owned(),
                ],
            )
            .expect("deprecated cancel route");
            prop_assert!(
                matches!(
                    deprecated,
                    ParsedOperation::Cancel { operation_token, token_from_header: false, .. }
                        if operation_token == path_token
                ),
                "deprecated route did not retain its path token"
            );
        }
    }

    // Feature: edge-nexus-http-dispatch, Property 4: a waiter accepts exactly
    // one outcome and cannot be completed again after delivery.
    proptest! {
        #[test]
        fn property_http_waiter_single_delivery(operation_token in "[a-z0-9_-]{1,24}") {
            let runtime = tokio::runtime::Runtime::new().expect("runtime");
            runtime.block_on(async move {
                let registry = NexusHttpWaiterRegistry::default();
                let mut waiter = registry.register();
                let outcome = NexusHttpWorkerOutcome::Completed(nexus_v1::Response {
                    variant: Some(nexus_v1::response::Variant::StartOperation(
                        nexus_v1::StartOperationResponse {
                            variant: Some(
                                nexus_v1::start_operation_response::Variant::AsyncSuccess(
                                    nexus_v1::start_operation_response::Async {
                                        operation_token,
                                        ..Default::default()
                                    },
                                ),
                            ),
                        },
                    )),
                });
                prop_assert!(registry.complete(waiter.id(), outcome.clone()));
                prop_assert!(!registry.complete(waiter.id(), outcome.clone()));
                prop_assert_eq!(waiter.outcome().await.expect("outcome"), outcome);
                Ok(())
            })?;
        }
    }

    // Feature: edge-nexus-http-dispatch, Property 5: every completed/failed
    // worker outcome maps to the v1.31 HTTP status, headers, body, and outcome family.
    proptest! {
        #[test]
        fn property_worker_outcome_response_serialization(
            payload in proptest::collection::vec(any::<u8>(), 0..128),
            operation_token in "[a-z0-9_-]{1,24}",
            operation_state in "[a-z]{1,12}",
            error_type in prop_oneof![Just("BAD_REQUEST"), Just("UNAVAILABLE"), Just("INTERNAL")],
        ) {
            let operation = ParsedOperation::Start {
                service: "service".to_owned(),
                operation: "operation".to_owned(),
                links: Vec::new(),
                request_timeout: None,
            };
            let sync = nexus_v1::Response {
                variant: Some(nexus_v1::response::Variant::StartOperation(
                    nexus_v1::StartOperationResponse {
                        variant: Some(
                            nexus_v1::start_operation_response::Variant::SyncSuccess(
                                nexus_v1::start_operation_response::Sync {
                                    payload: Some(payload_with_encoding("binary/plain", payload.clone())),
                                    links: Vec::new(),
                                },
                            ),
                        ),
                    },
                )),
            };
            let (sync_response, sync_outcome) =
                completed_outcome_to_http(sync, &operation, false);
            prop_assert_eq!(sync_response.status, 200);
            prop_assert_eq!(sync_response.body, payload);
            prop_assert_eq!(sync_outcome, "sync_success");

            let asynchronous = nexus_v1::Response {
                variant: Some(nexus_v1::response::Variant::StartOperation(
                    nexus_v1::StartOperationResponse {
                        variant: Some(
                            nexus_v1::start_operation_response::Variant::AsyncSuccess(
                                nexus_v1::start_operation_response::Async {
                                    operation_token: operation_token.clone(),
                                    ..Default::default()
                                },
                            ),
                        ),
                    },
                )),
            };
            let (async_response, async_outcome) =
                completed_outcome_to_http(asynchronous, &operation, false);
            prop_assert_eq!(async_response.status, 201);
            let body: serde_json::Value =
                serde_json::from_slice(&async_response.body).expect("async response JSON");
            prop_assert_eq!(body["token"].as_str(), Some(operation_token.as_str()));
            prop_assert_eq!(body["state"].as_str(), Some("running"));
            prop_assert_eq!(async_outcome, "async_success");

            let operation_error = nexus_v1::Response {
                variant: Some(nexus_v1::response::Variant::StartOperation(
                    nexus_v1::StartOperationResponse {
                        variant: Some(
                            nexus_v1::start_operation_response::Variant::OperationError(
                                nexus_v1::UnsuccessfulOperationError {
                                    operation_state: operation_state.clone(),
                                    failure: Some(nexus_v1::Failure {
                                        message: "failed".to_owned(),
                                        details: b"{}".to_vec(),
                                        ..Default::default()
                                    }),
                                },
                            ),
                        ),
                    },
                )),
            };
            let (error_response, error_outcome) =
                completed_outcome_to_http(operation_error, &operation, true);
            prop_assert_eq!(error_response.status, 424);
            let carries_operation_state = error_response.headers.iter().any(|(name, value)| {
                name == OPERATION_STATE_HEADER && value == &operation_state
            });
            prop_assert!(carries_operation_state);
            prop_assert_eq!(error_outcome, "operation_error");

            let temporal_failure = failure_proto::Failure {
                message: "failed".to_owned(),
                ..Default::default()
            };
            let direct_failure = nexus_v1::Response {
                variant: Some(nexus_v1::response::Variant::StartOperation(
                    nexus_v1::StartOperationResponse {
                        variant: Some(
                            nexus_v1::start_operation_response::Variant::Failure(
                                temporal_failure.clone(),
                            ),
                        ),
                    },
                )),
            };
            let (failure_response, failure_outcome) =
                completed_outcome_to_http(direct_failure, &operation, true);
            prop_assert_eq!(failure_response.status, 424);
            prop_assert_eq!(failure_outcome, "failure");

            let cancel_operation = ParsedOperation::Cancel {
                service: "service".to_owned(),
                operation: "operation".to_owned(),
                operation_token: operation_token.clone(),
                request_timeout: None,
                token_from_header: true,
            };
            let (cancel_response, cancel_outcome) = completed_outcome_to_http(
                nexus_v1::Response {
                    variant: Some(nexus_v1::response::Variant::CancelOperation(
                        nexus_v1::CancelOperationResponse {},
                    )),
                },
                &cancel_operation,
                false,
            );
            prop_assert_eq!(cancel_response.status, 202);
            prop_assert_eq!(cancel_outcome, "success");

            let modern_failure = failure_proto::Failure {
                message: "handler failed".to_owned(),
                failure_info: Some(
                    failure_proto::failure::FailureInfo::NexusHandlerFailureInfo(
                        failure_proto::NexusHandlerFailureInfo {
                            r#type: error_type.to_owned(),
                            retry_behavior: 0,
                        },
                    ),
                ),
                ..Default::default()
            };
            let (modern_response, modern_outcome) =
                failed_outcome_to_http(None, Some(modern_failure), true);
            let modern_failure_source = modern_response.headers.iter().any(|(name, value)| {
                name == FAILURE_SOURCE_HEADER && value == "worker"
            });
            prop_assert!(modern_failure_source);
            prop_assert_eq!(modern_outcome, format!("handler_error:{error_type}"));

            let (deprecated_response, deprecated_outcome) = failed_outcome_to_http(
                Some(nexus_v1::HandlerError {
                    error_type: error_type.to_owned(),
                    failure: Some(nexus_v1::Failure::default()),
                    retry_behavior: 0,
                }),
                None,
                false,
            );
            let deprecated_failure_source =
                deprecated_response.headers.iter().any(|(name, value)| {
                name == FAILURE_SOURCE_HEADER && value == "worker"
            });
            prop_assert!(deprecated_failure_source);
            prop_assert_eq!(deprecated_outcome, format!("handler_error:{error_type}"));

            let (empty_response, empty_outcome) =
                completed_outcome_to_http(nexus_v1::Response::default(), &operation, false);
            prop_assert_eq!(empty_response.status, 500);
            prop_assert_eq!(empty_outcome, "handler_error:EMPTY_OUTCOME");
        }
    }

    // Feature: edge-nexus-http-dispatch, Property 6: authorization observes
    // the resolved target before invalid input can be converted or published.
    proptest! {
        #[test]
        fn property_authorization_precedes_payload_and_publication(
            reason in "[a-z ]{0,24}",
        ) {
            let runtime = tokio::runtime::Runtime::new().expect("runtime");
            runtime.block_on(async move {
                let namespaces = Arc::new(InMemoryNamespaceCache::new());
                namespaces
                    .insert(ResolvedNamespace::active("default"))
                    .await
                    .expect("namespace");
                let broker = NexusTaskBroker::default();
                let authorizer = Arc::new(RecordingAuthorizer {
                    decision: NexusAuthorizationDecision::Deny {
                        reason: Some(reason.clone()),
                    },
                    targets: Mutex::new(Vec::new()),
                });
                let handler = NexusHttpHandler::new(
                    namespaces,
                    Arc::new(InMemoryNexusEndpointStore::new()),
                    broker.clone(),
                    NexusHttpWaiterRegistry::default(),
                    authorizer.clone(),
                );
                let mut request = start_request(
                    "/namespaces/default/task-queues/queue/nexus-services/service/operation"
                        .to_owned(),
                    Vec::new(),
                );
                request.body_too_large = true;
                let response = handler.handle(request).await;
                let body: serde_json::Value =
                    serde_json::from_slice(&response.body).expect("denial JSON");
                let expected = if reason.is_empty() {
                    "permission denied".to_owned()
                } else {
                    format!("permission denied: {reason}")
                };
                prop_assert_eq!(body["message"].as_str(), Some(expected.as_str()));
                let targets = authorizer
                    .targets
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                prop_assert_eq!(targets.len(), 1);
                prop_assert_eq!(targets[0].namespace.as_str(), "default");
                prop_assert_eq!(targets[0].api_name.as_str(), NAMESPACE_API);
                prop_assert!(
                    broker
                        .poll(
                            crate::translate::to_internal::namespace_id_for("default"),
                            TaskQueueName("queue".to_owned()),
                            tokio::time::Duration::ZERO,
                        )
                        .await
                        .is_none()
                );
                Ok(())
            })?;
        }
    }
}
