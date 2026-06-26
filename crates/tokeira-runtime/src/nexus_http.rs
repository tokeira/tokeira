//! Outbound Nexus HTTP client (`HttpNexusClient`) — the real implementation of
//! the [`crate::nexus::NexusHttpClient`] trait that tokeirad wires for
//! External-target Nexus endpoints.
//!
//! This sits on the **runtime** plane: it is a derived dispatch effect invoked by
//! `RuntimeDispatchPublisher::handle_schedule_nexus_operation` /
//! `handle_cancel_nexus_operation`. It owns no durable state; its outcomes flow
//! back through the existing `NexusResolution` → kernel-event path (history is
//! authority, AGENTS §3).
//!
//! ## Wire contract (ground truth)
//!
//! Every request/response detail here is pinned to Temporal v1.31.0's own client
//! wrapper, which vendors `nexus-rpc/sdk-go v0.6.0`:
//! `common/nexus/nexusrpc/client.go` (StartOperation), `handle.go`
//! (CancelOperation), `api.go` (header names, `Nexus-Link` encode/decode,
//! `FormatDuration`), `payload_serializer.go` (body content-type mapping), and
//! `link_converter.go` (the `temporal://` link URL scheme) — all `@ v1.31.0`.
//! No Rust crate implements the Nexus wire protocol, so this is hand-rolled on
//! `reqwest` (already a workspace dependency); see `.kiro/specs/runtime-nexus-http-client/`.
//!
//! ## Deliberate scope boundaries (Req 5 of the spec)
//!
//! - **No outbound caller links / callback URL.** The `NexusHttpClient` trait does
//!   not carry caller links or a callback target, and tokeira hosts no inbound
//!   completion-callback endpoint yet (deferred surface). The targeted conformance
//!   cluster completes either synchronously (200) or stays started then
//!   cancelled/timed-out, so none of it needs an async callback. Emitting a
//!   callback URL with nowhere to receive it would be worse than omitting it.
//! - **Single attempt.** Retry/backoff is `nexus-retry-policy`, not here.
//! - **`__temporal_system` is internal** (`startOnHistoryService @ v1.31.0`) and is
//!   never routed through this client.

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use opentelemetry::KeyValue;
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use time::Duration;
use tokeira_kernel::{Link, LinkWorkflowEventReference};
use tokeira_types::{Payload, Payloads};
use url::Url;

use crate::nexus::{
    CompletionDeliveryOutcome, NEXUS_OPERATION_STATE_HEADER, NexusCompletion,
    NexusCompletionClient, NexusHttpClient, NexusHttpFailureBody, NexusStartResult,
    TEMPORAL_CALLBACK_TOKEN_HEADER,
};

// Header names and the operation-unsuccessful status are lower-cased wire
// constants from `common/nexus/nexusrpc/api.go @ v1.31.0` (HTTP header names are
// case-insensitive; the SDK normalises to lower case).
const HEADER_REQUEST_ID: &str = "nexus-request-id";
const HEADER_LINK: &str = "nexus-link";
const HEADER_OPERATION_TOKEN: &str = "nexus-operation-token";
/// The Nexus per-request deadline header (`nexus.HeaderRequestTimeout`,
/// `common/nexus/nexusrpc/api.go:134 @ v1.31.0`), carrying the timeout for THIS single
/// start/cancel request (not the whole-operation schedule-to-close). Value is
/// `FormatDuration` = "<ms>ms".
const HEADER_REQUEST_TIMEOUT: &str = "Request-Timeout";

/// Timeout for a single Nexus start/cancel request, pinned to v1.31.0's
/// `component.nexusoperations.request.timeout` default (`components/nexusoperations/config.go:15-18
/// @ v1.31.0`). Config-as-constant: promoted to real config only if it becomes deployment policy.
const NEXUS_REQUEST_TIMEOUT: Duration = Duration::seconds(10);
/// Minimum remaining budget below which a request is not made: a non-retryable timeout is
/// returned instead of dispatching (v1.31.0 `MinRequestTimeout`, `config.go:20-24 @ v1.31.0`).
const NEXUS_MIN_REQUEST_TIMEOUT: Duration = Duration::milliseconds(1500);
/// `nexus-operation-state` header on a completion `POST` (`headerOperationState`,
/// `common/nexus/nexusrpc/api.go:23 @ v1.31.0`). Aliases the public protocol const so
/// the firing client and the inbound `/nexus/callback` server share one source of truth.
const HEADER_OPERATION_STATE: &str = NEXUS_OPERATION_STATE_HEADER;
/// `nexus-request-retryable` response header that overrides the status-derived
/// retryability (`headerRetryable`, `common/nexus/nexusrpc/api.go:28 @ v1.31.0`).
const HEADER_RETRYABLE: &str = "nexus-request-retryable";
/// `User-Agent` value the completion handler requires (`userAgent =
/// "temporalio/server"`, `common/nexus/nexusrpc/client.go:37 @ v1.31.0`); the inbound
/// handler rejects a mismatching agent as `BadRequest`.
const NEXUS_USER_AGENT: &str = "temporalio/server";
/// Content type for a `failed`/`canceled` completion's JSON Nexus failure body
/// (`completion.go` sets `Content-Type: application/json` @ v1.31.0).
const CONTENT_TYPE_JSON: &str = "application/json";
/// 424 Failed Dependency — `statusOperationUnsuccessful @ v1.31.0` (the
/// operation completed as failed or canceled, distinct from a handler error).
const STATUS_OPERATION_UNSUCCESSFUL: u16 = 424;

/// The link `type` string for a workflow-event link: the proto FullName of
/// `Link.WorkflowEvent` (`link_converter.go` uses
/// `we.ProtoReflect().Descriptor().FullName()` @ v1.31.0). Only this link type
/// maps onto the kernel `Link`; other types are observability metadata we do not
/// model and skip.
const LINK_TYPE_WORKFLOW_EVENT: &str = "temporal.api.common.v1.Link.WorkflowEvent";
/// The `temporal://` URL scheme links use (`urlSchemeTemporalKey @ v1.31.0`).
const URL_SCHEME_TEMPORAL: &str = "temporal";
/// Query `referenceType` values (`Descriptor().Name()` of the two reference
/// messages, `link_converter.go @ v1.31.0`).
const EVENT_REFERENCE_TYPE: &str = "EventReference";
const REQUEST_ID_REFERENCE_TYPE: &str = "RequestIdReference";

/// reqwest-backed [`NexusHttpClient`]. The inner client is cheaply cloneable and
/// connection-pools, so a single instance is shared across all dispatches.
pub struct HttpNexusClient {
    http: reqwest::Client,
}

impl Default for HttpNexusClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpNexusClient {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
        }
    }

    #[cfg(test)]
    pub fn with_client(http: reqwest::Client) -> Self {
        Self { http }
    }
}

#[async_trait]
impl NexusHttpClient for HttpNexusClient {
    async fn start_operation(
        &self,
        address: &str,
        operation_id: &str,
        service: &str,
        operation: &str,
        input: &Payloads,
        schedule_to_close_timeout: Option<Duration>,
        trace_headers: &[KeyValue],
    ) -> Result<NexusStartResult> {
        // Path is `{base}/{escape(service)}/{escape(operation)}`
        // (`serviceBaseURL.JoinPath(PathEscape(service), PathEscape(operation)) @
        // v1.31.0`). `path_segments_mut` percent-encodes each pushed segment.
        let mut url =
            Url::parse(address).map_err(|e| anyhow!("invalid nexus endpoint url: {e}"))?;
        url.path_segments_mut()
            .map_err(|_| anyhow!("nexus endpoint url cannot be a base: {address}"))?
            .pop_if_empty()
            .extend([service, operation]);

        let (body, request_content_type) = payload_to_body(input);

        let mut request = self.http.post(url);
        request = request.header(HEADER_REQUEST_ID, operation_id);
        if let Some(ct) = &request_content_type {
            request = request.header(reqwest::header::CONTENT_TYPE, ct);
        }
        // Per-request timeout, DECOUPLED from the operation deadline. A single Nexus request
        // gets RequestTimeout (10s), capped by the remaining schedule-to-close, mirroring
        // v1.31.0's executor (`components/nexusoperations/executors.go:222-275 @ v1.31.0`).
        // Conflating it with schedule-to-close (the whole-operation deadline, which bounds the
        // operation across retries via the nexus_timeout scanner) under-times a single connect:
        // on a dual-stack `localhost` a sub-second budget fires before happy-eyeballs falls back
        // from a dead `::1` to `127.0.0.1`. (Dispatch is prompt, so the remaining deadline is
        // approximated by the full schedule-to-close rather than subtracting elapsed time.)
        // A zero (or absent) schedule-to-close means "unbounded" in Temporal, not a 0ms
        // deadline — cap by it only when positive (v1.31.0 `if scheduleToCloseTimeout > 0`,
        // `executors.go:229-231 @ v1.31.0`); otherwise the full RequestTimeout applies.
        let call_timeout = match schedule_to_close_timeout {
            Some(stc) if stc.is_positive() => stc.min(NEXUS_REQUEST_TIMEOUT),
            _ => NEXUS_REQUEST_TIMEOUT,
        };
        // Below MinRequestTimeout there is not enough budget to make the call; v1.31.0 returns a
        // non-retryable timeout (`operationTimeoutBelowMinError`) without dispatching. (The
        // outbound metric tags this as `unknown-error` rather than `operation-timeout` for now —
        // a narrow metric-fidelity gap on an untested below-floor path, not a behaviour gap.)
        if call_timeout < NEXUS_MIN_REQUEST_TIMEOUT {
            bail!(
                "nexus start: remaining operation timeout {} is below the minimum request timeout {}",
                format_duration_ms(call_timeout),
                format_duration_ms(NEXUS_MIN_REQUEST_TIMEOUT)
            );
        }
        request = request.header(HEADER_REQUEST_TIMEOUT, format_duration_ms(call_timeout));
        if let Ok(std) = call_timeout.try_into() {
            request = request.timeout(std);
        }
        for kv in trace_headers {
            request = request.header(kv.key.as_str(), kv.value.as_str().as_ref());
        }
        request = request.body(body);

        let response = request
            .send()
            .await
            .map_err(|e| anyhow!("nexus start request failed: {}", error_chain(&e)))?;

        let status = response.status();
        // Links are read from response headers up front (mirrors v1.31.0, which
        // parses `Nexus-Link` before branching on status). Header decoding is
        // strict; the `temporal://` → kernel-`Link` conversion is lenient (a
        // non-workflow-event link is skipped, never an error) because dropping an
        // observability link must not strand an otherwise-successful operation.
        let links = parse_response_links(response.headers())?;
        // Capture the response content-type before the body is consumed; it drives
        // how the result bytes decode into a `Payload` (`payload_serializer.go`).
        let response_content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);

        if status.as_u16() == 200 {
            let bytes = response
                .bytes()
                .await
                .map_err(|e| anyhow!("nexus start: reading 200 body failed: {e}"))?;
            return Ok(NexusStartResult::SyncCompleted {
                result: body_to_payloads(&bytes, response_content_type.as_deref()),
                links,
            });
        }

        match status.as_u16() {
            201 => {
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|e| anyhow!("nexus start: reading 201 body failed: {e}"))?;
                let info: OperationInfo = serde_json::from_slice(&bytes).map_err(|e| {
                    anyhow!("nexus start: 201 body is not OperationInfo json: {e} (content-type {response_content_type:?})")
                })?;
                // v1.31.0 requires the async-start state to be exactly `running`
                // and the token to be non-empty (`client.go:330-345 @ v1.31.0`).
                if info.state != "running" {
                    bail!(
                        "nexus start: 201 operation state is {:?}, expected running",
                        info.state
                    );
                }
                if info.token.is_empty() {
                    bail!("nexus start: 201 response carried an empty operation token");
                }
                Ok(NexusStartResult::AsyncAccepted {
                    operation_token: info.token,
                    links,
                })
            }
            STATUS_OPERATION_UNSUCCESSFUL => {
                let bytes = response.bytes().await.unwrap_or_default();
                Ok(NexusStartResult::SyncFailed {
                    message: failure_message(&bytes)
                        .unwrap_or_else(|| "nexus operation failed".to_owned()),
                })
            }
            other => {
                // A non-2xx, non-424 status is a Nexus *handler* error (e.g. 400
                // BAD_REQUEST). Surface its type + body so the publisher resolves the
                // caller's operation as failed AND tags `nexus_outbound_requests` with
                // `handler-error:<TYPE>` (`startCallOutcomeTag @ v1.31.0`). A single
                // attempt only — no retry classification here (Req 5.3).
                let bytes = response.bytes().await.unwrap_or_default();
                // The body is a JSON `nexus.Failure` ({message, metadata, details}) when the
                // handler returned one; decode it (best-effort) so the caller's error chain
                // can be rehydrated. A non-JSON body just yields None.
                let failure = serde_json::from_slice::<NexusHttpFailureBody>(&bytes).ok();
                Ok(NexusStartResult::HandlerError {
                    error_type: handler_error_type_for_status(other).to_string(),
                    failure,
                })
            }
        }
    }

    async fn cancel_operation(
        &self,
        address: &str,
        service: &str,
        operation: &str,
        operation_token: &str,
        trace_headers: &[KeyValue],
    ) -> Result<()> {
        // `serviceBaseURL.JoinPath(escape(service), escape(operation), "cancel")`
        // with the token in the `Nexus-Operation-Token` header
        // (`handle.go:25-30 @ v1.31.0`).
        let mut url =
            Url::parse(address).map_err(|e| anyhow!("invalid nexus endpoint url: {e}"))?;
        url.path_segments_mut()
            .map_err(|_| anyhow!("nexus endpoint url cannot be a base: {address}"))?
            .pop_if_empty()
            .extend([service, operation, "cancel"]);

        let mut request = self
            .http
            .post(url)
            .header(HEADER_OPERATION_TOKEN, operation_token);
        for kv in trace_headers {
            request = request.header(kv.key.as_str(), kv.value.as_str().as_ref());
        }

        let response = request
            .send()
            .await
            .map_err(|e| anyhow!("nexus cancel request failed: {e}"))?;

        // v1.31.0 treats anything other than 202 Accepted as a handler error
        // (`handle.go:45 @ v1.31.0`).
        if response.status().as_u16() != 202 {
            let status = response.status();
            let bytes = response.bytes().await.unwrap_or_default();
            bail!(
                "nexus cancel: unexpected status {}: {}",
                status,
                String::from_utf8_lossy(&bytes)
            );
        }
        Ok(())
    }
}

/// reqwest-backed [`NexusCompletionClient`]. Like [`HttpNexusClient`] the inner
/// client connection-pools, so one instance is shared across all completion
/// deliveries.
pub struct HttpNexusCompletionClient {
    http: reqwest::Client,
}

impl Default for HttpNexusCompletionClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpNexusCompletionClient {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::new(),
        }
    }

    #[cfg(test)]
    pub fn with_client(http: reqwest::Client) -> Self {
        Self { http }
    }
}

/// Classify a completion-`POST` HTTP status the way the firing path does
/// (`components/callbacks/nexus_invocation.go:105-110 @ v1.31.0`: `isRetryableCallError`
/// → `HandlerError.Retryable()`, with a non-`HandlerError` error defaulting to retryable).
///
/// Returns `Some(retryable)` for a status v1.31.0 maps to a `HandlerError`
/// (`httpStatusCodeToHandlerErrorType`, `common/nexus/nexusrpc/client.go:425-451 @
/// v1.31.0`) — where the `nexus-request-retryable` header may override the default — or
/// `None` for an **unmapped** status, which v1.31.0 surfaces as an
/// `UnexpectedResponseError` (a non-`HandlerError`) and therefore always retries,
/// ignoring the header.
///
/// `408`/`429` are the retryable 4xx (`retryable4xxErrorTypes`, `nexus_invocation.go:22 @
/// v1.31.0`); `500`/`503`/`520` are retryable 5xx; `400`/`401`/`403`/`404`/`409`/`501`
/// are terminal handler errors. NOTE: the per-type `HandlerError.Retryable()` default
/// lives in the external `github.com/nexus-rpc/sdk-go v0.6.0` (not in the v1.31.0
/// checkout); the `409`/`501` terminal classification follows the SDK's conventional
/// permanent-error set and is flagged for reconfirmation in the Wave 8 conformance pass.
/// Render an error plus its full `source` chain, so a transport failure surfaces its root
/// cause (e.g. `... -> tcp connect error -> Connection refused (os error 61)` on a specific
/// address) rather than reqwest's opaque "error sending request for url (...)".
fn error_chain(err: &dyn std::error::Error) -> String {
    let mut out = err.to_string();
    let mut src = err.source();
    while let Some(s) = src {
        out.push_str(" -> ");
        out.push_str(&s.to_string());
        src = s.source();
    }
    out
}

/// Map an HTTP status code to the Nexus `HandlerErrorType` string the SDK uses, mirroring
/// `httpStatusCodeToHandlerErrorType` (`common/nexus/nexusrpc/client.go @ v1.31.0`; the
/// per-status table itself lives in the external `github.com/nexus-rpc/sdk-go`, like the
/// retry-default note on [`mapped_handler_error_retryable`]). An unmapped status reports
/// `INTERNAL`, matching the SDK surfacing an `UnexpectedResponseError` as an internal
/// handler error. The non-`400` rows follow the SDK's conventional status↔type set and are
/// flagged for reconfirmation alongside the Wave 8 conformance pass.
fn handler_error_type_for_status(status: u16) -> &'static str {
    match status {
        400 => "BAD_REQUEST",
        401 => "UNAUTHENTICATED",
        403 => "UNAUTHORIZED",
        404 => "NOT_FOUND",
        429 => "RESOURCE_EXHAUSTED",
        500 => "INTERNAL",
        501 => "NOT_IMPLEMENTED",
        503 => "UNAVAILABLE",
        504 => "UPSTREAM_TIMEOUT",
        _ => "INTERNAL",
    }
}

fn mapped_handler_error_retryable(status: u16) -> Option<bool> {
    match status {
        408 | 429 => Some(true),
        400 | 401 | 403 | 404 | 409 | 501 => Some(false),
        500 | 503 | 520 => Some(true),
        // Unmapped → UnexpectedResponseError (non-HandlerError) → always retryable.
        _ => None,
    }
}

#[async_trait]
impl NexusCompletionClient for HttpNexusCompletionClient {
    async fn complete_operation(
        &self,
        url: &str,
        token: &str,
        completion: NexusCompletion,
        // Best-effort `Nexus-Link` headers are not essential to resolution (design.md
        // §5); their encoder lands with the link producer in Wave 4/5.
        _links: &[Link],
    ) -> Result<CompletionDeliveryOutcome> {
        let mut request = self
            .http
            .post(url)
            .header(HEADER_OPERATION_STATE, completion.operation_state())
            .header(TEMPORAL_CALLBACK_TOKEN_HEADER, token)
            .header(reqwest::header::USER_AGENT, NEXUS_USER_AGENT);

        let (bytes, content_type) = match completion {
            // Reuse the exact payload→body content-type mapping the start path uses
            // (`payloadSerializer.Serialize @ v1.31.0`).
            NexusCompletion::Succeeded(payloads) => payload_to_body(&payloads),
            // failed/canceled both carry a JSON Nexus failure body.
            NexusCompletion::Failed(json) | NexusCompletion::Canceled(json) => {
                (json, Some(CONTENT_TYPE_JSON.to_owned()))
            }
        };
        if let Some(content_type) = &content_type {
            request = request.header(reqwest::header::CONTENT_TYPE, content_type);
        }
        request = request.body(bytes);

        let response = match request.send().await {
            Ok(response) => response,
            // No response: transport/connect/timeout error → retryable
            // (`isRetryableCallError` non-HandlerError default, `nexus_invocation.go:110 @ v1.31.0`).
            Err(error) => {
                return Ok(CompletionDeliveryOutcome::RetryableError {
                    detail: format!("nexus completion request failed: {error}"),
                });
            }
        };

        let status = response.status();
        let code = status.as_u16();
        // The `nexus-request-retryable` header overrides the per-type default, but only
        // for a status v1.31.0 maps to a `HandlerError` (`retryBehaviorFromHeader`,
        // `nexusrpc/client.go:455 @ v1.31.0`, read only in `defaultErrorFromResponse`).
        let retry_override = response
            .headers()
            .get(HEADER_RETRYABLE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| match value.to_ascii_lowercase().as_str() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            });

        if (200..300).contains(&code) {
            return Ok(CompletionDeliveryOutcome::Delivered);
        }

        let detail_body = response.text().await.unwrap_or_default();
        let detail = format!("nexus completion status {code}: {detail_body}");
        let retryable = match mapped_handler_error_retryable(code) {
            // Mapped handler error: the header overrides the per-type default.
            Some(default) => retry_override.unwrap_or(default),
            // Unmapped status: always retryable; the header is not consulted.
            None => true,
        };
        Ok(if retryable {
            CompletionDeliveryOutcome::RetryableError { detail }
        } else {
            CompletionDeliveryOutcome::NonRetryableError { detail }
        })
    }
}

/// `OperationInfo` JSON returned on a 201 async start (`OperationInfo` schema,
/// nexus-rpc SPEC.md; `api.go @ v1.31.0`).
#[derive(Deserialize)]
struct OperationInfo {
    #[serde(default)]
    token: String,
    #[serde(default)]
    state: String,
}

/// Minimal `Failure` JSON shape — we only surface the message, which is all the
/// `SyncFailed` variant carries onward to the kernel failure payload.
#[derive(Deserialize)]
struct NexusFailureBody {
    message: Option<String>,
}

fn failure_message(bytes: &[u8]) -> Option<String> {
    serde_json::from_slice::<NexusFailureBody>(bytes)
        .ok()
        .and_then(|f| f.message)
        .filter(|m| !m.is_empty())
}

/// Map a Nexus link `eventType` query value to the kernel's i32 event type.
///
/// Mirrors `enumspb.EventTypeFromString` (`enums/v1/event_type.go-helpers.pb.go @
/// api-go`), which `link_converter.go @ v1.31.0` uses to decode the link. The
/// quirk: Temporal's `EventType.String()` is **custom** and emits the PascalCase
/// shorthand (`WorkflowExecutionStarted`), not the protojson SCREAMING_CASE name,
/// so that is what the link query actually carries. `EventTypeFromString` accepts
/// *both* forms (two maps). prost's `from_str_name` only knows the SCREAMING_CASE
/// name, so a shorthand value is expanded to `EVENT_TYPE_<SCREAMING_SNAKE>` and
/// retried. Unknown values default to UNSPECIFIED (0): a cosmetic link must never
/// fail the operation.
fn event_type_from_link_value(s: &str) -> i32 {
    use tokeira_proto::enums::EventType;
    if let Some(e) = EventType::from_str_name(s) {
        return e as i32;
    }
    // PascalCase shorthand -> EVENT_TYPE_SCREAMING_SNAKE: a separating underscore
    // precedes every uppercase letter (the leading "EVENT_TYPE" supplies the first).
    let mut screaming = String::from("EVENT_TYPE");
    for ch in s.chars() {
        if ch.is_ascii_uppercase() {
            screaming.push('_');
        }
        screaming.push(ch.to_ascii_uppercase());
    }
    EventType::from_str_name(&screaming)
        .map(|e| e as i32)
        .unwrap_or(0)
}

/// `FormatDuration` = whole milliseconds + `ms` (`api.go:279 @ v1.31.0`).
fn format_duration_ms(d: Duration) -> String {
    format!("{}ms", d.whole_milliseconds())
}

/// Serialize a Nexus operation input into an HTTP body + optional content-type,
/// mirroring `payloadSerializer.Serialize @ v1.31.0`. Nexus operations take a
/// single input, so only the first payload is sent; an empty `Payloads` is the
/// nil body (no content-type), matching the SDK's nil serializer.
///
/// The mapping is keyed on the Temporal payload `encoding` metadata. The common
/// SDK cases (`json/plain`, `binary/plain`, the protobuf encodings, `binary/null`)
/// map to their wire content-types verbatim; anything else (including a payload
/// with no metadata) falls back to `application/x-temporal-payload` carrying the
/// proto-marshalled `Payload`, exactly as the Go serializer does. This is what
/// lets a handler reconstruct the original payload byte-for-byte.
fn payload_to_body(input: &Payloads) -> (Vec<u8>, Option<String>) {
    let Some(payload) = input.0.first() else {
        return (Vec::new(), None);
    };

    let encoding = payload.metadata.get("encoding").map(String::as_str);
    let message_type = payload.metadata.get("messageType").cloned();

    match encoding {
        Some("json/plain") => (payload.data.clone(), Some("application/json".to_owned())),
        Some("binary/plain") if payload.metadata.len() == 1 => (
            payload.data.clone(),
            Some("application/octet-stream".to_owned()),
        ),
        // Unset type: a null payload carries no content-type and no body.
        Some("binary/null") if payload.metadata.len() == 1 => (payload.data.clone(), None),
        Some("json/protobuf") if payload.metadata.len() == 2 && message_type.is_some() => (
            payload.data.clone(),
            Some(format!(
                "application/json; format=protobuf; message-type=\"{}\"",
                message_type.unwrap()
            )),
        ),
        Some("binary/protobuf") if payload.metadata.len() == 2 && message_type.is_some() => (
            payload.data.clone(),
            Some(format!(
                "application/x-protobuf; message-type=\"{}\"",
                message_type.unwrap()
            )),
        ),
        _ => (
            x_temporal_payload_bytes(payload),
            Some("application/x-temporal-payload".to_owned()),
        ),
    }
}

/// Proto-marshal a tokeira `Payload` as a `temporal.api.common.v1.Payload` — the
/// `application/x-temporal-payload` fallback body (`xTemporalPayload @ v1.31.0`).
fn x_temporal_payload_bytes(payload: &Payload) -> Vec<u8> {
    use prost::Message;
    let proto = tokeira_proto::common::Payload {
        metadata: payload
            .metadata
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().into_bytes()))
            .collect(),
        data: payload.data.clone(),
        ..Default::default()
    };
    proto.encode_to_vec()
}

/// Decode a sync (200) result body into `Payloads`, mirroring
/// `payloadSerializer.Deserialize @ v1.31.0`: the content-type media type and its
/// params select the Temporal `encoding` metadata; `application/x-temporal-payload`
/// is the proto-encoded `Payload` and is decoded back directly.
///
/// An empty body with no content-type is `binary/null` (the SDK's nil shape). The
/// result is always a single-element `Payloads` (a Nexus result is one value).
///
/// Reused by the inbound `/nexus/callback` handler (tokeira-edge, Wave 5) to decode a
/// `succeeded` completion body — one serializer for both the sync-result and
/// async-completion paths.
pub fn body_to_payloads(body: &[u8], content_type: Option<&str>) -> Payloads {
    let Some(content_type) = content_type.map(str::trim).filter(|s| !s.is_empty()) else {
        // No content-type: nil result iff body is also empty, else opaque bytes.
        let mut metadata = std::collections::BTreeMap::new();
        if body.is_empty() {
            metadata.insert("encoding".to_owned(), "binary/null".to_owned());
        } else {
            metadata.insert("encoding".to_owned(), "binary/plain".to_owned());
        }
        return Payloads(vec![Payload {
            data: body.to_vec(),
            metadata,
        }]);
    };

    let (media_type, params) = parse_media_type(content_type);
    let mut metadata = std::collections::BTreeMap::new();
    match media_type.as_str() {
        "application/x-temporal-payload" => {
            if let Some(p) = decode_x_temporal_payload(body) {
                return Payloads(vec![p]);
            }
            metadata.insert("encoding".to_owned(), "unknown/nexus-content".to_owned());
        }
        "application/json" => {
            if let (Some(format), Some(mt)) = (params.get("format"), params.get("message-type"))
                && format == "protobuf"
                && !mt.is_empty()
            {
                metadata.insert("encoding".to_owned(), "json/protobuf".to_owned());
                metadata.insert("messageType".to_owned(), mt.clone());
            } else {
                metadata.insert("encoding".to_owned(), "json/plain".to_owned());
            }
        }
        "application/x-protobuf" => match params.get("message-type") {
            Some(mt) if !mt.is_empty() => {
                metadata.insert("encoding".to_owned(), "binary/protobuf".to_owned());
                metadata.insert("messageType".to_owned(), mt.clone());
            }
            _ => {
                metadata.insert("encoding".to_owned(), "unknown/nexus-content".to_owned());
            }
        },
        "application/octet-stream" => {
            metadata.insert("encoding".to_owned(), "binary/plain".to_owned());
        }
        _ => {
            metadata.insert("encoding".to_owned(), "unknown/nexus-content".to_owned());
            metadata.insert("type".to_owned(), content_type.to_owned());
        }
    }
    Payloads(vec![Payload {
        data: body.to_vec(),
        metadata,
    }])
}

fn decode_x_temporal_payload(body: &[u8]) -> Option<Payload> {
    use prost::Message;
    let proto = tokeira_proto::common::Payload::decode(body).ok()?;
    Some(Payload {
        data: proto.data,
        metadata: proto
            .metadata
            .into_iter()
            .map(|(k, v)| (k, String::from_utf8_lossy(&v).into_owned()))
            .collect(),
    })
}

/// Split `type; key=value; key="value"` into a lower-cased media type and its
/// params (RFC 2045-ish; we do not need full MIME grammar here).
fn parse_media_type(content_type: &str) -> (String, std::collections::HashMap<String, String>) {
    let mut parts = content_type.split(';');
    let media_type = parts.next().unwrap_or("").trim().to_ascii_lowercase();
    let mut params = std::collections::HashMap::new();
    for part in parts {
        if let Some((k, v)) = part.split_once('=') {
            let v = v.trim().trim_matches('"').to_owned();
            params.insert(k.trim().to_ascii_lowercase(), v);
        }
    }
    (media_type, params)
}

/// Decode every `Nexus-Link` response header into kernel links. Header decoding
/// follows `decodeLink @ v1.31.0` (RFC 8288 `<url>; type="..."`, comma-separable);
/// a malformed header is a hard error. Workflow-event links convert to a kernel
/// `Link::WorkflowEvent`; other link types are skipped (logged), since they carry
/// no kernel representation and are not part of the operation's correctness.
fn parse_response_links(headers: &reqwest::header::HeaderMap) -> Result<Vec<Link>> {
    let mut links = Vec::new();
    for value in headers.get_all(HEADER_LINK) {
        let raw = value
            .to_str()
            .map_err(|e| anyhow!("nexus-link header is not valid text: {e}"))?;
        for encoded in raw.split(',') {
            if encoded.trim().is_empty() {
                continue;
            }
            let (url, link_type) = decode_link_header(encoded)?;
            if link_type != LINK_TYPE_WORKFLOW_EVENT {
                tracing::debug!(link_type, "skipping non-workflow-event nexus link");
                continue;
            }
            match nexus_url_to_workflow_event_link(&url) {
                Some(link) => links.push(link),
                None => {
                    tracing::debug!(%url, "skipping unparseable workflow-event nexus link");
                }
            }
        }
    }
    Ok(links)
}

/// Parse one `<url>; type="..."` link header entry (`decodeLink @ v1.31.0`).
fn decode_link_header(encoded: &str) -> Result<(String, String)> {
    let encoded = encoded.trim();
    let url_end = encoded
        .find('>')
        .filter(|_| encoded.starts_with('<'))
        .ok_or_else(|| anyhow!("invalid nexus-link header (missing <url>): {encoded}"))?;
    let url = encoded[1..url_end].trim().to_owned();
    if url.is_empty() {
        bail!("invalid nexus-link header (empty url): {encoded}");
    }
    let mut link_type = None;
    for param in encoded[url_end + 1..].split(';') {
        let param = param.trim();
        if param.is_empty() {
            continue;
        }
        if let Some((k, v)) = param.split_once('=')
            && k.trim() == "type"
        {
            link_type = Some(v.trim().trim_matches('"').to_owned());
        }
    }
    let link_type =
        link_type.ok_or_else(|| anyhow!("invalid nexus-link header (no type param): {encoded}"))?;
    Ok((url, link_type))
}

/// Convert a `temporal://` workflow-event link URL into a kernel `Link`
/// (`ConvertNexusLinkToLinkWorkflowEvent @ v1.31.0`). Returns `None` if the URL
/// does not match the `/namespaces/{ns}/workflows/{wf}/{run}/history` shape or the
/// reference query is unrecognised — the caller treats that as "no kernel link".
fn nexus_url_to_workflow_event_link(url_str: &str) -> Option<Link> {
    let url = Url::parse(url_str).ok()?;
    if url.scheme() != URL_SCHEME_TEMPORAL {
        return None;
    }
    // Expect path `/namespaces/{ns}/workflows/{wf}/{run}/history` (segments are
    // percent-encoded; decode each).
    let segments: Vec<String> = url
        .path_segments()?
        .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
        .collect();
    if segments.len() != 6
        || segments[0] != "namespaces"
        || segments[2] != "workflows"
        || segments[5] != "history"
    {
        return None;
    }
    let namespace = segments[1].clone();
    let workflow_id = segments[3].clone();
    let run_id = segments[4].clone();

    let mut reference_type = None;
    let mut event_id = 0i64;
    let mut event_type_name = None;
    let mut request_id = String::new();
    for (k, v) in url.query_pairs() {
        match k.as_ref() {
            "referenceType" => reference_type = Some(v.into_owned()),
            "eventID" => event_id = v.parse().unwrap_or(0),
            "eventType" => event_type_name = Some(v.into_owned()),
            "requestID" => request_id = v.into_owned(),
            _ => {}
        }
    }
    let event_type = event_type_name
        .as_deref()
        .map(event_type_from_link_value)
        .unwrap_or(0);

    let reference = match reference_type.as_deref() {
        Some(EVENT_REFERENCE_TYPE) => Some(LinkWorkflowEventReference::Event {
            event_id,
            event_type,
        }),
        Some(REQUEST_ID_REFERENCE_TYPE) => Some(LinkWorkflowEventReference::RequestId {
            request_id,
            event_type,
        }),
        _ => None,
    };

    Some(Link::WorkflowEvent {
        namespace,
        workflow_id,
        run_id,
        reference,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    // The exact `Nexus-Link` header value Temporal's
    // `ConvertLinkWorkflowEventToNexusLink` produces for the SyncCompletion test's
    // handler link (`tests/nexus_workflow_test.go @ v1.31.0`): namespace
    // "handler-ns", workflow "handler-wf-id", run "handler-run-id", EventRef with
    // EVENT_TYPE_WORKFLOW_EXECUTION_STARTED and no event id. `url.Values.Encode`
    // sorts keys, so `eventType` precedes `referenceType`. The `eventType` value is
    // the PascalCase shorthand `WorkflowExecutionStarted` because Temporal's
    // `EventType.String()` is custom (NOT the SCREAMING_CASE protojson name).
    const HANDLER_LINK_HEADER: &str = "<temporal:///namespaces/handler-ns/workflows/handler-wf-id/handler-run-id/history?eventType=WorkflowExecutionStarted&referenceType=EventReference>; type=\"temporal.api.common.v1.Link.WorkflowEvent\"";

    #[test]
    fn decodes_workflow_event_link_header() {
        let (url, link_type) = decode_link_header(HANDLER_LINK_HEADER).unwrap();
        assert_eq!(link_type, LINK_TYPE_WORKFLOW_EVENT);
        let link = nexus_url_to_workflow_event_link(&url).unwrap();
        // WorkflowExecutionStarted is enum value 1; the converter maps the enum
        // name string back to that i32.
        let started = tokeira_proto::enums::EventType::WorkflowExecutionStarted as i32;
        assert_eq!(
            link,
            Link::WorkflowEvent {
                namespace: "handler-ns".to_owned(),
                workflow_id: "handler-wf-id".to_owned(),
                run_id: "handler-run-id".to_owned(),
                reference: Some(LinkWorkflowEventReference::Event {
                    event_id: 0,
                    event_type: started,
                }),
            }
        );
    }

    #[test]
    fn event_type_accepts_both_shorthand_and_canonical() {
        let started = tokeira_proto::enums::EventType::WorkflowExecutionStarted as i32;
        // Temporal's link writes the PascalCase shorthand; we must also accept the
        // SCREAMING_CASE protojson name (EventTypeFromString accepts both).
        assert_eq!(
            event_type_from_link_value("WorkflowExecutionStarted"),
            started
        );
        assert_eq!(
            event_type_from_link_value("EVENT_TYPE_WORKFLOW_EXECUTION_STARTED"),
            started
        );
        // A multi-word shorthand round-trips too.
        let scheduled = tokeira_proto::enums::EventType::NexusOperationScheduled as i32;
        assert_eq!(
            event_type_from_link_value("NexusOperationScheduled"),
            scheduled
        );
        // Unknown -> UNSPECIFIED (0), never a panic.
        assert_eq!(event_type_from_link_value("NotAnEventType"), 0);
    }

    #[test]
    fn non_workflow_event_link_type_is_skipped() {
        // A link with an unmodelled type decodes at the header level but yields no
        // kernel link (it is observability metadata we do not represent).
        let header = "<http://example.test/x>; type=\"com.example.Other\"";
        let mut map = reqwest::header::HeaderMap::new();
        map.append(HEADER_LINK, header.parse().unwrap());
        assert!(parse_response_links(&map).unwrap().is_empty());
    }

    #[test]
    fn json_plain_payload_maps_to_application_json() {
        let input = Payloads(vec![Payload {
            data: b"\"input\"".to_vec(),
            metadata: BTreeMap::from([("encoding".to_owned(), "json/plain".to_owned())]),
        }]);
        let (body, content_type) = payload_to_body(&input);
        assert_eq!(body, b"\"input\"");
        assert_eq!(content_type.as_deref(), Some("application/json"));
    }

    #[test]
    fn empty_input_is_nil_body() {
        let (body, content_type) = payload_to_body(&Payloads(Vec::new()));
        assert!(body.is_empty());
        assert!(content_type.is_none());
    }

    #[test]
    fn application_json_body_decodes_to_json_plain() {
        let payloads = body_to_payloads(b"\"result\"", Some("application/json"));
        assert_eq!(payloads.0.len(), 1);
        assert_eq!(payloads.0[0].data, b"\"result\"");
        assert_eq!(
            payloads.0[0].metadata.get("encoding").map(String::as_str),
            Some("json/plain")
        );
    }

    /// Serve exactly one HTTP request from a fresh loopback listener, replying with
    /// `response`. Reads the full request (headers + Content-Length body) first so
    /// reqwest does not see a reset before its body write completes. Returns the
    /// `http://addr` base. No sleeps: the bound address is known before the client
    /// connects, and the accept future drives the exchange.
    async fn serve_once(response: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut tmp = [0u8; 1024];
            loop {
                let n = socket.read(&mut tmp).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
                if let Some(header_end) = find_header_end(&buf) {
                    let content_length = parse_content_length(&buf[..header_end]);
                    if buf.len() >= header_end + content_length {
                        break;
                    }
                }
            }
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.flush().await.unwrap();
        });
        format!("http://{addr}")
    }

    fn find_header_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
    }

    fn parse_content_length(headers: &[u8]) -> usize {
        let text = String::from_utf8_lossy(headers);
        for line in text.lines() {
            if let Some((k, v)) = line.split_once(':')
                && k.trim().eq_ignore_ascii_case("content-length")
            {
                return v.trim().parse().unwrap_or(0);
            }
        }
        0
    }

    fn empty_input() -> Payloads {
        Payloads(Vec::new())
    }

    #[tokio::test]
    async fn start_sync_200_yields_completed_with_link() {
        let response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nNexus-Link: <temporal:///namespaces/handler-ns/workflows/handler-wf-id/handler-run-id/history?eventType=WorkflowExecutionStarted&referenceType=EventReference>; type=\"temporal.api.common.v1.Link.WorkflowEvent\"\r\nContent-Length: 8\r\n\r\n\"result\"";
        let base = serve_once(response).await;
        let client = HttpNexusClient::new();
        let result = client
            .start_operation(
                &base,
                "req-1",
                "service",
                "operation",
                &empty_input(),
                None,
                &[],
            )
            .await
            .unwrap();
        match result {
            NexusStartResult::SyncCompleted { result, links } => {
                assert_eq!(result.0[0].data, b"\"result\"");
                assert_eq!(links.len(), 1, "handler link must be carried through");
                let started = tokeira_proto::enums::EventType::WorkflowExecutionStarted as i32;
                assert_eq!(
                    links[0],
                    Link::WorkflowEvent {
                        namespace: "handler-ns".to_owned(),
                        workflow_id: "handler-wf-id".to_owned(),
                        run_id: "handler-run-id".to_owned(),
                        reference: Some(LinkWorkflowEventReference::Event {
                            event_id: 0,
                            event_type: started,
                        }),
                    },
                    "handler link must round-trip with its event_type"
                );
            }
            other => panic!("expected SyncCompleted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn start_async_201_yields_accepted_with_token() {
        let response = "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: 35\r\n\r\n{\"token\":\"tok-1\",\"state\":\"running\"}";
        let base = serve_once(response).await;
        let client = HttpNexusClient::new();
        let result = client
            .start_operation(
                &base,
                "req-1",
                "service",
                "operation",
                &empty_input(),
                None,
                &[],
            )
            .await
            .unwrap();
        match result {
            NexusStartResult::AsyncAccepted {
                operation_token,
                links,
            } => {
                assert_eq!(operation_token, "tok-1");
                assert!(links.is_empty());
            }
            other => panic!("expected AsyncAccepted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn start_unsuccessful_424_yields_sync_failed() {
        let response = "HTTP/1.1 424 Failed Dependency\r\nContent-Type: application/json\r\nContent-Length: 18\r\n\r\n{\"message\":\"boom\"}";
        let base = serve_once(response).await;
        let client = HttpNexusClient::new();
        let result = client
            .start_operation(
                &base,
                "req-1",
                "service",
                "operation",
                &empty_input(),
                None,
                &[],
            )
            .await
            .unwrap();
        match result {
            NexusStartResult::SyncFailed { message } => assert_eq!(message, "boom"),
            other => panic!("expected SyncFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_202_is_ok_and_non_202_errors() {
        let base = serve_once("HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n").await;
        let client = HttpNexusClient::new();
        client
            .cancel_operation(&base, "service", "operation", "tok-1", &[])
            .await
            .unwrap();

        let base =
            serve_once("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n").await;
        assert!(
            client
                .cancel_operation(&base, "service", "operation", "tok-1", &[])
                .await
                .is_err()
        );
    }

    // ---- nexus-async-completion Wave 3: completion client ----

    /// Like [`serve_once`] but also hands the captured raw request back to the caller
    /// so a test can assert the wire shape (method, headers, body).
    async fn serve_once_capture(
        response: &'static str,
    ) -> (String, tokio::sync::oneshot::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut tmp = [0u8; 1024];
            loop {
                let n = socket.read(&mut tmp).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
                if let Some(header_end) = find_header_end(&buf) {
                    let content_length = parse_content_length(&buf[..header_end]);
                    if buf.len() >= header_end + content_length {
                        break;
                    }
                }
            }
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.flush().await.unwrap();
            let _ = tx.send(String::from_utf8_lossy(&buf).into_owned());
        });
        (format!("http://{addr}"), rx)
    }

    #[test]
    fn completion_status_classification_matches_v1_31_0() {
        // Retryable 4xx (`retryable4xxErrorTypes`, `nexus_invocation.go:22 @ v1.31.0`).
        assert_eq!(mapped_handler_error_retryable(408), Some(true));
        assert_eq!(mapped_handler_error_retryable(429), Some(true));
        // Terminal mapped handler errors.
        for terminal in [400u16, 401, 403, 404, 409, 501] {
            assert_eq!(
                mapped_handler_error_retryable(terminal),
                Some(false),
                "{terminal} must be terminal"
            );
        }
        // Retryable mapped 5xx.
        for retry in [500u16, 503, 520] {
            assert_eq!(
                mapped_handler_error_retryable(retry),
                Some(true),
                "{retry} must be retryable"
            );
        }
        // Unmapped → None: v1.31.0 makes these UnexpectedResponseError → always retryable.
        for unmapped in [402u16, 405, 410, 418, 451, 599] {
            assert_eq!(
                mapped_handler_error_retryable(unmapped),
                None,
                "{unmapped} is unmapped"
            );
        }
    }

    #[tokio::test]
    async fn completion_succeeded_posts_state_token_and_payload_body() {
        let (base, rx) = serve_once_capture("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").await;
        let outcome = HttpNexusCompletionClient::new()
            .complete_operation(
                &base,
                "tok-abc",
                NexusCompletion::Succeeded(Payloads(vec![Payload {
                    data: b"\"r\"".to_vec(),
                    metadata: BTreeMap::from([("encoding".to_owned(), "json/plain".to_owned())]),
                }])),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(outcome, CompletionDeliveryOutcome::Delivered);

        let req = rx.await.unwrap();
        assert!(req.starts_with("POST "), "must be a POST: {req}");
        let lower = req.to_lowercase();
        assert!(lower.contains("nexus-operation-state: succeeded"));
        assert!(lower.contains("temporal-callback-token: tok-abc"));
        assert!(lower.contains("user-agent: temporalio/server"));
        assert!(lower.contains("content-type: application/json"));
        assert!(req.contains("\"r\""), "result payload is the body: {req}");
    }

    #[tokio::test]
    async fn completion_canceled_posts_json_failure_body() {
        let (base, rx) = serve_once_capture("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").await;
        let outcome = HttpNexusCompletionClient::new()
            .complete_operation(
                &base,
                "tok",
                NexusCompletion::Canceled(b"{\"message\":\"operation canceled\"}".to_vec()),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(outcome, CompletionDeliveryOutcome::Delivered);

        let req = rx.await.unwrap();
        let lower = req.to_lowercase();
        // Canceled is delivered as state=canceled with a JSON failure body, NOT failed
        // (`GetNexusCompletion` canceled arm @ v1.31.0).
        assert!(lower.contains("nexus-operation-state: canceled"));
        assert!(lower.contains("content-type: application/json"));
        assert!(req.contains("operation canceled"));
    }

    /// Drive `complete_operation` against a fixed response and return the outcome.
    async fn deliver_against(response: &'static str) -> CompletionDeliveryOutcome {
        let (base, _rx) = serve_once_capture(response).await;
        HttpNexusCompletionClient::new()
            .complete_operation(&base, "tok", NexusCompletion::Succeeded(empty_input()), &[])
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn completion_400_is_non_retryable() {
        let outcome =
            deliver_against("HTTP/1.1 400 Bad Request\r\nContent-Length: 3\r\n\r\nbad").await;
        assert!(
            matches!(outcome, CompletionDeliveryOutcome::NonRetryableError { .. }),
            "got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn completion_503_is_retryable() {
        let outcome =
            deliver_against("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n").await;
        assert!(
            matches!(outcome, CompletionDeliveryOutcome::RetryableError { .. }),
            "got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn completion_unmapped_4xx_is_retryable() {
        // 418 is not a mapped handler-error status → UnexpectedResponseError → retryable
        // (the bug the Wave 3 review caught: an unmapped 4xx must not be terminal).
        let outcome = deliver_against("HTTP/1.1 418 Teapot\r\nContent-Length: 0\r\n\r\n").await;
        assert!(
            matches!(outcome, CompletionDeliveryOutcome::RetryableError { .. }),
            "unmapped 4xx must be retryable: {outcome:?}"
        );
    }

    #[tokio::test]
    async fn completion_retryable_header_overrides_terminal_status() {
        // 400 is normally terminal, but `nexus-request-retryable: true` forces a retry
        // (`retryBehaviorFromHeader`, `nexusrpc/client.go:455 @ v1.31.0`).
        let outcome = deliver_against(
            "HTTP/1.1 400 Bad Request\r\nnexus-request-retryable: true\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert!(
            matches!(outcome, CompletionDeliveryOutcome::RetryableError { .. }),
            "got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn completion_unmapped_status_ignores_retryable_false_header() {
        // For an unmapped status v1.31.0 never consults the header (it is not a
        // HandlerError), so it stays retryable even with `nexus-request-retryable: false`.
        let outcome = deliver_against(
            "HTTP/1.1 418 Teapot\r\nnexus-request-retryable: false\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        assert!(
            matches!(outcome, CompletionDeliveryOutcome::RetryableError { .. }),
            "header must be ignored for an unmapped status: {outcome:?}"
        );
    }

    #[tokio::test]
    async fn completion_transport_error_is_retryable() {
        // An unroutable address yields a transport error → retryable (no response).
        let outcome = HttpNexusCompletionClient::new()
            .complete_operation(
                "http://127.0.0.1:1",
                "tok",
                NexusCompletion::Succeeded(empty_input()),
                &[],
            )
            .await
            .unwrap();
        assert!(
            matches!(outcome, CompletionDeliveryOutcome::RetryableError { .. }),
            "got {outcome:?}"
        );
    }
}
