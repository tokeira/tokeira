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

use crate::nexus::{NexusHttpClient, NexusStartResult};

// Header names and the operation-unsuccessful status are lower-cased wire
// constants from `common/nexus/nexusrpc/api.go @ v1.31.0` (HTTP header names are
// case-insensitive; the SDK normalises to lower case).
const HEADER_REQUEST_ID: &str = "nexus-request-id";
const HEADER_LINK: &str = "nexus-link";
const HEADER_OPERATION_TOKEN: &str = "nexus-operation-token";
const HEADER_OPERATION_TIMEOUT: &str = "operation-timeout";
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
        // The operation-level deadline doubles as the request timeout so a hung
        // handler cannot pin the dispatch task (Error Handling, design.md).
        if let Some(timeout) = schedule_to_close_timeout {
            request = request.header(HEADER_OPERATION_TIMEOUT, format_duration_ms(timeout));
            if let Ok(std) = timeout.try_into() {
                request = request.timeout(std);
            }
        }
        for kv in trace_headers {
            request = request.header(kv.key.as_str(), kv.value.as_str().as_ref());
        }
        request = request.body(body);

        let response = request
            .send()
            .await
            .map_err(|e| anyhow!("nexus start request failed: {e}"))?;

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
                // Any other status is a handler/transport error; surface the body
                // so the publisher's Failed mapping carries a useful cause. A
                // single attempt only — no retry classification here (Req 5.3).
                let bytes = response.bytes().await.unwrap_or_default();
                let detail = String::from_utf8_lossy(&bytes);
                bail!("nexus start: unexpected status {other}: {detail}")
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
fn body_to_payloads(body: &[u8], content_type: Option<&str>) -> Payloads {
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
        .and_then(tokeira_proto::enums::EventType::from_str_name)
        .map(|e| e as i32)
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
    // sorts keys, so `eventType` precedes `referenceType`.
    const HANDLER_LINK_HEADER: &str = "<temporal:///namespaces/handler-ns/workflows/handler-wf-id/handler-run-id/history?eventType=EVENT_TYPE_WORKFLOW_EXECUTION_STARTED&referenceType=EventReference>; type=\"temporal.api.common.v1.Link.WorkflowEvent\"";

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
        let response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nNexus-Link: <temporal:///namespaces/handler-ns/workflows/handler-wf-id/handler-run-id/history?eventType=EVENT_TYPE_WORKFLOW_EXECUTION_STARTED&referenceType=EventReference>; type=\"temporal.api.common.v1.Link.WorkflowEvent\"\r\nContent-Length: 8\r\n\r\n\"result\"";
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
}
