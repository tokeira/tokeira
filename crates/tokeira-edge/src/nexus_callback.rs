//! Inbound `/nexus/callback` completion handler (Wave 5).
//!
//! Receives an async Nexus operation's eventual outcome — `POST`ed by the handler side
//! to the callback URL the caller minted at schedule time — and resolves the originator
//! (caller) workflow's pending operation. This is the inbound half of the loopback the
//! firing client (Wave 4, `tokeira-runtime`) drives: in single-cluster operation the
//! firing `POST` lands right back here.
//!
//! Mirrors `completionHTTPHandler.ServeHTTP` + `nexusoperations.CompletionHandler`
//! (`common/nexus/nexusrpc/completion.go`, `components/nexusoperations/completion.go @
//! v1.31.0`):
//!
//! - the `nexus-operation-state` header selects the resolution shape (succeeded / failed /
//!   canceled); any other value is a bad request;
//! - `failed`/`canceled` require `Content-Type: application/json` and a decodable failure
//!   body, matching the server's `isMediaTypeJSON` + `json.Unmarshal` guards;
//! - a missing / undecodable / wrong-version callback token is a bad request
//!   (`DecodeCallbackToken` → `InvalidArgument`);
//! - an already-resolved or absent operation resolves to "not found" (v1.31.0 maps the
//!   `hsm.ErrInvalidTransition` from a closed operation to `serviceerror.NewNotFound`,
//!   completion.go:198-200) — idempotent, no second terminal event.
//!
//! The handler is deliberately transport-agnostic: it takes already-extracted header
//! values and the raw body, so it is unit-testable without an HTTP server and the
//! `tokeirad` hyper listener (Wave 5.3) only has to extract headers and map the
//! [`CallbackResponse`] status onto the wire.

use tokeira_kernel::NexusResolution;
use tokeira_runtime::{NexusCompletionFailureBody, NexusCompletionToken, body_to_payloads};

use crate::workflow_service::WorkflowRuntimeApi;

/// Transport-agnostic result of handling a completion `POST`. The caller (the hyper
/// listener) writes `status` as the HTTP status and `body`, if any, as the response body.
#[derive(Clone, Debug, PartialEq)]
pub struct CallbackResponse {
    /// HTTP status code: `200` accepted, `400` malformed request, `404` no such pending
    /// operation (absent or already resolved — idempotent), `503` transient internal error.
    pub status: u16,
    /// Optional response body (a short diagnostic for non-2xx; the firing client keys off
    /// the status, not the body).
    pub body: Option<Vec<u8>>,
}

impl CallbackResponse {
    fn accepted() -> Self {
        Self {
            status: 200,
            body: None,
        }
    }

    fn bad_request(detail: impl Into<String>) -> Self {
        Self {
            status: 400,
            body: Some(detail.into().into_bytes()),
        }
    }

    fn not_found() -> Self {
        Self {
            status: 404,
            body: Some(b"operation not found".to_vec()),
        }
    }

    fn internal() -> Self {
        Self {
            status: 503,
            body: Some(b"completion resolution failed".to_vec()),
        }
    }
}

/// Handle an inbound Nexus completion `POST`, resolving the originator's pending operation.
///
/// `callback_token_header` / `operation_state_header` / `content_type_header` are the
/// extracted values of `Temporal-Callback-Token`, `nexus-operation-state`, and
/// `Content-Type`; `body` is the raw request body. See the module docs for the
/// status taxonomy.
pub async fn handle_nexus_callback(
    runtime: &dyn WorkflowRuntimeApi,
    callback_token_header: Option<&str>,
    operation_state_header: Option<&str>,
    content_type_header: Option<&str>,
    body: &[u8],
) -> CallbackResponse {
    // Decode and version-check the callback token. A missing, malformed, or
    // wrong-version token is a bad request (`DecodeCallbackToken` → `InvalidArgument`).
    let Some(token_str) = callback_token_header
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return CallbackResponse::bad_request("missing Temporal-Callback-Token header");
    };
    let token = match NexusCompletionToken::decode(token_str) {
        Ok(token) => token,
        Err(error) => {
            return CallbackResponse::bad_request(format!("invalid callback token: {error}"));
        }
    };

    // The operation state selects the resolution shape; it is mandatory.
    let Some(state) = operation_state_header
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return CallbackResponse::bad_request("missing nexus-operation-state header");
    };

    let resolution = match state {
        "succeeded" => NexusResolution::Completed {
            result: body_to_payloads(body, content_type_header),
            // Nexus-Link parsing is deferred to `nexus-multi-cluster` (design "Out of
            // Scope"); a single-cluster loopback carries no inbound links.
            links: Vec::new(),
        },
        "failed" | "canceled" => {
            // v1.31.0 requires JSON content-type and a decodable failure for both states
            // (completion.go:257-268). We validate identically; `canceled` discards the
            // decoded failure (`NexusResolution::Canceled` carries none) but a malformed
            // body is still a bad request, matching the server's strictness.
            if !content_type_is_json(content_type_header) {
                return CallbackResponse::bad_request(format!(
                    "{state} completion requires Content-Type: application/json"
                ));
            }
            let failure_body = match NexusCompletionFailureBody::decode(body) {
                Ok(failure_body) => failure_body,
                Err(error) => {
                    return CallbackResponse::bad_request(format!(
                        "invalid completion failure body: {error}"
                    ));
                }
            };
            if state == "failed" {
                NexusResolution::Failed {
                    failure: failure_body.failure,
                }
            } else {
                NexusResolution::Canceled
            }
        }
        other => {
            return CallbackResponse::bad_request(format!(
                "unsupported nexus-operation-state: {other}"
            ));
        }
    };

    match runtime
        .resolve_nexus_operation(
            token.originator_run_key,
            token.operation_id,
            token.scheduled_event_id,
            resolution,
        )
        .await
    {
        // A new terminal event was recorded.
        Ok(true) => CallbackResponse::accepted(),
        // The kernel rejected the command: the run is absent, or the operation is stale /
        // already resolved. Idempotent — report not-found and record no second event,
        // matching v1.31.0's `serviceerror.NewNotFound` for `ErrInvalidTransition`.
        Ok(false) => CallbackResponse::not_found(),
        // A transient internal failure (lane submit / load); the firing client retries.
        Err(_) => CallbackResponse::internal(),
    }
}

/// Whether a `Content-Type` value's media type is `application/json`, ignoring parameters
/// (e.g. `application/json; charset=utf-8`) and ASCII case — mirrors v1.31.0's
/// `isMediaTypeJSON` (`common/nexus/nexusrpc/completion.go`).
fn content_type_is_json(content_type: Option<&str>) -> bool {
    content_type
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .is_some_and(|media_type| media_type.eq_ignore_ascii_case("application/json"))
}
