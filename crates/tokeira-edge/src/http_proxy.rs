//! The Temporal-style HTTP API surface: transport plumbing only.
//!
//! Temporal exposes its gRPC services over HTTP at `/api/v1/{service}/{method}`.
//! This module parses that path into a structured [`ProxyRoute`] and shapes
//! transport-level responses. What it deliberately does **not** do is understand
//! workflow semantics or decode request bodies into domain types — that belongs to
//! the domain services above it. Keeping the proxy this thin is what lets the HTTP
//! and gRPC fronts share one logic path: both decode to the same edge inputs and
//! converge on the same [`EdgeError`] → status mapping.

use http::{Response, StatusCode};

use crate::errors::{EdgeError, EdgeResult};

/// Service names exposed by the Temporal-style HTTP proxy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceName {
    Workflow,
    Operator,
    Health,
}

impl ServiceName {
    pub fn as_path_segment(&self) -> &'static str {
        match self {
            ServiceName::Workflow => "WorkflowService",
            ServiceName::Operator => "OperatorService",
            ServiceName::Health => "HealthService",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyRoute {
    pub service: ServiceName,
    pub method: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyCall {
    pub route: ProxyRoute,
    pub body: Vec<u8>,
}

/// HTTP proxy helper.
///
/// The important thing here is what the proxy *doesn't* do: it does not understand
/// workflow semantics. It only understands transport-level path parsing and basic
/// response shaping. Actual request decoding and dispatch should sit above this.
#[derive(Debug, Default)]
pub struct HttpProxy;

impl HttpProxy {
    /// Parse `/api/v1/{service}/{method}` into a structured route.
    pub fn parse_route(path: &str) -> EdgeResult<ProxyRoute> {
        let trimmed = path.trim_matches('/');
        let parts: Vec<_> = trimmed.split('/').collect();

        if parts.len() != 4 || parts[0] != "api" || parts[1] != "v1" {
            return Err(EdgeError::BadRequest(format!(
                "expected /api/v1/{{service}}/{{method}}, got `{path}`"
            )));
        }

        let service = match parts[2] {
            "WorkflowService" => ServiceName::Workflow,
            "OperatorService" => ServiceName::Operator,
            "HealthService" => ServiceName::Health,
            other => {
                return Err(EdgeError::BadRequest(format!(
                    "unknown proxy service `{other}`"
                )));
            }
        };

        Ok(ProxyRoute {
            service,
            method: parts[3].to_string(),
        })
    }

    pub fn path_for(route: &ProxyRoute) -> String {
        format!(
            "/api/v1/{}/{}",
            route.service.as_path_segment(),
            route.method
        )
    }

    pub fn into_call(path: &str, body: Vec<u8>) -> EdgeResult<ProxyCall> {
        Ok(ProxyCall {
            route: Self::parse_route(path)?,
            body,
        })
    }

    pub fn ok_json(body: Vec<u8>) -> Response<Vec<u8>> {
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(body)
            .expect("response build should not fail")
    }

    pub fn error_response(error: &EdgeError) -> Response<Vec<u8>> {
        let body = serde_json::json!({
            "error": error.action_name(),
            "message": error.to_string(),
        })
        .to_string()
        .into_bytes();

        Response::builder()
            .status(error.status_code())
            .header("content-type", "application/json")
            .body(body)
            .expect("response build should not fail")
    }
}
