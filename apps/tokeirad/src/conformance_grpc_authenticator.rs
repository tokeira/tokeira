//! Conformance-only adapter for Temporal's in-process gRPC authorization hooks.
//!
//! Shape-2 runs the corpus and `tokeirad` in separate processes, so callbacks
//! installed through `SetOnGetClaims` and `SetOnAuthorize` cannot otherwise
//! observe an HTTP request after it has entered the ordinary Tonic service path.
//! This adapter is absent from production builds and delegates the exact public
//! action and unresolved namespace to the harness's loopback callback.

use async_trait::async_trait;
use std::collections::BTreeMap;

use http::HeaderMap;
use serde::{Deserialize, Serialize};
use tokeira_auth::{AuthPrincipal, Claims};
use tokeira_edge::{Action, Authenticator, EdgeError, EdgeResult};

const CALLBACK_URL_ENV: &str = "TOKEIRA_CONFORMANCE_AUTH_CALLBACK_URL";

/// Request-scoped harness authenticator enabled only by the conformance callback URL.
#[derive(Clone, Debug)]
pub(crate) struct ConformanceGrpcAuthenticator {
    client: reqwest::Client,
    callback_url: String,
}

impl ConformanceGrpcAuthenticator {
    /// Construct from the harness-provided callback URL.
    pub(crate) fn from_environment() -> Option<Self> {
        let callback_url = std::env::var(CALLBACK_URL_ENV).ok()?;
        let callback_url = callback_url.trim();
        if callback_url.is_empty() {
            return None;
        }
        Some(Self {
            client: reqwest::Client::new(),
            callback_url: callback_url.to_owned(),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
enum CallbackDecision {
    Allow,
    Unauthenticated,
    Deny {
        reason: Option<String>,
    },
    AuthorizerError {
        error_type: Option<String>,
        message: String,
    },
}

#[derive(Debug, Serialize)]
struct CallbackRequest<'a> {
    api_name: &'a str,
    namespace: &'a str,
    nexus_endpoint_name: Option<&'a str>,
    auth_token: &'a str,
    extra_data: &'a str,
    metadata: &'a BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct RequestCarrier {
    extra_data: String,
    metadata: BTreeMap<String, Vec<String>>,
}

#[async_trait]
impl Authenticator for ConformanceGrpcAuthenticator {
    async fn authenticate(&self, headers: &HeaderMap) -> EdgeResult<Option<Claims>> {
        let auth_token = header(headers, "authorization")?.unwrap_or_default();
        let extra_data = header(headers, "authorization-extras")?.unwrap_or_default();
        let mut metadata = BTreeMap::<String, Vec<String>>::new();
        for (name, value) in headers {
            let Ok(value) = value.to_str() else {
                continue;
            };
            metadata
                .entry(name.as_str().to_owned())
                .or_default()
                .push(value.to_owned());
        }
        let carrier = serde_json::to_string(&RequestCarrier {
            extra_data: extra_data.to_owned(),
            metadata,
        })
        .map_err(|error| denied(Some(format!("cannot encode request metadata: {error}"))))?;
        // These two fields are only a request-local carrier between the existing
        // two-stage edge callbacks. The harness remains the sole claims authority;
        // no synthetic role is granted or persisted by `tokeirad`.
        Ok(Some(Claims {
            subject: auth_token.to_owned(),
            auth_type: carrier,
            ..Claims::default()
        }))
    }

    async fn authorize(
        &self,
        claims: Option<&Claims>,
        action: Action,
        namespace_name: Option<&str>,
    ) -> EdgeResult<Option<AuthPrincipal>> {
        let carrier = claims
            .map(|claims| serde_json::from_str::<RequestCarrier>(&claims.auth_type))
            .transpose()
            .map_err(|error| denied(Some(format!("cannot decode request metadata: {error}"))))?
            .unwrap_or_default();
        let response = match self
            .client
            .post(&self.callback_url)
            .json(&CallbackRequest {
                api_name: action.api_name(),
                namespace: namespace_name.unwrap_or_default(),
                nexus_endpoint_name: None,
                auth_token: claims
                    .map(|claims| claims.subject.as_str())
                    .unwrap_or_default(),
                extra_data: &carrier.extra_data,
                metadata: &carrier.metadata,
            })
            .send()
            .await
        {
            Ok(response) => response,
            Err(_) if namespace_name.is_none() => {
                // The runner probes cluster-scoped GetSystemInfo before the Go
                // callback listener exists. No suite hook can be scoped to that
                // pre-cluster request, so retain the ordinary allow-all result.
                return Ok(None);
            }
            Err(error) => {
                return Err(denied(Some(format!(
                    "authorization callback unavailable: {error}"
                ))));
            }
        };
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            // A dedicated cluster with no installed hook has the stock onebox
            // permissive behavior. The callback's namespace lookup keeps hooks
            // from leaking between concurrently alive suite clusters.
            return Ok(None);
        }
        match response.json::<CallbackDecision>().await {
            Ok(CallbackDecision::Allow) => Ok(None),
            Ok(CallbackDecision::Unauthenticated) => Err(denied(None)),
            Ok(CallbackDecision::Deny { reason }) => Err(denied(reason)),
            Ok(CallbackDecision::AuthorizerError {
                error_type,
                message,
            }) => {
                let expose = tokeira_conformance::overrides()
                    .get_bool("frontend.exposeAuthorizerErrors")
                    .unwrap_or(false);
                if expose {
                    Err(EdgeError::PermissionDenied {
                        message: error_type
                            .map(|error_type| format!("{error_type}: {message}"))
                            .unwrap_or(message),
                        reason: None,
                    })
                } else {
                    Err(denied(None))
                }
            }
            Err(error) => Err(denied(Some(format!(
                "invalid authorization callback response: {error}"
            )))),
        }
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> EdgeResult<Option<&'a str>> {
    headers
        .get(name)
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| denied(None))
}

fn denied(reason: Option<String>) -> EdgeError {
    EdgeError::PermissionDenied {
        message: "Request unauthorized.".to_owned(),
        reason,
    }
}
