//! Conformance-only adapter for Temporal's in-process `SetOnAuthorize` hook.
//!
//! The functional corpus installs arbitrary Go closures on its onebox host. An
//! out-of-process server cannot execute those closures directly, so the harness
//! exposes a loopback callback server and this adapter sends it only the resolved
//! Nexus call target. This module is absent from production builds.

use async_trait::async_trait;
use serde::Deserialize;
use tokeira_edge::nexus_http::{
    NexusAuthorizationDecision, NexusAuthorizationTarget, NexusHttpAuthorizer,
};

const CALLBACK_URL_ENV: &str = "TOKEIRA_CONFORMANCE_AUTH_CALLBACK_URL";

/// Namespace-scoped callback authorizer used only by the functional harness.
#[derive(Clone)]
pub struct ConformanceNexusHttpAuthorizer {
    client: reqwest::Client,
    callback_url: String,
    fallback: std::sync::Arc<dyn NexusHttpAuthorizer>,
}

impl std::fmt::Debug for ConformanceNexusHttpAuthorizer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConformanceNexusHttpAuthorizer")
            .field("callback_url", &self.callback_url)
            .field("fallback", &"dyn NexusHttpAuthorizer")
            .finish_non_exhaustive()
    }
}

impl ConformanceNexusHttpAuthorizer {
    /// Construct from the harness-provided callback URL, or return `None` when
    /// the callback bridge is not active for this conformance process.
    pub fn from_environment(fallback: std::sync::Arc<dyn NexusHttpAuthorizer>) -> Option<Self> {
        let callback_url = std::env::var(CALLBACK_URL_ENV).ok()?;
        let callback_url = callback_url.trim();
        if callback_url.is_empty() {
            return None;
        }
        Some(Self {
            client: reqwest::Client::new(),
            callback_url: callback_url.to_owned(),
            fallback,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
enum CallbackDecision {
    Allow,
    Deny {
        reason: Option<String>,
    },
    AuthorizerError {
        error_type: Option<String>,
        message: String,
    },
}

#[async_trait]
impl NexusHttpAuthorizer for ConformanceNexusHttpAuthorizer {
    async fn authorize(
        &self,
        target: &NexusAuthorizationTarget,
        headers: &[(String, String)],
    ) -> NexusAuthorizationDecision {
        let response = match self
            .client
            .post(&self.callback_url)
            .json(target)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                // A configured authorization hook that cannot be reached is an
                // authorizer failure, not evidence of permission. v1.31.0 fails
                // such calls closed; only an explicit bridge 404 means this
                // namespace has no test hook and retains stock permissive behavior.
                tracing::warn!(?error, "conformance authorization callback unavailable");
                return NexusAuthorizationDecision::Deny { reason: None };
            }
        };
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return self.fallback.authorize(target, headers).await;
        }
        match response.json::<CallbackDecision>().await {
            Ok(CallbackDecision::Allow) => NexusAuthorizationDecision::Allow,
            Ok(CallbackDecision::Deny { reason }) => NexusAuthorizationDecision::Deny { reason },
            Ok(CallbackDecision::AuthorizerError {
                error_type,
                message,
            }) => {
                let expose = tokeira_conformance::overrides()
                    .get_bool("frontend.exposeAuthorizerErrors")
                    .unwrap_or(false);
                match (expose, error_type) {
                    (true, Some(error_type)) => NexusAuthorizationDecision::ExposedError {
                        error_type,
                        message,
                    },
                    _ => NexusAuthorizationDecision::Deny { reason: None },
                }
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    "invalid conformance authorization callback response"
                );
                NexusAuthorizationDecision::Deny { reason: None }
            }
        }
    }
}
