//! Readiness reporting primitives.
//!
//! Readiness is intentionally distinct from liveness. `/healthz` answers
//! whether the process can respond; `/readyz` answers whether dependencies and
//! background loops are healthy enough to receive traffic. Check messages are
//! sanitized because readiness endpoints are often exposed to operators more
//! broadly than internal logs.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use async_trait::async_trait;
use serde::Serialize;

use crate::ObservabilityError;

/// Readiness state for one dependency or background loop.
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessStatus {
    /// Fully healthy and ready to receive traffic.
    Ready,
    /// Serving but impaired; operators should investigate.
    Degraded,
    /// Not ready to receive traffic.
    NotReady,
}

/// Serialized readiness check result returned from `/readyz`.
#[derive(Clone, Debug, Serialize)]
pub struct ReadinessResult {
    /// Stable check name.
    pub name: String,
    /// Current status.
    pub status: ReadinessStatus,
    /// Sanitized operator-facing message.
    pub message: Option<String>,
    /// Wall-clock check time for humans and probes.
    pub checked_at_unix_seconds: u64,
    /// Check latency, saturated at `u64::MAX`.
    pub latency_millis: u64,
}

/// Internal check result before the registry adds timing metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadinessCheckResult {
    /// Current status.
    pub status: ReadinessStatus,
    /// Sanitized operator-facing message.
    pub message: Option<String>,
}

impl ReadinessCheckResult {
    /// Construct a result with no message.
    pub fn new(status: ReadinessStatus) -> Self {
        Self {
            status,
            message: None,
        }
    }

    /// Construct a result with a sanitized message.
    pub fn with_message(status: ReadinessStatus, message: impl Into<String>) -> Self {
        Self {
            status,
            message: Some(sanitize_readiness_message(message.into())),
        }
    }
}

/// Async readiness check owned by a dependency or loop.
#[async_trait]
pub trait ReadinessCheck: Send + Sync {
    /// Stable check name used in `/readyz` output.
    fn name(&self) -> &'static str;
    /// Compute current readiness.
    async fn check(&self) -> Result<ReadinessCheckResult, ObservabilityError>;
}

/// Shared registry of readiness checks.
#[derive(Clone, Default)]
pub struct ReadinessRegistry {
    checks: Arc<Vec<Arc<dyn ReadinessCheck>>>,
}

/// Mutable readiness handle for long-running loops.
///
/// Background components such as projection workers can update this handle when
/// they enter degraded or unavailable states without implementing a bespoke
/// async check.
#[derive(Clone, Debug)]
pub struct ReadinessHandle {
    check: Arc<MutableReadinessCheck>,
}

#[derive(Debug)]
struct MutableReadinessCheck {
    name: &'static str,
    state: Mutex<ReadinessCheckResult>,
}

#[async_trait]
impl ReadinessCheck for MutableReadinessCheck {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn check(&self) -> Result<ReadinessCheckResult, ObservabilityError> {
        Ok(self.state.lock().unwrap().clone())
    }
}

impl ReadinessHandle {
    /// Update the check state with an optional sanitized message.
    pub fn update(&self, status: ReadinessStatus, message: Option<String>) {
        *self.check.state.lock().unwrap() = ReadinessCheckResult {
            status,
            message: message.map(sanitize_readiness_message),
        };
    }

    /// Mark the component ready and clear any previous message.
    pub fn ready(&self) {
        self.update(ReadinessStatus::Ready, None);
    }

    /// Mark the component degraded with an operator-facing message.
    pub fn degraded(&self, message: impl Into<String>) {
        self.update(ReadinessStatus::Degraded, Some(message.into()));
    }

    /// Mark the component not ready with an operator-facing message.
    pub fn not_ready(&self, message: impl Into<String>) {
        self.update(ReadinessStatus::NotReady, Some(message.into()));
    }

    /// Expose this mutable handle as a registry check.
    pub fn as_check(&self) -> Arc<dyn ReadinessCheck> {
        self.check.clone() as Arc<dyn ReadinessCheck>
    }
}

impl std::fmt::Debug for ReadinessRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadinessRegistry")
            .field("checks", &self.checks.len())
            .finish()
    }
}

impl ReadinessRegistry {
    /// Construct a registry with no checks.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Construct a registry from explicit async checks.
    pub fn new(checks: Vec<Arc<dyn ReadinessCheck>>) -> Self {
        Self {
            checks: Arc::new(checks),
        }
    }

    /// Construct a registry containing one mutable check.
    pub fn mutable(
        name: &'static str,
        initial_status: ReadinessStatus,
        message: Option<String>,
    ) -> (Self, ReadinessHandle) {
        let handle = ReadinessHandle {
            check: Arc::new(MutableReadinessCheck {
                name,
                state: Mutex::new(ReadinessCheckResult {
                    status: initial_status,
                    message: message.map(sanitize_readiness_message),
                }),
            }),
        };
        (
            Self::new(vec![handle.check.clone() as Arc<dyn ReadinessCheck>]),
            handle,
        )
    }

    /// Construct a registry from mutable handles.
    pub fn from_handles(handles: Vec<ReadinessHandle>) -> Self {
        Self::new(
            handles
                .into_iter()
                .map(|handle| handle.as_check())
                .collect(),
        )
    }

    /// Run all checks sequentially and return serialized results.
    ///
    /// A failing check implementation is treated as `NotReady` so one broken
    /// dependency cannot prevent the readiness endpoint from responding.
    pub async fn check_all(&self) -> Vec<ReadinessResult> {
        let mut results = Vec::with_capacity(self.checks.len());
        for check in self.checks.iter() {
            let started_at = Instant::now();
            let checked = check.check().await.unwrap_or_else(|error| {
                ReadinessCheckResult::with_message(ReadinessStatus::NotReady, error.to_string())
            });
            results.push(ReadinessResult {
                name: check.name().to_string(),
                status: checked.status,
                message: checked.message,
                checked_at_unix_seconds: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                latency_millis: duration_millis(started_at.elapsed()),
            });
        }
        results
    }
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn sanitize_readiness_message(message: String) -> String {
    let mut sanitized = message;
    for marker in ["password", "token", "secret", "credential", "authorization"] {
        if sanitized.to_lowercase().contains(marker) {
            sanitized = "[redacted]".to_string();
            break;
        }
    }
    sanitized
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::*;

    #[derive(Debug)]
    struct StaticCheck {
        status: ReadinessStatus,
    }

    #[async_trait]
    impl ReadinessCheck for StaticCheck {
        fn name(&self) -> &'static str {
            "static"
        }

        async fn check(&self) -> Result<ReadinessCheckResult, ObservabilityError> {
            Ok(ReadinessCheckResult::new(self.status))
        }
    }

    #[tokio::test]
    async fn mutable_handle_updates_background_loop_status() {
        let (registry, handle) =
            ReadinessRegistry::mutable("runtime_loop", ReadinessStatus::NotReady, None);

        handle.ready();
        let results = registry.check_all().await;

        assert_eq!(results[0].name, "runtime_loop");
        assert_eq!(results[0].status, ReadinessStatus::Ready);
        assert!(results[0].message.is_none());
    }

    #[tokio::test]
    async fn readiness_result_includes_latency_and_sanitized_messages() {
        let (registry, handle) =
            ReadinessRegistry::mutable("storage", ReadinessStatus::Ready, None);
        handle.not_ready("token expired for dependency");

        let results = registry.check_all().await;

        assert_eq!(results[0].status, ReadinessStatus::NotReady);
        assert_eq!(results[0].message.as_deref(), Some("[redacted]"));
        assert!(results[0].checked_at_unix_seconds > 0);
    }

    #[tokio::test]
    async fn registry_accepts_static_and_mutable_checks() {
        let (_, mutable) =
            ReadinessRegistry::mutable("projection", ReadinessStatus::Degraded, None);
        let registry = ReadinessRegistry::new(vec![
            Arc::new(StaticCheck {
                status: ReadinessStatus::Ready,
            }),
            mutable.check.clone() as Arc<dyn ReadinessCheck>,
        ]);

        let results = registry.check_all().await;

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].status, ReadinessStatus::Ready);
        assert_eq!(results[1].status, ReadinessStatus::Degraded);
    }
}
