use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use http::HeaderMap;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{
    errors::{EdgeError, EdgeResult},
    interceptors::{Action, EdgeInterceptors},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthState {
    Serving,
    Degraded,
    NotServing,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub component: String,
    pub state: HealthState,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthSnapshot {
    pub observed_at: OffsetDateTime,
    pub overall: HealthState,
    pub components: Vec<ComponentHealth>,
}

#[async_trait]
pub trait HealthReporter: Send + Sync + 'static {
    async fn snapshot(&self) -> Result<HealthSnapshot>;
}

#[derive(Debug, Default)]
pub struct StaticHealthReporter;

#[async_trait]
impl HealthReporter for StaticHealthReporter {
    async fn snapshot(&self) -> Result<HealthSnapshot> {
        Ok(HealthSnapshot {
            observed_at: OffsetDateTime::now_utc(),
            overall: HealthState::Serving,
            components: vec![ComponentHealth {
                component: "edge".to_string(),
                state: HealthState::Serving,
                detail: Some("request admission operational".to_string()),
            }],
        })
    }
}

/// Tiny health service wrapper.
///
/// Health is a surprisingly important boundary: operators and load balancers hit
/// it constantly, and it must stay cheap even during partial outages.
#[derive(Clone)]
pub struct HealthService {
    reporter: Arc<dyn HealthReporter>,
    interceptors: Arc<EdgeInterceptors>,
}

impl std::fmt::Debug for HealthService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthService").finish_non_exhaustive()
    }
}

impl HealthService {
    pub fn new(reporter: Arc<dyn HealthReporter>, interceptors: Arc<EdgeInterceptors>) -> Self {
        Self {
            reporter,
            interceptors,
        }
    }

    pub async fn check(&self, headers: &HeaderMap) -> EdgeResult<HealthSnapshot> {
        let _ctx = self
            .interceptors
            .begin(headers, None, Action::HealthRead, false)
            .await?;

        self.reporter.snapshot().await.map_err(EdgeError::from)
    }
}
