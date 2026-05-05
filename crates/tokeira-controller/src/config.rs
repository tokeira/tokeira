//! Placement controller configuration.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::Duration;

/// Configuration for active-active placement controllers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerConfig {
    pub controller_addr: String,
    pub heartbeat_interval: Duration,
    pub grace_interval: Duration,
    pub snapshot_publish_interval: Duration,
    pub bundle_count: u32,
    pub partition_count: u32,
    pub shard_count: u32,
    pub hash_version: u32,
    pub budget_directive_validity: Duration,
    pub dsql_connection_rate_budget: f64,
    pub dsql_connection_capacity_budget: u64,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            controller_addr: "127.0.0.1:7240".to_owned(),
            heartbeat_interval: Duration::seconds(5),
            grace_interval: Duration::seconds(15),
            snapshot_publish_interval: Duration::seconds(1),
            bundle_count: 64,
            partition_count: 1024,
            shard_count: 64,
            hash_version: 1,
            budget_directive_validity: Duration::seconds(30),
            dsql_connection_rate_budget: 100.0,
            dsql_connection_capacity_budget: 10_000,
        }
    }
}

impl ControllerConfig {
    pub fn validate(&self) -> Result<(), ControllerConfigError> {
        if self.bundle_count == 0 {
            return Err(ControllerConfigError::ZeroBundleCount);
        }
        if self.partition_count == 0 {
            return Err(ControllerConfigError::ZeroPartitionCount);
        }
        if self.shard_count == 0 {
            return Err(ControllerConfigError::ZeroShardCount);
        }
        if self.heartbeat_interval <= Duration::ZERO {
            return Err(ControllerConfigError::NonPositiveHeartbeatInterval);
        }
        if self.grace_interval < self.heartbeat_interval {
            return Err(ControllerConfigError::GraceShorterThanHeartbeat);
        }
        if self.dsql_connection_rate_budget <= 0.0 || !self.dsql_connection_rate_budget.is_finite()
        {
            return Err(ControllerConfigError::InvalidRateBudget);
        }
        if self.dsql_connection_capacity_budget == 0 {
            return Err(ControllerConfigError::ZeroCapacityBudget);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ControllerConfigError {
    #[error("bundle_count must be greater than zero")]
    ZeroBundleCount,
    #[error("partition_count must be greater than zero")]
    ZeroPartitionCount,
    #[error("shard_count must be greater than zero")]
    ZeroShardCount,
    #[error("heartbeat_interval must be positive")]
    NonPositiveHeartbeatInterval,
    #[error("grace_interval must be at least heartbeat_interval")]
    GraceShorterThanHeartbeat,
    #[error("dsql_connection_rate_budget must be positive and finite")]
    InvalidRateBudget,
    #[error("dsql_connection_capacity_budget must be greater than zero")]
    ZeroCapacityBudget,
}
