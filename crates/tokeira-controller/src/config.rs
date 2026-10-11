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
        // Nodes and durable state use one execution-home hash until an
        // explicit placement migration supports different cardinalities.
        if self.shard_count != self.bundle_count {
            return Err(ControllerConfigError::UnequalHomeCounts {
                shard_count: self.shard_count,
                bundle_count: self.bundle_count,
            });
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
    /// Different execution-home cardinalities cannot be served without migration.
    #[error(
        "placement.shard_count ({shard_count}) must equal placement.bundle_count ({bundle_count}) for workflow dispatch; preserve the existing durable placement count; automatic repartitioning is not supported"
    )]
    UnequalHomeCounts {
        /// Cardinality already used by durable execution placement.
        shard_count: u32,
        /// Cardinality advertised for acquired placement bundles.
        bundle_count: u32,
    },
    #[error("heartbeat_interval must be positive")]
    NonPositiveHeartbeatInterval,
    #[error("grace_interval must be at least heartbeat_interval")]
    GraceShorterThanHeartbeat,
    #[error("dsql_connection_rate_budget must be positive and finite")]
    InvalidRateBudget,
    #[error("dsql_connection_capacity_budget must be greater than zero")]
    ZeroCapacityBudget,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unequal_execution_home_counts_are_rejected() {
        for (shard_count, bundle_count) in [(8, 4), (4, 8)] {
            let config = ControllerConfig {
                shard_count,
                bundle_count,
                ..Default::default()
            };
            assert_eq!(
                config.validate(),
                Err(ControllerConfigError::UnequalHomeCounts {
                    shard_count,
                    bundle_count
                })
            );
            let message = config.validate().unwrap_err().to_string();
            assert!(message.contains(&format!("placement.shard_count ({shard_count})")));
            assert!(message.contains(&format!("placement.bundle_count ({bundle_count})")));
            assert!(message.contains("automatic repartitioning is not supported"));
        }
        ControllerConfig::default().validate().unwrap();
        ControllerConfig {
            shard_count: 8,
            bundle_count: 8,
            ..Default::default()
        }
        .validate()
        .unwrap();
    }
}
