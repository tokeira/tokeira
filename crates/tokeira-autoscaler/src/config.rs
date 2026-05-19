//! Autoscaler configuration model.
//!
//! # Relationship between polling interval, consecutive samples, and reaction time
//!
//! The autoscaler's effective reaction time is:
//!
//!   `reaction_time = polling_interval × consecutive_samples`
//!
//! For example, with the defaults (15s poll, 2 scale-out samples), the
//! autoscaler takes 30s to react to sustained scale-out pressure. Scale-in
//! uses 6 samples (90s) because premature scale-in is operationally more
//! dangerous than a brief over-provision.
//!
//! The cooldown period is separate from hysteresis — it prevents ANY scaling
//! action (in either direction) for a fixed window after the last action.
//! This gives the system time to stabilize after a change before the
//! autoscaler re-evaluates. Without cooldown, rapid successive scale-outs
//! could overshoot before the new capacity has time to absorb load.
//!
//! # DSQL budget fields
//!
//! The `dsql_connection_budget` and `dsql_connection_rate_budget` fields
//! represent the cluster-wide DSQL limits that the autoscaler must respect.
//! The `per_runtime_*` fields represent how much of that budget each new
//! runtime host consumes. Together they define the scaling envelope (see
//! `envelope.rs`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutoscalerServiceConfig {
    #[serde(with = "duration_secs")]
    pub polling_interval: Duration,
    pub scale_out_consecutive_samples: u32,
    pub scale_in_consecutive_samples: u32,
    #[serde(with = "duration_secs")]
    pub cooldown: Duration,
    pub mimir_endpoint: String,
    #[serde(with = "duration_secs")]
    pub staleness_threshold: Duration,
    pub dsql_connection_budget: u32,
    pub dsql_connection_rate_budget: u32,
    pub per_runtime_reserved_connections: u32,
    pub per_runtime_startup_connection_rate: u32,
    pub cluster_name: String,
    pub service_configs: BTreeMap<String, ServiceScaleConfig>,
}

/// Per-service scaling bounds and step size.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceScaleConfig {
    pub min: u32,
    pub max: u32,
    pub step: u32,
}

impl Default for AutoscalerServiceConfig {
    fn default() -> Self {
        Self {
            polling_interval: Duration::seconds(15),
            scale_out_consecutive_samples: 2,
            scale_in_consecutive_samples: 6,
            cooldown: Duration::minutes(5),
            mimir_endpoint: "http://mimir.tokeira.local:9009".into(),
            staleness_threshold: Duration::seconds(45),
            dsql_connection_budget: 10_000,
            dsql_connection_rate_budget: 1_000,
            per_runtime_reserved_connections: 64,
            per_runtime_startup_connection_rate: 10,
            cluster_name: "tokeira".into(),
            service_configs: default_service_configs(),
        }
    }
}

fn default_service_configs() -> BTreeMap<String, ServiceScaleConfig> {
    [
        ("tokeira-edge-api", 1, 16, 1),
        ("tokeira-edge-poll", 1, 16, 1),
        ("tokeira-projection", 1, 8, 1),
        ("tokeira-controller", 1, 3, 1),
        ("tokeira-autoscaler", 1, 3, 1),
    ]
    .into_iter()
    .map(|(name, min, max, step)| (name.into(), ServiceScaleConfig { min, max, step }))
    .collect()
}

mod duration_secs {
    use serde::{Deserialize, Deserializer, Serializer};
    use time::Duration;

    pub(super) fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(duration.whole_seconds())
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Duration::seconds(i64::deserialize(deserializer)?))
    }
}
