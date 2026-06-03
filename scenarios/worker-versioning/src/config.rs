//! Shared scenario configuration, resolved from the environment.
//!
//! Both the worker and the starter read the same deployment name, task queue,
//! server address, and namespace so they agree on what they are operating on.

#![allow(dead_code)]

/// Connection + identity settings shared by the worker and starter binaries.
pub struct ScenarioConfig {
    /// Server address (`TEMPORAL_ADDRESS`, default `http://[::1]:7233`).
    pub address: String,
    /// Namespace (`TEMPORAL_NAMESPACE`, default `default`).
    pub namespace: String,
    /// Worker Deployment name (`SCENARIO_DEPLOYMENT`, default `orders`).
    pub deployment: String,
    /// Shared task queue (`SCENARIO_TASK_QUEUE`, default `orders`).
    pub task_queue: String,
}

impl ScenarioConfig {
    /// Resolve configuration from the environment, applying defaults.
    pub fn from_env() -> Self {
        Self {
            address: env_or("TEMPORAL_ADDRESS", "http://[::1]:7233"),
            namespace: env_or("TEMPORAL_NAMESPACE", "default"),
            deployment: env_or("SCENARIO_DEPLOYMENT", "orders"),
            task_queue: env_or("SCENARIO_TASK_QUEUE", "orders"),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

/// Build ids the scenario operates on.
pub const BUILD_V1: &str = "1.0";
pub const BUILD_V2: &str = "2.0";
