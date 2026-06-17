//! Scenario configuration, resolved from the environment.
//!
//! The driver plays both the client (start/describe/terminate) and the worker
//! (poll/respond) against one running `tokeirad`, so a single config object covers
//! the address, namespace, and task queue all three roles share.

#![allow(dead_code)]

/// Connection + identity settings for the driver.
pub struct ScenarioConfig {
    /// Server address (`TEMPORAL_ADDRESS`, default `http://127.0.0.1:7233`).
    ///
    /// The default is IPv4 loopback on purpose: `tokeirad` binds `0.0.0.0:7233`
    /// (IPv4 wildcard), which does not accept IPv6 `[::1]` connections on macOS, so
    /// an IPv6 default would fail to connect against a default server.
    pub address: String,
    /// Namespace name the activities live in (`TEMPORAL_NAMESPACE`, default
    /// `default`). The server resolves this name to its internal namespace id.
    pub namespace: String,
    /// Task queue the activities are scheduled on and the worker polls
    /// (`SCENARIO_TASK_QUEUE`, default `standalone-activities`).
    pub task_queue: String,
    /// Worker/client identity recorded on requests.
    pub identity: String,
}

impl ScenarioConfig {
    /// Resolve configuration from the environment, applying defaults.
    pub fn from_env() -> Self {
        Self {
            address: env_or("TEMPORAL_ADDRESS", "http://127.0.0.1:7233"),
            namespace: env_or("TEMPORAL_NAMESPACE", "default"),
            task_queue: env_or("SCENARIO_TASK_QUEUE", "standalone-activities"),
            identity: env_or("SCENARIO_IDENTITY", "standalone-activities-scenario"),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}
