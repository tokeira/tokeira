//! The activity configuration (config-as-constant).
//!
//! Ground truth: `chasm/lib/activity/config.go @ v1.31.0`. Per `AGENTS`
//! Configuration, these are config-as-constant values with no env vars: defaults
//! characterized by behaviour, `serde(deny_unknown_fields)` so typos fail at parse
//! time, and a lossless round-trip (Requirement 11.11, 11.12).

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Default long-poll timeout (`activity.longPollTimeout = 20s @ v1.31.0`).
const DEFAULT_LONG_POLL_TIMEOUT: Duration = Duration::from_secs(20);
/// Default long-poll buffer (`activity.longPollBuffer = 1s @ v1.31.0`).
const DEFAULT_LONG_POLL_BUFFER: Duration = Duration::from_secs(1);

fn default_long_poll_timeout() -> Duration {
    DEFAULT_LONG_POLL_TIMEOUT
}

fn default_long_poll_buffer() -> Duration {
    DEFAULT_LONG_POLL_BUFFER
}

/// Standalone-activity configuration.
///
/// `enable_standalone` defaults to `false` per namespace (the edge gate consults
/// it — Requirement 11.10, 11.11); the long-poll values feed the engine's
/// monotonic long-poll. Unknown fields are rejected (Requirement 11.12).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityConfig {
    /// Whether standalone activities are admitted for the namespace. Default
    /// `false`.
    #[serde(default)]
    pub enable_standalone: bool,
    /// How long a poll blocks before returning empty. Default 20s.
    #[serde(default = "default_long_poll_timeout")]
    pub long_poll_timeout: Duration,
    /// Slack subtracted from the timeout so a client can resubmit. Default 1s.
    #[serde(default = "default_long_poll_buffer")]
    pub long_poll_buffer: Duration,
}

impl Default for ActivityConfig {
    fn default() -> Self {
        Self {
            enable_standalone: false,
            long_poll_timeout: DEFAULT_LONG_POLL_TIMEOUT,
            long_poll_buffer: DEFAULT_LONG_POLL_BUFFER,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_v1_31_0() {
        let config = ActivityConfig::default();
        assert!(!config.enable_standalone);
        assert_eq!(config.long_poll_timeout, Duration::from_secs(20));
        assert_eq!(config.long_poll_buffer, Duration::from_secs(1));
    }

    #[test]
    fn round_trips_without_loss() {
        let config = ActivityConfig {
            enable_standalone: true,
            long_poll_timeout: Duration::from_secs(30),
            long_poll_buffer: Duration::from_millis(500),
        };
        let json = serde_json::to_string(&config).expect("serialize");
        let back: ActivityConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(config, back);
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let err =
            serde_json::from_str::<ActivityConfig>(r#"{"enable_standalone": true, "bogus": 1}"#);
        assert!(err.is_err());
    }

    #[test]
    fn missing_fields_use_defaults() {
        let config: ActivityConfig = serde_json::from_str("{}").expect("deserialize");
        assert_eq!(config, ActivityConfig::default());
    }
}
