use serde::{Deserialize, Serialize};
use time::Duration;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub initial_interval: Duration,
    pub backoff_coefficient: f64,
    pub maximum_interval: Option<Duration>,
    pub maximum_attempts: u32,
    pub non_retryable_error_types: Vec<String>,
}
