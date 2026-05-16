//! DynamoDB-backed global connection creation token bucket.
//!
//! Every refiller in the deployment shares one DynamoDB row. The row is updated
//! with a conditional write so concurrent nodes naturally serialize token
//! consumption without holding a long-lived lock. This protects the DSQL
//! endpoint and IAM token path from coordinated cold-start bursts.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use aws_sdk_dynamodb::{Client, types::AttributeValue};

use crate::metrics;

const BUCKET_KEY: &str = "bucket#global";
/// Upper bound for one wait call. A caller that cannot obtain a token inside
/// this window should surface backpressure instead of hiding an unhealthy
/// coordination table behind an indefinitely pending checkout.
const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const DISTRIBUTED_BURST: u64 = 1_000;

#[derive(Clone, Debug)]
pub struct DistributedTokenBucket {
    client: Client,
    table_name: String,
    owner_id: String,
    rate_per_second: f64,
    burst_capacity: f64,
    local_only: bool,
}

#[derive(Clone, Copy, Debug)]
struct BucketState {
    tokens: f64,
    updated_at_ms: u64,
}

impl DistributedTokenBucket {
    /// Construct the production distributed bucket.
    ///
    /// The `owner_id` is informational for diagnostics; correctness is provided
    /// by the conditional write on `updated_at_ms`.
    pub fn new(
        client: Client,
        table_name: impl Into<String>,
        owner_id: impl Into<String>,
        rate_per_second: f64,
        burst_capacity: u64,
    ) -> Self {
        Self {
            client,
            table_name: table_name.into(),
            owner_id: owner_id.into(),
            rate_per_second,
            burst_capacity: burst_capacity as f64,
            local_only: false,
        }
    }

    #[cfg(any(test, feature = "dsql-integration"))]
    pub fn local_for_tests(rate_per_second: f64, burst_capacity: u64) -> Self {
        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
            .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
            .build();
        Self {
            client: Client::from_conf(config),
            table_name: "local-test".to_owned(),
            owner_id: "local-test".to_owned(),
            rate_per_second,
            burst_capacity: burst_capacity as f64,
            local_only: true,
        }
    }

    pub async fn validate_table(&self) -> Result<()> {
        if self.local_only {
            return Ok(());
        }
        self.client
            .describe_table()
            .table_name(&self.table_name)
            .send()
            .await
            .with_context(|| {
                format!(
                    "failed to describe DSQL rate limiter DynamoDB table {}",
                    self.table_name
                )
            })?;
        Ok(())
    }

    pub async fn wait(&self) -> Result<()> {
        if self.local_only {
            return Ok(());
        }
        let started = Instant::now();
        loop {
            let wait_for = self.try_acquire().await?;
            if wait_for.is_zero() {
                return Ok(());
            }
            if started.elapsed().saturating_add(wait_for) > DEFAULT_WAIT_TIMEOUT {
                bail!("timed out waiting for DSQL distributed rate limiter token");
            }
            metrics::record_dsql_rate_limiter_throttled();
            let throttle_started = Instant::now();
            tokio::time::sleep(wait_for).await;
            metrics::record_dsql_rate_limiter_throttle_duration(throttle_started.elapsed());
        }
    }

    async fn try_acquire(&self) -> Result<Duration> {
        let current = self.read_state().await?;
        let now_ms = unix_millis();
        let elapsed_seconds = now_ms.saturating_sub(current.updated_at_ms) as f64 / 1000.0;
        let replenished = replenished_tokens(
            current.tokens,
            elapsed_seconds,
            self.rate_per_second,
            self.burst_capacity,
        );
        metrics::record_dsql_pool_rate_limiter(replenished, self.rate_per_second);
        metrics::set_dsql_rate_limiter_tokens_remaining(replenished);

        if replenished < 1.0 {
            // Return a caller-side sleep instead of sleeping under any DynamoDB
            // request context. That keeps the conditional write path short and
            // lets competing refillers make progress while this one waits.
            let deficit = 1.0 - replenished;
            let seconds = deficit / self.rate_per_second.max(f64::EPSILON);
            return Ok(Duration::from_secs_f64(seconds.max(0.01)));
        }

        let next_tokens = replenished - 1.0;
        match self
            .write_state(current.updated_at_ms, now_ms, next_tokens)
            .await
        {
            Ok(()) => Ok(Duration::ZERO),
            // Another node consumed the bucket state first. A short randomized
            // backoff would also work; the fixed delay is sufficient because
            // the token bucket itself controls global refill rate.
            Err(error) if is_conditional_check(&error) => Ok(Duration::from_millis(25)),
            Err(error) => Err(error),
        }
    }

    async fn read_state(&self) -> Result<BucketState> {
        let output = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("pk", AttributeValue::S(BUCKET_KEY.to_owned()))
            .consistent_read(true)
            .send()
            .await
            .with_context(|| {
                format!(
                    "failed to read DSQL rate limiter state from {}",
                    self.table_name
                )
            })?;
        let Some(item) = output.item() else {
            // First writer initializes from a full bucket. The following
            // conditional put prevents multiple cold-start nodes from all
            // successfully consuming that same initial capacity.
            return Ok(BucketState {
                tokens: self.burst_capacity,
                updated_at_ms: 0,
            });
        };
        Ok(BucketState {
            tokens: parse_f64(item.get("tokens")).unwrap_or(self.burst_capacity),
            updated_at_ms: parse_u64(item.get("updated_at_ms")).unwrap_or(0),
        })
    }

    async fn write_state(&self, expected_updated_at: u64, now_ms: u64, tokens: f64) -> Result<()> {
        // TTL is hygiene for abandoned bucket rows, not part of the rate-limit
        // correctness model. The conditional expression is the correctness
        // boundary.
        let ttl_epoch = unix_seconds().saturating_add(300);
        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("pk", AttributeValue::S(BUCKET_KEY.to_owned()))
            .item("owner_id", AttributeValue::S(self.owner_id.clone()))
            .item("tokens", AttributeValue::N(tokens.to_string()))
            .item("updated_at_ms", AttributeValue::N(now_ms.to_string()))
            .item("ttl_epoch", AttributeValue::N(ttl_epoch.to_string()))
            .condition_expression("attribute_not_exists(pk) OR updated_at_ms = :expected")
            .expression_attribute_values(
                ":expected",
                AttributeValue::N(expected_updated_at.to_string()),
            )
            .send()
            .await
            .with_context(|| {
                format!(
                    "failed to update DSQL rate limiter state in {}",
                    self.table_name
                )
            })?;
        metrics::record_dsql_pool_rate_limiter(tokens, self.rate_per_second);
        metrics::set_dsql_rate_limiter_tokens_remaining(tokens);
        Ok(())
    }
}

fn parse_f64(value: Option<&AttributeValue>) -> Option<f64> {
    value.and_then(|value| value.as_n().ok())?.parse().ok()
}

fn parse_u64(value: Option<&AttributeValue>) -> Option<u64> {
    value.and_then(|value| value.as_n().ok())?.parse().ok()
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn is_conditional_check(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().contains("ConditionalCheckFailed"))
}

fn replenished_tokens(current_tokens: f64, elapsed_seconds: f64, rate: f64, capacity: f64) -> f64 {
    (current_tokens + elapsed_seconds * rate).min(capacity)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn distributed_rate_burst_limit(
            current in 0.0f64..2_000.0,
            elapsed in 0.0f64..60.0,
            rate in 1.0f64..500.0,
        ) {
            let tokens = replenished_tokens(current, elapsed, rate, DISTRIBUTED_BURST as f64);
            prop_assert!(tokens <= DISTRIBUTED_BURST as f64);
        }
    }
}
