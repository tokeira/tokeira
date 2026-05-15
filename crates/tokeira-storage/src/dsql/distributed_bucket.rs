//! DynamoDB-backed global connection creation token bucket.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use aws_sdk_dynamodb::{Client, types::AttributeValue};

use crate::metrics;

const BUCKET_KEY: &str = "bucket#global";
const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

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
        let replenished =
            (current.tokens + elapsed_seconds * self.rate_per_second).min(self.burst_capacity);
        metrics::record_dsql_pool_rate_limiter(replenished, self.rate_per_second);
        metrics::set_dsql_rate_limiter_tokens_remaining(replenished);

        if replenished < 1.0 {
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
