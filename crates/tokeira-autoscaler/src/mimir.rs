//! Mimir client for reading autoscaler metrics.
//!
//! # Why Mimir instead of Prometheus directly?
//!
//! The autoscaler needs a global view of metrics across all runtime hosts.
//! Individual Prometheus instances (or Alloy scrapers) hold only local data.
//! Mimir provides a federated query layer that aggregates metrics from all
//! scrapers into a single queryable endpoint, giving the autoscaler the
//! cluster-wide perspective it needs for correct scaling decisions.
//!
//! Additionally, Mimir's multi-tenant architecture means the autoscaler's
//! queries don't compete with dashboard queries for Prometheus resources.
//!
//! # Staleness classification
//!
//! Metric samples are classified by age relative to a configurable threshold:
//! - **Fresh** — within the threshold, safe to use for scaling decisions.
//! - **Stale** — older than the threshold, may not reflect current state.
//!   The freshness policy blocks scale-in when metrics are stale.
//! - **Missing** — no sample returned at all. Treated as the most dangerous
//!   state because it could mean the metrics pipeline is broken.
//!
//! The staleness threshold should be set slightly above `2 × scrape_interval`
//! to account for one missed scrape without triggering false staleness alerts.

use anyhow::{Context, Result};
use serde::Deserialize;
use time::{Duration, OffsetDateTime};

use crate::freshness::MetricFreshness;

#[derive(Debug, Clone)]
pub struct MimirClient {
    endpoint: String,
    client: reqwest::Client,
    staleness_threshold: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetricSample {
    pub value: f64,
    pub timestamp: OffsetDateTime,
}

impl MimirClient {
    pub fn new(endpoint: String, staleness_threshold: Duration) -> Self {
        Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            client: reqwest::Client::new(),
            staleness_threshold,
        }
    }

    pub async fn query_instant(&self, query: &str) -> Result<MetricFreshness> {
        let response: PromQueryResponse = self
            .client
            .get(format!("{}/api/v1/query", self.endpoint))
            .query(&[("query", query)])
            .send()
            .await
            .context("failed to query Mimir instant endpoint")?
            .error_for_status()
            .context("Mimir instant query failed")?
            .json()
            .await
            .context("failed to decode Mimir instant response")?;
        let Some(sample) = response.into_samples().into_iter().next() else {
            return Ok(MetricFreshness::Missing);
        };
        classify_sample(sample, self.staleness_threshold)
    }

    pub async fn query_range(
        &self,
        query: &str,
        range: Duration,
        step: Duration,
    ) -> Result<Vec<MetricSample>> {
        let end = OffsetDateTime::now_utc();
        let start = end - range;
        let response: PromQueryResponse = self
            .client
            .get(format!("{}/api/v1/query_range", self.endpoint))
            .query(&[
                ("query", query.to_owned()),
                ("start", start.unix_timestamp().to_string()),
                ("end", end.unix_timestamp().to_string()),
                ("step", step.whole_seconds().max(1).to_string()),
            ])
            .send()
            .await
            .context("failed to query Mimir range endpoint")?
            .error_for_status()
            .context("Mimir range query failed")?
            .json()
            .await
            .context("failed to decode Mimir range response")?;
        Ok(response.into_samples())
    }

    pub async fn is_available(&self) -> bool {
        self.client
            .get(format!("{}/-/ready", self.endpoint))
            .send()
            .await
            .map(|response| response.status().is_success())
            .unwrap_or(false)
    }
}

fn classify_sample(sample: MetricSample, threshold: Duration) -> Result<MetricFreshness> {
    let age = OffsetDateTime::now_utc() - sample.timestamp;
    if age <= threshold {
        Ok(MetricFreshness::Fresh)
    } else {
        Ok(MetricFreshness::Stale)
    }
}

#[derive(Debug, Deserialize)]
struct PromQueryResponse {
    status: String,
    data: PromData,
}

impl PromQueryResponse {
    fn into_samples(self) -> Vec<MetricSample> {
        if self.status != "success" {
            // Scaling treats failed/malformed query results as missing data so
            // freshness policy can block unsafe scale-in instead of inventing
            // a zero-valued signal.
            return Vec::new();
        }
        match self.data.result {
            PromResultSet::Vector(results) | PromResultSet::Matrix(results) => results
                .into_iter()
                .flat_map(PromSeries::into_samples)
                .collect(),
            PromResultSet::Scalar(value) => parse_prom_value(value).into_iter().collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct PromData {
    result: PromResultSet,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PromResultSet {
    Vector(Vec<PromSeries>),
    Matrix(Vec<PromSeries>),
    Scalar(PromValue),
}

#[derive(Debug, Deserialize)]
struct PromSeries {
    #[serde(default)]
    value: Option<PromValue>,
    #[serde(default)]
    values: Vec<PromValue>,
}

impl PromSeries {
    fn into_samples(self) -> Vec<MetricSample> {
        self.value
            .into_iter()
            .chain(self.values)
            .filter_map(parse_prom_value)
            .collect()
    }
}

type PromValue = (f64, String);

fn parse_prom_value(value: PromValue) -> Option<MetricSample> {
    let timestamp = OffsetDateTime::from_unix_timestamp(value.0 as i64).ok()?;
    let value = value.1.parse().ok()?;
    Some(MetricSample { value, timestamp })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_prometheus_vector_response() {
        let response: PromQueryResponse = serde_json::from_str(
            r#"{"status":"success","data":{"result":[{"value":[1700000000,"42"]}]}}"#,
        )
        .expect("valid response");

        let samples = response.into_samples();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].value, 42.0);
    }
}
