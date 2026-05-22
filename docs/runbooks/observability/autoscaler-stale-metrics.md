# Autoscaler Stale Metrics

The autoscaler is suppressing decisions because input metrics are stale.

Likely causes: Mimir query failures, scrape delays, missing Alloy targets, or backend ingestion lag.

First dashboard: Autoscaler.

First queries: `tokeira_autoscaler_stale_metrics_total`, `tokeira_autoscaler_metric_freshness_age_seconds`, `tokeira_autoscaler_mimir_query_duration_seconds`.

Safe remediation: verify Mimir queries directly, inspect Alloy scrape health, and avoid manual scale-in while metrics freshness is degraded.

Escalate if stale metrics persist and service pressure is rising.

Related alerts: ScrapeFailing, TelemetryIngestionStalled.
