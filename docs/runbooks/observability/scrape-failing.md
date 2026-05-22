# Scrape Failing

Alloy cannot scrape one or more telemetry targets.

Likely causes: process down, wrong metrics port, Service Connect/Docker discovery issue, or metrics endpoint disabled.

First dashboard: Infrastructure Health.

First queries: `up`, `up{target_kind="process"}`, `up{target_kind="infrastructure"}`.

Safe remediation: check the target container health, confirm `/metrics` is reachable from Alloy, and inspect Alloy logs.

Escalate if the failed scrape hides an active production incident.

Related alerts: TelemetryIngestionStalled.
