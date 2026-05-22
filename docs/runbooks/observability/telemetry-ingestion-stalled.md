# Telemetry Ingestion Stalled

No scrape targets are reporting healthy.

Likely causes: Alloy outage, Mimir remote-write failure, bad generated config, or network partition.

First dashboard: Infrastructure Health.

First queries: `up`, Alloy logs, and Mimir ingestion logs.

Safe remediation: restart Alloy after validating generated config, verify Mimir is reachable, and check local disk or network failures.

Escalate immediately because observability coverage is impaired.

Related alerts: ScrapeFailing.
