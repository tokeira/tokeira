# DSQL Rate Limiter Throttling

The distributed DSQL connection rate limiter is delaying connection creation.

Likely causes: process restart fan-out, slot lease churn, or aggressive reservoir refills.

First dashboard: DSQL Connection Health.

First queries: `tokeira_dsql_rate_limiter_throttled_total`, `tokeira_dsql_rate_limiter_tokens_remaining`, `tokeira_dsql_reservoir_refill_errors_total`.

Safe remediation: avoid restarting all runtimes at once, verify DynamoDB coordination availability, and let reservoirs refill gradually.

Escalate if throttling coincides with empty reservoirs.

Related alerts: DsqlReservoirExhaustion.
