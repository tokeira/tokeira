# DSQL Reservoir Exhaustion

The DSQL reservoir is close to empty or highly utilized.

Likely causes: sudden workflow load increase, connection creation throttling, or long-held DSQL permits.

First dashboard: DSQL Connection Health.

First queries: `tokeira_dsql_reservoir_utilization_ratio`, `tokeira_dsql_pool_empty_reservoir_total`, `tokeira_dsql_pool_class_waiters`.

Safe remediation: inspect class waiters, check for leak alerts, temporarily reduce admission pressure, and scale runtime capacity only after confirming DSQL slot capacity.

Escalate if utilization remains above threshold for more than 15 minutes.

Related alerts: DsqlConnectionLeakDetected, DsqlRateLimiterThrottling, DsqlClassBudgetSaturation.
