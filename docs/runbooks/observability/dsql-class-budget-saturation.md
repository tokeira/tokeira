# DSQL Class Budget Saturation

One DSQL class budget is saturated.

Likely causes: commit-heavy traffic, projection backlog, control-plane pressure, or an imbalanced class allocation.

First dashboard: DSQL Connection Health.

First queries: `tokeira_dsql_pool_class_in_use`, `tokeira_dsql_pool_class_budget_total`, `tokeira_dsql_pool_class_waiters`.

Safe remediation: identify the saturated class, inspect matching operation latency, and reduce load in the affected path before changing budget allocations.

Escalate if saturation blocks workflow completions or projection progress.

Related alerts: DsqlReservoirExhaustion, ProjectionCheckpointLag.
