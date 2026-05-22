# DSQL OCC Conflict Spike

DSQL commits are seeing elevated serialization conflicts.

Likely causes: hot workflows, uneven shard routing, or a surge of concurrent commands for the same run set.

First dashboard: OCC Contention.

First queries: `sum(rate(tokeira_storage_dsql_occ_conflict_total[5m])) by (operation)`, `tokeira_storage_dsql_commit_retries`.

Safe remediation: inspect lane queue wait and hot operation labels, reduce benchmark concurrency if this is load testing, and verify run-key routing is stable.

Escalate if retries exhaust or conflict rate keeps rising after load stabilizes.

Related alerts: DsqlClassBudgetSaturation, ProjectionSinkErrorRate.
