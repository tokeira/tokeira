# DSQL Connection Leak Detected

A DSQL checkout exceeded the leak detection deadline.

Likely causes: a stuck storage operation, dropped future, or code path holding a permit across unexpected work.

First dashboard: DSQL Connection Health.

First queries: `tokeira_dsql_connection_leak_detected_total`, `tokeira_dsql_connection_leak_suspects`, `tokeira_dsql_connection_checkout_overdue_seconds`.

Safe remediation: identify the bounded checkout call-site label, inspect correlated logs, and roll back recent storage-path changes if leaks align with a deployment.

Escalate immediately if suspects keep increasing.

Related alerts: DsqlReservoirExhaustion, DsqlClassBudgetSaturation.
