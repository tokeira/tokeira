# Autoscaler Active Reconciler Absent

No autoscaler instance currently holds the active reconciliation lease.

Likely causes: autoscaler process down, lease repository unavailable, or DSQL coordination failure.

First dashboard: Autoscaler.

First queries: `tokeira_autoscaler_active_reconciler_lease_held`, autoscaler logs for lease errors.

Safe remediation: restart one autoscaler instance, verify DSQL endpoint and credentials, and check lease table health.

Escalate if no reconciler can acquire the lease after restart.

Related alerts: AutoscalerStaleMetrics.
