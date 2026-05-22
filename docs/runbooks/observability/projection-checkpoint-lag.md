# Projection Checkpoint Lag

Projection workers have not written a checkpoint recently.

Likely causes: empty or stalled projection loop, sink failures, checkpoint store errors, or high DSQL latency.

First dashboard: Projection Workers.

First queries: `tokeira_projection_checkpoint_lag_seconds`, `tokeira_projection_worker_lag_records`, `tokeira_projection_sink_error_total`.

Safe remediation: inspect sink errors, verify checkpoint store availability, and confirm projection workers are running.

Escalate if visibility freshness is user-visible.

Related alerts: ProjectionSinkErrorRate, DsqlClassBudgetSaturation.
