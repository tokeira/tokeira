# Projection Sink Error Rate

Projection sink writes are failing.

Likely causes: visibility schema mismatch, DSQL write failure, search attribute type mismatch, or checkpoint/storage outage.

First dashboard: Projection Workers.

First queries: `tokeira_projection_sink_error_total`, `tokeira_projection_records_processed_total`, projection worker logs.

Safe remediation: inspect bounded `error_kind`, verify recent schema migrations, and pause dependent visibility consumers if data is stale.

Escalate if sink errors persist after schema and storage checks.

Related alerts: ProjectionCheckpointLag.
