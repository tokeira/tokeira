CREATE TABLE IF NOT EXISTS worker_compute_controller (
    namespace_id UUID NOT NULL,
    deployment_name TEXT NOT NULL,
    build_id TEXT NOT NULL,
    revision BIGINT NOT NULL,
    active BOOLEAN NOT NULL,
    slot SMALLINT,
    next_metrics_poll_at TIMESTAMPTZ,
    lease_owner UUID,
    lease_epoch BIGINT NOT NULL,
    lease_until TIMESTAMPTZ,
    record_data BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (namespace_id, deployment_name, build_id)
)
