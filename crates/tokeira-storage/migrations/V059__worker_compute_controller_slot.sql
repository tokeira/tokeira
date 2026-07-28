CREATE TABLE IF NOT EXISTS worker_compute_controller_slot (
    namespace_id UUID NOT NULL,
    slot SMALLINT NOT NULL,
    deployment_name TEXT NOT NULL,
    build_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (namespace_id, slot)
)
