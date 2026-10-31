CREATE TABLE IF NOT EXISTS run_operation (
    source_run_key UUID NOT NULL,
    kind SMALLINT NOT NULL,
    initiating_event_id BIGINT NOT NULL,
    source_home UUID NOT NULL,
    requested_event_id BIGINT NOT NULL,
    attempt BIGINT NOT NULL,
    due_at TIMESTAMPTZ NOT NULL,
    target_namespace UUID,
    target_workflow_id TEXT,
    PRIMARY KEY (source_run_key, kind, initiating_event_id)
);
