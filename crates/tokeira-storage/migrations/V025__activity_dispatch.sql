CREATE TABLE IF NOT EXISTS activity_dispatch (
    key UUID NOT NULL,
    run_key UUID NOT NULL,
    activity_id TEXT NOT NULL,
    shard_id UUID NOT NULL,
    queue_namespace UUID NOT NULL,
    queue_name TEXT NOT NULL,
    task_kind SMALLINT NOT NULL,
    deployment TEXT,
    build_id TEXT,
    schedule_event_id BIGINT NOT NULL,
    attempt INTEGER NOT NULL,
    input_data BYTEA NOT NULL,
    priority_data BYTEA,
    dispatch_revision BIGINT NOT NULL DEFAULT 0,
    stamp BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (key)
);
