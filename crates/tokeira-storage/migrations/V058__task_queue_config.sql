CREATE TABLE task_queue_config (
    namespace_id UUID NOT NULL,
    task_queue TEXT NOT NULL,
    task_kind SMALLINT NOT NULL,
    revision BIGINT NOT NULL,
    record_data BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, task_queue, task_kind)
)
