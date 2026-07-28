CREATE TABLE IF NOT EXISTS worker_task_provenance (
    token_digest BYTEA NOT NULL,
    namespace_id UUID NOT NULL,
    normal_task_queue TEXT NOT NULL,
    task_class SMALLINT NOT NULL,
    deployment_name TEXT NOT NULL,
    build_id TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (token_digest)
)
