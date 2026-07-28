CREATE TABLE IF NOT EXISTS worker_compute_queue_sample (
    namespace_id UUID NOT NULL,
    deployment_name TEXT NOT NULL,
    build_id TEXT NOT NULL,
    task_type SMALLINT NOT NULL,
    task_queue TEXT NOT NULL,
    writer_id UUID NOT NULL,
    writer_sequence BIGINT NOT NULL,
    backlog_count BIGINT NOT NULL,
    add_rate DOUBLE PRECISION NOT NULL,
    dispatch_rate DOUBLE PRECISION NOT NULL,
    sampled_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (namespace_id, deployment_name, build_id, task_type, task_queue)
)
