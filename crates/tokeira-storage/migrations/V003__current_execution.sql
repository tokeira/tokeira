CREATE TABLE IF NOT EXISTS current_execution (
    namespace_id  UUID        NOT NULL,
    workflow_id   TEXT        NOT NULL,
    run_key       UUID        NOT NULL,
    run_id        TEXT        NOT NULL,
    is_open       BOOLEAN     NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, workflow_id)
);
