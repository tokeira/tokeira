CREATE TABLE IF NOT EXISTS current_execution (
    key         UUID        NOT NULL,
    namespace_id UUID      NOT NULL,
    workflow_id TEXT       NOT NULL,
    run_key     UUID       NOT NULL,
    run_id      UUID       NOT NULL,
    is_open     BOOLEAN    NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (key)
);
