CREATE TABLE IF NOT EXISTS workflow_hot (
    run_key         UUID        NOT NULL,
    namespace_id    UUID        NOT NULL,
    workflow_id     TEXT        NOT NULL,
    shard_id        UUID        NOT NULL,
    transition_seq  BIGINT      NOT NULL,
    state_data      BYTEA       NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_key)
);
