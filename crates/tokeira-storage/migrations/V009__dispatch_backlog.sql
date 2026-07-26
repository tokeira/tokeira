CREATE TABLE IF NOT EXISTS dispatch_backlog (
    key             UUID        NOT NULL,
    partition_id    INTEGER     NOT NULL,
    queue_namespace UUID        NOT NULL,
    queue_name      TEXT        NOT NULL,
    task_kind       SMALLINT    NOT NULL,
    deployment      TEXT,
    build_id        TEXT,
    priority_key    SMALLINT    NOT NULL,
    fair_pass       BIGINT      NOT NULL,
    insertion_tie   BIGINT      NOT NULL,
    run_key         UUID        NOT NULL,
    payload_data    BYTEA       NOT NULL,
    scheduled_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (key)
);
