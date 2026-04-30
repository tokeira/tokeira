CREATE TABLE IF NOT EXISTS dispatch_backlog (
    partition_id    INTEGER     NOT NULL,
    queue_namespace UUID        NOT NULL,
    queue_name      TEXT        NOT NULL,
    insertion_seq   BIGINT      NOT NULL,
    run_key         UUID        NOT NULL,
    payload_data    BYTEA       NOT NULL,
    scheduled_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (partition_id, queue_namespace, queue_name, insertion_seq)
);
