CREATE TABLE IF NOT EXISTS projector_checkpoint (
    sink_id              TEXT        NOT NULL,
    partition_id         INTEGER     NOT NULL,
    fanout               SMALLINT    NOT NULL,
    last_applied_cursor  BYTEA       NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sink_id, partition_id, fanout)
);
