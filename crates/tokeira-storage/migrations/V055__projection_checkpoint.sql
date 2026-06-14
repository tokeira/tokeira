CREATE TABLE IF NOT EXISTS projection_checkpoint (
    partition_id          INT         NOT NULL,
    last_applied_version  BYTEA       NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (partition_id)
);
