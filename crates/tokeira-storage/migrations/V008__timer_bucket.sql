CREATE TABLE IF NOT EXISTS timer_bucket (
    shard_id    UUID        NOT NULL,
    fire_at     TIMESTAMPTZ NOT NULL,
    run_key     UUID        NOT NULL,
    timer_id    TEXT        NOT NULL,
    timer_data  BYTEA       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (shard_id, fire_at, run_key, timer_id)
);
