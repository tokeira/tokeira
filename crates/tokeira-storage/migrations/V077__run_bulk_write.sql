CREATE TABLE IF NOT EXISTS run_bulk_write (
    run_key UUID PRIMARY KEY,
    shard_id UUID NOT NULL,
    phase SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
