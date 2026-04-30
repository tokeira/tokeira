CREATE TABLE IF NOT EXISTS shard_lease (
    shard_id      UUID        NOT NULL,
    owner         TEXT        NOT NULL,
    epoch         BIGINT      NOT NULL,
    lease_expiry  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (shard_id)
);
