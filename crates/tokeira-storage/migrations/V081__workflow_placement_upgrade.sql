CREATE TABLE IF NOT EXISTS workflow_placement_upgrade (
    name          TEXT   NOT NULL PRIMARY KEY,
    shard_count   BIGINT NOT NULL,
    revision      BIGINT NOT NULL,
    progress_data BYTEA  NOT NULL
);
