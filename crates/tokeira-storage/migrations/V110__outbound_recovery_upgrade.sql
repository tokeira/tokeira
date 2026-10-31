CREATE TABLE IF NOT EXISTS outbound_recovery_upgrade (
    name TEXT PRIMARY KEY,
    algorithm_version INTEGER NOT NULL,
    revision BIGINT NOT NULL,
    phase SMALLINT NOT NULL,
    progress_data BYTEA NOT NULL
);
