CREATE TABLE IF NOT EXISTS run_preparation (
    preparation_id UUID PRIMARY KEY,
    request_key TEXT NOT NULL,
    target_run_key UUID NOT NULL,
    run_id UUID NOT NULL,
    started_at_data BYTEA NOT NULL,
    source_home UUID NOT NULL,
    generation UUID NOT NULL,
    phase SMALLINT NOT NULL,
    manifest_version INTEGER NOT NULL,
    manifest_digest BYTEA NOT NULL,
    manifest_data BYTEA NOT NULL,
    manifest_parts INTEGER NOT NULL
);
