CREATE TABLE IF NOT EXISTS schema_compatibility (
    schema_version INTEGER NOT NULL,
    tokeira_release TEXT NOT NULL,
    migration_set_digest TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_version)
);
