-- Serialization format: postcard for BYTEA columns.
CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER     NOT NULL,
    name        TEXT        NOT NULL,
    checksum    TEXT        NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (version)
);
