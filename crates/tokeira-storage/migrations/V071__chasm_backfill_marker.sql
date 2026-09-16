-- Durable completion must not inherit schema-version or expiring-lease semantics.
CREATE TABLE IF NOT EXISTS chasm_backfill_marker (
    marker_name  TEXT        NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (marker_name)
);
