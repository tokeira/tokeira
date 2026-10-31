CREATE TABLE IF NOT EXISTS run_intent (
    source_run_key UUID NOT NULL,
    intent_id TEXT NOT NULL,
    source_home UUID NOT NULL,
    close_seq BIGINT NOT NULL,
    kind SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    payload BYTEA NOT NULL,
    payload_parts INTEGER NOT NULL,
    PRIMARY KEY (source_run_key, intent_id)
);
