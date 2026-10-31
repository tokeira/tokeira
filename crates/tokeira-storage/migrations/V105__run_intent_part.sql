CREATE TABLE IF NOT EXISTS run_intent_part (
    source_run_key UUID NOT NULL,
    intent_id TEXT NOT NULL,
    part_no INTEGER NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (source_run_key, intent_id, part_no)
);
