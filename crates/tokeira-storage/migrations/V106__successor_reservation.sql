CREATE TABLE IF NOT EXISTS successor_reservation (
    key UUID PRIMARY KEY,
    namespace_id UUID NOT NULL,
    workflow_id TEXT NOT NULL,
    source_home UUID NOT NULL,
    predecessor_run_key UUID NOT NULL,
    predecessor_close_seq BIGINT NOT NULL,
    successor_run_key UUID NOT NULL,
    intent_id TEXT NOT NULL
);
