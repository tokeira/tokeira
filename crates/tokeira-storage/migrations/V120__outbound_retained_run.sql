CREATE TABLE IF NOT EXISTS outbound_retained_run (
    run_key UUID PRIMARY KEY,
    namespace_id UUID NOT NULL,
    workflow_id TEXT NOT NULL,
    state_data BYTEA NOT NULL,
    state_parts INTEGER
);
