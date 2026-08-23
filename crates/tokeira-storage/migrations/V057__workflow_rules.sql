CREATE TABLE IF NOT EXISTS workflow_rules (
    namespace_id UUID NOT NULL,
    rule_id TEXT NOT NULL,
    expiration_time TIMESTAMPTZ,
    record_data BYTEA NOT NULL,
    PRIMARY KEY (namespace_id, rule_id)
)
