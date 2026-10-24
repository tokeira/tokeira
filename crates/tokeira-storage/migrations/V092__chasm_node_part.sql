CREATE TABLE IF NOT EXISTS chasm_node_part (
    namespace_id UUID NOT NULL,
    business_id TEXT NOT NULL,
    run_id UUID NOT NULL,
    encoded_path TEXT NOT NULL,
    part_no INTEGER NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (namespace_id, business_id, run_id, encoded_path, part_no)
);
