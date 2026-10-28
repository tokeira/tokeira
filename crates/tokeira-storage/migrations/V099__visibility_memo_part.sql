CREATE TABLE IF NOT EXISTS visibility_memo_part (
    namespace_id UUID NOT NULL,
    archetype_id BIGINT NOT NULL,
    run_key UUID NOT NULL,
    part_no INTEGER NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (namespace_id, archetype_id, run_key, part_no)
);
