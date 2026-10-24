CREATE TABLE IF NOT EXISTS run_value_part (
    run_key UUID NOT NULL,
    value_kind SMALLINT NOT NULL,
    value_key BIGINT NOT NULL,
    part_no INTEGER NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (run_key, value_kind, value_key, part_no)
);
