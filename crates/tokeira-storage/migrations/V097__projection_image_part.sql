CREATE TABLE IF NOT EXISTS projection_image_part (
    partition_id INTEGER NOT NULL,
    fanout SMALLINT NOT NULL,
    run_key UUID NOT NULL,
    transition_seq BIGINT NOT NULL,
    part_no INTEGER NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (partition_id, fanout, run_key, transition_seq, part_no)
);
