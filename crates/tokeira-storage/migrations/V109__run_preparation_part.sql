CREATE TABLE IF NOT EXISTS run_preparation_part (
    preparation_id UUID NOT NULL,
    generation UUID NOT NULL,
    part_no INTEGER NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (preparation_id, generation, part_no)
);
