CREATE TABLE IF NOT EXISTS outbound_upgrade_state_part (
    run_key UUID NOT NULL,
    generation UUID NOT NULL,
    part_no INTEGER NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (run_key, generation, part_no)
);
