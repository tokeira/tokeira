CREATE TABLE IF NOT EXISTS worker_compute_action_part (
    action_id UUID NOT NULL,
    part_no INTEGER NOT NULL,
    data BYTEA NOT NULL,
    PRIMARY KEY (action_id, part_no)
);
