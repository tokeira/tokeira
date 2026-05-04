CREATE TABLE IF NOT EXISTS sa_current (
    run_key    UUID   NOT NULL,
    attr_id    BIGINT NOT NULL,
    value_data BYTEA  NOT NULL,
    PRIMARY KEY (run_key, attr_id)
);
