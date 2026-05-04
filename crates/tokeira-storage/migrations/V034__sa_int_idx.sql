CREATE TABLE IF NOT EXISTS sa_int_idx (
    namespace_id UUID   NOT NULL,
    attr_id      BIGINT NOT NULL,
    value        BIGINT NOT NULL,
    run_key      UUID   NOT NULL,
    PRIMARY KEY (namespace_id, attr_id, value, run_key)
);
