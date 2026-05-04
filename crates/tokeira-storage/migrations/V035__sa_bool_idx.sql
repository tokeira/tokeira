CREATE TABLE IF NOT EXISTS sa_bool_idx (
    namespace_id UUID    NOT NULL,
    attr_id      BIGINT  NOT NULL,
    value        BOOLEAN NOT NULL,
    run_key      UUID    NOT NULL,
    PRIMARY KEY (namespace_id, attr_id, value, run_key)
);
