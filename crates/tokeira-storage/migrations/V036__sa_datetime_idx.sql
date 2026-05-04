CREATE TABLE IF NOT EXISTS sa_datetime_idx (
    namespace_id UUID        NOT NULL,
    attr_id      BIGINT      NOT NULL,
    value        TIMESTAMPTZ NOT NULL,
    run_key      UUID        NOT NULL,
    PRIMARY KEY (namespace_id, attr_id, value, run_key)
);
