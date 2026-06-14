CREATE TABLE IF NOT EXISTS execution_visibility_attr_index (
    namespace_id  UUID     NOT NULL,
    archetype_id  BIGINT   NOT NULL,
    run_key       UUID     NOT NULL,
    generation    BIGINT   NOT NULL,
    attr_id       BIGINT   NOT NULL,
    attr_type     SMALLINT NOT NULL,
    keyword_value TEXT,
    int_value     BIGINT,
    bool_value    BOOLEAN,
    double_value  DOUBLE PRECISION,
    datetime_value TIMESTAMPTZ,
    text_token    TEXT,
    value_data    BYTEA    NOT NULL,
    PRIMARY KEY (namespace_id, archetype_id, run_key, generation, attr_id, attr_type)
);
