CREATE TABLE IF NOT EXISTS sa_registry (
    attr_id      BIGINT   NOT NULL,
    namespace_id UUID     NOT NULL,
    attr_name    TEXT     NOT NULL,
    attr_type    SMALLINT NOT NULL,
    PRIMARY KEY (attr_id)
);
