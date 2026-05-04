CREATE TABLE IF NOT EXISTS vis_rollup (
    namespace_id UUID     NOT NULL,
    dimension    SMALLINT NOT NULL,
    value        TEXT     NOT NULL,
    counter      BIGINT   NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace_id, dimension, value)
);
