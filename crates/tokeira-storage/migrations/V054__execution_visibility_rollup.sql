CREATE TABLE IF NOT EXISTS execution_visibility_rollup (
    namespace_id  UUID     NOT NULL,
    archetype_id  BIGINT   NOT NULL,
    dimension     SMALLINT NOT NULL,
    value         TEXT     NOT NULL,
    stripe        INT      NOT NULL,
    counter       BIGINT   NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace_id, archetype_id, dimension, value, stripe)
);
