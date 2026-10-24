CREATE TABLE IF NOT EXISTS execution_visibility_attr_value (
    namespace_id          UUID     NOT NULL,
    run_key               UUID     NOT NULL,
    authority_epoch       BIGINT   NOT NULL,
    source_transition_seq BIGINT   NOT NULL,
    attr_id               BIGINT   NOT NULL,
    attr_type             SMALLINT NOT NULL,
    value_data            BYTEA    NOT NULL,
    PRIMARY KEY (namespace_id, run_key, authority_epoch, source_transition_seq, attr_id, attr_type)
);
-- One encoded attribute value per image, written with the run's row. The component
-- List hydration and the load's used-versions seed read it at the row's version.
