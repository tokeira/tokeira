CREATE TABLE IF NOT EXISTS execution_visibility_attr_index (
    namespace_id  UUID     NOT NULL,
    archetype_id  BIGINT   NOT NULL,
    run_key       UUID     NOT NULL,
    -- Generation = the snapshot version that wrote these rows. Rows become visible
    -- exactly when execution_visibility_current.(authority_epoch,
    -- source_transition_seq) equals this generation; distinct concurrent snapshots
    -- write distinct generations (no collision), a retry writes the same generation
    -- (idempotent), and the apply-iff-newer flip selects the winner
    -- (reference/DECISION-visibility-dsql-schema.md).
    gen_authority_epoch       BIGINT NOT NULL,
    gen_source_transition_seq BIGINT NOT NULL,
    attr_id       BIGINT   NOT NULL,
    attr_type     SMALLINT NOT NULL,
    keyword_value TEXT,
    int_value     BIGINT,
    bool_value    BOOLEAN,
    double_value  DOUBLE PRECISION,
    datetime_value TIMESTAMPTZ,
    text_token    TEXT,
    value_data    BYTEA    NOT NULL,
    PRIMARY KEY (
        namespace_id, archetype_id, run_key,
        gen_authority_epoch, gen_source_transition_seq, attr_id, attr_type
    )
);
