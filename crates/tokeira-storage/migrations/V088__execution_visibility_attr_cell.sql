CREATE TABLE IF NOT EXISTS execution_visibility_attr_cell (
    namespace_id          UUID             NOT NULL,
    run_key               UUID             NOT NULL,
    authority_epoch       BIGINT           NOT NULL,
    source_transition_seq BIGINT           NOT NULL,
    attr_id               BIGINT           NOT NULL,
    attr_type             SMALLINT         NOT NULL,
    -- Canonical string of the indexed value, element or token, as in V052.
    value_discriminator   TEXT             NOT NULL,
    keyword_value         TEXT,
    int_value             BIGINT,
    bool_value            BOOLEAN,
    double_value          DOUBLE PRECISION,
    datetime_value        TIMESTAMPTZ,
    text_token            TEXT,
    PRIMARY KEY (namespace_id, run_key, authority_epoch, source_transition_seq,
                 attr_id, attr_type, value_discriminator)
);
-- An index row belongs to the image at `(authority_epoch, source_transition_seq)`.
-- A run's rows at its `execution_visibility_current` version are its attributes;
-- advancing that row under the version guard is what makes an image's rows current,
-- and rows below the current version are deleted afterwards
-- (`bounded-visibility-apply`). Values live once per image in
-- `execution_visibility_attr_value`.
