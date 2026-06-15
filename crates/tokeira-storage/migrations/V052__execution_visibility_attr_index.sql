CREATE TABLE IF NOT EXISTS execution_visibility_attr_index (
    namespace_id        UUID     NOT NULL,
    run_key             UUID     NOT NULL,
    attr_id             BIGINT   NOT NULL,
    attr_type           SMALLINT NOT NULL,
    -- Canonical string of the indexed (element) value. It makes the primary key
    -- unique for multi-value attributes: a KeywordList or tokenized Text writes one
    -- row per element/token, all sharing (run_key, attr_id, attr_type).
    value_discriminator TEXT     NOT NULL,
    -- Typed value columns for typed predicates (range on int/double/datetime,
    -- equality on keyword/bool). Exactly one is populated per row.
    keyword_value       TEXT,
    int_value           BIGINT,
    bool_value          BOOLEAN,
    double_value        DOUBLE PRECISION,
    datetime_value      TIMESTAMPTZ,
    text_token          TEXT,
    value_data          BYTEA    NOT NULL,
    PRIMARY KEY (namespace_id, run_key, attr_id, attr_type, value_discriminator)
);
-- Deliberate deviation from the design's generation pattern (recorded in
-- reference/DECISION-visibility-dsql-schema.md): the visibility apply replaces a
-- run's whole attribute set in the fenced transaction (delete-then-insert), so only
-- the current image exists and queries need no generation join. Chosen over the
-- generation pattern because attribute sets are tiny (activities contribute none,
-- workflows a handful), which makes the generation pattern's hot-path
-- conflict-surface benefit marginal and not worth its query-side complexity. This
-- is why there are no generation columns and run_key alone (not archetype_id) keys
-- the rows (run_key already determines the archetype via execution_visibility_current).
