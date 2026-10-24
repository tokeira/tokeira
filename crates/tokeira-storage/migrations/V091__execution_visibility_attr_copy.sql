CREATE TABLE IF NOT EXISTS execution_visibility_attr_copy (
    name          TEXT        NOT NULL,
    position_data BYTEA,
    completed_at  TIMESTAMPTZ,
    PRIMARY KEY (name)
);
-- The one-time copy of `execution_visibility_attr_index` into the versioned cells and
-- values (`bounded-visibility-apply`): the position it has reached and when it
-- finished. Each copy transaction reads its row `FOR UPDATE` and writes the new
-- position in the same transaction, so concurrent copiers take turns and a restarted
-- copy resumes where it stopped.
