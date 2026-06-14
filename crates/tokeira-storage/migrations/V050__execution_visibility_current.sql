CREATE TABLE IF NOT EXISTS execution_visibility_current (
    namespace_id           UUID        NOT NULL,
    archetype_id           BIGINT      NOT NULL,
    run_key                UUID        NOT NULL,
    business_id            TEXT        NOT NULL,
    run_id                 UUID        NOT NULL,
    authority_epoch        BIGINT      NOT NULL,
    source_transition_seq  BIGINT      NOT NULL,
    status_keyword         TEXT        NOT NULL,
    lifecycle_state        SMALLINT    NOT NULL,
    start_time             TIMESTAMPTZ NOT NULL,
    update_time            TIMESTAMPTZ NOT NULL,
    close_time             TIMESTAMPTZ,
    execution_type         TEXT,
    task_queue             TEXT,
    transition_count       BIGINT      NOT NULL DEFAULT 0,
    history_size_bytes     BIGINT      NOT NULL DEFAULT 0,
    memo_blob              BYTEA,
    search_attr_generation BIGINT      NOT NULL DEFAULT 0,
    -- Archetype-fidelity columns. The generalized index is one wide table over
    -- all archetypes; these are populated for the workflow archetype and left
    -- NULL where an archetype has no analog, so workflow List/Describe keeps the
    -- fields the wire response carries (Requirement 10.13). `history_length` is
    -- the workflow event count, kept distinct from the generic `transition_count`
    -- (the per-run transition seq) because they are not equal for workflows.
    execution_time         TIMESTAMPTZ,
    execution_duration     BIGINT,
    history_length         BIGINT      NOT NULL DEFAULT 0,
    parent_workflow_id     TEXT,
    parent_run_id          UUID,
    root_workflow_id       TEXT,
    root_run_id            UUID,
    PRIMARY KEY (namespace_id, archetype_id, run_key)
);
