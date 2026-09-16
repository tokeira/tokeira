-- Archetype-scoped current pointer. Mirrors current_chasm_executions.sql under
-- schema/postgresql/v12/temporal/versioned/v1.19 @ v1.31.0, without a shard column.
-- Status retains V056's encoding: 0 Running, 1 Completed, 2 Failed.
CREATE TABLE IF NOT EXISTS chasm_current_execution (
    namespace_id      UUID        NOT NULL,
    archetype_id      BIGINT      NOT NULL,
    business_id       TEXT        NOT NULL,
    run_id            UUID        NOT NULL,
    request_id        TEXT        NOT NULL,
    status            SMALLINT    NOT NULL,
    failover_version  BIGINT      NOT NULL,
    transition_count  BIGINT      NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, archetype_id, business_id)
);
