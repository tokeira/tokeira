-- Current-run pointer for CHASM entities (activity-executions-first-class, design
-- Item 1). Resolves a bare (namespace_id, business_id) request to its current run,
-- the CHASM analog of the workflow `current_execution` table (V003). One row per
-- business id (activity id); written co-transactionally with the run's root node on
-- Start (see `DsqlChasmNodeRepository::persist_new_execution`).
--
-- DSQL-safe: composite spread-key PK, UUID/TEXT/SMALLINT/BIGINT/TIMESTAMPTZ columns,
-- one CREATE statement, no BIGSERIAL / CHECK / foreign keys. `status` encodes
-- `LifecycleState` (0=Running, 1=Completed, 2=Failed); `failover_version` +
-- `transition_count` are the run's committing VersionedTransition (the advance fence).
CREATE TABLE IF NOT EXISTS chasm_current_run (
    namespace_id      UUID        NOT NULL,
    business_id       TEXT        NOT NULL,
    run_id            UUID        NOT NULL,
    status            SMALLINT    NOT NULL,
    failover_version  BIGINT      NOT NULL,
    transition_count  BIGINT      NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, business_id)
);
