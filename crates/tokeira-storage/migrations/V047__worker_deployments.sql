-- Worker Deployment registry: one document per (namespace_id, deployment_name).
--
-- Key ordering is deliberate. `namespace_id` leads so namespace-scoped range scans
-- (list_deployments / list_all_for_namespace) stay efficient. This is the low-cardinality
-- leading-column shape DSQL normally warns against for hot keys, but it is acceptable here:
-- this is control-plane data written by operators (create / promote / ramp), not the
-- workflow hot path, so per-key-range write volume is low. Concurrent writes to the same
-- deployment row serialize and surface as a CAS Conflict (SQLSTATE 40001 -> retry) rather
-- than as contention. Do not mistake this for a hot-key oversight.
CREATE TABLE IF NOT EXISTS worker_deployments (
    namespace_id    UUID        NOT NULL,
    deployment_name TEXT        NOT NULL,
    conflict_token  BYTEA       NOT NULL,
    record_data     BYTEA       NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, deployment_name)
);
