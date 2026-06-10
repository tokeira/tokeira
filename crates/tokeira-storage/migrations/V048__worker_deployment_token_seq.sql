-- Persistent conflict-token high-water-mark for Worker Deployments.
--
-- The conflict token must increase monotonically across the entire lifetime of a
-- deployment *name*, including delete-then-recreate. In v1.31.0 the token is
-- `workflow.Now(ctx).MarshalBinary()` of the deployment entity workflow
-- (`service/worker/workerdeployment/workflow.go:248,502 @ v1.31.0`); a recreated
-- deployment runs a fresh entity workflow and therefore observes a strictly later
-- time. Because `worker_deployments` rows are physically removed on delete, the
-- generation counter stored on that row cannot survive a recreate. This companion
-- table holds the per-name generation and is never deleted, so a recreated
-- deployment resumes from the prior high-water-mark instead of resetting to 1.
--
-- This is control-plane data (one row per deployment name, ever), so the
-- low-cardinality leading key and forever-growing row count are acceptable; this
-- is not the workflow hot path.
CREATE TABLE IF NOT EXISTS worker_deployment_token_seq (
    namespace_id    UUID   NOT NULL,
    deployment_name TEXT   NOT NULL,
    generation      BIGINT NOT NULL,
    PRIMARY KEY (namespace_id, deployment_name)
);
