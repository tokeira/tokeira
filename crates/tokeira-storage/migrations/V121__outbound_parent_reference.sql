CREATE TABLE IF NOT EXISTS outbound_parent_reference (
    child_run_key UUID PRIMARY KEY,
    parent_namespace_id UUID NOT NULL,
    parent_workflow_id TEXT NOT NULL
);
