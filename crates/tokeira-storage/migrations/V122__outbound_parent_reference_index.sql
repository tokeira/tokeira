CREATE INDEX ASYNC IF NOT EXISTS outbound_parent_reference_target ON outbound_parent_reference (parent_namespace_id, parent_workflow_id);
