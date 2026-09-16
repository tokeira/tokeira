CREATE INDEX ASYNC IF NOT EXISTS idx_chasm_current_execution_status ON chasm_current_execution (namespace_id, status, archetype_id, business_id);
