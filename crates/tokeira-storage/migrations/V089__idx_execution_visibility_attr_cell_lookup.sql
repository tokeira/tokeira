CREATE INDEX ASYNC IF NOT EXISTS idx_execution_visibility_attr_cell_lookup
    ON execution_visibility_attr_cell
    (namespace_id, attr_id, attr_type, keyword_value, run_key, authority_epoch, source_transition_seq);
