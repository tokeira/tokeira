CREATE UNIQUE INDEX ASYNC idx_request_dedupe_ns_wf_req ON request_dedupe (namespace_id, workflow_id, request_id);
