ALTER TABLE request_dedupe ADD COLUMN IF NOT EXISTS outbound_initiating_event_id BIGINT;
