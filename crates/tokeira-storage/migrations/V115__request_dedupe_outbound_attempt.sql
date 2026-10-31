ALTER TABLE request_dedupe ADD COLUMN IF NOT EXISTS outbound_attempt BIGINT;
