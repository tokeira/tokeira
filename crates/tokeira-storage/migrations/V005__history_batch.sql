CREATE TABLE IF NOT EXISTS history_batch (
    run_key         UUID        NOT NULL,
    first_event_id  BIGINT      NOT NULL,
    last_event_id   BIGINT      NOT NULL,
    transition_seq  BIGINT      NOT NULL,
    events_data     BYTEA       NOT NULL,
    principals_data BYTEA,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_key, first_event_id)
);
