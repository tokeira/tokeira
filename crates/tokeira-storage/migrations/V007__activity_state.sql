CREATE TABLE IF NOT EXISTS activity_state (
    run_key             UUID        NOT NULL,
    schedule_event_id   BIGINT      NOT NULL,
    shard_id            UUID        NOT NULL,
    activity_id         TEXT        NOT NULL,
    queue_namespace     UUID        NOT NULL,
    queue_name          TEXT        NOT NULL,
    attempt             INTEGER     NOT NULL,
    state_data          BYTEA       NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_key, schedule_event_id)
);
