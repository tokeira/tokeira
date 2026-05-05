CREATE TABLE IF NOT EXISTS routing_generation (
    id         INTEGER     PRIMARY KEY,
    generation BIGINT      NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
