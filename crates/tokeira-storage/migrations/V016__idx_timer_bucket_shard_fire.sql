CREATE INDEX ASYNC IF NOT EXISTS idx_timer_bucket_shard_fire
    ON timer_bucket (shard_id, fire_at);
