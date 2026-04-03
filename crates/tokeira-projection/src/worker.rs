use anyhow::Result;
use tokeira_storage::ProjectionLog;
use tokeira_types::ProjectionCursor;
use tracing::{debug, info};

use crate::sink::ProjectionSink;

/// Drives one `(partition_id, fanout)` projection substream.
///
/// Insight: we model one substream per worker because it makes replay and
/// checkpoint semantics obvious. A richer implementation may multiplex many
/// substreams through one task later, but the semantics should stay the same.
pub struct ProjectionWorker<L, S> {
    pub log: L,
    pub sink: S,
    pub batch_size: usize,
}

impl<L, S> ProjectionWorker<L, S>
where
    L: ProjectionLog,
    S: ProjectionSink,
{
    pub async fn run_once(&self, cursor: ProjectionCursor) -> Result<ProjectionCursor> {
        let batch = self.log.read_from(&cursor, self.batch_size).await?;
        if batch.records.is_empty() {
            debug!(
                partition = cursor.partition_id,
                fanout = cursor.fanout,
                "projection substream idle"
            );
            return Ok(cursor);
        }

        for record in &batch.records {
            self.sink.apply(record).await?;
        }

        info!(
            partition = batch.next_cursor.partition_id,
            fanout = batch.next_cursor.fanout,
            count = batch.records.len(),
            "applied projection batch"
        );
        Ok(batch.next_cursor)
    }

    // TODO(projection): add a long-running loop with backoff, cancellation, and
    // externally persisted checkpoints.
    // TODO(projection): add lag metrics and per-sink failure policies.
}
