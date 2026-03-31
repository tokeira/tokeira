use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Mutex;
use tokeira_kernel::ProjectionOp;
use tokeira_storage::ProjectionRecord;
use tokeira_types::{ExecutionStatus, ExecutionSummary, ProjectionCursor, RunId, RunKey};

use crate::sink::ProjectionSink;

/// Tiny in-memory visibility sink.
///
/// This is not meant to model the final SQL visibility design. It simply makes
/// projection effects visible to tests and local development.
#[derive(Default, Clone)]
pub struct InMemoryVisibilitySink {
    summaries: Arc<Mutex<HashMap<RunKey, VisibilityRow>>>,
}

#[derive(Clone, Debug)]
pub struct VisibilityRow {
    pub status: ExecutionStatus,
    pub closed: bool,
}

#[async_trait]
impl ProjectionSink for InMemoryVisibilitySink {
    async fn apply(&self, record: &ProjectionRecord) -> Result<()> {
        let mut rows = self.summaries.lock().await;
        let row = rows.entry(record.run_key).or_insert(VisibilityRow {
            status: ExecutionStatus::Running,
            closed: false,
        });
        for op in &record.ops {
            match op {
                ProjectionOp::UpsertExecution { status, .. } => {
                    row.status = *status;
                }
                ProjectionOp::CloseExecution { status, .. } => {
                    row.status = *status;
                    row.closed = true;
                }
            }
        }
        Ok(())
    }
}

impl InMemoryVisibilitySink {
    pub async fn get(&self, run_key: RunKey) -> Option<VisibilityRow> {
        self.summaries.lock().await.get(&run_key).cloned()
    }

    // TODO(sql-visibility): replace this with a richer query surface that
    // mirrors the list/count semantics of the canonical SQL projection.
}
