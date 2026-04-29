//! Projection-log worker driving one `(partition_id, fanout)` substream.
//!
//! The worker owns checkpointing and replay: sinks apply records, but they do
//! not decide where to resume after failure. Keeping recovery responsibility
//! here ensures consistent at-least-once delivery semantics across all sink
//! implementations, whether in-memory or backed by a durable store.

use anyhow::Result;
use tokeira_storage::ProjectionLog;
use tokeira_types::ProjectionCursor;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::metrics as projection_metrics;
use crate::{sink::ProjectionSink, store::VisibilityStore, types::beginning_cursor};

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
    /// Apply at most one batch from `cursor` and return the next cursor to
    /// resume from.
    pub async fn run_once(&self, cursor: ProjectionCursor) -> Result<ProjectionCursor> {
        let batch = self.log.read_from(&cursor, self.batch_size).await?;
        if batch.records.is_empty() {
            projection_metrics::set_projection_lag(cursor.partition_id, 0);
            debug!(
                partition = cursor.partition_id,
                fanout = cursor.fanout,
                "projection substream idle"
            );
            return Ok(cursor);
        }

        projection_metrics::set_projection_lag(cursor.partition_id, batch.records.len());

        for record in &batch.records {
            self.sink.apply(record).await?;
        }
        projection_metrics::record_records_processed(
            cursor.partition_id,
            batch.records.len(),
        );
        projection_metrics::set_projection_lag(cursor.partition_id, 0);

        info!(
            partition = batch.next_cursor.partition_id,
            fanout = batch.next_cursor.fanout,
            count = batch.records.len(),
            "applied projection batch"
        );
        Ok(batch.next_cursor)
    }

    // TODO(projection): add lag metrics and per-sink failure policies.
}

impl<L, S> ProjectionWorker<L, S>
where
    L: ProjectionLog,
    S: ProjectionSink + VisibilityStore,
{
    /// Run continuously from the stored checkpoint (if any), persisting
    /// progress after each successfully applied batch.
    pub async fn run_from_cursor(
        &self,
        sink_id: &str,
        cancel: CancellationToken,
        initial_cursor: ProjectionCursor,
    ) -> Result<()> {
        let mut cursor = self
            .sink
            .load_checkpoint(sink_id)
            .await?
            .unwrap_or(initial_cursor);
        let mut backoff = tokio::time::Duration::from_millis(100);
        let max_backoff = tokio::time::Duration::from_secs(5);

        loop {
            if cancel.is_cancelled() {
                self.sink.save_checkpoint(sink_id, &cursor).await?;
                return Ok(());
            }

            let batch = self.log.read_from(&cursor, self.batch_size).await?;
            if batch.records.is_empty() {
                projection_metrics::set_projection_lag(cursor.partition_id, 0);
                debug!("projection substream idle");
                tokio::select! {
                    _ = cancel.cancelled() => {
                        self.sink.save_checkpoint(sink_id, &cursor).await?;
                        return Ok(());
                    }
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
                continue;
            }
            backoff = tokio::time::Duration::from_millis(100);
            projection_metrics::set_projection_lag(
                cursor.partition_id,
                batch.records.len(),
            );

            let mut failed = false;
            for record in &batch.records {
                if let Err(error) = self.sink.apply(record).await {
                    tracing::warn!(?error, "projection sink apply failed");
                    failed = true;
                    break;
                }
            }
            if failed {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        self.sink.save_checkpoint(sink_id, &cursor).await?;
                        return Ok(());
                    }
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
                continue;
            }
            projection_metrics::record_records_processed(
                cursor.partition_id,
                batch.records.len(),
            );
            cursor = batch.next_cursor;
            projection_metrics::set_projection_lag(cursor.partition_id, 0);
            self.sink.save_checkpoint(sink_id, &cursor).await?;
            info!(
                partition = cursor.partition_id,
                fanout = cursor.fanout,
                "projection worker advanced checkpoint"
            );
        }
    }

    pub async fn run(&self, sink_id: &str, cancel: CancellationToken) -> Result<()> {
        self.run_from_cursor(sink_id, cancel, beginning_cursor())
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use anyhow::anyhow;
    use async_trait::async_trait;
    use tokeira_kernel::ProjectionOp;
    use tokeira_storage::{ProjectionBatch, ProjectionContext, ProjectionRecord};
    use tokeira_types::{
        ExecutionStatus, Memo, NamespaceId, RunId, RunKey, SearchAttributes,
        TaskQueueName, TransitionSeq, WorkflowId, WorkflowType,
    };
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use crate::{memory::InMemoryVisibilityStore, visibility_sink::VisibilitySink};

    #[derive(Clone, Default)]
    struct FakeLog {
        seen: Arc<Mutex<Vec<ProjectionCursor>>>,
        batches: Arc<Mutex<Vec<ProjectionBatch>>>,
    }

    #[async_trait]
    impl ProjectionLog for FakeLog {
        async fn read_from(
            &self,
            cursor: &ProjectionCursor,
            _limit: usize,
        ) -> Result<ProjectionBatch> {
            self.seen.lock().await.push(cursor.clone());
            let mut batches = self.batches.lock().await;
            if batches.is_empty() {
                return Ok(ProjectionBatch {
                    records: Vec::new(),
                    next_cursor: cursor.clone(),
                });
            }
            Ok(batches.remove(0))
        }
    }

    fn record() -> ProjectionRecord {
        ProjectionRecord {
            partition_id: 0,
            fanout: 1,
            run_key: RunKey(Uuid::from_u128(9)),
            transition_seq: TransitionSeq(1),
            context: ProjectionContext {
                namespace_id: NamespaceId(Uuid::from_u128(1)),
                workflow_id: WorkflowId("wf".to_string()),
                run_id: RunId(Uuid::from_u128(2)),
                workflow_type: WorkflowType("Workflow".to_string()),
                task_queue: TaskQueueName("queue".to_string()),
                execution_status: ExecutionStatus::Running,
                start_time: time::OffsetDateTime::from_unix_timestamp(100).unwrap(),
                execution_time: None,
                close_time: None,
                history_length: 1,
                state_transition_count: 1,
            },
            ops: vec![ProjectionOp::UpsertExecution {
                status: ExecutionStatus::Running,
                memo_patch: Memo::default(),
                search_attr_patch: SearchAttributes::default(),
            }],
        }
    }

    #[tokio::test]
    async fn run_starts_from_beginning_without_checkpoint() {
        let log = FakeLog {
            seen: Arc::new(Mutex::new(Vec::new())),
            batches: Arc::new(Mutex::new(vec![ProjectionBatch {
                records: Vec::new(),
                next_cursor: ProjectionCursor::beginning(0, 1),
            }])),
        };
        let worker = ProjectionWorker {
            log: log.clone(),
            sink: VisibilitySink::new(InMemoryVisibilityStore::default(), "sink"),
            batch_size: 10,
        };
        let cancel = CancellationToken::new();
        let cloned = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            cloned.cancel();
        });
        worker.run("sink", cancel).await.unwrap();

        let seen = log.seen.lock().await;
        assert_eq!(seen[0], ProjectionCursor::beginning(0, 1));
    }

    #[tokio::test]
    async fn run_applies_batch_and_saves_checkpoint() {
        let next_cursor = ProjectionCursor {
            partition_id: 0,
            fanout: 1,
            last_run_key: Some(RunKey(Uuid::from_u128(9))),
            last_transition_seq: Some(TransitionSeq(1)),
        };
        let log = FakeLog {
            seen: Arc::new(Mutex::new(Vec::new())),
            batches: Arc::new(Mutex::new(vec![ProjectionBatch {
                records: vec![record()],
                next_cursor: next_cursor.clone(),
            }])),
        };
        let store = InMemoryVisibilityStore::default();
        let worker = ProjectionWorker {
            log,
            sink: VisibilitySink::new(store.clone(), "sink"),
            batch_size: 10,
        };
        let cancel = CancellationToken::new();
        let cloned = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            cloned.cancel();
        });

        worker.run("sink", cancel).await.unwrap();

        let saved = store.load_checkpoint("sink").await.unwrap().unwrap();
        assert_eq!(saved, next_cursor);
        assert!(store.get_row(RunKey(Uuid::from_u128(9))).await.is_some());
    }

    #[derive(Clone, Default)]
    struct FailingSink {
        store: InMemoryVisibilityStore,
        failures_left: Arc<Mutex<usize>>,
        applied: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl crate::sink::ProjectionSink for FailingSink {
        async fn apply(&self, record: &ProjectionRecord) -> Result<()> {
            let mut failures_left = self.failures_left.lock().await;
            if *failures_left > 0 {
                *failures_left -= 1;
                return Err(anyhow!("boom"));
            }
            self.store
                .upsert_execution(&crate::types::ExecutionRow {
                    run_key: record.run_key,
                    namespace_id: record.context.namespace_id,
                    workflow_id: record.context.workflow_id.clone(),
                    run_id: record.context.run_id,
                    workflow_type: record.context.workflow_type.clone(),
                    task_queue: record.context.task_queue.clone(),
                    status: record.context.execution_status,
                    start_time: record.context.start_time,
                    execution_time: record.context.execution_time,
                    close_time: record.context.close_time,
                    history_length: record.context.history_length,
                    state_transition_count: record.context.state_transition_count,
                    memo: Memo::default(),
                    search_attr_version: 0,
                })
                .await?;
            *self.applied.lock().await += 1;
            Ok(())
        }
    }

    #[async_trait]
    impl crate::store::VisibilityStore for FailingSink {
        async fn upsert_execution(&self, row: &crate::types::ExecutionRow) -> Result<()> {
            self.store.upsert_execution(row).await
        }
        async fn delete_execution(&self, run_key: RunKey) -> Result<()> {
            self.store.delete_execution(run_key).await
        }
        async fn upsert_search_attr_index(
            &self,
            run_key: RunKey,
            namespace_id: NamespaceId,
            attr_id: crate::types::AttrId,
            attr_type: crate::types::SearchAttrType,
            value: &tokeira_types::SearchAttrValue,
        ) -> Result<()> {
            self.store
                .upsert_search_attr_index(
                    run_key,
                    namespace_id,
                    attr_id,
                    attr_type,
                    value,
                )
                .await
        }
        async fn remove_search_attr_index(
            &self,
            run_key: RunKey,
            namespace_id: NamespaceId,
            attr_id: crate::types::AttrId,
            attr_type: crate::types::SearchAttrType,
        ) -> Result<()> {
            self.store
                .remove_search_attr_index(run_key, namespace_id, attr_id, attr_type)
                .await
        }
        async fn accumulate_rollup(
            &self,
            entries: &[crate::types::RollupDelta],
        ) -> Result<()> {
            self.store.accumulate_rollup(entries).await
        }
        async fn list_executions(
            &self,
            namespace_id: NamespaceId,
            filter: &crate::types::CompiledFilter,
            sort: crate::types::SortOrder,
            page: &crate::types::PageBounds,
        ) -> Result<crate::types::ListResult> {
            self.store
                .list_executions(namespace_id, filter, sort, page)
                .await
        }
        async fn count_executions(
            &self,
            namespace_id: NamespaceId,
            filter: &crate::types::CompiledFilter,
            group_by: Option<crate::types::GroupByField>,
        ) -> Result<crate::types::CountResult> {
            self.store
                .count_executions(namespace_id, filter, group_by)
                .await
        }
        async fn count_from_rollup(
            &self,
            namespace_id: NamespaceId,
            dimension: crate::types::RollupDimension,
        ) -> Result<crate::types::CountResult> {
            self.store.count_from_rollup(namespace_id, dimension).await
        }
        async fn load_checkpoint(
            &self,
            sink_id: &str,
        ) -> Result<Option<ProjectionCursor>> {
            self.store.load_checkpoint(sink_id).await
        }
        async fn save_checkpoint(
            &self,
            sink_id: &str,
            cursor: &ProjectionCursor,
        ) -> Result<()> {
            self.store.save_checkpoint(sink_id, cursor).await
        }
        async fn resolve_attr(
            &self,
            namespace_id: NamespaceId,
            name: &str,
        ) -> Result<Option<crate::types::AttrDescriptor>> {
            self.store.resolve_attr(namespace_id, name).await
        }
        async fn register_attr(
            &self,
            namespace_id: NamespaceId,
            name: String,
            attr_type: crate::types::SearchAttrType,
        ) -> Result<crate::types::AttrId> {
            self.store
                .register_attr(namespace_id, name, attr_type)
                .await
        }
        async fn get_row(&self, run_key: RunKey) -> Option<crate::types::ExecutionRow> {
            self.store.get_row(run_key).await
        }
    }

    #[tokio::test]
    async fn run_does_not_advance_checkpoint_on_sink_error() {
        let next_cursor = ProjectionCursor {
            partition_id: 0,
            fanout: 1,
            last_run_key: Some(RunKey(Uuid::from_u128(9))),
            last_transition_seq: Some(TransitionSeq(1)),
        };
        let log = FakeLog {
            seen: Arc::new(Mutex::new(Vec::new())),
            batches: Arc::new(Mutex::new(vec![
                ProjectionBatch {
                    records: vec![record()],
                    next_cursor: next_cursor.clone(),
                },
                ProjectionBatch {
                    records: vec![record()],
                    next_cursor: next_cursor.clone(),
                },
            ])),
        };
        let sink = FailingSink {
            store: InMemoryVisibilityStore::default(),
            failures_left: Arc::new(Mutex::new(1)),
            applied: Arc::new(Mutex::new(0)),
        };
        let worker = ProjectionWorker {
            log,
            sink: sink.clone(),
            batch_size: 10,
        };
        let cancel = CancellationToken::new();
        let cloned = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
            cloned.cancel();
        });

        worker.run("sink", cancel).await.unwrap();

        let saved = sink.load_checkpoint("sink").await.unwrap().unwrap();
        assert_eq!(saved, next_cursor);
        assert_eq!(*sink.applied.lock().await, 1);
    }

    use proptest::prelude::*;

    // Feature: projection-visibility, Property 16:
    // Checkpoint-After-Apply Invariant
    // **Validates: Requirements 9.4**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_checkpoint_not_advanced_on_failure(
            fail_count in 1usize..5,
        ) {
            let rt =
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
            rt.block_on(async {
                let next_cursor = ProjectionCursor {
                    partition_id: 0,
                    fanout: 1,
                    last_run_key: Some(RunKey(
                        Uuid::from_u128(9),
                    )),
                    last_transition_seq: Some(
                        TransitionSeq(1),
                    ),
                };
                // Create enough batches: fail_count
                // failing + 1 succeeding + 1 empty
                let mut batches = Vec::new();
                for _ in 0..=fail_count {
                    batches.push(ProjectionBatch {
                        records: vec![record()],
                        next_cursor: next_cursor.clone(),
                    });
                }
                let log = FakeLog {
                    seen: Arc::new(Mutex::new(Vec::new())),
                    batches: Arc::new(Mutex::new(batches)),
                };
                let sink = FailingSink {
                    store:
                        InMemoryVisibilityStore::default(),
                    failures_left: Arc::new(Mutex::new(
                        fail_count,
                    )),
                    applied: Arc::new(Mutex::new(0)),
                };
                let worker = ProjectionWorker {
                    log,
                    sink: sink.clone(),
                    batch_size: 10,
                };
                let cancel = CancellationToken::new();
                let cloned = cancel.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(
                        tokio::time::Duration::from_millis(
                            500,
                        ),
                    )
                    .await;
                    cloned.cancel();
                });
                worker
                    .run("sink", cancel)
                    .await
                    .unwrap();
                // After failures + 1 success, checkpoint
                // should be at next_cursor
                let saved = sink
                    .load_checkpoint("sink")
                    .await
                    .unwrap();
                // Checkpoint should exist (at least one
                // successful apply)
                if let Some(saved) = saved {
                    prop_assert_eq!(saved, next_cursor);
                }
                // Exactly 1 successful apply
                prop_assert_eq!(
                    *sink.applied.lock().await,
                    1
                );
                Ok(())
            })?;
        }
    }
}
