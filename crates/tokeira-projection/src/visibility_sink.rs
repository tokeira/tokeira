//! Projection sink that materializes post-transition visibility snapshots.
//!
//! The sink is the write-side of the CQRS projection plane. It receives
//! `ProjectionRecord`s from the projection worker, resolves search-attribute
//! schemas, computes rollup deltas, and persists the results through the
//! [`VisibilityStore`] trait.
//!
//! Rollup deltas are computed by [`compute_rollup_deltas`] so that aggregate
//! counts (by status, workflow type, task queue) stay consistent without
//! full-table scans.

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokeira_storage::ProjectionRecord;

use crate::{
    metrics as projection_metrics,
    rollup::compute_rollup_deltas,
    sink::ProjectionSink,
    store::VisibilityStore,
    types::{
        AttrDescriptor, AttrId, CompiledFilter, CountResult, ExecutionRow, GroupByField,
        ListResult, PageBounds, RollupDelta, RollupDimension, SearchAttrType, SortOrder,
        VisibilitySnapshot, search_attr_type_of, visibility_version_is_newer,
    },
};

pub struct VisibilitySink<S> {
    store: S,
}

impl<S> VisibilitySink<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S> VisibilityStore for VisibilitySink<S>
where
    S: VisibilityStore,
{
    async fn upsert_snapshot(&self, snapshot: &VisibilitySnapshot) -> Result<()> {
        self.store.upsert_snapshot(snapshot).await
    }
    async fn upsert_execution(&self, row: &ExecutionRow) -> Result<()> {
        self.store.upsert_execution(row).await
    }
    async fn delete_execution(&self, run_key: tokeira_types::RunKey) -> Result<()> {
        self.store.delete_execution(run_key).await
    }
    async fn upsert_search_attr_index(
        &self,
        run_key: tokeira_types::RunKey,
        namespace_id: tokeira_types::NamespaceId,
        attr_id: AttrId,
        attr_type: SearchAttrType,
        value: &tokeira_types::SearchAttrValue,
    ) -> Result<()> {
        self.store
            .upsert_search_attr_index(run_key, namespace_id, attr_id, attr_type, value)
            .await
    }
    async fn remove_search_attr_index(
        &self,
        run_key: tokeira_types::RunKey,
        namespace_id: tokeira_types::NamespaceId,
        attr_id: AttrId,
        attr_type: SearchAttrType,
    ) -> Result<()> {
        self.store
            .remove_search_attr_index(run_key, namespace_id, attr_id, attr_type)
            .await
    }
    async fn accumulate_rollup(&self, entries: &[RollupDelta]) -> Result<()> {
        self.store.accumulate_rollup(entries).await
    }
    async fn list_executions(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        filter: &CompiledFilter,
        sort: SortOrder,
        page: &PageBounds,
    ) -> Result<ListResult> {
        self.store
            .list_executions(namespace_id, filter, sort, page)
            .await
    }
    async fn count_executions(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        filter: &CompiledFilter,
        group_by: Option<GroupByField>,
    ) -> Result<CountResult> {
        self.store
            .count_executions(namespace_id, filter, group_by)
            .await
    }
    async fn count_from_rollup(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        archetype_id: tokeira_types::ArchetypeId,
        dimension: RollupDimension,
    ) -> Result<CountResult> {
        self.store
            .count_from_rollup(namespace_id, archetype_id, dimension)
            .await
    }
    async fn load_checkpoint(
        &self,
        partition_id: u32,
    ) -> Result<Option<tokeira_types::ProjectionCursor>> {
        self.store.load_checkpoint(partition_id).await
    }
    async fn save_checkpoint(
        &self,
        cursor: &tokeira_types::ProjectionCursor,
    ) -> Result<()> {
        self.store.save_checkpoint(cursor).await
    }
    async fn resolve_attr(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        name: &str,
    ) -> Result<Option<AttrDescriptor>> {
        self.store.resolve_attr(namespace_id, name).await
    }
    async fn register_attr(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        name: String,
        attr_type: SearchAttrType,
    ) -> Result<AttrId> {
        self.store
            .register_attr(namespace_id, name, attr_type)
            .await
    }
    async fn get_row(&self, run_key: tokeira_types::RunKey) -> Option<ExecutionRow> {
        self.store.get_row(run_key).await
    }
}

#[async_trait]
impl<S> ProjectionSink for VisibilitySink<S>
where
    S: VisibilityStore,
{
    async fn apply(&self, record: &ProjectionRecord, _partition_id: u32) -> Result<()> {
        let started = std::time::Instant::now();
        let previous = self.store.get_row(record.run_key).await;
        if !visibility_version_is_newer(
            previous.as_ref(),
            record.context.authority_epoch,
            record.transition_seq,
        ) {
            return Ok(());
        }
        // Visibility rows are full post-transition snapshots, not deltas: every
        // column is taken from the context image, so the row is constructed
        // directly from `record.context` rather than cloned from `previous` and
        // patched field-by-field. Building from context makes it structurally
        // impossible to leave a column frozen at its prior value across a
        // superseding transition — the omission failure mode Property 12 (and the
        // earlier stale `status`/`close_time`) exercised. `previous` is still
        // consulted below to retire search-attribute index entries this snapshot
        // dropped and to compute rollup deltas.
        let mut row = ExecutionRow {
            run_key: record.run_key,
            namespace_id: record.context.namespace_id,
            archetype_id: record.context.archetype_id,
            business_id: record.context.business_id.clone(),
            workflow_id: record.context.workflow_id.clone(),
            run_id: record.context.run_id,
            authority_epoch: record.context.authority_epoch,
            source_transition_seq: record.transition_seq,
            status_keyword: record.context.status_keyword.clone(),
            lifecycle_state: record.context.lifecycle_state,
            workflow_type: record.context.workflow_type.clone(),
            task_queue: record.context.task_queue.clone(),
            status: record.context.execution_status,
            start_time: record.context.start_time,
            update_time: record.context.update_time,
            execution_time: record.context.execution_time,
            close_time: record.context.close_time,
            history_length: record.context.history_length,
            execution_duration: record.context.execution_duration,
            state_transition_count: record.context.state_transition_count,
            history_size_bytes: record.context.history_size_bytes,
            parent_workflow_id: record.context.parent_workflow_id.clone(),
            parent_run_id: record.context.parent_run_id,
            root_workflow_id: record
                .context
                .root_workflow_id
                .clone()
                .unwrap_or_else(|| record.context.workflow_id.clone()),
            root_run_id: record.context.root_run_id.unwrap_or(record.context.run_id),
            memo: record.context.memo.clone(),
            search_attributes: record.context.search_attributes.clone(),
            transition_count: record.context.transition_count,
            search_attr_generation: record.context.search_attr_generation,
            search_attr_version: 0,
        };
        let search_patch = record.context.search_attributes.clone();

        self.store.upsert_execution(&row).await?;
        if let Some(previous) = previous.as_ref() {
            for (name, previous_value) in &previous.search_attributes.0 {
                if search_patch.0.contains_key(name) {
                    continue;
                }
                let Some(attr) = self
                    .store
                    .resolve_attr(record.context.namespace_id, name)
                    .await?
                else {
                    continue;
                };
                self.store
                    .remove_search_attr_index(
                        record.run_key,
                        record.context.namespace_id,
                        attr.attr_id,
                        search_attr_type_of(previous_value),
                    )
                    .await?;
            }
        }
        for (name, value) in &search_patch.0 {
            let Some(attr) = self
                .store
                .resolve_attr(record.context.namespace_id, name)
                .await?
            else {
                projection_metrics::record_sink_error_with_kind(
                    record.partition_id,
                    tokeira_observability::ProjectionErrorKindLabel::Sink,
                );
                return Err(anyhow!("unknown search attribute: {name}"));
            };
            let expected = attr.attr_type;
            let actual = search_attr_type_of(value);
            if expected != actual {
                projection_metrics::record_sink_error_with_kind(
                    record.partition_id,
                    tokeira_observability::ProjectionErrorKindLabel::Serialization,
                );
                return Err(anyhow!(
                    "search attribute type mismatch for {name}: expected {:?}, got {:?}",
                    expected,
                    actual
                ));
            }
            self.store
                .remove_search_attr_index(
                    record.run_key,
                    record.context.namespace_id,
                    attr.attr_id,
                    attr.attr_type,
                )
                .await?;
            self.store
                .upsert_search_attr_index(
                    record.run_key,
                    record.context.namespace_id,
                    attr.attr_id,
                    attr.attr_type,
                    value,
                )
                .await?;
            row.search_attributes.0.insert(name.clone(), value.clone());
            row.search_attr_version += 1;
        }
        self.store.upsert_execution(&row).await?;
        let deltas = compute_rollup_deltas(previous.as_ref(), &row);
        self.store.accumulate_rollup(&deltas).await?;
        projection_metrics::record_sink_write_duration(record.partition_id, started.elapsed());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::InMemoryVisibilityStore;
    use time::OffsetDateTime;
    use tokeira_storage::ProjectionContext;
    use tokeira_types::{
        ArchetypeId, ExecutionStatus, Memo, NamespaceId, Payload, RunId, RunKey, SearchAttrValue,
        SearchAttributes, TaskQueueName, TransitionSeq, VisibilityLifecycleState, WorkflowId,
        WorkflowType,
    };
    use uuid::Uuid;

    use crate::{FieldRef, FilterExpr};

    fn context(namespace_id: NamespaceId) -> ProjectionContext {
        ProjectionContext {
            archetype_id: ArchetypeId::WORKFLOW,
            namespace_id,
            business_id: "wf-1".to_string(),
            authority_epoch: 0,
            status_keyword: "Running".to_string(),
            lifecycle_state: VisibilityLifecycleState::Open,
            workflow_id: WorkflowId("wf-1".to_string()),
            run_id: RunId(Uuid::from_u128(2)),
            workflow_type: WorkflowType("Workflow".to_string()),
            task_queue: TaskQueueName("queue-a".to_string()),
            execution_status: ExecutionStatus::Running,
            start_time: OffsetDateTime::from_unix_timestamp(10).unwrap(),
            update_time: OffsetDateTime::from_unix_timestamp(10).unwrap(),
            execution_time: Some(OffsetDateTime::from_unix_timestamp(11).unwrap()),
            close_time: None,
            history_length: 3,
            execution_duration: None,
            state_transition_count: 4,
            transition_count: 4,
            history_size_bytes: 0,
            parent_workflow_id: None,
            parent_run_id: None,
            root_workflow_id: Some(WorkflowId("wf-1".to_string())),
            root_run_id: Some(RunId(Uuid::from_u128(2))),
            search_attr_generation: 0,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
        }
    }

    fn record_with_search_attr(
        namespace_id: NamespaceId,
        value: SearchAttrValue,
    ) -> ProjectionRecord {
        let mut memo_patch = Memo::default();
        memo_patch
            .0
            .insert("note".to_string(), Payload::new("hello"));
        let mut search_attr_patch = SearchAttributes::default();
        search_attr_patch
            .0
            .insert("CustomKeyword".to_string(), value);

        let mut context = context(namespace_id);
        context.memo = memo_patch;
        context.search_attributes = search_attr_patch;

        ProjectionRecord {
            partition_id: 0,
            fanout: 1,
            run_key: RunKey(Uuid::from_u128(3)),
            transition_seq: TransitionSeq(1),
            context,
        }
    }

    use proptest::prelude::*;

    fn arb_status() -> impl Strategy<Value = ExecutionStatus> {
        prop_oneof![
            Just(ExecutionStatus::Running),
            Just(ExecutionStatus::Completed),
            Just(ExecutionStatus::Failed),
            Just(ExecutionStatus::Cancelled),
            Just(ExecutionStatus::Terminated),
            Just(ExecutionStatus::ContinuedAsNew),
            Just(ExecutionStatus::TimedOut),
        ]
    }

    fn arb_context(ns: NamespaceId) -> impl Strategy<Value = ProjectionContext> {
        (
            arb_status(),
            "[a-z]{1,8}",
            any::<u128>(),
            "[A-Z][a-z]{1,6}",
            "[a-z]{1,6}",
            1i64..1_000_000,
            1i64..100,
            1i64..100,
        )
            .prop_map(
                move |(status, wf_id, run_id, wf_type, tq, start, hl, stc)| ProjectionContext {
                    archetype_id: ArchetypeId::WORKFLOW,
                    namespace_id: ns,
                    business_id: wf_id.clone(),
                    authority_epoch: 0,
                    status_keyword: crate::types::workflow_status_keyword(status),
                    lifecycle_state: crate::types::workflow_lifecycle_state(status),
                    workflow_id: WorkflowId(wf_id.clone()),
                    run_id: RunId(Uuid::from_u128(run_id)),
                    workflow_type: WorkflowType(wf_type),
                    task_queue: TaskQueueName(tq),
                    execution_status: status,
                    start_time: OffsetDateTime::from_unix_timestamp(start).unwrap(),
                    update_time: OffsetDateTime::from_unix_timestamp(start).unwrap(),
                    execution_time: None,
                    close_time: None,
                    history_length: hl,
                    execution_duration: None,
                    state_transition_count: stc,
                    transition_count: stc,
                    history_size_bytes: 0,
                    parent_workflow_id: None,
                    parent_run_id: None,
                    root_workflow_id: Some(WorkflowId(wf_id)),
                    root_run_id: Some(RunId(Uuid::from_u128(run_id))),
                    search_attr_generation: stc as u64,
                    memo: Memo::default(),
                    search_attributes: SearchAttributes::default(),
                },
            )
    }

    fn arb_record(ns: NamespaceId) -> impl Strategy<Value = ProjectionRecord> {
        (any::<u128>(), arb_context(ns)).prop_map(|(rk, ctx)| ProjectionRecord {
            partition_id: 0,
            fanout: 1,
            run_key: RunKey(Uuid::from_u128(rk)),
            transition_seq: TransitionSeq(1),
            context: ctx,
        })
    }

    // Feature: projection-visibility, Property 1:
    // Apply Correctness
    // **Validates: Requirements 1.1, 1.2, 1.4, 1.5**
    proptest! {
        #![proptest_config(
            ProptestConfig::with_cases(100)
        )]
        #[test]
        fn prop_apply_correctness(
            record in arb_record(
                NamespaceId(Uuid::from_u128(1))
            ),
        ) {
            let rt =
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
            rt.block_on(async {
                let store =
                    InMemoryVisibilityStore::default();
                let sink =
                    VisibilitySink::new(store.clone());
                sink.apply(&record, 0).await.unwrap();
                let row =
                    store.get_row(record.run_key).await.unwrap();
                prop_assert_eq!(
                    row.namespace_id,
                    record.context.namespace_id
                );
                prop_assert_eq!(
                    row.workflow_id,
                    record.context.workflow_id
                );
                prop_assert_eq!(
                    row.run_id, record.context.run_id
                );
                prop_assert_eq!(
                    row.workflow_type,
                    record.context.workflow_type
                );
                prop_assert_eq!(
                    row.task_queue,
                    record.context.task_queue
                );
                prop_assert_eq!(
                    row.start_time,
                    record.context.start_time
                );
                prop_assert_eq!(
                    row.history_length,
                    record.context.history_length
                );
                prop_assert_eq!(
                    row.state_transition_count,
                    record.context.state_transition_count
                );
                // Snapshot application is intentionally self-contained; the
                // context is the post-transition image produced by storage.
                prop_assert_eq!(row.status, record.context.execution_status);
                Ok(())
            })?;
        }
    }

    // Feature: projection-visibility, Property 2:
    // Idempotent Apply
    // **Validates: Requirements 1.3**
    proptest! {
        #![proptest_config(
            ProptestConfig::with_cases(100)
        )]
        #[test]
        fn prop_idempotent_apply(
            record in arb_record(
                NamespaceId(Uuid::from_u128(1))
            ),
        ) {
            let rt =
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
            rt.block_on(async {
                let store =
                    InMemoryVisibilityStore::default();
                let sink =
                    VisibilitySink::new(store.clone());
                sink.apply(&record, 0).await.unwrap();
                let row1 =
                    store.get_row(record.run_key).await.unwrap();
                sink.apply(&record, 0).await.unwrap();
                let row2 =
                    store.get_row(record.run_key).await.unwrap();
                // Core fields must be identical
                prop_assert_eq!(
                    row1.namespace_id,
                    row2.namespace_id
                );
                prop_assert_eq!(
                    row1.workflow_id, row2.workflow_id
                );
                prop_assert_eq!(row1.run_id, row2.run_id);
                prop_assert_eq!(row1.status, row2.status);
                prop_assert_eq!(
                    row1.start_time, row2.start_time
                );
                prop_assert_eq!(
                    row1.close_time, row2.close_time
                );
                prop_assert_eq!(
                    row1.workflow_type,
                    row2.workflow_type
                );
                prop_assert_eq!(
                    row1.task_queue, row2.task_queue
                );
                prop_assert_eq!(
                    row1.history_length,
                    row2.history_length
                );
                prop_assert_eq!(
                    row1.state_transition_count,
                    row2.state_transition_count
                );
                Ok(())
            })?;
        }
    }

    #[tokio::test]
    async fn apply_rejects_unknown_search_attribute() {
        let namespace_id = NamespaceId(Uuid::from_u128(1));
        let store = InMemoryVisibilityStore::default();
        let sink = VisibilitySink::new(store);

        let error = sink
            .apply(
                &record_with_search_attr(
                    namespace_id,
                    SearchAttrValue::Keyword("blue".to_string()),
                ),
                0,
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("unknown search attribute"));
    }

    #[tokio::test]
    async fn apply_rejects_type_mismatch() {
        let namespace_id = NamespaceId(Uuid::from_u128(1));
        let store = InMemoryVisibilityStore::default();
        store
            .register_attr(
                namespace_id,
                "CustomKeyword".to_string(),
                SearchAttrType::Int,
            )
            .await
            .unwrap();
        let sink = VisibilitySink::new(store);

        let error = sink
            .apply(
                &record_with_search_attr(
                    namespace_id,
                    SearchAttrValue::Keyword("blue".to_string()),
                ),
                0,
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("type mismatch"));
    }

    #[tokio::test]
    async fn apply_persists_row_and_indexes() {
        let namespace_id = NamespaceId(Uuid::from_u128(1));
        let store = InMemoryVisibilityStore::default();
        store
            .register_attr(
                namespace_id,
                "CustomKeyword".to_string(),
                SearchAttrType::Keyword,
            )
            .await
            .unwrap();
        let sink = VisibilitySink::new(store.clone());
        let record =
            record_with_search_attr(namespace_id, SearchAttrValue::Keyword("blue".to_string()));

        sink.apply(&record, 0).await.unwrap();

        let row = store.get_row(record.run_key).await.unwrap();
        assert_eq!(row.workflow_id.0, "wf-1");
        assert_eq!(row.status, ExecutionStatus::Running);
        assert_eq!(row.memo.0["note"], Payload::new("hello"));

        let filter = CompiledFilter {
            expr: Some(FilterExpr::Compare {
                field: FieldRef::Custom {
                    name: "CustomKeyword".to_string(),
                    attr_id: AttrId(1),
                    attr_type: SearchAttrType::Keyword,
                },
                op: crate::types::CompareOp::Eq,
                value: crate::types::FilterValue::String("blue".to_string()),
            }),
        };
        let result = store
            .list_executions(
                namespace_id,
                &filter,
                SortOrder::Default,
                &PageBounds {
                    limit: 10,
                    after: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn newer_snapshot_removes_omitted_search_attributes() {
        let namespace_id = NamespaceId(Uuid::from_u128(1));
        let store = InMemoryVisibilityStore::default();
        store
            .register_attr(
                namespace_id,
                "CustomKeyword".to_string(),
                SearchAttrType::Keyword,
            )
            .await
            .unwrap();
        let sink = VisibilitySink::new(store.clone());
        let first =
            record_with_search_attr(namespace_id, SearchAttrValue::Keyword("blue".to_string()));
        let mut second_context = context(namespace_id);
        second_context.search_attr_generation = 2;
        let second = ProjectionRecord {
            partition_id: first.partition_id,
            fanout: first.fanout,
            run_key: first.run_key,
            transition_seq: TransitionSeq(2),
            context: second_context,
        };

        sink.apply(&first, 0).await.unwrap();
        sink.apply(&second, 0).await.unwrap();

        let filter = CompiledFilter {
            expr: Some(FilterExpr::Compare {
                field: FieldRef::Custom {
                    name: "CustomKeyword".to_string(),
                    attr_id: AttrId(1),
                    attr_type: SearchAttrType::Keyword,
                },
                op: crate::types::CompareOp::Eq,
                value: crate::types::FilterValue::String("blue".to_string()),
            }),
        };
        let result = store
            .list_executions(
                namespace_id,
                &filter,
                SortOrder::Default,
                &PageBounds {
                    limit: 10,
                    after: None,
                },
            )
            .await
            .unwrap();
        assert!(result.rows.is_empty());
    }

    /// Build a projection record whose entire content is a deterministic
    /// function of its version `(authority_epoch, transition_seq)`.
    ///
    /// Because content depends only on the version, two records that tie on
    /// version are byte-identical, so "the newest version's image" is
    /// unambiguous regardless of apply order, retries, or ties — which is
    /// exactly the convergence target Properties 12 and 13 assert against.
    /// `transition_seq` selects the status from a table spanning open and
    /// terminal statuses, so a stale (older, possibly-open) snapshot applied
    /// after a newer terminal one must be rejected by the version gate, not
    /// allowed to reopen the row. `workflow_type`/`task_queue` derive from the
    /// run key (stable across an execution's versions), so the rollup
    /// dimensions partition executions the way a real producer would.
    fn record_for(
        run_key: RunKey,
        namespace_id: NamespaceId,
        authority_epoch: i64,
        seq: u64,
    ) -> ProjectionRecord {
        const STATUSES: [ExecutionStatus; 8] = [
            ExecutionStatus::Running,
            ExecutionStatus::Paused,
            ExecutionStatus::Completed,
            ExecutionStatus::Failed,
            ExecutionStatus::Cancelled,
            ExecutionStatus::Terminated,
            ExecutionStatus::ContinuedAsNew,
            ExecutionStatus::TimedOut,
        ];
        let status = STATUSES[(seq % 8) as usize];
        let h = run_key.0.as_u128();
        let business = format!("wf-{}", run_key.0);
        let start = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        let close = if status.is_open() {
            None
        } else {
            Some(OffsetDateTime::from_unix_timestamp(1_000 + seq as i64).unwrap())
        };
        let context = ProjectionContext {
            archetype_id: ArchetypeId::WORKFLOW,
            namespace_id,
            business_id: business.clone(),
            authority_epoch,
            status_keyword: crate::types::workflow_status_keyword(status),
            lifecycle_state: crate::types::workflow_lifecycle_state(status),
            workflow_id: WorkflowId(business.clone()),
            run_id: RunId(run_key.0),
            workflow_type: WorkflowType(format!("Type{}", h % 3)),
            task_queue: TaskQueueName(format!("queue-{}", h % 2)),
            execution_status: status,
            start_time: start,
            update_time: close.unwrap_or(start),
            execution_time: Some(start),
            close_time: close,
            history_length: seq as i64,
            execution_duration: None,
            state_transition_count: seq as i64,
            transition_count: seq as i64,
            history_size_bytes: 0,
            parent_workflow_id: None,
            parent_run_id: None,
            root_workflow_id: Some(WorkflowId(business)),
            root_run_id: Some(RunId(run_key.0)),
            search_attr_generation: seq,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
        };
        ProjectionRecord {
            partition_id: 0,
            fanout: 1,
            run_key,
            transition_seq: TransitionSeq(seq),
            context,
        }
    }

    /// Collapse a count result into a value→count map, dropping zero buckets.
    ///
    /// `accumulate_rollup` leaves a `0` counter behind when an execution leaves
    /// a status/type/queue bucket (the `-1` is not GC'd), whereas a fresh scan
    /// only reports buckets with live rows. Treating "absent" and "zero" as
    /// equal is the right comparison: both mean "no live execution here".
    fn nonzero_map(result: &CountResult) -> std::collections::BTreeMap<String, i64> {
        result
            .groups
            .iter()
            .filter(|g| g.count != 0)
            .map(|g| (g.value.clone(), g.count))
            .collect()
    }

    // Feature: chasm-foundation, Property 12: Monotonic snapshot apply.
    // For any interleaving/retry/out-of-order set of versioned snapshots for one
    // execution, the sink converges to the newest-version image and the stored
    // version never regresses — so a stale (older) snapshot can neither revert
    // the status nor reopen a closed execution.
    // **Validates: Requirements 10.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_monotonic_snapshot_apply(
            versions in proptest::collection::vec((0i64..3, 1u64..40), 1..12),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let ns = NamespaceId(Uuid::from_u128(1));
                let run_key = RunKey(Uuid::from_u128(7));
                let store = InMemoryVisibilityStore::default();
                let sink = VisibilitySink::new(store.clone());

                // Apply each version in the generated (arbitrary) order, with
                // duplicates standing in for retries. After every apply the
                // stored row must equal the image of the highest version seen
                // so far — a step-wise non-regression invariant that also proves
                // final convergence on the last iteration.
                let mut applied_max: Option<(i64, u64)> = None;
                for &(epoch, seq) in &versions {
                    sink.apply(&record_for(run_key, ns, epoch, seq), 0)
                        .await
                        .unwrap();
                    applied_max = Some(match applied_max {
                        Some(m) => m.max((epoch, seq)),
                        None => (epoch, seq),
                    });
                    let (max_epoch, max_seq) = applied_max.unwrap();
                    let expected = record_for(run_key, ns, max_epoch, max_seq);
                    let row = store.get_row(run_key).await.unwrap();

                    prop_assert_eq!(row.authority_epoch, max_epoch);
                    prop_assert_eq!(row.source_transition_seq, TransitionSeq(max_seq));
                    prop_assert_eq!(row.status, expected.context.execution_status);
                    prop_assert_eq!(row.status_keyword, expected.context.status_keyword);
                    prop_assert_eq!(row.lifecycle_state, expected.context.lifecycle_state);
                    prop_assert_eq!(row.close_time, expected.context.close_time);
                    prop_assert_eq!(row.history_length, expected.context.history_length);
                    prop_assert_eq!(row.transition_count, expected.context.transition_count);
                }
                Ok(())
            })?;
        }
    }

    // Feature: chasm-foundation, Property 13: Idempotent rollups.
    // For any replayed/superseded/out-of-order stream of versioned snapshots
    // across many executions, the maintained striped rollup never double-counts:
    // for every dimension it equals a fresh rebuild scanned from the current
    // rows, and its total equals the live execution count.
    // **Validates: Requirements 10.8**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_rollup_idempotent_rebuildable(
            // (execution, authority_epoch, transition_seq) applied in order;
            // repeated triples model retries, arbitrary ordering models the
            // interleaving of independent executions' transitions.
            ops in proptest::collection::vec((0u128..5, 0i64..3, 1u64..30), 1..40),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let ns = NamespaceId(Uuid::from_u128(1));
                let store = InMemoryVisibilityStore::default();
                let sink = VisibilitySink::new(store.clone());

                for &(exec, epoch, seq) in &ops {
                    let run_key = RunKey(Uuid::from_u128(exec + 1));
                    sink.apply(&record_for(run_key, ns, epoch, seq), 0)
                        .await
                        .unwrap();
                }

                for (dimension, group_by) in [
                    (
                        RollupDimension::ExecutionStatus,
                        GroupByField::System(crate::types::SystemField::ExecutionStatus),
                    ),
                    (
                        RollupDimension::WorkflowType,
                        GroupByField::System(crate::types::SystemField::WorkflowType),
                    ),
                    (
                        RollupDimension::TaskQueue,
                        GroupByField::System(crate::types::SystemField::TaskQueue),
                    ),
                ] {
                    let rollup = store
                        .count_from_rollup(ns, ArchetypeId::WORKFLOW, dimension)
                        .await
                        .unwrap();
                    let scan = store
                        .count_executions(ns, &CompiledFilter::default(), Some(group_by))
                        .await
                        .unwrap();
                    prop_assert_eq!(
                        nonzero_map(&rollup),
                        nonzero_map(&scan),
                        "maintained rollup must equal the rebuild for {:?}",
                        dimension
                    );
                    // Every live row falls in exactly one bucket of a
                    // partitioning dimension, so the rollup total equals the
                    // scanned live count.
                    let live: i64 = scan.groups.iter().map(|g| g.count).sum();
                    let rollup_total: i64 =
                        rollup.groups.iter().filter(|g| g.count != 0).map(|g| g.count).sum();
                    prop_assert_eq!(rollup_total, live);
                }
                Ok(())
            })?;
        }
    }
}
