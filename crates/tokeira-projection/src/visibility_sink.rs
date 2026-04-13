use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokeira_kernel::ProjectionOp;
use tokeira_storage::ProjectionRecord;
use tokeira_types::{Memo, SearchAttributes};

use crate::{
    rollup::compute_rollup_deltas,
    sink::ProjectionSink,
    store::VisibilityStore,
    types::{
        AttrDescriptor, AttrId, CompiledFilter, CountResult, ExecutionRow, GroupByField,
        ListResult, PageBounds, RollupDelta, RollupDimension, SearchAttrType,
        SortOrder, search_attr_type_of,
    },
};

pub struct VisibilitySink<S> {
    store: S,
    sink_id: String,
}

impl<S> VisibilitySink<S> {
    pub fn new(store: S, sink_id: impl Into<String>) -> Self {
        Self {
            store,
            sink_id: sink_id.into(),
        }
    }

    pub fn sink_id(&self) -> &str {
        &self.sink_id
    }
}

#[async_trait]
impl<S> VisibilityStore for VisibilitySink<S>
where
    S: VisibilityStore,
{
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
        self.store.list_executions(namespace_id, filter, sort, page).await
    }
    async fn count_executions(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        filter: &CompiledFilter,
        group_by: Option<GroupByField>,
    ) -> Result<CountResult> {
        self.store.count_executions(namespace_id, filter, group_by).await
    }
    async fn count_from_rollup(
        &self,
        namespace_id: tokeira_types::NamespaceId,
        dimension: RollupDimension,
    ) -> Result<CountResult> {
        self.store.count_from_rollup(namespace_id, dimension).await
    }
    async fn load_checkpoint(
        &self,
        sink_id: &str,
    ) -> Result<Option<tokeira_types::ProjectionCursor>> {
        self.store.load_checkpoint(sink_id).await
    }
    async fn save_checkpoint(
        &self,
        sink_id: &str,
        cursor: &tokeira_types::ProjectionCursor,
    ) -> Result<()> {
        self.store.save_checkpoint(sink_id, cursor).await
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
        self.store.register_attr(namespace_id, name, attr_type).await
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
    async fn apply(&self, record: &ProjectionRecord) -> Result<()> {
        let mut row = self.store.get_row(record.run_key).await.unwrap_or(ExecutionRow {
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
        });
        let previous = Some(row.clone());
        let mut search_patch = SearchAttributes::default();

        row.namespace_id = record.context.namespace_id;
        row.workflow_id = record.context.workflow_id.clone();
        row.run_id = record.context.run_id;
        row.workflow_type = record.context.workflow_type.clone();
        row.task_queue = record.context.task_queue.clone();
        row.start_time = record.context.start_time;
        row.execution_time = record.context.execution_time;
        row.history_length = record.context.history_length;
        row.state_transition_count = record.context.state_transition_count;

        for op in &record.ops {
            match op {
                ProjectionOp::UpsertExecution {
                    status,
                    memo_patch,
                    search_attr_patch,
                } => {
                    row.status = *status;
                    row.memo.0.extend(memo_patch.0.clone());
                    search_patch.0.extend(search_attr_patch.0.clone());
                }
                ProjectionOp::CloseExecution { status, closed_at } => {
                    row.status = *status;
                    row.close_time = Some(*closed_at);
                }
            }
        }

        self.store.upsert_execution(&row).await?;
        for (name, value) in &search_patch.0 {
            let Some(attr) = self
                .store
                .resolve_attr(record.context.namespace_id, name)
                .await?
            else {
                return Err(anyhow!("unknown search attribute: {name}"));
            };
            let expected = attr.attr_type;
            let actual = search_attr_type_of(value);
            if expected != actual {
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
            row.search_attr_version += 1;
        }
        self.store.upsert_execution(&row).await?;
        let deltas = compute_rollup_deltas(previous.as_ref(), &row);
        self.store.accumulate_rollup(&deltas).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::InMemoryVisibilityStore;
    use time::OffsetDateTime;
    use tokeira_kernel::ProjectionOp;
    use tokeira_storage::ProjectionContext;
    use tokeira_types::{
        ExecutionStatus, NamespaceId, Payload, RunId, RunKey, SearchAttrValue,
        TaskQueueName, TransitionSeq, WorkflowId, WorkflowType,
    };
    use uuid::Uuid;

    use crate::{FieldRef, FilterExpr};

    fn context(namespace_id: NamespaceId) -> ProjectionContext {
        ProjectionContext {
            namespace_id,
            workflow_id: WorkflowId("wf-1".to_string()),
            run_id: RunId(Uuid::from_u128(2)),
            workflow_type: WorkflowType("Workflow".to_string()),
            task_queue: TaskQueueName("queue-a".to_string()),
            execution_status: ExecutionStatus::Running,
            start_time: OffsetDateTime::from_unix_timestamp(10).unwrap(),
            execution_time: Some(OffsetDateTime::from_unix_timestamp(11).unwrap()),
            close_time: None,
            history_length: 3,
            state_transition_count: 4,
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

        ProjectionRecord {
            partition_id: 0,
            fanout: 1,
            run_key: RunKey(Uuid::from_u128(3)),
            transition_seq: TransitionSeq(1),
            context: context(namespace_id),
            ops: vec![ProjectionOp::UpsertExecution {
                status: ExecutionStatus::Running,
                memo_patch,
                search_attr_patch,
            }],
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

    fn arb_context(
        ns: NamespaceId,
    ) -> impl Strategy<Value = ProjectionContext> {
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
                move |(
                    status,
                    wf_id,
                    run_id,
                    wf_type,
                    tq,
                    start,
                    hl,
                    stc,
                )| {
                    ProjectionContext {
                        namespace_id: ns,
                        workflow_id: WorkflowId(wf_id),
                        run_id: RunId(Uuid::from_u128(run_id)),
                        workflow_type: WorkflowType(wf_type),
                        task_queue: TaskQueueName(tq),
                        execution_status: status,
                        start_time: OffsetDateTime::from_unix_timestamp(start).unwrap(),
                        execution_time: None,
                        close_time: None,
                        history_length: hl,
                        state_transition_count: stc,
                    }
                },
            )
    }

    fn arb_record(
        ns: NamespaceId,
    ) -> impl Strategy<Value = ProjectionRecord> {
        (
            any::<u128>(),
            arb_context(ns),
            arb_status(),
        )
            .prop_map(|(rk, ctx, status)| {
                ProjectionRecord {
                    partition_id: 0,
                    fanout: 1,
                    run_key: RunKey(Uuid::from_u128(rk)),
                    transition_seq: TransitionSeq(1),
                    context: ctx,
                    ops: vec![
                        ProjectionOp::UpsertExecution {
                            status,
                            memo_patch: Memo::default(),
                            search_attr_patch:
                                SearchAttributes::default(),
                        },
                    ],
                }
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
                    VisibilitySink::new(store.clone(), "s");
                sink.apply(&record).await.unwrap();
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
                // Status comes from the op, not context
                if let ProjectionOp::UpsertExecution {
                    status, ..
                } = &record.ops[0]
                {
                    prop_assert_eq!(row.status, *status);
                }
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
                    VisibilitySink::new(store.clone(), "s");
                sink.apply(&record).await.unwrap();
                let row1 =
                    store.get_row(record.run_key).await.unwrap();
                sink.apply(&record).await.unwrap();
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
        let sink = VisibilitySink::new(store, "sink");

        let error = sink
            .apply(&record_with_search_attr(
                namespace_id,
                SearchAttrValue::Keyword("blue".to_string()),
            ))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("unknown search attribute"));
    }

    #[tokio::test]
    async fn apply_rejects_type_mismatch() {
        let namespace_id = NamespaceId(Uuid::from_u128(1));
        let store = InMemoryVisibilityStore::default();
        store
            .register_attr(namespace_id, "CustomKeyword".to_string(), SearchAttrType::Int)
            .await
            .unwrap();
        let sink = VisibilitySink::new(store, "sink");

        let error = sink
            .apply(&record_with_search_attr(
                namespace_id,
                SearchAttrValue::Keyword("blue".to_string()),
            ))
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
        let sink = VisibilitySink::new(store.clone(), "sink");
        let record = record_with_search_attr(
            namespace_id,
            SearchAttrValue::Keyword("blue".to_string()),
        );

        sink.apply(&record).await.unwrap();

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
}
