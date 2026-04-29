//! Read-side query service for the visibility projection.
//!
//! Implements the [`VisibilityApi`] trait that the edge layer depends on,
//! translating `ListWorkflowExecutions` and `CountWorkflowExecutions`
//! requests into store queries. Filter compilation, pagination token
//! encoding, and namespace resolution happen here so that the store
//! implementations stay purely mechanical.

use crate::visibility_api::{
    CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse, GroupCount,
    ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse, VisibilityApi,
    WorkflowExecutionSummary,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokeira_types::{NamespaceId, SearchAttributes};
use uuid::Uuid;

use crate::{
    filter::compile_filter,
    store::VisibilityStore,
    types::{
        GroupByField, PageBounds, PageToken, RollupDimension, SortOrder, SystemField,
    },
};

pub struct VisibilityQueryService<S> {
    store: S,
}

impl<S> VisibilityQueryService<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S> VisibilityApi for VisibilityQueryService<S>
where
    S: VisibilityStore + 'static,
{
    async fn list_workflows(
        &self,
        req: ListWorkflowExecutionsRequest,
    ) -> Result<ListWorkflowExecutionsResponse> {
        let namespace_id = parse_namespace(&req.namespace)?;
        let filter =
            compile_filter(req.query.as_deref(), namespace_id, &self.store).await?;
        let page = PageBounds {
            limit: req.page_size.clamp(1, crate::types::MAX_PAGE_SIZE),
            after: req
                .next_page_token
                .as_deref()
                .map(PageToken::decode)
                .transpose()?,
        };
        let result = self
            .store
            .list_executions(namespace_id, &filter, SortOrder::Default, &page)
            .await?;
        Ok(ListWorkflowExecutionsResponse {
            executions: result.rows.into_iter().map(map_summary).collect(),
            next_page_token: result.next_page_token.map(|t| t.encode()).transpose()?,
        })
    }

    async fn count_workflows(
        &self,
        req: CountWorkflowExecutionsRequest,
    ) -> Result<CountWorkflowExecutionsResponse> {
        let namespace_id = parse_namespace(&req.namespace)?;
        let filter =
            compile_filter(req.query.as_deref(), namespace_id, &self.store).await?;
        let group_by =
            parse_group_by(req.group_by.as_deref(), namespace_id, &self.store).await?;
        let result = match (&filter.expr, &group_by) {
            (None, Some(GroupByField::System(SystemField::ExecutionStatus))) => {
                self.store
                    .count_from_rollup(namespace_id, RollupDimension::ExecutionStatus)
                    .await?
            }
            (None, Some(GroupByField::System(SystemField::WorkflowType))) => {
                self.store
                    .count_from_rollup(namespace_id, RollupDimension::WorkflowType)
                    .await?
            }
            (None, Some(GroupByField::System(SystemField::TaskQueue))) => {
                self.store
                    .count_from_rollup(namespace_id, RollupDimension::TaskQueue)
                    .await?
            }
            _ => {
                self.store
                    .count_executions(namespace_id, &filter, group_by)
                    .await?
            }
        };
        Ok(CountWorkflowExecutionsResponse {
            total_count: result.total_count,
            groups: result
                .groups
                .into_iter()
                .map(|g| GroupCount {
                    value: g.value,
                    count: g.count,
                })
                .collect(),
        })
    }

    async fn delete_execution(&self, run_key: tokeira_types::RunKey) -> Result<()> {
        self.store.delete_execution(run_key).await
    }
}

fn parse_namespace(input: &str) -> Result<NamespaceId> {
    if let Ok(uuid) = Uuid::parse_str(input) {
        return Ok(NamespaceId(uuid));
    }
    Ok(namespace_id_for(input))
}

fn namespace_id_for(name: &str) -> NamespaceId {
    let mut bytes = *b"tokeira-edge-ns!";
    for (idx, byte) in name.as_bytes().iter().enumerate() {
        let slot = idx % 16;
        bytes[slot] = bytes[slot]
            .wrapping_add(*byte)
            .rotate_left((idx % 8) as u32);
    }
    NamespaceId(Uuid::from_bytes(bytes))
}

async fn parse_group_by<S: VisibilityStore>(
    input: Option<&str>,
    namespace_id: NamespaceId,
    store: &S,
) -> Result<Option<GroupByField>> {
    let Some(input) = input.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let system = match input {
        "ExecutionStatus" => Some(SystemField::ExecutionStatus),
        "WorkflowType" => Some(SystemField::WorkflowType),
        "TaskQueue" => Some(SystemField::TaskQueue),
        "WorkflowId" => Some(SystemField::WorkflowId),
        "RunId" => Some(SystemField::RunId),
        "StartTime" => Some(SystemField::StartTime),
        "CloseTime" => Some(SystemField::CloseTime),
        "HistoryLength" => Some(SystemField::HistoryLength),
        "StateTransitionCount" => Some(SystemField::StateTransitionCount),
        _ => None,
    };
    if let Some(field) = system {
        return Ok(Some(GroupByField::System(field)));
    }
    let Some(attr) = store.resolve_attr(namespace_id, input).await? else {
        return Err(anyhow!("unknown search attribute: {input}"));
    };
    Ok(Some(GroupByField::Custom {
        name: input.to_string(),
        attr_id: attr.attr_id,
        attr_type: attr.attr_type,
    }))
}

fn map_summary(row: crate::types::ExecutionRow) -> WorkflowExecutionSummary {
    WorkflowExecutionSummary {
        namespace: row.namespace_id.0.to_string(),
        workflow_id: row.workflow_id.0,
        run_id: row.run_id,
        workflow_type: row.workflow_type.0,
        task_queue: row.task_queue.0,
        status: row.status,
        start_time: Some(row.start_time),
        close_time: row.close_time,
        history_length: row.history_length,
        state_transition_count: row.state_transition_count,
        memo: row.memo,
        search_attributes: SearchAttributes::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        memory::InMemoryVisibilityStore, sink::ProjectionSink,
        visibility_sink::VisibilitySink,
    };
    use time::OffsetDateTime;
    use tokeira_kernel::ProjectionOp;
    use tokeira_storage::{ProjectionContext, ProjectionRecord};
    use tokeira_types::{
        ExecutionStatus, Memo, NamespaceId, RunId, RunKey, SearchAttrValue,
        SearchAttributes, TaskQueueName, TransitionSeq, WorkflowId, WorkflowType,
    };
    use uuid::Uuid;

    fn projection_record(
        namespace_id: NamespaceId,
        run_key: RunKey,
        workflow_id: &str,
        task_queue: &str,
        close_time: Option<OffsetDateTime>,
    ) -> ProjectionRecord {
        let mut search_attr_patch = SearchAttributes::default();
        search_attr_patch.0.insert(
            "CustomKeyword".to_string(),
            SearchAttrValue::Keyword("blue".to_string()),
        );

        ProjectionRecord {
            partition_id: 0,
            fanout: 1,
            run_key,
            transition_seq: TransitionSeq(1),
            context: ProjectionContext {
                namespace_id,
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id: RunId(Uuid::from_u128(run_key.0.as_u128() + 100)),
                workflow_type: WorkflowType("Workflow".to_string()),
                task_queue: TaskQueueName(task_queue.to_string()),
                execution_status: if close_time.is_some() {
                    ExecutionStatus::Completed
                } else {
                    ExecutionStatus::Running
                },
                start_time: OffsetDateTime::from_unix_timestamp(
                    run_key.0.as_u128() as i64
                )
                .unwrap(),
                execution_time: None,
                close_time,
                history_length: 10,
                state_transition_count: 20,
            },
            ops: vec![ProjectionOp::UpsertExecution {
                status: if close_time.is_some() {
                    ExecutionStatus::Completed
                } else {
                    ExecutionStatus::Running
                },
                memo_patch: Memo::default(),
                search_attr_patch,
            }],
        }
    }

    async fn build_service()
    -> (NamespaceId, VisibilityQueryService<InMemoryVisibilityStore>) {
        let namespace_id = NamespaceId(Uuid::from_u128(1));
        let store = InMemoryVisibilityStore::default();
        store
            .register_attr(
                namespace_id,
                "CustomKeyword".to_string(),
                crate::types::SearchAttrType::Keyword,
            )
            .await
            .unwrap();
        let sink = VisibilitySink::new(store.clone(), "sink");
        sink.apply(&projection_record(
            namespace_id,
            RunKey(Uuid::from_u128(1)),
            "wf-a",
            "queue-a",
            None,
        ))
        .await
        .unwrap();
        sink.apply(&projection_record(
            namespace_id,
            RunKey(Uuid::from_u128(2)),
            "wf-b",
            "queue-b",
            Some(OffsetDateTime::from_unix_timestamp(200).unwrap()),
        ))
        .await
        .unwrap();
        (namespace_id, VisibilityQueryService::new(store))
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

    fn arb_execution_row() -> impl Strategy<Value = crate::types::ExecutionRow> {
        (
            any::<u128>(),
            any::<u128>(),
            "[a-z]{1,8}",
            any::<u128>(),
            "[A-Z][a-z]{1,6}",
            "[a-z]{1,6}",
            arb_status(),
            1i64..1_000_000,
            proptest::option::of(1i64..2_000_000),
            1i64..100,
            1i64..100,
        )
            .prop_map(
                |(ns, rk, wf_id, run_id, wf_type, tq, status, start, close, hl, stc)| {
                    crate::types::ExecutionRow {
                        run_key: RunKey(Uuid::from_u128(rk)),
                        namespace_id: NamespaceId(Uuid::from_u128(ns)),
                        workflow_id: WorkflowId(wf_id),
                        run_id: RunId(Uuid::from_u128(run_id)),
                        workflow_type: WorkflowType(wf_type),
                        task_queue: TaskQueueName(tq),
                        status,
                        start_time: time::OffsetDateTime::from_unix_timestamp(start)
                            .unwrap(),
                        execution_time: None,
                        close_time: close.map(|c| {
                            time::OffsetDateTime::from_unix_timestamp(c).unwrap()
                        }),
                        history_length: hl,
                        state_transition_count: stc,
                        memo: Memo::default(),
                        search_attr_version: 0,
                    }
                },
            )
    }

    // Feature: projection-visibility, Property 14:
    // ExecutionRow to Summary Mapping
    // **Validates: Requirements 7.1, 7.5**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_row_to_summary_mapping(
            row in arb_execution_row(),
        ) {
            let summary = map_summary(row.clone());
            prop_assert_eq!(
                summary.namespace,
                row.namespace_id.0.to_string()
            );
            prop_assert_eq!(
                summary.workflow_id, row.workflow_id.0
            );
            prop_assert_eq!(summary.run_id, row.run_id);
            prop_assert_eq!(
                summary.workflow_type,
                row.workflow_type.0
            );
            prop_assert_eq!(
                summary.task_queue, row.task_queue.0
            );
            prop_assert_eq!(summary.status, row.status);
            prop_assert_eq!(
                summary.start_time,
                Some(row.start_time)
            );
            prop_assert_eq!(
                summary.close_time, row.close_time
            );
        }
    }

    #[tokio::test]
    async fn list_workflows_returns_rows_and_pagination() {
        let (namespace_id, service) = build_service().await;
        let first = service
            .list_workflows(ListWorkflowExecutionsRequest {
                namespace: namespace_id.0.to_string(),
                query: None,
                page_size: 1,
                next_page_token: None,
            })
            .await
            .unwrap();
        assert_eq!(first.executions.len(), 1);
        assert!(first.next_page_token.is_some());

        let second = service
            .list_workflows(ListWorkflowExecutionsRequest {
                namespace: namespace_id.0.to_string(),
                query: None,
                page_size: 1,
                next_page_token: first.next_page_token,
            })
            .await
            .unwrap();
        assert_eq!(second.executions.len(), 1);
        assert_ne!(
            first.executions[0].workflow_id,
            second.executions[0].workflow_id
        );
    }

    #[tokio::test]
    async fn list_workflows_rejects_invalid_page_token() {
        let (namespace_id, service) = build_service().await;
        let error = service
            .list_workflows(ListWorkflowExecutionsRequest {
                namespace: namespace_id.0.to_string(),
                query: None,
                page_size: 10,
                next_page_token: Some("nope".to_string()),
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("malformed page token"));
    }

    #[tokio::test]
    async fn count_workflows_supports_total_and_grouping() {
        let (namespace_id, service) = build_service().await;
        let total = service
            .count_workflows(CountWorkflowExecutionsRequest {
                namespace: namespace_id.0.to_string(),
                query: None,
                group_by: None,
            })
            .await
            .unwrap();
        assert_eq!(total.total_count, 2);

        let grouped = service
            .count_workflows(CountWorkflowExecutionsRequest {
                namespace: namespace_id.0.to_string(),
                query: Some("CustomKeyword = \"blue\"".to_string()),
                group_by: Some("TaskQueue".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(grouped.total_count, 2);
        assert_eq!(grouped.groups.len(), 2);
    }
}
