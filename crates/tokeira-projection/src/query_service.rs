//! Read-side query service for the visibility projection.
//!
//! Implements the [`VisibilityApi`] trait that the edge layer depends on,
//! translating `ListWorkflowExecutions` and `CountWorkflowExecutions`
//! requests into store queries. Filter compilation, pagination token
//! encoding, and namespace resolution happen here so that the store
//! implementations stay purely mechanical.

use crate::visibility_api::{
    ActivityExecutionSummary, CountActivityExecutionsRequest, CountActivityExecutionsResponse,
    CountWorkflowExecutionsRequest, CountWorkflowExecutionsResponse, GroupCount,
    ListActivityExecutionsRequest, ListActivityExecutionsResponse, ListWorkflowExecutionsRequest,
    ListWorkflowExecutionsResponse, VisibilityApi, WorkflowExecutionSummary,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokeira_types::{ArchetypeId, NamespaceId, SearchAttributes};
use uuid::Uuid;

use crate::{
    filter::compile_filter,
    store::VisibilityStore,
    types::{GroupByField, PageBounds, PageToken, RollupDimension, SortOrder, SystemField},
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
        let mut filter = compile_filter(req.query.as_deref(), namespace_id, &self.store).await?;
        // Scope to the workflow archetype so the shared index never lists
        // activities (or other archetypes) through the workflow endpoint
        // (Requirement 13.1). Set after compiling the user query — no escape.
        filter.archetype = Some(ArchetypeId::WORKFLOW);
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
        let mut filter = compile_filter(req.query.as_deref(), namespace_id, &self.store).await?;
        // Workflow-scoped count: the unfiltered grouped paths below already pin
        // `ArchetypeId::WORKFLOW` on the rollup; pin it on the filtered path too so
        // a query-filtered count never spans archetypes (Requirement 13.1).
        filter.archetype = Some(ArchetypeId::WORKFLOW);
        let group_by = parse_group_by(req.group_by.as_deref(), namespace_id, &self.store).await?;
        let result = match (&filter.expr, &group_by) {
            // This service answers the workflow visibility endpoints, so the
            // rollup is scoped to the workflow archetype.
            (None, Some(GroupByField::System(SystemField::ExecutionStatus))) => {
                self.store
                    .count_from_rollup(
                        namespace_id,
                        ArchetypeId::WORKFLOW,
                        RollupDimension::ExecutionStatus,
                    )
                    .await?
            }
            (None, Some(GroupByField::System(SystemField::WorkflowType))) => {
                self.store
                    .count_from_rollup(
                        namespace_id,
                        ArchetypeId::WORKFLOW,
                        RollupDimension::WorkflowType,
                    )
                    .await?
            }
            (None, Some(GroupByField::System(SystemField::TaskQueue))) => {
                self.store
                    .count_from_rollup(
                        namespace_id,
                        ArchetypeId::WORKFLOW,
                        RollupDimension::TaskQueue,
                    )
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

    async fn list_activities(
        &self,
        archetype_id: ArchetypeId,
        req: ListActivityExecutionsRequest,
    ) -> Result<ListActivityExecutionsResponse> {
        let namespace_id = parse_namespace(&req.namespace)?;
        let mut filter = compile_filter(req.query.as_deref(), namespace_id, &self.store).await?;
        // Force the activity archetype the edge resolved; no caller escape (Req 13.1).
        filter.archetype = Some(archetype_id);
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
        Ok(ListActivityExecutionsResponse {
            executions: result.rows.into_iter().map(map_activity_summary).collect(),
            next_page_token: result.next_page_token.map(|t| t.encode()).transpose()?,
        })
    }

    async fn count_activities(
        &self,
        archetype_id: ArchetypeId,
        req: CountActivityExecutionsRequest,
    ) -> Result<CountActivityExecutionsResponse> {
        let namespace_id = parse_namespace(&req.namespace)?;
        let mut filter = compile_filter(req.query.as_deref(), namespace_id, &self.store).await?;
        filter.archetype = Some(archetype_id);
        let group_by = parse_group_by(req.group_by.as_deref(), namespace_id, &self.store).await?;
        let result = match (&filter.expr, &group_by) {
            // Unfiltered grouped counts read the archetype-scoped striped rollup,
            // pinned to the activity archetype the edge resolved.
            (None, Some(GroupByField::System(SystemField::ExecutionStatus))) => {
                self.store
                    .count_from_rollup(namespace_id, archetype_id, RollupDimension::ExecutionStatus)
                    .await?
            }
            (None, Some(GroupByField::System(SystemField::WorkflowType))) => {
                self.store
                    .count_from_rollup(namespace_id, archetype_id, RollupDimension::WorkflowType)
                    .await?
            }
            (None, Some(GroupByField::System(SystemField::TaskQueue))) => {
                self.store
                    .count_from_rollup(namespace_id, archetype_id, RollupDimension::TaskQueue)
                    .await?
            }
            // The filtered path honours `filter.archetype` directly.
            _ => {
                self.store
                    .count_executions(namespace_id, &filter, group_by)
                    .await?
            }
        };
        Ok(CountActivityExecutionsResponse {
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

    async fn apply_deletion(&self, tombstone: tokeira_storage::ProjectionRecord) -> Result<()> {
        self.store.apply_deletion(&tombstone).await
    }

    async fn unknown_search_attribute(
        &self,
        namespace_id: NamespaceId,
        keys: &[String],
    ) -> Result<Option<String>> {
        // A key is registered iff the store resolves it (system predefined keys are
        // seeded at namespace registration; custom keys arrive via AddSearchAttributes).
        // The query path uses the same resolution to compile filters, so admission and
        // query agree on what "defined" means.
        for key in keys {
            if self.store.resolve_attr(namespace_id, key).await?.is_none() {
                return Ok(Some(key.clone()));
            }
        }
        Ok(None)
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
        "ExecutionTime" => Some(SystemField::ExecutionTime),
        "CloseTime" => Some(SystemField::CloseTime),
        "HistoryLength" => Some(SystemField::HistoryLength),
        "ExecutionDuration" => Some(SystemField::ExecutionDuration),
        "StateTransitionCount" => Some(SystemField::StateTransitionCount),
        "HistorySizeBytes" => Some(SystemField::HistorySizeBytes),
        "ParentWorkflowId" => Some(SystemField::ParentWorkflowId),
        "ParentRunId" => Some(SystemField::ParentRunId),
        "RootWorkflowId" => Some(SystemField::RootWorkflowId),
        "RootRunId" => Some(SystemField::RootRunId),
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
        execution_time: row.execution_time,
        close_time: row.close_time,
        history_length: row.history_length,
        state_transition_count: row.state_transition_count,
        memo: row.memo,
        search_attributes: SearchAttributes::default(),
    }
}

fn map_activity_summary(row: crate::types::ExecutionRow) -> ActivityExecutionSummary {
    ActivityExecutionSummary {
        namespace: row.namespace_id.0.to_string(),
        // Generic business identity / execution type carry the activity id / type.
        activity_id: row.business_id,
        run_id: row.run_id,
        activity_type: row.workflow_type.0,
        task_queue: row.task_queue.0,
        // The index stores the collapsed API status (23.7/24.3); the edge maps it to
        // the `ActivityExecutionStatus` wire enum.
        status_keyword: row.status_keyword,
        schedule_time: Some(row.start_time),
        close_time: row.close_time,
        // `transition_count` → `state_transition_count` (Requirement 10.14).
        state_transition_count: row.transition_count,
        state_size_bytes: row.history_size_bytes,
        execution_duration: row.execution_duration,
        // The list query does not load the SA index (same as the workflow summary);
        // activities contribute no user search attributes anyway (24.3).
        search_attributes: SearchAttributes::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        memory::InMemoryVisibilityStore, sink::ProjectionSink, store::VisibilityStore,
        types::SearchAttrType, visibility_sink::VisibilitySink,
    };
    use time::OffsetDateTime;
    use tokeira_storage::{ProjectionContext, ProjectionRecord};
    use tokeira_types::{
        ExecutionStatus, Memo, NamespaceId, RunId, RunKey, SearchAttrValue, SearchAttributes,
        TaskQueueName, TransitionSeq, WorkflowId, WorkflowType,
    };
    use uuid::Uuid;

    #[tokio::test]
    async fn unknown_search_attribute_rejects_only_unregistered_keys() {
        // A registered key (system predefined or custom-added) is accepted; an
        // unregistered key is returned so the edge can raise
        // "search attribute <key> is not defined" (standalone_activity_test.go:521).
        let store = InMemoryVisibilityStore::default();
        let namespace_id = NamespaceId(Uuid::from_u128(7));
        store
            .register_attr(
                namespace_id,
                "CustomKeywordField".to_owned(),
                SearchAttrType::Keyword,
            )
            .await
            .expect("register custom SA");
        let svc = VisibilityQueryService::new(store);

        assert_eq!(
            svc.unknown_search_attribute(namespace_id, &["CustomKeywordField".to_owned()])
                .await
                .unwrap(),
            None,
            "a registered key is accepted"
        );
        assert_eq!(
            svc.unknown_search_attribute(namespace_id, &["InvalidSearchAttributeKey".to_owned()])
                .await
                .unwrap(),
            Some("InvalidSearchAttributeKey".to_owned()),
            "an unregistered key is reported"
        );
    }

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
                archetype_id: tokeira_types::ArchetypeId::WORKFLOW,
                namespace_id,
                business_id: workflow_id.to_string(),
                authority_epoch: 0,
                status_keyword: if close_time.is_some() {
                    "Completed".to_string()
                } else {
                    "Running".to_string()
                },
                lifecycle_state: if close_time.is_some() {
                    tokeira_types::VisibilityLifecycleState::Closed
                } else {
                    tokeira_types::VisibilityLifecycleState::Open
                },
                workflow_id: WorkflowId(workflow_id.to_string()),
                run_id: RunId(Uuid::from_u128(run_key.0.as_u128() + 100)),
                workflow_type: WorkflowType("Workflow".to_string()),
                task_queue: TaskQueueName(task_queue.to_string()),
                execution_status: if close_time.is_some() {
                    ExecutionStatus::Completed
                } else {
                    ExecutionStatus::Running
                },
                start_time: OffsetDateTime::from_unix_timestamp(run_key.0.as_u128() as i64)
                    .unwrap(),
                update_time: close_time.unwrap_or_else(|| {
                    OffsetDateTime::from_unix_timestamp(run_key.0.as_u128() as i64).unwrap()
                }),
                execution_time: None,
                close_time,
                history_length: 10,
                execution_duration: None,
                state_transition_count: 20,
                transition_count: 20,
                history_size_bytes: 0,
                parent_workflow_id: None,
                parent_run_id: None,
                root_workflow_id: Some(WorkflowId(workflow_id.to_string())),
                root_run_id: Some(RunId(Uuid::from_u128(run_key.0.as_u128() + 100))),
                search_attr_generation: 1,
                memo: Memo::default(),
                search_attributes: search_attr_patch,
            },
        }
    }

    async fn build_service() -> (NamespaceId, VisibilityQueryService<InMemoryVisibilityStore>) {
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
        let sink = VisibilitySink::new(store.clone());
        sink.apply(
            &projection_record(
                namespace_id,
                RunKey(Uuid::from_u128(1)),
                "wf-a",
                "queue-a",
                None,
            ),
            0,
        )
        .await
        .unwrap();
        sink.apply(
            &projection_record(
                namespace_id,
                RunKey(Uuid::from_u128(2)),
                "wf-b",
                "queue-b",
                Some(OffsetDateTime::from_unix_timestamp(200).unwrap()),
            ),
            0,
        )
        .await
        .unwrap();
        (namespace_id, VisibilityQueryService::new(store))
    }

    #[tokio::test]
    async fn activity_endpoints_are_archetype_scoped_and_mapped() {
        let namespace_id = NamespaceId(Uuid::from_u128(7));
        let activity = tokeira_types::ArchetypeId(99);
        let store = InMemoryVisibilityStore::default();
        let sink = VisibilitySink::new(store.clone());

        // One workflow and one activity share the index.
        let mut wf =
            projection_record(namespace_id, RunKey(Uuid::from_u128(1)), "wf-a", "qa", None);
        wf.context.search_attributes = SearchAttributes::default();
        sink.apply(&wf, 0).await.unwrap();

        let mut act = projection_record(
            namespace_id,
            RunKey(Uuid::from_u128(2)),
            "act-1",
            "act-q",
            None,
        );
        act.context.search_attributes = SearchAttributes::default();
        act.context.archetype_id = activity;
        act.context.workflow_type = WorkflowType("MyActivity".to_string());
        act.context.transition_count = 5;
        sink.apply(&act, 0).await.unwrap();

        let service = VisibilityQueryService::new(store);

        // list_activities returns only the activity row, mapped to the activity shape.
        let listed = service
            .list_activities(
                activity,
                ListActivityExecutionsRequest {
                    namespace: namespace_id.0.to_string(),
                    query: None,
                    page_size: 10,
                    next_page_token: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(listed.executions.len(), 1);
        let a = &listed.executions[0];
        assert_eq!(a.activity_id, "act-1");
        assert_eq!(a.activity_type, "MyActivity"); // execution_type, not the workflow type
        assert_eq!(a.task_queue, "act-q");
        assert_eq!(a.status_keyword, "Running");
        assert_eq!(a.state_transition_count, 5); // generic transition_count (Req 10.14)

        // The workflow endpoint excludes the activity (the leak fix, end to end).
        let workflows = service
            .list_workflows(ListWorkflowExecutionsRequest {
                namespace: namespace_id.0.to_string(),
                query: None,
                page_size: 10,
                next_page_token: None,
            })
            .await
            .unwrap();
        assert_eq!(workflows.executions.len(), 1);
        assert_eq!(workflows.executions[0].workflow_id, "wf-a");

        // count_activities counts only activities.
        let count = service
            .count_activities(
                activity,
                CountActivityExecutionsRequest {
                    namespace: namespace_id.0.to_string(),
                    query: None,
                    group_by: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(count.total_count, 1);
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
                        archetype_id: tokeira_types::ArchetypeId::WORKFLOW,
                        business_id: wf_id.clone(),
                        workflow_id: WorkflowId(wf_id.clone()),
                        run_id: RunId(Uuid::from_u128(run_id)),
                        authority_epoch: 0,
                        source_transition_seq: TransitionSeq(stc as u64),
                        status_keyword: crate::types::workflow_status_keyword(status),
                        lifecycle_state: crate::types::workflow_lifecycle_state(status),
                        workflow_type: WorkflowType(wf_type),
                        task_queue: TaskQueueName(tq),
                        status,
                        start_time: time::OffsetDateTime::from_unix_timestamp(start).unwrap(),
                        update_time: close
                            .map(|c| time::OffsetDateTime::from_unix_timestamp(c).unwrap())
                            .unwrap_or_else(|| {
                                time::OffsetDateTime::from_unix_timestamp(start).unwrap()
                            }),
                        execution_time: None,
                        close_time: close
                            .map(|c| time::OffsetDateTime::from_unix_timestamp(c).unwrap()),
                        history_length: hl,
                        execution_duration: close.map(|c| (c - start) * 1_000_000_000),
                        state_transition_count: stc,
                        history_size_bytes: 0,
                        parent_workflow_id: None,
                        parent_run_id: None,
                        root_workflow_id: WorkflowId(wf_id),
                        root_run_id: RunId(Uuid::from_u128(run_id)),
                        memo: Memo::default(),
                        search_attributes: SearchAttributes::default(),
                        transition_count: stc,
                        search_attr_generation: 0,
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
