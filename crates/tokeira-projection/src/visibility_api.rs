//! Visibility query API types and trait.
//!
//! These types define the contract between the projection plane (which owns
//! visibility data) and the edge layer (which translates gRPC requests into
//! these types). The trait and DTOs live here in `tokeira-projection` because
//! projection is the authoritative owner of visibility state.

use anyhow::Result;
use async_trait::async_trait;
use time::OffsetDateTime;
use tokeira_storage::ProjectionRecord;
use tokeira_types::{
    ArchetypeId, ExecutionStatus, Memo, NamespaceId, RunId, SearchAttributes, WorkflowId,
};

/// Summary of a single workflow execution for list/count responses.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowExecutionSummary {
    pub namespace: String,
    pub workflow_id: String,
    pub run_id: RunId,
    pub workflow_type: String,
    pub task_queue: String,
    pub status: ExecutionStatus,
    pub start_time: Option<OffsetDateTime>,
    /// Scheduled first-workflow-task time (`start_time + FirstWorkflowTaskBackoff`);
    /// distinct from `start_time` for cron/delayed/retry starts (v1.31.0
    /// `WorkflowExecutionInfo.execution_time`).
    pub execution_time: Option<OffsetDateTime>,
    pub close_time: Option<OffsetDateTime>,
    pub history_length: i64,
    pub state_transition_count: i64,
    pub parent_workflow_id: Option<WorkflowId>,
    pub parent_run_id: Option<RunId>,
    pub root_workflow_id: WorkflowId,
    pub root_run_id: RunId,
    pub memo: Memo,
    pub search_attributes: SearchAttributes,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListWorkflowExecutionsRequest {
    pub namespace: String,
    pub query: Option<String>,
    pub page_size: usize,
    pub next_page_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListWorkflowExecutionsResponse {
    pub executions: Vec<WorkflowExecutionSummary>,
    pub next_page_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GroupCount {
    pub value: String,
    pub count: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CountWorkflowExecutionsRequest {
    pub namespace: String,
    pub query: Option<String>,
    pub group_by: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CountWorkflowExecutionsResponse {
    pub total_count: i64,
    pub groups: Vec<GroupCount>,
}

/// Summary of a single standalone-activity execution for list responses.
///
/// A projection of the same generic visibility row as [`WorkflowExecutionSummary`],
/// shaped for the activity archetype: `activity_id`/`activity_type` are the generic
/// `business_id`/`execution_type`, and `state_transition_count` is the generic
/// `transition_count` (Requirement 10.14). `status_keyword` is the **collapsed** API
/// status the index stores (23.7/24.3); the edge maps it to the
/// `ActivityExecutionStatus` wire enum (Requirement 13.3).
#[derive(Clone, Debug, PartialEq)]
pub struct ActivityExecutionSummary {
    pub namespace: String,
    pub activity_id: String,
    pub run_id: RunId,
    pub activity_type: String,
    pub task_queue: String,
    pub status_keyword: String,
    pub schedule_time: Option<OffsetDateTime>,
    pub close_time: Option<OffsetDateTime>,
    pub state_transition_count: i64,
    pub state_size_bytes: i64,
    pub execution_duration: Option<i64>,
    pub search_attributes: SearchAttributes,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListActivityExecutionsRequest {
    pub namespace: String,
    pub query: Option<String>,
    pub page_size: usize,
    pub next_page_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListActivityExecutionsResponse {
    pub executions: Vec<ActivityExecutionSummary>,
    pub next_page_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CountActivityExecutionsRequest {
    pub namespace: String,
    pub query: Option<String>,
    pub group_by: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CountActivityExecutionsResponse {
    pub total_count: i64,
    pub groups: Vec<GroupCount>,
}

/// Visibility query interface.
///
/// Implemented by [`crate::VisibilityQueryService`] in the projection plane.
/// The edge layer depends on this trait to dispatch list/count/delete requests
/// without knowing the projection internals.
///
/// The activity endpoints take an explicit `archetype_id` because the projection
/// plane is archetype-neutral: it does not know which id "activity" maps to. The
/// edge (which holds the CHASM registry) resolves it and forces the scope here
/// (Requirement 13.1).
#[async_trait]
pub trait VisibilityApi: Send + Sync + 'static {
    async fn list_workflows(
        &self,
        req: ListWorkflowExecutionsRequest,
    ) -> Result<ListWorkflowExecutionsResponse>;

    async fn count_workflows(
        &self,
        req: CountWorkflowExecutionsRequest,
    ) -> Result<CountWorkflowExecutionsResponse>;

    async fn list_activities(
        &self,
        archetype_id: ArchetypeId,
        req: ListActivityExecutionsRequest,
    ) -> Result<ListActivityExecutionsResponse>;

    async fn count_activities(
        &self,
        archetype_id: ArchetypeId,
        req: CountActivityExecutionsRequest,
    ) -> Result<CountActivityExecutionsResponse>;

    /// Apply the durable deletion tombstone produced by authoritative storage.
    async fn apply_deletion(&self, tombstone: ProjectionRecord) -> Result<()>;

    /// Probe the namespace's registered search attributes (system predefined +
    /// custom-registered) for any key in `keys` that is NOT registered, returning
    /// the first such key (or `None` when all are known). The edge turns a returned
    /// key into the v1.31.0 admission error `InvalidArgument "search attribute <key>
    /// is not defined"` (`common/searchattribute/validator.go:101 @ v1.31.0`).
    ///
    /// The default is permissive — a deployment without a search-attribute registry
    /// validates nothing — so only the store-backed query service enforces it. Note
    /// this admits *registered* keys regardless of category; rejecting a user-set
    /// *system* SA ("`<name>` attribute can't be set in SearchAttributes") is a
    /// separate v1.31.0 rule not yet modelled here.
    async fn unknown_search_attribute(
        &self,
        _namespace_id: NamespaceId,
        _keys: &[String],
    ) -> Result<Option<String>> {
        Ok(None)
    }
}

/// No-op visibility implementation for tests and minimal bootstraps.
#[derive(Debug, Default)]
pub struct EmptyVisibilityApi;

#[async_trait]
impl VisibilityApi for EmptyVisibilityApi {
    async fn list_workflows(
        &self,
        _req: ListWorkflowExecutionsRequest,
    ) -> Result<ListWorkflowExecutionsResponse> {
        Ok(ListWorkflowExecutionsResponse {
            executions: Vec::new(),
            next_page_token: None,
        })
    }

    async fn count_workflows(
        &self,
        _req: CountWorkflowExecutionsRequest,
    ) -> Result<CountWorkflowExecutionsResponse> {
        Ok(CountWorkflowExecutionsResponse {
            total_count: 0,
            groups: Vec::new(),
        })
    }

    async fn list_activities(
        &self,
        _archetype_id: ArchetypeId,
        _req: ListActivityExecutionsRequest,
    ) -> Result<ListActivityExecutionsResponse> {
        Ok(ListActivityExecutionsResponse {
            executions: Vec::new(),
            next_page_token: None,
        })
    }

    async fn count_activities(
        &self,
        _archetype_id: ArchetypeId,
        _req: CountActivityExecutionsRequest,
    ) -> Result<CountActivityExecutionsResponse> {
        Ok(CountActivityExecutionsResponse {
            total_count: 0,
            groups: Vec::new(),
        })
    }

    async fn apply_deletion(&self, _tombstone: ProjectionRecord) -> Result<()> {
        Ok(())
    }
}
