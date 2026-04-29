//! Visibility query API types and trait.
//!
//! These types define the contract between the projection plane (which owns
//! visibility data) and the edge layer (which translates gRPC requests into
//! these types). The trait and DTOs live here in `tokeira-projection` because
//! projection is the authoritative owner of visibility state.

use anyhow::Result;
use async_trait::async_trait;
use time::OffsetDateTime;
use tokeira_types::{ExecutionStatus, Memo, RunId, RunKey, SearchAttributes};

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
    pub close_time: Option<OffsetDateTime>,
    pub history_length: i64,
    pub state_transition_count: i64,
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

/// Visibility query interface.
///
/// Implemented by [`crate::VisibilityQueryService`] in the projection plane.
/// The edge layer depends on this trait to dispatch list/count/delete requests
/// without knowing the projection internals.
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

    async fn delete_execution(&self, run_key: RunKey) -> Result<()>;
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

    async fn delete_execution(&self, _run_key: RunKey) -> Result<()> {
        Ok(())
    }
}
