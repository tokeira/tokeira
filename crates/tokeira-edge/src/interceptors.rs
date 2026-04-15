use std::sync::Arc;

use async_trait::async_trait;
use http::HeaderMap;
use time::OffsetDateTime;

use crate::{
    errors::{EdgeError, EdgeResult},
    namespace_cache::{NamespaceCache, ResolvedNamespace},
    request_id::{
        RequestId, RequestIdGenerator, UuidRequestIdGenerator, extract_or_generate,
    },
};

/// High-level action names used for authorization.
///
/// The edge should authorize the *kind* of user intent, not low-level internal
/// implementation details.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    StartWorkflowExecution,
    SignalWorkflowExecution,
    PollWorkflowTaskQueue,
    RespondWorkflowTaskCompleted,
    RespondQueryTaskCompleted,
    DescribeWorkflowExecution,
    ListWorkflowExecutions,
    CountWorkflowExecutions,
    PollActivityTaskQueue,
    RespondActivityTaskCompleted,
    RespondActivityTaskFailed,
    RecordActivityTaskHeartbeat,
    TerminateWorkflowExecution,
    RequestCancelWorkflowExecution,
    QueryWorkflow,
    UpdateWorkflowExecution,
    DescribeNamespace,
    ListNamespaces,
    RegisterNamespace,
    GetWorkflowExecutionHistoryReverse,
    DescribeTaskQueue,
    DeleteWorkflowExecution,
    ResetWorkflowExecution,
    SignalWithStartWorkflowExecution,
    GetClusterInfo,
    GetSystemInfo,
    OperatorRead,
    OperatorWrite,
    HealthRead,
}

impl Action {
    pub fn as_str(&self) -> &'static str {
        match self {
            Action::StartWorkflowExecution => "start_workflow_execution",
            Action::SignalWorkflowExecution => "signal_workflow_execution",
            Action::PollWorkflowTaskQueue => "poll_workflow_task_queue",
            Action::RespondWorkflowTaskCompleted => "respond_workflow_task_completed",
            Action::RespondQueryTaskCompleted => "respond_query_task_completed",
            Action::DescribeWorkflowExecution => "describe_workflow_execution",
            Action::ListWorkflowExecutions => "list_workflow_executions",
            Action::CountWorkflowExecutions => "count_workflow_executions",
            Action::PollActivityTaskQueue => "poll_activity_task_queue",
            Action::RespondActivityTaskCompleted => "respond_activity_task_completed",
            Action::RespondActivityTaskFailed => "respond_activity_task_failed",
            Action::RecordActivityTaskHeartbeat => "record_activity_task_heartbeat",
            Action::TerminateWorkflowExecution => "terminate_workflow_execution",
            Action::RequestCancelWorkflowExecution => "request_cancel_workflow_execution",
            Action::QueryWorkflow => "query_workflow",
            Action::UpdateWorkflowExecution => "update_workflow_execution",
            Action::DescribeNamespace => "describe_namespace",
            Action::ListNamespaces => "list_namespaces",
            Action::RegisterNamespace => "register_namespace",
            Action::GetWorkflowExecutionHistoryReverse => {
                "get_workflow_execution_history_reverse"
            }
            Action::DescribeTaskQueue => "describe_task_queue",
            Action::DeleteWorkflowExecution => "delete_workflow_execution",
            Action::ResetWorkflowExecution => "reset_workflow_execution",
            Action::SignalWithStartWorkflowExecution => {
                "signal_with_start_workflow_execution"
            }
            Action::GetClusterInfo => "get_cluster_info",
            Action::GetSystemInfo => "get_system_info",
            Action::OperatorRead => "operator_read",
            Action::OperatorWrite => "operator_write",
            Action::HealthRead => "health_read",
        }
    }
}

/// Authenticated caller metadata.
///
/// This stays deliberately compact. The edge mostly needs a stable subject and
/// optional scopes/roles; richer identity objects belong in a dedicated auth crate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    pub subject: String,
    pub scopes: Vec<String>,
}

impl Principal {
    pub fn root() -> Self {
        Self {
            subject: "root".to_string(),
            scopes: vec!["*".to_string()],
        }
    }
}

#[async_trait]
pub trait Authenticator: Send + Sync + 'static {
    async fn authenticate(&self, headers: &HeaderMap) -> EdgeResult<Principal>;

    async fn authorize(
        &self,
        principal: &Principal,
        action: Action,
        namespace: Option<&ResolvedNamespace>,
    ) -> EdgeResult<()>;
}

/// Useful development default.
///
/// The code makes this explicit rather than hiding it behind `Option<Authenticator>`
/// so deployments cannot accidentally drift into an unauthenticated mode.
#[derive(Debug, Default)]
pub struct AllowAllAuthenticator;

#[async_trait]
impl Authenticator for AllowAllAuthenticator {
    async fn authenticate(&self, _headers: &HeaderMap) -> EdgeResult<Principal> {
        Ok(Principal::root())
    }

    async fn authorize(
        &self,
        _principal: &Principal,
        _action: Action,
        _namespace: Option<&ResolvedNamespace>,
    ) -> EdgeResult<()> {
        Ok(())
    }
}

/// Request-scoped context produced by the edge.
///
/// This context is intentionally the thing that all service methods share:
/// request id, principal, namespace metadata, and arrival timestamp.
#[derive(Clone, Debug)]
pub struct EdgeContext {
    pub request_id: RequestId,
    pub principal: Principal,
    pub namespace: Option<ResolvedNamespace>,
    pub received_at: OffsetDateTime,
    pub is_long_poll: bool,
}

#[derive(Clone)]
pub struct EdgeInterceptors {
    request_ids: Arc<dyn RequestIdGenerator>,
    authenticator: Arc<dyn Authenticator>,
    namespaces: Arc<dyn NamespaceCache>,
}

impl std::fmt::Debug for EdgeInterceptors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeInterceptors").finish_non_exhaustive()
    }
}

impl EdgeInterceptors {
    pub fn new(
        request_ids: Arc<dyn RequestIdGenerator>,
        authenticator: Arc<dyn Authenticator>,
        namespaces: Arc<dyn NamespaceCache>,
    ) -> Self {
        Self {
            request_ids,
            authenticator,
            namespaces,
        }
    }

    pub fn permissive(namespaces: Arc<dyn NamespaceCache>) -> Self {
        Self {
            request_ids: Arc::new(UuidRequestIdGenerator),
            authenticator: Arc::new(AllowAllAuthenticator),
            namespaces,
        }
    }

    /// Perform the standard edge pipeline:
    ///
    /// 1. assign or recover request id,
    /// 2. authenticate the caller,
    /// 3. resolve namespace metadata when a namespace is present,
    /// 4. authorize the action against that namespace.
    ///
    /// Doing this consistently up front keeps individual service handlers small.
    pub async fn begin(
        &self,
        headers: &HeaderMap,
        namespace_name: Option<&str>,
        action: Action,
        is_long_poll: bool,
    ) -> EdgeResult<EdgeContext> {
        let request_id = extract_or_generate(headers, self.request_ids.as_ref());
        let principal = self.authenticator.authenticate(headers).await?;

        let namespace = match namespace_name {
            Some(name) => {
                let Some(ns) =
                    self.namespaces.get(name).await.map_err(EdgeError::from)?
                else {
                    return Err(EdgeError::NamespaceNotFound(name.to_string()));
                };

                if ns.deleted {
                    return Err(EdgeError::NamespaceDeleted(name.to_string()));
                }

                Some(ns)
            }
            None => None,
        };

        self.authenticator
            .authorize(&principal, action, namespace.as_ref())
            .await?;

        Ok(EdgeContext {
            request_id,
            principal,
            namespace,
            received_at: OffsetDateTime::now_utc(),
            is_long_poll,
        })
    }
}
