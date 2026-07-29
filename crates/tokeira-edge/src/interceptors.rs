//! The edge's authentication / authorization / observability seam.
//!
//! Every public RPC begins here: a domain-service method calls
//! [`EdgeInterceptors::begin`] with the request headers, the target namespace (or
//! `None` for cluster-global resources), the [`Action`] it intends, and whether it
//! is a long poll. `begin` runs the configured interceptor chain (authn → authz),
//! resolves the namespace, threads a request id, and returns an [`EdgeContext`] the
//! handler holds for the duration of the call. A handler that does not call `begin`
//! is unauthorized by construction — this is the single chokepoint, so the
//! authorization decision is made once, consistently, for the *kind* of user intent
//! rather than per low-level code path.
//!
//! [`Action`] names that intent at the API level (e.g. `StartWorkflowExecution`,
//! `OperatorWrite`), deliberately decoupled from internal implementation detail so
//! an authorizer reasons about what the caller is trying to do. The default
//! interceptor is permissive (a deployment supplies a real authorizer); the
//! load-bearing property is that the chain is *invoked*, so wiring a real policy
//! gates every routed RPC without touching handlers.

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use http::HeaderMap;
use time::OffsetDateTime;
use tokeira_auth::{
    Access, AuthPrincipal, Authorizer, AuthzDecision, CallClassification, CallTarget, ClaimMapper,
    Claims, Scope, WorkerCallTarget, WorkerOperation, WorkerScopeDenyReason, WorkerTarget,
};
use tokeira_types::{EventPrincipal, NamespaceId, WorkerTaskClass};

use crate::{
    errors::{EdgeError, EdgeResult},
    metrics::{
        record_authorization_denied, record_authorization_duration, record_authorization_error,
        record_scoped_worker_authorization_denied,
    },
    namespace_cache::{NamespaceCache, ResolvedNamespace},
    request_id::{RequestId, RequestIdGenerator, UuidRequestIdGenerator, extract_or_generate},
};

/// High-level action names used for authorization.
///
/// The edge should authorize the *kind* of user intent, not low-level internal
/// implementation details.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    GetWorkflowExecutionHistory,
    StartWorkflowExecution,
    SignalWorkflowExecution,
    PollWorkflowTaskQueue,
    RespondWorkflowTaskCompleted,
    RespondWorkflowTaskFailed,
    RespondQueryTaskCompleted,
    DescribeWorkflowExecution,
    ListWorkflowExecutions,
    CountWorkflowExecutions,
    ListActivityExecutions,
    CountActivityExecutions,
    PollActivityTaskQueue,
    RespondActivityTaskCompleted,
    RespondActivityTaskFailed,
    RespondActivityTaskCanceled,
    RespondActivityTaskCompletedById,
    RespondActivityTaskFailedById,
    RespondActivityTaskCanceledById,
    UpdateActivityOptions,
    PauseActivity,
    UnpauseActivity,
    ResetActivity,
    WorkflowRulesRead,
    WorkflowRulesWrite,
    RecordWorkerHeartbeat,
    ShutdownWorker,
    DescribeWorker,
    ListWorkers,
    PollNexusTaskQueue,
    RespondNexusTaskCompleted,
    RespondNexusTaskFailed,
    RecordActivityTaskHeartbeat,
    RecordActivityTaskHeartbeatById,
    TerminateWorkflowExecution,
    RequestCancelWorkflowExecution,
    PauseWorkflowExecution,
    UnpauseWorkflowExecution,
    QueryWorkflow,
    UpdateWorkflowExecution,
    PollWorkflowExecutionUpdate,
    UpdateWorkflowExecutionOptions,
    DescribeNamespace,
    ListNamespaces,
    RegisterNamespace,
    UpdateNamespace,
    GetWorkflowExecutionHistoryReverse,
    DescribeTaskQueue,
    ListTaskQueuePartitions,
    DeleteWorkflowExecution,
    ResetWorkflowExecution,
    ResetStickyTaskQueue,
    StartBatchOperation,
    StopBatchOperation,
    DescribeBatchOperation,
    ListBatchOperations,
    SignalWithStartWorkflowExecution,
    GetClusterInfo,
    GetSystemInfo,
    GetSearchAttributes,
    AddSearchAttributes,
    RemoveSearchAttributes,
    ListSearchAttributes,
    DeleteNamespace,
    AddOrUpdateRemoteCluster,
    RemoveRemoteCluster,
    ListClusters,
    CreateNexusEndpoint,
    UpdateNexusEndpoint,
    DeleteNexusEndpoint,
    GetNexusEndpoint,
    ListNexusEndpoints,
    StartActivityExecution,
    DescribeActivityExecution,
    PollActivityExecution,
    RequestCancelActivityExecution,
    TerminateActivityExecution,
    DeleteActivityExecution,
    DescribeMutableState,
    HealthRead,
}

impl Action {
    /// Complete authorization-action catalog used by fixed-surface audits.
    pub const ALL: &'static [Self] = &[
        Self::GetWorkflowExecutionHistory,
        Self::StartWorkflowExecution,
        Self::SignalWorkflowExecution,
        Self::PollWorkflowTaskQueue,
        Self::RespondWorkflowTaskCompleted,
        Self::RespondWorkflowTaskFailed,
        Self::RespondQueryTaskCompleted,
        Self::DescribeWorkflowExecution,
        Self::ListWorkflowExecutions,
        Self::CountWorkflowExecutions,
        Self::ListActivityExecutions,
        Self::CountActivityExecutions,
        Self::PollActivityTaskQueue,
        Self::RespondActivityTaskCompleted,
        Self::RespondActivityTaskFailed,
        Self::RespondActivityTaskCanceled,
        Self::RespondActivityTaskCompletedById,
        Self::RespondActivityTaskFailedById,
        Self::RespondActivityTaskCanceledById,
        Self::UpdateActivityOptions,
        Self::PauseActivity,
        Self::UnpauseActivity,
        Self::ResetActivity,
        Self::WorkflowRulesRead,
        Self::WorkflowRulesWrite,
        Self::RecordWorkerHeartbeat,
        Self::ShutdownWorker,
        Self::DescribeWorker,
        Self::ListWorkers,
        Self::PollNexusTaskQueue,
        Self::RespondNexusTaskCompleted,
        Self::RespondNexusTaskFailed,
        Self::RecordActivityTaskHeartbeat,
        Self::RecordActivityTaskHeartbeatById,
        Self::TerminateWorkflowExecution,
        Self::RequestCancelWorkflowExecution,
        Self::PauseWorkflowExecution,
        Self::UnpauseWorkflowExecution,
        Self::QueryWorkflow,
        Self::UpdateWorkflowExecution,
        Self::PollWorkflowExecutionUpdate,
        Self::UpdateWorkflowExecutionOptions,
        Self::DescribeNamespace,
        Self::ListNamespaces,
        Self::RegisterNamespace,
        Self::UpdateNamespace,
        Self::GetWorkflowExecutionHistoryReverse,
        Self::DescribeTaskQueue,
        Self::ListTaskQueuePartitions,
        Self::DeleteWorkflowExecution,
        Self::ResetWorkflowExecution,
        Self::ResetStickyTaskQueue,
        Self::StartBatchOperation,
        Self::StopBatchOperation,
        Self::DescribeBatchOperation,
        Self::ListBatchOperations,
        Self::SignalWithStartWorkflowExecution,
        Self::GetClusterInfo,
        Self::GetSystemInfo,
        Self::GetSearchAttributes,
        Self::AddSearchAttributes,
        Self::RemoveSearchAttributes,
        Self::ListSearchAttributes,
        Self::DeleteNamespace,
        Self::AddOrUpdateRemoteCluster,
        Self::RemoveRemoteCluster,
        Self::ListClusters,
        Self::CreateNexusEndpoint,
        Self::UpdateNexusEndpoint,
        Self::DeleteNexusEndpoint,
        Self::GetNexusEndpoint,
        Self::ListNexusEndpoints,
        Self::StartActivityExecution,
        Self::DescribeActivityExecution,
        Self::PollActivityExecution,
        Self::RequestCancelActivityExecution,
        Self::TerminateActivityExecution,
        Self::DeleteActivityExecution,
        Self::DescribeMutableState,
        Self::HealthRead,
    ];

    /// Stable metric label for the public API intent.
    pub fn as_str(&self) -> &'static str {
        match self {
            Action::GetWorkflowExecutionHistory => "get_workflow_execution_history",
            Action::StartWorkflowExecution => "start_workflow_execution",
            Action::SignalWorkflowExecution => "signal_workflow_execution",
            Action::PollWorkflowTaskQueue => "poll_workflow_task_queue",
            Action::RespondWorkflowTaskCompleted => "respond_workflow_task_completed",
            Action::RespondWorkflowTaskFailed => "respond_workflow_task_failed",
            Action::RespondQueryTaskCompleted => "respond_query_task_completed",
            Action::DescribeWorkflowExecution => "describe_workflow_execution",
            Action::ListWorkflowExecutions => "list_workflow_executions",
            Action::CountWorkflowExecutions => "count_workflow_executions",
            Action::ListActivityExecutions => "list_activity_executions",
            Action::CountActivityExecutions => "count_activity_executions",
            Action::PollActivityTaskQueue => "poll_activity_task_queue",
            Action::RespondActivityTaskCompleted => "respond_activity_task_completed",
            Action::RespondActivityTaskFailed => "respond_activity_task_failed",
            Action::RespondActivityTaskCanceled => "respond_activity_task_canceled",
            Action::RespondActivityTaskCompletedById => "respond_activity_task_completed_by_id",
            Action::RespondActivityTaskFailedById => "respond_activity_task_failed_by_id",
            Action::RespondActivityTaskCanceledById => "respond_activity_task_canceled_by_id",
            Action::UpdateActivityOptions => "update_activity_options",
            Action::PauseActivity => "pause_activity",
            Action::UnpauseActivity => "unpause_activity",
            Action::ResetActivity => "reset_activity",
            Action::WorkflowRulesRead => "workflow_rules_read",
            Action::WorkflowRulesWrite => "workflow_rules_write",
            Action::RecordWorkerHeartbeat => "record_worker_heartbeat",
            Action::ShutdownWorker => "shutdown_worker",
            Action::DescribeWorker => "describe_worker",
            Action::ListWorkers => "list_workers",
            Action::PollNexusTaskQueue => "poll_nexus_task_queue",
            Action::RespondNexusTaskCompleted => "respond_nexus_task_completed",
            Action::RespondNexusTaskFailed => "respond_nexus_task_failed",
            Action::RecordActivityTaskHeartbeat => "record_activity_task_heartbeat",
            Action::RecordActivityTaskHeartbeatById => "record_activity_task_heartbeat_by_id",
            Action::TerminateWorkflowExecution => "terminate_workflow_execution",
            Action::RequestCancelWorkflowExecution => "request_cancel_workflow_execution",
            Action::PauseWorkflowExecution => "pause_workflow_execution",
            Action::UnpauseWorkflowExecution => "unpause_workflow_execution",
            Action::QueryWorkflow => "query_workflow",
            Action::UpdateWorkflowExecution => "update_workflow_execution",
            Action::PollWorkflowExecutionUpdate => "poll_workflow_execution_update",
            Action::UpdateWorkflowExecutionOptions => "update_workflow_execution_options",
            Action::DescribeNamespace => "describe_namespace",
            Action::ListNamespaces => "list_namespaces",
            Action::RegisterNamespace => "register_namespace",
            Action::UpdateNamespace => "update_namespace",
            Action::GetWorkflowExecutionHistoryReverse => "get_workflow_execution_history_reverse",
            Action::DescribeTaskQueue => "describe_task_queue",
            Action::ListTaskQueuePartitions => "list_task_queue_partitions",
            Action::DeleteWorkflowExecution => "delete_workflow_execution",
            Action::ResetWorkflowExecution => "reset_workflow_execution",
            Action::ResetStickyTaskQueue => "reset_sticky_task_queue",
            Action::StartBatchOperation => "start_batch_operation",
            Action::StopBatchOperation => "stop_batch_operation",
            Action::DescribeBatchOperation => "describe_batch_operation",
            Action::ListBatchOperations => "list_batch_operations",
            Action::SignalWithStartWorkflowExecution => "signal_with_start_workflow_execution",
            Action::GetClusterInfo => "get_cluster_info",
            Action::GetSystemInfo => "get_system_info",
            Action::GetSearchAttributes => "get_search_attributes",
            Action::AddSearchAttributes => "add_search_attributes",
            Action::RemoveSearchAttributes => "remove_search_attributes",
            Action::ListSearchAttributes => "list_search_attributes",
            Action::DeleteNamespace => "delete_namespace",
            Action::AddOrUpdateRemoteCluster => "add_or_update_remote_cluster",
            Action::RemoveRemoteCluster => "remove_remote_cluster",
            Action::ListClusters => "list_clusters",
            Action::CreateNexusEndpoint => "create_nexus_endpoint",
            Action::UpdateNexusEndpoint => "update_nexus_endpoint",
            Action::DeleteNexusEndpoint => "delete_nexus_endpoint",
            Action::GetNexusEndpoint => "get_nexus_endpoint",
            Action::ListNexusEndpoints => "list_nexus_endpoints",
            Action::StartActivityExecution => "start_activity_execution",
            Action::DescribeActivityExecution => "describe_activity_execution",
            Action::PollActivityExecution => "poll_activity_execution",
            Action::RequestCancelActivityExecution => "request_cancel_activity_execution",
            Action::TerminateActivityExecution => "terminate_activity_execution",
            Action::DeleteActivityExecution => "delete_activity_execution",
            Action::DescribeMutableState => "describe_mutable_state",
            Action::HealthRead => "health_read",
        }
    }

    /// Exact public method name used by Temporal's authorization contract.
    pub fn api_name(self) -> &'static str {
        let method = match self {
            Action::HealthRead => return "/grpc.health.v1.Health/Check",
            Action::AddSearchAttributes => "AddSearchAttributes",
            Action::RemoveSearchAttributes => "RemoveSearchAttributes",
            Action::ListSearchAttributes => "ListSearchAttributes",
            Action::DeleteNamespace => "DeleteNamespace",
            Action::AddOrUpdateRemoteCluster => "AddOrUpdateRemoteCluster",
            Action::RemoveRemoteCluster => "RemoveRemoteCluster",
            Action::ListClusters => "ListClusters",
            Action::CreateNexusEndpoint => "CreateNexusEndpoint",
            Action::UpdateNexusEndpoint => "UpdateNexusEndpoint",
            Action::DeleteNexusEndpoint => "DeleteNexusEndpoint",
            Action::GetNexusEndpoint => "GetNexusEndpoint",
            Action::ListNexusEndpoints => "ListNexusEndpoints",
            Action::DescribeMutableState => {
                return "/temporal.server.api.adminservice.v1.AdminService/DescribeMutableState";
            }
            Action::GetWorkflowExecutionHistory => "GetWorkflowExecutionHistory",
            Action::StartWorkflowExecution => "StartWorkflowExecution",
            Action::SignalWorkflowExecution => "SignalWorkflowExecution",
            Action::PollWorkflowTaskQueue => "PollWorkflowTaskQueue",
            Action::RespondWorkflowTaskCompleted => "RespondWorkflowTaskCompleted",
            Action::RespondWorkflowTaskFailed => "RespondWorkflowTaskFailed",
            Action::RespondQueryTaskCompleted => "RespondQueryTaskCompleted",
            Action::DescribeWorkflowExecution => "DescribeWorkflowExecution",
            Action::ListWorkflowExecutions => "ListWorkflowExecutions",
            Action::CountWorkflowExecutions => "CountWorkflowExecutions",
            Action::ListActivityExecutions => "ListActivityExecutions",
            Action::CountActivityExecutions => "CountActivityExecutions",
            Action::PollActivityTaskQueue => "PollActivityTaskQueue",
            Action::RespondActivityTaskCompleted => "RespondActivityTaskCompleted",
            Action::RespondActivityTaskFailed => "RespondActivityTaskFailed",
            Action::RespondActivityTaskCanceled => "RespondActivityTaskCanceled",
            Action::RespondActivityTaskCompletedById => "RespondActivityTaskCompletedById",
            Action::RespondActivityTaskFailedById => "RespondActivityTaskFailedById",
            Action::RespondActivityTaskCanceledById => "RespondActivityTaskCanceledById",
            Action::UpdateActivityOptions => "UpdateActivityOptions",
            Action::PauseActivity => "PauseActivity",
            Action::UnpauseActivity => "UnpauseActivity",
            Action::ResetActivity => "ResetActivity",
            Action::WorkflowRulesRead => "DescribeWorkflowRule",
            Action::WorkflowRulesWrite => "CreateWorkflowRule",
            Action::RecordWorkerHeartbeat => "RecordWorkerHeartbeat",
            Action::ShutdownWorker => "ShutdownWorker",
            Action::DescribeWorker => "DescribeWorker",
            Action::ListWorkers => "ListWorkers",
            Action::PollNexusTaskQueue => "PollNexusTaskQueue",
            Action::RespondNexusTaskCompleted => "RespondNexusTaskCompleted",
            Action::RespondNexusTaskFailed => "RespondNexusTaskFailed",
            Action::RecordActivityTaskHeartbeat => "RecordActivityTaskHeartbeat",
            Action::RecordActivityTaskHeartbeatById => "RecordActivityTaskHeartbeatById",
            Action::TerminateWorkflowExecution => "TerminateWorkflowExecution",
            Action::RequestCancelWorkflowExecution => "RequestCancelWorkflowExecution",
            Action::PauseWorkflowExecution => "PauseWorkflowExecution",
            Action::UnpauseWorkflowExecution => "UnpauseWorkflowExecution",
            Action::QueryWorkflow => "QueryWorkflow",
            Action::UpdateWorkflowExecution => "UpdateWorkflowExecution",
            Action::PollWorkflowExecutionUpdate => "PollWorkflowExecutionUpdate",
            Action::UpdateWorkflowExecutionOptions => "UpdateWorkflowExecutionOptions",
            Action::DescribeNamespace => "DescribeNamespace",
            Action::ListNamespaces => "ListNamespaces",
            Action::RegisterNamespace => "RegisterNamespace",
            Action::UpdateNamespace => "UpdateNamespace",
            Action::GetWorkflowExecutionHistoryReverse => "GetWorkflowExecutionHistoryReverse",
            Action::DescribeTaskQueue => "DescribeTaskQueue",
            Action::ListTaskQueuePartitions => "ListTaskQueuePartitions",
            Action::DeleteWorkflowExecution => "DeleteWorkflowExecution",
            Action::ResetWorkflowExecution => "ResetWorkflowExecution",
            Action::ResetStickyTaskQueue => "ResetStickyTaskQueue",
            Action::StartBatchOperation => "StartBatchOperation",
            Action::StopBatchOperation => "StopBatchOperation",
            Action::DescribeBatchOperation => "DescribeBatchOperation",
            Action::ListBatchOperations => "ListBatchOperations",
            Action::SignalWithStartWorkflowExecution => "SignalWithStartWorkflowExecution",
            Action::GetClusterInfo => "GetClusterInfo",
            Action::GetSystemInfo => "GetSystemInfo",
            Action::GetSearchAttributes => "GetSearchAttributes",
            Action::StartActivityExecution => "StartActivityExecution",
            Action::DescribeActivityExecution => "DescribeActivityExecution",
            Action::PollActivityExecution => "PollActivityExecution",
            Action::RequestCancelActivityExecution => "RequestCancelActivityExecution",
            Action::TerminateActivityExecution => "TerminateActivityExecution",
            Action::DeleteActivityExecution => "DeleteActivityExecution",
        };
        let service = if matches!(
            self,
            Action::AddSearchAttributes
                | Action::RemoveSearchAttributes
                | Action::ListSearchAttributes
                | Action::DeleteNamespace
                | Action::AddOrUpdateRemoteCluster
                | Action::RemoveRemoteCluster
                | Action::ListClusters
                | Action::CreateNexusEndpoint
                | Action::UpdateNexusEndpoint
                | Action::DeleteNexusEndpoint
                | Action::GetNexusEndpoint
                | Action::ListNexusEndpoints
        ) {
            "operatorservice"
        } else {
            "workflowservice"
        };
        match service {
            "operatorservice" => match method {
                "AddSearchAttributes" => {
                    "/temporal.api.operatorservice.v1.OperatorService/AddSearchAttributes"
                }
                "RemoveSearchAttributes" => {
                    "/temporal.api.operatorservice.v1.OperatorService/RemoveSearchAttributes"
                }
                "ListSearchAttributes" => {
                    "/temporal.api.operatorservice.v1.OperatorService/ListSearchAttributes"
                }
                "DeleteNamespace" => {
                    "/temporal.api.operatorservice.v1.OperatorService/DeleteNamespace"
                }
                "AddOrUpdateRemoteCluster" => {
                    "/temporal.api.operatorservice.v1.OperatorService/AddOrUpdateRemoteCluster"
                }
                "RemoveRemoteCluster" => {
                    "/temporal.api.operatorservice.v1.OperatorService/RemoveRemoteCluster"
                }
                "ListClusters" => "/temporal.api.operatorservice.v1.OperatorService/ListClusters",
                "CreateNexusEndpoint" => {
                    "/temporal.api.operatorservice.v1.OperatorService/CreateNexusEndpoint"
                }
                "UpdateNexusEndpoint" => {
                    "/temporal.api.operatorservice.v1.OperatorService/UpdateNexusEndpoint"
                }
                "DeleteNexusEndpoint" => {
                    "/temporal.api.operatorservice.v1.OperatorService/DeleteNexusEndpoint"
                }
                "GetNexusEndpoint" => {
                    "/temporal.api.operatorservice.v1.OperatorService/GetNexusEndpoint"
                }
                "ListNexusEndpoints" => {
                    "/temporal.api.operatorservice.v1.OperatorService/ListNexusEndpoints"
                }
                _ => unreachable!("operator action must have an operator method"),
            },
            _ => match method {
                "GetWorkflowExecutionHistory" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory"
                }
                "StartWorkflowExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"
                }
                "SignalWorkflowExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/SignalWorkflowExecution"
                }
                "PollWorkflowTaskQueue" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue"
                }
                "RespondWorkflowTaskCompleted" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted"
                }
                "RespondWorkflowTaskFailed" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskFailed"
                }
                "RespondQueryTaskCompleted" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RespondQueryTaskCompleted"
                }
                "DescribeWorkflowExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/DescribeWorkflowExecution"
                }
                "ListWorkflowExecutions" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowExecutions"
                }
                "CountWorkflowExecutions" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/CountWorkflowExecutions"
                }
                "ListActivityExecutions" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/ListActivityExecutions"
                }
                "CountActivityExecutions" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/CountActivityExecutions"
                }
                "PollActivityTaskQueue" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/PollActivityTaskQueue"
                }
                "RespondActivityTaskCompleted" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted"
                }
                "RespondActivityTaskFailed" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskFailed"
                }
                "RespondActivityTaskCanceled" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCanceled"
                }
                "RespondActivityTaskCompletedById" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompletedById"
                }
                "RespondActivityTaskFailedById" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskFailedById"
                }
                "RespondActivityTaskCanceledById" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCanceledById"
                }
                "UpdateActivityOptions" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/UpdateActivityOptions"
                }
                "PauseActivity" => "/temporal.api.workflowservice.v1.WorkflowService/PauseActivity",
                "UnpauseActivity" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/UnpauseActivity"
                }
                "ResetActivity" => "/temporal.api.workflowservice.v1.WorkflowService/ResetActivity",
                "DescribeWorkflowRule" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/DescribeWorkflowRule"
                }
                "CreateWorkflowRule" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/CreateWorkflowRule"
                }
                "RecordWorkerHeartbeat" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RecordWorkerHeartbeat"
                }
                "ShutdownWorker" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/ShutdownWorker"
                }
                "DescribeWorker" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/DescribeWorker"
                }
                "ListWorkers" => "/temporal.api.workflowservice.v1.WorkflowService/ListWorkers",
                "PollNexusTaskQueue" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/PollNexusTaskQueue"
                }
                "RespondNexusTaskCompleted" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RespondNexusTaskCompleted"
                }
                "RespondNexusTaskFailed" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RespondNexusTaskFailed"
                }
                "RecordActivityTaskHeartbeat" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RecordActivityTaskHeartbeat"
                }
                "RecordActivityTaskHeartbeatById" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RecordActivityTaskHeartbeatById"
                }
                "TerminateWorkflowExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/TerminateWorkflowExecution"
                }
                "RequestCancelWorkflowExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RequestCancelWorkflowExecution"
                }
                "PauseWorkflowExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/PauseWorkflowExecution"
                }
                "UnpauseWorkflowExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/UnpauseWorkflowExecution"
                }
                "QueryWorkflow" => "/temporal.api.workflowservice.v1.WorkflowService/QueryWorkflow",
                "UpdateWorkflowExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkflowExecution"
                }
                "PollWorkflowExecutionUpdate" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowExecutionUpdate"
                }
                "UpdateWorkflowExecutionOptions" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkflowExecutionOptions"
                }
                "DescribeNamespace" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace"
                }
                "ListNamespaces" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/ListNamespaces"
                }
                "RegisterNamespace" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RegisterNamespace"
                }
                "UpdateNamespace" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/UpdateNamespace"
                }
                "GetWorkflowExecutionHistoryReverse" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistoryReverse"
                }
                "DescribeTaskQueue" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/DescribeTaskQueue"
                }
                "ListTaskQueuePartitions" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/ListTaskQueuePartitions"
                }
                "DeleteWorkflowExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/DeleteWorkflowExecution"
                }
                "ResetWorkflowExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/ResetWorkflowExecution"
                }
                "ResetStickyTaskQueue" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/ResetStickyTaskQueue"
                }
                "StartBatchOperation" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/StartBatchOperation"
                }
                "StopBatchOperation" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/StopBatchOperation"
                }
                "DescribeBatchOperation" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/DescribeBatchOperation"
                }
                "ListBatchOperations" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/ListBatchOperations"
                }
                "SignalWithStartWorkflowExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/SignalWithStartWorkflowExecution"
                }
                "GetClusterInfo" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/GetClusterInfo"
                }
                "GetSystemInfo" => "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo",
                "GetSearchAttributes" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/GetSearchAttributes"
                }
                "StartActivityExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/StartActivityExecution"
                }
                "DescribeActivityExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/DescribeActivityExecution"
                }
                "PollActivityExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/PollActivityExecution"
                }
                "RequestCancelActivityExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/RequestCancelActivityExecution"
                }
                "TerminateActivityExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/TerminateActivityExecution"
                }
                "DeleteActivityExecution" => {
                    "/temporal.api.workflowservice.v1.WorkflowService/DeleteActivityExecution"
                }
                _ => unreachable!("workflow action must have a workflow method"),
            },
        }
    }

    /// Authorization metadata verified against `common/api/metadata.go @ v1.31.0`.
    pub const fn classification(self) -> CallClassification {
        let (scope, access) = match self {
            Action::HealthRead
            | Action::GetClusterInfo
            | Action::GetSystemInfo
            | Action::GetSearchAttributes => (Scope::Cluster, Access::ReadOnly),
            Action::ListNamespaces => (Scope::Cluster, Access::ReadOnly),
            Action::AddOrUpdateRemoteCluster
            | Action::RemoveRemoteCluster
            | Action::ListClusters
            | Action::CreateNexusEndpoint
            | Action::UpdateNexusEndpoint
            | Action::DeleteNexusEndpoint
            | Action::GetNexusEndpoint
            | Action::ListNexusEndpoints
            | Action::DescribeMutableState => (Scope::Cluster, Access::Admin),
            Action::DescribeWorkflowExecution
            | Action::GetWorkflowExecutionHistory
            | Action::GetWorkflowExecutionHistoryReverse
            | Action::ListWorkflowExecutions
            | Action::CountWorkflowExecutions
            | Action::ListActivityExecutions
            | Action::CountActivityExecutions
            | Action::QueryWorkflow
            | Action::PollWorkflowExecutionUpdate
            | Action::DescribeNamespace
            | Action::DescribeTaskQueue
            | Action::ListTaskQueuePartitions
            | Action::DescribeBatchOperation
            | Action::ListBatchOperations
            | Action::WorkflowRulesRead
            | Action::ListSearchAttributes
            | Action::DescribeWorker
            | Action::ListWorkers
            | Action::DescribeActivityExecution
            | Action::PollActivityExecution => (Scope::Namespace, Access::ReadOnly),
            Action::RegisterNamespace
            | Action::UpdateNamespace
            | Action::AddSearchAttributes
            | Action::RemoveSearchAttributes
            | Action::DeleteNamespace => (Scope::Namespace, Access::Admin),
            Action::StartWorkflowExecution
            | Action::SignalWorkflowExecution
            | Action::PollWorkflowTaskQueue
            | Action::RespondWorkflowTaskCompleted
            | Action::RespondWorkflowTaskFailed
            | Action::RespondQueryTaskCompleted
            | Action::PollActivityTaskQueue
            | Action::RespondActivityTaskCompleted
            | Action::RespondActivityTaskFailed
            | Action::RespondActivityTaskCanceled
            | Action::RespondActivityTaskCompletedById
            | Action::RespondActivityTaskFailedById
            | Action::RespondActivityTaskCanceledById
            | Action::UpdateActivityOptions
            | Action::PauseActivity
            | Action::UnpauseActivity
            | Action::ResetActivity
            | Action::WorkflowRulesWrite
            | Action::RecordWorkerHeartbeat
            | Action::ShutdownWorker
            | Action::PollNexusTaskQueue
            | Action::RespondNexusTaskCompleted
            | Action::RespondNexusTaskFailed
            | Action::RecordActivityTaskHeartbeat
            | Action::RecordActivityTaskHeartbeatById
            | Action::TerminateWorkflowExecution
            | Action::RequestCancelWorkflowExecution
            | Action::PauseWorkflowExecution
            | Action::UnpauseWorkflowExecution
            | Action::UpdateWorkflowExecution
            | Action::UpdateWorkflowExecutionOptions
            | Action::DeleteWorkflowExecution
            | Action::ResetWorkflowExecution
            | Action::ResetStickyTaskQueue
            | Action::StartBatchOperation
            | Action::StopBatchOperation
            | Action::SignalWithStartWorkflowExecution
            | Action::StartActivityExecution
            | Action::RequestCancelActivityExecution
            | Action::TerminateActivityExecution
            | Action::DeleteActivityExecution => (Scope::Namespace, Access::Write),
        };
        CallClassification { scope, access }
    }

    /// Fixed scoped-Worker operation for this API, when one exists.
    ///
    /// Activity By-ID aliases are deliberately absent: v1.31.0 exposes them
    /// as separate public methods, but they carry no server-authored task
    /// token and therefore cannot satisfy exact task-origin authorization.
    pub const fn worker_operation(self) -> Option<WorkerOperation> {
        match self {
            Self::PollWorkflowTaskQueue => Some(WorkerOperation::PollWorkflowTaskQueue),
            Self::RespondWorkflowTaskCompleted => {
                Some(WorkerOperation::RespondWorkflowTaskCompleted)
            }
            Self::RespondWorkflowTaskFailed => Some(WorkerOperation::RespondWorkflowTaskFailed),
            Self::RespondQueryTaskCompleted => Some(WorkerOperation::RespondQueryTaskCompleted),
            Self::PollActivityTaskQueue => Some(WorkerOperation::PollActivityTaskQueue),
            Self::RespondActivityTaskCompleted => {
                Some(WorkerOperation::RespondActivityTaskCompleted)
            }
            Self::RespondActivityTaskFailed => Some(WorkerOperation::RespondActivityTaskFailed),
            Self::RespondActivityTaskCanceled => Some(WorkerOperation::RespondActivityTaskCanceled),
            Self::PollNexusTaskQueue => Some(WorkerOperation::PollNexusTaskQueue),
            Self::RespondNexusTaskCompleted => Some(WorkerOperation::RespondNexusTaskCompleted),
            Self::RespondNexusTaskFailed => Some(WorkerOperation::RespondNexusTaskFailed),
            Self::RecordActivityTaskHeartbeat => Some(WorkerOperation::RecordActivityTaskHeartbeat),
            Self::RecordWorkerHeartbeat => Some(WorkerOperation::RecordWorkerHeartbeat),
            Self::ShutdownWorker => Some(WorkerOperation::ShutdownWorker),
            Self::DescribeTaskQueue => Some(WorkerOperation::DescribeTaskQueue),
            Self::GetWorkflowExecutionHistory
            | Self::StartWorkflowExecution
            | Self::SignalWorkflowExecution
            | Self::DescribeWorkflowExecution
            | Self::ListWorkflowExecutions
            | Self::CountWorkflowExecutions
            | Self::ListActivityExecutions
            | Self::CountActivityExecutions
            | Self::RespondActivityTaskCompletedById
            | Self::RespondActivityTaskFailedById
            | Self::RespondActivityTaskCanceledById
            | Self::UpdateActivityOptions
            | Self::PauseActivity
            | Self::UnpauseActivity
            | Self::ResetActivity
            | Self::WorkflowRulesRead
            | Self::WorkflowRulesWrite
            | Self::DescribeWorker
            | Self::ListWorkers
            | Self::RecordActivityTaskHeartbeatById
            | Self::TerminateWorkflowExecution
            | Self::RequestCancelWorkflowExecution
            | Self::PauseWorkflowExecution
            | Self::UnpauseWorkflowExecution
            | Self::QueryWorkflow
            | Self::UpdateWorkflowExecution
            | Self::PollWorkflowExecutionUpdate
            | Self::UpdateWorkflowExecutionOptions
            | Self::DescribeNamespace
            | Self::ListNamespaces
            | Self::RegisterNamespace
            | Self::UpdateNamespace
            | Self::GetWorkflowExecutionHistoryReverse
            | Self::ListTaskQueuePartitions
            | Self::DeleteWorkflowExecution
            | Self::ResetWorkflowExecution
            | Self::ResetStickyTaskQueue
            | Self::StartBatchOperation
            | Self::StopBatchOperation
            | Self::DescribeBatchOperation
            | Self::ListBatchOperations
            | Self::SignalWithStartWorkflowExecution
            | Self::GetClusterInfo
            | Self::GetSystemInfo
            | Self::GetSearchAttributes
            | Self::AddSearchAttributes
            | Self::RemoveSearchAttributes
            | Self::ListSearchAttributes
            | Self::DeleteNamespace
            | Self::AddOrUpdateRemoteCluster
            | Self::RemoveRemoteCluster
            | Self::ListClusters
            | Self::CreateNexusEndpoint
            | Self::UpdateNexusEndpoint
            | Self::DeleteNexusEndpoint
            | Self::GetNexusEndpoint
            | Self::ListNexusEndpoints
            | Self::StartActivityExecution
            | Self::DescribeActivityExecution
            | Self::PollActivityExecution
            | Self::RequestCancelActivityExecution
            | Self::TerminateActivityExecution
            | Self::DeleteActivityExecution
            | Self::DescribeMutableState
            | Self::HealthRead => None,
        }
    }
}

/// Edge authentication and authorization pipeline.
#[async_trait]
pub trait Authenticator: Send + Sync + 'static {
    /// Authenticate raw request metadata into role claims.
    async fn authenticate(&self, headers: &HeaderMap) -> EdgeResult<Option<Claims>>;

    /// Decide one API intent against the unresolved namespace name.
    async fn authorize(
        &self,
        claims: Option<&Claims>,
        action: Action,
        namespace_name: Option<&str>,
    ) -> EdgeResult<Option<AuthPrincipal>>;

    /// Decide a fully classified scoped-Worker intent without re-authenticating.
    ///
    /// Custom authenticators that predate scoped Workers retain their ordinary
    /// behavior through this default. The configured policy adapter overrides
    /// it to pass the exact Worker target into the shared authorizer.
    async fn authorize_worker(
        &self,
        claims: Option<&Claims>,
        action: Action,
        namespace_name: Option<&str>,
        _worker: WorkerCallTarget<'_>,
    ) -> EdgeResult<Option<AuthPrincipal>> {
        self.authorize(claims, action, namespace_name).await
    }
}

/// Useful development default.
///
/// The code makes this explicit rather than hiding it behind `Option<Authenticator>`
/// so deployments cannot accidentally drift into an unauthenticated mode.
#[derive(Debug, Default)]
pub struct AllowAllAuthenticator;

#[async_trait]
impl Authenticator for AllowAllAuthenticator {
    async fn authenticate(&self, _headers: &HeaderMap) -> EdgeResult<Option<Claims>> {
        Ok(None)
    }

    async fn authorize(
        &self,
        _claims: Option<&Claims>,
        _action: Action,
        _namespace_name: Option<&str>,
    ) -> EdgeResult<Option<AuthPrincipal>> {
        Ok(None)
    }
}

/// Configured Temporal-compatible claims and role authorizer adapter.
pub struct PolicyAuthenticator {
    claim_mapper: Arc<dyn ClaimMapper>,
    authorizer: Arc<dyn Authorizer>,
    expose_authorizer_errors: bool,
}

impl std::fmt::Debug for PolicyAuthenticator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PolicyAuthenticator")
            .field("expose_authorizer_errors", &self.expose_authorizer_errors)
            .finish_non_exhaustive()
    }
}

impl PolicyAuthenticator {
    /// Construct the configured policy adapter.
    pub fn new(
        claim_mapper: Arc<dyn ClaimMapper>,
        authorizer: Arc<dyn Authorizer>,
        expose_authorizer_errors: bool,
    ) -> Self {
        Self {
            claim_mapper,
            authorizer,
            expose_authorizer_errors,
        }
    }

    fn denied(reason: Option<String>) -> EdgeError {
        EdgeError::PermissionDenied {
            message: "Request unauthorized.".to_owned(),
            reason,
        }
    }

    fn authorize_target(
        &self,
        claims: Option<&Claims>,
        target: CallTarget<'_>,
    ) -> EdgeResult<Option<AuthPrincipal>> {
        match self.authorizer.authorize(claims, &target) {
            Ok(AuthzDecision::Allow { principal }) => Ok(principal),
            Ok(AuthzDecision::Deny { reason }) => {
                record_authorization_denied(target.api_name);
                Err(Self::denied(reason))
            }
            Err(error) => {
                tracing::error!(%error, api = target.api_name, "request authorizer failed");
                record_authorization_error(target.api_name, "authorize");
                if authorizer_errors_exposed(self.expose_authorizer_errors) {
                    Err(EdgeError::PermissionDenied {
                        message: error.to_string(),
                        reason: None,
                    })
                } else {
                    Err(Self::denied(None))
                }
            }
        }
    }
}

pub(crate) fn authorizer_errors_exposed(configured: bool) -> bool {
    #[cfg(feature = "conformance")]
    {
        tokeira_conformance::overrides()
            .get_bool("frontend.exposeAuthorizerErrors")
            .unwrap_or(configured)
    }
    #[cfg(not(feature = "conformance"))]
    {
        configured
    }
}

fn token_namespace_enforcement_enabled() -> bool {
    #[cfg(feature = "conformance")]
    {
        tokeira_conformance::overrides()
            .get_bool("frontend.enableTokenNamespaceEnforcement")
            .unwrap_or(true)
    }
    #[cfg(not(feature = "conformance"))]
    {
        true
    }
}

#[async_trait]
impl Authenticator for PolicyAuthenticator {
    async fn authenticate(&self, headers: &HeaderMap) -> EdgeResult<Option<Claims>> {
        let token = headers
            .get("authorization")
            .map(|value| value.to_str())
            .transpose()
            .map_err(|error| {
                tracing::warn!(%error, "authorization header is not valid text");
                Self::denied(None)
            })?
            .unwrap_or_default();
        let extras = headers
            .get("authorization-extras")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        self.claim_mapper
            .get_claims(token, extras)
            .await
            .map(Some)
            .map_err(|error| {
                // v1.31.0 never exposes claim-mapper diagnostics to callers;
                // they are retained only in server logs (`interceptor.go @ v1.31.0`).
                tracing::warn!(%error, "request authentication failed");
                Self::denied(None)
            })
    }

    async fn authorize(
        &self,
        claims: Option<&Claims>,
        action: Action,
        namespace_name: Option<&str>,
    ) -> EdgeResult<Option<AuthPrincipal>> {
        let target = CallTarget {
            api_name: action.api_name(),
            namespace: namespace_name,
            classification: action.classification(),
            worker: None,
        };
        self.authorize_target(claims, target)
    }

    async fn authorize_worker(
        &self,
        claims: Option<&Claims>,
        action: Action,
        namespace_name: Option<&str>,
        worker: WorkerCallTarget<'_>,
    ) -> EdgeResult<Option<AuthPrincipal>> {
        self.authorize_target(
            claims,
            CallTarget {
                api_name: action.api_name(),
                namespace: namespace_name,
                classification: action.classification(),
                worker: Some(worker),
            },
        )
    }
}

/// Request-scoped context produced by the edge.
///
/// This context is intentionally the thing that all service methods share:
/// request id, authenticated claims/principal, namespace metadata, and arrival timestamp.
#[derive(Clone, Debug)]
pub struct EdgeContext {
    /// Request correlation identifier.
    pub request_id: RequestId,
    /// Authenticated claims, absent under the stock permissive short-circuit.
    pub claims: Option<Claims>,
    /// Server-computed principal eligible for history attribution.
    pub auth_principal: Option<AuthPrincipal>,
    /// Resolved namespace metadata for existing namespace-scoped resources.
    pub namespace: Option<ResolvedNamespace>,
    /// Edge admission timestamp.
    pub received_at: OffsetDateTime,
    /// Whether the admitted RPC may remain parked as a long poll.
    pub is_long_poll: bool,
}

impl EdgeContext {
    /// Return the authenticated identity in the transport-neutral form carried
    /// by authoritative requests.
    ///
    /// This conversion is the sole attribution source. Request metadata and
    /// client-authored identity fields are intentionally ignored, preserving
    /// Temporal's strip-then-propagate spoofing invariant without introducing
    /// an internode metadata hop (`interceptor.go:153-155 @ v1.31.0`).
    pub fn event_principal(&self) -> Option<EventPrincipal> {
        self.auth_principal
            .as_ref()
            .map(|principal| EventPrincipal {
                principal_type: principal.principal_type.clone(),
                name: principal.name.clone(),
            })
    }
}

#[cfg(not(feature = "conformance"))]
#[inline]
fn principal_propagation_enabled(configured: bool) -> bool {
    configured
}

#[cfg(feature = "conformance")]
#[inline]
fn principal_propagation_enabled(configured: bool) -> bool {
    // The harness registry is process-global, so Tokeira deliberately
    // collapses v1.31.0's namespace-scoped gate to one live value.
    tokeira_conformance::overrides()
        .get_bool("frontend.enablePrincipalPropagation")
        .unwrap_or(configured)
}

#[cfg(not(feature = "conformance"))]
#[inline]
pub(crate) fn cross_namespace_commands_enabled() -> bool {
    false
}

#[cfg(feature = "conformance")]
#[inline]
pub(crate) fn cross_namespace_commands_enabled() -> bool {
    tokeira_conformance::overrides()
        .get_bool("system.enableCrossNamespaceCommands")
        .unwrap_or(false)
}

#[derive(Clone)]
pub struct EdgeInterceptors {
    request_ids: Arc<dyn RequestIdGenerator>,
    authenticator: Arc<dyn Authenticator>,
    namespaces: Arc<dyn NamespaceCache>,
    principal_attribution: bool,
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
            principal_attribution: false,
        }
    }

    pub fn permissive(namespaces: Arc<dyn NamespaceCache>) -> Self {
        Self {
            request_ids: Arc::new(UuidRequestIdGenerator),
            authenticator: Arc::new(AllowAllAuthenticator),
            namespaces,
            principal_attribution: false,
        }
    }

    /// Construct the edge seam around a configured authentication pipeline.
    pub fn configured(
        namespaces: Arc<dyn NamespaceCache>,
        authenticator: Arc<dyn Authenticator>,
        principal_attribution: bool,
    ) -> Self {
        Self {
            request_ids: Arc::new(UuidRequestIdGenerator),
            authenticator,
            namespaces,
            principal_attribution,
        }
    }

    /// Perform the standard edge pipeline:
    ///
    /// 1. assign or recover request id,
    /// 2. authenticate the caller,
    /// 3. authorize against the request's namespace name,
    /// 4. resolve existing namespace metadata when applicable.
    ///
    /// Doing this consistently up front keeps individual service handlers small.
    pub async fn begin(
        &self,
        headers: &HeaderMap,
        namespace_name: Option<&str>,
        action: Action,
        is_long_poll: bool,
    ) -> EdgeResult<EdgeContext> {
        self.begin_with_worker_target(headers, namespace_name, action, is_long_poll, None)
            .await
    }

    /// Authenticate and perform the namespace/API preflight for a Worker RPC.
    ///
    /// Resource decoding and any broker/runtime effect must occur only after
    /// this succeeds. The returned claims are reused by
    /// [`Self::authorize_worker_target`], so signed credentials and STS
    /// requests are verified exactly once.
    pub async fn begin_worker_preflight(
        &self,
        headers: &HeaderMap,
        namespace_name: Option<&str>,
        action: Action,
        is_long_poll: bool,
    ) -> EdgeResult<EdgeContext> {
        let operation = action.worker_operation().ok_or_else(|| {
            EdgeError::Internal("non-Worker API requested Worker preflight".to_owned())
        })?;
        self.begin_with_worker_target(
            headers,
            namespace_name,
            action,
            is_long_poll,
            Some(WorkerCallTarget {
                operation,
                target: WorkerTarget::Preflight,
            }),
        )
        .await
    }

    async fn begin_with_worker_target(
        &self,
        headers: &HeaderMap,
        namespace_name: Option<&str>,
        action: Action,
        is_long_poll: bool,
        worker: Option<WorkerCallTarget<'_>>,
    ) -> EdgeResult<EdgeContext> {
        let request_id = extract_or_generate(headers, self.request_ids.as_ref());
        let started = Instant::now();
        let claims = match self.authenticator.authenticate(headers).await {
            Ok(claims) => claims,
            Err(error) => {
                record_authorization_error(action.api_name(), "authenticate");
                record_authorization_duration(action.api_name(), "error", started.elapsed());
                return Err(error);
            }
        };

        // Authorization deliberately precedes existence resolution so an
        // unprivileged caller cannot probe namespace names (`interceptor.go @ v1.31.0`).
        let authorization = match worker {
            Some(worker) => {
                self.authenticator
                    .authorize_worker(claims.as_ref(), action, namespace_name, worker)
                    .await
            }
            None => {
                self.authenticator
                    .authorize(claims.as_ref(), action, namespace_name)
                    .await
            }
        };
        let auth_principal = match authorization {
            Ok(principal) => principal,
            Err(error) => {
                if let Some(reason) =
                    scoped_worker_deny_reason(claims.as_ref(), namespace_name, worker)
                {
                    record_scoped_worker_authorization_denied(
                        action.api_name(),
                        reason.metric_label(),
                    );
                    log_scoped_worker_authorization_denied(claims.as_ref(), action, reason);
                }
                record_authorization_duration(action.api_name(), "denied", started.elapsed());
                return Err(error);
            }
        }
        .filter(|_| principal_propagation_enabled(self.principal_attribution));
        record_authorization_duration(action.api_name(), "allowed", started.elapsed());

        // Namespace lifecycle metadata RPCs authorize the requested/resolved
        // name but perform their own lookup because creation has no record yet
        // and Describe/Update must remain able to observe deleted tombstones.
        let namespace = match namespace_name.filter(|_| {
            !matches!(
                action,
                Action::RegisterNamespace | Action::DescribeNamespace | Action::UpdateNamespace
            )
        }) {
            Some(name) => {
                let Some(ns) = self.namespaces.get(name).await.map_err(EdgeError::from)? else {
                    return Err(EdgeError::NamespaceNotFound(name.to_string()));
                };

                if ns.deleted {
                    return Err(EdgeError::NamespaceDeleted(name.to_string()));
                }

                Some(ns)
            }
            None => None,
        };

        Ok(EdgeContext {
            request_id,
            claims,
            auth_principal,
            namespace,
            received_at: OffsetDateTime::now_utc(),
            is_long_poll,
        })
    }

    /// Complete authorization against a normalized Worker resource.
    ///
    /// Callers must invoke this after preflight and before registering a
    /// waiter, claiming work, mutating a heartbeat/session registry, or
    /// resolving a task token against runtime state.
    pub async fn authorize_worker_target(
        &self,
        context: &EdgeContext,
        action: Action,
        target: WorkerTarget<'_>,
    ) -> EdgeResult<()> {
        let operation = action.worker_operation().ok_or_else(|| {
            EdgeError::Internal("non-Worker API requested Worker target authorization".to_owned())
        })?;
        self.authorize_worker_operation_target(context, action, operation, target)
            .await
    }

    /// Authorize one fixed Worker operation as part of an enclosing Worker RPC.
    ///
    /// Compound RPCs use this for nested Worker resources such as a shutdown
    /// heartbeat. `action` retains the outer API name for metrics and policy
    /// diagnostics while `operation` selects the fixed resource shape.
    pub(crate) async fn authorize_worker_operation_target(
        &self,
        context: &EdgeContext,
        action: Action,
        operation: WorkerOperation,
        target: WorkerTarget<'_>,
    ) -> EdgeResult<()> {
        let worker = WorkerCallTarget { operation, target };
        let namespace_name = context
            .namespace
            .as_ref()
            .map(|namespace| namespace.name.as_str());
        let started = Instant::now();
        match self
            .authenticator
            .authorize_worker(context.claims.as_ref(), action, namespace_name, worker)
            .await
        {
            Ok(_) => {
                record_authorization_duration(action.api_name(), "allowed", started.elapsed());
                Ok(())
            }
            Err(error) => {
                let reason = scoped_worker_deny_reason(
                    context.claims.as_ref(),
                    namespace_name,
                    Some(worker),
                )
                .unwrap_or(WorkerScopeDenyReason::Operation);
                record_scoped_worker_authorization_denied(action.api_name(), reason.metric_label());
                log_scoped_worker_authorization_denied(context.claims.as_ref(), action, reason);
                record_authorization_duration(action.api_name(), "denied", started.elapsed());
                Err(error)
            }
        }
    }

    /// Back-fill an omitted request namespace from a decoded task token, then
    /// run normal authentication and authorization against the current name.
    ///
    /// Callers must decode the token before invoking this method. That preserves
    /// v1.31.0's empty-namespace precedence: malformed tokens fail before auth,
    /// while a stable ID resolves ahead of the authorization interceptor
    /// (`namespace_validator.go:104-148`, `service/frontend/fx.go:256-290 @
    /// v1.31.0`).
    pub async fn begin_with_task_token_backfill(
        &self,
        headers: &HeaderMap,
        token_namespace_id: Option<NamespaceId>,
        action: Action,
    ) -> EdgeResult<(EdgeContext, String)> {
        let Some(token_namespace_id) = token_namespace_id else {
            return self
                .begin(headers, None, action, false)
                .await
                .map(|context| (context, String::new()));
        };
        let namespace_id = token_namespace_id.0.to_string();
        let namespace = self
            .namespaces
            .get_by_id(&namespace_id)
            .await
            .map_err(EdgeError::from)?
            .ok_or(EdgeError::NamespaceNotFound(namespace_id))?;
        let current_name = namespace.name;
        let context = self
            .begin(headers, Some(&current_name), action, false)
            .await?;
        Ok((context, current_name))
    }

    /// Back-fill an omitted namespace, then perform Worker preflight admission.
    ///
    /// Token decoding remains the caller's responsibility so the empty-name
    /// validation precedence stays identical to
    /// [`Self::begin_with_task_token_backfill`]. The returned context is reused
    /// for exact provenance authorization without verifying credentials twice.
    pub async fn begin_worker_with_task_token_backfill(
        &self,
        headers: &HeaderMap,
        token_namespace_id: Option<NamespaceId>,
        action: Action,
    ) -> EdgeResult<(EdgeContext, String)> {
        let Some(token_namespace_id) = token_namespace_id else {
            return self
                .begin_worker_preflight(headers, None, action, false)
                .await
                .map(|context| (context, String::new()));
        };
        let namespace_id = token_namespace_id.0.to_string();
        let namespace = self
            .namespaces
            .get_by_id(&namespace_id)
            .await
            .map_err(EdgeError::from)?
            .ok_or(EdgeError::NamespaceNotFound(namespace_id))?;
        let current_name = namespace.name;
        let context = self
            .begin_worker_preflight(headers, Some(&current_name), action, false)
            .await?;
        Ok((context, current_name))
    }

    /// Re-authorize one target namespace using the caller admitted at `begin`.
    ///
    /// Cross-namespace workflow commands do not re-run authentication: the
    /// workflow-task completion has one caller, but each distinct target/API
    /// pair needs its own role decision (`interceptor.go:287-353 @ v1.31.0`).
    pub(crate) async fn authorize_existing_context(
        &self,
        context: &EdgeContext,
        action: Action,
        namespace_name: &str,
    ) -> EdgeResult<()> {
        let started = Instant::now();
        match self
            .authenticator
            .authorize(context.claims.as_ref(), action, Some(namespace_name))
            .await
        {
            Ok(_) => {
                record_authorization_duration(action.api_name(), "allowed", started.elapsed());
                Ok(())
            }
            Err(error) => {
                record_authorization_duration(action.api_name(), "denied", started.elapsed());
                Err(error)
            }
        }
    }
}

fn scoped_worker_deny_reason(
    claims: Option<&Claims>,
    namespace_name: Option<&str>,
    worker: Option<WorkerCallTarget<'_>>,
) -> Option<WorkerScopeDenyReason> {
    let claims = claims?;
    let scope = claims.worker_scope.as_ref()?;
    let worker = worker?;
    match scope.authorize(
        worker.operation,
        namespace_name.unwrap_or_default(),
        worker.target,
    ) {
        tokeira_auth::WorkerScopeDecision::Allow => None,
        tokeira_auth::WorkerScopeDecision::Deny(reason) => Some(reason),
    }
}

fn log_scoped_worker_authorization_denied(
    claims: Option<&Claims>,
    action: Action,
    reason: WorkerScopeDenyReason,
) {
    let Some(claims) = claims.filter(|claims| claims.worker_scope.is_some()) else {
        return;
    };
    // Subject is the only identity detail permitted by Requirement 11.6. Keep
    // resource coordinates and bearer material out of this event; the bounded
    // reason is sufficient for an operator to diagnose the policy dimension.
    tracing::warn!(
        subject = %claims.subject,
        api = action.api_name(),
        reason = reason.metric_label(),
        "scoped Worker authorization denied"
    );
}

/// Normalize a poll's complete wire target into the fixed authorization shape.
///
/// A false `scoped_versioned` deliberately erases any deprecated routing
/// coordinates. They remain available to ordinary compatibility routing, but
/// cannot prove authority for a scoped identity. Missing sticky `normal_name`
/// and partial modern versions normalize to empty exact coordinates, which a
/// valid non-empty [`tokeira_auth::WorkerScope`] necessarily rejects.
pub(crate) fn normalize_worker_poll_target<'a>(
    task_queue: &'a str,
    normal_task_queue: Option<&'a str>,
    is_sticky: bool,
    scoped_versioned: bool,
    deployment_name: Option<&'a str>,
    build_id: Option<&'a str>,
    task_class: WorkerTaskClass,
) -> WorkerTarget<'a> {
    let normal_task_queue = if is_sticky {
        normal_task_queue.unwrap_or_default()
    } else {
        task_queue
    };
    WorkerTarget::VersionedTask {
        normal_task_queue,
        task_class,
        deployment_name: scoped_versioned
            .then_some(deployment_name)
            .flatten()
            .unwrap_or_default(),
        build_id: scoped_versioned
            .then_some(build_id)
            .flatten()
            .unwrap_or_default(),
    }
}

/// Validate the decoded token namespace after a non-empty request namespace
/// has already passed authorization and namespace resolution.
pub(crate) fn validate_task_token_namespace(
    context: &EdgeContext,
    token_namespace_id: Option<NamespaceId>,
) -> EdgeResult<()> {
    if !token_namespace_enforcement_enabled() {
        return Ok(());
    }
    let Some(token_namespace_id) = token_namespace_id else {
        return Ok(());
    };
    let namespace = context.namespace.as_ref().ok_or_else(|| {
        EdgeError::Internal("namespace-scoped admission returned no namespace".to_owned())
    })?;
    let request_namespace_id = namespace.namespace_id.clone().unwrap_or_else(|| {
        crate::translate::to_internal::namespace_id_for(&namespace.name)
            .0
            .to_string()
    });
    if request_namespace_id != token_namespace_id.0.to_string() {
        return Err(EdgeError::BadRequest(
            "Operation requested with a token from a different namespace.".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use proptest::prelude::*;
    use tokeira_auth::{
        AuthError, AuthzError, DefaultAuthorizer, Role, WorkerScope, WorkerScopeDecision,
    };

    use super::*;

    #[derive(Debug)]
    struct StaticMapper {
        claims: Claims,
    }

    #[async_trait]
    impl ClaimMapper for StaticMapper {
        async fn get_claims(&self, _token: &str, _extra: &str) -> Result<Claims, AuthError> {
            Ok(self.claims.clone())
        }
    }

    #[derive(Debug)]
    struct CountingMapper {
        claims: Claims,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ClaimMapper for CountingMapper {
        async fn get_claims(&self, _token: &str, _extra: &str) -> Result<Claims, AuthError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.claims.clone())
        }
    }

    #[derive(Debug)]
    struct FailingMapper;

    #[async_trait]
    impl ClaimMapper for FailingMapper {
        async fn get_claims(&self, _token: &str, _extra: &str) -> Result<Claims, AuthError> {
            Err(AuthError::new("sensitive mapper diagnostic"))
        }
    }

    #[derive(Debug)]
    struct ReasonAuthorizer;

    impl Authorizer for ReasonAuthorizer {
        fn authorize(
            &self,
            _claims: Option<&Claims>,
            _target: &CallTarget<'_>,
        ) -> Result<AuthzDecision, AuthzError> {
            Ok(AuthzDecision::Deny {
                reason: Some("namespace policy".to_owned()),
            })
        }
    }

    #[derive(Debug)]
    struct FailingAuthorizer;

    impl Authorizer for FailingAuthorizer {
        fn authorize(
            &self,
            _claims: Option<&Claims>,
            _target: &CallTarget<'_>,
        ) -> Result<AuthzDecision, AuthzError> {
            Err(AuthzError::new("policy backend unavailable"))
        }
    }

    #[test]
    fn classifications_match_v1_31_non_negotiable_rows() {
        for action in [
            Action::CreateNexusEndpoint,
            Action::UpdateNexusEndpoint,
            Action::DeleteNexusEndpoint,
            Action::GetNexusEndpoint,
            Action::ListNexusEndpoints,
        ] {
            assert_eq!(
                action.classification(),
                CallClassification {
                    scope: Scope::Cluster,
                    access: Access::Admin,
                }
            );
        }
        assert_eq!(
            Action::ListSearchAttributes.classification(),
            CallClassification {
                scope: Scope::Namespace,
                access: Access::ReadOnly,
            }
        );
        for action in [
            Action::RegisterNamespace,
            Action::UpdateNamespace,
            Action::DeleteNamespace,
        ] {
            assert_eq!(
                action.classification(),
                CallClassification {
                    scope: Scope::Namespace,
                    access: Access::Admin,
                }
            );
        }
        for action in [
            Action::PollWorkflowTaskQueue,
            Action::PollActivityTaskQueue,
            Action::PollNexusTaskQueue,
        ] {
            assert_eq!(action.classification().access, Access::Write);
        }
        assert_eq!(
            Action::PollActivityExecution.classification().access,
            Access::ReadOnly
        );
        assert_eq!(
            Action::DescribeMutableState.classification(),
            CallClassification {
                scope: Scope::Cluster,
                access: Access::Admin,
            }
        );
    }

    #[test]
    fn action_api_names_are_exact_public_paths() {
        assert_eq!(
            Action::GetNexusEndpoint.api_name(),
            "/temporal.api.operatorservice.v1.OperatorService/GetNexusEndpoint"
        );
        assert_eq!(
            Action::PollWorkflowExecutionUpdate.api_name(),
            "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowExecutionUpdate"
        );
        assert_eq!(
            Action::DescribeMutableState.api_name(),
            "/temporal.server.api.adminservice.v1.AdminService/DescribeMutableState"
        );
    }

    #[test]
    fn fixed_worker_action_mapping_excludes_activity_by_id_aliases() {
        assert_eq!(
            Action::RespondActivityTaskCompleted.worker_operation(),
            Some(WorkerOperation::RespondActivityTaskCompleted)
        );
        for action in [
            Action::RespondActivityTaskCompletedById,
            Action::RespondActivityTaskFailedById,
            Action::RespondActivityTaskCanceledById,
            Action::RecordActivityTaskHeartbeatById,
        ] {
            assert_eq!(action.worker_operation(), None);
            assert_eq!(action.classification().access, Access::Write);
        }
    }

    #[test]
    fn fixed_worker_action_mapping_is_exhaustive() {
        assert_eq!(
            Action::ALL.len(),
            Action::HealthRead as usize + 1,
            "Action::ALL must include every action exactly once"
        );
        for action in Action::ALL {
            let expected = matches!(
                action,
                Action::PollWorkflowTaskQueue
                    | Action::RespondWorkflowTaskCompleted
                    | Action::RespondWorkflowTaskFailed
                    | Action::RespondQueryTaskCompleted
                    | Action::PollActivityTaskQueue
                    | Action::RespondActivityTaskCompleted
                    | Action::RespondActivityTaskFailed
                    | Action::RespondActivityTaskCanceled
                    | Action::PollNexusTaskQueue
                    | Action::RespondNexusTaskCompleted
                    | Action::RespondNexusTaskFailed
                    | Action::RecordActivityTaskHeartbeat
                    | Action::RecordWorkerHeartbeat
                    | Action::ShutdownWorker
                    | Action::DescribeTaskQueue
            );
            assert_eq!(
                action.worker_operation().is_some(),
                expected,
                "unexpected scoped Worker mapping for {action:?}"
            );
        }
        for denied in [
            Action::ResetStickyTaskQueue,
            Action::ListWorkers,
            Action::DescribeWorker,
            Action::RespondActivityTaskCompletedById,
            Action::StartActivityExecution,
        ] {
            assert_eq!(denied.worker_operation(), None);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn property_poll_target_normalization(
            namespace_matches in any::<bool>(),
            queue_matches in any::<bool>(),
            sticky in any::<bool>(),
            sticky_normal_present in any::<bool>(),
            version_case in 0_u8..4,
            deployment_matches in any::<bool>(),
            build_matches in any::<bool>(),
        ) {
            // Feature: scoped-worker-authorization, Property 5: Poll-target normalization
            let scope = WorkerScope::try_new(
                "payments".to_owned(),
                vec!["payments-queue".to_owned()],
                "payments-deployment".to_owned(),
                "build-a".to_owned(),
            )
            .expect("fixed valid scope");
            let namespace = if namespace_matches { "payments" } else { "other" };
            let selected_queue = if queue_matches {
                "payments-queue"
            } else {
                "other-queue"
            };
            let task_queue = if sticky { "sticky-cache" } else { selected_queue };
            let normal_task_queue = (sticky && sticky_normal_present).then_some(selected_queue);
            let scoped_versioned = version_case == 0 || version_case == 1;
            let deployment = match version_case {
                0 => Some(if deployment_matches {
                    "payments-deployment"
                } else {
                    "other-deployment"
                }),
                1 => None,
                2 => Some("payments-deployment"), // deprecated-only coordinates
                _ => None,
            };
            let build_id = match version_case {
                0 => Some(if build_matches { "build-a" } else { "build-b" }),
                1 => Some("build-a"), // partial modern options
                2 => Some("build-a"),
                _ => None,
            };
            let target = normalize_worker_poll_target(
                task_queue,
                normal_task_queue,
                sticky,
                scoped_versioned,
                deployment,
                build_id,
                WorkerTaskClass::Workflow,
            );

            let actual = scope.authorize(
                WorkerOperation::PollWorkflowTaskQueue,
                namespace,
                target,
            );
            let expected = namespace_matches
                && queue_matches
                && (!sticky || sticky_normal_present)
                && version_case == 0
                && deployment_matches
                && build_matches;
            prop_assert_eq!(actual == WorkerScopeDecision::Allow, expected);
        }

        #[test]
        fn property_scoped_denial_classification_is_bounded_and_secret_free(
            reason_index in 0_usize..8,
            secret in "[A-Za-z0-9]{8,64}",
        ) {
            // Feature: scoped-worker-authorization, Property 13: Bounded denial classification
            //
            // The sensitive value is prefixed so it is distinctive by
            // construction: the labels below are fixed identifiers, and an
            // unconstrained 1..64-char generator could (rarely) produce one
            // of their substrings — a flaky false positive, not a leak. A
            // real leak embeds the caller's value verbatim and is still
            // caught.
            let sensitive = format!("sk-{secret}");
            let reasons = [
                WorkerScopeDenyReason::Operation,
                WorkerScopeDenyReason::Namespace,
                WorkerScopeDenyReason::Queue,
                WorkerScopeDenyReason::Version,
                WorkerScopeDenyReason::TaskOrigin,
                WorkerScopeDenyReason::Heartbeat,
                WorkerScopeDenyReason::WorkerSession,
                WorkerScopeDenyReason::AmbiguousMapping,
            ];
            let label = reasons[reason_index].metric_label();
            prop_assert!(matches!(
                label,
                "operation"
                    | "namespace"
                    | "queue"
                    | "version"
                    | "task_origin"
                    | "heartbeat"
                    | "worker_session"
                    | "ambiguous_mapping"
            ));
            prop_assert!(!label.contains(&sensitive));
            let public = PolicyAuthenticator::denied(None).to_string();
            prop_assert_eq!(public.as_str(), "Request unauthorized.");
            prop_assert!(!public.contains(&sensitive));
        }
    }

    #[cfg(not(feature = "conformance"))]
    #[test]
    fn production_auth_gates_equal_their_static_defaults() {
        // Feature: authorization-foundation, Property 7: a production build
        // has no mutable override surface and evaluates only the configured or
        // v1.31.0-pinned fallback (Requirement 9.2-9.3).
        assert!(principal_propagation_enabled(true));
        assert!(!principal_propagation_enabled(false));
        assert!(authorizer_errors_exposed(true));
        assert!(!authorizer_errors_exposed(false));
        assert!(token_namespace_enforcement_enabled());
        assert!(!cross_namespace_commands_enabled());
    }

    #[tokio::test]
    async fn authorization_denies_before_namespace_existence_is_resolved() {
        let namespaces = Arc::new(crate::namespace_cache::InMemoryNamespaceCache::new());
        let authenticator = Arc::new(PolicyAuthenticator::new(
            Arc::new(StaticMapper {
                claims: Claims::default(),
            }),
            Arc::new(DefaultAuthorizer),
            false,
        ));
        let edge = EdgeInterceptors::configured(namespaces, authenticator, false);
        let error = edge
            .begin(
                &HeaderMap::new(),
                Some("private"),
                Action::StartWorkflowExecution,
                false,
            )
            .await
            .expect_err("role-less caller must be denied");
        assert!(matches!(error, EdgeError::PermissionDenied { .. }));
    }

    #[tokio::test]
    async fn namespace_lookup_runs_after_an_allow_decision() {
        let namespaces = Arc::new(crate::namespace_cache::InMemoryNamespaceCache::new());
        let authenticator = Arc::new(PolicyAuthenticator::new(
            Arc::new(StaticMapper {
                claims: Claims {
                    system: Role::ADMIN,
                    ..Default::default()
                },
            }),
            Arc::new(DefaultAuthorizer),
            false,
        ));
        let edge = EdgeInterceptors::configured(namespaces, authenticator, false);
        let error = edge
            .begin(
                &HeaderMap::new(),
                Some("missing"),
                Action::StartWorkflowExecution,
                false,
            )
            .await
            .expect_err("allowed caller reaches lookup");
        assert!(matches!(error, EdgeError::NamespaceNotFound(namespace) if namespace == "missing"));
    }

    #[tokio::test]
    async fn worker_preflight_and_final_target_reuse_one_authentication() {
        let namespace_id = NamespaceId::new();
        let namespaces = Arc::new(crate::namespace_cache::InMemoryNamespaceCache::new());
        namespaces
            .insert(ResolvedNamespace {
                namespace_id: Some(namespace_id.0.to_string()),
                ..ResolvedNamespace::active("payments")
            })
            .await
            .expect("insert namespace");
        let calls = Arc::new(AtomicUsize::new(0));
        let authenticator = Arc::new(PolicyAuthenticator::new(
            Arc::new(CountingMapper {
                claims: Claims {
                    subject: "worker-a".to_owned(),
                    auth_type: "jwt".to_owned(),
                    worker_scope: Some(
                        WorkerScope::try_new(
                            "payments".to_owned(),
                            vec!["payments-queue".to_owned()],
                            "payments-deployment".to_owned(),
                            "build-a".to_owned(),
                        )
                        .expect("scope"),
                    ),
                    ..Claims::default()
                },
                calls: calls.clone(),
            }),
            Arc::new(DefaultAuthorizer),
            false,
        ));
        let edge = EdgeInterceptors::configured(namespaces, authenticator, false);

        let context = edge
            .begin_worker_preflight(
                &HeaderMap::new(),
                Some("payments"),
                Action::PollWorkflowTaskQueue,
                true,
            )
            .await
            .expect("preflight");
        edge.authorize_worker_target(
            &context,
            Action::PollWorkflowTaskQueue,
            WorkerTarget::VersionedTask {
                normal_task_queue: "payments-queue",
                task_class: WorkerTaskClass::Workflow,
                deployment_name: "payments-deployment",
                build_id: "build-a",
            },
        )
        .await
        .expect("final target");

        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn task_token_backfill_authorizes_the_current_namespace_name() {
        let namespace_id = NamespaceId::new();
        let namespaces = Arc::new(crate::namespace_cache::InMemoryNamespaceCache::new());
        namespaces
            .insert(ResolvedNamespace {
                namespace_id: Some(namespace_id.0.to_string()),
                ..ResolvedNamespace::active("payments")
            })
            .await
            .expect("insert namespace");
        let authenticator = Arc::new(PolicyAuthenticator::new(
            Arc::new(StaticMapper {
                claims: Claims {
                    namespaces: [("payments".to_owned(), Role::WRITER)]
                        .into_iter()
                        .collect(),
                    ..Default::default()
                },
            }),
            Arc::new(DefaultAuthorizer),
            false,
        ));
        let edge = EdgeInterceptors::configured(namespaces, authenticator, false);

        let (context, effective_name) = edge
            .begin_with_task_token_backfill(
                &HeaderMap::new(),
                Some(namespace_id),
                Action::RespondActivityTaskCompleted,
            )
            .await
            .expect("back-filled namespace authorizes");

        assert_eq!(effective_name, "payments");
        assert_eq!(context.namespace.expect("resolved").name, "payments");
    }

    #[tokio::test]
    async fn task_token_mismatch_is_checked_after_explicit_namespace_authorization() {
        let request_namespace_id = NamespaceId::new();
        let token_namespace_id = NamespaceId::new();
        let namespaces = Arc::new(crate::namespace_cache::InMemoryNamespaceCache::new());
        namespaces
            .insert(ResolvedNamespace {
                namespace_id: Some(request_namespace_id.0.to_string()),
                ..ResolvedNamespace::active("payments")
            })
            .await
            .expect("insert namespace");
        let authenticator = Arc::new(PolicyAuthenticator::new(
            Arc::new(StaticMapper {
                claims: Claims {
                    namespaces: [("payments".to_owned(), Role::WRITER)]
                        .into_iter()
                        .collect(),
                    ..Default::default()
                },
            }),
            Arc::new(DefaultAuthorizer),
            false,
        ));
        let edge = EdgeInterceptors::configured(namespaces, authenticator, false);
        let context = edge
            .begin(
                &HeaderMap::new(),
                Some("payments"),
                Action::RespondActivityTaskCompleted,
                false,
            )
            .await
            .expect("explicit namespace authorizes");

        let error = validate_task_token_namespace(&context, Some(token_namespace_id))
            .expect_err("mismatch must reject");
        assert!(
            matches!(error, EdgeError::BadRequest(message) if message == "Operation requested with a token from a different namespace.")
        );
    }

    #[tokio::test]
    async fn authorizer_errors_are_exposed_only_when_configured() {
        for (expose, expected) in [
            (false, "Request unauthorized."),
            (true, "policy backend unavailable"),
        ] {
            let namespaces = Arc::new(crate::namespace_cache::InMemoryNamespaceCache::new());
            let authenticator = Arc::new(PolicyAuthenticator::new(
                Arc::new(StaticMapper {
                    claims: Claims::default(),
                }),
                Arc::new(FailingAuthorizer),
                expose,
            ));
            let edge = EdgeInterceptors::configured(namespaces, authenticator, false);
            let error = edge
                .begin(&HeaderMap::new(), None, Action::GetClusterInfo, false)
                .await
                .expect_err("authorizer failure");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[tokio::test]
    async fn claim_mapper_errors_are_never_exposed() {
        for expose in [false, true] {
            let authenticator = PolicyAuthenticator::new(
                Arc::new(FailingMapper),
                Arc::new(DefaultAuthorizer),
                expose,
            );
            let error = authenticator
                .authenticate(&HeaderMap::new())
                .await
                .expect_err("claim mapper failure");
            let EdgeError::PermissionDenied { message, reason } = error else {
                panic!("claim mapper failure must be permission denied");
            };
            assert_eq!(message, "Request unauthorized.");
            assert_eq!(reason, None);
        }
    }

    #[tokio::test]
    async fn authorizer_denial_preserves_only_the_typed_reason() {
        let authenticator = PolicyAuthenticator::new(
            Arc::new(StaticMapper {
                claims: Claims::default(),
            }),
            Arc::new(ReasonAuthorizer),
            true,
        );
        let claims = authenticator
            .authenticate(&HeaderMap::new())
            .await
            .expect("claims");
        let error = authenticator
            .authorize(claims.as_ref(), Action::GetClusterInfo, None)
            .await
            .expect_err("intentional denial");
        let EdgeError::PermissionDenied { message, reason } = error else {
            panic!("authorizer denial must be permission denied");
        };
        assert_eq!(message, "Request unauthorized.");
        assert_eq!(reason.as_deref(), Some("namespace policy"));
    }

    #[tokio::test]
    async fn anonymous_get_system_info_is_allowed_under_enforcement() {
        let namespaces = Arc::new(crate::namespace_cache::InMemoryNamespaceCache::new());
        let authenticator = Arc::new(PolicyAuthenticator::new(
            Arc::new(StaticMapper {
                claims: Claims::default(),
            }),
            Arc::new(DefaultAuthorizer),
            false,
        ));
        let edge = EdgeInterceptors::configured(namespaces, authenticator, false);
        for action in [Action::GetSystemInfo, Action::HealthRead] {
            edge.begin(&HeaderMap::new(), None, action, false)
                .await
                .expect("v1.31 health API is allowed before role checks");
        }
    }

    #[tokio::test]
    async fn begin_covers_chasm_and_admin_public_surfaces() {
        let namespaces = Arc::new(crate::namespace_cache::InMemoryNamespaceCache::new());
        let authenticator = Arc::new(PolicyAuthenticator::new(
            Arc::new(StaticMapper {
                claims: Claims::default(),
            }),
            Arc::new(DefaultAuthorizer),
            false,
        ));
        let edge = EdgeInterceptors::configured(namespaces, authenticator, false);

        for action in [
            Action::StartActivityExecution,
            Action::DescribeActivityExecution,
            Action::PollActivityExecution,
            Action::RequestCancelActivityExecution,
            Action::TerminateActivityExecution,
            Action::DeleteActivityExecution,
            Action::DescribeMutableState,
        ] {
            let namespace = (!matches!(action, Action::DescribeMutableState)).then_some("private");
            let error = edge
                .begin(&HeaderMap::new(), namespace, action, false)
                .await
                .expect_err("role-less caller must not bypass begin");
            assert_eq!(error.to_string(), "Request unauthorized.");
        }
    }

    #[tokio::test]
    async fn attribution_uses_only_authorizer_principal_and_honors_gate() {
        // Feature: authorization-foundation, Property 11: client-controlled
        // principal-looking metadata cannot influence the durable stamp. The
        // only source is the server-side authorizer decision (Requirements
        // 7.1, 7.3, 7.4, 7.6).
        let namespaces = Arc::new(crate::namespace_cache::InMemoryNamespaceCache::new());
        let authenticator = Arc::new(PolicyAuthenticator::new(
            Arc::new(StaticMapper {
                claims: Claims {
                    subject: String::new(),
                    auth_type: "jwt".into(),
                    system: Role::ADMIN,
                    ..Default::default()
                },
            }),
            Arc::new(DefaultAuthorizer),
            false,
        ));
        let mut headers = HeaderMap::new();
        headers.insert(
            "temporal-principal-type",
            http::HeaderValue::from_static("spoofed-type"),
        );
        headers.insert(
            "temporal-principal-name",
            http::HeaderValue::from_static("spoofed-name"),
        );

        let attributed =
            EdgeInterceptors::configured(namespaces.clone(), authenticator.clone(), true)
                .begin(&headers, None, Action::GetClusterInfo, false)
                .await
                .expect("admin caller authorizes");
        assert_eq!(
            attributed.event_principal(),
            Some(EventPrincipal {
                principal_type: "jwt".into(),
                name: String::new(),
            })
        );

        let propagation_off = EdgeInterceptors::configured(namespaces, authenticator, false)
            .begin(&headers, None, Action::GetClusterInfo, false)
            .await
            .expect("authorization remains active with attribution off");
        assert_eq!(propagation_off.event_principal(), None);
    }

    #[tokio::test]
    async fn cross_namespace_reauthorization_uses_original_claims_for_target() {
        let namespaces = Arc::new(crate::namespace_cache::InMemoryNamespaceCache::new());
        namespaces
            .insert(ResolvedNamespace::active("source"))
            .await
            .expect("insert source namespace");
        let authenticator = Arc::new(PolicyAuthenticator::new(
            Arc::new(StaticMapper {
                claims: Claims {
                    namespaces: [("source".to_owned(), Role::WRITER)].into_iter().collect(),
                    ..Default::default()
                },
            }),
            Arc::new(DefaultAuthorizer),
            false,
        ));
        let edge = EdgeInterceptors::configured(namespaces, authenticator, false);
        let context = edge
            .begin(
                &HeaderMap::new(),
                Some("source"),
                Action::RespondWorkflowTaskCompleted,
                false,
            )
            .await
            .expect("source writer may complete its workflow task");

        let error = edge
            .authorize_existing_context(&context, Action::StartWorkflowExecution, "target")
            .await
            .expect_err("source-only claim must not pivot into target");

        assert!(matches!(error, EdgeError::PermissionDenied { .. }));
        assert_eq!(error.to_string(), "Request unauthorized.");
    }
}
