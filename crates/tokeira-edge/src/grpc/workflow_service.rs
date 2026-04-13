use tonic::{Request, Response, Status};
use tracing::debug;

use tokeira_proto::workflowservice::{
    self, workflow_service_server::WorkflowService as WorkflowServiceGrpcApi,
    workflow_service_server::WorkflowServiceServer,
};

use crate::{
    grpc::{
        errors::proto_conversion_status, metadata::metadata_to_header_map, translate,
    },
    workflow_service::WorkflowService,
};

#[derive(Clone)]
pub struct WorkflowServiceGrpc {
    inner: WorkflowService,
}

impl WorkflowServiceGrpc {
    pub fn new(inner: WorkflowService) -> Self {
        Self { inner }
    }

    pub fn into_service(self) -> WorkflowServiceServer<Self> {
        WorkflowServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl WorkflowServiceGrpcApi for WorkflowServiceGrpc {
    async fn start_workflow_execution(
        &self,
        request: Request<workflowservice::StartWorkflowExecutionRequest>,
    ) -> Result<Response<workflowservice::StartWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::start_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        debug!(workflow_id = %edge_req.workflow_id, workflow_type = %edge_req.workflow_type, "start_workflow_execution");
        let edge_resp = self
            .inner
            .start_workflow_execution(&headers, edge_req)
            .await?;
        debug!(run_id = ?edge_resp.run_id, "start_workflow_execution success");
        Ok(Response::new(translate::start_response_to_proto(edge_resp)))
    }

    async fn signal_workflow_execution(
        &self,
        request: Request<workflowservice::SignalWorkflowExecutionRequest>,
    ) -> Result<Response<workflowservice::SignalWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::signal_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .signal_workflow_execution(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::signal_response_to_proto(
            edge_resp,
        )))
    }

    async fn poll_workflow_task_queue(
        &self,
        request: Request<workflowservice::PollWorkflowTaskQueueRequest>,
    ) -> Result<Response<workflowservice::PollWorkflowTaskQueueResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::poll_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        debug!(task_queue = %edge_req.task_queue, "poll_workflow_task_queue");
        let edge_resp = self
            .inner
            .poll_workflow_task_queue(&headers, edge_req)
            .await?;

        let has_task = edge_resp.is_some();
        debug!(has_task, "poll_workflow_task_queue response");
        Ok(Response::new(match edge_resp {
            Some(resp) => translate::poll_response_to_proto(resp),
            None => workflowservice::PollWorkflowTaskQueueResponse::default(),
        }))
    }

    async fn respond_workflow_task_completed(
        &self,
        request: Request<workflowservice::RespondWorkflowTaskCompletedRequest>,
    ) -> Result<Response<workflowservice::RespondWorkflowTaskCompletedResponse>, Status>
    {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::respond_completed_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        debug!(num_commands = edge_req.commands.len(), "respond_workflow_task_completed");
        let edge_resp = self
            .inner
            .respond_workflow_task_completed(&headers, edge_req)
            .await?;
        debug!("respond_workflow_task_completed success");
        Ok(Response::new(translate::completed_response_to_proto(
            edge_resp,
        )))
    }

    async fn describe_workflow_execution(
        &self,
        request: Request<workflowservice::DescribeWorkflowExecutionRequest>,
    ) -> Result<Response<workflowservice::DescribeWorkflowExecutionResponse>, Status>
    {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::describe_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        debug!(workflow_id = %edge_req.workflow_id, "describe_workflow_execution");
        let edge_resp = self
            .inner
            .describe_workflow_execution(&headers, edge_req)
            .await;
        match edge_resp {
            Ok(resp) => {
                debug!("describe_workflow_execution success");
                Ok(Response::new(translate::describe_response_to_proto(resp)))
            }
            Err(e) => {
                debug!(error = %e, "describe_workflow_execution failed");
                Err(e.into())
            }
        }
    }

    async fn list_workflow_executions(
        &self,
        request: Request<workflowservice::ListWorkflowExecutionsRequest>,
    ) -> Result<Response<workflowservice::ListWorkflowExecutionsResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::list_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .list_workflow_executions(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::list_response_to_proto(edge_resp)))
    }

    async fn count_workflow_executions(
        &self,
        request: Request<workflowservice::CountWorkflowExecutionsRequest>,
    ) -> Result<Response<workflowservice::CountWorkflowExecutionsResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::count_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .count_workflow_executions(&headers, edge_req)
            .await?;
        Ok(Response::new(translate::count_response_to_proto(edge_resp)))
    }

    async fn poll_activity_task_queue(
        &self,
        request: Request<workflowservice::PollActivityTaskQueueRequest>,
    ) -> Result<
        Response<workflowservice::PollActivityTaskQueueResponse>,
        Status,
    > {
        let headers =
            metadata_to_header_map(request.metadata());
        let edge_req =
            translate::poll_activity_request_to_edge(
                request.into_inner(),
            )
            .map_err(proto_conversion_status)?;
        debug!(task_queue = %edge_req.task_queue, "poll_activity_task_queue");
        let edge_resp = self
            .inner
            .poll_activity_task_queue(&headers, edge_req)
            .await?;

        let has_task = edge_resp.is_some();
        debug!(has_task, "poll_activity_task_queue response");
        Ok(Response::new(match edge_resp {
            Some(resp) => {
                translate::poll_activity_response_to_proto(
                    resp,
                )
            }
            None => {
                workflowservice::PollActivityTaskQueueResponse::default()
            }
        }))
    }

    async fn respond_activity_task_completed(
        &self,
        request: Request<workflowservice::RespondActivityTaskCompletedRequest>,
    ) -> Result<
        Response<workflowservice::RespondActivityTaskCompletedResponse>,
        Status,
    > {
        let headers =
            metadata_to_header_map(request.metadata());
        let edge_req =
            translate::respond_activity_completed_to_edge(
                request.into_inner(),
            )
            .map_err(proto_conversion_status)?;
        debug!("respond_activity_task_completed");
        let _edge_resp = self
            .inner
            .respond_activity_task_completed(
                &headers, edge_req,
            )
            .await?;
        debug!("respond_activity_task_completed success");
        Ok(Response::new(
            translate::respond_activity_completed_to_proto(),
        ))
    }

    async fn respond_activity_task_failed(
        &self,
        request: Request<workflowservice::RespondActivityTaskFailedRequest>,
    ) -> Result<
        Response<workflowservice::RespondActivityTaskFailedResponse>,
        Status,
    > {
        let headers =
            metadata_to_header_map(request.metadata());
        let edge_req =
            translate::respond_activity_failed_to_edge(
                request.into_inner(),
            )
            .map_err(proto_conversion_status)?;
        let _edge_resp = self
            .inner
            .respond_activity_task_failed(
                &headers, edge_req,
            )
            .await?;
        Ok(Response::new(
            translate::respond_activity_failed_to_proto(),
        ))
    }

    async fn record_activity_task_heartbeat(
        &self,
        request: Request<workflowservice::RecordActivityTaskHeartbeatRequest>,
    ) -> Result<
        Response<workflowservice::RecordActivityTaskHeartbeatResponse>,
        Status,
    > {
        let headers =
            metadata_to_header_map(request.metadata());
        let edge_req =
            translate::record_heartbeat_to_edge(
                request.into_inner(),
            )
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .record_activity_task_heartbeat(
                &headers, edge_req,
            )
            .await?;
        Ok(Response::new(
            translate::record_heartbeat_to_proto(edge_resp),
        ))
    }

    async fn terminate_workflow_execution(
        &self,
        request: Request<workflowservice::TerminateWorkflowExecutionRequest>,
    ) -> Result<
        Response<workflowservice::TerminateWorkflowExecutionResponse>,
        Status,
    > {
        let headers =
            metadata_to_header_map(request.metadata());
        let edge_req =
            translate::terminate_request_to_edge(
                request.into_inner(),
            )
            .map_err(proto_conversion_status)?;
        let _edge_resp = self
            .inner
            .terminate_workflow_execution(
                &headers, edge_req,
            )
            .await?;
        Ok(Response::new(
            translate::terminate_response_to_proto(),
        ))
    }

    async fn request_cancel_workflow_execution(
        &self,
        request: Request<workflowservice::RequestCancelWorkflowExecutionRequest>,
    ) -> Result<
        Response<workflowservice::RequestCancelWorkflowExecutionResponse>,
        Status,
    > {
        let headers =
            metadata_to_header_map(request.metadata());
        let edge_req =
            translate::cancel_request_to_edge(
                request.into_inner(),
            )
            .map_err(proto_conversion_status)?;
        let _edge_resp = self
            .inner
            .request_cancel_workflow_execution(
                &headers, edge_req,
            )
            .await?;
        Ok(Response::new(
            translate::cancel_response_to_proto(),
        ))
    }

    async fn query_workflow(
        &self,
        request: Request<workflowservice::QueryWorkflowRequest>,
    ) -> Result<
        Response<workflowservice::QueryWorkflowResponse>,
        Status,
    > {
        let headers =
            metadata_to_header_map(request.metadata());
        let edge_req =
            translate::query_request_to_edge(
                request.into_inner(),
            )
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .query_workflow(&headers, edge_req)
            .await?;
        Ok(Response::new(
            translate::query_response_to_proto(edge_resp),
        ))
    }

    async fn update_workflow_execution(
        &self,
        request: Request<workflowservice::UpdateWorkflowExecutionRequest>,
    ) -> Result<
        Response<workflowservice::UpdateWorkflowExecutionResponse>,
        Status,
    > {
        let headers =
            metadata_to_header_map(request.metadata());
        let edge_req =
            translate::update_request_to_edge(
                request.into_inner(),
            )
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .update_workflow_execution(&headers, edge_req)
            .await?;
        Ok(Response::new(
            translate::update_response_to_proto(edge_resp),
        ))
    }

    async fn get_workflow_execution_history(
        &self,
        request: Request<
            workflowservice::GetWorkflowExecutionHistoryRequest,
        >,
    ) -> Result<
        Response<
            workflowservice::GetWorkflowExecutionHistoryResponse,
        >,
        Status,
    > {
        let headers =
            metadata_to_header_map(request.metadata());
        let edge_req =
            translate::get_history_request_to_edge(
                request.into_inner(),
            )
            .map_err(proto_conversion_status)?;
        let filter_type = edge_req.history_event_filter_type;
        debug!(
            workflow_id = %edge_req.workflow_id,
            filter_type,
            wait_new_event = edge_req.wait_new_event,
            "get_workflow_execution_history"
        );
        let edge_resp = self
            .inner
            .get_workflow_execution_history(
                &headers, edge_req,
            )
            .await?;
        let num_events = edge_resp.history.len();
        let resp = translate::get_history_response_to_proto(
            edge_resp,
            filter_type,
        );
        let filtered_events = resp.history.as_ref().map(|h| h.events.len()).unwrap_or(0);
        debug!(num_events, filtered_events, "get_workflow_execution_history response");
        Ok(Response::new(resp))
    }

    async fn register_namespace(&self, request: Request<workflowservice::RegisterNamespaceRequest>) -> Result<Response<workflowservice::RegisterNamespaceResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::register_namespace_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        self.inner.register_namespace(&headers, edge_req).await?;
        Ok(Response::new(workflowservice::RegisterNamespaceResponse {}))
    }
    async fn describe_namespace(&self, request: Request<workflowservice::DescribeNamespaceRequest>) -> Result<Response<workflowservice::DescribeNamespaceResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        let namespace = if !req.namespace.is_empty() {
            req.namespace
        } else if !req.id.is_empty() {
            req.id
        } else {
            return Err(Status::invalid_argument("namespace or id is required"));
        };
        let edge_resp = self
            .inner
            .describe_namespace(&headers, &namespace)
            .await?;
        Ok(Response::new(translate::namespace_to_proto(edge_resp)))
    }
    async fn list_namespaces(&self, request: Request<workflowservice::ListNamespacesRequest>) -> Result<Response<workflowservice::ListNamespacesResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_resp = self.inner.list_namespaces(&headers).await?;
        Ok(Response::new(translate::list_namespaces_to_proto(edge_resp)))
    }
    async fn update_namespace(&self, _request: Request<workflowservice::UpdateNamespaceRequest>) -> Result<Response<workflowservice::UpdateNamespaceResponse>, Status> {
        Err(Status::unimplemented("update_namespace"))
    }
    async fn deprecate_namespace(&self, _request: Request<workflowservice::DeprecateNamespaceRequest>) -> Result<Response<workflowservice::DeprecateNamespaceResponse>, Status> {
        Err(Status::unimplemented("deprecate_namespace"))
    }
    async fn execute_multi_operation(&self, _request: Request<workflowservice::ExecuteMultiOperationRequest>) -> Result<Response<workflowservice::ExecuteMultiOperationResponse>, Status> {
        Err(Status::unimplemented("execute_multi_operation"))
    }
    async fn get_workflow_execution_history_reverse(&self, request: Request<workflowservice::GetWorkflowExecutionHistoryReverseRequest>) -> Result<Response<workflowservice::GetWorkflowExecutionHistoryReverseResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::get_history_reverse_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .get_workflow_execution_history_reverse(&headers, edge_req)
            .await?;
        Ok(Response::new(
            translate::get_history_reverse_response_to_proto(edge_resp),
        ))
    }
    async fn respond_workflow_task_failed(&self, _request: Request<workflowservice::RespondWorkflowTaskFailedRequest>) -> Result<Response<workflowservice::RespondWorkflowTaskFailedResponse>, Status> {
        Err(Status::unimplemented("respond_workflow_task_failed"))
    }
    async fn record_activity_task_heartbeat_by_id(&self, _request: Request<workflowservice::RecordActivityTaskHeartbeatByIdRequest>) -> Result<Response<workflowservice::RecordActivityTaskHeartbeatByIdResponse>, Status> {
        Err(Status::unimplemented("record_activity_task_heartbeat_by_id"))
    }
    async fn respond_activity_task_completed_by_id(&self, _request: Request<workflowservice::RespondActivityTaskCompletedByIdRequest>) -> Result<Response<workflowservice::RespondActivityTaskCompletedByIdResponse>, Status> {
        Err(Status::unimplemented("respond_activity_task_completed_by_id"))
    }
    async fn respond_activity_task_failed_by_id(&self, _request: Request<workflowservice::RespondActivityTaskFailedByIdRequest>) -> Result<Response<workflowservice::RespondActivityTaskFailedByIdResponse>, Status> {
        Err(Status::unimplemented("respond_activity_task_failed_by_id"))
    }
    async fn respond_activity_task_canceled(&self, _request: Request<workflowservice::RespondActivityTaskCanceledRequest>) -> Result<Response<workflowservice::RespondActivityTaskCanceledResponse>, Status> {
        Err(Status::unimplemented("respond_activity_task_canceled"))
    }
    async fn respond_activity_task_canceled_by_id(&self, _request: Request<workflowservice::RespondActivityTaskCanceledByIdRequest>) -> Result<Response<workflowservice::RespondActivityTaskCanceledByIdResponse>, Status> {
        Err(Status::unimplemented("respond_activity_task_canceled_by_id"))
    }
    async fn signal_with_start_workflow_execution(&self, request: Request<workflowservice::SignalWithStartWorkflowExecutionRequest>) -> Result<Response<workflowservice::SignalWithStartWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::signal_with_start_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .signal_with_start_workflow_execution(&headers, edge_req)
            .await?;
        Ok(Response::new(
            translate::signal_with_start_response_to_proto(edge_resp),
        ))
    }
    async fn reset_workflow_execution(&self, request: Request<workflowservice::ResetWorkflowExecutionRequest>) -> Result<Response<workflowservice::ResetWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::reset_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self
            .inner
            .reset_workflow_execution(&headers, edge_req)
            .await?;
        Ok(Response::new(
            translate::reset_response_to_proto(edge_resp),
        ))
    }
    async fn delete_workflow_execution(&self, request: Request<workflowservice::DeleteWorkflowExecutionRequest>) -> Result<Response<workflowservice::DeleteWorkflowExecutionResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::delete_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        self.inner.delete_workflow_execution(&headers, edge_req).await?;
        Ok(Response::new(
            workflowservice::DeleteWorkflowExecutionResponse {},
        ))
    }
    async fn list_open_workflow_executions(&self, _request: Request<workflowservice::ListOpenWorkflowExecutionsRequest>) -> Result<Response<workflowservice::ListOpenWorkflowExecutionsResponse>, Status> {
        Err(Status::unimplemented("list_open_workflow_executions"))
    }
    async fn list_closed_workflow_executions(&self, _request: Request<workflowservice::ListClosedWorkflowExecutionsRequest>) -> Result<Response<workflowservice::ListClosedWorkflowExecutionsResponse>, Status> {
        Err(Status::unimplemented("list_closed_workflow_executions"))
    }
    async fn list_archived_workflow_executions(&self, _request: Request<workflowservice::ListArchivedWorkflowExecutionsRequest>) -> Result<Response<workflowservice::ListArchivedWorkflowExecutionsResponse>, Status> {
        Err(Status::unimplemented("list_archived_workflow_executions"))
    }
    async fn scan_workflow_executions(&self, _request: Request<workflowservice::ScanWorkflowExecutionsRequest>) -> Result<Response<workflowservice::ScanWorkflowExecutionsResponse>, Status> {
        Err(Status::unimplemented("scan_workflow_executions"))
    }
    async fn get_search_attributes(&self, _request: Request<workflowservice::GetSearchAttributesRequest>) -> Result<Response<workflowservice::GetSearchAttributesResponse>, Status> {
        Err(Status::unimplemented("get_search_attributes"))
    }
    async fn respond_query_task_completed(&self, _request: Request<workflowservice::RespondQueryTaskCompletedRequest>) -> Result<Response<workflowservice::RespondQueryTaskCompletedResponse>, Status> {
        Err(Status::unimplemented("respond_query_task_completed"))
    }
    async fn reset_sticky_task_queue(&self, _request: Request<workflowservice::ResetStickyTaskQueueRequest>) -> Result<Response<workflowservice::ResetStickyTaskQueueResponse>, Status> {
        Err(Status::unimplemented("reset_sticky_task_queue"))
    }
    async fn shutdown_worker(&self, _request: Request<workflowservice::ShutdownWorkerRequest>) -> Result<Response<workflowservice::ShutdownWorkerResponse>, Status> {
        Err(Status::unimplemented("shutdown_worker"))
    }
    async fn describe_task_queue(&self, request: Request<workflowservice::DescribeTaskQueueRequest>) -> Result<Response<workflowservice::DescribeTaskQueueResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_req = translate::describe_task_queue_request_to_edge(request.into_inner())
            .map_err(proto_conversion_status)?;
        let edge_resp = self.inner.describe_task_queue(&headers, edge_req).await?;
        Ok(Response::new(
            translate::describe_task_queue_response_to_proto(edge_resp),
        ))
    }
    async fn get_cluster_info(&self, request: Request<workflowservice::GetClusterInfoRequest>) -> Result<Response<workflowservice::GetClusterInfoResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_resp = self.inner.get_cluster_info(&headers).await?;
        Ok(Response::new(translate::cluster_info_to_proto(edge_resp)))
    }
    async fn get_system_info(&self, request: Request<workflowservice::GetSystemInfoRequest>) -> Result<Response<workflowservice::GetSystemInfoResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let edge_resp = self.inner.get_system_info(&headers).await?;
        Ok(Response::new(translate::system_info_to_proto(edge_resp)))
    }
    async fn list_task_queue_partitions(&self, _request: Request<workflowservice::ListTaskQueuePartitionsRequest>) -> Result<Response<workflowservice::ListTaskQueuePartitionsResponse>, Status> {
        Err(Status::unimplemented("list_task_queue_partitions"))
    }
    async fn create_schedule(&self, _request: Request<workflowservice::CreateScheduleRequest>) -> Result<Response<workflowservice::CreateScheduleResponse>, Status> {
        Err(Status::unimplemented("create_schedule"))
    }
    async fn describe_schedule(&self, _request: Request<workflowservice::DescribeScheduleRequest>) -> Result<Response<workflowservice::DescribeScheduleResponse>, Status> {
        Err(Status::unimplemented("describe_schedule"))
    }
    async fn update_schedule(&self, _request: Request<workflowservice::UpdateScheduleRequest>) -> Result<Response<workflowservice::UpdateScheduleResponse>, Status> {
        Err(Status::unimplemented("update_schedule"))
    }
    async fn patch_schedule(&self, _request: Request<workflowservice::PatchScheduleRequest>) -> Result<Response<workflowservice::PatchScheduleResponse>, Status> {
        Err(Status::unimplemented("patch_schedule"))
    }
    async fn list_schedule_matching_times(&self, _request: Request<workflowservice::ListScheduleMatchingTimesRequest>) -> Result<Response<workflowservice::ListScheduleMatchingTimesResponse>, Status> {
        Err(Status::unimplemented("list_schedule_matching_times"))
    }
    async fn delete_schedule(&self, _request: Request<workflowservice::DeleteScheduleRequest>) -> Result<Response<workflowservice::DeleteScheduleResponse>, Status> {
        Err(Status::unimplemented("delete_schedule"))
    }
    async fn list_schedules(&self, _request: Request<workflowservice::ListSchedulesRequest>) -> Result<Response<workflowservice::ListSchedulesResponse>, Status> {
        Err(Status::unimplemented("list_schedules"))
    }
    async fn update_worker_build_id_compatibility(&self, _request: Request<workflowservice::UpdateWorkerBuildIdCompatibilityRequest>) -> Result<Response<workflowservice::UpdateWorkerBuildIdCompatibilityResponse>, Status> {
        Err(Status::unimplemented("update_worker_build_id_compatibility"))
    }
    async fn get_worker_build_id_compatibility(&self, _request: Request<workflowservice::GetWorkerBuildIdCompatibilityRequest>) -> Result<Response<workflowservice::GetWorkerBuildIdCompatibilityResponse>, Status> {
        Err(Status::unimplemented("get_worker_build_id_compatibility"))
    }
    async fn update_worker_versioning_rules(&self, _request: Request<workflowservice::UpdateWorkerVersioningRulesRequest>) -> Result<Response<workflowservice::UpdateWorkerVersioningRulesResponse>, Status> {
        Err(Status::unimplemented("update_worker_versioning_rules"))
    }
    async fn get_worker_versioning_rules(&self, _request: Request<workflowservice::GetWorkerVersioningRulesRequest>) -> Result<Response<workflowservice::GetWorkerVersioningRulesResponse>, Status> {
        Err(Status::unimplemented("get_worker_versioning_rules"))
    }
    async fn get_worker_task_reachability(&self, _request: Request<workflowservice::GetWorkerTaskReachabilityRequest>) -> Result<Response<workflowservice::GetWorkerTaskReachabilityResponse>, Status> {
        Err(Status::unimplemented("get_worker_task_reachability"))
    }
    async fn describe_deployment(&self, _request: Request<workflowservice::DescribeDeploymentRequest>) -> Result<Response<workflowservice::DescribeDeploymentResponse>, Status> {
        Err(Status::unimplemented("describe_deployment"))
    }
    async fn list_deployments(&self, _request: Request<workflowservice::ListDeploymentsRequest>) -> Result<Response<workflowservice::ListDeploymentsResponse>, Status> {
        Err(Status::unimplemented("list_deployments"))
    }
    async fn get_deployment_reachability(&self, _request: Request<workflowservice::GetDeploymentReachabilityRequest>) -> Result<Response<workflowservice::GetDeploymentReachabilityResponse>, Status> {
        Err(Status::unimplemented("get_deployment_reachability"))
    }
    async fn get_current_deployment(&self, _request: Request<workflowservice::GetCurrentDeploymentRequest>) -> Result<Response<workflowservice::GetCurrentDeploymentResponse>, Status> {
        Err(Status::unimplemented("get_current_deployment"))
    }
    async fn set_current_deployment(&self, _request: Request<workflowservice::SetCurrentDeploymentRequest>) -> Result<Response<workflowservice::SetCurrentDeploymentResponse>, Status> {
        Err(Status::unimplemented("set_current_deployment"))
    }
    async fn poll_workflow_execution_update(&self, _request: Request<workflowservice::PollWorkflowExecutionUpdateRequest>) -> Result<Response<workflowservice::PollWorkflowExecutionUpdateResponse>, Status> {
        Err(Status::unimplemented("poll_workflow_execution_update"))
    }
    async fn start_batch_operation(&self, _request: Request<workflowservice::StartBatchOperationRequest>) -> Result<Response<workflowservice::StartBatchOperationResponse>, Status> {
        Err(Status::unimplemented("start_batch_operation"))
    }
    async fn stop_batch_operation(&self, _request: Request<workflowservice::StopBatchOperationRequest>) -> Result<Response<workflowservice::StopBatchOperationResponse>, Status> {
        Err(Status::unimplemented("stop_batch_operation"))
    }
    async fn describe_batch_operation(&self, _request: Request<workflowservice::DescribeBatchOperationRequest>) -> Result<Response<workflowservice::DescribeBatchOperationResponse>, Status> {
        Err(Status::unimplemented("describe_batch_operation"))
    }
    async fn list_batch_operations(&self, _request: Request<workflowservice::ListBatchOperationsRequest>) -> Result<Response<workflowservice::ListBatchOperationsResponse>, Status> {
        Err(Status::unimplemented("list_batch_operations"))
    }
    async fn poll_nexus_task_queue(&self, _request: Request<workflowservice::PollNexusTaskQueueRequest>) -> Result<Response<workflowservice::PollNexusTaskQueueResponse>, Status> {
        Err(Status::unimplemented("poll_nexus_task_queue"))
    }
    async fn respond_nexus_task_completed(&self, _request: Request<workflowservice::RespondNexusTaskCompletedRequest>) -> Result<Response<workflowservice::RespondNexusTaskCompletedResponse>, Status> {
        Err(Status::unimplemented("respond_nexus_task_completed"))
    }
    async fn respond_nexus_task_failed(&self, _request: Request<workflowservice::RespondNexusTaskFailedRequest>) -> Result<Response<workflowservice::RespondNexusTaskFailedResponse>, Status> {
        Err(Status::unimplemented("respond_nexus_task_failed"))
    }
    async fn update_activity_options_by_id(&self, _request: Request<workflowservice::UpdateActivityOptionsByIdRequest>) -> Result<Response<workflowservice::UpdateActivityOptionsByIdResponse>, Status> {
        Err(Status::unimplemented("update_activity_options_by_id"))
    }
    async fn update_workflow_execution_options(&self, _request: Request<workflowservice::UpdateWorkflowExecutionOptionsRequest>) -> Result<Response<workflowservice::UpdateWorkflowExecutionOptionsResponse>, Status> {
        Err(Status::unimplemented("update_workflow_execution_options"))
    }
    async fn pause_activity_by_id(&self, _request: Request<workflowservice::PauseActivityByIdRequest>) -> Result<Response<workflowservice::PauseActivityByIdResponse>, Status> {
        Err(Status::unimplemented("pause_activity_by_id"))
    }
    async fn unpause_activity_by_id(&self, _request: Request<workflowservice::UnpauseActivityByIdRequest>) -> Result<Response<workflowservice::UnpauseActivityByIdResponse>, Status> {
        Err(Status::unimplemented("unpause_activity_by_id"))
    }
    async fn reset_activity_by_id(&self, _request: Request<workflowservice::ResetActivityByIdRequest>) -> Result<Response<workflowservice::ResetActivityByIdResponse>, Status> {
        Err(Status::unimplemented("reset_activity_by_id"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use async_trait::async_trait;
    use tokio::sync::Notify;
    use time::OffsetDateTime;
    use tonic::Request;
    use uuid::Uuid;

    use super::*;
    use crate::{
        history_wait::{HistoryNotifyingRepository, HistoryWaitRegistry},
        interceptors::EdgeInterceptors,
        long_poll::{LongPollConfig, LongPollGate},
        namespace_cache::{InMemoryNamespaceCache, NamespaceCache, ResolvedNamespace},
        operator_service::InMemoryOperatorApi,
        poller_registry::PollerRegistry,
        routing::LocalOnlyRouter,
        translate::to_internal::namespace_id_for,
        workflow_service::{
            EmptyVisibilityApi, ExecutionResolver, WorkflowMutationOutcome,
            WorkflowRuntimeApi,
        },
    };
    use tokeira_kernel::{
        BasicKernel, Command, Kernel, LoadedRun, SignalRequest, StartRequest,
    };
    use tokeira_storage::{CommitResult, RunRepository};
    use tokeira_types::{
        Memo, Payloads, RequestContext, RequestId, RunId, RunKey,
        SearchAttributes, ShardEpoch, TaskQueueName, WorkflowId, WorkflowType,
    };

    struct PollNoneRuntime;

    struct BlockingPollRuntime {
        ready: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl WorkflowRuntimeApi for PollNoneRuntime {
        async fn start_workflow(
            &self,
            _req: tokeira_kernel::StartRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn start_workflow_with_policy(
            &self,
            _req: tokeira_kernel::StartRequest,
        ) -> Result<tokeira_runtime::StartWorkflowResult> {
            unreachable!()
        }

        async fn signal_with_start_workflow(
            &self,
            _req: tokeira_kernel::SignalWithStartRequest,
        ) -> Result<tokeira_runtime::SignalWithStartResult> {
            unreachable!()
        }

        async fn signal_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::SignalRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn poll_workflow_task(
            &self,
            _queue: tokeira_types::QueueKey,
            _worker_identity: tokeira_types::WorkerIdentity,
            _timeout: std::time::Duration,
        ) -> Result<Option<tokeira_runtime::StartedWorkflowTask>> {
            Ok(None)
        }

        async fn complete_workflow_task(
            &self,
            _req: tokeira_kernel::WorkflowTaskCompletedRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn poll_activity_task(
            &self,
            _queue: tokeira_types::QueueKey,
            _worker_identity: tokeira_types::WorkerIdentity,
            _timeout: std::time::Duration,
        ) -> Result<Option<tokeira_runtime::StartedActivityTask>> {
            Ok(None)
        }

        async fn complete_activity_task(
            &self,
            _token: tokeira_types::ActivityTaskToken,
            _result: tokeira_types::Payloads,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn fail_activity_task(
            &self,
            _token: tokeira_types::ActivityTaskToken,
            _failure_message: String,
            _failure_error_type: Option<String>,
        ) -> Result<()> {
            unreachable!()
        }

        async fn record_activity_heartbeat(
            &self,
            _token: tokeira_types::ActivityTaskToken,
        ) -> Result<bool> {
            unreachable!()
        }

        async fn terminate_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::TerminateRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn cancel_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::CancelRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn reset_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _req: tokeira_kernel::ResetRequest,
        ) -> Result<tokeira_runtime::ResetWorkflowResult> {
            unreachable!()
        }

        async fn query_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _query_type: String,
            _query_args: tokeira_types::Payloads,
            _timeout: std::time::Duration,
        ) -> Result<tokeira_runtime::QueryResult> {
            unreachable!()
        }

        async fn update_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _update_id: String,
            _update_name: String,
            _input: tokeira_types::Payloads,
            _request: tokeira_types::RequestContext,
            _timeout: std::time::Duration,
            _wait_policy: tokeira_runtime::UpdateWaitPolicy,
        ) -> Result<tokeira_runtime::UpdateOutcome> {
            unreachable!()
        }
    }

    #[async_trait]
    impl WorkflowRuntimeApi for BlockingPollRuntime {
        async fn start_workflow(
            &self,
            _req: tokeira_kernel::StartRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn start_workflow_with_policy(
            &self,
            _req: tokeira_kernel::StartRequest,
        ) -> Result<tokeira_runtime::StartWorkflowResult> {
            unreachable!()
        }

        async fn signal_with_start_workflow(
            &self,
            _req: tokeira_kernel::SignalWithStartRequest,
        ) -> Result<tokeira_runtime::SignalWithStartResult> {
            unreachable!()
        }

        async fn signal_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::SignalRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn poll_workflow_task(
            &self,
            _queue: tokeira_types::QueueKey,
            _worker_identity: tokeira_types::WorkerIdentity,
            _timeout: std::time::Duration,
        ) -> Result<Option<tokeira_runtime::StartedWorkflowTask>> {
            self.ready.notify_waiters();
            self.release.notified().await;
            Ok(None)
        }

        async fn complete_workflow_task(
            &self,
            _req: tokeira_kernel::WorkflowTaskCompletedRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn poll_activity_task(
            &self,
            _queue: tokeira_types::QueueKey,
            _worker_identity: tokeira_types::WorkerIdentity,
            _timeout: std::time::Duration,
        ) -> Result<Option<tokeira_runtime::StartedActivityTask>> {
            unreachable!()
        }

        async fn complete_activity_task(
            &self,
            _token: tokeira_types::ActivityTaskToken,
            _result: tokeira_types::Payloads,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn fail_activity_task(
            &self,
            _token: tokeira_types::ActivityTaskToken,
            _failure_message: String,
            _failure_error_type: Option<String>,
        ) -> Result<()> {
            unreachable!()
        }

        async fn record_activity_heartbeat(
            &self,
            _token: tokeira_types::ActivityTaskToken,
        ) -> Result<bool> {
            unreachable!()
        }

        async fn terminate_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::TerminateRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn cancel_workflow(
            &self,
            _run_key: tokeira_types::RunKey,
            _req: tokeira_kernel::CancelRequest,
        ) -> Result<WorkflowMutationOutcome> {
            unreachable!()
        }

        async fn reset_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _req: tokeira_kernel::ResetRequest,
        ) -> Result<tokeira_runtime::ResetWorkflowResult> {
            unreachable!()
        }

        async fn query_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _query_type: String,
            _query_args: tokeira_types::Payloads,
            _timeout: std::time::Duration,
        ) -> Result<tokeira_runtime::QueryResult> {
            unreachable!()
        }

        async fn update_workflow(
            &self,
            _execution: tokeira_types::ExecutionRef,
            _update_id: String,
            _update_name: String,
            _input: tokeira_types::Payloads,
            _request: tokeira_types::RequestContext,
            _timeout: std::time::Duration,
            _wait_policy: tokeira_runtime::UpdateWaitPolicy,
        ) -> Result<tokeira_runtime::UpdateOutcome> {
            unreachable!()
        }
    }

    #[derive(Default)]
    struct NoopResolver;

    #[async_trait]
    impl ExecutionResolver for NoopResolver {
        async fn current_run_key(
            &self,
            _namespace: &str,
            _workflow_id: &str,
        ) -> Result<Option<tokeira_types::RunKey>> {
            Ok(None)
        }

        async fn describe_execution(
            &self,
            _namespace: &str,
            _workflow_id: &str,
        ) -> Result<Option<crate::WorkflowExecutionDescription>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn poll_none_returns_default_proto_response() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();
        let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));

        let service = WorkflowService::new(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(
                tokeira_storage::InMemoryStore::default(),
            ),
            operator_api,
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
        );
        let grpc = WorkflowServiceGrpc::new(service);

        let response = grpc
            .poll_workflow_task_queue(Request::new(
                workflowservice::PollWorkflowTaskQueueRequest {
                    namespace: "default".to_string(),
                    task_queue: Some(tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                        name: "queue".to_string(),
                        ..Default::default()
                    }),
                    identity: "worker".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect("poll should succeed")
            .into_inner();

        assert_eq!(
            response,
            workflowservice::PollWorkflowTaskQueueResponse::default()
        );
    }

    #[tokio::test]
    async fn activity_poll_none_returns_default_response() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();
        let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));

        let service = WorkflowService::new(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(
                tokeira_storage::InMemoryStore::default(),
            ),
            operator_api,
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
        );
        let grpc = WorkflowServiceGrpc::new(service);

        let response = grpc
            .poll_activity_task_queue(Request::new(
                workflowservice::PollActivityTaskQueueRequest {
                    namespace: "default".to_string(),
                    task_queue: Some(
                        tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                            name: "queue".to_string(),
                            ..Default::default()
                        },
                    ),
                    identity: "worker".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect("poll should succeed")
            .into_inner();

        assert_eq!(
            response,
            workflowservice::PollActivityTaskQueueResponse::default()
        );
    }

    #[tokio::test]
    async fn activity_poll_shares_long_poll_gate() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();
        let operator_api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));

        // Create a gate with max_concurrent = 1 and
        // short admission timeout
        let gate = LongPollGate::new(LongPollConfig {
            max_concurrent: 1,
            acquire_timeout: std::time::Duration::from_millis(10),
        });

        // Directly acquire the single permit to
        // simulate a workflow poll holding it
        let permit = gate.acquire().await.unwrap();

        let service = WorkflowService::new(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(
                tokeira_storage::InMemoryStore::default(),
            ),
            operator_api,
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(
                cache,
            )),
            PollerRegistry::default(),
            gate,
            Arc::new(LocalOnlyRouter),
        );

        // Activity poll should be rejected because
        // the gate is exhausted
        let headers = http::HeaderMap::new();
        let req =
            crate::translate::PollActivityTaskQueueRequest {
                namespace: "default".to_string(),
                task_queue: "queue".to_string(),
                worker_identity: "w2".to_string(),
                timeout: std::time::Duration::from_millis(
                    50,
                ),
            };
        let result = service
            .poll_activity_task_queue(&headers, req)
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        let status: tonic::Status = err.into();
        assert_eq!(
            status.code(),
            tonic::Code::DeadlineExceeded
        );

        // Release the permit
        drop(permit);
    }

    #[tokio::test]
    async fn describe_task_queue_lists_active_pollers() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();
        let ready = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let service = WorkflowService::new(
            Arc::new(BlockingPollRuntime {
                ready: ready.clone(),
                release: release.clone(),
            }),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(tokeira_storage::InMemoryStore::default()),
            Arc::new(InMemoryOperatorApi::new("tokeira-local")),
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
        );
        let grpc = WorkflowServiceGrpc::new(service);

        let poll = tokio::spawn({
            let grpc = grpc.clone();
            async move {
                grpc.poll_workflow_task_queue(Request::new(
                    workflowservice::PollWorkflowTaskQueueRequest {
                        namespace: "default".to_string(),
                        task_queue: Some(
                            tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                                name: "queue".to_string(),
                                ..Default::default()
                            },
                        ),
                        identity: "worker-1".to_string(),
                        ..Default::default()
                    },
                ))
                .await
            }
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), ready.notified())
            .await
            .expect("poll should register before describe");

        let describe = grpc
            .describe_task_queue(Request::new(
                workflowservice::DescribeTaskQueueRequest {
                    namespace: "default".to_string(),
                    task_queue: Some(
                        tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueue {
                            name: "queue".to_string(),
                            ..Default::default()
                        },
                    ),
                    task_queue_type: tokeira_proto::enums::TaskQueueType::Workflow as i32,
                    include_task_queue_status: true,
                    ..Default::default()
                },
            ))
            .await
            .expect("describe should succeed")
            .into_inner();

        assert_eq!(describe.pollers.len(), 1);
        assert_eq!(describe.pollers[0].identity, "worker-1");
        assert!(describe.pollers[0].last_access_time.is_some());
        assert_eq!(
            describe
                .task_queue_status
                .as_ref()
                .map(|status| status.backlog_count_hint),
            Some(0)
        );

        release.notify_waiters();
        let response = poll.await.unwrap().unwrap().into_inner();
        assert_eq!(
            response,
            workflowservice::PollWorkflowTaskQueueResponse::default()
        );
    }

    #[tokio::test]
    async fn delete_workflow_execution_missing_returns_not_found() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();

        let service = WorkflowService::new(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(tokeira_storage::InMemoryStore::default()),
            Arc::new(InMemoryOperatorApi::new("tokeira-local")),
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
        );
        let grpc = WorkflowServiceGrpc::new(service);

        let error = grpc
            .delete_workflow_execution(Request::new(
                workflowservice::DeleteWorkflowExecutionRequest {
                    namespace: "default".to_string(),
                    workflow_execution: Some(
                        tokeira_proto::common::WorkflowExecution {
                            workflow_id: "missing".to_string(),
                            run_id: String::new(),
                        },
                    ),
                },
            ))
            .await
            .expect_err("missing workflow should fail");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn reset_workflow_execution_invalid_event_returns_invalid_argument() {
        let (grpc, _repo, _run_key, run_id) = history_test_service().await;

        let error = grpc
            .reset_workflow_execution(Request::new(
                workflowservice::ResetWorkflowExecutionRequest {
                    namespace: "default".to_string(),
                    workflow_execution: Some(
                        tokeira_proto::common::WorkflowExecution {
                            workflow_id: "wf".to_string(),
                            run_id: run_id.0.to_string(),
                        },
                    ),
                    reason: "operator reset".to_string(),
                    workflow_task_finish_event_id: 2,
                    request_id: "reset-1".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("invalid reset target should fail");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn reset_workflow_execution_missing_returns_not_found() {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();

        let service = WorkflowService::new(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            Arc::new(tokeira_storage::InMemoryStore::default()),
            Arc::new(InMemoryOperatorApi::new("tokeira-local")),
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
        );
        let grpc = WorkflowServiceGrpc::new(service);

        let error = grpc
            .reset_workflow_execution(Request::new(
                workflowservice::ResetWorkflowExecutionRequest {
                    namespace: "default".to_string(),
                    workflow_execution: Some(
                        tokeira_proto::common::WorkflowExecution {
                            workflow_id: "missing".to_string(),
                            run_id: String::new(),
                        },
                    ),
                    reason: "operator reset".to_string(),
                    workflow_task_finish_event_id: 1,
                    request_id: "reset-missing".to_string(),
                    ..Default::default()
                },
            ))
            .await
            .expect_err("missing workflow should fail");

        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    async fn history_test_service(
    ) -> (
        WorkflowServiceGrpc,
        Arc<HistoryNotifyingRepository<tokeira_storage::InMemoryStore>>,
        RunKey,
        RunId,
    ) {
        let cache = Arc::new(InMemoryNamespaceCache::new());
        cache
            .insert(ResolvedNamespace::active("default"))
            .await
            .unwrap();

        let waits = HistoryWaitRegistry::default();
        let store = Arc::new(tokeira_storage::InMemoryStore::default());
        let repo = Arc::new(HistoryNotifyingRepository::new(
            store,
            waits.clone(),
        ));

        let run_key = RunKey::new();
        let run_id = RunId(Uuid::new_v4());
        seed_started_run(repo.as_ref(), run_key, run_id).await;

        let service = WorkflowService::new_with_history_wait_registry(
            Arc::new(PollNoneRuntime),
            Arc::new(NoopResolver),
            Arc::new(EmptyVisibilityApi),
            repo.clone(),
            Arc::new(InMemoryOperatorApi::new("tokeira-local")),
            cache.clone(),
            Arc::new(EdgeInterceptors::permissive(cache)),
            PollerRegistry::default(),
            LongPollGate::new(LongPollConfig::default()),
            Arc::new(LocalOnlyRouter),
            waits,
        );
        (WorkflowServiceGrpc::new(service), repo, run_key, run_id)
    }

    async fn seed_started_run(
        repo: &HistoryNotifyingRepository<tokeira_storage::InMemoryStore>,
        run_key: RunKey,
        run_id: RunId,
    ) {
        let start = StartRequest {
            run_key,
            namespace_id: namespace_id_for("default"),
            workflow_id: WorkflowId("wf".to_string()),
            run_id,
            workflow_type: WorkflowType("wf-type".to_string()),
            task_queue: TaskQueueName("q".to_string()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: time::Duration::seconds(10),
            retry_policy: None,
            conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
            reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
            attempt: 1,
            continued_execution_run_id: None,
            first_execution_run_id: Some(run_id),
            parent_run_key: None,
            parent_workflow_id: None,
            first_run_started_at: None,
            request: RequestContext {
                request_id: RequestId("seed-start".to_string()),
                caller_identity: None,
                received_at: OffsetDateTime::now_utc(),
            },
            now: OffsetDateTime::now_utc(),
        };

        let transition = BasicKernel::default()
            .apply(LoadedRun::Absent, Command::Start(start))
            .expect("start transition");
        let result = repo
            .commit_transition(run_key, transition, ShardEpoch::ZERO)
            .await
            .expect("commit start");
        assert!(matches!(result, CommitResult::Applied { .. }));
    }

    async fn append_signal_event(
        repo: Arc<HistoryNotifyingRepository<tokeira_storage::InMemoryStore>>,
        run_key: RunKey,
    ) {
        let loaded = repo.load_run(run_key).await.expect("load run");
        let transition = BasicKernel::default()
            .apply(
                loaded,
                Command::Signal(SignalRequest {
                    signal_name: "sig".to_string(),
                    input: Payloads::default(),
                    request: RequestContext {
                        request_id: RequestId("sig-1".to_string()),
                        caller_identity: None,
                        received_at: OffsetDateTime::now_utc(),
                    },
                    now: OffsetDateTime::now_utc(),
                }),
            )
            .expect("signal transition");
        let result = repo
            .commit_transition(run_key, transition, ShardEpoch::ZERO)
            .await
            .expect("commit signal");
        assert!(matches!(result, CommitResult::Applied { .. }));
    }

    fn history_request(
        run_id: RunId,
        wait_new_event: bool,
        next_page_token: Vec<u8>,
    ) -> workflowservice::GetWorkflowExecutionHistoryRequest {
        workflowservice::GetWorkflowExecutionHistoryRequest {
            namespace: "default".to_string(),
            execution: Some(tokeira_proto::common::WorkflowExecution {
                workflow_id: "wf".to_string(),
                run_id: run_id.0.to_string(),
                ..Default::default()
            }),
            wait_new_event,
            history_event_filter_type: 1,
            next_page_token,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn history_immediate_return_when_wait_disabled() {
        let (grpc, _repo, _run_key, run_id) = history_test_service().await;

        let response = grpc
            .get_workflow_execution_history(Request::new(history_request(
                run_id,
                false,
                Vec::new(),
            )))
            .await
            .expect("history call should succeed")
            .into_inner();

        let history = response.history.expect("history");
        assert_eq!(history.events.len(), 2);
        assert_eq!(response.next_page_token, 2i64.to_be_bytes());
    }

    #[tokio::test]
    async fn history_long_poll_wakes_when_event_arrives() {
        let (grpc, repo, run_key, run_id) = history_test_service().await;
        let baseline = grpc
            .get_workflow_execution_history(Request::new(history_request(
                run_id,
                false,
                Vec::new(),
            )))
            .await
            .expect("baseline history call should succeed")
            .into_inner();

        let task = tokio::spawn({
            let grpc = grpc.clone();
            let next_page_token = baseline.next_page_token.clone();
            async move {
                grpc.get_workflow_execution_history(Request::new(history_request(
                    run_id,
                    true,
                    next_page_token,
                )))
                .await
                .expect("history call should succeed")
                .into_inner()
            }
        });

        tokio::task::yield_now().await;
        append_signal_event(repo, run_key).await;

        let response = task.await.expect("join");
        let history = response.history.expect("history");
        assert_eq!(history.events.len(), 1);
        assert_eq!(response.next_page_token, 3i64.to_be_bytes());
    }

    #[tokio::test(start_paused = true)]
    async fn history_long_poll_times_out_without_new_event() {
        let (grpc, _repo, _run_key, run_id) = history_test_service().await;
        let baseline = grpc
            .get_workflow_execution_history(Request::new(history_request(
                run_id,
                false,
                Vec::new(),
            )))
            .await
            .expect("baseline history call should succeed")
            .into_inner();

        let task = tokio::spawn({
            let grpc = grpc.clone();
            let next_page_token = baseline.next_page_token.clone();
            async move {
                grpc.get_workflow_execution_history(Request::new(history_request(
                    run_id,
                    true,
                    next_page_token,
                )))
                .await
                .expect("history call should succeed")
                .into_inner()
            }
        });

        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(61)).await;

        let response = task.await.expect("join");
        let history = response.history.expect("history");
        assert!(history.events.is_empty());
        assert_eq!(response.next_page_token, baseline.next_page_token);
    }

    #[tokio::test]
    async fn history_next_page_token_tracks_position_across_calls() {
        let (grpc, repo, run_key, run_id) = history_test_service().await;

        let first = grpc
            .get_workflow_execution_history(Request::new(history_request(
                run_id,
                false,
                Vec::new(),
            )))
            .await
            .expect("first history call should succeed")
            .into_inner();
        assert_eq!(first.next_page_token, 2i64.to_be_bytes());

        let second = grpc
            .get_workflow_execution_history(Request::new(history_request(
                run_id,
                false,
                first.next_page_token.clone(),
            )))
            .await
            .expect("second history call should succeed")
            .into_inner();
        let second_history = second.history.expect("history");
        assert!(second_history.events.is_empty());
        assert_eq!(second.next_page_token, 2i64.to_be_bytes());

        append_signal_event(repo, run_key).await;

        let third = grpc
            .get_workflow_execution_history(Request::new(history_request(
                run_id,
                false,
                second.next_page_token,
            )))
            .await
            .expect("third history call should succeed")
            .into_inner();
        let third_history = third.history.expect("history");
        assert_eq!(third_history.events.len(), 1);
        assert_eq!(third.next_page_token, 3i64.to_be_bytes());
    }

    #[tokio::test]
    async fn reverse_history_paginates_in_descending_event_order() {
        let (grpc, repo, run_key, run_id) = history_test_service().await;
        append_signal_event(repo, run_key).await;

        let first = grpc
            .get_workflow_execution_history_reverse(Request::new(
                workflowservice::GetWorkflowExecutionHistoryReverseRequest {
                    namespace: "default".to_string(),
                    execution: Some(tokeira_proto::common::WorkflowExecution {
                        workflow_id: "wf".to_string(),
                        run_id: run_id.0.to_string(),
                        ..Default::default()
                    }),
                    maximum_page_size: 1,
                    next_page_token: Vec::new(),
                },
            ))
            .await
            .expect("reverse history should succeed")
            .into_inner();
        let first_history = first.history.expect("history");
        assert_eq!(first_history.events.len(), 1);
        let newest_event_id = first_history.events[0].event_id;
        assert_eq!(first.next_page_token, newest_event_id.to_be_bytes());

        let second = grpc
            .get_workflow_execution_history_reverse(Request::new(
                workflowservice::GetWorkflowExecutionHistoryReverseRequest {
                    namespace: "default".to_string(),
                    execution: Some(tokeira_proto::common::WorkflowExecution {
                        workflow_id: "wf".to_string(),
                        run_id: run_id.0.to_string(),
                        ..Default::default()
                    }),
                    maximum_page_size: 10,
                    next_page_token: first.next_page_token,
                },
            ))
            .await
            .expect("reverse history second page should succeed")
            .into_inner();
        let second_history = second.history.expect("history");
        assert!(!second_history.events.is_empty());
        assert!(second_history
            .events
            .iter()
            .all(|event| event.event_id < newest_event_id));
        for pair in second_history.events.windows(2) {
            assert!(pair[0].event_id > pair[1].event_id);
        }
    }
}
