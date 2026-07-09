//! Minimal AdminService gRPC surface.
//!
//! tokeira serves only `DescribeMutableState`, which the reset conformance suite
//! (`TestWorkflowResetTestSuite`) uses to read a run's internal `ResetRunId` link
//! and raw status. The response populates only `ExecutionInfo.reset_run_id` and
//! `ExecutionState.status`; every other `WorkflowMutableState` field defaults (the
//! proto's field numbers match the real persistence proto, so the harness client
//! decodes it correctly). Every other AdminService RPC is `Unimplemented`.

use tonic::{Request, Response, Status, codec::CompressionEncoding};

use tokeira_proto::adminservice::{
    self, DescribeMutableStateResponse, WorkflowExecutionInfo, WorkflowExecutionState,
    WorkflowMutableState,
    admin_service_server::{AdminService as AdminServiceGrpcApi, AdminServiceServer},
};

use crate::{grpc::translate::execution_status_to_proto, workflow_service::WorkflowService};

#[derive(Clone)]
pub struct AdminServiceGrpc {
    inner: WorkflowService,
}

impl AdminServiceGrpc {
    pub fn new(inner: WorkflowService) -> Self {
        Self { inner }
    }

    pub fn into_service(self) -> AdminServiceServer<Self> {
        AdminServiceServer::new(self)
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip)
    }
}

#[tonic::async_trait]
impl AdminServiceGrpcApi for AdminServiceGrpc {
    async fn describe_mutable_state(
        &self,
        request: Request<adminservice::DescribeMutableStateRequest>,
    ) -> Result<Response<DescribeMutableStateResponse>, Status> {
        let req = request.into_inner();
        let execution = req
            .execution
            .ok_or_else(|| Status::invalid_argument("DescribeMutableStateRequest.execution"))?;
        let run_id = (!execution.run_id.is_empty()).then_some(execution.run_id.as_str());

        let summary = self
            .inner
            .describe_mutable_state(&req.namespace, &execution.workflow_id, run_id)
            .await?;

        let mutable_state = WorkflowMutableState {
            execution_info: Some(WorkflowExecutionInfo {
                reset_run_id: summary
                    .reset_run_id
                    .map(|id| id.0.to_string())
                    .unwrap_or_default(),
                original_execution_run_id: summary
                    .original_execution_run_id
                    .map(|id| id.0.to_string())
                    .unwrap_or_default(),
            }),
            execution_state: Some(WorkflowExecutionState {
                status: execution_status_to_proto(summary.status),
            }),
        };

        Ok(Response::new(DescribeMutableStateResponse {
            // Populate both cache and database images — the suite reads whichever
            // `GetDatabaseMutableState()` returns.
            database_mutable_state: Some(mutable_state.clone()),
            cache_mutable_state: Some(mutable_state),
            ..Default::default()
        }))
    }
}
