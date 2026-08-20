//! Focused lifecycle and SDK-connection coverage for the zero-listener engine.

use std::net::TcpListener;

use anyhow::Result;
use http::HeaderMap;
use prost::Message as _;
use temporalio_client::{Connection, ConnectionOptions};
use tokeira_engine::{Engine, InProcessGrpcRequest, TokeiraConfig};
use tokeira_proto::workflowservice::{GetSystemInfoRequest, GetSystemInfoResponse};

const WORKFLOW_SERVICE: &str = "temporal.api.workflowservice.v1.WorkflowService";

#[tokio::test]
async fn raw_endpoint_dispatches_get_system_info() -> Result<()> {
    let engine = Engine::start().await?;
    let response = engine
        .endpoint()
        .call(InProcessGrpcRequest {
            service: WORKFLOW_SERVICE.to_owned(),
            rpc: "GetSystemInfo".to_owned(),
            headers: HeaderMap::new(),
            proto: GetSystemInfoRequest::default().encode_to_vec().into(),
        })
        .await?;

    let decoded = GetSystemInfoResponse::decode(response.proto.as_slice())?;
    assert!(
        decoded.capabilities.is_some(),
        "the embedded endpoint must expose Tokeira's real system capabilities"
    );
    engine.shutdown().await
}

#[tokio::test]
async fn temporal_client_connects_through_service_override() -> Result<()> {
    let engine = Engine::start().await?;
    let options = ConnectionOptions::new(url::Url::parse("http://tokeira-engine.invalid:7233")?)
        .service_override(engine.service_override())
        .dns_load_balancing(None)
        .build();

    let connection = Connection::connect(options).await?;
    assert!(
        connection.capabilities().is_some(),
        "Connection::connect must complete GetSystemInfo through the override"
    );
    drop(connection);
    engine.shutdown().await
}

#[tokio::test]
async fn embedded_start_does_not_bind_configured_listeners() -> Result<()> {
    let occupied_grpc = TcpListener::bind("127.0.0.1:0")?;
    let occupied_nexus = TcpListener::bind("127.0.0.1:0")?;
    let mut config = TokeiraConfig::default();
    config.infrastructure.network.grpc_addr = occupied_grpc.local_addr()?.to_string();
    config.policy.nexus_completion.http_addr = occupied_nexus.local_addr()?.to_string();

    // Construction would fail with AddressInUse if either the Temporal or Nexus
    // callback transport were still an implicit part of engine startup.
    let engine = Engine::start_with_config(config).await?;
    engine.shutdown().await
}

#[tokio::test]
async fn shutdown_closes_existing_endpoint_clones() -> Result<()> {
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    engine.shutdown().await?;

    let status = endpoint
        .call(InProcessGrpcRequest {
            service: WORKFLOW_SERVICE.to_owned(),
            rpc: "GetSystemInfo".to_owned(),
            headers: HeaderMap::new(),
            proto: GetSystemInfoRequest::default().encode_to_vec().into(),
        })
        .await
        .expect_err("an endpoint clone must reject calls after engine shutdown");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    Ok(())
}

#[tokio::test]
async fn unknown_rpc_preserves_unimplemented_status() -> Result<()> {
    let engine = Engine::start().await?;
    let status = engine
        .endpoint()
        .call(InProcessGrpcRequest {
            service: WORKFLOW_SERVICE.to_owned(),
            rpc: "NoSuchRpc".to_owned(),
            headers: HeaderMap::new(),
            proto: Vec::new().into(),
        })
        .await
        .expect_err("the tonic router must reject an unknown method");

    assert_eq!(status.code(), tonic::Code::Unimplemented);
    engine.shutdown().await
}
