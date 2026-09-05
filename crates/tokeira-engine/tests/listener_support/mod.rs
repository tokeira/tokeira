//! Shared helpers for the listener integration tests: a transport-neutral
//! unary call surface over the in-process endpoint or a bound listener.
#![allow(dead_code)]

use std::{collections::BTreeMap, net::SocketAddr, str::FromStr as _, time::Duration};

use anyhow::{Context as _, Result};
use http::{HeaderMap, HeaderName, HeaderValue};
use prost::Message;
use tokeira_engine::{Engine, EngineListener, InProcessGrpcRequest, TemporalEndpoint};
use tokeira_proto::{
    common::{Payload, Payloads, WorkflowExecution},
    taskqueue::TaskQueue,
    workflowservice::{GetSystemInfoRequest, GetSystemInfoResponse},
};
use tonic::{Status, client::Grpc, codec::ProstCodec, codegen::http::uri::PathAndQuery};

pub(crate) const WORKFLOW_SERVICE: &str = "temporal.api.workflowservice.v1.WorkflowService";
/// Every step is bounded well below the 60 s server long poll so a wrong
/// assumption fails fast instead of hanging the suite.
pub(crate) const STEP: Duration = Duration::from_secs(20);

/// A way to reach one engine: its in-process endpoint or a bound listener.
#[derive(Clone)]
pub(crate) enum Transport {
    InProcess(TemporalEndpoint),
    Network(tonic::transport::Channel),
}

impl Transport {
    pub(crate) async fn network(addr: SocketAddr) -> Result<Self> {
        let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))?
            .connect()
            .await?;
        Ok(Self::Network(channel))
    }

    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::InProcess(_) => "in-process",
            Self::Network(_) => "network",
        }
    }

    /// One unary WorkflowService call with request metadata.
    pub(crate) async fn unary<Req, Resp>(
        &self,
        rpc: &str,
        request: Req,
        headers: &[(&str, &str)],
    ) -> Result<Resp, Status>
    where
        Req: Message + Send + Sync + 'static,
        Resp: Message + Default + Send + Sync + 'static,
    {
        match self {
            Self::InProcess(endpoint) => {
                let mut map = HeaderMap::new();
                for (name, value) in headers {
                    map.insert(
                        HeaderName::from_bytes(name.as_bytes())
                            .map_err(|error| Status::internal(error.to_string()))?,
                        HeaderValue::from_str(value)
                            .map_err(|error| Status::internal(error.to_string()))?,
                    );
                }
                let response = endpoint
                    .call(InProcessGrpcRequest {
                        service: WORKFLOW_SERVICE.to_owned(),
                        rpc: rpc.to_owned(),
                        headers: map,
                        proto: request.encode_to_vec().into(),
                    })
                    .await?;
                Resp::decode(response.proto.as_slice())
                    .map_err(|error| Status::internal(error.to_string()))
            }
            Self::Network(channel) => {
                let mut grpc = Grpc::new(channel.clone());
                grpc.ready()
                    .await
                    .map_err(|error| Status::unavailable(error.to_string()))?;
                let path = PathAndQuery::from_str(&format!("/{WORKFLOW_SERVICE}/{rpc}"))
                    .map_err(|error| Status::internal(error.to_string()))?;
                let mut request = tonic::Request::new(request);
                for (name, value) in headers {
                    request.metadata_mut().insert(
                        tonic::metadata::MetadataKey::from_str(name)
                            .map_err(|error| Status::internal(error.to_string()))?,
                        tonic::metadata::MetadataValue::from_str(value)
                            .map_err(|error| Status::internal(error.to_string()))?,
                    );
                }
                grpc.unary(request, path, ProstCodec::default())
                    .await
                    .map(tonic::Response::into_inner)
            }
        }
    }

    pub(crate) async fn system_info(&self) -> Result<GetSystemInfoResponse, Status> {
        self.unary("GetSystemInfo", GetSystemInfoRequest::default(), &[])
            .await
    }
}

pub(crate) fn payloads(value: &str) -> Payloads {
    Payloads {
        payloads: vec![Payload {
            metadata: BTreeMap::from([("encoding".to_owned(), b"json/plain".to_vec())]),
            data: format!("\"{value}\"").into_bytes(),
            ..Default::default()
        }],
    }
}

pub(crate) fn execution(workflow_id: &str, run_id: &str) -> WorkflowExecution {
    WorkflowExecution {
        workflow_id: workflow_id.to_owned(),
        run_id: run_id.to_owned(),
    }
}

pub(crate) fn task_queue(name: &str) -> TaskQueue {
    TaskQueue {
        name: name.to_owned(),
        ..Default::default()
    }
}

pub(crate) fn seconds(seconds: i64) -> prost_types::Duration {
    prost_types::Duration { seconds, nanos: 0 }
}

pub(crate) async fn start_engine_with_listener()
-> Result<(Engine, EngineListener, Transport, Transport)> {
    let engine = Engine::start().await?;
    let listener = engine.listen("127.0.0.1:0".parse()?).await?;
    let network = Transport::network(listener.bound_addr()).await?;
    let in_process = Transport::InProcess(engine.endpoint());
    Ok((engine, listener, in_process, network))
}

/// Retry until the socket refuses connections; a stopped listener's socket
/// closes when its task exits, which is asynchronous after a bare drop, so
/// this synchronises on the observable state instead of sleeping.
pub(crate) async fn wait_until_refused(addr: SocketAddr) -> Result<()> {
    tokio::time::timeout(STEP, async {
        loop {
            match Transport::network(addr).await {
                Ok(transport) => {
                    if transport.system_info().await.is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .with_context(|| format!("listener on {addr} kept serving after it was stopped"))
}

pub(crate) fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("test runtime builds")
}
