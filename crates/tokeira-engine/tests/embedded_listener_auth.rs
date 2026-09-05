//! Authorization parity across transports (Property 2).
//!
//! The harness fallback authenticator is process-global, so this property
//! lives in its own test binary: every engine started here sees the same
//! header-gated policy, and no other test does.

mod listener_support;

use std::sync::Arc;

use async_trait::async_trait;
use http::HeaderMap;
use listener_support::{Transport, execution, runtime, start_engine_with_listener, task_queue};
use proptest::prelude::*;
use tokeira_auth::{AuthPrincipal, Claims};
use tokeira_edge::{Action, Authenticator, EdgeError, EdgeResult};
use tokeira_engine::harness::{self, HarnessHooks};
use tokeira_proto::{
    common::WorkflowType,
    workflowservice::{
        DescribeNamespaceRequest, DescribeNamespaceResponse, DescribeWorkflowExecutionRequest,
        DescribeWorkflowExecutionResponse, GetSystemInfoRequest, GetSystemInfoResponse,
        StartWorkflowExecutionRequest, StartWorkflowExecutionResponse,
    },
};
use tonic::{Code, Status};

const GATE_HEADER: &str = "x-parity-auth";

/// Authenticates purely from one request header so every outcome the edge can
/// produce (allowed, denied, missing identity) is reachable from a test.
struct HeaderGate;

#[async_trait]
impl Authenticator for HeaderGate {
    async fn authenticate(&self, headers: &HeaderMap) -> EdgeResult<Option<Claims>> {
        match headers
            .get(GATE_HEADER)
            .and_then(|value| value.to_str().ok())
        {
            Some("allow") => Ok(None),
            Some(other) => Err(EdgeError::PermissionDenied {
                message: format!("{GATE_HEADER} value {other:?} is denied"),
                reason: Some("parity-gate".to_owned()),
            }),
            None => Err(EdgeError::PermissionDenied {
                message: format!("{GATE_HEADER} is required"),
                reason: None,
            }),
        }
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

fn install_gate() {
    harness::install(HarnessHooks {
        fallback_grpc_authenticator: Some(Arc::new(HeaderGate)),
        ..Default::default()
    });
}

#[derive(Clone, Copy, Debug)]
enum HeaderCase {
    Missing,
    Allow,
    Deny,
}

#[derive(Clone, Copy, Debug)]
enum RpcCase {
    SystemInfo,
    DescribeNamespace,
    StartWorkflow,
    DescribeMissingWorkflow,
}

fn header_strategy() -> impl Strategy<Value = HeaderCase> {
    prop_oneof![
        Just(HeaderCase::Missing),
        Just(HeaderCase::Allow),
        Just(HeaderCase::Deny),
    ]
}

fn rpc_strategy() -> impl Strategy<Value = RpcCase> {
    prop_oneof![
        Just(RpcCase::SystemInfo),
        Just(RpcCase::DescribeNamespace),
        Just(RpcCase::StartWorkflow),
        Just(RpcCase::DescribeMissingWorkflow),
    ]
}

/// The observable outcome of one call: gRPC code and message.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Outcome {
    code: Code,
    message: String,
}

fn outcome<T>(result: Result<T, Status>) -> Outcome {
    match result {
        Ok(_) => Outcome {
            code: Code::Ok,
            message: String::new(),
        },
        Err(status) => Outcome {
            code: status.code(),
            message: status.message().to_owned(),
        },
    }
}

async fn call(transport: &Transport, header: HeaderCase, rpc: RpcCase, index: usize) -> Outcome {
    let headers: Vec<(&str, &str)> = match header {
        HeaderCase::Missing => Vec::new(),
        HeaderCase::Allow => vec![(GATE_HEADER, "allow")],
        HeaderCase::Deny => vec![(GATE_HEADER, "deny")],
    };
    match rpc {
        RpcCase::SystemInfo => outcome(
            transport
                .unary::<_, GetSystemInfoResponse>(
                    "GetSystemInfo",
                    GetSystemInfoRequest::default(),
                    &headers,
                )
                .await,
        ),
        RpcCase::DescribeNamespace => outcome(
            transport
                .unary::<_, DescribeNamespaceResponse>(
                    "DescribeNamespace",
                    DescribeNamespaceRequest {
                        namespace: "default".to_owned(),
                        ..Default::default()
                    },
                    &headers,
                )
                .await,
        ),
        RpcCase::StartWorkflow => outcome(
            transport
                .unary::<_, StartWorkflowExecutionResponse>(
                    "StartWorkflowExecution",
                    StartWorkflowExecutionRequest {
                        namespace: "default".to_owned(),
                        workflow_id: format!("parity-{index}"),
                        workflow_type: Some(WorkflowType {
                            name: "parity".to_owned(),
                        }),
                        task_queue: Some(task_queue("parity-queue")),
                        request_id: format!("parity-start-{index}"),
                        ..Default::default()
                    },
                    &headers,
                )
                .await,
        ),
        RpcCase::DescribeMissingWorkflow => outcome(
            transport
                .unary::<_, DescribeWorkflowExecutionResponse>(
                    "DescribeWorkflowExecution",
                    DescribeWorkflowExecutionRequest {
                        namespace: "default".to_owned(),
                        execution: Some(execution("parity-missing", "")),
                    },
                    &headers,
                )
                .await,
        ),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]

    // Feature: embedded-engine-listener, Property 2: authorization parity across transports
    #[test]
    fn authorization_parity_across_transports(
        cases in prop::collection::vec((header_strategy(), rpc_strategy()), 1..6),
    ) {
        install_gate();
        runtime().block_on(async {
            let (engine, listener, in_process, network) =
                start_engine_with_listener().await.expect("engine with listener");
            for (index, (header, rpc)) in cases.into_iter().enumerate() {
                // The same request through both transports; a start is issued
                // with one request id so the second observation dedupes to the
                // first outcome rather than colliding.
                let expected = call(&in_process, header, rpc, index).await;
                let observed = call(&network, header, rpc, index).await;
                prop_assert_eq!(&observed, &expected, "{:?} {:?} diverged", header, rpc);
                if matches!(header, HeaderCase::Deny | HeaderCase::Missing)
                    && !matches!(rpc, RpcCase::SystemInfo)
                {
                    prop_assert_eq!(observed.code, Code::PermissionDenied);
                }
            }
            listener.shutdown().await.expect("listener stops");
            engine.shutdown().await.expect("engine stops");
            Ok(())
        })?;
    }
}
