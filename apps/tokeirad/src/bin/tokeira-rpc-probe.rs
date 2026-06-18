//! `tokeira-rpc-probe` — a minimal diagnostic that calls a running `tokeirad`'s
//! `QueryWorkflow` and `DescribeWorkflowExecution` RPCs over tokeira's **own**
//! generated gRPC client (`tokeira-proto` + `tonic`, no external SDK) and prints
//! the **raw `tonic::Status`** (gRPC code + message) the server returns.
//!
//! Why this exists: the `temporal` CLI collapses these failures into an opaque
//! `"querying workflow failed"` / `"failed describing workflow"` with no status
//! code, which hides the one signal that points at the failing handler — e.g.
//! `NotFound` (execution lookup) vs `Unimplemented` (stubbed RPC) vs `Internal`
//! (handler panic). This probe surfaces that code directly. It first localized the
//! per-namespace execution-lookup `NotFound` in `QueryWorkflow`/`DescribeWorkflowExecution`.
//!
//! Build:
//!   cargo build -p tokeirad --bin tokeira-rpc-probe
//!
//! Usage (point it at a workflow that is currently Running):
//!   tokeira-rpc-probe <workflow-id> [query-type] [run-id]
//!
//! An empty/omitted `run-id` sends Temporal's "resolve to the current run"
//! convention; pass an explicit run-id to test exact-run resolution. The
//! query-type defaults to Temporal's built-in `__stack_trace` query, so the probe
//! is useful even against a workflow whose worker registers no custom query.
//!
//! Env: `TOKEIRAD_URL` (default `http://localhost:7233`),
//! `TOKEIRAD_NAMESPACE` (default `default`).

// Requests use `..Default::default()` for forward-compat against upstream proto
// field additions (TEMPORAL_PROTO_VERSION bumps), matching the grpc roundtrip tests.
#![allow(clippy::needless_update)]

use std::env;

use anyhow::Context;
use tokeira_proto::{
    common::WorkflowExecution,
    public::temporal::api::query::v1::WorkflowQuery,
    workflowservice::{
        DescribeWorkflowExecutionRequest, QueryWorkflowRequest,
        workflow_service_client::WorkflowServiceClient,
    },
};

const DEFAULT_TOKEIRAD_URL: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_QUERY_TYPE: &str = "__stack_trace";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let workflow_id = env::args()
        .nth(1)
        .context("usage: tokeira-rpc-probe <workflow-id> [query-type] [run-id]")?;
    let query_type = env::args()
        .nth(2)
        .unwrap_or_else(|| DEFAULT_QUERY_TYPE.to_string());
    let run_id = env::args().nth(3).unwrap_or_default();
    let target = env::var("TOKEIRAD_URL").unwrap_or_else(|_| DEFAULT_TOKEIRAD_URL.to_string());
    let namespace =
        env::var("TOKEIRAD_NAMESPACE").unwrap_or_else(|_| DEFAULT_NAMESPACE.to_string());

    let mut client = WorkflowServiceClient::connect(target.clone())
        .await
        .with_context(|| format!("connect to tokeirad at {target}"))?;

    let run_label = if run_id.is_empty() {
        "<current run>"
    } else {
        run_id.as_str()
    };
    println!(
        "tokeira-rpc-probe: target={target} namespace={namespace} workflow_id={workflow_id} \
         query_type={query_type} run_id={run_label}\n"
    );

    // QueryWorkflow — the RPC behind `temporal workflow query`.
    let query_req = QueryWorkflowRequest {
        namespace: namespace.clone(),
        execution: Some(WorkflowExecution {
            workflow_id: workflow_id.clone(),
            run_id: run_id.clone(),
        }),
        query: Some(WorkflowQuery {
            query_type: query_type.clone(),
            query_args: None,
            header: None,
        }),
        // QUERY_REJECT_CONDITION_NONE — don't reject on a non-running workflow.
        query_reject_condition: 1,
        ..Default::default()
    };
    match client.query_workflow(query_req).await {
        Ok(resp) => {
            let inner = resp.into_inner();
            let payloads = inner.query_result.map(|p| p.payloads.len()).unwrap_or(0);
            println!(
                "QueryWorkflow                => OK   (result_payloads={payloads}, rejected={})",
                inner.query_rejected.is_some()
            );
        }
        Err(status) => println!(
            "QueryWorkflow                => ERR  code={:?}  message={:?}",
            status.code(),
            status.message()
        ),
    }

    // DescribeWorkflowExecution — the RPC behind `temporal workflow describe`.
    let describe_req = DescribeWorkflowExecutionRequest {
        namespace: namespace.clone(),
        execution: Some(WorkflowExecution {
            workflow_id: workflow_id.clone(),
            run_id: run_id.clone(),
        }),
        ..Default::default()
    };
    match client.describe_workflow_execution(describe_req).await {
        Ok(_) => println!("DescribeWorkflowExecution    => OK"),
        Err(status) => println!(
            "DescribeWorkflowExecution    => ERR  code={:?}  message={:?}",
            status.code(),
            status.message()
        ),
    }

    Ok(())
}
