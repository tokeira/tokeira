//! Diagnostic: does the visibility plane actually hold the standalone activities?
//!
//! Queries `CountActivityExecutions` / `ListActivityExecutions` against a running
//! `tokeirad`, and `ListWorkflowExecutions` to confirm SAs are *not* leaking into
//! the workflow list (they are deliberately archetype-scoped out). Connects via
//! `TEMPORAL_ADDRESS` (default `http://127.0.0.1:7233`); optional `QUERY` env var.
//!
//! Run: `cargo run --manifest-path scenarios/standalone-activities/Cargo.toml --bin list`

use anyhow::{Result, anyhow};
use tokeira_proto::public::temporal::api::workflowservice::v1 as wf;
use tokeira_proto::public::temporal::api::workflowservice::v1::workflow_service_client::WorkflowServiceClient;

#[tokio::main]
async fn main() -> Result<()> {
    let address =
        std::env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| "http://127.0.0.1:7233".to_owned());
    let namespace = std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".to_owned());
    let query = std::env::var("QUERY").unwrap_or_default();

    let mut client = WorkflowServiceClient::connect(address.clone())
        .await
        .map_err(|e| anyhow!("connect to {address}: {e}"))?;
    println!("address={address} namespace={namespace} query={query:?}\n");

    // --- Activity visibility (what an SA-aware UI would read) ---
    match client
        .count_activity_executions(wf::CountActivityExecutionsRequest {
            namespace: namespace.clone(),
            query: query.clone(),
        })
        .await
    {
        Ok(r) => println!("CountActivityExecutions => {}", r.into_inner().count),
        Err(s) => println!("CountActivityExecutions ERROR: {s}"),
    }

    match client
        .list_activity_executions(wf::ListActivityExecutionsRequest {
            namespace: namespace.clone(),
            page_size: 100,
            next_page_token: Vec::new(),
            query: query.clone(),
        })
        .await
    {
        Ok(r) => {
            let list = r.into_inner();
            println!("ListActivityExecutions => {} row(s)", list.executions.len());
            for (i, e) in list.executions.iter().enumerate() {
                println!("  [{i}] {e:?}");
            }
        }
        Err(s) => println!("ListActivityExecutions ERROR: {s}"),
    }

    // --- Workflow visibility (SAs must be ABSENT here — archetype-scoped out) ---
    match client
        .list_workflow_executions(wf::ListWorkflowExecutionsRequest {
            namespace: namespace.clone(),
            page_size: 100,
            next_page_token: Vec::new(),
            query: query.clone(),
        })
        .await
    {
        Ok(r) => println!(
            "\nListWorkflowExecutions => {} row(s) (SAs should be absent here)",
            r.into_inner().executions.len()
        ),
        Err(s) => println!("\nListWorkflowExecutions ERROR: {s}"),
    }

    Ok(())
}
