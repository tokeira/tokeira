//! Starter binary for the hello-world example.

mod workflows;

use std::str::FromStr;
use temporalio_client::{
    Client, ClientOptions, Connection,
    ConnectionOptions, WorkflowGetResultOptions,
    WorkflowStartOptions,
};
use temporalio_sdk_core::Url;
use workflows::HelloWorldWorkflow;

#[tokio::main]
async fn main()
-> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("TEMPORAL_ADDRESS")
        .unwrap_or_else(|_| {
            "http://[::1]:7233".to_string()
        });
    let url = Url::from_str(&addr)?;

    let connection = Connection::connect(
        ConnectionOptions::new(url).build(),
    )
    .await?;
    let client = Client::new(
        connection,
        ClientOptions::new("default").build(),
    )?;

    let handle = client
        .start_workflow(
            HelloWorldWorkflow::run,
            "Temporal".to_string(),
            WorkflowStartOptions::new(
                "hello-world",
                "hello-world-workflow-id",
            )
            .build(),
        )
        .await?;

    println!(
        "Started workflow, run_id: {:?}",
        handle.run_id()
    );

    let result = handle
        .get_result(
            WorkflowGetResultOptions::default(),
        )
        .await?;
    println!("Workflow result: {result}");

    Ok(())
}
