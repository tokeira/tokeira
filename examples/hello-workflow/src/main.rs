//! A complete durable round-trip against embedded Tokeira: the engine, a
//! worker, and one workflow execution — in a single process, no server.

use std::time::Duration;

use anyhow::Result;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowStartOptions,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, Runtime, Worker, WorkerOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use tokeira_engine::Engine;

#[workflow]
#[derive(Default)]
struct HelloWorkflow;

#[workflow_methods]
impl HelloWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        let greeting = ctx
            .execute_activity(
                Greetings::greet,
                name,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await?;
        Ok(greeting)
    }
}

struct Greetings;

#[activities]
impl Greetings {
    #[activity]
    async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}! This greeting is durable."))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // A Temporal-compatible engine, in-process. No listener, no daemon.
    let engine = Engine::embedded().await?;

    // The Temporal Rust SDK reaches it over an in-memory duplex.
    let options = ConnectionOptions::new("http://tokeira-engine.invalid:7233".parse::<url::Url>()?)
        .service_override(engine.service_override())
        .dns_load_balancing(None)
        .build();
    let connection = Connection::connect(options).await?;
    let client = Client::new(connection, ClientOptions::new("default").build())?;

    // One worker on this process's runtime, serving the workflow above.
    let runtime = Runtime::new_assume_tokio(Default::default())?;
    let worker_options = WorkerOptions::new("hello")
        .register_workflow::<HelloWorkflow>()?
        .register_activities(Greetings)
        .build();
    let mut worker = Worker::new(&runtime, client.clone(), worker_options)?;
    let shutdown = worker.shutdown_handle();

    // Drive the workflow from a task while the worker serves in the
    // foreground. The workflow ID is the idempotency key: starting it again
    // joins the existing run instead of duplicating the work.
    let starter = tokio::spawn(async move {
        let handle = client
            .start_workflow(
                HelloWorkflow::run,
                "Tokeira".to_string(),
                WorkflowStartOptions::new("hello", "hello-1").build(),
            )
            .await?;
        let result: String = handle
            .get_result(WorkflowGetResultOptions::default())
            .await?;
        println!("{result}");
        shutdown();
        anyhow::Ok(())
    });

    worker.run().await?;
    starter.await??;

    // Stop the engine the worker was connected to.
    engine.shutdown().await?;
    Ok(())
}
