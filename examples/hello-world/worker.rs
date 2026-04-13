//! Worker binary for the hello-world example.

mod workflows;

use std::str::FromStr;
use temporalio_client::{
    Client, ClientOptions, Connection,
    ConnectionOptions,
};
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::{
    CoreRuntime, RuntimeOptions, Url,
};
use workflows::{
    GreetingActivities, HelloWorldWorkflow,
};

#[tokio::main]
async fn main()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder().build()?,
    )?;

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

    let worker_options =
        WorkerOptions::new("hello-world")
            .register_workflow::<HelloWorldWorkflow>()
            .register_activities(GreetingActivities)
            .build();

    let mut worker =
        Worker::new(&runtime, client, worker_options)?;
    println!(
        "Worker started on task queue: hello-world"
    );
    worker.run().await?;

    Ok(())
}
