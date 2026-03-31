use std::sync::Arc;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

use tokeira_projection::{InMemoryVisibilitySink, ProjectionWorker};
use tokeira_runtime::TokeiraRuntime;
use tokeira_storage::InMemoryStore;
use tokeira_types::{ProjectionCursor, QueueKey, TaskKind, TaskQueueName, WorkerIdentity, NamespaceId};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    info!("starting minimal tokeirad shell");

    let store = Arc::new(InMemoryStore::default());
    let runtime = TokeiraRuntime::new(store.clone(), 4);

    // The app intentionally only wires the pieces together lightly. The point of
    // this binary is to provide a place for local smoke tests and future control
    // plane wiring without pretending that the transport/server layer already
    // exists.
    let projector = ProjectionWorker {
        log: store.clone(),
        sink: InMemoryVisibilitySink::default(),
        batch_size: 128,
    };

    let _cursor = projector
        .run_once(ProjectionCursor::beginning(0, 1))
        .await?;

    info!("minimal tokeirad bootstrap complete");

    // TODO(edge): expose transport endpoints.
    // TODO(controller): attach placement and lease loops.
    // TODO(autoscaler): connect to the future tokeira-autoscaler service.
    // TODO(archival): run archival and purge workers as distinct services.

    // Keep the process alive briefly so `cargo run` users can see startup.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    Ok(())
}
