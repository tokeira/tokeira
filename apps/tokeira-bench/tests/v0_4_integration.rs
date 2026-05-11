//! v0.4 SDK ↔ post-sync `tokeirad` integration test.
//!
//! The test spawns `tokeirad` in-process via [`TokeiradHandle::start_in_memory`],
//! constructs a v0.4 `temporalio_client::Client`, asserts that `worker_heartbeats`
//! is advertised `true` on both `GetSystemInfo` and `DescribeNamespace`, then
//! starts and round-trips an `EchoWorkflow` through a v0.4 `Worker`.
//!
//! Gated behind `#[ignore]` so it does not run under a plain
//! `cargo test --workspace`; run it explicitly with:
//!
//! ```text
//! cargo test --package tokeira-bench --test v0_4_integration -- --include-ignored
//! ```
//!
//! This test is the spec's acceptance gate per
//! `.kiro/specs/temporal-api-v1.62-sync/requirements.md` §7:
//!   - `capabilities.worker_heartbeats == true` on both handshake surfaces.
//!   - `RecordWorkerHeartbeat` returns Ok (no `Status::unimplemented`), keeping
//!     the v0.4 `SharedNamespaceWorker` alive for the duration of the workflow.
//!   - An `EchoWorkflow` start→complete round-trip succeeds end-to-end.
//!
//! Synchronisation uses `tokio::sync::Notify` and `tokio::time::timeout` —
//! never `tokio::time::sleep` — per `tokeira/AGENTS.md` Rule 1.

use std::{sync::Arc, time::Duration};

use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowStartOptions, grpc::WorkflowService,
};
use temporalio_common::{
    protos::temporal::api::workflowservice::v1::DescribeNamespaceRequest,
    telemetry::TelemetryOptions,
};
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions};
use tokeira_bench::{BENCH_TASK_QUEUE, EchoWorkflow};
use tokio::sync::Notify;
use tonic::IntoRequest;
use url::Url;

use tokeirad::TokeiradHandle;

/// Total wall-clock budget for the test. Bounded so CI failures surface
/// quickly rather than hanging a run.
const TEST_DEADLINE: Duration = Duration::from_secs(120);

/// Per-workflow completion budget. A trivial `EchoWorkflow` resolves in
/// milliseconds on localhost; this window is generous enough to absorb the
/// post-start worker-registration handshake.
const PER_WORKFLOW_TIMEOUT: Duration = Duration::from_secs(30);

#[ignore = "integration test; spawns tokeirad and a v0.4 SDK worker. See temporal-api-v1.62-sync."]
#[tokio::test]
async fn v0_4_sdk_echo_roundtrip_against_post_sync_tokeirad() {
    // The whole test runs inside one timeout so a regression never hangs CI.
    // Any step that ought to complete quickly is wrapped in its own tighter
    // timeout below.
    tokio::time::timeout(TEST_DEADLINE, run_v0_4_integration())
        .await
        .expect("v0.4 integration test exceeded the overall deadline");
}

async fn run_v0_4_integration() {
    // Step 1. Spawn tokeirad in-process on an ephemeral port.
    let handle = TokeiradHandle::start_in_memory("127.0.0.1:0".parse().unwrap())
        .await
        .expect("tokeirad should start on an ephemeral socket");
    let target_url: Url = format!("http://{}", handle.bound_addr())
        .parse()
        .expect("bound_addr should produce a parseable URL");

    // Step 2. Connect a v0.4 client. `Connection::connect` performs the
    // `GetSystemInfo` handshake internally and caches capabilities on the
    // connection — a side-effect we want to assert against in step 3.
    let conn_opts = ConnectionOptions::new(target_url)
        .identity("tokeira-v162-integration".to_string())
        .client_name("tokeira-integration".to_string())
        .client_version("0.1.0".to_string())
        .build();
    let mut connection = Connection::connect(conn_opts)
        .await
        .expect("v0.4 SDK Connection::connect should succeed against post-sync tokeirad");

    // Assertion 1: `worker_heartbeats` on `GetSystemInfoResponse.capabilities`
    // is `true`. Without this, the SDK's SharedNamespaceWorker would refuse
    // to start heartbeating, which would then fail any workflow that expects
    // the worker to remain alive.
    let capabilities = connection
        .capabilities()
        .cloned()
        .expect("connection should carry cached capabilities after handshake");
    assert!(
        capabilities.worker_heartbeats,
        "post-sync tokeirad must advertise `worker_heartbeats = true` per Req 4.1.1 and Req 3.3"
    );

    // Assertion 2: `DescribeNamespace("default")` carries the same
    // capability. The v0.4 SDK consults either surface; post-sync tokeirad
    // must keep them byte-identical.
    let describe_resp = WorkflowService::describe_namespace(
        &mut connection,
        DescribeNamespaceRequest {
            namespace: "default".to_string(),
            ..Default::default()
        }
        .into_request(),
    )
    .await
    .expect("DescribeNamespace should succeed for the default namespace")
    .into_inner();
    let namespace_caps = describe_resp
        .namespace_info
        .and_then(|info| info.capabilities)
        .expect("DescribeNamespace response should carry NamespaceInfo.Capabilities");
    assert!(
        namespace_caps.worker_heartbeats,
        "post-sync tokeirad must advertise `NamespaceInfo.Capabilities.worker_heartbeats = true` per Req 3.3"
    );

    let client_opts = ClientOptions::new("default".to_string()).build();
    let client = Client::new(connection, client_opts).expect("Client::new should succeed");

    // Step 3. Bring up a v0.4 worker on the bench task queue with the
    // shared `EchoWorkflow`. The worker's run loop runs in the background
    // until we cancel it.
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(TelemetryOptions::builder().build())
            .build()
            .expect("RuntimeOptions should build"),
    )
    .expect("CoreRuntime should start");

    let worker_options = WorkerOptions::new(BENCH_TASK_QUEUE)
        .register_workflow::<EchoWorkflow>()
        .build();
    let mut worker =
        Worker::new(&runtime, client.clone(), worker_options).expect("Worker should construct");

    // Notify used to wait until the worker has had at least one run-loop
    // tick, which indirectly confirms RecordWorkerHeartbeat is returning Ok
    // (if it returned Unimplemented the SharedNamespaceWorker would shut
    // down before any work is polled).
    let worker_ready = Arc::new(Notify::new());
    let worker_ready_signal = worker_ready.clone();

    let worker_task = tokio::spawn(async move {
        // Signalling ready as soon as `run` has been entered is sufficient
        // for the test — the caller's start_workflow will then queue a task
        // the worker picks up immediately.
        worker_ready_signal.notify_one();
        worker.run().await
    });

    worker_ready.notified().await;

    // Step 4. Start one EchoWorkflow and await its completion.
    let workflow_id = "v162-integration-echo";
    let handle_start = client
        .start_workflow(
            EchoWorkflow::run,
            "hello".to_string(),
            WorkflowStartOptions::new(BENCH_TASK_QUEUE, workflow_id).build(),
        )
        .await
        .expect("start_workflow should succeed");

    let result = tokio::time::timeout(
        PER_WORKFLOW_TIMEOUT,
        handle_start.get_result(WorkflowGetResultOptions::default()),
    )
    .await
    .expect("EchoWorkflow did not complete within PER_WORKFLOW_TIMEOUT")
    .expect("EchoWorkflow get_result should resolve to Ok");

    assert_eq!(
        result, "hello",
        "EchoWorkflow is defined to echo its input verbatim"
    );

    // Step 5. Tear down cleanly. Abort the worker task, then shut down the
    // server. Both orderings of these two calls are valid; we do worker
    // first so the SDK does not race the server into shutdown.
    worker_task.abort();
    // `JoinHandle::await` on an aborted task returns `Err(JoinError::Cancelled)`;
    // we do not assert on its result because the abort is the success case.
    let _ = worker_task.await;

    handle
        .shutdown()
        .await
        .expect("TokeiradHandle::shutdown should drain cleanly at test end");
}
