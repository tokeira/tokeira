//! Temporal Rust SDK 1.0.0 ↔ `tokeirad` integration tests.
//!
//! The first test spawns `tokeirad` in-process via [`TokeiradHandle::start_in_memory`],
//! constructs an SDK `temporalio_client::Client`, asserts that `worker_heartbeats`
//! is advertised `true` on `DescribeNamespace`, then starts and round-trips an
//! `EchoWorkflow` through an SDK `Worker`.
//!
//! The second test brings up a scoped Worker credential through the production
//! authorization bootstrap and proves the scope holds across workflow, activity,
//! and Nexus polling. The SDK's high-level worker derives its task types from the
//! definitions registered on it and never polls Nexus tasks, so that test builds
//! the Core worker itself, hands it to the SDK worker for workflows and activities,
//! and drives Nexus through the Core worker directly, which is what a Rust host
//! that handles Nexus operations has to do today.
//!
//! Both tests are gated behind `#[ignore]` so they do not run under a plain
//! `cargo test --workspace`; run them explicitly with:
//!
//! ```text
//! cargo test --package tokeira-bench --test v0_4_integration -- --include-ignored
//! ```
//!
//! Synchronisation uses `tokio::sync::Notify` and `tokio::time::timeout` —
//! never `tokio::time::sleep` — per `tokeira/AGENTS.md` Rule 1.

// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]
// Temporal's SDK macros generate public helper types without Debug impls in
// this integration-test crate.
#![allow(missing_debug_implementations, unreachable_pub)]
use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::anyhow;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowStartOptions,
    errors::WorkflowStartError,
    grpc::{OperatorService, WorkflowService},
    tonic::{Code, IntoRequest, Status},
};
use temporalio_common::{
    protos::{
        coresdk::nexus::{
            NexusTaskCompletion, nexus_operation_result, nexus_task, nexus_task_completion,
        },
        temporal::api::{
            deployment::v1::WorkerDeploymentOptions as ProtoWorkerDeploymentOptions,
            enums::v1::{TaskQueueType, WorkerVersioningMode},
            nexus::v1::{
                EndpointSpec, EndpointTarget, Response as NexusResponse, StartOperationResponse,
                endpoint_target, response as nexus_response, start_operation_response,
            },
            operatorservice::v1::CreateNexusEndpointRequest,
            taskqueue::v1::TaskQueue,
            workflowservice::v1::{
                DescribeNamespaceRequest, DescribeTaskQueueRequest, ListNamespacesRequest,
                PollWorkflowTaskQueueRequest, SetWorkerDeploymentCurrentVersionRequest,
            },
        },
    },
    worker::{
        VersioningBehavior, WorkerDeploymentOptions, WorkerDeploymentVersion, WorkerTaskTypes,
    },
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ApplicationFailure, NexusOperationOptions, Runtime, Worker, WorkerOptions,
    WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
    runtime::WorkerConfig,
};
use temporalio_sdk_core::{
    CoreRuntime, PollError, RuntimeOptions as CoreRuntimeOptions, WorkerVersioningStrategy,
    init_worker,
};
use tokeira_bench::{BENCH_TASK_QUEUE, EchoWorkflow};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Notify, oneshot},
};
use url::Url;

use tokeirad::TokeiradHandle;

/// Total wall-clock budget for the test. Bounded so CI failures surface
/// quickly rather than hanging a run.
const TEST_DEADLINE: Duration = Duration::from_secs(120);

/// Per-workflow completion budget. A trivial `EchoWorkflow` resolves in
/// milliseconds on localhost; this window is generous enough to absorb the
/// post-start worker-registration handshake.
const PER_WORKFLOW_TIMEOUT: Duration = Duration::from_secs(30);

const SCOPED_TASK_QUEUE: &str = "scoped-integration";
const SCOPED_DEPLOYMENT: &str = "scoped-deployment";
const SCOPED_BUILD_ID: &str = "scoped-build";
const SCOPED_NEXUS_ENDPOINT: &str = "ScopedIntegrationEndpoint";
const JWT_HEADER: &str = "eyJhbGciOiJFUzI1NiIsImtpZCI6ImZpeHR1cmUiLCJ0eXAiOiJKV1QifQ";
const ADMIN_JWT_PAYLOAD: &str = "eyJpc3MiOiJodHRwczovL3Njb3BlZC13b3JrZXIuZml4dHVyZS5pbnZhbGlkIiwic3ViIjoiaW50ZWdyYXRpb24tYWRtaW4iLCJhdWQiOiJ0b2tlaXJhLWludGVncmF0aW9uIiwiZXhwIjo0MTAyNDQ0ODAwLCJwZXJtaXNzaW9ucyI6WyJ0ZW1wb3JhbC1zeXN0ZW06YWRtaW4iLCJkZWZhdWx0OmFkbWluIl19";
const ADMIN_JWT_SIGNATURE: &str =
    "MiSEsKHvdfCIEHgtmJlWbKHuy6rqKh1c_Evi0gQNxCXS2-2ZqXKRMIN7EmRUsTyigXcx9REoXNDjaHAqp7WXfA";
const WORKER_JWT_PAYLOAD: &str = "eyJpc3MiOiJodHRwczovL3Njb3BlZC13b3JrZXIuZml4dHVyZS5pbnZhbGlkIiwic3ViIjoiaW50ZWdyYXRpb24td29ya2VyIiwiYXVkIjoidG9rZWlyYS1pbnRlZ3JhdGlvbiIsImV4cCI6NDEwMjQ0NDgwMCwidG9rZWlyYV93b3JrZXJfc2NvcGUiOnsidmVyc2lvbiI6MSwibmFtZXNwYWNlIjoiZGVmYXVsdCIsInRhc2tfcXVldWVzIjpbInNjb3BlZC1pbnRlZ3JhdGlvbiJdLCJkZXBsb3ltZW50X25hbWUiOiJzY29wZWQtZGVwbG95bWVudCIsImJ1aWxkX2lkIjoic2NvcGVkLWJ1aWxkIn19";
const WORKER_JWT_SIGNATURE: &str =
    "YnsG8PtEdt_s9w6aVKRBbieePUuQE6tUY2_4zQA4OBv7npH9fAjheScH7ciTGNbV_II8cEF_58TphEGWnAde7w";
const FIXTURE_JWKS: &str = r#"{"keys":[{"kty":"EC","crv":"P-256","alg":"ES256","kid":"fixture","x":"oAfw-_pFf7Je95MNHGL6bxqIdQVfJV0IuC_-y8KvRV4","y":"_xHqnzgHA3RGIBPgGt588mZxxD9N081Q7-2mN15Iw5M"}]}"#;

pub struct ScopedIntegrationActivities;

#[activities]
impl ScopedIntegrationActivities {
    #[activity]
    pub async fn heartbeat_echo(
        context: ActivityContext,
        input: String,
    ) -> Result<String, ActivityError> {
        let _ = context.record_heartbeat(input.clone()).await;
        Ok(input)
    }
}

#[workflow]
#[derive(Default, Debug)]
pub struct ScopedIntegrationWorkflow;

#[workflow_methods]
impl ScopedIntegrationWorkflow {
    #[run]
    pub async fn run(
        context: &mut WorkflowContext<Self>,
        endpoint: String,
    ) -> WorkflowResult<String> {
        let activity_result = context
            .execute_activity(
                ScopedIntegrationActivities::heartbeat_echo,
                "activity-complete".to_owned(),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(10))
                    .heartbeat_timeout(Duration::from_secs(5))
                    .build(),
            )
            .await?;
        let started = context
            .start_nexus_operation(
                NexusOperationOptions::builder()
                    .endpoint(endpoint)
                    .service("integration-service")
                    .operation("integration-operation")
                    .schedule_to_close_timeout(Duration::from_secs(10))
                    .build(),
            )
            .await
            .map_err(|failure| {
                ApplicationFailure::new(anyhow!("scoped Nexus start failed: {}", failure.message))
            })?;
        let result = started.result().await;
        if !matches!(
            result.status,
            Some(nexus_operation_result::Status::Completed(_))
        ) {
            return Err(ApplicationFailure::new(anyhow!(
                "scoped Nexus operation did not complete"
            ))
            .into());
        }
        Ok(activity_result)
    }
}

#[ignore = "integration test; spawns tokeirad and an SDK 1.0.0 worker. See temporal-api-v1.62-sync."]
#[tokio::test]
async fn sdk_echo_roundtrip_against_post_sync_tokeirad() {
    // The whole test runs inside one timeout so a regression never hangs CI.
    // Any step that ought to complete quickly is wrapped in its own tighter
    // timeout below.
    tokio::time::timeout(TEST_DEADLINE, run_echo_integration())
        .await
        .expect("SDK integration test exceeded the overall deadline");
}

async fn run_echo_integration() {
    // Step 1. Spawn tokeirad in-process on an ephemeral port.
    let handle = TokeiradHandle::start_in_memory("127.0.0.1:0".parse().unwrap())
        .await
        .expect("tokeirad should start on an ephemeral socket");
    let target_url: Url = format!("http://{}", handle.bound_addr())
        .parse()
        .expect("bound_addr should produce a parseable URL");

    // Step 2. Connect an SDK client. `Connection::connect` performs the
    // `GetSystemInfo` handshake internally and caches capabilities on the
    // connection — a side-effect we want to assert against in step 3.
    let conn_opts = ConnectionOptions::new(target_url)
        .identity("tokeira-v162-integration".to_string())
        .client_name("tokeira-integration".to_string())
        .client_version("0.1.0".to_string())
        .build();
    let mut connection = Connection::connect(conn_opts)
        .await
        .expect("SDK Connection::connect should succeed against post-sync tokeirad");

    // Assertion 1: `DescribeNamespace("default")` carries the worker-heartbeat
    // capability. The SDK's SharedNamespaceWorker checks this namespace
    // surface before enabling heartbeats.
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

    // Step 3. Bring up an SDK worker on the bench task queue with the
    // shared `EchoWorkflow`. The worker's run loop runs in the background
    // until we cancel it.
    let runtime = Runtime::from_current_tokio(Default::default()).expect("SDK runtime");

    let worker_options = WorkerOptions::new(BENCH_TASK_QUEUE)
        .register_workflow::<EchoWorkflow>()
        .expect("EchoWorkflow registers")
        .build();
    let mut worker =
        Worker::new(&runtime, client.clone(), worker_options).expect("Worker should construct");
    let shutdown_worker = worker.shutdown_handle();

    tokio::task::LocalSet::new()
        .run_until(async move {
            // Notify used to wait until the worker has had at least one run-loop
            // tick, which indirectly confirms RecordWorkerHeartbeat is returning Ok
            // (if it returned Unimplemented the SharedNamespaceWorker would shut
            // down before any work is polled).
            let worker_ready = Arc::new(Notify::new());
            let worker_ready_signal = worker_ready.clone();

            let worker_task = tokio::task::spawn_local(async move {
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

            // Step 5. Tear down cleanly. A graceful worker shutdown ends its
            // long polls, so the server's drain below does not wait for a poll
            // timeout; the worker goes first so the SDK does not race the
            // server into shutdown.
            shutdown_worker();
            tokio::time::timeout(Duration::from_secs(20), worker_task)
                .await
                .expect("echo Worker graceful shutdown timed out")
                .expect("echo Worker task")
                .expect("echo Worker shutdown");

            handle
                .shutdown()
                .await
                .expect("TokeiradHandle::shutdown should drain cleanly at test end");
        })
        .await;
}

#[ignore = "integration test; starts a JWKS endpoint, tokeirad, and an exact-version scoped SDK worker"]
#[tokio::test]
async fn sdk_scoped_worker_roundtrip_uses_production_authorization_bootstrap() {
    tokio::time::timeout(TEST_DEADLINE, run_scoped_worker_integration())
        .await
        .expect("scoped Worker integration test exceeded the overall deadline");
}

async fn run_scoped_worker_integration() {
    // Diagnostics: `RUST_LOG` selects engine and SDK Core tracing output.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
    let (jwks_addr, jwks_shutdown, jwks_task) = start_fixture_jwks().await;
    let config = format!(
        r#"
[policy.authorization]
principal_attribution = true

[[policy.authorization.jwt.issuers]]
name = "integration"
issuer = "https://scoped-worker.fixture.invalid"
jwks_uri = "http://{jwks_addr}/keys"
audience = "tokeira-integration"
permissions_claim = "permissions"

[[policy.authorization.jwt.issuers.worker_scopes]]
match_sub = "integration-worker"
namespace = "default"
task_queues = ["{SCOPED_TASK_QUEUE}"]
deployment_name = "{SCOPED_DEPLOYMENT}"
build_id = "{SCOPED_BUILD_ID}"
"#
    );
    let handle = tokio::time::timeout(
        Duration::from_secs(10),
        TokeiradHandle::start_in_memory_with_toml("127.0.0.1:0".parse().unwrap(), &config),
    )
    .await
    .expect("configured tokeirad startup timed out")
    .expect("configured tokeirad should start");
    let target_url: Url = format!("http://{}", handle.bound_addr())
        .parse()
        .expect("server target");

    let admin_connection = tokio::time::timeout(
        Duration::from_secs(10),
        authenticated_connection(
            target_url.clone(),
            fixture_jwt(ADMIN_JWT_PAYLOAD, ADMIN_JWT_SIGNATURE),
        ),
    )
    .await
    .expect("admin connection timed out");
    let worker_connection = tokio::time::timeout(
        Duration::from_secs(10),
        authenticated_connection(
            target_url,
            fixture_jwt(WORKER_JWT_PAYLOAD, WORKER_JWT_SIGNATURE),
        ),
    )
    .await
    .expect("worker connection timed out");
    let admin_client = Client::new(
        admin_connection.clone(),
        ClientOptions::new("default".to_owned()).build(),
    )
    .expect("admin client");
    let worker_client = Client::new(
        worker_connection.clone(),
        ClientOptions::new("default".to_owned()).build(),
    )
    .expect("worker client");

    // SDK 1.0.0 workers resolve the namespace description before they poll. The
    // scoped credential may describe its own namespace by name and nothing else
    // about namespaces: another name, any ID, and listing stay denied, and the
    // denial for an ID does not depend on whether that ID exists. The Worker
    // below therefore starts on the scoped credential alone.
    let own_namespace = WorkflowService::describe_namespace(
        &mut worker_connection.clone(),
        DescribeNamespaceRequest {
            namespace: "default".to_owned(),
            ..Default::default()
        }
        .into_request(),
    )
    .await
    .expect("scoped identity describes its own namespace by name")
    .into_inner();
    let own_namespace_info = own_namespace
        .namespace_info
        .expect("own-namespace description carries namespace info");
    assert_eq!(own_namespace_info.name, "default");
    assert!(
        own_namespace_info
            .capabilities
            .is_some_and(|capabilities| capabilities.worker_heartbeats),
        "the scoped Worker's heartbeat loop keys off the advertised capability"
    );

    let other_namespace = WorkflowService::describe_namespace(
        &mut worker_connection.clone(),
        DescribeNamespaceRequest {
            namespace: "other-namespace".to_owned(),
            ..Default::default()
        }
        .into_request(),
    )
    .await
    .expect_err("scoped identity must not describe another namespace");
    assert_eq!(other_namespace.code(), Code::PermissionDenied);

    for id in [
        own_namespace_info.id.clone(),
        "00000000-0000-0000-0000-000000000000".to_owned(),
    ] {
        let by_id = WorkflowService::describe_namespace(
            &mut worker_connection.clone(),
            DescribeNamespaceRequest {
                id,
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .expect_err("scoped identity must not describe a namespace by ID");
        assert_eq!(by_id.code(), Code::PermissionDenied);
    }

    let listed = WorkflowService::list_namespaces(
        &mut worker_connection.clone(),
        ListNamespacesRequest::default().into_request(),
    )
    .await
    .expect_err("scoped identity must not list namespaces");
    assert_eq!(listed.code(), Code::PermissionDenied);

    OperatorService::create_nexus_endpoint(
        &mut admin_connection.clone(),
        CreateNexusEndpointRequest {
            spec: Some(EndpointSpec {
                name: SCOPED_NEXUS_ENDPOINT.to_owned(),
                description: None,
                target: Some(EndpointTarget {
                    variant: Some(endpoint_target::Variant::Worker(endpoint_target::Worker {
                        namespace: "default".to_owned(),
                        task_queue: SCOPED_TASK_QUEUE.to_owned(),
                    })),
                }),
            }),
        }
        .into_request(),
    )
    .await
    .expect("admin should create the Worker-backed Nexus endpoint");

    // The SDK's high-level worker derives its task types from what is registered
    // on it and never enables Nexus polling, so the Core worker is built here with
    // Nexus enabled and the scoped deployment version, then shared: the SDK worker
    // wraps it for workflows and activities while this test polls Nexus tasks
    // from it directly.
    let deployment_options = WorkerDeploymentOptions::new(
        WorkerDeploymentVersion::builder()
            .deployment_name(SCOPED_DEPLOYMENT)
            .build_id(SCOPED_BUILD_ID)
            .build(),
    )
    .use_worker_versioning(true)
    .default_versioning_behavior(VersioningBehavior::AutoUpgrade)
    .build();
    let core_runtime =
        CoreRuntime::new_assume_tokio(CoreRuntimeOptions::default()).expect("Core runtime");
    let core_config = WorkerConfig::builder()
        .namespace("default")
        .task_queue(SCOPED_TASK_QUEUE)
        .task_types(WorkerTaskTypes {
            enable_workflows: true,
            enable_local_activities: false,
            enable_remote_activities: true,
            enable_nexus: true,
        })
        .versioning_strategy(WorkerVersioningStrategy::WorkerDeploymentBased(
            deployment_options.clone(),
        ))
        .build()
        .expect("scoped Core worker config");
    let core_worker = Arc::new(
        init_worker(
            &core_runtime,
            core_config,
            worker_client.connection().clone(),
        )
        .expect("scoped Core worker"),
    );
    let worker_options = WorkerOptions::new(SCOPED_TASK_QUEUE)
        // The SDK completes workflow tasks with the versioning behaviour from its
        // own options, so they must agree with the Core worker's poll identity.
        .deployment_options(deployment_options)
        .register_activities(ScopedIntegrationActivities)
        .register_workflow::<ScopedIntegrationWorkflow>()
        .expect("ScopedIntegrationWorkflow registers")
        .build();
    let mut worker = Worker::new_from_core_options(
        Arc::clone(&core_worker),
        worker_client.options().clone(),
        worker_options,
    )
    .expect("scoped Worker");
    let nexus_worker = Arc::clone(&core_worker);
    let shutdown_worker = worker.shutdown_handle();

    tokio::task::LocalSet::new()
        .run_until(async move {
            let worker_task = tokio::task::spawn_local(async move { worker.run().await });
            let nexus_task = tokio::task::spawn_local(async move {
                let polled = nexus_worker
                    .poll_nexus_task()
                    .await
                    .expect("scoped Nexus poll");
                let Some(nexus_task::Variant::Task(task)) = polled.variant else {
                    panic!("the first Nexus poll must deliver the scoped task");
                };
                nexus_worker
                    .complete_nexus_task(NexusTaskCompletion {
                        task_token: task.task_token,
                        status: Some(nexus_task_completion::Status::Completed(NexusResponse {
                            variant: Some(nexus_response::Variant::StartOperation(
                                StartOperationResponse {
                                    variant: Some(start_operation_response::Variant::SyncSuccess(
                                        start_operation_response::Sync {
                                            payload: None,
                                            links: Vec::new(),
                                        },
                                    )),
                                },
                            )),
                        })),
                    })
                    .await
                    .expect("scoped Nexus completion");
            });

            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let request = SetWorkerDeploymentCurrentVersionRequest {
                        namespace: "default".to_owned(),
                        deployment_name: SCOPED_DEPLOYMENT.to_owned(),
                        build_id: SCOPED_BUILD_ID.to_owned(),
                        identity: "integration-admin".to_owned(),
                        allow_no_pollers: true,
                        ignore_missing_task_queues: true,
                        ..Default::default()
                    };
                    if WorkflowService::set_worker_deployment_current_version(
                        &mut admin_connection.clone(),
                        request.into_request(),
                    )
                    .await
                    .is_ok()
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("Worker Deployment should become routable");

            let describe = tokio::time::timeout(
                Duration::from_secs(10),
                describe_task_queue(&mut worker_connection.clone(), SCOPED_TASK_QUEUE),
            )
            .await
            .expect("scoped readiness DescribeTaskQueue timed out")
            .expect("scoped readiness DescribeTaskQueue");
            let current = describe
                .versioning_info
                .and_then(|info| info.current_deployment_version)
                .expect("DQT should expose current Worker Deployment version");
            assert_eq!(current.deployment_name, SCOPED_DEPLOYMENT);
            assert_eq!(current.build_id, SCOPED_BUILD_ID);

            let wrong_queue = tokio::time::timeout(
                Duration::from_secs(10),
                describe_task_queue(&mut worker_connection.clone(), "other-task-queue"),
            )
            .await
            .expect("wrong-queue DescribeTaskQueue timed out")
            .expect_err("second queue must be outside the credential scope");
            assert_eq!(wrong_queue.code(), Code::PermissionDenied);

            for (task_queue, build_id) in [
                ("other-task-queue", SCOPED_BUILD_ID),
                (SCOPED_TASK_QUEUE, "other-build"),
            ] {
                let denied_poll = WorkflowService::poll_workflow_task_queue(
                    &mut worker_connection.clone(),
                    PollWorkflowTaskQueueRequest {
                        namespace: "default".to_owned(),
                        task_queue: Some(TaskQueue {
                            name: task_queue.to_owned(),
                            ..Default::default()
                        }),
                        identity: "scoped-negative-poll".to_owned(),
                        worker_instance_key: "scoped-negative-poll".to_owned(),
                        deployment_options: Some(ProtoWorkerDeploymentOptions {
                            deployment_name: SCOPED_DEPLOYMENT.to_owned(),
                            build_id: build_id.to_owned(),
                            worker_versioning_mode: WorkerVersioningMode::Versioned as i32,
                        }),
                        ..Default::default()
                    }
                    .into_request(),
                )
                .await
                .expect_err("queue/version mismatch must deny before long-poll registration");
                assert_eq!(denied_poll.code(), Code::PermissionDenied);
            }

            let denied_start = tokio::time::timeout(
                Duration::from_secs(10),
                worker_client.start_workflow(
                    ScopedIntegrationWorkflow::run,
                    SCOPED_NEXUS_ENDPOINT.to_owned(),
                    WorkflowStartOptions::new(SCOPED_TASK_QUEUE, "scoped-denied-start").build(),
                ),
            )
            .await
            .expect("scoped start denial timed out");
            match denied_start {
                Err(WorkflowStartError::Rpc(status)) => {
                    assert_eq!(status.code(), Code::PermissionDenied);
                }
                Err(error) => panic!("unexpected scoped start error: {error}"),
                Ok(_) => panic!("scoped identity must not start workflows"),
            }

            let handle_start = admin_client
                .start_workflow(
                    ScopedIntegrationWorkflow::run,
                    SCOPED_NEXUS_ENDPOINT.to_owned(),
                    WorkflowStartOptions::new(SCOPED_TASK_QUEUE, "scoped-worker-roundtrip").build(),
                )
                .await
                .expect("admin workflow start");
            let result = tokio::time::timeout(
                PER_WORKFLOW_TIMEOUT,
                handle_start.get_result(WorkflowGetResultOptions::default()),
            )
            .await
            .expect("scoped workflow did not complete")
            .expect("scoped workflow result");
            assert_eq!(result, "activity-complete");
            nexus_task.await.expect("scoped Nexus task");

            // The Rust SDK's high-level worker does not run a Nexus handler
            // loop. This test's Core bridge must therefore perform the
            // language-SDK responsibility of continuing to poll until Core
            // reports shutdown after the handled task.
            let nexus_shutdown_poll = tokio::task::spawn_local(async move {
                assert!(matches!(
                    core_worker.poll_nexus_task().await,
                    Err(PollError::ShutDown)
                ));
            });
            shutdown_worker();
            tokio::time::timeout(Duration::from_secs(20), worker_task)
                .await
                .expect("scoped Worker graceful shutdown timed out")
                .expect("scoped Worker task")
                .expect("scoped Worker shutdown");
            nexus_shutdown_poll
                .await
                .expect("Nexus bridge should observe Core shutdown");
        })
        .await;

    handle.shutdown().await.expect("server shutdown");
    let _ = jwks_shutdown.send(());
    jwks_task.await.expect("JWKS server task");
}

async fn authenticated_connection(target: Url, jwt: String) -> Connection {
    let options = ConnectionOptions::new(target)
        .identity("scoped-worker-integration".to_owned())
        .headers(HashMap::from([(
            "authorization".to_owned(),
            format!("Bearer {jwt}"),
        )]))
        .client_name("tokeira-integration".to_owned())
        .client_version("0.1.0".to_owned())
        .build();
    Connection::connect(options)
        .await
        .expect("authenticated SDK connection")
}

async fn describe_task_queue(
    connection: &mut Connection,
    task_queue: &str,
) -> Result<
    temporalio_common::protos::temporal::api::workflowservice::v1::DescribeTaskQueueResponse,
    Status,
> {
    WorkflowService::describe_task_queue(
        connection,
        DescribeTaskQueueRequest {
            namespace: "default".to_owned(),
            task_queue: Some(TaskQueue {
                name: task_queue.to_owned(),
                ..Default::default()
            }),
            task_queue_type: TaskQueueType::Workflow as i32,
            report_stats: true,
            ..Default::default()
        }
        .into_request(),
    )
    .await
    .map(|response| response.into_inner())
}

fn fixture_jwt(payload: &str, signature: &str) -> String {
    format!("{JWT_HEADER}.{payload}.{signature}")
}

async fn start_fixture_jwks() -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("JWKS listener");
    let address = listener.local_addr().expect("JWKS address");
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {
                result = listener.accept() => Some(result.expect("JWKS accept")),
                _ = &mut shutdown_rx => None,
            };
            let Some((mut stream, _)) = accepted else {
                break;
            };
            let mut request = vec![0; 4096];
            let _ = stream.read(&mut request).await.expect("JWKS request");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                FIXTURE_JWKS.len(),
                FIXTURE_JWKS
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("JWKS response");
        }
    });
    (address, shutdown_tx, task)
}
