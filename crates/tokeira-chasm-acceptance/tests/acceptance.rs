//! One public-builder scenario; the process-global authentication hook is local
//! to this test binary. All state assertions follow synchronous post-commit work.
#![cfg(test)]

#[path = "support/commands.rs"]
mod commands;

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use http::HeaderMap;
use prost::Message;
use tokeira_auth::{AuthError, ClaimMapper, Claims, DefaultAuthorizer, Role, WorkerScope};
use tokeira_chasm::{ExecutionKey, archetype_id_for_fqn};
use tokeira_chasm_acceptance::{
    AcceptanceLibrary, OperationOutcome, ReconcileInput, Resource, commands::AcceptanceError,
};
use tokeira_edge::{PolicyAuthenticator, translate::to_internal::namespace_id_for};
use tokeira_engine::{
    EmbeddedEngineConfig, Engine, InProcessGrpcRequest,
    chasm::{Component, ComponentRef, DeploymentVersionTarget},
    harness::{self, HarnessHooks},
};
use tokeira_proto::{
    enums::WorkerVersioningMode,
    failure::{ApplicationFailureInfo, Failure, failure::FailureInfo},
    public::temporal::api::deployment::v1::WorkerDeploymentOptions,
    taskqueue::TaskQueue,
    workflowservice::{
        ListWorkflowExecutionsRequest, ListWorkflowExecutionsResponse,
        PollActivityTaskQueueRequest, PollActivityTaskQueueResponse,
        RespondActivityTaskCompletedRequest, RespondActivityTaskFailedRequest,
    },
};

const QUEUE: &str = "acceptance-worker-queue";

struct HeaderClaims;

impl ClaimMapper for HeaderClaims {
    // Match the trait's boxed-future ABI without adding an async-trait dependency
    // to the independent acceptance crate for this one test adapter.
    fn get_claims<'a, 'b, 'c, 'f>(
        &'a self,
        token: &'b str,
        _: &'c str,
    ) -> Pin<Box<dyn Future<Output = Result<Claims, AuthError>> + Send + 'f>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        Self: 'f,
    {
        Box::pin(async move {
            match token {
                "v1" | "v2" => Ok(Claims {
                    subject: format!("worker-{token}"),
                    worker_scope: Some(
                        WorkerScope::try_new(
                            "default".into(),
                            vec![QUEUE.into()],
                            "acceptance".into(),
                            token.into(),
                        )
                        .unwrap(),
                    ),
                    ..Default::default()
                }),
                // Unscoped is still authenticated: DefaultAuthorizer requires a
                // writer role for polling (it claims work) and visibility. No attenuation.
                "observer" => Ok(Claims {
                    subject: "observer".into(),
                    system: Role::WRITER,
                    ..Default::default()
                }),
                _ => Ok(Claims::default()),
            }
        })
    }
}

fn request<M: Message>(rpc: &str, identity: &str, message: M) -> InProcessGrpcRequest {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", identity.parse().unwrap());
    InProcessGrpcRequest {
        service: "temporal.api.workflowservice.v1.WorkflowService".into(),
        rpc: rpc.into(),
        headers,
        proto: message.encode_to_vec().into(),
    }
}

async fn poll(engine: &Engine, identity: &str) -> PollActivityTaskQueueResponse {
    let response = engine
        .endpoint()
        .call(request(
            "PollActivityTaskQueue",
            identity,
            PollActivityTaskQueueRequest {
                namespace: "default".into(),
                task_queue: Some(TaskQueue {
                    name: QUEUE.into(),
                    ..Default::default()
                }),
                identity: identity.into(),
                worker_instance_key: format!("instance-{identity}"),
                deployment_options: (identity != "observer").then(|| WorkerDeploymentOptions {
                    worker_versioning_mode: WorkerVersioningMode::Versioned as i32,
                    deployment_name: "acceptance".into(),
                    build_id: identity.into(),
                }),
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    PollActivityTaskQueueResponse::decode(response.proto.as_slice()).expect("poll response")
}

#[tokio::test]
async fn resource_reconciles_through_scoped_workers_and_the_public_builder() {
    harness::install(HarnessHooks {
        fallback_grpc_authenticator: Some(Arc::new(PolicyAuthenticator::new(
            Arc::new(HeaderClaims),
            Arc::new(DefaultAuthorizer),
            false,
        ))),
        force_chasm_timer_sweeper: true,
        ..Default::default()
    });
    let mut config = EmbeddedEngineConfig::default();
    config
        .server
        .policy
        .compatibility
        .enable_standalone_activities = true;
    assert!(
        !config
            .server
            .policy
            .compatibility
            .enable_standalone_activity_callbacks
    );
    // Token provenance expires against wall time. Freeze CHASM at this test's
    // start; deterministic replay itself uses the runtime harness's virtual time.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let engine = Engine::builder(config)
        .library::<AcceptanceLibrary>()
        .clock(Arc::new(move || now))
        .build()
        .await
        .unwrap();
    let handle = engine.chasm::<Resource>().unwrap();
    let key = ExecutionKey::new(
        namespace_id_for("default").0.to_string(),
        "builder-resource",
        "00000000-0000-0000-0000-000000000013",
    );
    let reference = ComponentRef::new(
        key.clone(),
        archetype_id_for_fqn(Resource::FQN),
        Default::default(),
        vec![],
        Default::default(),
    );
    let target = DeploymentVersionTarget {
        deployment_name: "acceptance".into(),
        build_id: "v1".into(),
    };
    assert_eq!(
        commands::create(&handle, key.clone(), "r1", "initial", target.clone(), QUEUE)
            .await
            .unwrap(),
        1
    );
    let candidate = ExecutionKey {
        run_id: "00000000-0000-0000-0000-000000000014".into(),
        ..key.clone()
    };
    assert_eq!(
        commands::create(
            &handle,
            candidate.clone(),
            "r1",
            "initial",
            target.clone(),
            QUEUE
        )
        .await
        .unwrap(),
        1
    );
    assert!(matches!(
        commands::create(
            &handle,
            candidate.clone(),
            "r1",
            "different",
            target.clone(),
            QUEUE
        )
        .await,
        Err(AcceptanceError::Conflict)
    ));
    assert!(
        matches!(commands::create(&handle, candidate, "r2", "initial", target, QUEUE).await, Err(AcceptanceError::AlreadyStarted { run_id }) if run_id == key.run_id)
    );
    assert_eq!(
        commands::update(&handle, &reference, 1, "generation-two")
            .await
            .unwrap(),
        2
    );
    assert!(matches!(
        commands::update(&handle, &reference, 1, "rejected").await,
        Err(AcceptanceError::GenerationMismatch {
            expected: 1,
            actual: 2
        })
    ));
    // Empty polls fall through to matching's long-poll timeout. Advance only
    // transport time automatically; CHASM stays on the injected clock and no
    // reconciliation assertion depends on a background sweep.
    tokio::time::pause();
    assert!(poll(&engine, "observer").await.task_token.is_empty());
    assert!(poll(&engine, "v2").await.task_token.is_empty());
    tokio::time::resume();
    let task = poll(&engine, "v1").await;
    assert!(!task.task_token.is_empty());
    assert_eq!(task.activity_id, "builder-resource/gen-2/try-1");
    let input = task.input.unwrap();
    assert_eq!(
        ReconcileInput::decode(input.payloads[0].data.as_slice()).unwrap(),
        ReconcileInput {
            generation: 2,
            digest: "generation-two".into()
        }
    );
    engine
        .endpoint()
        .call(request(
            "RespondActivityTaskCompleted",
            "v1",
            RespondActivityTaskCompletedRequest {
                namespace: "default".into(),
                task_token: task.task_token,
                identity: "v1".into(),
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    let view = commands::read(&handle, &reference).await.unwrap();
    assert_eq!(view.observed_generation, 2);
    assert_eq!(view.history.len(), 1);
    assert_eq!(view.history[0].outcome(), OperationOutcome::Completed);
    assert!(view.active_operation.is_none());
    assert_eq!(
        commands::update(&handle, &reference, 2, "generation-three")
            .await
            .unwrap(),
        3
    );
    let task = poll(&engine, "v1").await;
    assert!(!task.task_token.is_empty());
    // Check the wrong release while provenance is still held; a successful worker
    // response consumes it, so testing afterward would only prove missing evidence.
    let denied = engine
        .endpoint()
        .call(request(
            "RespondActivityTaskCompleted",
            "v2",
            RespondActivityTaskCompletedRequest {
                namespace: "default".into(),
                task_token: task.task_token.clone(),
                ..Default::default()
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code() as i32, 7);
    let failure = Failure {
        message: "worker failed generation three".into(),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: "acceptance".into(),
                non_retryable: false,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    engine
        .endpoint()
        .call(request(
            "RespondActivityTaskFailed",
            "v1",
            RespondActivityTaskFailedRequest {
                namespace: "default".into(),
                task_token: task.task_token,
                identity: "v1".into(),
                failure: Some(failure.clone()),
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    let view = commands::read(&handle, &reference).await.unwrap();
    assert_eq!(
        (
            view.desired_generation,
            view.observed_generation,
            view.retry_attempt
        ),
        (3, 2, 1)
    );
    assert_eq!(view.history.len(), 2);
    assert_eq!(view.last_failure, failure.encode_to_vec());
    assert_eq!(view.status, "Failed");
    let response = engine
        .endpoint()
        .call(request(
            "ListWorkflowExecutions",
            "observer",
            ListWorkflowExecutionsRequest {
                namespace: "default".into(),
                query: "DesiredGeneration = 3 AND ObservedGeneration = 2".into(),
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    assert!(
        ListWorkflowExecutionsResponse::decode(response.proto.as_slice())
            .unwrap()
            .executions
            .is_empty()
    );
    engine.shutdown().await.unwrap();
}
