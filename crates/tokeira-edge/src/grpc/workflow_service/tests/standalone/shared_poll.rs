//! Shared activity polls exercise both arrival orders with deterministic time.

use super::*;
use std::{collections::VecDeque, future::Future, pin::Pin, task::Poll};

#[derive(Default)]
struct SharedPollRuntime {
    tasks: tokio::sync::Mutex<VecDeque<tokeira_runtime::StartedActivityTask>>,
    available: Notify,
    polls: std::sync::atomic::AtomicUsize,
    start_barriers: Mutex<Option<(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>)>>,
}

impl SharedPollRuntime {
    async fn poll(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<tokeira_runtime::StartedActivityTask>> {
        self.poll_with_wake(timeout, Box::pin(std::future::pending()))
            .await
    }

    async fn poll_with_wake(
        &self,
        timeout: std::time::Duration,
        mut wake: Pin<Box<dyn Future<Output = ()> + Send + '_>>,
    ) -> Result<Option<tokeira_runtime::StartedActivityTask>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let available = self.available.notified();
            tokio::pin!(available);
            available.as_mut().enable();
            let task = self.tasks.lock().await.pop_front();
            if let Some(task) = task {
                let barriers = self.start_barriers.lock().unwrap().take();
                if let Some((entered, release)) = barriers {
                    entered.wait().await;
                    release.wait().await;
                }
                return Ok(Some(task));
            }
            tokio::select! {
                _ = &mut wake => return Ok(None),
                _ = available => {},
                _ = tokio::time::sleep_until(deadline) => return Ok(None),
            }
        }
    }

    async fn publish(&self) {
        let queue = tokeira_types::QueueKey {
            namespace_id: crate::translate::to_internal::namespace_id_for("default"),
            task_queue: TaskQueueName(QUEUE.into()),
            task_kind: tokeira_types::TaskKind::Activity,
            deployment: None,
            build_id: None,
        };
        let run_key = RunKey(uuid::Uuid::new_v4());
        self.tasks
            .lock()
            .await
            .push_back(tokeira_runtime::StartedActivityTask {
                replayed: false,
                priority: None,
                run_key,
                run_id: tokeira_types::RunId(uuid::Uuid::new_v4()),
                activity_id: "workflow-activity".into(),
                activity_type: "Activity".into(),
                task_queue: queue.task_queue.clone(),
                token: tokeira_types::ActivityTaskToken {
                    run_key,
                    activity_id: "workflow-activity".into(),
                    schedule_event_id: 5,
                    attempt: 1,
                    shard_epoch: tokeira_types::ShardEpoch(1),
                },
                input: tokeira_types::Payloads::default(),
                attempt: 1,
                workflow_id: "workflow".into(),
                workflow_type: "Workflow".into(),
                header: None,
                retry_policy: None,
                heartbeat_details: None,
                scheduled_time: time::OffsetDateTime::UNIX_EPOCH,
                current_attempt_scheduled_time: None,
                started_time: time::OffsetDateTime::UNIX_EPOCH,
                schedule_to_close_timeout: None,
                start_to_close_timeout: None,
                heartbeat_timeout: None,
                origin: tokeira_types::WorkerTaskOrigin::from_queue_key(
                    &queue,
                    queue.task_queue.clone(),
                    WorkerTaskClass::Activity,
                ),
            });
        self.available.notify_waiters();
    }
}

fn fixture(runtime: Arc<SharedPollRuntime>) -> Harness {
    Harness::with_runtime_store(
        ActivityConfig {
            enable_standalone: true,
            ..Default::default()
        },
        Arc::new(EmptyVisibilityApi),
        runtime,
        Arc::new(InMemoryChasmNodeStore::new()),
    )
}

fn request() -> Request<workflowservice::PollActivityTaskQueueRequest> {
    Request::new(workflowservice::PollActivityTaskQueueRequest {
        namespace: "default".into(),
        task_queue: Some(tokeira_proto::taskqueue::TaskQueue {
            name: QUEUE.into(),
            ..Default::default()
        }),
        identity: "worker".into(),
        ..Default::default()
    })
}

async fn pending<F: Future>(mut future: Pin<&mut F>) -> bool {
    std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await
}

async fn fresh_arrival(h: &Harness) {
    let entered = tokio::time::Instant::now();
    let poll = h.grpc.poll_activity_task_queue(request());
    tokio::pin!(poll);
    assert!(pending(poll.as_mut()).await);
    h.bridge
        .start(h.start_request("fresh", "00000000-0000-4000-8000-000000000001"))
        .await
        .unwrap();
    let response = poll.await.unwrap().into_inner();
    assert_eq!(
        response.activity_id,
        "fresh",
        "the waiting poll must serve the arrival; elapsed {:?}",
        entered.elapsed()
    );
    assert!(entered.elapsed() < std::time::Duration::from_secs(1));
}

#[tokio::test(start_paused = true)]
async fn shared_poll_fresh_queue_standalone_arrival() {
    fresh_arrival(&fixture(Arc::new(SharedPollRuntime::default()))).await;
}

#[tokio::test(start_paused = true)]
async fn shared_poll_seen_queue_workflow_arrival() {
    let runtime = Arc::new(SharedPollRuntime::default());
    let h = fixture(runtime.clone());
    h.bridge
        .start(h.start_request("previous", "00000000-0000-4000-8000-000000000002"))
        .await
        .unwrap();
    let task = h
        .bridge
        .poll_activity_task(QUEUE, "worker", None)
        .await
        .unwrap()
        .unwrap();
    h.bridge
        .respond_activity_task_completed(
            &task.task_token,
            &h.namespace,
            Vec::new(),
            "worker".into(),
        )
        .await
        .unwrap();
    let entered = tokio::time::Instant::now();
    let poll = h.grpc.poll_activity_task_queue(request());
    tokio::pin!(poll);
    assert!(pending(poll.as_mut()).await);
    runtime.publish().await;
    let first = poll.await.unwrap().into_inner();
    let first_elapsed = entered.elapsed();
    let second = h
        .grpc
        .poll_activity_task_queue(request())
        .await
        .unwrap()
        .into_inner();
    let third = h
        .grpc
        .poll_activity_task_queue(request())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        first.activity_id,
        "workflow-activity",
        "three polls: {:?}; elapsed {:?}; workflow tasks still queued: {}",
        [&first.activity_id, &second.activity_id, &third.activity_id],
        entered.elapsed(),
        runtime.tasks.lock().await.len()
    );
    assert!(first_elapsed < std::time::Duration::from_secs(1));
    assert!(second.task_token.is_empty() && third.task_token.is_empty());
}

#[tokio::test(start_paused = true)]
async fn shared_poll_restart_without_pending_dispatch() {
    let old = fixture(Arc::new(SharedPollRuntime::default()));
    old.bridge
        .start(old.start_request("previous", "00000000-0000-4000-8000-000000000003"))
        .await
        .unwrap();
    let task = old
        .bridge
        .poll_activity_task(QUEUE, "worker", None)
        .await
        .unwrap()
        .unwrap();
    old.bridge
        .respond_activity_task_completed(
            &task.task_token,
            &old.namespace,
            Vec::new(),
            "worker".into(),
        )
        .await
        .unwrap();
    let h = Harness::with_runtime_store(
        ActivityConfig {
            enable_standalone: true,
            ..Default::default()
        },
        Arc::new(EmptyVisibilityApi),
        Arc::new(SharedPollRuntime::default()),
        old.store.clone(),
    );
    let rebuilt = tokeira_runtime::chasm::OutboxRebuildScanner::new(
        h.store.clone(),
        h.engine.clone(),
        h.mux.clone(),
    )
    .rebuild_once()
    .await
    .unwrap();
    assert_eq!(rebuilt.effects_dispatched, 0);
    fresh_arrival(&h).await;
}

#[async_trait]
impl WorkflowRuntimeApi for SharedPollRuntime {
    async fn start_workflow(
        &self,
        _req: tokeira_kernel::StartRequest,
    ) -> Result<WorkflowMutationOutcome> {
        unreachable!()
    }

    async fn start_workflow_with_policy(
        &self,
        _req: tokeira_kernel::StartRequest,
    ) -> Result<tokeira_runtime::StartWorkflowResult> {
        unreachable!()
    }

    async fn start_workflow_with_policy_and_callback_limit(
        &self,
        _req: tokeira_kernel::StartRequest,
        _completion_callback_limit: usize,
    ) -> Result<tokeira_runtime::StartWorkflowResult> {
        unreachable!()
    }

    async fn signal_with_start_workflow(
        &self,
        _req: tokeira_kernel::SignalWithStartRequest,
    ) -> Result<tokeira_runtime::SignalWithStartResult> {
        unreachable!()
    }

    async fn signal_workflow(
        &self,
        _run_key: tokeira_types::RunKey,
        _req: tokeira_kernel::SignalRequest,
    ) -> Result<WorkflowMutationOutcome> {
        unreachable!()
    }

    async fn poll_workflow_task(
        &self,
        _queue: tokeira_types::QueueKey,
        _worker_identity: tokeira_types::WorkerIdentity,
        _timeout: std::time::Duration,
    ) -> Result<Option<tokeira_runtime::StartedWorkflowTask>> {
        Ok(None)
    }

    async fn complete_workflow_task(
        &self,
        _req: tokeira_kernel::WorkflowTaskCompletedRequest,
    ) -> Result<WorkflowMutationOutcome> {
        unreachable!()
    }

    async fn poll_activity_task(
        &self,
        _queue: tokeira_types::QueueKey,
        _worker_identity: tokeira_types::WorkerIdentity,
        timeout: std::time::Duration,
    ) -> Result<Option<tokeira_runtime::StartedActivityTask>> {
        self.poll(timeout).await
    }

    async fn poll_activity_task_until_woken<'a>(
        &'a self,
        _queue: tokeira_types::QueueKey,
        _worker_identity: tokeira_types::WorkerIdentity,
        timeout: std::time::Duration,
        _caller_deadline: Option<tokio::time::Instant>,
        wake: Pin<Box<dyn Future<Output = ()> + Send + 'a>>,
    ) -> Result<Option<tokeira_runtime::StartedActivityTask>> {
        self.poll_with_wake(timeout, wake).await
    }

    async fn complete_activity_task(
        &self,
        _token: tokeira_types::ActivityTaskToken,
        _result: tokeira_types::Payloads,
        _worker_identity: Option<tokeira_types::WorkerIdentity>,
        _request: tokeira_types::RequestContext,
    ) -> Result<WorkflowMutationOutcome> {
        unreachable!()
    }

    async fn fail_activity_task(
        &self,
        _token: tokeira_types::ActivityTaskToken,
        _failure: tokeira_types::Payload,
        _failure_error_type: Option<String>,
        _is_non_retryable: bool,
        _last_heartbeat_details: Option<tokeira_types::Payloads>,
        _worker_identity: Option<tokeira_types::WorkerIdentity>,
        _request: tokeira_types::RequestContext,
    ) -> Result<()> {
        unreachable!()
    }

    async fn record_activity_heartbeat(
        &self,
        _token: tokeira_types::ActivityTaskToken,
        _details: Option<tokeira_types::Payloads>,
        _identity: Option<tokeira_types::WorkerIdentity>,
    ) -> Result<tokeira_runtime::ActivityHeartbeatOutcome> {
        unreachable!()
    }

    async fn terminate_workflow(
        &self,
        _run_key: tokeira_types::RunKey,
        _req: tokeira_kernel::TerminateRequest,
    ) -> Result<WorkflowMutationOutcome> {
        unreachable!()
    }

    async fn cancel_workflow(
        &self,
        _run_key: tokeira_types::RunKey,
        _req: tokeira_kernel::CancelRequest,
    ) -> Result<WorkflowMutationOutcome> {
        unreachable!()
    }

    async fn reset_workflow(
        &self,
        _execution: tokeira_types::ExecutionRef,
        _req: tokeira_kernel::ResetRequest,
    ) -> Result<tokeira_runtime::ResetWorkflowResult> {
        unreachable!()
    }

    async fn query_workflow(
        &self,
        _execution: tokeira_types::ExecutionRef,
        _query_type: String,
        _query_args: tokeira_types::Payloads,
        _timeout: std::time::Duration,
    ) -> Result<tokeira_runtime::QueryResult> {
        unreachable!()
    }

    async fn update_workflow(
        &self,
        _execution: tokeira_types::ExecutionRef,
        _update_id: String,
        _update_name: String,
        _input: tokeira_types::Payloads,
        _request: tokeira_types::RequestContext,
        _timeout: std::time::Duration,
        _wait_policy: tokeira_runtime::UpdateWaitPolicy,
        _request_bytes: u64,
    ) -> Result<tokeira_runtime::UpdateLifecycleSnapshot> {
        unreachable!()
    }

    async fn pending_update_transports(
        &self,
        _run_key: tokeira_types::RunKey,
        _include_sent: bool,
    ) -> Result<Vec<tokeira_runtime::PendingUpdateTransport>> {
        Ok(Vec::new())
    }

    async fn resolve_update_transport(
        &self,
        _run_key: tokeira_types::RunKey,
        _update_id: String,
        _resolution: tokeira_runtime::UpdateTransportResolution,
    ) -> Result<bool> {
        Ok(false)
    }
    async fn peek_update_info(
        &self,
        _run_key: tokeira_types::RunKey,
        _update_id: String,
    ) -> Result<Option<(String, tokeira_types::Payloads)>> {
        Ok(None)
    }

    async fn resolve_nexus_operation(
        &self,
        _run_key: tokeira_types::RunKey,
        _operation_id: String,
        _scheduled_event_id: i64,
        __expected_attempt: Option<u32>,
        _resolution: NexusResolution,
    ) -> Result<bool> {
        Ok(false)
    }

    async fn record_nexus_cancellation_attempt(
        &self,
        _run_key: tokeira_types::RunKey,
        _operation_id: String,
        _scheduled_event_id: i64,
        _requested_event_id: i64,
        _expected_attempt: u32,
        _outcome: tokeira_kernel::NexusCancellationAttemptOutcome,
    ) -> Result<bool> {
        Ok(false)
    }
}

#[tokio::test(start_paused = true)]
async fn shared_poll_workflow_start_keeps_standalone_queued() {
    let runtime = Arc::new(SharedPollRuntime::default());
    let h = fixture(runtime.clone());
    let entered = Arc::new(tokio::sync::Barrier::new(2));
    let release = Arc::new(tokio::sync::Barrier::new(2));
    *runtime.start_barriers.lock().unwrap() = Some((entered.clone(), release.clone()));
    let poll = h.grpc.poll_activity_task_queue(request());
    tokio::pin!(poll);
    assert!(pending(poll.as_mut()).await);
    runtime.publish().await;
    let arrival = async {
        entered.wait().await;
        h.bridge
            .start(h.start_request("during-start", "00000000-0000-4000-8000-000000000004"))
            .await
            .unwrap();
        release.wait().await;
    };
    let (response, ()) = tokio::join!(poll, arrival);
    assert_eq!(
        response.unwrap().into_inner().activity_id,
        "workflow-activity"
    );
    assert_eq!(
        h.grpc
            .poll_activity_task_queue(request())
            .await
            .unwrap()
            .into_inner()
            .activity_id,
        "during-start"
    );
}

#[derive(Default)]
struct PickupBarrier {
    barriers: Mutex<Option<(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>)>>,
}

#[async_trait]
impl tokeira_runtime::chasm::VisibilitySink for PickupBarrier {
    async fn record(
        &self,
        _key: &ExecutionKey,
        _archetype: u32,
        _version: tokeira_chasm::VersionedTransition,
        _snapshot: tokeira_chasm::VisibilitySnapshot,
    ) -> anyhow::Result<()> {
        let barriers = self.barriers.lock().unwrap().take();
        if let Some((entered, release)) = barriers {
            entered.wait().await;
            release.wait().await;
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn shared_poll_standalone_start_keeps_workflow_queued() {
    let runtime = Arc::new(SharedPollRuntime::default());
    let sink = Arc::new(PickupBarrier::default());
    let h = Harness::with_runtime_store_sink(
        ActivityConfig {
            enable_standalone: true,
            ..Default::default()
        },
        Arc::new(EmptyVisibilityApi),
        runtime.clone(),
        Arc::new(InMemoryChasmNodeStore::new()),
        sink.clone(),
    );
    let poll = h.grpc.poll_activity_task_queue(request());
    tokio::pin!(poll);
    assert!(pending(poll.as_mut()).await);
    h.bridge
        .start(h.start_request("starting", "00000000-0000-4000-8000-000000000005"))
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Barrier::new(2));
    let release = Arc::new(tokio::sync::Barrier::new(2));
    *sink.barriers.lock().unwrap() = Some((entered.clone(), release.clone()));
    let arrival = async {
        entered.wait().await;
        runtime.publish().await;
        release.wait().await;
    };
    let (response, ()) = tokio::join!(poll, arrival);
    assert_eq!(response.unwrap().into_inner().activity_id, "starting");
    assert_eq!(
        h.grpc
            .poll_activity_task_queue(request())
            .await
            .unwrap()
            .into_inner()
            .activity_id,
        "workflow-activity"
    );
}

#[tokio::test(start_paused = true)]
async fn shared_poll_delayed_retry_waits_until_release() {
    let runtime = Arc::new(SharedPollRuntime::default());
    let h = fixture(runtime.clone());
    h.bridge
        .start(h.start_request("retry", "00000000-0000-4000-8000-000000000006"))
        .await
        .unwrap();
    let first = h
        .grpc
        .poll_activity_task_queue(request())
        .await
        .unwrap()
        .into_inner();
    let entered = tokio::time::Instant::now();
    let poll = h.grpc.poll_activity_task_queue(request());
    tokio::pin!(poll);
    assert!(pending(poll.as_mut()).await);
    h.bridge
        .respond_activity_task_failed(
            &first.task_token,
            &h.namespace,
            "retry".into(),
            tokeira_proto::failure::Failure {
                message: "retry".into(),
                failure_info: Some(
                    tokeira_proto::failure::failure::FailureInfo::ApplicationFailureInfo(
                        tokeira_proto::failure::ApplicationFailureInfo::default(),
                    ),
                ),
                ..Default::default()
            }
            .encode_to_vec(),
            Vec::new(),
            "worker".into(),
        )
        .await
        .unwrap();
    assert!(pending(poll.as_mut()).await);
    assert_eq!(runtime.polls.load(Ordering::SeqCst), 1);
    h.advance(SEC - 1);
    tokio::time::advance(std::time::Duration::from_nanos((SEC - 1) as u64)).await;
    assert!(pending(poll.as_mut()).await);
    h.advance(1);
    tokio::time::advance(std::time::Duration::from_nanos(1)).await;
    let task = poll.await.unwrap().into_inner();
    assert_eq!(task.activity_id, "retry");
    assert_eq!(task.attempt, 2);
    assert_eq!(entered.elapsed(), std::time::Duration::from_secs(1));
}

#[tokio::test(start_paused = true)]
async fn shared_poll_unrelated_dispatches_keep_waiting_until_deadline() {
    let runtime = Arc::new(SharedPollRuntime::default());
    let h = fixture(runtime.clone());
    let entered = tokio::time::Instant::now();
    let poll = h.grpc.poll_activity_task_queue(request());
    tokio::pin!(poll);
    assert!(pending(poll.as_mut()).await);
    let mut other = h.start_request("other-queue", "00000000-0000-4000-8000-000000000007");
    other.task_queue = "another-queue".into();
    h.bridge.start(other).await.unwrap();
    assert!(pending(poll.as_mut()).await);
    let mut targeted = h.start_request("other-target", "00000000-0000-4000-8000-000000000008");
    targeted.version_target = Some(DeploymentVersionTarget {
        deployment_name: "deployment".into(),
        build_id: "build".into(),
    });
    h.bridge.start(targeted).await.unwrap();
    assert!(pending(poll.as_mut()).await);
    assert!(poll.await.unwrap().into_inner().task_token.is_empty());
    assert_eq!(entered.elapsed(), crate::long_poll::TASK_POLL_TIMEOUT);
    assert_eq!(runtime.polls.load(Ordering::SeqCst), 1);
    assert!(
        h.bridge
            .poll_activity_task("another-queue", "worker", None)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        h.bridge
            .poll_activity_task(
                QUEUE,
                "worker",
                Some(&DeploymentVersionTarget {
                    deployment_name: "deployment".into(),
                    build_id: "build".into()
                })
            )
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test(start_paused = true)]
async fn shared_poll_disabled_standalone_preserves_workflow_poll() {
    let runtime = Arc::new(SharedPollRuntime::default());
    let h = Harness::with_runtime_store(
        ActivityConfig::default(),
        Arc::new(EmptyVisibilityApi),
        runtime.clone(),
        Arc::new(InMemoryChasmNodeStore::new()),
    );
    let poll = h.grpc.poll_activity_task_queue(request());
    tokio::pin!(poll);
    assert!(pending(poll.as_mut()).await);
    runtime.publish().await;
    assert_eq!(
        poll.await.unwrap().into_inner().activity_id,
        "workflow-activity"
    );
    let entered = tokio::time::Instant::now();
    assert!(
        h.grpc
            .poll_activity_task_queue(request())
            .await
            .unwrap()
            .into_inner()
            .task_token
            .is_empty()
    );
    assert_eq!(entered.elapsed(), crate::long_poll::TASK_POLL_TIMEOUT);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    // Both ready kinds are served once, with standalone priority and no dropped task.
    #[test]
    fn shared_poll_ready_kinds_preserve_priority(standalone_first in any::<bool>(), delay in 0u64..1000) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let runtime = Arc::new(SharedPollRuntime::default());
            let h = fixture(runtime.clone());
            if !standalone_first { runtime.publish().await; }
            h.advance(i64::try_from(delay).unwrap());
            tokio::time::advance(std::time::Duration::from_nanos(delay)).await;
            h.bridge.start(h.start_request("ready", "00000000-0000-4000-8000-000000000009")).await.unwrap();
            if standalone_first { runtime.publish().await; }
            let first = h.grpc.poll_activity_task_queue(request()).await.unwrap().into_inner();
            let second = h.grpc.poll_activity_task_queue(request()).await.unwrap().into_inner();
            prop_assert_eq!(first.activity_id, "ready");
            prop_assert_eq!(second.activity_id, "workflow-activity");
            prop_assert!(runtime.tasks.lock().await.is_empty());
            prop_assert!(h.bridge.poll_activity_task(QUEUE, "worker", None).await.unwrap().is_none());
            Ok(())
        })?;
    }
}

#[tokio::test(start_paused = true)]
async fn shared_poll_scoped_wait_serves_only_the_admitted_target() {
    let runtime = Arc::new(SharedPollRuntime::default());
    let h = fixture(runtime.clone());
    let target = DeploymentVersionTarget {
        deployment_name: "deployment".into(),
        build_id: "own".into(),
    };
    let other = DeploymentVersionTarget {
        build_id: "other".into(),
        ..target.clone()
    };
    let mut previous = h.start_request("previous", "00000000-0000-4000-8000-000000000010");
    previous.version_target = Some(target.clone());
    h.bridge.start(previous).await.unwrap();
    let previous = h
        .bridge
        .poll_activity_task(QUEUE, "worker", Some(&target))
        .await
        .unwrap()
        .unwrap();
    h.bridge
        .respond_activity_task_completed(
            &previous.task_token,
            &h.namespace,
            Vec::new(),
            "worker".into(),
        )
        .await
        .unwrap();
    let provenance = Arc::new(tokeira_storage::InMemoryStore::default());
    let grpc = scoped_grpc_with_runtime(&h, &target, provenance.clone(), runtime);
    let mut req = request();
    req.get_mut().worker_instance_key = "instance".into();
    req.get_mut().deployment_options = Some(WorkerDeploymentOptions {
        worker_versioning_mode: WorkerVersioningMode::Versioned as i32,
        deployment_name: target.deployment_name.clone(),
        build_id: target.build_id.clone(),
    });
    let poll = grpc.poll_activity_task_queue(req);
    tokio::pin!(poll);
    assert!(pending(poll.as_mut()).await);
    h.bridge
        .start(h.start_request("unscoped", "00000000-0000-4000-8000-000000000011"))
        .await
        .unwrap();
    let mut wrong = h.start_request("wrong", "00000000-0000-4000-8000-000000000012");
    wrong.version_target = Some(other.clone());
    h.bridge.start(wrong).await.unwrap();
    assert!(pending(poll.as_mut()).await);
    let mut own = h.start_request("own", "00000000-0000-4000-8000-000000000013");
    own.version_target = Some(target.clone());
    h.bridge.start(own).await.unwrap();
    let served = poll.await.unwrap().into_inner();
    assert_eq!(served.activity_id, "own");
    assert!(
        provenance
            .get(tokeira_storage::worker_task_token_digest(
                &served.task_token
            ))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        h.bridge
            .poll_activity_task(QUEUE, "worker", None)
            .await
            .unwrap()
            .unwrap()
            .activity_id,
        "unscoped"
    );
    assert_eq!(
        h.bridge
            .poll_activity_task(QUEUE, "worker", Some(&other))
            .await
            .unwrap()
            .unwrap()
            .activity_id,
        "wrong"
    );
}

#[tokio::test(start_paused = true)]
async fn shared_poll_empty_queue_ends_at_deadline() {
    let runtime = Arc::new(SharedPollRuntime::default());
    let h = fixture(runtime.clone());
    let entered = tokio::time::Instant::now();
    assert!(
        h.grpc
            .poll_activity_task_queue(request())
            .await
            .unwrap()
            .into_inner()
            .task_token
            .is_empty()
    );
    assert_eq!(entered.elapsed(), crate::long_poll::TASK_POLL_TIMEOUT);
    assert_eq!(runtime.polls.load(Ordering::SeqCst), 1);
}
