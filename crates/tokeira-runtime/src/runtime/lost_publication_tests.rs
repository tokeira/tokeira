//! A failed post-commit publication is offered by the next scheduled discovery pass.

use super::*;
use crate::discovery::{DISCOVERY_PERIOD, QueueHomeProvider};
use std::{future::Future, task::Poll};
use tokeira_storage::InMemoryStore;

pub(super) const DELIVERY_BOUND: std::time::Duration = DISCOVERY_PERIOD.saturating_mul(2);
const POLL_TIMEOUT: std::time::Duration = DELIVERY_BOUND.saturating_mul(10);

pub(super) fn request() -> StartRequest {
    let mut request = tests::sample_start_request(None, None);
    request.workflow_task_timeout = Duration::minutes(5);
    request
}

async fn completion<R: RunRepository + 'static>(
    runtime: &TokeiraRuntime<R>,
    request: StartRequest,
    queue: &QueueKey,
) -> Result<WorkflowTaskCompletedRequest> {
    runtime.start_workflow(request).await?;
    let Some(WorkflowActivation::WorkflowTask(task)) = runtime
        .poll_workflow_activation(
            queue.clone(),
            None,
            WorkerIdentity("setup".into()),
            POLL_TIMEOUT,
        )
        .await?
    else {
        anyhow::bail!("initial workflow task missing")
    };
    Ok(WorkflowTaskCompletedRequest {
        client_discards_speculative_with_events: false,
        token: task.token,
        identity: WorkerIdentity("setup".into()),
        sdk_metadata: None,
        metering_metadata: None,
        worker_version: None,
        versioning_behavior: Default::default(),
        deployment_version: None,
        worker_deployment_name: None,
        sticky: None,
        commands: Vec::new(),
        force_new_workflow_task: false,
        limits: Default::default(),
        delivered_update_ids: Vec::new(),
        request: RequestContext::unattributed(OffsetDateTime::now_utc()),
        now: OffsetDateTime::now_utc(),
        command_sizes: Vec::new(),
    })
}

pub(super) async fn lost_publication<R: RunRepository + 'static>(
    runtime: &TokeiraRuntime<R>,
    request: StartRequest,
    kind: TaskKind,
) -> Result<()> {
    let run_key = request.run_key;
    let mut queue = QueueKey {
        namespace_id: request.namespace_id,
        task_queue: request.task_queue.clone(),
        task_kind: TaskKind::Workflow,
        deployment: None,
        build_id: None,
    };
    let mut completion = completion(runtime, request, &queue).await?;
    queue.task_kind = kind;
    if kind == TaskKind::Activity {
        completion.commands.push(WorkflowCommand::ScheduleActivity {
            activity_id: "lost-publication".into(),
            activity_type: "work".into(),
            task_queue: queue.task_queue.clone(),
            input: Payloads::default(),
            header: None,
            request_eager_execution: false,
            retry_policy: None,
            deployment: None,
            build_id: None,
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: Some(Duration::minutes(5)),
            heartbeat_timeout: None,
            priority: None,
        });
    } else {
        completion.force_new_workflow_task = true;
    }
    let gate = match kind {
        TaskKind::Workflow => &runtime.discovery.0.test_gate,
        TaskKind::Activity => &runtime.activity_discovery.test_gate,
    };
    gate.arm();
    // Match the runtime's workflow poll registration while observing matching's
    // offer, before owner-start I/O. Activity's public offer seam does both.
    let _demand = (kind == TaskKind::Workflow).then(|| {
        runtime.discovery.0.register_poll(
            queue.clone(),
            runtime.discovery.1.local_home(&queue).unwrap(),
            tokio::time::Instant::now(),
        )
    });
    let worker = WorkerIdentity("parked".into());
    let poll = async {
        match kind {
            TaskKind::Workflow => {
                let offered = runtime
                    .broker
                    .poll_workflow_task(&queue, &worker, POLL_TIMEOUT)
                    .await?
                    .ok_or_else(|| anyhow!("workflow poll timed out"))?;
                let WorkflowPollResult::Queued(task, _) = offered else {
                    anyhow::bail!("unexpected reserved delivery");
                };
                Ok::<_, anyhow::Error>(task.run_key)
            }
            TaskKind::Activity => {
                let offered = runtime
                    .poll_activity_task_offer(queue.clone(), worker.clone(), POLL_TIMEOUT)
                    .await?
                    .ok_or_else(|| anyhow!("activity poll timed out"))?;
                Ok(offered.task.run_key)
            }
        }
    };
    tokio::pin!(poll);
    std::future::poll_fn(|cx| {
        assert!(
            poll.as_mut().poll(cx).is_pending(),
            "poll must park before commit"
        );
        Poll::Ready(())
    })
    .await;
    tokio::time::timeout(POLL_TIMEOUT, gate.wait()).await?;
    *runtime
        .outbound_publisher
        .publication_failure
        .lock()
        .unwrap() = Some(kind);
    let result = runtime.complete_workflow_task(completion).await?;
    let committed_at = tokio::time::Instant::now();
    let CommitResult::Applied { new_state } = result else {
        anyhow::bail!("completion did not commit");
    };
    assert_eq!(
        *runtime
            .outbound_publisher
            .publication_failure
            .lock()
            .unwrap(),
        None,
        "the commit must take the actual publish-error path"
    );
    match kind {
        TaskKind::Workflow => {
            let pending = new_state.pending_workflow_task.as_ref().unwrap();
            assert!(
                !runtime
                    .broker
                    .knows_offer((run_key, pending.logical_seq))
                    .await
            );
            assert!(!runtime.broker.has_runnable_backlog(&queue).await);
        }
        TaskKind::Activity => {
            let count = runtime.shard_owner.read().unwrap().shard_count();
            let home = execution_home_bundle(
                new_state.namespace_id.0.as_bytes(),
                new_state.workflow_id.0.as_bytes(),
                count,
            );
            let rows = tokeira_storage::derive_activity_dispatch(&new_state, home)?;
            assert_eq!(rows.len(), 1);
            for row in rows.values() {
                assert!(
                    !runtime
                        .activity_broker
                        .contains_activity_task(&row.task())
                        .await
                );
            }
            assert!(!runtime.activity_broker.has_runnable_backlog(&queue).await);
        }
    }
    std::future::poll_fn(|cx| {
        assert!(
            poll.as_mut().poll(cx).is_pending(),
            "failed publish cannot deliver to the poll"
        );
        Poll::Ready(())
    })
    .await;
    // The empty pass held subsequent scans across the commit solely to make the
    // lost-publication assertion race-free on DSQL. Release preserves its deadline;
    // neither a publication nor an explicit discovery wake is issued here.
    gate.release();
    let delivered = tokio::time::timeout_at(committed_at + DELIVERY_BOUND, poll).await??;
    let elapsed = committed_at.elapsed();
    assert_eq!(delivered, run_key);
    // One period of slack covers scheduler/query completion, never the 20s poll timeout.
    assert!(
        elapsed <= DELIVERY_BOUND,
        "delivery {elapsed:?} exceeds {DELIVERY_BOUND:?}"
    );
    tracing::info!(?kind, ?elapsed, bound = ?DELIVERY_BOUND, "lost publication recovered");
    // Captured by nextest --success-output for the verification receipt.
    #[allow(clippy::print_stderr)] // Test measurement requested by the verification contract.
    {
        eprintln!("{kind:?} lost publication: delivery={elapsed:?}, bound={DELIVERY_BOUND:?}");
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn workflow_dispatch_lost_publication_offered_by_next_pass() {
    memory_lost_publication(TaskKind::Workflow).await;
}

#[tokio::test(start_paused = true)]
async fn activity_dispatch_lost_publication_offered_by_next_pass() {
    memory_lost_publication(TaskKind::Activity).await;
}

async fn memory_lost_publication(kind: TaskKind) {
    let runtime = super::discovery_tests::runtime(
        Arc::new(InMemoryStore::with_shard_count(8)),
        crate::discovery::tests::home(0),
    );
    runtime.ready().await.unwrap();
    lost_publication(&runtime, request(), kind).await.unwrap();
    super::discovery_tests::stop(&runtime).await;
}
