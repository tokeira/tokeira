use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use anyhow::{anyhow, Result};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    BasicKernel, Command, SignalRequest, StartRequest, StartWorkflowTaskRequest,
    WorkflowTaskCompletedRequest,
};
use tokeira_storage::{CommitResult, DispatchableWorkflowTask, RunRepository};
use tokeira_types::{
    ExecutionRef, QueueKey, RunKey, TaskQueueName, WorkerIdentity, WorkflowTaskToken,
};

use crate::{
    broker::InMemoryBroker,
    lane::{spawn_lane, LaneHandle},
};

/// Public runtime facade.
///
/// This is intentionally small. The point is to expose the core server actions
/// without dragging transport or authentication into the same crate.
pub struct TokeiraRuntime<R> {
    repo: Arc<R>,
    broker: InMemoryBroker,
    lanes: Vec<LaneHandle>,
}

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    pub fn new(repo: Arc<R>, lane_count: usize) -> Self {
        let lanes = (0..lane_count.max(1))
            .map(|_| spawn_lane(BasicKernel::default(), repo.clone()))
            .collect();
        Self {
            repo,
            broker: InMemoryBroker::default(),
            lanes,
        }
    }

    pub fn broker(&self) -> InMemoryBroker {
        self.broker.clone()
    }

    pub async fn start_workflow(&self, request: StartRequest) -> Result<CommitResult> {
        let result = self.submit(request.run_key, Command::Start(request)).await?;
        self.publish_pending_workflow_task(&result).await;
        Ok(result)
    }

    pub async fn signal_workflow(
        &self,
        execution: ExecutionRef,
        request: SignalRequest,
    ) -> Result<CommitResult> {
        let run_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or_else(|| anyhow!("execution not found"))?;
        let result = self.submit(run_key, Command::Signal(request)).await?;
        self.publish_pending_workflow_task(&result).await;
        Ok(result)
    }

    pub async fn poll_workflow_task(
        &self,
        queue: QueueKey,
        worker_identity: WorkerIdentity,
        timeout_after: tokio::time::Duration,
    ) -> Result<Option<StartedWorkflowTask>> {
        let offered = match self
            .broker
            .poll_workflow_task(&queue, &worker_identity, timeout_after)
            .await?
        {
            Some(task) => task,
            None => return Ok(None),
        };

        let started = self.start_polled_workflow_task(offered, worker_identity).await?;
        Ok(Some(started))
    }

    pub async fn complete_workflow_task(
        &self,
        req: WorkflowTaskCompletedRequest,
    ) -> Result<CommitResult> {
        let run_key = req.token.run_key;
        let result = self.submit(run_key, Command::WorkflowTaskCompleted(req)).await?;
        self.publish_pending_workflow_task(&result).await;
        Ok(result)
    }

    async fn start_polled_workflow_task(
        &self,
        offered: DispatchableWorkflowTask,
        worker_identity: WorkerIdentity,
    ) -> Result<StartedWorkflowTask> {
        let now = OffsetDateTime::now_utc();
        let request = StartWorkflowTaskRequest {
            logical_seq: offered.logical_seq,
            worker_identity: worker_identity.clone(),
            sticky_ttl: Some(Duration::seconds(30)),
            now,
        };
        let result = self
            .submit(offered.run_key, Command::WorkflowTaskStarted(request))
            .await?;

        let new_state = match result {
            CommitResult::Applied { new_state } => new_state,
            CommitResult::Conflict { reason } => {
                return Err(anyhow!(
                    "failed to start workflow task due to conflict: {reason}"
                ))
            }
            CommitResult::Duplicate => {
                return Err(anyhow!("unexpected duplicate while starting workflow task"))
            }
        };

        let pending = new_state
            .pending_workflow_task
            .clone()
            .ok_or_else(|| anyhow!("workflow task missing after start"))?;
        let started_event_id = pending
            .started_event_id
            .ok_or_else(|| anyhow!("workflow task started without started_event_id"))?;

        let token = WorkflowTaskToken {
            run_key: new_state.run_key,
            logical_seq: pending.logical_seq,
            started_event_id,
            attempt: pending.attempt,
            shard_epoch: tokeira_types::ShardEpoch::ZERO,
        };

        Ok(StartedWorkflowTask {
            run_key: new_state.run_key,
            workflow_id: new_state.workflow_id,
            task_queue: new_state.task_queue,
            token,
        })
    }

    async fn submit(&self, run_key: RunKey, command: Command) -> Result<CommitResult> {
        let lane = self.pick_lane(run_key);
        lane.submit(run_key, command).await
    }

    fn pick_lane(&self, run_key: RunKey) -> &LaneHandle {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        run_key.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % self.lanes.len();
        &self.lanes[idx]
    }

    /// Sweep helper used by recovery/admin flows.
    ///
    /// Hot-path code should prefer `publish_pending_workflow_task`, which
    /// republishes at most the newly-eligible task from the affected run. The
    /// explicit sweep path is still useful for repair and manual testing.
    pub async fn republish_queue(&self, queue: QueueKey, limit: usize) -> Result<usize> {
        let tasks = self.repo.list_dispatchable_workflow_tasks(&queue, limit).await?;
        let count = tasks.len();
        for task in tasks {
            self.broker.publish_workflow_task(task).await;
        }
        Ok(count)
    }

    async fn publish_pending_workflow_task(&self, result: &CommitResult) {
        let CommitResult::Applied { new_state } = result else {
            return;
        };
        let Some(pending) = &new_state.pending_workflow_task else {
            return;
        };
        if pending.started_event_id.is_some() {
            return;
        }

        self.broker
            .publish_workflow_task(DispatchableWorkflowTask {
                run_key: new_state.run_key,
                queue: workflow_queue_for(new_state),
                logical_seq: pending.logical_seq,
                sticky_preferred: new_state.sticky.as_ref().map(|s| s.worker_identity.clone()),
                sticky_expires_at: new_state.sticky.as_ref().map(|s| s.expires_at),
            })
            .await;
    }
}

#[derive(Clone, Debug)]
pub struct StartedWorkflowTask {
    pub run_key: RunKey,
    pub workflow_id: tokeira_types::WorkflowId,
    pub task_queue: TaskQueueName,
    pub token: WorkflowTaskToken,
}

fn workflow_queue_for(state: &tokeira_kernel::WorkflowState) -> QueueKey {
    QueueKey {
        namespace_id: state.namespace_id,
        task_queue: state.task_queue.clone(),
        task_kind: tokeira_types::TaskKind::Workflow,
        deployment: None,
        build_id: None,
    }
}
