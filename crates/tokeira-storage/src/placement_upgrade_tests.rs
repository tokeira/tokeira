//! Shared stopped-upgrade reset lineage for both placement backends.

use anyhow::{Result, ensure};
use tokeira_kernel::{
    BasicKernel, HistoryEvent, HistoryEventKind, LoadedRun, ReplayContext, TimerOp, WorkflowState,
};
use tokeira_types::{LogicalTaskSeq, RunId, RunKey, ShardEpoch, WorkerIdentity};
use uuid::Uuid;

use crate::{
    CommitResult, RunRepository,
    memory::projection_accumulator_tests::{fresh_transition, reset_history},
};

/// Materialize a real reset successor with a timer and no subsequent command.
pub(crate) async fn reset_successor(repo: &dyn RunRepository) -> Result<WorkflowState> {
    let mut transition = fresh_transition(RunKey::new());
    let template = transition.next_state.clone();
    let home = tokeira_types::execution_home_bundle(
        template.namespace_id.0.as_bytes(),
        template.workflow_id.0.as_bytes(),
        8,
    );
    let (run_id, key) = (1..256)
        .find_map(|index| {
            let id = RunId(Uuid::from_u128(index));
            let key = RunKey::derive(template.namespace_id, &template.workflow_id, id);
            (((key.0.as_u128() as u32) % 8) != home.0).then_some((id, key))
        })
        .expect("fixture distinguishes homes");
    let mut history = reset_history(&template);
    history.truncate(4);
    for kind in [
        HistoryEventKind::TimerStarted {
            workflow_task_completed_event_id: 4,
            timer_id: "reset-timer".into(),
            fire_at: template.started_at + time::Duration::days(1),
        },
        HistoryEventKind::WorkflowTaskScheduled {
            logical_seq: LogicalTaskSeq(2),
            task_queue: template.task_queue.clone(),
            workflow_task_timeout: template.workflow_task_timeout,
            attempt: 1,
        },
        HistoryEventKind::WorkflowTaskStarted {
            logical_seq: LogicalTaskSeq(2),
            scheduled_event_id: 6,
            attempt: 1,
            identity: WorkerIdentity("reset-worker".into()),
            request_id: "reset-start".into(),
            history_size_bytes: 0,
            suggest_continue_as_new: false,
            suggest_continue_as_new_reasons: vec![],
            target_worker_deployment_version_changed: false,
            target_version_changed_enabled: false,
            target_deployment_version: None,
        },
    ] {
        history.push(HistoryEvent {
            event_id: history.len() as i64 + 1,
            happened_at: template.started_at,
            kind,
        });
    }
    transition.next_state = BasicKernel.replay_history_prefix(
        ReplayContext {
            run_key: template.run_key,
            namespace_id: template.namespace_id,
            workflow_id: template.workflow_id,
            run_id: template.run_id,
            deployment: template.deployment,
            build_id: template.build_id,
            parent_run_key: template.parent_run_key,
            parent_workflow_id: template.parent_workflow_id,
            first_run_started_at: template.first_run_started_at,
        },
        &history,
    )?;
    transition.next_state.transition_seq = template.transition_seq;
    transition.timer_ops = transition
        .next_state
        .timers
        .values()
        .cloned()
        .map(TimerOp::Upsert)
        .collect();
    transition.event_principals = vec![None; history.len()].into();
    transition.history_events = history.into();
    ensure!(
        matches!(
            repo.commit_transition(template.run_key, transition, ShardEpoch::ZERO)
                .await?,
            CommitResult::Applied { .. }
        ),
        "reset source must commit"
    );
    repo.materialize_reset_successor(template.run_key, 7, run_id, Some(template.run_key))
        .await?;
    let LoadedRun::Existing(state) = repo.load_run(key).await? else {
        anyhow::bail!("reset successor missing")
    };
    Ok(state)
}
