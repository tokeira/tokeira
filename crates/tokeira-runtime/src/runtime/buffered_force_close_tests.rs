//! Force-close retries preserve failure attempts through polling and materialization.

use super::{
    completion_inline_tests::completion,
    discovery_tests::stop,
    paused_workflow_task_tests::{Fixture, at, context, history_shape},
    *,
};
use tokeira_kernel::{
    ContinueAsNewAdvicePolicy, StickySpec, UpdateProtocolBody, WorkflowTaskFailedCause,
    WorkflowTaskFailedRequest, WorkflowTaskType, limits::MAXIMUM_BUFFERED_EVENTS_BATCH,
};

#[derive(Clone, Copy, PartialEq)]
enum TaskKindUnderTest {
    Normal,
    Sticky,
    Transient,
    Speculative,
}

async fn force_close_history(kind: TaskKindUnderTest) {
    let f = Fixture::new().await;
    let first = f.poll_and_start(1).await;
    let mut expected: Vec<String> = [
        "ExecutionStarted",
        "Scheduled(1,workflow-q)",
        "Started(2,1)",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    match kind {
        TaskKindUnderTest::Normal => {}
        TaskKindUnderTest::Sticky => {
            let mut request = completion(&first, true);
            request.now = at(2);
            request.request = context("sticky", 2);
            request.force_new_workflow_task = true;
            request.sticky = Some(StickySpec {
                queue: TaskQueueName("cache".into()),
                schedule_to_start_timeout: Duration::seconds(5),
            });
            f.runtime
                .complete_workflow_task_with_result(request)
                .await
                .unwrap();
            expected.extend(
                ["Completed(2,3)", "Scheduled(1,cache)", "Started(5,1)"]
                    .into_iter()
                    .map(str::to_owned),
            );
        }
        TaskKindUnderTest::Transient => {
            f.apply(Command::WorkflowTaskFailed(WorkflowTaskFailedRequest {
                logical_seq: first.token.logical_seq,
                started_event_id: first.token.started_event_id,
                failure_cause: WorkflowTaskFailedCause::NonDeterminismError,
                failure_details: None,
                worker_identity: WorkerIdentity("worker".into()),
                request: context("failure", 2),
                now: at(2),
                history_size_bytes: 0,
                advice_policy: ContinueAsNewAdvicePolicy::V1_31_0,
                reset_reapply: Vec::new(),
                terminate_reason: None,
            }))
            .await;
            f.poll_and_start(3).await;
            expected.push("Failed(2,3)".into());
        }
        TaskKindUnderTest::Speculative => {
            f.runtime
                .update_workflow(
                    ExecutionRef {
                        namespace_id: first.origin.namespace_id,
                        workflow_id: first.workflow_id.clone(),
                        run_id: Some(first.run_id),
                    },
                    "update".into(),
                    "handler".into(),
                    Payloads::default(),
                    context("update", 2),
                    Duration::seconds(1),
                    UpdateWaitPolicy::Admitted,
                    0,
                )
                .await
                .unwrap();
            let mut request = completion(&first, true);
            request.now = at(3);
            request.request = context("complete-speculative", 3);
            f.runtime
                .complete_workflow_task_with_result(request)
                .await
                .unwrap();
            assert_eq!(
                f.state().await.pending_workflow_task.unwrap().task_type,
                WorkflowTaskType::Speculative
            );
            expected.push("Completed(2,3)".into());
        }
    }
    let before = f.state().await;
    let pending = before.pending_workflow_task.as_ref().unwrap();
    let scheduled_id = pending.scheduled_event_id;
    let started_id = pending.started_event_id.unwrap();
    if kind == TaskKindUnderTest::Speculative {
        expected.extend(
            ["Scheduled(1,workflow-q)", "Started(5,1)"]
                .into_iter()
                .map(str::to_owned),
        );
    }
    if kind != TaskKindUnderTest::Transient {
        expected.push(format!("Failed({scheduled_id},{started_id})"));
    }
    for index in 0..=MAXIMUM_BUFFERED_EVENTS_BATCH {
        f.signal(10 + index as i64).await;
        expected.push("Signal(buffered)".into());
    }
    let retry_scheduled_id = expected.len() as i64 + 1;
    let attempt = if kind == TaskKindUnderTest::Sticky {
        1
    } else {
        pending.attempt + 1
    };
    if attempt == 1 {
        expected.push("Scheduled(1,workflow-q)".into());
    }
    let history = f.history().await;
    assert_eq!(
        history_shape(&history),
        expected,
        "history after the force-close"
    );
    let force_closes: Vec<_> = history
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                HistoryEventKind::WorkflowTaskFailed {
                    failure_cause: WorkflowTaskFailedCause::ForceCloseCommand,
                    ..
                }
            )
        })
        .collect();
    assert_eq!(
        force_closes.len(),
        usize::from(kind != TaskKindUnderTest::Transient)
    );
    if kind == TaskKindUnderTest::Speculative {
        assert_eq!(history[4].happened_at, pending.scheduled_at);
        assert_eq!(history[5].happened_at, pending.started_at.unwrap());
    }
    let state = f.state().await;
    assert!(state.buffered_events.is_empty());
    assert!(state.sticky.is_none());
    assert_eq!(
        state.workflow_task_attempts_since_last_success,
        before.workflow_task_attempts_since_last_success
            + u32::from(kind != TaskKindUnderTest::Sticky)
    );
    let next = f.poll_and_start(200).await;
    assert_eq!(next.token.attempt, attempt);
    if attempt == 1 {
        expected.push(format!("Started({retry_scheduled_id},1)"));
    }
    assert_eq!(
        history_shape(&f.history().await),
        expected,
        "history after polling and starting"
    );
    if attempt > 1 {
        let mut request = completion(&next, false);
        request.now = at(201);
        request.request = context("complete-retry", 201);
        if kind == TaskKindUnderTest::Speculative {
            request.commands.push(WorkflowCommand::ProtocolMessage {
                message_id: "reject".into(),
                body: UpdateProtocolBody::Rejected {
                    update_id: "update".into(),
                    failure: Payload::new(Vec::new()),
                },
            });
        }
        f.runtime
            .complete_workflow_task_with_result(request)
            .await
            .unwrap();
        expected.push(format!("Scheduled({attempt},workflow-q)"));
        expected.push(format!("Started({retry_scheduled_id},{attempt})"));
        expected.push(format!(
            "Completed({retry_scheduled_id},{})",
            retry_scheduled_id + 1
        ));
        let history = f.history().await;
        assert_eq!(
            history_shape(&history),
            expected,
            "history after transient materialization"
        );
        assert_eq!(
            history[(retry_scheduled_id - 1) as usize].happened_at,
            at(110)
        );
        assert_eq!(history[retry_scheduled_id as usize].happened_at, at(200));
    }
    stop(&f.runtime).await;
}

#[tokio::test]
async fn buffered_force_close_history_normal() {
    force_close_history(TaskKindUnderTest::Normal).await;
}
#[tokio::test]
async fn buffered_force_close_history_sticky() {
    force_close_history(TaskKindUnderTest::Sticky).await;
}
#[tokio::test]
async fn buffered_force_close_history_transient() {
    force_close_history(TaskKindUnderTest::Transient).await;
}
#[tokio::test]
async fn buffered_force_close_history_speculative() {
    force_close_history(TaskKindUnderTest::Speculative).await;
}
