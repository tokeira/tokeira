//! The transition check: history-integrity rules every kernel transition must
//! satisfy before it leaves the kernel.
//!
//! The transition builder numbers each history event as it is appended, and a
//! workflow task can hold event ids it has not written yet. A speculative task
//! (from scheduling) and a retry task with attempt > 1 (once a worker started
//! it) are given `last_event_id + 1` for their `WorkflowTaskScheduled` and
//! `+ 2` for their `WorkflowTaskStarted`; the worker holding a started task sees
//! history only up to those ids, and the update request delivered with a
//! speculative task points at the reserved Scheduled id. v1.31.0 keeps those
//! ids valid by structure: external events wait in a buffer during the request,
//! and at transaction close a speculative task is converted before the buffer
//! is flushed (`closeTransaction`, mutable_state_impl.go:7086-7100;
//! `convertSpeculativeWorkflowTaskToNormal`,
//! workflow_task_state_machine.go:1466-1537 @ v1.31.0). The kernel instead
//! decides at append time, so this check refuses a transition in which any
//! path got that wrong, rather than letting it commit:
//!
//! - event ids continue contiguously from the run's `last_event_id`;
//! - a task that stays started across the transition sees no new history;
//! - a task that stays speculative across the transition sees no new history;
//! - no other event takes a task's reserved ids while it still claims them, and
//!   a task that is written lands its Scheduled and Started exactly on them.
//!
//! The check is pure and linear in the transition's events, and runs in release
//! builds: a refused transition commits nothing, the outcome v1.31.0 gives when
//! a speculative task's Scheduled id disagrees with its reserved one ("it could
//! be a bug", workflow_task_state_machine.go:1501-1503 @ v1.31.0).

use tokeira_types::LogicalTaskSeq;

use crate::{
    event::HistoryEventKind,
    kernel::Reject,
    state::{WorkflowState, WorkflowTaskType},
    transition::Transition,
};

/// The parts of a run's pre-transition state the check needs, captured before
/// the state moves into the transition builder.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PriorRun {
    last_event_id: i64,
    task: Option<PriorTask>,
}

/// The pending workflow task as it stood before the transition.
#[derive(Clone, Copy, Debug)]
struct PriorTask {
    logical_seq: LogicalTaskSeq,
    speculative: bool,
    attempt: u32,
    scheduled_event_id: i64,
    started_event_id: Option<i64>,
}

impl PriorRun {
    /// A run that does not exist yet: its first event takes id 1.
    pub(crate) const ABSENT: Self = Self {
        last_event_id: 0,
        task: None,
    };

    pub(crate) fn of(state: &WorkflowState) -> Self {
        Self {
            last_event_id: state.last_event_id,
            task: state
                .pending_workflow_task
                .as_ref()
                .map(|pending| PriorTask {
                    logical_seq: pending.logical_seq,
                    speculative: pending.task_type == WorkflowTaskType::Speculative,
                    attempt: pending.attempt,
                    scheduled_event_id: pending.scheduled_event_id,
                    started_event_id: pending.started_event_id,
                }),
        }
    }
}

impl PriorTask {
    /// Whether the task holds reserved event ids no other event may take.
    ///
    /// A speculative task holds them from scheduling. A retry task holds them
    /// only once started: before that no worker has seen them, so an event may
    /// take a scheduled retry task's id and the task gets new ids when it starts
    /// (`AddWorkflowTaskStartedEvent`, workflow_task_state_machine.go:559-576
    /// @ v1.31.0).
    ///
    /// Only a task whose reserved Scheduled id is still the run's next id is
    /// protected. A task that already lost it did so before this check existed,
    /// in state an earlier engine version wrote; the existing renumbering paths
    /// still heal such a run, where refusing every later transition would wedge
    /// it.
    fn holds_reserved_ids(&self, last_event_id: i64) -> bool {
        let reserves = self.speculative || (self.attempt > 1 && self.started_event_id.is_some());
        reserves && self.scheduled_event_id == last_event_id + 1
    }
}

/// Check `transition` against the run state it was computed from.
pub(crate) fn check_transition(prior: &PriorRun, transition: &Transition) -> Result<(), Reject> {
    let events = &transition.history_events;
    for (offset, event) in events.iter().enumerate() {
        let expected = prior.last_event_id + 1 + offset as i64;
        if event.event_id != expected {
            return Err(violation(
                "event ids continue contiguously from the run's last event",
                format!(
                    "event {offset} has id {}, expected {expected}",
                    event.event_id
                ),
            ));
        }
    }
    let expected_last = prior.last_event_id + events.len() as i64;
    if transition.next_state.last_event_id != expected_last {
        return Err(violation(
            "event ids continue contiguously from the run's last event",
            format!(
                "last_event_id is {}, expected {expected_last}",
                transition.next_state.last_event_id
            ),
        ));
    }

    let Some(task) = prior.task else {
        return Ok(());
    };
    let next_task = transition
        .next_state
        .pending_workflow_task
        .as_ref()
        .filter(|pending| pending.logical_seq == task.logical_seq);

    if !events.is_empty() {
        // A worker holding a started task reads history only up to the task's
        // Started event; anything appended before the task closes lands inside
        // the window it has already been given. Such events buffer instead.
        if task.started_event_id.is_some()
            && next_task.is_some_and(|pending| pending.started_event_id.is_some())
        {
            return Err(violation(
                "a started workflow task's history is frozen until it closes",
                format!(
                    "task {} stayed started while {} event(s) were appended",
                    task.logical_seq.0,
                    events.len()
                ),
            ));
        }
        // The first event appended converts a scheduled speculative task, so
        // one that is still speculative afterwards had an event appended over
        // its reserved Scheduled id.
        if task.speculative
            && next_task.is_some_and(|pending| pending.task_type == WorkflowTaskType::Speculative)
        {
            return Err(violation(
                "a speculative workflow task converts before anything is appended",
                format!(
                    "task {} stayed speculative while {} event(s) were appended",
                    task.logical_seq.0,
                    events.len()
                ),
            ));
        }
    }

    if !task.holds_reserved_ids(prior.last_event_id) {
        return Ok(());
    }
    // A retained retry gets a new reserved Scheduled id; only a task that still
    // claims the old ids keeps other events off them.
    let still_claims = next_task.is_some_and(|pending| {
        pending.scheduled_event_id == task.scheduled_event_id
            && pending.started_event_id == task.started_event_id
    });
    let mut wrote_scheduled = false;
    let mut wrote_started = false;
    for event in events {
        match &event.kind {
            HistoryEventKind::WorkflowTaskScheduled { logical_seq, .. }
                if *logical_seq == task.logical_seq =>
            {
                if event.event_id != task.scheduled_event_id {
                    return Err(reserved_violation(
                        task,
                        format!("its WorkflowTaskScheduled took id {}", event.event_id),
                    ));
                }
                wrote_scheduled = true;
                continue;
            }
            HistoryEventKind::WorkflowTaskStarted { logical_seq, .. }
                if *logical_seq == task.logical_seq =>
            {
                if Some(event.event_id) != task.started_event_id {
                    return Err(reserved_violation(
                        task,
                        format!("its WorkflowTaskStarted took id {}", event.event_id),
                    ));
                }
                wrote_started = true;
                continue;
            }
            // The close event names the task's Scheduled and Started ids,
            // which exist only if this transition wrote them first. A close
            // event whose ids were written falls through to the reserved-id
            // check below like any other event.
            HistoryEventKind::WorkflowTaskCompleted { logical_seq, .. }
            | HistoryEventKind::WorkflowTaskFailed { logical_seq, .. }
            | HistoryEventKind::WorkflowTaskTimedOut { logical_seq, .. }
                if *logical_seq == task.logical_seq
                    && (!wrote_scheduled
                        || (task.started_event_id.is_some() && !wrote_started)) =>
            {
                return Err(reserved_violation(
                    task,
                    format!(
                        "event {} closes it before its reserved events were written",
                        event.event_id
                    ),
                ));
            }
            _ => {}
        }
        if still_claims
            && (event.event_id == task.scheduled_event_id
                || Some(event.event_id) == task.started_event_id)
        {
            return Err(reserved_violation(
                task,
                format!("another event took reserved id {}", event.event_id),
            ));
        }
    }
    Ok(())
}

fn reserved_violation(task: PriorTask, detail: String) -> Reject {
    violation(
        "a workflow task's reserved event ids belong to it",
        format!(
            "task {} reserved {}{}; {detail}",
            task.logical_seq.0,
            task.scheduled_event_id,
            task.started_event_id
                .map(|started| format!(" and {started}"))
                .unwrap_or_default()
        ),
    )
}

fn violation(rule: &str, detail: String) -> Reject {
    Reject::HistoryIntegrity(format!("{rule}: {detail}"))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_types::{LogicalTaskSeq, Payloads, TaskQueueName, WorkerIdentity};

    use super::{PriorRun, check_transition};
    use crate::{
        command::WorkflowTaskFailedCause,
        event::{HistoryEvent, HistoryEventKind},
        kernel::Reject,
        state::{PendingWorkflowTask, WorkflowState, WorkflowTaskType, tests::open_state},
        transition::Transition,
    };

    const SEQ: LogicalTaskSeq = LogicalTaskSeq(7);

    fn task(
        task_type: WorkflowTaskType,
        attempt: u32,
        scheduled: i64,
        started: Option<i64>,
    ) -> PendingWorkflowTask {
        PendingWorkflowTask {
            advice: Default::default(),
            task_type,
            schedule_to_start_deadline: None,
            target_worker_deployment_version_changed: false,
            target_version_changed_enabled: false,
            target_deployment_version: None,
            logical_seq: SEQ,
            scheduled_event_id: scheduled,
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            started_event_id: started,
            started_at: started.map(|_| OffsetDateTime::UNIX_EPOCH),
            attempt,
        }
    }

    fn state(last_event_id: i64, pending: Option<PendingWorkflowTask>) -> WorkflowState {
        let mut state = open_state();
        state.last_event_id = last_event_id;
        state.pending_workflow_task = pending;
        state
    }

    fn event(event_id: i64, kind: HistoryEventKind) -> HistoryEvent {
        HistoryEvent {
            event_id,
            happened_at: OffsetDateTime::UNIX_EPOCH,
            kind,
        }
    }

    fn signaled(event_id: i64) -> HistoryEvent {
        event(
            event_id,
            HistoryEventKind::WorkflowExecutionSignaled {
                signal_name: "sig".into(),
                input: Payloads::default(),
                header: None,
                links: Vec::new(),
                request_id: format!("signal-{event_id}"),
                identity: None,
            },
        )
    }

    fn scheduled(event_id: i64) -> HistoryEvent {
        event(
            event_id,
            HistoryEventKind::WorkflowTaskScheduled {
                logical_seq: SEQ,
                task_queue: TaskQueueName("queue".into()),
                workflow_task_timeout: time::Duration::seconds(10),
                attempt: 1,
            },
        )
    }

    fn failed(event_id: i64, scheduled: i64, started: i64) -> HistoryEvent {
        event(
            event_id,
            HistoryEventKind::WorkflowTaskFailed {
                logical_seq: SEQ,
                scheduled_event_id: scheduled,
                started_event_id: started,
                failure_cause: WorkflowTaskFailedCause::ForceCloseCommand,
                failure_details: None,
                identity: WorkerIdentity("history-service".into()),
                base_run_id: None,
                new_run_id: None,
                fork_event_version: None,
                fork_event_id: None,
            },
        )
    }

    /// A transition whose next state is `next` with `last_event_id` advanced
    /// past `events`, as the transition builder leaves it.
    fn transition(mut next: WorkflowState, events: Vec<HistoryEvent>) -> Transition {
        if let Some(last) = events.last() {
            next.last_event_id = last.event_id;
        }
        let principals = events.iter().map(|_| None).collect();
        Transition {
            expected_seq: next.transition_seq,
            next_state: next,
            history_events: events.into_iter().collect(),
            event_principals: principals,
            request_dedupe_ops: Default::default(),
            activity_ops: Default::default(),
            timer_ops: Default::default(),
            dispatch_ops: Default::default(),
            events_numbered_at_close: 0,
            growth_limits: None,
        }
    }

    fn rule_of(result: Result<(), Reject>) -> String {
        match result {
            Err(Reject::HistoryIntegrity(message)) => message,
            other => panic!("expected a history-integrity rejection, got {other:?}"),
        }
    }

    #[test]
    fn accepts_conversion_of_a_scheduled_speculative_task() {
        let prior = state(4, Some(task(WorkflowTaskType::Speculative, 1, 5, None)));
        let next = state(4, Some(task(WorkflowTaskType::Normal, 1, 5, None)));
        let result = check_transition(
            &PriorRun::of(&prior),
            &transition(next, vec![scheduled(5), signaled(6)]),
        );
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn refuses_an_event_appended_over_a_speculative_task() {
        let prior = state(4, Some(task(WorkflowTaskType::Speculative, 1, 5, None)));
        let next = prior.clone();
        let message = rule_of(check_transition(
            &PriorRun::of(&prior),
            &transition(next, vec![signaled(5)]),
        ));
        assert!(
            message.starts_with("a speculative workflow task converts"),
            "{message}"
        );
    }

    #[test]
    fn refuses_a_foreign_event_at_a_claimed_reserved_id() {
        let prior = state(4, Some(task(WorkflowTaskType::Speculative, 1, 5, None)));
        let next = state(4, Some(task(WorkflowTaskType::Normal, 1, 5, None)));
        let message = rule_of(check_transition(
            &PriorRun::of(&prior),
            &transition(next, vec![signaled(5), signaled(6)]),
        ));
        assert!(
            message.starts_with("a workflow task's reserved event ids"),
            "{message}"
        );
    }

    #[test]
    fn refuses_a_scheduled_event_written_past_its_reserved_id() {
        let prior = state(4, Some(task(WorkflowTaskType::Speculative, 1, 5, None)));
        let next = state(4, Some(task(WorkflowTaskType::Normal, 1, 6, None)));
        let message = rule_of(check_transition(
            &PriorRun::of(&prior),
            &transition(next, vec![signaled(5), scheduled(6)]),
        ));
        assert!(
            message.contains("its WorkflowTaskScheduled took id 6"),
            "{message}"
        );
    }

    #[test]
    fn refuses_closing_a_speculative_task_before_writing_it() {
        let prior = state(4, Some(task(WorkflowTaskType::Speculative, 1, 5, Some(6))));
        let next = state(4, None);
        let message = rule_of(check_transition(
            &PriorRun::of(&prior),
            &transition(next, vec![failed(5, 5, 6), signaled(6)]),
        ));
        assert!(
            message.contains("closes it before its reserved events"),
            "{message}"
        );
    }

    #[test]
    fn refuses_history_appended_under_a_started_task() {
        let prior = state(9, Some(task(WorkflowTaskType::Normal, 1, 8, Some(9))));
        let next = prior.clone();
        let message = rule_of(check_transition(
            &PriorRun::of(&prior),
            &transition(next, vec![signaled(10)]),
        ));
        assert!(
            message.starts_with("a started workflow task's history is frozen"),
            "{message}"
        );
    }

    #[test]
    fn accepts_a_failed_retry_releasing_its_ids() {
        // A failed retry task writes no event; the flushed buffer takes the ids
        // it had reserved and the retained retry gets new ones.
        let prior = state(4, Some(task(WorkflowTaskType::Normal, 2, 5, Some(6))));
        let next = state(4, Some(task(WorkflowTaskType::Normal, 3, 6, None)));
        let result = check_transition(&PriorRun::of(&prior), &transition(next, vec![signaled(5)]));
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn leaves_a_task_that_lost_its_ids_earlier_to_be_renumbered() {
        // Written by an earlier engine version: the speculative task's reserved
        // Scheduled id (5) was already taken. Its start renumbers it.
        let prior = state(5, Some(task(WorkflowTaskType::Speculative, 1, 5, None)));
        let next = state(5, Some(task(WorkflowTaskType::Normal, 1, 6, Some(7))));
        let result = check_transition(&PriorRun::of(&prior), &transition(next, vec![scheduled(6)]));
        assert_eq!(result, Ok(()));
    }

    proptest! {
        // Feature: speculative-wft, Property 10 — the check refuses any
        // transition whose event ids do not continue from the run's last
        // event, and accepts the same transition with ids that do.
        #[test]
        fn ids_must_continue_from_the_last_event(
            last_event_id in 0i64..1_000,
            count in 1usize..6,
            gap in prop_oneof![Just(-1i64), Just(1i64), 2i64..5],
        ) {
            let prior = state(last_event_id, None);
            let contiguous: Vec<HistoryEvent> = (1..=count as i64)
                .map(|offset| signaled(last_event_id + offset))
                .collect();
            let accepted =
                check_transition(&PriorRun::of(&prior), &transition(prior.clone(), contiguous));
            prop_assert_eq!(accepted, Ok(()));

            let shifted: Vec<HistoryEvent> = (1..=count as i64)
                .map(|offset| signaled(last_event_id + offset + gap))
                .collect();
            let refused =
                check_transition(&PriorRun::of(&prior), &transition(prior.clone(), shifted));
            let is_contiguity = matches!(
                &refused,
                Err(Reject::HistoryIntegrity(message)) if message.starts_with("event ids continue")
            );
            prop_assert!(is_contiguity);
        }
    }
}
