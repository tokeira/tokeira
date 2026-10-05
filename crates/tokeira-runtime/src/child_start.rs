//! Retries for a child workflow's start and for its confirmation to the parent.
//!
//! v1.31.0's transfer queue retries `processStartChildExecution` until it reaches a
//! definitive outcome, and records a failed child start only for an existing workflow or a
//! missing namespace (`service/history/transfer_queue_active_task_executor.go:1032-1074 @
//! v1.31.0`). The publisher drives a child start through [`start_child`] and its
//! confirmation through [`confirm_child_start`], reaching lanes and storage through
//! [`ChildStartIo`] (`runtime-child-workflows` Requirements 1.5, 1.7 and 7).

use std::time::Duration;

use tokeira_kernel::{ChildWorkflowState, Reject};
use tokeira_storage::CommitResult;
use tokeira_types::{RunKey, WorkflowId};

use crate::lane::KernelRejected;

/// v1.31.0's `CreateTaskReschedulePolicy`: the first retry waits 1 s, each later one 1.1
/// times longer, up to 3 minutes, with no expiry (`common/util.go:69-71, 225-230 @
/// v1.31.0`).
#[derive(Clone, Debug)]
pub(crate) struct TaskRescheduleBackoff {
    next: Duration,
}

impl TaskRescheduleBackoff {
    const INITIAL: Duration = Duration::from_secs(1);
    const COEFFICIENT: f64 = 1.1;
    const MAX: Duration = Duration::from_secs(3 * 60);

    pub(crate) fn new() -> Self {
        Self {
            next: Self::INITIAL,
        }
    }

    /// The wait before the next retry.
    pub(crate) fn next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = delay.mul_f64(Self::COEFFICIENT).min(Self::MAX);
        delay
    }
}

/// What one submission to a lane returned, reduced to what the retries decide on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Submitted {
    /// `Applied` or `Duplicate`: the command took effect, on this attempt or an earlier one.
    Applied,
    /// The kernel rejected the command because the run already exists.
    RunAlreadyExists,
    /// The kernel rejected the command for another reason.
    Rejected(String),
    /// A running execution holds the workflow id (`CurrentExecutionConflict`).
    CurrentExecution(RunKey),
    /// Nothing was decided: a `Conflict`, or an error from storage or the lane.
    Failed(String),
}

impl Submitted {
    pub(crate) fn from_result(result: anyhow::Result<CommitResult>) -> Self {
        match result {
            Ok(CommitResult::Applied { .. } | CommitResult::Duplicate) => Self::Applied,
            Ok(CommitResult::CurrentExecutionConflict {
                existing_run_key, ..
            }) => Self::CurrentExecution(existing_run_key),
            Ok(CommitResult::Conflict { reason }) => Self::Failed(format!("conflict: {reason}")),
            Err(error) => match error.downcast_ref::<KernelRejected>() {
                Some(KernelRejected(Reject::RunAlreadyExists)) => Self::RunAlreadyExists,
                Some(rejected) => Self::Rejected(rejected.to_string()),
                None => Self::Failed(format!("{error:#}")),
            },
        }
    }
}

/// The lanes, storage and clock a child start needs. The publisher implements it with its
/// lanes and repository; tests script it.
pub(crate) trait ChildStartIo {
    /// Submit the child's `Command::Start`, with the same run key, run id and request id on
    /// every attempt.
    async fn submit_start(&mut self) -> Submitted;
    /// Terminate a running incumbent that a TERMINATE_EXISTING start displaces.
    async fn terminate_incumbent(&mut self, run_key: RunKey);
    /// Whether the parent is open and still holds the child as initiated and unconfirmed.
    async fn parent_awaits_child(&mut self) -> bool;
    /// Submit the `Command::ChildStartConfirmed` to the parent.
    async fn submit_confirmation(&mut self) -> Submitted;
    /// Wait out a backoff delay.
    async fn wait(&mut self, delay: Duration);
}

/// How a child start ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChildStartOutcome {
    /// The child's run exists, from this attempt or an earlier one (Requirements 1.4, 1.7).
    Started,
    /// A running execution holds the workflow id under the FAIL policy (Requirement 1.5).
    AlreadyExists,
    /// The parent stopped awaiting the child before a retry, so there is nothing to start
    /// or confirm (Requirement 7.5).
    Abandoned,
}

/// Attempt the child's start until a definitive outcome (Requirement 7.1).
///
/// A TERMINATE_EXISTING start terminates a running incumbent once and attempts again at
/// once. Every other undecided attempt waits out the backoff and, if the parent still
/// awaits the child, tries again.
pub(crate) async fn start_child(
    io: &mut impl ChildStartIo,
    child_workflow_id: &WorkflowId,
    terminate_on_conflict: bool,
) -> ChildStartOutcome {
    let mut backoff = TaskRescheduleBackoff::new();
    let mut terminated_incumbent = false;
    let mut attempt: u32 = 1;
    loop {
        match io.submit_start().await {
            Submitted::Applied | Submitted::RunAlreadyExists => return ChildStartOutcome::Started,
            Submitted::CurrentExecution(existing_run_key)
                if terminate_on_conflict && !terminated_incumbent =>
            {
                terminated_incumbent = true;
                io.terminate_incumbent(existing_run_key).await;
            }
            Submitted::CurrentExecution(existing_run_key) => {
                tracing::warn!(
                    ?child_workflow_id,
                    ?existing_run_key,
                    "child workflow start hit a running duplicate workflow id"
                );
                return ChildStartOutcome::AlreadyExists;
            }
            Submitted::Rejected(reason) | Submitted::Failed(reason) => {
                let delay = backoff.next_delay();
                tracing::warn!(
                    ?child_workflow_id,
                    attempt,
                    ?delay,
                    %reason,
                    "child workflow start failed; retrying"
                );
                io.wait(delay).await;
                if !io.parent_awaits_child().await {
                    tracing::debug!(
                        ?child_workflow_id,
                        "parent no longer awaits the child; abandoning its start"
                    );
                    return ChildStartOutcome::Abandoned;
                }
            }
        }
        attempt += 1;
    }
}

/// How a confirmation ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConfirmOutcome {
    /// The parent recorded the outcome.
    Delivered,
    /// The kernel rejected it: the parent has closed or no longer awaits this confirmation.
    Rejected,
}

/// Deliver the child start's outcome to the parent, retrying anything but a kernel
/// rejection with the same backoff (Requirement 7.2).
pub(crate) async fn confirm_child_start(
    io: &mut impl ChildStartIo,
    child_workflow_id: &WorkflowId,
) -> ConfirmOutcome {
    let mut backoff = TaskRescheduleBackoff::new();
    let mut attempt: u32 = 1;
    loop {
        match io.submit_confirmation().await {
            Submitted::Applied => return ConfirmOutcome::Delivered,
            Submitted::RunAlreadyExists | Submitted::Rejected(_) => {
                tracing::warn!(
                    ?child_workflow_id,
                    "parent no longer awaits the child start confirmation"
                );
                return ConfirmOutcome::Rejected;
            }
            Submitted::CurrentExecution(_) | Submitted::Failed(_) => {
                let delay = backoff.next_delay();
                tracing::warn!(
                    ?child_workflow_id,
                    attempt,
                    ?delay,
                    "failed to deliver child start confirmation; retrying"
                );
                io.wait(delay).await;
            }
        }
        attempt += 1;
    }
}

/// Whether a parent that is `open` and holds `child` still awaits the start initiated at
/// `initiated_event_id`: the child entry is for that initiation and hasn't started.
pub(crate) fn awaits_child(
    open: bool,
    child: Option<&ChildWorkflowState>,
    initiated_event_id: i64,
) -> bool {
    open && child.is_some_and(|child| {
        child.initiated_event_id == initiated_event_id && child.started_event_id.is_none()
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, future::Future};

    use proptest::prelude::*;
    use tokeira_kernel::ParentClosePolicy;
    use tokeira_types::{ExecutionStatus, NamespaceId, RunId, WorkflowType};

    use super::*;

    /// A scripted [`ChildStartIo`]: each call takes the next scripted answer, and every call
    /// is recorded.
    #[derive(Debug, Default)]
    struct Script {
        starts: VecDeque<Submitted>,
        awaits: VecDeque<bool>,
        confirmations: VecDeque<Submitted>,
        start_calls: usize,
        await_calls: usize,
        confirmation_calls: usize,
        terminated: Vec<RunKey>,
        waits: Vec<Duration>,
    }

    impl ChildStartIo for Script {
        async fn submit_start(&mut self) -> Submitted {
            self.start_calls += 1;
            self.starts.pop_front().expect("a scripted start outcome")
        }

        async fn terminate_incumbent(&mut self, run_key: RunKey) {
            self.terminated.push(run_key);
        }

        async fn parent_awaits_child(&mut self) -> bool {
            self.await_calls += 1;
            self.awaits.pop_front().unwrap_or(true)
        }

        async fn submit_confirmation(&mut self) -> Submitted {
            self.confirmation_calls += 1;
            self.confirmations
                .pop_front()
                .expect("a scripted confirmation outcome")
        }

        async fn wait(&mut self, delay: Duration) {
            self.waits.push(delay);
        }
    }

    fn run<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime")
            .block_on(future)
    }

    fn child() -> WorkflowId {
        WorkflowId("child".to_string())
    }

    /// Outcomes that decide nothing, so the start is retried.
    fn undecided() -> impl Strategy<Value = Submitted> {
        prop_oneof![
            Just(Submitted::Failed("storage unavailable".to_string())),
            Just(Submitted::Failed(
                "conflict: not owner of execution-home shard".to_string()
            )),
            Just(Submitted::Failed("lane OCC retry exhausted".to_string())),
            Just(Submitted::Rejected("kernel rejected command".to_string())),
        ]
    }

    fn rejected(reject: Reject) -> anyhow::Result<CommitResult> {
        Err(anyhow::Error::new(KernelRejected(reject)))
    }

    fn child_entry(initiated_event_id: i64, started_event_id: Option<i64>) -> ChildWorkflowState {
        ChildWorkflowState {
            child_workflow_id: child(),
            namespace_id: NamespaceId::new(),
            namespace: None,
            workflow_type: WorkflowType("child-type".to_string()),
            header: None,
            child_run_id: started_event_id.map(|_| RunId::new()),
            initiated_event_id,
            started_event_id,
            parent_close_policy: ParentClosePolicy::Terminate,
        }
    }

    #[test]
    fn backoff_follows_the_v1_31_task_reschedule_policy() {
        let mut backoff = TaskRescheduleBackoff::new();
        let first: Vec<f64> = (0..3).map(|_| backoff.next_delay().as_secs_f64()).collect();
        for (actual, expected) in first.iter().zip([1.0, 1.1, 1.21]) {
            assert!((actual - expected).abs() < 1e-6, "{first:?}");
        }
        let later = (0..200).map(|_| backoff.next_delay()).last();
        assert_eq!(later, Some(Duration::from_secs(180)));
    }

    #[test]
    fn commit_outcomes_reduce_to_what_the_retries_decide_on() {
        let incumbent = RunKey::new();
        assert_eq!(
            Submitted::from_result(Ok(CommitResult::Duplicate)),
            Submitted::Applied
        );
        assert_eq!(
            Submitted::from_result(Ok(CommitResult::CurrentExecutionConflict {
                existing_run_key: incumbent,
                existing_status: ExecutionStatus::Running,
                request_ids: Vec::new(),
            })),
            Submitted::CurrentExecution(incumbent)
        );
        assert!(matches!(
            Submitted::from_result(Ok(CommitResult::Conflict {
                reason: "not owner of execution-home shard".to_string(),
            })),
            Submitted::Failed(_)
        ));
        assert_eq!(
            Submitted::from_result(rejected(Reject::RunAlreadyExists)),
            Submitted::RunAlreadyExists
        );
        assert!(matches!(
            Submitted::from_result(rejected(Reject::MissingRun)),
            Submitted::Rejected(_)
        ));
        assert!(matches!(
            Submitted::from_result(Err(anyhow::anyhow!("storage unavailable"))),
            Submitted::Failed(_)
        ));
    }

    #[test]
    fn terminate_existing_terminates_the_incumbent_once() {
        let (first, second) = (RunKey::new(), RunKey::new());
        let mut script = Script::default();
        script.starts.extend([
            Submitted::CurrentExecution(first),
            Submitted::CurrentExecution(second),
        ]);
        assert_eq!(
            run(start_child(&mut script, &child(), true)),
            ChildStartOutcome::AlreadyExists
        );
        assert_eq!(script.terminated, vec![first]);
        assert!(script.waits.is_empty());

        let mut script = Script::default();
        script
            .starts
            .extend([Submitted::CurrentExecution(first), Submitted::Applied]);
        assert_eq!(
            run(start_child(&mut script, &child(), true)),
            ChildStartOutcome::Started
        );
    }

    #[test]
    fn a_parent_awaits_only_an_unstarted_child_from_the_same_initiation() {
        assert!(awaits_child(true, Some(&child_entry(7, None)), 7));
        assert!(!awaits_child(false, Some(&child_entry(7, None)), 7));
        assert!(!awaits_child(true, None, 7));
        assert!(!awaits_child(true, Some(&child_entry(8, None)), 7));
        assert!(!awaits_child(true, Some(&child_entry(7, Some(9))), 7));
    }

    proptest! {
        // Feature: runtime-child-workflows, Property 3: Only an existing workflow fails a child start
        #[test]
        fn only_an_existing_workflow_fails_a_child_start(
            undecided_attempts in prop::collection::vec(undecided(), 0..6),
            already_exists in any::<bool>(),
        ) {
            let mut script = Script::default();
            script.starts.extend(undecided_attempts.iter().cloned());
            script.starts.push_back(if already_exists {
                Submitted::CurrentExecution(RunKey::new())
            } else {
                Submitted::Applied
            });

            let outcome = run(start_child(&mut script, &child(), false));

            let expected = if already_exists {
                ChildStartOutcome::AlreadyExists
            } else {
                ChildStartOutcome::Started
            };
            prop_assert_eq!(outcome, expected);
            prop_assert_eq!(script.start_calls, undecided_attempts.len() + 1);
            prop_assert_eq!(script.await_calls, undecided_attempts.len());
            let mut backoff = TaskRescheduleBackoff::new();
            let expected_waits: Vec<_> =
                undecided_attempts.iter().map(|_| backoff.next_delay()).collect();
            prop_assert_eq!(script.waits, expected_waits);
        }

        // Feature: runtime-child-workflows, Property 8: A start that already committed confirms as started
        #[test]
        fn a_start_that_already_committed_confirms_as_started(
            undecided_attempts in prop::collection::vec(undecided(), 1..4),
            duplicate in any::<bool>(),
        ) {
            // The first undecided attempt stands for one that committed but reported an
            // error, so the retry finds the run already there.
            let found = if duplicate {
                Ok(CommitResult::Duplicate)
            } else {
                rejected(Reject::RunAlreadyExists)
            };
            let mut script = Script::default();
            script.starts.extend(undecided_attempts.iter().cloned());
            script.starts.push_back(Submitted::from_result(found));
            script.confirmations.push_back(Submitted::Applied);

            prop_assert_eq!(
                run(start_child(&mut script, &child(), false)),
                ChildStartOutcome::Started
            );
            prop_assert_eq!(
                run(confirm_child_start(&mut script, &child())),
                ConfirmOutcome::Delivered
            );
            prop_assert_eq!(script.confirmation_calls, 1);
        }

        // Feature: runtime-child-workflows, Property 9: Retries stop when the parent stops awaiting the child
        #[test]
        fn retries_stop_when_the_parent_stops_awaiting_the_child(
            undecided_attempts in prop::collection::vec(undecided(), 1..8),
            stop_at in 0usize..8,
        ) {
            let stop_at = stop_at % undecided_attempts.len();
            let mut script = Script::default();
            script.starts.extend(undecided_attempts.iter().cloned());
            script.awaits.extend((0..undecided_attempts.len()).map(|retry| retry < stop_at));

            prop_assert_eq!(
                run(start_child(&mut script, &child(), false)),
                ChildStartOutcome::Abandoned
            );
            prop_assert_eq!(script.start_calls, stop_at + 1);
            prop_assert_eq!(script.confirmation_calls, 0);
        }

        // Feature: runtime-child-workflows, Property 10: Confirmation is retried until applied or rejected
        #[test]
        fn confirmation_is_retried_until_applied_or_rejected(
            failures in prop::collection::vec(
                prop_oneof![
                    Just(Submitted::Failed("parent lane closed".to_string())),
                    Just(Submitted::Failed("conflict: stale shard epoch".to_string())),
                ],
                0..6,
            ),
            rejected in any::<bool>(),
        ) {
            let mut script = Script::default();
            script.confirmations.extend(failures.iter().cloned());
            script.confirmations.push_back(if rejected {
                Submitted::Rejected("kernel rejected command: run closed".to_string())
            } else {
                Submitted::Applied
            });

            let expected = if rejected {
                ConfirmOutcome::Rejected
            } else {
                ConfirmOutcome::Delivered
            };
            prop_assert_eq!(run(confirm_child_start(&mut script, &child())), expected);
            prop_assert_eq!(script.confirmation_calls, failures.len() + 1);
            prop_assert_eq!(script.waits.len(), failures.len());
        }
    }
}
