//! Authoritative activity starts across control races and workflow lifecycles.
//! Injected time and repository barriers keep the observed order independent of scheduling.

use std::collections::BTreeMap;

use proptest::prelude::*;
use tokeira_storage::{
    InMemoryStore,
    read_probe::{ReadProbe, ReadReport},
};

use super::{
    repair_tests::{Point, Repo},
    *,
};
use crate::{ActivityStartOutcome, ActivityTaskStartRequest, test_clock::TestClock};

fn request_context(now: OffsetDateTime) -> RequestContext {
    RequestContext {
        request_id: RequestId(Uuid::new_v4().to_string()),
        ..RequestContext::unattributed(now)
    }
}

struct Fixture {
    repo: Arc<Repo>,
    runtime: Arc<TokeiraRuntime<Repo>>,
    start: StartRequest,
    request: ActivityTaskStartRequest,
    workflow_queue: QueueKey,
    input: Payloads,
    header: Option<Headers>,
}

impl Fixture {
    async fn new(data: Vec<u8>, retry: bool, eager: bool, sticky: bool) -> Self {
        let clock: Arc<dyn Clock> = Arc::new(TestClock::new());
        let repo = Arc::new(Repo::with_inner(
            InMemoryStore::default().with_clock(clock.clone()),
        ));
        let runtime = Arc::new(
            timer_deadline_tests::fixture_on(repo.clone(), clock)
                .await
                .with_activity_eager_execution(eager),
        );
        let start = timer_deadline_tests::start(&runtime, false);
        let workflow_queue = QueueKey {
            namespace_id: start.namespace_id,
            task_queue: start.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: None,
            build_id: None,
        };
        runtime.start_workflow(start.clone()).await.unwrap();
        let first = runtime
            .poll_workflow_task(
                workflow_queue.clone(),
                WorkerIdentity("workflow".into()),
                std::time::Duration::from_secs(1),
            )
            .await
            .unwrap()
            .unwrap();
        let input = Payloads(vec![Payload::new(data.clone())]);
        let header = Some(Headers(BTreeMap::from([(
            "trace".into(),
            Payload::new(data),
        )])));
        let mut command = completion_eager_tests::schedule("activity", retry);
        if let WorkflowCommand::ScheduleActivity {
            input: target,
            header: target_header,
            ..
        } = &mut command
        {
            *target = input.clone();
            *target_header = header.clone();
        }
        let mut completion = completion_inline_tests::completion(&first, false);
        completion.now = runtime.clock.now();
        completion.request.received_at = completion.now;
        completion.commands = vec![command];
        if sticky {
            completion.sticky = Some(tokeira_kernel::StickySpec {
                queue: TaskQueueName("sticky".into()),
                schedule_to_start_timeout: Duration::seconds(5),
            });
        }
        let result = runtime
            .complete_workflow_task_with_result(completion)
            .await
            .unwrap();
        assert_eq!(result.activity_tasks.len(), usize::from(eager));
        // Dropping this completed response models reply loss without undoing its commit.
        drop(result);
        let LoadedRun::Existing(state) = repo.load_run(start.run_key).await.unwrap() else {
            panic!("run")
        };
        let request = ActivityTaskStartRequest {
            offered: activity_dispatch_task(start.run_key, &state, &state.activities["activity"]),
            request_id: Uuid::new_v4(),
            worker: WorkerIdentity("activity".into()),
        };
        Self {
            repo,
            runtime,
            start,
            request,
            workflow_queue,
            input,
            header,
        }
    }

    fn execution(&self) -> ExecutionRef {
        ExecutionRef {
            namespace_id: self.start.namespace_id,
            workflow_id: self.start.workflow_id.clone(),
            run_id: Some(self.start.run_id),
        }
    }

    async fn state(&self) -> WorkflowState {
        let LoadedRun::Existing(state) = self.repo.load_run(self.start.run_key).await.unwrap()
        else {
            panic!("run")
        };
        state
    }

    async fn second_workflow_task(&self) -> StartedWorkflowTask {
        let now = self.runtime.clock.now();
        self.runtime
            .submit(
                self.start.run_key,
                Command::Signal(SignalRequest {
                    signal_name: "next".into(),
                    input: Payloads::default(),
                    header: None,
                    links: Vec::new(),
                    request: request_context(now),
                    now,
                }),
            )
            .await
            .unwrap();
        self.runtime
            .poll_workflow_task(
                self.workflow_queue.clone(),
                WorkerIdentity("workflow".into()),
                std::time::Duration::from_secs(1),
            )
            .await
            .unwrap()
            .unwrap()
    }

    async fn poll(&self) -> StartedActivityTask {
        self.runtime
            .poll_activity_task(
                self.request.offered.queue.clone(),
                self.request.worker.clone(),
                std::time::Duration::from_secs(1),
            )
            .await
            .unwrap()
            .unwrap()
    }

    async fn stop(&self) {
        self.runtime.runtime_shutdown.begin_shutdown();
        self.runtime
            .runtime_shutdown
            .wait(std::time::Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
    }
}

fn reads(report: &ReadReport, phase: &'static str) -> usize {
    report.counts.get(&(phase, "logical")).copied().unwrap_or(0)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 100, failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::WithSource("proptest-regressions"))), ..ProptestConfig::default() })]

    // Home zero takes the hinted local route; a legacy publication retains exactly one routing read.
    #[test]
    fn activity_local_hint_zero_read_budget(legacy in any::<bool>(), conflict in any::<bool>(), data in prop::collection::vec(any::<u8>(), 0..64)) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let fixture = Fixture::new(data, false, false, false).await;
            if legacy {
                fixture.runtime.activity_broker.remove_run(fixture.start.run_key).await;
                fixture.runtime.activity_broker.publish_activity_task(fixture.request.offered.clone(), None).await.unwrap();
            }
            let probe = ReadProbe::start(fixture.start.run_key, "local-hint");
            let history_reads = fixture.repo.history_reads.load(std::sync::atomic::Ordering::SeqCst);
            if conflict { probe.inject_conflicts(1); }
            let started = fixture.poll().await;
            let report = probe.report(); drop(probe);
            prop_assert_eq!(reads(&report, "queue-home/activity-owner"), usize::from(legacy));
            prop_assert_eq!(reads(&report, "execution-owner/activity-start"), 1 + usize::from(conflict));
            prop_assert_eq!(reads(&report, "execution-owner/activity-replay"), 0);
            prop_assert_eq!(fixture.repo.history_reads.load(std::sync::atomic::Ordering::SeqCst) - history_reads, 1);
            prop_assert_eq!(started.input, fixture.input.clone());
            prop_assert_eq!(started.header, fixture.header.clone());
            fixture.stop().await;
            Ok(())
        })?;
    }

    // Retained prior-attempt coordinates cannot start a retry or consume its newer stamped obligation.
    #[test]
    fn activity_obsolete_offer_after_retry(data in prop::collection::vec(any::<u8>(), 0..64), conflict in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let fixture = Fixture::new(data, true, false, false).await;
            let first = fixture.poll().await;
            fixture.runtime.fail_activity_task(first.token, Payload::new(Vec::new()), crate::retry::ActivityFailure::Other, None, None, request_context(fixture.runtime.clock.now())).await.unwrap();
            // A retry needs a positive interval (service/history/workflow/retry.go:100-107 @ v1.31.0).
            tokio::time::advance(std::time::Duration::from_nanos(1)).await;
            let current = fixture.state().await;
            let task = activity_dispatch_task(current.run_key, &current, &current.activities["activity"]);
            prop_assert_eq!(task.attempt, fixture.request.offered.attempt + 1);
            for coordinate in 0..3 {
                let mut obsolete = fixture.request.clone(); obsolete.offered = task.clone();
                match coordinate {
                    0 => obsolete.offered.schedule_event_id -= 1,
                    1 => obsolete.offered.attempt = fixture.request.offered.attempt,
                    _ => obsolete.offered.stamp = fixture.request.offered.stamp,
                }
                let probe = ReadProbe::start(current.run_key, "obsolete-attempt");
                prop_assert!(matches!(fixture.runtime.start_forwarded_activity_task(obsolete).await.unwrap(), ActivityStartOutcome::Stale));
                let report = probe.report(); drop(probe);
                prop_assert_eq!(reads(&report, "execution-owner/activity-start"), 1);
                prop_assert_eq!(fixture.state().await, current.clone());
            }
            let probe = ReadProbe::start(current.run_key, "current-retry");
            if conflict { probe.inject_conflicts(1); }
            let second = fixture.poll().await;
            let report = probe.report(); drop(probe);
            prop_assert_eq!(reads(&report, "queue-home/activity-owner"), 0);
            prop_assert_eq!(reads(&report, "execution-owner/activity-start"), 1 + usize::from(conflict));
            prop_assert_eq!(second.attempt, 2);
            prop_assert_eq!(second.input, fixture.input.clone());
            prop_assert_eq!(fixture.state().await.transition_seq, current.transition_seq.next());
            fixture.stop().await;
            Ok(())
        })?;
    }

    // A control committed behind a start's read is revalidated after OCC, never overwritten by the retained hint.
    #[test]
    fn hinted_activity_control_races(race in 0u8..4, data in prop::collection::vec(any::<u8>(), 0..64)) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let fixture = Fixture::new(data, false, false, false).await;
            let before = fixture.state().await;
            let probe = ReadProbe::start(before.run_key, "control-race");
            let (entered, resume) = fixture.repo.pause(Point::Commit);
            let pending = tokio::spawn({ let runtime = fixture.runtime.clone(); let request = fixture.request.clone(); async move {
                runtime.poll_activity_task(request.offered.queue, request.worker, std::time::Duration::from_secs(1)).await
            }});
            entered.await.unwrap();
            let now = fixture.runtime.clock.now();
            match race {
                0 => { assert!(matches!(fixture.runtime.start_forwarded_activity_task(fixture.request.clone()).await.unwrap(), ActivityStartOutcome::Started(_))); },
                1 => { fixture.runtime.pause_activities(before.run_key, PauseActivityRequest { target: ActivityControlTarget::All, identity: "control".into(), reason: "pause".into(), rule_id: None, request: request_context(now), now }).await.unwrap(); },
                2 => { fixture.runtime.pause_workflow(fixture.execution(), PauseWorkflowRequest { identity: "control".into(), reason: "pause".into(), request: request_context(now), now }).await.unwrap(); },
                _ => { fixture.runtime.terminate_workflow(fixture.execution(), TerminateRequest { identity: "control".into(), reason: "close".into(), details: None, links: Vec::new(), request: request_context(now), now }).await.unwrap(); },
            }
            resume.send(()).unwrap();
            prop_assert!(pending.await.unwrap().unwrap().is_none());
            let report = probe.report(); drop(probe);
            prop_assert_eq!(reads(&report, "queue-home/activity-owner"), 0);
            prop_assert_eq!(reads(&report, "execution-owner/activity-start"), 2 + usize::from(race == 0));
            let after = fixture.state().await;
            prop_assert_eq!(after.activity_start_receipts.len(), usize::from(race == 0));
            prop_assert_eq!(after.activities["activity"].started_at.is_some(), race == 0);
            prop_assert_eq!(fixture.repo.commit_conflicts.load(std::sync::atomic::Ordering::SeqCst), 1);
            fixture.stop().await;
            Ok(())
        })?;
    }

    // Cancellation after the commit leaves one start and a separately fenced, read-only receipt observation.
    #[test]
    fn hinted_activity_cancelled_committed_reply(data in prop::collection::vec(any::<u8>(), 0..64), conflict in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let fixture = Fixture::new(data, true, false, false).await;
            let before = fixture.state().await;
            let probe = ReadProbe::start(before.run_key, "lost-reply");
            if conflict { probe.inject_conflicts(1); }
            let (entered, resume) = fixture.repo.pause(Point::Committed);
            let pending = tokio::spawn({ let runtime = fixture.runtime.clone(); let request = fixture.request.clone(); async move { runtime.start_forwarded_activity_task(request).await }});
            entered.await.unwrap();
            // The first boundary can be the injected conflict; release it before
            // parking the applied transition whose reply is deliberately lost.
            if conflict {
                let (committed, release) = fixture.repo.pause(Point::Committed);
                resume.send(()).unwrap(); committed.await.unwrap();
                pending.abort(); prop_assert!(pending.await.unwrap_err().is_cancelled()); drop(release);
            } else { pending.abort(); prop_assert!(pending.await.unwrap_err().is_cancelled()); drop(resume); }
            let report = probe.report(); drop(probe);
            prop_assert_eq!(reads(&report, "execution-owner/activity-start"), 1 + usize::from(conflict));
            let committed = fixture.state().await;
            prop_assert_eq!(committed.transition_seq, before.transition_seq.next());
            let probe = ReadProbe::start(before.run_key, "receipt-observation");
            let ActivityStartOutcome::Replayed(replay) = fixture.runtime.start_forwarded_activity_task(fixture.request.clone()).await.unwrap() else { panic!("replay") };
            let report = probe.report(); drop(probe);
            prop_assert_eq!(reads(&report, "execution-owner/activity-start"), 1);
            prop_assert_eq!(reads(&report, "execution-owner/activity-replay"), 1);
            prop_assert_eq!(replay.input, fixture.input.clone()); prop_assert_eq!(replay.header, fixture.header.clone());
            prop_assert_eq!(fixture.state().await, committed);
            fixture.stop().await;
            Ok(())
        })?;
    }

    // Workflow controls and lineage boundaries retain their authoritative activity behavior and exact scheduled payloads.
    #[test]
    fn activity_delivery_feature_interactions(row in 0u8..8, data in prop::collection::vec(any::<u8>(), 0..64), retry in any::<bool>(), transient in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let fixture = Fixture::new(data, retry, false, row == 2).await;
            let before = fixture.state().await;
            let now = fixture.runtime.clock.now();
            match row {
                0 => {
                    fixture.runtime.pause_workflow(fixture.execution(), PauseWorkflowRequest { identity: "control".into(), reason: "pause".into(), request: request_context(now), now }).await.unwrap();
                    prop_assert!(matches!(fixture.runtime.start_forwarded_activity_task(fixture.request.clone()).await.unwrap(), ActivityStartOutcome::Suppressed));
                    prop_assert!(tokeira_storage::derive_activity_dispatch(&fixture.state().await, ShardId(0)).unwrap().is_empty());
                    fixture.runtime.unpause_workflow(fixture.execution(), UnpauseWorkflowRequest { identity: "control".into(), reason: "resume".into(), request: request_context(now), now }).await.unwrap();
                },
                1 => {
                    fixture.runtime.pause_activities(before.run_key, PauseActivityRequest { target: ActivityControlTarget::All, identity: "control".into(), reason: "pause".into(), rule_id: None, request: request_context(now), now }).await.unwrap();
                    prop_assert!(matches!(fixture.runtime.start_forwarded_activity_task(fixture.request.clone()).await.unwrap(), ActivityStartOutcome::Suppressed));
                    fixture.runtime.unpause_activities(before.run_key, UnpauseActivitiesRequest { target: ActivityControlTarget::All, reset_attempts: false, reset_heartbeat: false, jitter: None, request: request_context(now), now }).await.unwrap();
                    prop_assert!(matches!(fixture.runtime.start_forwarded_activity_task(fixture.request.clone()).await.unwrap(), ActivityStartOutcome::Stale));
                    prop_assert!(fixture.state().await.activities["activity"].stamp > before.activities["activity"].stamp);
                },
                2 => { prop_assert!(before.sticky.is_some()); fixture.runtime.reset_sticky_task_queue(fixture.execution()).await.unwrap(); },
                3 => {
                    fixture.runtime.update_workflow(fixture.execution(), "update".into(), "handler".into(), Payloads::default(), request_context(now), Duration::seconds(1), UpdateWaitPolicy::Admitted, 0).await.unwrap();
                    let speculative = fixture.state().await;
                    prop_assert_eq!(speculative.pending_workflow_task.as_ref().unwrap().task_type, tokeira_kernel::WorkflowTaskType::Speculative);
                    let started = fixture.poll().await;
                    prop_assert_eq!(started.input, fixture.input.clone()); prop_assert_eq!(started.header, fixture.header.clone());
                    let after = fixture.state().await;
                    // Retry-policy starts stay transient; appending a real start converts a scheduled speculative task.
                    // service/history/workflow/mutable_state_impl.go @ v1.31.0.
                    prop_assert_eq!(after.pending_workflow_task.as_ref().unwrap().task_type, if retry { tokeira_kernel::WorkflowTaskType::Speculative } else { tokeira_kernel::WorkflowTaskType::Normal });
                    if retry { prop_assert_eq!(after.last_event_id, speculative.last_event_id); }
                    prop_assert!(after.buffered_events.is_empty());
                },
                4 => {
                    let mut second = fixture.second_workflow_task().await;
                    if transient {
                        fixture.runtime.fail_workflow_task(second.token, tokeira_kernel::WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure, None, WorkerIdentity("workflow".into()), request_context(now), now).await.unwrap();
                        second = fixture.runtime.poll_workflow_task(fixture.workflow_queue.clone(), WorkerIdentity("workflow-retry".into()), std::time::Duration::from_secs(1)).await.unwrap().unwrap();
                        prop_assert!(fixture.state().await.pending_workflow_task.as_ref().unwrap().attempt > 1);
                    }
                    let before_start = fixture.state().await;
                    let started = fixture.poll().await;
                    prop_assert_eq!(started.input, fixture.input.clone()); prop_assert_eq!(started.header, fixture.header.clone());
                    let after = fixture.state().await;
                    prop_assert_eq!(after.last_event_id, before_start.last_event_id);
                    if !retry { prop_assert!(after.buffered_events.iter().any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskStarted { .. })), "start event must be retained"); }
                    let mut complete = completion_inline_tests::completion(&second, false);
                    complete.now = now; complete.request.received_at = now;
                    fixture.runtime.complete_workflow_task(complete).await.unwrap();
                    if !retry { prop_assert!(fixture.repo.read_history_to_end(before.run_key, 0).await.unwrap().iter().any(|event| matches!(event.kind, HistoryEventKind::ActivityTaskStarted { .. })), "start event must be retained"); }
                },
                5 => {
                    let second = fixture.second_workflow_task().await;
                    let successor = fixture.runtime.reset_workflow(fixture.execution(), tokeira_kernel::ResetRequest {
                        fork_event_id: second.token.started_event_id, new_run_id: RunId::new(), reapply_exclude_signal: false, reapply_exclude_update: false,
                        post_reset_versioning_overrides: Vec::new(), expected_current_run_key: None, reason: "reset".into(), request: request_context(now), now,
                    }).await.unwrap().successor_run_key;
                    prop_assert_ne!(successor, before.run_key);
                    prop_assert!(matches!(fixture.runtime.start_forwarded_activity_task(fixture.request.clone()).await.unwrap(), ActivityStartOutcome::Stale));
                    let started = fixture.poll().await;
                    prop_assert_eq!(started.run_key, successor);
                    prop_assert_eq!(started.input, fixture.input.clone()); prop_assert_eq!(started.header, fixture.header.clone());
                },
                6 => {
                    let second = fixture.second_workflow_task().await;
                    let mut complete = completion_inline_tests::completion(&second, false);
                    complete.now = now; complete.request.received_at = now;
                    let new_run_id = RunId::new();
                    complete.commands = vec![WorkflowCommand::ContinueAsNew {
                        new_run_id, workflow_type: fixture.start.workflow_type.clone(), task_queue: fixture.start.task_queue.clone(), input: Payloads::default(),
                        memo: Default::default(), search_attributes: Default::default(), workflow_execution_timeout: None, workflow_run_timeout: None, workflow_task_timeout: fixture.start.workflow_task_timeout,
                        retry_policy: None, header: None, initial_versioning_behavior: Default::default(), successor_versioning_info: None,
                    }];
                    fixture.runtime.complete_workflow_task(complete).await.unwrap();
                    prop_assert!(matches!(fixture.runtime.start_forwarded_activity_task(fixture.request.clone()).await.unwrap(), ActivityStartOutcome::Stale));
                    let successor_key = RunKey::derive(fixture.start.namespace_id, &fixture.start.workflow_id, new_run_id);
                    let LoadedRun::Existing(successor_state) = fixture.repo.load_run(successor_key).await.unwrap() else { panic!("successor") };
                    let due = successor_state.started_at + successor_state.workflow_start_delay.unwrap_or(Duration::ZERO);
                    if due > fixture.runtime.clock.now() { tokio::time::advance((due - fixture.runtime.clock.now()).try_into().unwrap()).await; }
                    crate::scanner::scan_due_timers_once(fixture.repo.as_ref(), &TimerScannerConfig::default(), fixture.runtime.clock.now(), |timer, fired_at| {
                        let runtime = fixture.runtime.clone();
                        async move { runtime.submit(timer.run_key, crate::scanner::command_for_due_timer(timer, fired_at)).await.map(|_| ()) }
                    }).await;
                    let successor = fixture.runtime.poll_workflow_task(fixture.workflow_queue.clone(), WorkerIdentity("successor".into()), std::time::Duration::from_secs(1)).await.unwrap().unwrap();
                    prop_assert_ne!(successor.run_key, before.run_key);
                    let LoadedRun::Existing(state) = fixture.repo.load_run(successor.run_key).await.unwrap() else { panic!("successor") };
                    prop_assert!(state.activities.is_empty());
                    let mut complete = completion_inline_tests::completion(&successor, false);
                    complete.now = fixture.runtime.clock.now(); complete.request.received_at = complete.now; complete.commands = vec![completion_eager_tests::schedule("successor-activity", retry)];
                    fixture.runtime.complete_workflow_task(complete).await.unwrap();
                    let started = fixture.poll().await;
                    prop_assert_eq!(started.run_key, successor.run_key); prop_assert_eq!(started.activity_id, "successor-activity");
                },
                _ => {
                    fixture.runtime.terminate_workflow(fixture.execution(), TerminateRequest { identity: "control".into(), reason: "close".into(), details: None, links: Vec::new(), request: request_context(now), now }).await.unwrap();
                    prop_assert!(matches!(fixture.runtime.start_forwarded_activity_task(fixture.request.clone()).await.unwrap(), ActivityStartOutcome::Stale));
                    prop_assert!(fixture.state().await.activity_start_receipts.is_empty());
                },
            }
            if row < 3 {
                let started = fixture.poll().await;
                prop_assert_eq!(started.input, fixture.input.clone()); prop_assert_eq!(started.header, fixture.header.clone());
                prop_assert_eq!(started.task_queue, TaskQueueName("activities".into()));
            }
            fixture.stop().await;
            Ok(())
        })?;
    }

    // Losing an eager completion reply cannot republish its consumed incarnation, including after recovery.
    #[test]
    fn activity_eager_lost_reply_and_heartbeat_payloads(data in prop::collection::vec(any::<u8>(), 0..128), reset_heartbeat in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let fixture = Fixture::new(data.clone(), true, true, false).await;
            let before = fixture.state().await;
            prop_assert_eq!(before.activity_start_receipts.len(), 1);
            prop_assert!(!fixture.runtime.activity_broker.has_runnable_backlog(&fixture.request.offered.queue).await);
            prop_assert!(tokeira_storage::derive_activity_dispatch(&before, ShardId(0)).unwrap().is_empty());
            let epoch = fixture.runtime.shard_owner.read().unwrap().epoch_of(ShardId(0)).unwrap();
            fixture.runtime.recover_self_assigned_shard(ShardId(0), epoch).await.unwrap();
            prop_assert!(!fixture.runtime.activity_broker.has_runnable_backlog(&fixture.request.offered.queue).await);
            let token = ActivityTaskToken { run_key: before.run_key, activity_id: "activity".into(), schedule_event_id: fixture.request.offered.schedule_event_id, attempt: 1, shard_epoch: epoch };
            let heartbeat = Payloads(vec![Payload::new(data)]);
            fixture.runtime.record_activity_heartbeat(token.clone(), Some(heartbeat.clone()), None).await.unwrap();
            fixture.runtime.fail_activity_task(token, Payload::new(Vec::new()), crate::retry::ActivityFailure::Other, None, None, request_context(fixture.runtime.clock.now())).await.unwrap();
            // service/history/workflow/retry.go:100-107 @ v1.31.0 requires a positive retry interval.
            tokio::time::advance(std::time::Duration::from_nanos(1)).await;
            if reset_heartbeat {
                let now = fixture.runtime.clock.now();
                fixture.runtime.reset_activities(before.run_key, ResetActivitiesRequest { target: ActivityControlTarget::All, reset_heartbeat: true, keep_paused: false, jitter: None, restore_original_options: false, request: request_context(now), now }).await.unwrap();
            }
            let started = fixture.poll().await;
            prop_assert_eq!(started.heartbeat_details, (!reset_heartbeat).then_some(heartbeat));
            prop_assert_eq!(started.input, fixture.input.clone()); prop_assert_eq!(started.header, fixture.header.clone());
            fixture.stop().await;
            Ok(())
        })?;
    }

    // Resetting a running attempt or retaining pause cannot publish a new start obligation.
    #[test]
    fn activity_reset_running_or_kept_paused(data in prop::collection::vec(any::<u8>(), 0..64), running in any::<bool>(), reset_heartbeat in any::<bool>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let fixture = Fixture::new(data, true, running, false).await;
            let now = fixture.runtime.clock.now();
            if !running {
                fixture.runtime.pause_activities(fixture.start.run_key, PauseActivityRequest { target: ActivityControlTarget::All, identity: "control".into(), reason: "pause".into(), rule_id: None, request: request_context(now), now }).await.unwrap();
            }
            fixture.runtime.activity_broker.remove_run(fixture.start.run_key).await;
            fixture.runtime.reset_activities(fixture.start.run_key, ResetActivitiesRequest { target: ActivityControlTarget::All, reset_heartbeat, keep_paused: !running, jitter: None, restore_original_options: false, request: request_context(now), now }).await.unwrap();
            let state = fixture.state().await;
            prop_assert_eq!(state.activities["activity"].started_at.is_some(), running);
            prop_assert_eq!(state.activities["activity"].pause_info.is_some(), !running);
            prop_assert!(tokeira_storage::derive_activity_dispatch(&state, ShardId(0)).unwrap().is_empty());
            prop_assert!(!fixture.runtime.activity_broker.has_runnable_backlog(&fixture.request.offered.queue).await);
            fixture.stop().await;
            Ok(())
        })?;
    }
}
