//! Request identity follows a started incarnation through buffering and controls.

use tokeira_kernel::{ActivityStartMode, ActivityStartReceipt, ActivityStartRequest};
use tokeira_types::{QueueKey, TaskKind, Uuid};

use super::*;

fn attach_receipt(state: &mut WorkflowState, request_id: Uuid, offered_stamp: u64) {
    let activity = &state.activities["activity"];
    state.activity_start_receipts.insert(
        "activity".into(),
        ActivityStartReceipt {
            request_id,
            schedule_event_id: activity.schedule_event_id,
            attempt: activity.attempt,
            offered_stamp,
            worker_identity: activity.started_identity.clone().unwrap(),
            selected_queue: QueueKey {
                namespace_id: state.namespace_id,
                task_queue: activity.task_queue.clone(),
                task_kind: TaskKind::Activity,
                deployment: None,
                build_id: None,
            },
            dispatch_revision: 0,
        },
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: activity-dispatch, Property 16
    // Immediate, buffered and transient starts materialize the accepted ID and time once.
    #[test]
    fn accepted_request_survives_start_materialization(
        buffered in any::<bool>(), transient in any::<bool>(), by_id in any::<bool>(),
        attempt in 1u32..20, stamp in 0u64..1000, seed in any::<u64>(), identity in ".{0,30}",
    ) {
        let now = fixed_now();
        let completed_at = now + Duration::seconds(3);
        let mut state = with_activity(make_open_state(now), "activity");
        if buffered { state = with_pending_wft(state, 5, Some(19), 1); }
        let activity = state.activities.get_mut("activity").unwrap();
        activity.attempt = attempt;
        activity.stamp = stamp;
        if transient { activity.retry_policy = Some(sample_retry_policy()); }
        let request_id = Uuid::from_u128(u128::from(seed) + 1);
        let mut started = BasicKernel.apply_activity_started(LoadedRun::Existing(state), ActivityStartRequest {
            request_id: (!by_id).then_some(request_id), activity_id: "activity".into(),
            identity: WorkerIdentity(identity.clone()), principal: None,
            mode: if by_id { ActivityStartMode::CompletedById } else { ActivityStartMode::Poll }, now,
        }).unwrap();
        if !by_id { attach_receipt(&mut started.next_state, request_id, stamp); }
        let mut events = started.history_events;
        let mut resolved = BasicKernel.apply(LoadedRun::Existing(started.next_state), Command::ActivityResolved(ActivityResolvedRequest {
            activity_id: "activity".into(), resolution: ActivityResolution::Completed { result: Payloads::default() },
            worker_identity: Some(WorkerIdentity(identity.clone())), request: request_context("complete", completed_at), now: completed_at,
        })).unwrap();
        prop_assert!(resolved.next_state.activity_start_receipts.is_empty());
        events.extend(resolved.history_events);
        if buffered {
            let completion = completion_request(&resolved.next_state, Vec::new(), Default::default(), None, None, false, completed_at);
            resolved = BasicKernel.apply(LoadedRun::Existing(resolved.next_state), Command::WorkflowTaskCompleted(completion)).unwrap();
            events.extend(resolved.history_events);
        }
        let started_events = events.iter().filter(|event| matches!(event.kind, HistoryEventKind::ActivityTaskStarted { .. })).collect::<Vec<_>>();
        prop_assert_eq!(started_events.len(), 1);
        let event = started_events[0];
        prop_assert_eq!(event.happened_at, now);
        let HistoryEventKind::ActivityTaskStarted { request_id: recorded, identity: worker, attempt: recorded_attempt, .. } = &event.kind else { unreachable!() };
        let expected = if by_id { format!("activity-start-activity-{attempt}") } else { request_id.to_string() };
        prop_assert_eq!(recorded, &expected);
        prop_assert_eq!(&worker.0, &identity);
        prop_assert_eq!(*recorded_attempt, attempt);
        let completed = events.iter().find(|event| matches!(event.kind, HistoryEventKind::ActivityTaskCompleted { .. })).unwrap();
        prop_assert_eq!(completed.happened_at, completed_at);
    }

    // Feature: activity-dispatch, Property 16
    // Controls preserve the receipt until its schedule, attempt or started state changes.
    #[test]
    fn receipt_lifecycle_tracks_the_incarnation(actions in prop::collection::vec(0u8..6, 1..40), seed in any::<u64>()) {
        let now = fixed_now();
        let state = with_activity(make_open_state(now), "activity");
        let request_id = Uuid::from_u128(u128::from(seed) + 1);
        let mut state = BasicKernel.apply_activity_started(LoadedRun::Existing(state), ActivityStartRequest {
            request_id: Some(request_id), activity_id: "activity".into(), identity: WorkerIdentity("worker".into()),
            principal: None, mode: ActivityStartMode::Poll, now,
        }).unwrap().next_state;
        attach_receipt(&mut state, request_id, 0);
        let mut expected_live = true;
        for action in actions {
            match action {
                0 => state.activities.get_mut("activity").unwrap().stamp += 1,
                1 => state.activities.get_mut("activity").unwrap().task_queue = TaskQueueName("next-queue".into()),
                2 => {
                    state = BasicKernel.apply(LoadedRun::Existing(state), Command::PauseActivity(PauseActivityRequest {
                        target: ActivityControlTarget::Id("activity".into()), identity: "operator".into(), reason: "pause".into(),
                        rule_id: None, request: request_context("pause", now), now,
                    })).unwrap().next_state;
                },
                3 => { state.activities.get_mut("activity").unwrap().attempt += 1; expected_live = false; },
                4 => { state.activities.get_mut("activity").unwrap().schedule_event_id += 1; expected_live = false; },
                _ => { state.activities.get_mut("activity").unwrap().started_at = None; expected_live = false; },
            }
            state.retain_activity_start_receipts();
            prop_assert_eq!(state.activity_start_receipts.contains_key("activity"), expected_live);
        }
        state.activities.clear();
        state.retain_activity_start_receipts();
        prop_assert!(state.activity_start_receipts.is_empty());
    }

    // Feature: activity-dispatch, Property 16
    // Reset forks never retain a request accepted by their source lineage.
    #[test]
    fn reset_fork_clears_started_receipts(seed in any::<u64>(), wft_started in any::<bool>()) {
        let now = fixed_now();
        let mut state = with_pending_wft(with_activity(make_open_state(now), "activity"), 5, wft_started.then_some(19), 1);
        let activity = state.activities.get_mut("activity").unwrap();
        activity.started_event_id = Some(17); activity.started_at = Some(now);
        activity.started_identity = Some(WorkerIdentity("worker".into()));
        attach_receipt(&mut state, Uuid::from_u128(u128::from(seed) + 1), 0);
        let fork = tokeira_kernel::ResetFork {
            base_run_id: state.run_id,
            request: ResetRequest { fork_event_id: state.last_event_id + 1, new_run_id: state.run_id,
                reapply_exclude_signal: false, reapply_exclude_update: false, post_reset_versioning_overrides: Vec::new(),
                expected_current_run_key: None, reason: "fork".into(), request: request_context("reset", now), now },
            reapply: Vec::new(), workflow_task_failure: Payload::new("fork"),
            activity_failures: [("activity".into(), Payload::new("activity"))].into(),
            history_size_bytes: 2048, advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        };
        let result = BasicKernel.prepare_reset_fork(state, fork).unwrap();
        prop_assert!(result.next_state.activity_start_receipts.is_empty());
    }
}
