//! Commit-result fencing for heartbeat expiry, including failed and retried writes.

use super::*;
use tokeira_kernel::{
    BasicKernel, CompletionWorkflowTaskStart, WorkflowTaskCompletedRequest, WorkflowTaskHeartbeat,
};
use tokeira_types::WorkflowTaskToken;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Feature: transient-wft, Property C2
    // Only an applied expiry may report the committed timeout or invalidate the old token.
    #[test]
    fn heartbeat_committed_timeout_outcome(outcome in 0u8..5, limit in 1i64..3600, late in 1i64..100) {
        Runtime::new().unwrap().block_on(async {
            let run_key = RunKey::new();
            let mut state = sample_state(run_key);
            let at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
            let pending = state.pending_workflow_task.as_mut().unwrap();
            pending.original_scheduled_at = Some(at);
            pending.scheduled_at = at + Duration::seconds(limit);
            pending.started_at = Some(pending.scheduled_at);
            pending.started_event_id = Some(2);
            state.last_event_id = 2;
            state.next_workflow_task_seq = LogicalTaskSeq(2);
            let request = WorkflowTaskCompletedRequest {
                token: WorkflowTaskToken { run_key, logical_seq: LogicalTaskSeq::ONE, started_event_id: 2, attempt: 1, shard_epoch: ShardEpoch::ZERO },
                client_discards_speculative_with_events: false, identity: WorkerIdentity("worker".into()),
                sdk_metadata: None, metering_metadata: None, worker_version: None,
                versioning_behavior: VersioningBehavior::Unspecified, deployment_version: None,
                worker_deployment_name: None, sticky: None, commands: Vec::new(), command_sizes: Vec::new(),
                heartbeat: WorkflowTaskHeartbeat { wire_empty: true, timeout: Duration::seconds(limit) },
                force_new_workflow_task: true, eager_activities: Default::default(),
                inline_workflow_task: Some(CompletionWorkflowTaskStart { scope: None, history_size_bytes: 0, advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0 }),
                limits: Default::default(), delivered_update_ids: Vec::new(),
                request: RequestContext::unattributed(at), now: at + Duration::seconds(limit + late),
            };
            let kernel = BasicKernel;
            let transition = kernel.apply(LoadedRun::Existing(state.clone()), Command::WorkflowTaskCompleted(request.clone())).unwrap();
            assert!(transition.workflow_task_heartbeat_timed_out);
            assert!(!transition.history_events.iter().any(|event| matches!(event.kind, HistoryEventKind::WorkflowTaskCompleted { .. })));
            let behaviors = match outcome {
                0 => vec![CommitBehavior::Applied],
                1 => vec![CommitBehavior::Conflict, CommitBehavior::Applied],
                2 => vec![CommitBehavior::Conflict, CommitBehavior::Conflict],
                3 => vec![CommitBehavior::Error],
                _ => vec![CommitBehavior::Duplicate],
            };
            let repo = MockRepo::new(LoadedRun::Existing(state.clone()), behaviors);
            let publisher = MockPublisher::new();
            let (reply, response) = oneshot::channel();
            let first = LaneMessage::new(0, run_key, Command::WorkflowTaskCompleted(request.clone()), reply);
            let (_tx, mut rx) = mpsc::channel(8);
            let activity = crate::activity_timeout::ActivityTrackingState::default();
            let workflow = crate::timeout::WorkflowTimeoutTrackingState::default();
            let wft = crate::wft_timeout::WftTimeoutTrackingState::default();
            let nexus = crate::nexus::NexusTimeoutTrackingState::default();
            let updates = crate::UpdateRegistry::new();
            let owner = test_shard_owner();
            run_activation(&kernel, &repo, &publisher, &owner, &activity, &workflow, &wft, &nexus, &updates, &mut rx, first,
                &LaneConfig { max_occ_retries: 1, ..LaneConfig::default() }).await;
            let result = response.await.unwrap();
            let timed_out = result.as_ref().err().is_some_and(|error| error.downcast_ref::<crate::errors::WorkflowTaskHeartbeatTimedOut>().is_some());
            assert_eq!(timed_out, outcome < 2);
            let (_, commits, loaded) = repo.snapshot().await;
            assert_eq!(commits, if outcome == 1 || outcome == 2 { 2 } else { 1 });
            if outcome < 2 {
                let LoadedRun::Existing(actual) = loaded else { panic!("run"); };
                assert_eq!(actual, transition.next_state);
                assert!(kernel.apply(LoadedRun::Existing(actual), Command::WorkflowTaskCompleted(request)).is_err());
                assert_eq!(wft.snapshot().len(), 1);
            } else {
                assert_eq!(loaded, LoadedRun::Existing(state));
                assert!(wft.snapshot().is_empty());
                assert!(publisher.snapshot().await.publishes.is_empty());
            }
        });
    }
}
