//! Worker replies must retain dispatch ownership even across same-epoch takeover.

use super::*;
use proptest::prelude::*;
use tokeira_kernel::{BasicKernel, Kernel, PendingNexusOperation};
use tokeira_storage::{InMemoryStore, WriteFence};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: outbound-dispatch-recovery, Property 8: Nexus phase and attempt fencing
    #[test]
    fn nexus_worker_reply_cannot_borrow_replacement_ownership(cancel in any::<bool>(), same_epoch in any::<bool>(), generation in 0u32..40) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let repo = Arc::new(InMemoryStore::default());
            let runtime = TokeiraRuntime::new(repo.clone(), 1, LaneConfig::default(),
                TimerScannerConfig::default(), WorkflowTimeoutScannerConfig::default(), BacklogConfig::default());
            runtime.ready().await.unwrap();
            let mut start = tests::sample_start_request(None, None);
            start.run_key = RunKey::derive(start.namespace_id, &start.workflow_id, start.run_id);
            let mut transition = BasicKernel.apply(LoadedRun::Absent, Command::Start(start)).unwrap();
            let scheduled_at = OffsetDateTime::now_utc() + Duration::days(1);
            transition.next_state.pending_nexus_operations.insert("operation".into(), PendingNexusOperation {
                operation_id: "operation".into(), scheduled_event_id: 12, endpoint: "worker".into(),
                service: "service".into(), operation: "operation".into(), input: Payloads::default(),
                schedule_to_close_timeout: None, schedule_to_start_timeout: None, start_to_close_timeout: None,
                scheduled_at, started: cancel, started_at: cancel.then_some(scheduled_at),
                attempt: generation, last_attempt_failure: None, next_attempt_at: None,
                operation_token: if cancel { "handler-token".into() } else { String::new() },
                cancellation: cancel.then_some(tokeira_kernel::NexusOperationCancellation {
                    requested_event_id: 14, requested_time: Some(scheduled_at),
                    state: tokeira_kernel::NexusOperationCancellationState::Scheduled,
                    attempt: generation, last_attempt_complete_time: None,
                    last_attempt_failure: None, next_attempt_at: None,
                }),
            });
            let source = transition.next_state.clone();
            repo.commit_transition(source.run_key, transition, WriteFence::Unfenced).await.unwrap();
            let kind = if cancel { tokeira_kernel::OperationKind::NexusCancel } else { tokeira_kernel::OperationKind::NexusStart };
            let row = tokeira_storage::derive_operations(&source, ShardId(0)).unwrap().into_iter()
                .find(|row| row.incarnation.key.kind == kind).unwrap();
            let attempt = crate::outbound::driver::Attempt::capture(&runtime.shard_owner, ShardId(0)).unwrap();
            let old = crate::NexusDeliveryAuthority::new(row.clone(), attempt.clone());
            {
                let mut owner = runtime.shard_owner.write().unwrap();
                let epoch = if same_epoch { attempt.acquisition.epoch } else { ShardEpoch(attempt.acquisition.epoch.0 + 1) };
                owner.record_acquired(ShardId(0), epoch);
                owner.mark_active(ShardId(0));
            }
            let outcome = if cancel { crate::NexusWorkerResult::Cancel(tokeira_kernel::NexusCancellationAttemptOutcome::Succeeded) }
                else { crate::NexusWorkerResult::Start(tokeira_kernel::NexusResolution::Completed { result: Payloads::default(), links: Vec::new() }) };
            prop_assert!(!runtime.resolve_nexus_worker_result(old, outcome.clone()).await.unwrap());
            prop_assert_eq!(repo.load_run(source.run_key).await.unwrap(), LoadedRun::Existing(source.clone()));
            let replacement = crate::outbound::driver::Attempt::capture(&runtime.shard_owner, ShardId(0)).unwrap();
            prop_assert!(runtime.resolve_nexus_worker_result(crate::NexusDeliveryAuthority::new(row, replacement), outcome).await.unwrap());
            let LoadedRun::Existing(result) = repo.load_run(source.run_key).await.unwrap() else { panic!("run remains"); };
            if cancel {
                prop_assert!(result.pending_nexus_operations.contains_key("operation"));
                prop_assert_eq!(result.pending_nexus_operations["operation"].cancellation.as_ref().unwrap().state,
                    tokeira_kernel::NexusOperationCancellationState::Succeeded);
            } else { prop_assert!(result.pending_nexus_operations.is_empty()); }
            let shutdown = runtime.shutdown_handle(); shutdown.begin_shutdown(); drop(runtime);
            shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(5)).await.unwrap();
            Ok(())
        })?;
    }
}
