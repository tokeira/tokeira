//! Local/peer destination parity through persisted recipes and both lease fences.

use super::*;
use crate::runtime::*;
use proptest::prelude::*;
use tokeira_kernel::{
    BasicKernel, Command, HistoryEvent, HistoryEventKind, Kernel, LoadedRun, OperationKind,
    PendingExternalSignal,
};
use tokeira_storage::{DeliveryGuard, ExecutionHomeFence, InMemoryStore, LeaseRepository};
use tokeira_types::{Payload, Payloads, RunKey, ShardEpoch, ShardId, WorkflowId};

fn home(start: &tokeira_kernel::StartRequest) -> ShardId {
    tokeira_types::execution_home_bundle(
        start.namespace_id.0.as_bytes(),
        start.workflow_id.0.as_bytes(),
        8,
    )
}

fn receiver(repo: Arc<InMemoryStore>, home: ShardId) -> TokeiraRuntime<InMemoryStore> {
    let runtime = TokeiraRuntime::new_with_nexus_and_shards(
        repo,
        2,
        LaneConfig {
            controller_managed_placement: true,
            ..Default::default()
        },
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        BacklogConfig::default(),
        ActivityTimeoutScannerConfig::default(),
        NexusTimeoutScannerConfig::default(),
        NexusEndpointRegistry::default(),
        Arc::new(NoopNexusHttpClient),
        NexusCompletionDeps::default(),
        8,
        "destination".into(),
        false,
    );
    {
        let mut owner = runtime.shard_owner.write().unwrap();
        owner.record_acquired(home, ShardEpoch(1));
        owner.mark_active(home);
    }
    runtime
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: outbound-dispatch-recovery, Property 18: Remote delivery grants no extra authority.
    #[test]
    fn peer_and_local_hydration_agree_under_owner_and_reference_changes(bytes in proptest::collection::vec(any::<u8>(), 0..128), mutation in 0u8..5) {
        tokio::runtime::Builder::new_current_thread().enable_all().start_paused(true).build().unwrap().block_on(async {
            let initial = InMemoryStore::with_shard_count(8);
            let repo = InMemoryStore::from_snapshot(&initial.snapshot().await.unwrap()).unwrap();
            let mut source = super::super::tests::sample_start_request(None, None);
            source.workflow_id = WorkflowId("source".into());
            source.run_key = RunKey::derive(source.namespace_id, &source.workflow_id, source.run_id);
            let source_home = home(&source);
            let mut target = source.clone(); target.run_id = tokeira_types::RunId::new();
            for i in 0..1000 {
                target.workflow_id = WorkflowId(format!("destination-{i}"));
                if home(&target) != source_home { break; }
            }
            let destination_home = home(&target); assert_ne!(source_home, destination_home);
            target.run_key = RunKey::derive(target.namespace_id, &target.workflow_id, target.run_id);
            target.request.request_id = tokeira_types::RequestId("target-start".into());
            let payload = Payloads(vec![Payload::new(bytes)]);
            let mut transition = BasicKernel.apply(LoadedRun::Absent, Command::Start(source.clone())).unwrap();
            let initiation = transition.next_state.last_event_id + 1;
            transition.history_events.push(HistoryEvent { event_id: initiation, happened_at: source.now,
                kind: HistoryEventKind::SignalExternalWorkflowExecutionInitiated { workflow_task_completed_event_id: 0,
                    namespace_id: target.namespace_id, namespace: None, target_workflow_id: target.workflow_id.clone(), target_run_id: Some(target.run_id),
                    signal_name: "signal".into(), input: payload.clone(), header: None, control: "control".into() } });
            transition.event_principals.push(None); transition.next_state.last_event_id = initiation;
            transition.next_state.pending_external_signals.insert(initiation, PendingExternalSignal {
                initiated_event_id: initiation, target_namespace_id: target.namespace_id, target_namespace: None,
                target_workflow_id: target.workflow_id.clone(), target_run_id: Some(target.run_id), signal_name: "signal".into(),
            });
            let state = transition.next_state.clone();
            repo.commit_transition(source.run_key, transition, WriteFence::Unfenced).await.unwrap();
            repo.commit_transition(target.run_key, BasicKernel.apply(LoadedRun::Absent, Command::Start(target.clone())).unwrap(), WriteFence::Unfenced).await.unwrap();
            tokeira_storage::prepare_execution_placement(&repo).await.unwrap();
            repo.try_acquire_bundle(source_home, "source".into(), "source:1".into()).await.unwrap();
            repo.try_acquire_bundle(destination_home, "destination".into(), "destination:1".into()).await.unwrap();
            let row = tokeira_storage::derive_operations(&state, source_home).unwrap().into_iter().find(|row| row.incarnation.key.kind == OperationKind::ExternalSignal).unwrap();
            let mut reference = OutboundDeliveryReference { authority: DeliveryGuard::Operation { incarnation: row.incarnation, source_seq: state.transition_seq },
                source_fence: WriteFence::Lease(ExecutionHomeFence { home: source_home, owner: "source".into(), epoch: ShardEpoch(1) }),
                phase: DeliveryPhase::External, destination: target.run_key, destination_home };
            match mutation {
                1 => { if let DeliveryGuard::Operation { source_seq, .. } = &mut reference.authority { source_seq.0 += 1; } },
                2 => reference.destination_home = source_home,
                3 | 4 => {
                    let (home, owner) = if mutation == 3 { (source_home, "source") } else { (destination_home, "destination") };
                    repo.relinquish_bundle(home, owner.into(), ShardEpoch(1)).await.unwrap();
                    repo.try_acquire_bundle(home, "replacement".into(), "replacement:1".into()).await.unwrap();
                }
                _ => {},
            }
            let snapshot = repo.snapshot().await.unwrap();
            let local_store = Arc::new(InMemoryStore::from_snapshot(&snapshot).unwrap());
            let remote_store = Arc::new(InMemoryStore::from_snapshot(&snapshot).unwrap());
            tokeira_storage::prepare_execution_placement(local_store.as_ref()).await.unwrap();
            tokeira_storage::prepare_execution_placement(remote_store.as_ref()).await.unwrap();
            let local = receiver(local_store.clone(), destination_home);
            let remote = receiver(remote_store.clone(), destination_home);
            let attempt = Attempt::capture(&local.shard_owner, destination_home).unwrap();
            let local_result = local.outbound_publisher.receive_outbound(&reference, attempt, &local.shard_owner).await;
            let remote_result = remote.deliver_outbound_reference(reference.clone()).await;
            prop_assert_eq!(matches!(local_result, Ok(CommitResult::Applied { .. })), mutation == 0);
            prop_assert_eq!(matches!(remote_result, OutboundDeliveryOutcome::Applied { .. }), mutation == 0);
            for store in [&local_store, &remote_store] {
                let LoadedRun::Existing(target_state) = store.load_run(target.run_key).await.unwrap() else { panic!("target missing"); };
                prop_assert_eq!(target_state.signal_count, u64::from(mutation == 0));
                prop_assert_eq!(store.load_run(source.run_key).await.unwrap(), LoadedRun::Existing(state.clone()));
            }
            if mutation == 0 {
                remote.deliver_outbound_reference(reference).await;
                let history = remote_store.read_history(target.run_key, 0, 100).await.unwrap();
                let signals: Vec<_> = history.iter().filter_map(|event| match &event.kind { HistoryEventKind::WorkflowExecutionSignaled { input, .. } => Some(input), _ => None }).collect();
                prop_assert_eq!(signals, vec![&payload]);
            }
            for runtime in [local, remote] {
                let shutdown = runtime.shutdown_handle(); shutdown.begin_shutdown(); drop(runtime);
                shutdown.wait(std::time::Instant::now() + std::time::Duration::from_secs(5)).await.unwrap();
            }
            Ok(())
        })?;
    }
}
