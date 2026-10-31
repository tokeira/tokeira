//! Publication, retirement and source/destination serialization contracts.

use proptest::prelude::*;
use tokeira_kernel::{
    ChildStartGate, ChildWorkflowState, Command, Kernel, OperationKind, ParentClosePolicy,
    PendingExternalCancel, PendingExternalSignal, TerminateRequest,
};
use tokeira_types::{RequestContext, WorkflowType};
use uuid::Uuid;

use super::*;
use crate::{
    DefinitiveOutcome, DeliveryGuard, IntentKind, IntentPayload, IntentRetirement, ScanBound,
    memory::projection_accumulator_tests::{applied, following, fresh_transition},
};

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

fn source(seed: u32, count: u8, kind: u8) -> Transition {
    let mut start = fresh_transition(RunKey(Uuid::from_u128(u128::from(seed) + 1)));
    start.next_state.workflow_id = WorkflowId(format!("source-{seed}"));
    let state = &mut start.next_state;
    for i in 1..=count {
        let event = i64::from(i);
        let target = WorkflowId(format!("target-{i}"));
        match kind {
            0 => {
                state.pending_external_signals.insert(
                    event,
                    PendingExternalSignal {
                        initiated_event_id: event,
                        target_namespace_id: state.namespace_id,
                        target_namespace: None,
                        target_workflow_id: target,
                        target_run_id: None,
                        signal_name: "signal".into(),
                    },
                );
            }
            1 => {
                state.pending_external_cancels.insert(
                    event,
                    PendingExternalCancel {
                        initiated_event_id: event,
                        target_namespace_id: state.namespace_id,
                        target_namespace: None,
                        target_workflow_id: target,
                        target_run_id: None,
                    },
                );
            }
            _ => {
                state.children.insert(
                    target.clone(),
                    ChildWorkflowState {
                        child_workflow_id: target,
                        namespace_id: state.namespace_id,
                        namespace: None,
                        workflow_type: WorkflowType("child".into()),
                        header: None,
                        child_run_id: None,
                        initiated_event_id: event,
                        started_event_id: None,
                        parent_close_policy: ParentClosePolicy::Terminate,
                    },
                );
            }
        }
    }
    start
}

fn kind(value: u8) -> OperationKind {
    match value {
        0 => OperationKind::ExternalSignal,
        1 => OperationKind::ExternalCancel,
        _ => OperationKind::ChildStart,
    }
}

fn home(state: &WorkflowState) -> ShardId {
    tokeira_types::execution_home_bundle(
        state.namespace_id.0.as_bytes(),
        state.workflow_id.0.as_bytes(),
        1,
    )
}

fn close(state: &WorkflowState) -> Transition {
    BasicKernel
        .apply(
            LoadedRun::Existing(state.clone()),
            Command::Terminate(TerminateRequest {
                reason: "test close".into(),
                details: None,
                identity: "test".into(),
                links: Vec::new(),
                request: RequestContext::unattributed(state.started_at),
                now: state.started_at,
            }),
        )
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: outbound-dispatch-recovery, Property 19: Retention cannot erase outstanding proof.
    #[test]
    fn future_child_result_pins_deleted_parent_reset_lineage(seed in any::<u32>(), redirect in any::<bool>()) {
        block_on(async {
            let store = InMemoryStore::default();
            let start = source(seed, 0, 0);
            let parent = applied(store.commit_transition(start.next_state.run_key, start, WriteFence::Unfenced).await.unwrap());
            let mut child = fresh_transition(RunKey::new());
            child.next_state.namespace_id = parent.namespace_id;
            child.next_state.parent_run_key = Some(parent.run_key);
            child.next_state.parent_run_id = Some(parent.run_id);
            child.next_state.parent_workflow_id = Some(parent.workflow_id.clone());
            child.next_state.parent_namespace_id = Some(parent.namespace_id);
            child.next_state.parent_initiated_event_id = 4;
            let child = applied(store.commit_transition(child.next_state.run_key, child, WriteFence::Unfenced).await.unwrap());
            let mut ending = close(&parent);
            let reset = RunId::new();
            if redirect { ending.next_state.reset_run_id = Some(reset); }
            let parent = applied(store.commit_transition(parent.run_key, ending, WriteFence::Unfenced).await.unwrap());
            let mut parents = vec![parent];
            if redirect {
                let mut fork = fresh_transition(RunKey::new());
                fork.next_state.namespace_id = parents[0].namespace_id;
                fork.next_state.workflow_id = parents[0].workflow_id.clone();
                fork.next_state.run_id = reset;
                fork.next_state.run_key = RunKey::derive(fork.next_state.namespace_id, &fork.next_state.workflow_id, reset);
                fork.request_dedupe_ops.clear();
                let fork = applied(store.commit_transition(fork.next_state.run_key, fork, WriteFence::Unfenced).await.unwrap());
                parents.push(applied(store.commit_transition(fork.run_key, close(&fork), WriteFence::Unfenced).await.unwrap()));
            }
            for parent in &parents {
                store.delete_run_for_bundle(parent.run_key, home(parent), crate::DeleteRunRequest {
                    expected_seq: parent.transition_seq, deleted_at: parent.started_at,
                }, WriteFence::Unfenced).await.unwrap();
                store.purge_run(parent.run_key).await.unwrap();
                prop_assert_eq!(store.load_run(parent.run_key).await.unwrap(), LoadedRun::Absent);
                prop_assert_eq!(store.load_outbound_source(parent.run_key).await.unwrap(), LoadedRun::Existing(parent.clone()));
            }
            let store = InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap();
            applied(store.commit_transition(child.run_key, close(&child), WriteFence::Unfenced).await.unwrap());
            for parent in &parents {
                store.purge_run(parent.run_key).await.unwrap();
                prop_assert_eq!(store.load_outbound_source(parent.run_key).await.unwrap(), LoadedRun::Existing(parent.clone()));
            }
            let intents = store.read_intent_page(ShardId(0), ScanBound::Capture, None, 100).await.unwrap().rows;
            prop_assert_eq!(intents.len(), 1);
            prop_assert_eq!(intents[0].key.kind, IntentKind::ChildResult);
            store.retire_intent(IntentRetirement { key: intents[0].key, outcome: DefinitiveOutcome::Absent }, WriteFence::Unfenced).await.unwrap();
            for parent in &parents {
                store.purge_run(parent.run_key).await.unwrap();
                prop_assert_eq!(store.load_outbound_source(parent.run_key).await.unwrap(), LoadedRun::Absent);
            }
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 19: Retention cannot erase outstanding proof.
    #[test]
    fn deleted_close_source_remains_internal_until_exact_intents_settle(seed in any::<u32>(), count in 1u8..15, split in 0usize..15) {
        block_on(async {
            let store = InMemoryStore::default();
            let start = source(seed, count, 2);
            let state = applied(store.commit_transition(start.next_state.run_key, start, WriteFence::Unfenced).await.unwrap());
            let closed = applied(store.commit_transition(state.run_key, close(&state), WriteFence::Unfenced).await.unwrap());
            let history = store.read_history(state.run_key, 0, 100).await.unwrap();
            store.delete_run_for_bundle(state.run_key, home(&state), crate::DeleteRunRequest {
                expected_seq: closed.transition_seq, deleted_at: closed.started_at,
            }, WriteFence::Unfenced).await.unwrap();
            prop_assert_eq!(store.load_run(state.run_key).await.unwrap(), LoadedRun::Absent);
            store.purge_run(state.run_key).await.unwrap();
            let restored = InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap();
            prop_assert_eq!(restored.load_outbound_source(state.run_key).await.unwrap(), LoadedRun::Existing(closed));
            prop_assert_eq!(restored.read_history(state.run_key, 0, 100).await.unwrap(), history);
            let intents = restored.read_intent_page(home(&state), ScanBound::Capture, None, 100).await.unwrap().rows;
            for (index, intent) in intents.iter().enumerate() {
                restored.retire_intent(IntentRetirement { key: intent.key, outcome: DefinitiveOutcome::Absent }, WriteFence::Unfenced).await.unwrap();
                if index == split {
                    restored.purge_run(state.run_key).await.unwrap();
                    if index + 1 < intents.len() { prop_assert!(matches!(restored.load_outbound_source(state.run_key).await.unwrap(), LoadedRun::Existing(_))); }
                }
            }
            restored.purge_run(state.run_key).await.unwrap();
            prop_assert_eq!(restored.load_outbound_source(state.run_key).await.unwrap(), LoadedRun::Absent);
            prop_assert!(restored.read_history(state.run_key, 0, 100).await.unwrap().is_empty());
            prop_assert!(restored.list_run_bulk_writes(home(&state), None, 100).await.unwrap().is_empty());
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 19: Retention cannot erase outstanding proof.
    #[test]
    fn target_evidence_outlives_logical_deletion_until_source_resolution(seed in any::<u32>(), cancel_source_by_delete in any::<bool>()) {
        block_on(async {
            let store = InMemoryStore::default();
            let start = source(seed, 1, 0);
            let source = applied(store.commit_transition(start.next_state.run_key, start, WriteFence::Unfenced).await.unwrap());
            let row = store.read_operation_page(home(&source), OperationKind::ExternalSignal, ScanBound::Capture, None, 1).await.unwrap().rows.remove(0);
            let mut target = fresh_transition(RunKey::new());
            target.next_state.namespace_id = source.namespace_id;
            target.next_state.workflow_id = WorkflowId("target-1".into());
            let target = applied(store.commit_transition(target.next_state.run_key, target, WriteFence::Unfenced).await.unwrap());
            let request_id = RequestId("target-effect".into());
            let mut request = RequestContext::unattributed(target.started_at); request.request_id = request_id.clone();
            let signal = BasicKernel.apply(LoadedRun::Existing(target.clone()), Command::Signal(tokeira_kernel::SignalRequest {
                signal_name: "signal".into(), input: tokeira_types::Payloads::default(), header: None,
                links: Vec::new(), request, now: target.started_at,
            })).unwrap();
            let target = applied(store.commit_outbound_transition(target.run_key, signal, DeliveryGuard::Operation {
                incarnation: row.incarnation, source_seq: source.transition_seq,
            }, WriteFence::Unfenced, WriteFence::Unfenced).await.unwrap());
            let target = applied(store.commit_transition(target.run_key, close(&target), WriteFence::Unfenced).await.unwrap());
            store.delete_run_for_bundle(target.run_key, home(&target), crate::DeleteRunRequest { expected_seq: target.transition_seq, deleted_at: target.started_at }, WriteFence::Unfenced).await.unwrap();
            store.purge_run(target.run_key).await.unwrap();
            let execution = tokeira_types::ExecutionRef { namespace_id: target.namespace_id, workflow_id: target.workflow_id.clone(), run_id: Some(target.run_id) };
            prop_assert_eq!(store.lookup_request_dedupe(&execution, &request_id).await.unwrap().unwrap().outbound_origin.unwrap().incarnation, row.incarnation);
            prop_assert_eq!(store.load_run(target.run_key).await.unwrap(), LoadedRun::Absent);
            if cancel_source_by_delete {
                let closed = applied(store.commit_transition(source.run_key, close(&source), WriteFence::Unfenced).await.unwrap());
                store.delete_run_for_bundle(source.run_key, home(&source), crate::DeleteRunRequest { expected_seq: closed.transition_seq, deleted_at: closed.started_at }, WriteFence::Unfenced).await.unwrap();
            } else {
                let mut resolution = following(&source); resolution.next_state.pending_external_signals.clear();
                applied(store.commit_transition(source.run_key, resolution, WriteFence::Unfenced).await.unwrap());
            }
            store.purge_run(target.run_key).await.unwrap();
            prop_assert!(store.lookup_request_dedupe(&execution, &request_id).await.unwrap().is_none());
            prop_assert_eq!(store.load_outbound_source(target.run_key).await.unwrap(), LoadedRun::Absent);
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 4: Ownership changes cannot authorize stale commits.
    #[test]
    fn destination_checks_original_source_lease(seed in any::<u32>(), lost_source in any::<bool>(), lost_target in any::<bool>()) {
        block_on(async {
            let store = InMemoryStore::default();
            store.inner.lock().await.shard_count = 8;
            let start = source(seed, 1, 0);
            let source = applied(store.commit_transition(start.next_state.run_key, start, WriteFence::Unfenced).await.unwrap());
            let source_home = tokeira_types::execution_home_bundle(source.namespace_id.0.as_bytes(), source.workflow_id.0.as_bytes(), 8);
            let row = store.read_operation_page(source_home, OperationKind::ExternalSignal, ScanBound::Capture, None, 1).await.unwrap().rows.remove(0);
            let mut target = fresh_transition(RunKey::new());
            let mut index = 0;
            let target_home = loop {
                target.next_state.workflow_id = WorkflowId(format!("other-{index}")); index += 1;
                let home = tokeira_types::execution_home_bundle(target.next_state.namespace_id.0.as_bytes(), target.next_state.workflow_id.0.as_bytes(), 8);
                if home != source_home { break home; }
            };
            let source_fence = WriteFence::Lease(crate::ExecutionHomeFence { home: source_home, owner: "source".into(), epoch: ShardEpoch(1) });
            let target_fence = WriteFence::Lease(crate::ExecutionHomeFence { home: target_home, owner: "target".into(), epoch: ShardEpoch(1) });
            {
                let mut inner = store.inner.lock().await;
                inner.bundle_leases.insert(source_home, (Some("source".into()), ShardEpoch(if lost_source { 2 } else { 1 }), None));
                inner.bundle_leases.insert(target_home, (Some("target".into()), ShardEpoch(if lost_target { 2 } else { 1 }), None));
            }
            let before = store.snapshot().await.unwrap();
            let result = store.commit_outbound_transition(target.next_state.run_key, target,
                DeliveryGuard::Operation { incarnation: row.incarnation, source_seq: source.transition_seq }, source_fence, target_fence).await;
            if lost_source || lost_target {
                prop_assert!(result.unwrap_err().downcast_ref::<crate::LeaseFenceLost>().is_some());
                prop_assert_eq!(store.snapshot().await.unwrap(), before);
            } else { prop_assert!(matches!(result.unwrap(), CommitResult::Applied { .. }), "both original leases admit the effect"); }
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 5: Child preparation survives restart without identity drift.
    #[test]
    fn child_reuse_decision_fences_the_observed_current_pointer(seed in any::<u32>(), had_current in any::<bool>()) {
        block_on(async {
            let store = InMemoryStore::default();
            let source = source(seed, 1, 2);
            let source = applied(store.commit_transition(source.next_state.run_key, source, WriteFence::Unfenced).await.unwrap());
            let operation = store.read_operation_page(home(&source), OperationKind::ChildStart, ScanBound::Capture, None, 1).await.unwrap().rows.remove(0);
            let mut first = fresh_transition(RunKey::new());
            first.next_state.workflow_id = WorkflowId("reused-child".into());
            let mut target = first.clone();
            target.next_state.run_key = RunKey::new(); target.next_state.run_id = RunId::new();
            target.request_dedupe_ops.clear();
            let prior = if had_current {
                let state = applied(store.commit_transition(first.next_state.run_key, first.clone(), WriteFence::Unfenced).await.unwrap());
                applied(store.commit_transition(state.run_key, close(&state), WriteFence::Unfenced).await.unwrap());
                Some(state.run_key)
            } else { None };
            let guard = DeliveryGuard::ChildStart { incarnation: operation.incarnation, source_seq: source.transition_seq, expected_current: prior };
            let mut competing = first;
            competing.next_state.run_key = RunKey::new(); competing.next_state.run_id = RunId::new();
            competing.request_dedupe_ops.clear();
            let current = applied(store.commit_transition(competing.next_state.run_key, competing, WriteFence::Unfenced).await.unwrap());
            applied(store.commit_transition(current.run_key, close(&current), WriteFence::Unfenced).await.unwrap());
            let before = store.snapshot().await.unwrap();
            let error = store.commit_outbound_transition(target.next_state.run_key, target, guard, WriteFence::Unfenced, WriteFence::Unfenced).await.unwrap_err();
            prop_assert!(matches!(error.downcast_ref::<crate::RecoveryError>(), Some(crate::RecoveryError::Obsolete)));
            prop_assert_eq!(store.snapshot().await.unwrap(), before);
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 1: Cause and derived operation atomicity.
    #[test]
    fn operations_change_with_cause_and_resolution(seed in any::<u32>(), count in 1u8..20, category in 0u8..3, keep_even in any::<bool>()) {
        block_on(async {
            let store = InMemoryStore::default();
            let start = source(seed, count, category);
            let key = start.next_state.run_key;
            let mut refused = start.clone(); refused.expected_seq = TransitionSeq(99);
            let before = store.snapshot().await.unwrap();
            prop_assert!(matches!(store.commit_transition(key, refused, WriteFence::Unfenced).await.unwrap(), CommitResult::Conflict { .. }), "conflicting source write is refused");
            prop_assert_eq!(store.snapshot().await.unwrap(), before);
            let state = applied(store.commit_transition(key, start, WriteFence::Unfenced).await.unwrap());
            let first = store.read_operation_page(home(&state), kind(category), ScanBound::Capture, None, 100).await.unwrap();
            prop_assert_eq!(first.rows.iter().map(|row| row.incarnation.key.initiating_event).collect::<Vec<_>>(), (1..=i64::from(count)).collect::<Vec<_>>());
            let mut resolve = following(&state);
            resolve.next_state.pending_external_signals.retain(|event, _| (*event % 2 == 0) == keep_even);
            resolve.next_state.pending_external_cancels.retain(|event, _| (*event % 2 == 0) == keep_even);
            resolve.next_state.children.retain(|_, child| (child.initiated_event_id % 2 == 0) == keep_even);
            applied(store.commit_transition(key, resolve, WriteFence::Unfenced).await.unwrap());
            let restored = InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap();
            let page = restored.read_operation_page(home(&state), kind(category), ScanBound::Capture, None, 100).await.unwrap();
            let expected: Vec<_> = (1..=i64::from(count)).filter(|event| (*event % 2 == 0) == keep_even).collect();
            prop_assert_eq!(page.rows.iter().map(|row| row.incarnation.key.initiating_event).collect::<Vec<_>>(), expected);
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 2: Intent retirement cannot lose or resurrect work.
    #[test]
    fn close_retirement_survives_restore_and_repair(seed in any::<u32>(), count in 1u8..20, retired in 0usize..20, earlier_reset in any::<bool>()) {
        block_on(async {
            let store = InMemoryStore::default();
            let mut start = source(seed, count, 2);
            if earlier_reset { start.next_state.reset_run_id = Some(RunId::new()); }
            let state = applied(store.commit_transition(start.next_state.run_key, start, WriteFence::Unfenced).await.unwrap());
            let closing = close(&state);
            let closed = applied(store.commit_transition(state.run_key, closing, WriteFence::Unfenced).await.unwrap());
            let page = store.read_intent_page(home(&state), ScanBound::Capture, None, 100).await.unwrap();
            prop_assert_eq!(page.rows.len(), usize::from(count));
            for (index, descriptor) in page.rows.iter().enumerate() {
                let row = store.load_intent(descriptor.key).await.unwrap().unwrap();
                prop_assert_eq!(row.key.kind, IntentKind::ParentClose);
                prop_assert!(matches!(row.payload, IntentPayload::ParentClose { initiating_event, .. } if initiating_event == i64::try_from(index + 1).unwrap()), "close intent preserves initiation");
                if index < retired {
                    let proof = IntentRetirement { key: row.key, outcome: DefinitiveOutcome::Obsolete };
                    prop_assert!(store.retire_intent(proof, WriteFence::Unfenced).await.unwrap());
                    prop_assert!(!store.retire_intent(proof, WriteFence::Unfenced).await.unwrap());
                }
            }
            let restored = InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap();
            restored.reconcile_outbound_run(home(&state), state.run_key, WriteFence::Unfenced).await.unwrap();
            let after = restored.read_intent_page(home(&state), ScanBound::Capture, None, 100).await.unwrap();
            prop_assert_eq!(after.rows.iter().map(|row| row.key).collect::<Vec<_>>(), page.rows.iter().skip(retired).map(|row| row.key).collect::<Vec<_>>());
            prop_assert_eq!(restored.load_run(state.run_key).await.unwrap(), LoadedRun::Existing(closed));
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 4: Ownership changes cannot authorize stale commits.
    #[test]
    fn obsolete_source_cannot_commit_a_late_destination(seed in any::<u32>(), category in 0u8..3) {
        block_on(async {
            let store = InMemoryStore::default();
            let start = source(seed, 1, category);
            let source = applied(store.commit_transition(start.next_state.run_key, start, WriteFence::Unfenced).await.unwrap());
            let operation = store.read_operation_page(home(&source), kind(category), ScanBound::Capture, None, 1).await.unwrap().rows.remove(0);
            let guard = DeliveryGuard::Operation { incarnation: operation.incarnation, source_seq: source.transition_seq };
            let mut target = fresh_transition(RunKey::new()); target.next_state.workflow_id = WorkflowId("guarded-target".into());
            applied(store.commit_transition(source.run_key, close(&source), WriteFence::Unfenced).await.unwrap());
            let before = store.snapshot().await.unwrap();
            let error = store.commit_outbound_transition(target.next_state.run_key, target, guard, WriteFence::Unfenced, WriteFence::Unfenced).await.unwrap_err();
            prop_assert!(matches!(error.downcast_ref::<crate::RecoveryError>(), Some(crate::RecoveryError::Obsolete)));
            prop_assert_eq!(store.snapshot().await.unwrap(), before);
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 16: Legacy preparation never guesses or crosses an unchecked boundary.
    #[test]
    fn snapshots_preserve_required_child_gates(seed in any::<u32>(), released in any::<bool>()) {
        block_on(async {
            let store = InMemoryStore::default();
            let mut start = source(seed, 0, 0);
            let gate = ChildStartGate { parent_run_key: RunKey::new(), initiating_event: 7, first_child_run_id: start.next_state.run_id, released };
            start.next_state.outbound.as_mut().unwrap().child_gate = Some(gate.clone());
            let key = start.next_state.run_key;
            applied(store.commit_transition(key, start, WriteFence::Unfenced).await.unwrap());
            let restored = InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap();
            let LoadedRun::Existing(state) = restored.load_run(key).await.unwrap() else { panic!("run missing"); };
            prop_assert_eq!(state.outbound.unwrap().child_gate, Some(gate));
            let old_version = postcard::to_allocvec(&5_u32).unwrap();
            prop_assert!(matches!(InMemoryStore::from_snapshot(&old_version), Err(SnapshotError::VersionMismatch { found: 5, .. })), "old snapshot is refused");
            Ok(())
        })?;
    }
}
