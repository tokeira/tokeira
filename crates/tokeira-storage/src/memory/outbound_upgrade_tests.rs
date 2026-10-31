//! Checked cutover through real metadata commits, snapshots and startup gates.

use super::*;
use crate::{
    memory::projection_accumulator_tests::{applied, fresh_transition},
    outbound_upgrade::*,
};
use proptest::prelude::*;
use tokeira_kernel::{
    ChildWorkflowState, HistoryEvent, HistoryEventKind, ParentClosePolicy, PendingExternalSignal,
    WorkflowIdReusePolicy,
};
use tokeira_types::{Payloads, WorkflowType};

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

async fn legacy_source(repo: &InMemoryStore, index: u8, child: bool) -> WorkflowState {
    let mut transition = fresh_transition(RunKey::new());
    transition.next_state.workflow_id = WorkflowId(format!("legacy-{index}"));
    let state = &mut transition.next_state;
    let event = state.last_event_id + 1;
    let kind = if child {
        state.children.insert(
            WorkflowId("child".into()),
            ChildWorkflowState {
                child_workflow_id: WorkflowId("child".into()),
                namespace_id: state.namespace_id,
                namespace: None,
                workflow_type: WorkflowType("child".into()),
                header: None,
                child_run_id: None,
                initiated_event_id: event,
                started_event_id: None,
                parent_close_policy: ParentClosePolicy::Abandon,
            },
        );
        HistoryEventKind::StartChildWorkflowExecutionInitiated {
            workflow_task_completed_event_id: 0,
            child_workflow_id: WorkflowId("child".into()),
            workflow_type: WorkflowType("child".into()),
            task_queue: state.task_queue.clone(),
            input: Payloads::default(),
            namespace_id: state.namespace_id,
            namespace: None,
            header: None,
            memo: Default::default(),
            search_attributes: Default::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: state.workflow_task_timeout,
            retry_policy: None,
            cron_schedule: None,
            parent_close_policy: ParentClosePolicy::Abandon,
            priority: None,
        }
    } else {
        state.pending_external_signals.insert(
            event,
            PendingExternalSignal {
                initiated_event_id: event,
                target_namespace_id: state.namespace_id,
                target_namespace: None,
                target_workflow_id: WorkflowId("destination".into()),
                target_run_id: None,
                signal_name: "signal".into(),
            },
        );
        HistoryEventKind::SignalExternalWorkflowExecutionInitiated {
            workflow_task_completed_event_id: 0,
            namespace_id: state.namespace_id,
            namespace: None,
            target_workflow_id: WorkflowId("destination".into()),
            target_run_id: None,
            signal_name: "signal".into(),
            input: Payloads::default(),
            header: None,
            control: String::new(),
        }
    };
    transition.history_events.push(HistoryEvent {
        event_id: event,
        happened_at: state.started_at,
        kind,
    });
    transition.event_principals.push(None);
    state.last_event_id = event;
    let mut source = applied(
        repo.commit_transition(state.run_key, transition.clone(), WriteFence::Unfenced)
            .await
            .unwrap(),
    );
    source.outbound = None;
    let mut store = repo.inner.lock().await;
    store.runs.insert(source.run_key, source.clone());
    store
        .outbound_operations
        .retain(|key, _| key.source_run != source.run_key);
    source
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: outbound-dispatch-recovery, Property 16: Legacy preparation never guesses or crosses an unchecked boundary.
    #[test]
    fn current_unresolved_records_remain_visible_without_implicit_repair(missing_history in any::<bool>(), seed in any::<u8>()) {
        block_on(async {
            let repo = InMemoryStore::default();
            let source = legacy_source(&repo, seed, true).await;
            {
                let mut store = repo.inner.lock().await;
                store.runs.get_mut(&source.run_key).unwrap().outbound = Some(Default::default());
                if missing_history { store.history.get_mut(&source.run_key).unwrap().pop(); store.history_principals.get_mut(&source.run_key).unwrap().pop(); }
            }
            let before = repo.snapshot().await.unwrap();
            let plan = plan_outbound_recovery(&repo, &[]).await.unwrap();
            prop_assert_eq!(plan.legacy, 0);
            prop_assert_eq!(plan.pending, 1);
            prop_assert_eq!(plan.unresolved_count, 1);
            prop_assert_eq!(plan.unresolved[0].missing_fields.as_slice(), &[if missing_history { "initiation_history".to_string() } else { "original_reuse_policy_or_verified_existing_child".to_string() }]);
            prop_assert!(apply_outbound_recovery(&repo, &plan.digest, true, &[]).await.is_err());
            prop_assert_eq!(repo.snapshot().await.unwrap(), before);
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 15: Large recovery data round-trips within bounded writes.
    #[test]
    fn legacy_large_state_and_index_fanout_remain_bounded_after_interruption(size in prop_oneof![9 => 0usize..32_768, 1 => 4usize * 1024 * 1024..4usize * 1024 * 1024 + 2048], count in 1001usize..1020, cut in 1usize..8) {
        block_on(async {
            let repo = InMemoryStore::default();
            let source = legacy_source(&repo, 0, false).await;
            let first = source.pending_external_signals.keys().next().copied().unwrap();
            let mut expected = source.clone();
            expected.last_completion_result = Some(Payloads(vec![tokeira_types::Payload::new(vec![7; size])]));
            let mut events = repo.read_history(source.run_key, 0, 100).await.unwrap();
            let prototype = events.iter().find(|event| event.event_id == first).unwrap().clone();
            let operation = expected.pending_external_signals[&first].clone();
            for index in 1..count {
                let mut operation = operation.clone(); operation.initiated_event_id = first + index as i64;
                expected.pending_external_signals.insert(operation.initiated_event_id, operation);
                let mut event = prototype.clone(); event.event_id = first + index as i64; events.push(event);
            }
            expected.last_event_id = events.last().unwrap().event_id;
            {
                let mut store = repo.inner.lock().await;
                store.runs.insert(source.run_key, expected.clone()); store.history.insert(source.run_key, events.clone());
                store.history_principals.insert(source.run_key, vec![None; events.len()]);
                store.modeled_transactions.clear();
            }
            let plan = plan_outbound_recovery(&repo, &[]).await.unwrap();
            prop_assert_eq!(plan.pending, count as u64);
            repo.inner.lock().await.bulk_write_failure = Some(cut);
            prop_assert!(apply_outbound_recovery(&repo, &plan.digest, true, &[]).await.is_err());
            prop_assert!(repo.ensure_outbound_prepared().await.is_err());
            let done = apply_outbound_recovery(&repo, &plan.digest, true, &[]).await.unwrap();
            prop_assert!(done.complete);
            let LoadedRun::Existing(actual) = repo.load_run(source.run_key).await.unwrap() else { panic!("source missing"); };
            prop_assert_eq!(actual.last_completion_result, expected.last_completion_result);
            prop_assert_eq!(actual.pending_external_signals, expected.pending_external_signals);
            prop_assert_eq!(repo.read_history(source.run_key, 0, events.len()).await.unwrap(), events);
            let store = repo.inner.lock().await;
            prop_assert_eq!(store.outbound_operations.len(), count);
            prop_assert!(store.modeled_transactions.len() > 10);
            for page in &store.modeled_transactions {
                prop_assert!(page.rows <= crate::write_budget::MAX_ROWS_PER_TRANSACTION);
                prop_assert!(page.bytes <= crate::write_budget::MAX_BYTES_PER_TRANSACTION);
            }
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 16: Legacy preparation never guesses or crosses an unchecked boundary.
    #[test]
    fn checked_cutover_resumes_exact_revision_and_keeps_public_history(count in 1u8..9, cut in 0usize..10, closed in any::<bool>(), live in any::<bool>()) {
        block_on(async {
            let repo = InMemoryStore::default();
            let mut history = std::collections::BTreeMap::new();
            for index in 0..count {
                let source = legacy_source(&repo, index, false).await;
                if closed && index == 0 {
                    let mut store = repo.inner.lock().await;
                    let state = store.runs.get_mut(&source.run_key).unwrap();
                    state.status = tokeira_types::ExecutionStatus::Completed;
                    state.closed_at = Some(state.started_at); state.pending_external_signals.clear();
                }
                history.insert(source.run_key, repo.read_history(source.run_key, 0, 100).await.unwrap());
            }
            prop_assert!(!repo.outbound_ready());
            prop_assert!(repo.ensure_outbound_prepared().await.is_err());
            let plan = plan_outbound_recovery(&repo, &[]).await.unwrap();
            prop_assert_eq!(plan.legacy, u64::from(count));
            prop_assert_eq!(plan.historical_unknown, u64::from(closed));
            let before = repo.snapshot().await.unwrap();
            prop_assert!(apply_outbound_recovery(&repo, &"0".repeat(64), true, &[]).await.is_err());
            prop_assert!(apply_outbound_recovery(&repo, &plan.digest, false, &[]).await.is_err());
            prop_assert_eq!(repo.snapshot().await.unwrap(), before);
            if live {
                repo.try_acquire_bundle(ShardId(0), "old-owner".into(), "owner:1".into()).await.unwrap();
                prop_assert!(apply_outbound_recovery(&repo, &plan.digest, true, &[]).await.is_err());
                repo.relinquish_bundle(ShardId(0), "old-owner".into(), ShardEpoch(1)).await.unwrap();
            }
            let plan = plan_outbound_recovery(&repo, &[]).await.unwrap();
            let repairs_digest = crate::schema_contract::sha256_hex(&postcard::to_allocvec(&Vec::<OutboundRepair>::new()).unwrap());
            let mut progress = OutboundUpgradeProgress { revision: 1, plan_digest: plan.digest.clone(), repairs_digest, ..Default::default() };
            repo.commit_outbound_upgrade_page(None, progress.clone(), None).await.unwrap();
            let stale = progress.clone();
            for _ in 0..cut.min(usize::from(count)) {
                let state = repo.read_outbound_upgrade_run(progress.after).await.unwrap().unwrap();
                let (write, issues) = classify(&repo, state, &[]).await.unwrap(); prop_assert!(issues.is_empty());
                let mut next = progress.clone(); next.revision += 1; next.after = Some(write.state.run_key);
                next.examined += 1; next.historical_unknown += u64::from(write.historical_unknown);
                repo.commit_outbound_upgrade_page(Some(progress), next.clone(), Some(write)).await.unwrap(); progress = next;
            }
            let restored = InMemoryStore::from_snapshot(&repo.snapshot().await.unwrap()).unwrap();
            prop_assert!(restored.ensure_outbound_prepared().await.is_err());
            let done = apply_outbound_recovery(&restored, &plan.digest, true, &[]).await.unwrap();
            prop_assert!(done.complete); prop_assert_eq!(done.examined, u64::from(count));
            prop_assert_eq!(done.historical_unknown, u64::from(closed));
            restored.ensure_outbound_prepared().await.unwrap();
            for (key, events) in history { prop_assert_eq!(restored.read_history(key, 0, 100).await.unwrap(), events); }
            let state = restored.inner.lock().await;
            prop_assert_eq!(state.outbound_operations.len(), usize::from(count) - usize::from(closed));
            prop_assert!(state.outbound_intents.is_empty()); drop(state);
            let stable = restored.snapshot().await.unwrap();
            prop_assert_eq!(apply_outbound_recovery(&restored, &plan.digest, true, &[]).await.unwrap(), done.clone());
            prop_assert!(restored.commit_outbound_upgrade_page(Some(stale), done, None).await.is_err());
            prop_assert_eq!(restored.snapshot().await.unwrap(), stable);
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 16: Legacy preparation never guesses or crosses an unchecked boundary.
    #[test]
    fn legacy_child_needs_original_policy_or_verified_existing_identity(adopt in any::<bool>(), unrelated in any::<bool>(), stale in any::<bool>(), choice in 0u8..4) {
        block_on(async {
            let repo = InMemoryStore::default();
            let source = legacy_source(&repo, 0, true).await;
            let initiation = source.children.values().next().unwrap().initiated_event_id;
            let mut child_id = None;
            if adopt {
                let mut start = fresh_transition(RunKey::new());
                let child = &mut start.next_state;
                child.namespace_id = source.namespace_id; child.workflow_id = WorkflowId("child".into());
                child.run_key = RunKey::derive(child.namespace_id, &child.workflow_id, child.run_id);
                child.first_execution_run_id = Some(child.run_id);
                child.parent_run_key = Some(if unrelated { RunKey::new() } else { source.run_key });
                child.parent_namespace_id = Some(source.namespace_id); child.parent_workflow_id = Some(source.workflow_id.clone());
                child.parent_run_id = Some(source.run_id); child.parent_initiated_event_id = initiation;
                child_id = Some(child.run_id);
                let key = child.run_key;
                repo.commit_transition(key, start, WriteFence::Unfenced).await.unwrap();
                repo.inner.lock().await.runs.get_mut(&key).unwrap().outbound = None;
            }
            let plan = plan_outbound_recovery(&repo, &[]).await.unwrap();
            prop_assert_eq!(plan.unresolved_count == 0, adopt && !unrelated);
            if plan.unresolved_count != 0 {
                prop_assert!(apply_outbound_recovery(&repo, &plan.digest, true, &[]).await.is_err());
            }
            let policy = match choice { 0 => WorkflowIdReusePolicy::AllowDuplicate, 1 => WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
                2 => WorkflowIdReusePolicy::RejectDuplicate, _ => WorkflowIdReusePolicy::TerminateIfRunning };
            let repairs = if adopt && !unrelated { Vec::new() } else { vec![OutboundRepair {
                run: source.run_key, source_seq: source.transition_seq, source_digest: outbound_source_digest(&source).unwrap(),
                initiation, original_policy: Some(policy), existing_child: None,
            }] };
            let plan = plan_outbound_recovery(&repo, &repairs).await.unwrap();
            prop_assert_eq!(plan.unresolved_count, 0);
            if stale {
                repo.inner.lock().await.runs.get_mut(&source.run_key).unwrap().transition_seq.0 += 1;
                prop_assert!(apply_outbound_recovery(&repo, &plan.digest, true, &repairs).await.is_err());
            } else {
                apply_outbound_recovery(&repo, &plan.digest, true, &repairs).await.unwrap();
                let LoadedRun::Existing(parent) = repo.load_run(source.run_key).await.unwrap() else { panic!("source missing"); };
                let metadata = parent.outbound.unwrap();
                if adopt && !unrelated {
                    prop_assert_eq!(metadata.legacy_adopted_children.get(&initiation).copied(), child_id);
                    prop_assert!(metadata.legacy_child_policies.is_empty());
                } else { prop_assert_eq!(metadata.legacy_child_policies.get(&initiation), Some(&policy)); }
            }
            Ok(())
        })?;
    }
}
