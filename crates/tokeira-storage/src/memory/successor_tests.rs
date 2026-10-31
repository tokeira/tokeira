//! A deferred chain claim survives every storage boundary until publication.

use super::*;
use crate::{
    DefinitiveOutcome, IntentKind, IntentRetirement, PublicationResult, RecoveryError,
    RunPreparation, ScanBound, memory::projection_accumulator_tests::applied,
};
use proptest::prelude::*;
use time::Duration;
use tokeira_kernel::{Command, Kernel, StartRequest};
use tokeira_types::RequestContext;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

pub(crate) fn start_request(
    state: &WorkflowState,
    predecessor: &WorkflowState,
    bytes: usize,
) -> StartRequest {
    StartRequest {
        advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        initiator: None,
        run_key: state.run_key,
        namespace_id: state.namespace_id,
        workflow_id: state.workflow_id.clone(),
        run_id: state.run_id,
        workflow_type: state.workflow_type.clone(),
        task_queue: state.task_queue.clone(),
        deployment: None,
        build_id: None,
        versioning_override: None,
        workflow_start_delay: None,
        client_cron_schedule: None,
        completion_callbacks: Vec::new(),
        user_metadata: None,
        links: Vec::new(),
        on_conflict_options: None,
        priority: None,
        input: tokeira_types::Payloads(vec![tokeira_types::Payload::new(vec![42; bytes])]),
        header: None,
        memo: Default::default(),
        search_attributes: Default::default(),
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
        reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
        attempt: 1,
        continued_execution_run_id: Some(predecessor.run_id),
        first_execution_run_id: Some(
            predecessor
                .first_execution_run_id
                .unwrap_or(predecessor.run_id),
        ),
        parent_run_key: None,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_namespace_name: None,
        parent_initiated_event_id: 0,
        root_workflow_id: None,
        root_run_id: None,
        original_execution_run_id: Some(predecessor.run_id),
        continued_failure: None,
        last_completion_result: None,
        first_run_started_at: Some(predecessor.started_at),
        request: RequestContext::unattributed(state.started_at),
        now: state.started_at,
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
    }
}

async fn fixture(
    kind: u8,
    bytes: usize,
) -> (
    InMemoryStore,
    WorkflowState,
    Transition,
    StartRequest,
    Transition,
) {
    let store = InMemoryStore::default();
    let mut initial = crate::atomic_successors_test_support::start();
    let s = &mut initial.next_state;
    s.run_key = RunKey::derive(s.namespace_id, &s.workflow_id, s.run_id);
    let state = applied(
        store
            .commit_transition(s.run_key, initial, WriteFence::Unfenced)
            .await
            .unwrap(),
    );
    let (close, mut next) = crate::atomic_successors_test_support::pair(
        &state,
        kind,
        Default::default(),
        Default::default(),
        None,
    );
    let s = &mut next.next_state;
    s.run_key = RunKey::derive(s.namespace_id, &s.workflow_id, s.run_id);
    let start = start_request(s, &state, bytes);
    let successor = BasicKernel
        .apply(LoadedRun::Absent, Command::Start(start.clone()))
        .unwrap();
    (store, state, close, start, successor)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: outbound-dispatch-recovery, Property 14: Successor fallback preserves the chain claim
    #[test]
    fn reservation_survives_restart_and_blocks_competing_pointer_writers(kind in 0u8..4, bytes in 0usize..4096, restart in any::<bool>(), lose_reply in any::<bool>()) {
        block_on(async {
            let (store, before, close, start, successor) = fixture(kind, bytes).await;
            let closed = applied(store.commit_successor_fallback(before.run_key, close.clone(), Box::new(start.clone()), WriteFence::Unfenced).await.unwrap());
            let store = if restart { InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap() } else { store };
            prop_assert!(matches!(store.commit_successor_fallback(before.run_key, close, Box::new(start.clone()), WriteFence::Unfenced).await.unwrap(), CommitResult::Duplicate));
            let home = ShardId(0);
            let page = store.read_intent_page(home, ScanBound::Capture, None, 100).await.unwrap();
            let key = page.rows.iter().find(|r| r.key.kind == IntentKind::Successor).unwrap().key;
            let mut competitor = start.clone(); competitor.run_id = RunId::new();
            competitor.run_key = RunKey::derive(competitor.namespace_id, &competitor.workflow_id, competitor.run_id);
            let competing = BasicKernel.apply(LoadedRun::Absent, Command::Start(competitor.clone())).unwrap();
            let original = store.snapshot().await.unwrap();
            let error = store.commit_transition(competitor.run_key, competing.clone(), WriteFence::Unfenced).await.unwrap_err();
            prop_assert!(matches!(error.downcast_ref::<RecoveryError>(), Some(RecoveryError::SuccessorReserved { .. })), "competing start is retryable");
            let error = store.delete_run_for_bundle(before.run_key, home, DeleteRunRequest { expected_seq: closed.transition_seq, deleted_at: closed.started_at }, WriteFence::Unfenced).await.unwrap_err();
            prop_assert!(matches!(error.downcast_ref::<RecoveryError>(), Some(RecoveryError::SuccessorReserved { .. })), "delete cannot steal chain claim");
            prop_assert!(store.retire_intent(IntentRetirement { key, outcome: DefinitiveOutcome::Absent }, WriteFence::Unfenced).await.is_err(), "generic retirement cannot release reservation");
            prop_assert_eq!(store.snapshot().await.unwrap(), original);

            let competing_reset = RunPreparation { request_key: "competing-reset".into(), expected_current: Some(before.run_key), participants: Vec::new(), new_run: Box::new(competing), successor_intent: None };
            let reset_token = store.prepare_run_publication(Box::new(competing_reset), WriteFence::Unfenced).await.unwrap();
            let error = store.publish_prepared_run(reset_token.clone(), WriteFence::Unfenced).await.unwrap_err();
            prop_assert!(matches!(error.downcast_ref::<RecoveryError>(), Some(RecoveryError::SuccessorReserved { .. })), "reset cannot steal chain claim");
            prop_assert!(store.abandon_prepared_run(reset_token, WriteFence::Unfenced).await.unwrap());

            let manifest = RunPreparation { request_key: key.storage_id(), expected_current: Some(before.run_key), participants: Vec::new(), new_run: Box::new(successor), successor_intent: Some(key) };
            let token = store.prepare_run_publication(Box::new(manifest), WriteFence::Unfenced).await.unwrap();
            prop_assert_eq!(store.load_run(start.run_key).await.unwrap(), LoadedRun::Absent);
            let store = InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap();
            prop_assert!(matches!(store.publish_prepared_run(token.clone(), WriteFence::Unfenced).await.unwrap(), PublicationResult::Applied { .. }), "reserved publication applies");
            prop_assert!(store.load_intent(key).await.unwrap().is_none());
            prop_assert!(store.inner.lock().await.successor_reservations.is_empty());
            prop_assert_eq!(store.inner.lock().await.current_execution.get(&(before.namespace_id, before.workflow_id.0.clone())).copied(), Some(start.run_key));
            if lose_reply {
                prop_assert!(matches!(store.publish_prepared_run(token, WriteFence::Unfenced).await.unwrap(), PublicationResult::AlreadyPublished { .. }), "published identity settles lost reply");
            }
            Ok(())
        })?;
    }

    // Feature: outbound-dispatch-recovery, Property 14: Successor fallback preserves the chain claim
    #[test]
    fn refused_fallback_exposes_neither_close_nor_reservation(kind in 0u8..4, malformed_recipe in any::<bool>(), bytes in 0usize..4096) {
        block_on(async {
            let (store, before, mut close, mut start, _) = fixture(kind, bytes).await;
            let original = store.snapshot().await.unwrap();
            if malformed_recipe { start.run_id = RunId::new(); }
            else { close.event_principals.clear(); }
            prop_assert!(store.commit_successor_fallback(before.run_key, close, Box::new(start), WriteFence::Unfenced).await.is_err());
            prop_assert_eq!(store.snapshot().await.unwrap(), original);
            Ok(())
        })?;
    }
}
