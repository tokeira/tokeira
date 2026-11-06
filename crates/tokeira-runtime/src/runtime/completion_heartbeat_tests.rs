//! Persistence and runtime policy for heartbeat-only workflow-task chains.

use super::{
    completion_inline_tests::{completion, fixture},
    discovery_tests::stop,
    *,
};
use tokeira_storage::InMemoryStore;

#[tokio::test]
async fn heartbeat_origin_survives_owner_reload_and_legacy_stays_absent() {
    for legacy in [false, true] {
        let (store, runtime, first, _) = fixture().await;
        let origin = first.scheduled_time;
        let mut request = completion(&first, true);
        request.force_new_workflow_task = true;
        request.heartbeat.wire_empty = true;
        request.now = origin + Duration::minutes(20);
        let outcome = runtime
            .complete_workflow_task_with_result(request)
            .await
            .unwrap();
        let inline = outcome.workflow_task.unwrap();
        stop(&runtime).await;
        if legacy {
            let LoadedRun::Existing(mut state) = store.load_run(first.run_key).await.unwrap()
            else {
                panic!("run");
            };
            let expected_seq = state.transition_seq;
            state.transition_seq = expected_seq.next();
            state
                .pending_workflow_task
                .as_mut()
                .unwrap()
                .original_scheduled_at = None;
            store
                .commit_transition(
                    first.run_key,
                    tokeira_kernel::Transition {
                        expected_seq,
                        next_state: state,
                        history_events: Default::default(),
                        event_principals: Default::default(),
                        request_dedupe_ops: Default::default(),
                        activity_ops: Default::default(),
                        timer_ops: Default::default(),
                        dispatch_ops: Default::default(),
                        events_numbered_at_close: 0,
                        workflow_task_heartbeat_timed_out: false,
                        growth_limits: None,
                    },
                    tokeira_storage::WriteFence::Unfenced,
                )
                .await
                .unwrap();
        }
        let restored =
            Arc::new(InMemoryStore::from_snapshot(&store.snapshot().await.unwrap()).unwrap());
        tokeira_storage::prepare_execution_placement(restored.as_ref())
            .await
            .unwrap();
        restored.ensure_outbound_prepared().await.unwrap();
        let runtime = TokeiraRuntime::new(
            restored.clone(),
            1,
            LaneConfig::default(),
            TimerScannerConfig::default(),
            WorkflowTimeoutScannerConfig::default(),
        );
        runtime.ready().await.unwrap();
        let LoadedRun::Existing(state) = restored.load_run(first.run_key).await.unwrap() else {
            panic!("run");
        };
        assert_eq!(
            state
                .pending_workflow_task
                .as_ref()
                .unwrap()
                .original_scheduled_at,
            (!legacy).then_some(origin)
        );
        assert!(
            runtime
                .wft_timeout_tracking
                .snapshot()
                .iter()
                .any(|entry| entry.started_event_id == inline.token.started_event_id)
        );
        let mut request = completion(&inline, true);
        request.force_new_workflow_task = true;
        request.heartbeat.wire_empty = true;
        request.now = origin + Duration::minutes(31);
        let result = runtime.complete_workflow_task_with_result(request).await;
        if legacy {
            let CommitResult::Applied { new_state } = result.unwrap().commit else {
                panic!("commit");
            };
            assert_eq!(
                new_state
                    .pending_workflow_task
                    .unwrap()
                    .original_scheduled_at,
                None
            );
        } else {
            assert!(
                result
                    .unwrap_err()
                    .is::<crate::errors::WorkflowTaskHeartbeatTimedOut>()
            );
        }
        stop(&runtime).await;
    }
}

#[tokio::test]
async fn heartbeat_override_is_scoped_and_production_ignores_it() {
    #[cfg(feature = "conformance")]
    crate::conformance::install_registry_reads();
    let registry = tokeira_conformance::overrides();
    registry
        .set(
            "history.workflowTaskHeartbeatTimeout",
            tokeira_conformance::OverrideValue::Duration(std::time::Duration::from_secs(5)),
        )
        .unwrap();
    for overridden in [true, false] {
        if !overridden {
            registry.reset();
        }
        let (_, runtime, first, _) = fixture().await;
        let mut request = completion(&first, true);
        request.force_new_workflow_task = true;
        request.heartbeat.wire_empty = true;
        // Public runtime submission always resolves policy, even if an internal caller supplies one.
        request.heartbeat.timeout = Duration::ZERO;
        request.now = first.scheduled_time + Duration::seconds(6);
        let result = runtime.complete_workflow_task_with_result(request).await;
        assert_eq!(
            result
                .as_ref()
                .err()
                .is_some_and(|error| error.is::<crate::errors::WorkflowTaskHeartbeatTimedOut>()),
            overridden && cfg!(feature = "conformance")
        );
        stop(&runtime).await;
    }
}
