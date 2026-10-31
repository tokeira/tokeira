//! Required outbound metadata must survive storage without altering old layouts.

use proptest::prelude::*;
use tokeira_kernel::{
    ChildReleaseState, ChildStartGate, ChildStartSeed, OutboundState, ParentClosePolicy,
    WorkflowIdReusePolicy,
};
use tokeira_types::{RequestId, RunId, WorkflowId};
use uuid::Uuid;

use super::*;

macro_rules! child_initiation {
    ($variant:ident, $($extra:tt)*) => {
        tokeira_kernel::HistoryEventKind::$variant {
            workflow_task_completed_event_id: 0,
            child_workflow_id: WorkflowId(String::new()),
            workflow_type: tokeira_types::WorkflowType(String::new()),
            task_queue: tokeira_types::TaskQueueName(String::new()),
            input: Default::default(), namespace_id: tokeira_types::NamespaceId(Uuid::nil()),
            namespace: None, header: None, memo: Default::default(), search_attributes: Default::default(),
            workflow_execution_timeout: None, workflow_run_timeout: None,
            workflow_task_timeout: time::Duration::ZERO, retry_policy: None, cron_schedule: None,
            parent_close_policy: ParentClosePolicy::Terminate, priority: None, $($extra)*
        }
    };
}

#[test]
fn legacy_child_initiation_has_its_frozen_positional_layout() {
    let kind = child_initiation!(StartChildWorkflowExecutionInitiated,);
    let mut frozen = vec![23, 0, 0, 0, 0, 0, 16];
    frozen.extend([0; 28]);
    assert_eq!(postcard::to_allocvec(&kind).unwrap(), frozen);
    assert_eq!(kind.child_reuse_policy(), None);
    let event = HistoryEvent {
        event_id: 7,
        happened_at: OffsetDateTime::UNIX_EPOCH,
        kind,
    };
    let bytes = encode_enveloped(
        PREVIOUS_HISTORY_BATCH_ENVELOPE_VERSION,
        &vec![event.clone()],
    )
    .unwrap();
    assert_eq!(
        decode_history_events(tests::layout_state().run_key, &bytes).unwrap(),
        vec![event]
    );
}

fn metadata(seed: u64, released: bool) -> OutboundState {
    let child_run_id = RunId(Uuid::from_u128(u128::from(seed) + 1));
    OutboundState {
        child_seeds: [(
            7,
            ChildStartSeed {
                child_run_id,
                request_id: RequestId(format!("child-{seed}")),
                prepared_at: OffsetDateTime::UNIX_EPOCH,
                parent_namespace_name: Some("parent-ns".into()),
                root_workflow_id: Some(WorkflowId("root".into())),
                root_run_id: Some(child_run_id),
                effective_priority: None,
                versioning_info: None,
            },
        )]
        .into(),
        child_releases: [(
            7,
            ChildReleaseState {
                namespace_id: tests::layout_state().namespace_id,
                workflow_id: WorkflowId("child".into()),
                initiating_event: 7,
                child_run_id,
                parent_started_event_id: Some(11),
                parent_close_policy: ParentClosePolicy::Abandon,
            },
        )]
        .into(),
        child_gate: Some(ChildStartGate {
            parent_run_key: RunKey(Uuid::from_u128(u128::from(seed) + 2)),
            initiating_event: 7,
            first_child_run_id: child_run_id,
            released,
        }),
        legacy_child_policies: [(19, WorkflowIdReusePolicy::RejectDuplicate)].into(),
        reset_request_id: Some(RequestId(format!("reset-{seed}"))),
        ..Default::default()
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: outbound-dispatch-recovery, Property 11: Child results resolve the correct initiation
    #[test]
    fn terminal_child_run_identity_round_trips(seed in any::<u128>(), timed_out in any::<bool>()) {
        let run_id = Some(RunId(Uuid::from_u128(seed)));
        let kind = if timed_out {
            tokeira_kernel::HistoryEventKind::ChildWorkflowExecutionTimedOutV2 {
                child_workflow_id: WorkflowId("child".into()), namespace_id: tests::layout_state().namespace_id,
                namespace: Some("child-ns".into()), child_run_id: run_id,
                workflow_type: tokeira_types::WorkflowType("child-type".into()),
                retry_state: tokeira_kernel::RetryState::MaximumAttemptsReached,
                initiated_event_id: 4, started_event_id: 6,
            }
        } else {
            tokeira_kernel::HistoryEventKind::ChildWorkflowExecutionTerminatedV2 {
                child_workflow_id: WorkflowId("child".into()), namespace_id: tests::layout_state().namespace_id,
                namespace: Some("child-ns".into()), child_run_id: run_id,
                workflow_type: tokeira_types::WorkflowType("child-type".into()),
                initiated_event_id: 4, started_event_id: 6,
            }
        };
        let events = vec![HistoryEvent { event_id: 7, happened_at: OffsetDateTime::UNIX_EPOCH, kind }];
        prop_assert_eq!(decode_history_events(tests::layout_state().run_key, &encode_history_events(&events).unwrap()).unwrap(), events);
    }


    // Feature: outbound-dispatch-recovery, Property 15: Large recovery data round-trips within bounded writes.
    #[test]
    fn external_resolution_metadata_round_trips(control in "[a-z0-9]{0,1024}", completed in 1i64..1000) {
        let event = HistoryEvent { event_id: 9, happened_at: OffsetDateTime::UNIX_EPOCH,
            kind: tokeira_kernel::HistoryEventKind::SignalExternalWorkflowExecutionFailedV2 {
                initiated_event_id: 7, namespace_id: tests::layout_state().namespace_id,
                namespace: None, target_workflow_id: WorkflowId("target".into()), target_run_id: None,
                cause: "NAMESPACE_NOT_FOUND".into(), metadata: tokeira_kernel::ExternalResolutionMetadata {
                    control, workflow_task_completed_event_id: completed,
                },
            },
        };
        let encoded = encode_history_events(std::slice::from_ref(&event)).unwrap();
        prop_assert_eq!(decode_history_events(tests::layout_state().run_key, &encoded).unwrap(), vec![event]);
    }

    // Feature: outbound-dispatch-recovery, Property 16: Legacy preparation never guesses or crosses an unchecked boundary
    #[test]
    fn new_child_policy_round_trips_behind_the_required_history_envelope(choice in 0usize..4) {
        let policy = [WorkflowIdReusePolicy::AllowDuplicate, WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
            WorkflowIdReusePolicy::RejectDuplicate, WorkflowIdReusePolicy::TerminateIfRunning][choice];
        let kind = child_initiation!(StartChildWorkflowExecutionInitiatedV2, reuse_policy: policy,);
        prop_assert_eq!(kind.child_reuse_policy(), Some(policy));
        let legacy = postcard::to_allocvec(&child_initiation!(StartChildWorkflowExecutionInitiated,)).unwrap();
        let current = postcard::to_allocvec(&kind).unwrap();
        prop_assert_eq!(&current[1..current.len()-1], &legacy[1..]);
        prop_assert_eq!(current[0], 59);
        let events = vec![HistoryEvent { event_id: 7, happened_at: OffsetDateTime::UNIX_EPOCH, kind }];
        let encoded = encode_history_events(&events).unwrap();
        let run = tests::layout_state().run_key;
        prop_assert_eq!(decode_history_events(run, &encoded).unwrap(), events);
        prop_assert!(envelope_payload("history_batch.events_data", &[PREVIOUS_HISTORY_BATCH_ENVELOPE_VERSION], run, &encoded).is_err());
    }

    // Feature: outbound-dispatch-recovery, Property 16: Legacy preparation never guesses or crosses an unchecked boundary
    #[test]
    fn required_metadata_round_trips(seed in any::<u64>(), released in any::<bool>()) {
        let mut state = tests::layout_state();
        state.outbound = Some(metadata(seed, released));
        let bytes = encode_workflow_state(&state).unwrap();
        prop_assert_eq!(workflow_state_encoded_len(&state).unwrap(), bytes.len());
        prop_assert_eq!(decode_workflow_state(state.run_key, &bytes).unwrap(), state.clone());
        for supported in [PREVIOUS_WORKFLOW_STATE_ENVELOPE_VERSION, PRE_OUTBOUND_WORKFLOW_STATE_ENVELOPE_VERSION] {
            let error = envelope_payload(WORKFLOW_STATE_KIND, &[supported], state.run_key, &bytes)
                .unwrap_err().downcast::<BlobFormatError>().unwrap();
            prop_assert_eq!(error.observed, WORKFLOW_STATE_ENVELOPE_VERSION);
        }
    }

    // Feature: outbound-dispatch-recovery, Property 16: Legacy preparation never guesses or crosses an unchecked boundary
    #[test]
    fn unknown_required_version_cannot_be_read_or_written(version in 2u32..u32::MAX) {
        let mut state = tests::layout_state();
        let outbound = OutboundState { version, ..Default::default() };
        state.outbound = Some(outbound.clone());
        prop_assert!(encode_workflow_state(&state).is_err());
        prop_assert!(workflow_state_encoded_len(&state).is_err());
        let mut bytes = encode_enveloped(WORKFLOW_STATE_ENVELOPE_VERSION, &state).unwrap();
        bytes.extend(encode_extension(WORKFLOW_STATE_EXTENSION_MAGIC, &[ExtensionSection {
            tag: OUTBOUND_STATE_SECTION,
            payload: postcard::to_allocvec(&outbound).unwrap(),
        }]).unwrap());
        prop_assert!(decode_workflow_state(state.run_key, &bytes)
            .unwrap_err().downcast::<StateExtensionError>().is_ok());
    }
}

#[test]
fn legacy_absence_is_not_current_empty_metadata() {
    let state = tests::layout_state();
    for version in [
        PREVIOUS_WORKFLOW_STATE_ENVELOPE_VERSION,
        PRE_OUTBOUND_WORKFLOW_STATE_ENVELOPE_VERSION,
    ] {
        let bytes = encode_enveloped(version, &state).unwrap();
        let legacy = decode_workflow_state(state.run_key, &bytes).unwrap();
        assert_eq!(legacy.outbound, None);
        assert!(encode_workflow_state(&legacy).is_err());
        assert!(workflow_state_encoded_len(&legacy).is_err());
    }
    let bytes = encode_workflow_state(&state).unwrap();
    assert_eq!(
        decode_workflow_state(state.run_key, &bytes)
            .unwrap()
            .outbound,
        Some(OutboundState::default())
    );
}
