//! Required receipts preserve request identity and reject ambiguous persisted data.

use proptest::prelude::*;
use tokeira_kernel::ActivityStartReceipt;
use tokeira_types::{BuildId, DeploymentId, QueueKey, TaskKind, TaskQueueName, WorkerIdentity};
use uuid::Uuid;

use super::*;

fn with_receipt(state: &mut WorkflowState, id: &str, attempt: u32, stamp: u64, seed: u128) {
    let mut activity = crate::activity_dispatch_tests::pending_activity(id, 9);
    activity.task_queue = TaskQueueName("q".into());
    activity.attempt = attempt;
    activity.stamp = stamp + 1;
    activity.started_at = Some(activity.scheduled_at);
    activity.started_event_id = Some(-2);
    activity.started_identity = Some(WorkerIdentity(String::new()));
    state.activities.insert(id.into(), activity);
    state.activity_start_receipts.insert(
        id.into(),
        ActivityStartReceipt {
            request_id: Uuid::from_u128(seed + 1),
            schedule_event_id: 9,
            attempt,
            offered_stamp: stamp,
            worker_identity: WorkerIdentity(String::new()),
            selected_queue: QueueKey {
                namespace_id: state.namespace_id,
                task_queue: TaskQueueName("q".into()),
                task_kind: TaskKind::Activity,
                deployment: None,
                build_id: None,
            },
            dispatch_revision: 4,
        },
    );
}

fn blob_with_sections(state: &WorkflowState, sections: &[ExtensionSection]) -> Vec<u8> {
    let mut blob = encode_enveloped(WORKFLOW_STATE_ENVELOPE_VERSION, state).unwrap();
    blob.extend(encode_extension(WORKFLOW_STATE_EXTENSION_MAGIC, sections).unwrap());
    blob
}

fn receipt_section(state: &WorkflowState) -> ExtensionSection {
    ExtensionSection {
        tag: ACTIVITY_START_RECEIPTS_SECTION,
        payload: activity_receipts::encode(state).unwrap(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: activity-dispatch, Property 16
    // Required receipt identity survives unrelated state extensions and stamp changes.
    #[test]
    fn receipt_codec_round_trips(
        entries in prop::collection::vec((1u32..1000, 0u64..1_000_000, any::<u64>(), any::<bool>()), 0..12),
        signal_count in any::<u64>(),
    ) {
        let mut state = tests::layout_state();
        state.signal_count = signal_count;
        state.used_worker_deployment_versions = Some(vec!["untouched".into()]);
        for (i, (attempt, stamp, seed, versioned)) in entries.iter().enumerate() {
            let id = format!("receipt-{i:03}");
            with_receipt(&mut state, &id, *attempt, *stamp, u128::from(*seed));
            if *versioned {
                let receipt = state.activity_start_receipts.get_mut(&id).unwrap();
                receipt.selected_queue.deployment = Some(DeploymentId(format!("deployment-{seed}")));
                receipt.selected_queue.build_id = Some(BuildId(String::new()));
            }
            state.activities.get_mut(&id).unwrap().stamp += 7;
            state.activities.get_mut(&id).unwrap().task_queue = TaskQueueName("next-queue".into());
        }
        let bytes = encode_workflow_state(&state).unwrap();
        prop_assert_eq!(workflow_state_encoded_len(&state).unwrap(), bytes.len());
        prop_assert_eq!(decode_workflow_state(state.run_key, &bytes).unwrap(), state.clone());
        for old in [PREVIOUS_WORKFLOW_STATE_ENVELOPE_VERSION, PRE_OUTBOUND_WORKFLOW_STATE_ENVELOPE_VERSION,
            PRE_ACTIVITY_RECEIPT_WORKFLOW_STATE_ENVELOPE_VERSION] {
            prop_assert!(envelope_payload(WORKFLOW_STATE_KIND, &[old], state.run_key, &bytes).is_err());
        }
    }

    // Feature: activity-dispatch, Property 16
    // Neither read nor write accepts a receipt bound to another incarnation or queue.
    #[test]
    fn invalid_receipt_bindings_fail_closed(defect in 0u8..13, seed in any::<u64>()) {
        let mut state = tests::layout_state();
        with_receipt(&mut state, "a", 2, 3, u128::from(seed));
        let receipt = state.activity_start_receipts.get_mut("a").unwrap();
        match defect {
            0 => receipt.request_id = Uuid::nil(),
            1 => receipt.attempt = 0,
            2 => receipt.attempt = u32::MAX,
            3 => receipt.offered_stamp = u64::MAX,
            4 => receipt.schedule_event_id = 0,
            5 => receipt.schedule_event_id += 1,
            6 => receipt.worker_identity.0 = "another-worker".into(),
            7 => receipt.selected_queue.namespace_id.0 = Uuid::nil(),
            8 => state.activities.get_mut("a").unwrap().activity_id = "another-slot".into(),
            9 => receipt.dispatch_revision = -1,
            10 => { state.activities.remove("a"); },
            11 => state.activities.get_mut("a").unwrap().started_event_id = None,
            _ => state.activities.get_mut("a").unwrap().started_at = None,
        }
        prop_assert!(encode_workflow_state(&state).is_err());
        prop_assert!(workflow_state_encoded_len(&state).is_err());
        let blob = blob_with_sections(&state, &[receipt_section(&state)]);
        prop_assert!(decode_workflow_state(state.run_key, &blob).is_err());
    }

    // Feature: activity-dispatch, Property 16
    // Corrupt framing cannot collapse two receipts into an apparently valid map.
    #[test]
    fn malformed_receipt_sections_are_rejected(defect in 0u8..4, seed in any::<u64>()) {
        let mut state = tests::layout_state();
        with_receipt(&mut state, "a", 1, 0, u128::from(seed));
        let mut section = receipt_section(&state);
        let sections = match defect {
            0 => vec![section.clone(), section],
            1 => { section.payload.push(0); vec![section] },
            2 => { section.payload.pop(); vec![section] },
            _ => {
                let entry = section.payload[1..].to_vec();
                section.payload[0] = 2;
                section.payload.extend(entry);
                vec![section]
            },
        };
        prop_assert!(decode_workflow_state(state.run_key, &blob_with_sections(&state, &sections)).is_err());
    }
}

#[test]
fn section_seven_bytes_are_frozen() {
    let mut state = tests::layout_state();
    with_receipt(&mut state, "a", 2, 3, 0);
    let mut expected = vec![1, 1, b'a'];
    expected.extend([0; 15]);
    expected.extend([1, 18, 2, 3, 0]);
    expected.extend([0; 15]);
    expected.extend([11, 1, b'q', 0, 0, 8]);
    assert_eq!(activity_receipts::encode(&state).unwrap(), expected);
    assert_eq!(WORKFLOW_STATE_ENVELOPE_VERSION, 0x544B_5734);
    assert_eq!(ACTIVITY_START_RECEIPTS_SECTION, 7);
    assert_eq!(HISTORY_BATCH_ENVELOPE_VERSION, 0x544B_4832);
}

#[test]
fn legacy_receipts_are_empty_without_changing_outbound_preparation() {
    let mut state = tests::layout_state();
    with_receipt(&mut state, "a", 1, 0, 0);
    for version in [
        PREVIOUS_WORKFLOW_STATE_ENVELOPE_VERSION,
        PRE_OUTBOUND_WORKFLOW_STATE_ENVELOPE_VERSION,
        PRE_ACTIVITY_RECEIPT_WORKFLOW_STATE_ENVELOPE_VERSION,
    ] {
        let bytes = encode_enveloped(version, &state).unwrap();
        let decoded = decode_workflow_state(state.run_key, &bytes).unwrap();
        assert!(decoded.activity_start_receipts.is_empty());
        assert_eq!(
            decoded.outbound.is_some(),
            version == PRE_ACTIVITY_RECEIPT_WORKFLOW_STATE_ENVELOPE_VERSION
        );
        let mut invalid = bytes;
        invalid.extend(
            encode_extension(WORKFLOW_STATE_EXTENSION_MAGIC, &[receipt_section(&state)]).unwrap(),
        );
        assert!(decode_workflow_state(state.run_key, &invalid).is_err());
    }
    state.activity_start_receipts.clear();
    let bytes = encode_workflow_state(&state).unwrap();
    assert!(
        decode_workflow_state(state.run_key, &bytes)
            .unwrap()
            .activity_start_receipts
            .is_empty()
    );
}

#[test]
fn receipt_queue_kind_is_checked_before_the_implicit_activity_encoding() {
    let mut state = tests::layout_state();
    with_receipt(&mut state, "a", 1, 0, 0);
    state
        .activity_start_receipts
        .get_mut("a")
        .unwrap()
        .selected_queue
        .task_kind = TaskKind::Workflow;
    assert!(encode_workflow_state(&state).is_err());
}
