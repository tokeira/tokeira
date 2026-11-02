//! Frozen section-7 layout and binding checks for required activity-start receipts.
//! Only primitive fields are serialized: changes to runtime queue/identity types
//! must not silently change the format protecting same-request recovery.

use serde::{Deserialize, Serialize};
use tokeira_kernel::{ActivityStartReceipt, WorkflowState};
use tokeira_types::{
    BuildId, DeploymentId, NamespaceId, QueueKey, TaskKind, TaskQueueName, WorkerIdentity,
};
use uuid::Uuid;

/// Field order and types are frozen for state-extension tag 7.
#[derive(Debug, Serialize, Deserialize)]
struct FrozenReceipt {
    activity_id: String,
    request_id: [u8; 16],
    schedule_event_id: i64,
    attempt: u32,
    offered_stamp: u64,
    worker_identity: String,
    namespace_id: [u8; 16],
    task_queue: String,
    deployment: Option<String>,
    build_id: Option<String>,
    dispatch_revision: i64,
}

pub(super) fn encode(state: &WorkflowState) -> postcard::Result<Vec<u8>> {
    let entries = state
        .activity_start_receipts
        .iter()
        .map(|(id, receipt)| FrozenReceipt {
            activity_id: id.clone(),
            request_id: *receipt.request_id.as_bytes(),
            schedule_event_id: receipt.schedule_event_id,
            attempt: receipt.attempt,
            offered_stamp: receipt.offered_stamp,
            worker_identity: receipt.worker_identity.0.clone(),
            namespace_id: *receipt.selected_queue.namespace_id.0.as_bytes(),
            task_queue: receipt.selected_queue.task_queue.0.clone(),
            deployment: receipt
                .selected_queue
                .deployment
                .as_ref()
                .map(|value| value.0.clone()),
            build_id: receipt
                .selected_queue
                .build_id
                .as_ref()
                .map(|value| value.0.clone()),
            dispatch_revision: receipt.dispatch_revision,
        })
        .collect::<Vec<_>>();
    postcard::to_allocvec(&entries)
}

pub(super) fn apply(state: &mut WorkflowState, payload: &[u8]) -> Result<(), &'static str> {
    let entries = super::decode_exact::<Vec<FrozenReceipt>>(payload)
        .ok_or("undecodable activity-start receipt section")?;
    if !entries
        .windows(2)
        .all(|pair| pair[0].activity_id < pair[1].activity_id)
    {
        return Err("activity-start receipts out of order or listed twice");
    }
    state.activity_start_receipts = entries
        .into_iter()
        .map(|entry| {
            (
                entry.activity_id,
                ActivityStartReceipt {
                    request_id: Uuid::from_bytes(entry.request_id),
                    schedule_event_id: entry.schedule_event_id,
                    attempt: entry.attempt,
                    offered_stamp: entry.offered_stamp,
                    worker_identity: WorkerIdentity(entry.worker_identity),
                    selected_queue: QueueKey {
                        namespace_id: NamespaceId(Uuid::from_bytes(entry.namespace_id)),
                        task_queue: TaskQueueName(entry.task_queue),
                        task_kind: TaskKind::Activity,
                        deployment: entry.deployment.map(DeploymentId),
                        build_id: entry.build_id.map(BuildId),
                    },
                    dispatch_revision: entry.dispatch_revision,
                },
            )
        })
        .collect();
    validate_activity_receipts(state)
}

/// Validate without repairing: dropping a corrupt receipt could turn a committed
/// start into an apparent new request. Current stamps may advance independently
/// while an attempt stays started, so they are deliberately not compared.
pub(crate) fn validate_activity_receipts(state: &WorkflowState) -> Result<(), &'static str> {
    for (id, receipt) in &state.activity_start_receipts {
        if id.is_empty()
            || receipt.request_id.is_nil()
            || receipt.schedule_event_id <= 0
            || receipt.attempt == 0
            || i32::try_from(receipt.attempt).is_err()
            || i64::try_from(receipt.offered_stamp).is_err()
            || receipt.dispatch_revision < 0
        {
            return Err("invalid activity-start receipt identity");
        }
        let activity = state
            .activities
            .get(id)
            .ok_or("activity-start receipt has no activity")?;
        if activity.activity_id != *id
            || activity.schedule_event_id != receipt.schedule_event_id
            || activity.attempt != receipt.attempt
            || activity.started_at.is_none()
            || activity.started_event_id.is_none()
            || activity.started_identity.as_ref() != Some(&receipt.worker_identity)
        {
            return Err("activity-start receipt does not match its started incarnation");
        }
        if receipt.selected_queue.namespace_id != state.namespace_id
            || receipt.selected_queue.task_kind != TaskKind::Activity
        {
            return Err("activity-start receipt does not match its namespace and task kind");
        }
        // The selected queue records the accepted start. Options may move the
        // activity's next dispatch while this attempt is still running, so the
        // current queue is not evidence that its earlier receipt is corrupt.
    }
    Ok(())
}
