//! Lossless translation for process-local worker heartbeat observations.
//!
//! The runtime consumes compact liveness fields and treats the encoded public
//! heartbeat as opaque. This module alone owns protobuf encoding/decoding so
//! `DescribeWorker` and `ListWorkers` can reproduce every worker-authored field.

use prost::Message as _;
use time::OffsetDateTime;
use tokeira_proto::public::temporal::api::{
    deployment::v1::WorkerDeploymentVersion, worker::v1 as proto_worker,
};
use tokeira_types::{
    NamespaceId, TaskQueueName, WorkerHeartbeat, WorkerHeartbeatStatus, WorkerIdentity,
    WorkerInstanceKey,
};

/// Decode one public heartbeat into the compact runtime observation model.
pub fn worker_heartbeat_from_proto(
    namespace_id: NamespaceId,
    proto: proto_worker::WorkerHeartbeat,
    now: OffsetDateTime,
) -> WorkerHeartbeat {
    tracing::trace!(
        worker_instance_key = %proto.worker_instance_key,
        "decoded worker heartbeat",
    );

    let encoded_heartbeat = proto.encode_to_vec();
    let (build_id, deployment_name) = proto
        .deployment_version
        .map(|version| (Some(version.build_id), Some(version.deployment_name)))
        .unwrap_or((None, None));

    WorkerHeartbeat {
        namespace_id,
        worker_instance_key: WorkerInstanceKey(proto.worker_instance_key),
        task_queue: TaskQueueName(proto.task_queue),
        worker_identity: WorkerIdentity(proto.worker_identity),
        last_seen: now,
        status: WorkerHeartbeatStatus(proto.status),
        build_id,
        deployment_name,
        sdk_name: non_empty(proto.sdk_name),
        sdk_version: non_empty(proto.sdk_version),
        encoded_heartbeat,
    }
}

/// Reconstruct the complete public heartbeat stored by the edge.
///
/// Empty encodings exist only in unit fixtures written before Tier 8.42. The
/// fallback keeps those fixtures usable while every production admission takes
/// the lossless decode branch.
pub fn worker_heartbeat_to_proto(
    heartbeat: &WorkerHeartbeat,
) -> Result<proto_worker::WorkerHeartbeat, prost::DecodeError> {
    if !heartbeat.encoded_heartbeat.is_empty() {
        return proto_worker::WorkerHeartbeat::decode(heartbeat.encoded_heartbeat.as_slice());
    }
    Ok(proto_worker::WorkerHeartbeat {
        worker_instance_key: heartbeat.worker_instance_key.0.clone(),
        worker_identity: heartbeat.worker_identity.0.clone(),
        task_queue: heartbeat.task_queue.0.clone(),
        deployment_version: heartbeat
            .build_id
            .as_ref()
            .zip(heartbeat.deployment_name.as_ref())
            .map(|(build_id, deployment_name)| WorkerDeploymentVersion {
                build_id: build_id.clone(),
                deployment_name: deployment_name.clone(),
            }),
        sdk_name: heartbeat.sdk_name.clone().unwrap_or_default(),
        sdk_version: heartbeat.sdk_version.clone().unwrap_or_default(),
        status: heartbeat.status.0,
        ..Default::default()
    })
}

fn non_empty(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use prost_types::{Duration, Timestamp};
    use tokeira_proto::public::temporal::api::{
        deployment::v1::WorkerDeploymentVersion,
        worker::v1::{
            PluginInfo, StorageDriverInfo, WorkerHostInfo, WorkerPollerInfo, WorkerSlotsInfo,
        },
    };
    use uuid::Uuid;

    proptest! {
        #[test]
        fn decodes_compact_fields(
            worker_instance_key in ".*",
            task_queue in ".*",
            worker_identity in ".*",
            sdk_name in ".*",
            sdk_version in ".*",
            status in any::<i32>(),
            build_id in proptest::option::of(".*"),
            deployment_name in proptest::option::of(".*"),
            timestamp_seconds in any::<i64>(),
            timestamp_nanos in 0i32..1_000_000_000,
            slot_count in any::<i32>(),
            poller_count in any::<i32>(),
            sticky_hit in any::<i32>(),
            sticky_miss in any::<i32>(),
            cache_size in any::<i32>(),
        ) {
            let namespace_id = NamespaceId(Uuid::from_u128(1));
            let now = OffsetDateTime::UNIX_EPOCH;
            let deployment_version = match (build_id.clone(), deployment_name.clone()) {
                (Some(build_id), Some(deployment_name)) => Some(WorkerDeploymentVersion {
                    build_id,
                    deployment_name,
                }),
                _ => None,
            };
            let heartbeat = proto_worker::WorkerHeartbeat {
                worker_instance_key: worker_instance_key.clone(),
                worker_identity: worker_identity.clone(),
                host_info: Some(WorkerHostInfo {
                    host_name: "host".to_owned(),
                    worker_grouping_key: "group".to_owned(),
                    process_id: "process".to_owned(),
                    current_host_cpu_usage: 0.25,
                    current_host_mem_usage: 0.5,
                }),
                task_queue: task_queue.clone(),
                deployment_version,
                sdk_name: sdk_name.clone(),
                sdk_version: sdk_version.clone(),
                status,
                start_time: Some(Timestamp {
                    seconds: timestamp_seconds,
                    nanos: timestamp_nanos,
                }),
                heartbeat_time: Some(Timestamp {
                    seconds: timestamp_seconds.saturating_add(1),
                    nanos: timestamp_nanos,
                }),
                elapsed_since_last_heartbeat: Some(Duration {
                    seconds: 1,
                    nanos: timestamp_nanos,
                }),
                workflow_task_slots_info: Some(WorkerSlotsInfo {
                    current_available_slots: slot_count,
                    current_used_slots: slot_count.saturating_add(1),
                    slot_supplier_kind: "Fixed".to_owned(),
                    total_processed_tasks: slot_count.saturating_add(2),
                    total_failed_tasks: slot_count.saturating_add(3),
                    last_interval_processed_tasks: slot_count.saturating_add(4),
                    last_interval_failure_tasks: slot_count.saturating_add(5),
                }),
                activity_task_slots_info: Some(WorkerSlotsInfo::default()),
                nexus_task_slots_info: Some(WorkerSlotsInfo::default()),
                local_activity_slots_info: Some(WorkerSlotsInfo::default()),
                workflow_poller_info: Some(WorkerPollerInfo {
                    current_pollers: poller_count,
                    last_successful_poll_time: Some(Timestamp {
                        seconds: timestamp_seconds,
                        nanos: timestamp_nanos,
                    }),
                    is_autoscaling: true,
                }),
                workflow_sticky_poller_info: Some(WorkerPollerInfo::default()),
                activity_poller_info: Some(WorkerPollerInfo::default()),
                nexus_poller_info: Some(WorkerPollerInfo::default()),
                total_sticky_cache_hit: sticky_hit,
                total_sticky_cache_miss: sticky_miss,
                current_sticky_cache_size: cache_size,
                plugins: vec![PluginInfo {
                    name: "plugin".to_owned(),
                    version: "1.0".to_owned(),
                }],
                drivers: vec![StorageDriverInfo {
                    r#type: "driver".to_owned(),
                }],
            };

            let decoded = worker_heartbeat_from_proto(namespace_id, heartbeat.clone(), now);
            prop_assert_eq!(worker_heartbeat_to_proto(&decoded).unwrap(), heartbeat);

            prop_assert_eq!(decoded.namespace_id, namespace_id);
            prop_assert_eq!(decoded.worker_instance_key, WorkerInstanceKey(worker_instance_key));
            prop_assert_eq!(decoded.task_queue, TaskQueueName(task_queue));
            prop_assert_eq!(decoded.worker_identity, WorkerIdentity(worker_identity));
            prop_assert_eq!(decoded.last_seen, now);
            prop_assert_eq!(decoded.status, WorkerHeartbeatStatus(status));
            prop_assert_eq!(decoded.sdk_name, non_empty(sdk_name));
            prop_assert_eq!(decoded.sdk_version, non_empty(sdk_version));
            if build_id.is_some() && deployment_name.is_some() {
                prop_assert_eq!(decoded.build_id, build_id);
                prop_assert_eq!(decoded.deployment_name, deployment_name);
            } else {
                prop_assert_eq!(decoded.build_id, None);
                prop_assert_eq!(decoded.deployment_name, None);
            }
        }
    }

    #[test]
    fn empty_sdk_metadata_decodes_to_none() {
        let decoded = worker_heartbeat_from_proto(
            NamespaceId(Uuid::from_u128(1)),
            proto_worker::WorkerHeartbeat {
                worker_instance_key: "worker".to_string(),
                worker_identity: "identity".to_string(),
                host_info: None,
                task_queue: "queue".to_string(),
                deployment_version: None,
                sdk_name: String::new(),
                sdk_version: String::new(),
                status: 1,
                start_time: None,
                heartbeat_time: None,
                elapsed_since_last_heartbeat: None,
                workflow_task_slots_info: None,
                activity_task_slots_info: None,
                nexus_task_slots_info: None,
                local_activity_slots_info: None,
                workflow_poller_info: None,
                workflow_sticky_poller_info: None,
                activity_poller_info: None,
                nexus_poller_info: None,
                total_sticky_cache_hit: 0,
                total_sticky_cache_miss: 0,
                current_sticky_cache_size: 0,
                plugins: Vec::new(),
                drivers: Vec::new(),
            },
            OffsetDateTime::UNIX_EPOCH,
        );

        assert_eq!(decoded.sdk_name, None);
        assert_eq!(decoded.sdk_version, None);
    }
}
