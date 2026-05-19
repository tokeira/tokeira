use time::OffsetDateTime;
use tokeira_proto::public::temporal::api::worker::v1 as proto_worker;
use tokeira_types::{
    NamespaceId, TaskQueueName, WorkerHeartbeat, WorkerHeartbeatStatus, WorkerIdentity,
    WorkerInstanceKey,
};

pub fn worker_heartbeat_from_proto(
    namespace_id: NamespaceId,
    proto: proto_worker::WorkerHeartbeat,
    now: OffsetDateTime,
) -> WorkerHeartbeat {
    tracing::trace!(
        worker_instance_key = %proto.worker_instance_key,
        "decoded worker heartbeat",
    );

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
    }
}

fn non_empty(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tokeira_proto::public::temporal::api::deployment::v1::WorkerDeploymentVersion;
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
                host_info: None,
                task_queue: task_queue.clone(),
                deployment_version,
                sdk_name: sdk_name.clone(),
                sdk_version: sdk_version.clone(),
                status,
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
            };

            let decoded = worker_heartbeat_from_proto(namespace_id, heartbeat, now);

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
