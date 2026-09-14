//! New campaign capability fields stay disabled until their owning delta specs land.

use std::collections::BTreeMap;

use tokeira_edge::{
    grpc::translate::{namespace_to_proto, system_info_to_proto},
    translate::{NamespaceCapabilities, NamespaceDescription, SystemCapabilities, SystemInfo},
};

// Feature: temporal-v1.32-compatibility, Property 7: capability literals match the policy table
#[test]
fn capability_literals_match_policy_table() {
    for standalone in [false, true] {
        let namespace = namespace_to_proto(
            NamespaceDescription {
                name: "default".to_string(),
                namespace_id: Some("ns".to_string()),
                is_global: false,
                visibility_enabled: true,
                deleted: false,
                description: String::new(),
                owner_email: String::new(),
                cluster_name: "local".to_string(),
                custom_search_attribute_aliases: BTreeMap::new(),
                retention: time::Duration::days(1),
                capabilities: NamespaceCapabilities {
                    worker_heartbeats: true,
                    reported_problems_search_attribute: true,
                    worker_commands: false,
                    standalone_nexus_operation: false,
                    workflow_update_callbacks: false,
                    poller_autoscaling_auto_enroll: false,
                    workflow_task_completion_pagination: false,
                    standalone_activity_start_delay: false,
                    standalone_activity_batch_operations: false,
                    standalone_activity_operator_commands: false,
                },
            },
            standalone,
        )
        .namespace_info
        .expect("namespace info");
        let caps = namespace.capabilities.expect("namespace capabilities");
        assert!(!caps.worker_commands);
        assert!(!caps.standalone_nexus_operation);
        assert!(!caps.workflow_update_callbacks);
        assert!(!caps.poller_autoscaling_auto_enroll);
        assert!(!caps.workflow_task_completion_pagination);
        assert!(!caps.standalone_activity_start_delay);
        assert!(!caps.standalone_activity_batch_operations);
        assert!(!caps.standalone_activity_operator_commands);
        assert_eq!(caps.standalone_activities, standalone);
        assert_eq!(
            namespace
                .limits
                .expect("namespace limits")
                .workflow_task_completion_size_limit_error,
            0
        );
    }
    let system = system_info_to_proto(SystemInfo {
        server_version: "1.31.0".to_string(),
        capabilities: SystemCapabilities {
            signal_and_query_header: true,
            internal_error_differentiation: true,
            activity_failure_include_heartbeat: true,
            supports_schedules: true,
            encoded_failure_attributes: true,
            build_id_based_versioning: true,
            upsert_memo: true,
            eager_workflow_start: true,
            sdk_metadata: true,
            count_group_by_execution_status: true,
            nexus: true,
            server_scaled_deployments: false,
            worker_heartbeats: true,
            server_scaled_provider_cloud_run: false,
        },
    });
    assert!(
        !system
            .capabilities
            .expect("system capabilities")
            .server_scaled_provider_cloud_run
    );
}
