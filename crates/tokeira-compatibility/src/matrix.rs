use crate::{
    CompatibilityEvidence, CompatibilitySurface, CompatibilitySurfaceKind, FeatureEntry,
    FeatureState,
};

const START_EVIDENCE: &[CompatibilityEvidence] = &[CompatibilityEvidence {
    kind: crate::CompatibilityEvidenceKind::Test,
    reference: "apps/tokeirad/tests/grpc_roundtrip.rs",
}];

const WORKER_DEPLOYMENT_EVIDENCE: &[CompatibilityEvidence] = &[
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-edge/src/grpc/workflow_service.rs::worker_deployment_handlers_are_no_longer_deferred",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-edge/src/grpc/workflow_service.rs::deployment_handlers_return_unimplemented_messages",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-runtime/src/runtime/activity.rs::activity_deployment_transition_lifecycle",
    },
];

const ACTIVITY_EXECUTION_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.ActivityExecutionManagement",
}];
const ACTIVITY_EXECUTION_RPCS: &[&str] = &[
    "WorkflowService.CountActivityExecutions",
    "WorkflowService.DeleteActivityExecution",
    "WorkflowService.DescribeActivityExecution",
    "WorkflowService.ListActivityExecutions",
    "WorkflowService.PauseActivity",
    "WorkflowService.PollActivityExecution",
    "WorkflowService.RequestCancelActivityExecution",
    "WorkflowService.ResetActivity",
    "WorkflowService.StartActivityExecution",
    "WorkflowService.TerminateActivityExecution",
    "WorkflowService.UnpauseActivity",
    "WorkflowService.UpdateActivityOptions",
];

const ACTIVITY_TASK_LIFECYCLE_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.ActivityTaskLifecycle",
}];
const ACTIVITY_TASK_LIFECYCLE_RPCS: &[&str] = &[
    "WorkflowService.PollActivityTaskQueue",
    "WorkflowService.RecordActivityTaskHeartbeat",
    "WorkflowService.RecordActivityTaskHeartbeatById",
    "WorkflowService.RespondActivityTaskCanceled",
    "WorkflowService.RespondActivityTaskCanceledById",
    "WorkflowService.RespondActivityTaskCompleted",
    "WorkflowService.RespondActivityTaskCompletedById",
    "WorkflowService.RespondActivityTaskFailed",
    "WorkflowService.RespondActivityTaskFailedById",
];

const BATCH_OPERATION_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.BatchOperations",
}];
const BATCH_OPERATION_RPCS: &[&str] = &[
    "WorkflowService.DescribeBatchOperation",
    "WorkflowService.ListBatchOperations",
    "WorkflowService.StartBatchOperation",
    "WorkflowService.StopBatchOperation",
];

const CLUSTER_INFO_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.ClusterInfo",
}];
const CLUSTER_INFO_RPCS: &[&str] = &[
    "WorkflowService.GetClusterInfo",
    "WorkflowService.GetSystemInfo",
];

const EAGER_WORKFLOW_START_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::ResponseField,
    identifier: "WorkflowService.StartWorkflowExecutionResponse.eager_workflow_task",
}];
const EMPTY_RPCS: &[&str] = &[];

const LEGACY_VISIBILITY_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.LegacyVisibility",
}];
const LEGACY_VISIBILITY_RPCS: &[&str] = &[
    "WorkflowService.ListArchivedWorkflowExecutions",
    "WorkflowService.ListClosedWorkflowExecutions",
    "WorkflowService.ListOpenWorkflowExecutions",
    "WorkflowService.ScanWorkflowExecutions",
];

const MULTI_OPERATION_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.MultiOperation",
}];
const MULTI_OPERATION_RPCS: &[&str] = &["WorkflowService.ExecuteMultiOperation"];
const MULTI_OPERATION_EVIDENCE: &[CompatibilityEvidence] = &[
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-edge/src/grpc/translate.rs::multi_operation_shape_gate_rejects_non_start_update_pairs",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-edge/src/grpc/translate.rs::multi_operation_start_conflict_keeps_already_exists_and_typed_detail",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-edge/src/grpc/translate.rs::multi_operation_response_serializes_ordered_start_update_pair",
    },
];

const NAMESPACE_MANAGEMENT_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.NamespaceManagement",
}];
const NAMESPACE_MANAGEMENT_RPCS: &[&str] = &[
    "OperatorService.DeleteNamespace",
    "WorkflowService.DeprecateNamespace",
    "WorkflowService.DescribeNamespace",
    "WorkflowService.ListNamespaces",
    "WorkflowService.RegisterNamespace",
    "WorkflowService.UpdateNamespace",
];

const NEXUS_ADMIN_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "OperatorService.NexusEndpointAdministration",
}];
const NEXUS_ADMIN_RPCS: &[&str] = &[
    "OperatorService.CreateNexusEndpoint",
    "OperatorService.DeleteNexusEndpoint",
    "OperatorService.GetNexusEndpoint",
    "OperatorService.ListNexusEndpoints",
    "OperatorService.UpdateNexusEndpoint",
];

const NEXUS_TASK_TRANSPORT_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.NexusTaskTransport",
}];
const NEXUS_TASK_TRANSPORT_RPCS: &[&str] = &[
    "WorkflowService.CountNexusOperationExecutions",
    "WorkflowService.DeleteNexusOperationExecution",
    "WorkflowService.DescribeNexusOperationExecution",
    "WorkflowService.ListNexusOperationExecutions",
    "WorkflowService.PollNexusOperationExecution",
    "WorkflowService.PollNexusTaskQueue",
    "WorkflowService.RequestCancelNexusOperationExecution",
    "WorkflowService.RespondNexusTaskCompleted",
    "WorkflowService.RespondNexusTaskFailed",
    "WorkflowService.StartNexusOperationExecution",
    "WorkflowService.TerminateNexusOperationExecution",
];

const REMOTE_CLUSTER_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "OperatorService.RemoteClusters",
}];
const REMOTE_CLUSTER_RPCS: &[&str] = &[
    "OperatorService.AddOrUpdateRemoteCluster",
    "OperatorService.ListClusters",
    "OperatorService.RemoveRemoteCluster",
];

const SCHEDULE_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.Schedules",
}];
const SCHEDULE_RPCS: &[&str] = &[
    "WorkflowService.CountSchedules",
    "WorkflowService.CreateSchedule",
    "WorkflowService.DeleteSchedule",
    "WorkflowService.DescribeSchedule",
    "WorkflowService.ListScheduleMatchingTimes",
    "WorkflowService.ListSchedules",
    "WorkflowService.PatchSchedule",
    "WorkflowService.UpdateSchedule",
];

const SEARCH_ATTRIBUTE_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "SearchAttributes",
}];
const SEARCH_ATTRIBUTE_RPCS: &[&str] = &[
    "OperatorService.AddSearchAttributes",
    "OperatorService.ListSearchAttributes",
    "OperatorService.RemoveSearchAttributes",
    "WorkflowService.GetSearchAttributes",
];

const TASK_QUEUE_MANAGEMENT_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.TaskQueueManagement",
}];
const TASK_QUEUE_MANAGEMENT_RPCS: &[&str] = &[
    "WorkflowService.DescribeTaskQueue",
    "WorkflowService.ListTaskQueuePartitions",
    "WorkflowService.UpdateTaskQueueConfig",
];

const VISIBILITY_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.Visibility",
}];
const VISIBILITY_RPCS: &[&str] = &[
    "WorkflowService.CountWorkflowExecutions",
    "WorkflowService.DescribeWorkflowExecution",
    "WorkflowService.ListWorkflowExecutions",
];

const WORKER_CONFIG_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkerConfig",
}];
const WORKER_CONFIG_RPCS: &[&str] = &[
    "WorkflowService.DescribeWorker",
    "WorkflowService.FetchWorkerConfig",
    "WorkflowService.ListWorkers",
    "WorkflowService.UpdateWorkerConfig",
];

const WORKER_DEPLOYMENT_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkerDeployments",
}];
const WORKER_DEPLOYMENT_RPCS: &[&str] = &[
    "WorkflowService.CreateWorkerDeployment",
    "WorkflowService.CreateWorkerDeploymentVersion",
    "WorkflowService.DeleteWorkerDeployment",
    "WorkflowService.DeleteWorkerDeploymentVersion",
    "WorkflowService.DescribeDeployment",
    "WorkflowService.DescribeWorkerDeployment",
    "WorkflowService.DescribeWorkerDeploymentVersion",
    "WorkflowService.GetCurrentDeployment",
    "WorkflowService.GetDeploymentReachability",
    "WorkflowService.ListDeployments",
    "WorkflowService.ListWorkerDeployments",
    "WorkflowService.SetCurrentDeployment",
    "WorkflowService.SetWorkerDeploymentCurrentVersion",
    "WorkflowService.SetWorkerDeploymentManager",
    "WorkflowService.SetWorkerDeploymentRampingVersion",
    "WorkflowService.UpdateWorkerDeploymentVersionComputeConfig",
    "WorkflowService.UpdateWorkerDeploymentVersionMetadata",
    "WorkflowService.ValidateWorkerDeploymentVersionComputeConfig",
];

const WORKER_HEARTBEAT_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkerHeartbeats",
}];
const WORKER_HEARTBEAT_RPCS: &[&str] = &[
    "WorkflowService.RecordWorkerHeartbeat",
    "WorkflowService.ShutdownWorker",
];

const WORKER_VERSIONING_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkerVersioningV2",
}];
const WORKER_VERSIONING_RPCS: &[&str] = &[
    "WorkflowService.GetWorkerBuildIdCompatibility",
    "WorkflowService.GetWorkerTaskReachability",
    "WorkflowService.GetWorkerVersioningRules",
    "WorkflowService.UpdateWorkerBuildIdCompatibility",
    "WorkflowService.UpdateWorkerVersioningRules",
];

const WORKFLOW_CANCEL_TERMINATE_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkflowCancelTerminate",
}];
const WORKFLOW_CANCEL_TERMINATE_RPCS: &[&str] = &[
    "WorkflowService.DeleteWorkflowExecution",
    "WorkflowService.RequestCancelWorkflowExecution",
    "WorkflowService.TerminateWorkflowExecution",
];

const WORKFLOW_HISTORY_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkflowHistory",
}];
const WORKFLOW_HISTORY_RPCS: &[&str] = &[
    "WorkflowService.GetWorkflowExecutionHistory",
    "WorkflowService.GetWorkflowExecutionHistoryReverse",
];

const WORKFLOW_PAUSE_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkflowPause",
}];
const WORKFLOW_PAUSE_RPCS: &[&str] = &[
    "WorkflowService.PauseWorkflowExecution",
    "WorkflowService.UnpauseWorkflowExecution",
];

const WORKFLOW_QUERY_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkflowQueries",
}];
const WORKFLOW_QUERY_RPCS: &[&str] = &[
    "WorkflowService.QueryWorkflow",
    "WorkflowService.RespondQueryTaskCompleted",
];

const WORKFLOW_RESET_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkflowReset",
}];
const WORKFLOW_RESET_RPCS: &[&str] = &["WorkflowService.ResetWorkflowExecution"];

const WORKFLOW_RULE_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkflowRules",
}];
const WORKFLOW_RULE_RPCS: &[&str] = &[
    "WorkflowService.CreateWorkflowRule",
    "WorkflowService.DeleteWorkflowRule",
    "WorkflowService.DescribeWorkflowRule",
    "WorkflowService.ListWorkflowRules",
    "WorkflowService.TriggerWorkflowRule",
];

const WORKFLOW_SIGNAL_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkflowSignal",
}];
const WORKFLOW_SIGNAL_RPCS: &[&str] = &["WorkflowService.SignalWorkflowExecution"];

const WORKFLOW_START_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkflowStart",
}];
const WORKFLOW_START_RPCS: &[&str] = &[
    "WorkflowService.SignalWithStartWorkflowExecution",
    "WorkflowService.StartWorkflowExecution",
    "WorkflowService.UpdateWorkflowExecutionOptions",
];

const WORKFLOW_TASK_LIFECYCLE_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkflowTaskLifecycle",
}];
const WORKFLOW_TASK_LIFECYCLE_RPCS: &[&str] = &[
    "WorkflowService.PollWorkflowTaskQueue",
    "WorkflowService.ResetStickyTaskQueue",
    "WorkflowService.RespondWorkflowTaskCompleted",
    "WorkflowService.RespondWorkflowTaskFailed",
];

const WORKFLOW_UPDATE_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkflowUpdates",
}];
const WORKFLOW_UPDATE_RPCS: &[&str] = &[
    "WorkflowService.PollWorkflowExecutionUpdate",
    "WorkflowService.UpdateWorkflowExecution",
];

pub const FEATURE_MATRIX: &[FeatureEntry] = &[
    FeatureEntry {
        id: "activity-executions",
        name: "Activity execution management",
        state: FeatureState::Experimental,
        surfaces: ACTIVITY_EXECUTION_SURFACES,
        capability_field: None,
        dynamic_config_key: Some("activity.enableStandalone"),
        rpcs: ACTIVITY_EXECUTION_RPCS,
        notes: "Standalone (CHASM) activity execution — the first CHASM component. A v1.31.0 feature gated per-namespace by `activity.enableStandalone` (default off); disabled it answers UNIMPLEMENTED (`chasm/lib/activity/frontend.go:36 @ v1.31.0`), enabled it is served, so default conformance is preserved. Tokeira's enable is a server-start config (`policy.compatibility.enable_standalone_activities`), server-uniform and not runtime-injectable: the functional harness's dynamic-config override path is unsupported, so SA functional tests run under the server's start-time setting.",
        evidence: &[CompatibilityEvidence {
            kind: crate::CompatibilityEvidenceKind::ManualReview,
            reference: "chasm-foundation spec; ground-truthed to chasm/lib/activity/{frontend.go,statemachine.go,config.go} @ v1.31.0",
        }],
    },
    FeatureEntry {
        id: "activity-task-lifecycle",
        name: "Activity task lifecycle",
        state: FeatureState::Partial,
        surfaces: ACTIVITY_TASK_LIFECYCLE_SURFACES,
        capability_field: Some("activity_failure_include_heartbeat"),
        dynamic_config_key: None,
        rpcs: ACTIVITY_TASK_LIFECYCLE_RPCS,
        notes: "Activity polling, heartbeats, and terminal responses exist, but strict Temporal conformance remains partial until SDK matrix coverage is complete.",
        evidence: &[],
    },
    FeatureEntry {
        id: "batch-operations",
        name: "Batch operations",
        state: FeatureState::Experimental,
        surfaces: BATCH_OPERATION_SURFACES,
        capability_field: None,
        dynamic_config_key: Some("compat.batch_operations"),
        rpcs: BATCH_OPERATION_RPCS,
        notes: "Batch APIs are visible but remain an experimental operator surface pending compatibility evidence.",
        evidence: &[],
    },
    FeatureEntry {
        id: "cluster-info",
        name: "Cluster and system metadata",
        state: FeatureState::Partial,
        surfaces: CLUSTER_INFO_SURFACES,
        capability_field: Some("internal_error_differentiation"),
        dynamic_config_key: None,
        rpcs: CLUSTER_INFO_RPCS,
        notes: "Cluster metadata and GetSystemInfo responses preserve the existing SDK-visible baseline while the matrix records conservative conformance state.",
        evidence: START_EVIDENCE,
    },
    FeatureEntry {
        id: "eager-workflow-start",
        name: "Eager workflow start",
        state: FeatureState::Experimental,
        surfaces: EAGER_WORKFLOW_START_SURFACES,
        capability_field: Some("eager_workflow_start"),
        dynamic_config_key: Some("compat.eager_workflow_start"),
        rpcs: EMPTY_RPCS,
        notes: "Eager start is a response-shape feature on StartWorkflowExecution rather than a standalone RPC.",
        evidence: &[],
    },
    FeatureEntry {
        id: "legacy-visibility",
        name: "Legacy visibility",
        state: FeatureState::Unsupported,
        surfaces: LEGACY_VISIBILITY_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: LEGACY_VISIBILITY_RPCS,
        notes: "Deprecated visibility scan/open/closed/archive APIs are not part of the current compatibility target.",
        evidence: &[],
    },
    FeatureEntry {
        id: "multi-operation",
        name: "Multi-operation execution",
        state: FeatureState::Implemented,
        surfaces: MULTI_OPERATION_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: MULTI_OPERATION_RPCS,
        notes: "ExecuteMultiOperation implements Update-with-Start ([Start, Update] only, per v1.31.0): atomic fresh-start admission via Command::StartAndUpdate, attach/dedup/already-completed paths, and the structured MultiOperationExecutionFailure error with the Aborted sibling.",
        evidence: MULTI_OPERATION_EVIDENCE,
    },
    FeatureEntry {
        id: "namespace-management",
        name: "Namespace management",
        state: FeatureState::Partial,
        surfaces: NAMESPACE_MANAGEMENT_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: NAMESPACE_MANAGEMENT_RPCS,
        notes: "Namespace APIs exist but remain partial because upstream namespace semantics are broader than the current implementation.",
        evidence: &[],
    },
    FeatureEntry {
        id: "nexus-admin",
        name: "Nexus endpoint administration",
        state: FeatureState::Unsupported,
        surfaces: NEXUS_ADMIN_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: NEXUS_ADMIN_RPCS,
        notes: "Operator Nexus endpoint administration is not implemented by the current edge.",
        evidence: &[],
    },
    FeatureEntry {
        id: "nexus-task-transport",
        name: "Nexus task transport",
        state: FeatureState::Experimental,
        surfaces: NEXUS_TASK_TRANSPORT_SURFACES,
        capability_field: Some("nexus"),
        dynamic_config_key: Some("compat.nexus_task_transport"),
        rpcs: NEXUS_TASK_TRANSPORT_RPCS,
        notes: "Nexus transport and operation execution surfaces are experimental until endpoint administration and SDK behavior are verified together.",
        evidence: &[],
    },
    FeatureEntry {
        id: "remote-cluster",
        name: "Remote cluster administration",
        state: FeatureState::Unsupported,
        surfaces: REMOTE_CLUSTER_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: REMOTE_CLUSTER_RPCS,
        notes: "Multi-cluster administration is outside the current deployment model.",
        evidence: &[],
    },
    FeatureEntry {
        id: "schedules",
        name: "Schedules",
        state: FeatureState::Experimental,
        surfaces: SCHEDULE_SURFACES,
        capability_field: Some("supports_schedules"),
        dynamic_config_key: Some("compat.schedules"),
        rpcs: SCHEDULE_RPCS,
        notes: "Schedule RPCs are visible but are not yet backed by conformance evidence.",
        evidence: &[],
    },
    FeatureEntry {
        id: "search-attributes",
        name: "Search attributes",
        state: FeatureState::Experimental,
        surfaces: SEARCH_ATTRIBUTE_SURFACES,
        capability_field: None,
        dynamic_config_key: Some("compat.search_attributes"),
        rpcs: SEARCH_ATTRIBUTE_RPCS,
        notes: "Search-attribute administration interacts with visibility projection and remains experimental.",
        evidence: &[],
    },
    FeatureEntry {
        id: "task-queue-management",
        name: "Task queue management",
        state: FeatureState::Partial,
        surfaces: TASK_QUEUE_MANAGEMENT_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: TASK_QUEUE_MANAGEMENT_RPCS,
        notes: "Task queue diagnostics and configuration APIs are present with partial compatibility coverage.",
        evidence: &[],
    },
    FeatureEntry {
        id: "visibility",
        name: "Workflow visibility",
        state: FeatureState::Partial,
        surfaces: VISIBILITY_SURFACES,
        capability_field: Some("count_group_by_execution_status"),
        dynamic_config_key: None,
        rpcs: VISIBILITY_RPCS,
        notes: "Visibility list/count/describe APIs are backed by projection, but strict Temporal query compatibility remains partial.",
        evidence: &[],
    },
    FeatureEntry {
        id: "worker-config",
        name: "Worker configuration and inventory",
        state: FeatureState::Unsupported,
        surfaces: WORKER_CONFIG_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKER_CONFIG_RPCS,
        notes: "Worker config and inventory APIs are distinct from the heartbeat ingestion path and are currently unsupported.",
        evidence: &[],
    },
    FeatureEntry {
        id: "worker-deployments",
        name: "Worker deployments",
        state: FeatureState::Implemented,
        surfaces: WORKER_DEPLOYMENT_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKER_DEPLOYMENT_RPCS,
        notes: "The v2 worker-deployment RPCs are implemented. Deprecated deployment companions are counted conformant because Temporal v1.31.0 returns UNIMPLEMENTED with the worker-deployments replacement message.",
        evidence: WORKER_DEPLOYMENT_EVIDENCE,
    },
    FeatureEntry {
        id: "worker-heartbeats",
        name: "Worker heartbeats",
        state: FeatureState::Partial,
        surfaces: WORKER_HEARTBEAT_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKER_HEARTBEAT_RPCS,
        notes: "RecordWorkerHeartbeat and ShutdownWorker are implemented for observability but not yet claimed as fully Temporal-conformant.",
        evidence: &[],
    },
    FeatureEntry {
        id: "worker-versioning-v2",
        name: "Worker versioning v2",
        state: FeatureState::Experimental,
        surfaces: WORKER_VERSIONING_SURFACES,
        capability_field: Some("build_id_based_versioning"),
        dynamic_config_key: Some("compat.worker_versioning_v2"),
        rpcs: WORKER_VERSIONING_RPCS,
        notes: "Worker versioning surfaces are retained as experimental until SDK behavior and deployment semantics are verified.",
        evidence: &[],
    },
    FeatureEntry {
        id: "workflow-cancel-terminate",
        name: "Workflow cancel and terminate",
        state: FeatureState::Partial,
        surfaces: WORKFLOW_CANCEL_TERMINATE_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKFLOW_CANCEL_TERMINATE_RPCS,
        notes: "Cancel, terminate, and delete requests exist but need broader failure-mode conformance evidence.",
        evidence: &[],
    },
    FeatureEntry {
        id: "workflow-history",
        name: "Workflow history reads",
        state: FeatureState::Partial,
        surfaces: WORKFLOW_HISTORY_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKFLOW_HISTORY_RPCS,
        notes: "History reads are core SDK surfaces, but the audit classifies them as partial until field-level completeness is proven.",
        evidence: START_EVIDENCE,
    },
    FeatureEntry {
        id: "workflow-pause",
        name: "Workflow pause",
        state: FeatureState::Unsupported,
        surfaces: WORKFLOW_PAUSE_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKFLOW_PAUSE_RPCS,
        notes: "Pause/unpause workflow APIs are upstream surfaces without current compatibility support.",
        evidence: &[],
    },
    FeatureEntry {
        id: "workflow-query",
        name: "Workflow query",
        state: FeatureState::Partial,
        surfaces: WORKFLOW_QUERY_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKFLOW_QUERY_RPCS,
        notes: "Query APIs are present but remain partial until ordering, consistency, and SDK behavior are covered.",
        evidence: &[],
    },
    FeatureEntry {
        id: "workflow-reset",
        name: "Workflow reset",
        state: FeatureState::Partial,
        surfaces: WORKFLOW_RESET_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKFLOW_RESET_RPCS,
        notes: "Reset has an edge surface but remains partial until full reset semantics are verified.",
        evidence: &[],
    },
    FeatureEntry {
        id: "workflow-rules",
        name: "Workflow rules",
        state: FeatureState::Unsupported,
        surfaces: WORKFLOW_RULE_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKFLOW_RULE_RPCS,
        notes: "Workflow rules are not implemented in the current runtime.",
        evidence: &[],
    },
    FeatureEntry {
        id: "workflow-signal",
        name: "Workflow signal",
        state: FeatureState::Partial,
        surfaces: WORKFLOW_SIGNAL_SURFACES,
        capability_field: Some("signal_and_query_header"),
        dynamic_config_key: None,
        rpcs: WORKFLOW_SIGNAL_RPCS,
        notes: "Signal delivery is part of the core workflow surface, but the audit classifies the broader compatibility state as partial.",
        evidence: &[],
    },
    FeatureEntry {
        id: "workflow-start",
        name: "Workflow start",
        state: FeatureState::Partial,
        surfaces: WORKFLOW_START_SURFACES,
        capability_field: Some("encoded_failure_attributes"),
        dynamic_config_key: None,
        rpcs: WORKFLOW_START_RPCS,
        notes: "Start and signal-with-start are accepted as core surfaces, but the server compatibility claim remains conservative.",
        evidence: START_EVIDENCE,
    },
    FeatureEntry {
        id: "workflow-task-lifecycle",
        name: "Workflow task lifecycle",
        state: FeatureState::Partial,
        surfaces: WORKFLOW_TASK_LIFECYCLE_SURFACES,
        capability_field: Some("sdk_metadata"),
        dynamic_config_key: None,
        rpcs: WORKFLOW_TASK_LIFECYCLE_RPCS,
        notes: "Workflow task polling and completion are core SDK paths. The current matrix shape records sdk_metadata as the primary capability; upsert_memo is preserved by the baseline until multi-capability matrix entries are introduced.",
        evidence: START_EVIDENCE,
    },
    FeatureEntry {
        id: "workflow-update",
        name: "Workflow updates",
        state: FeatureState::Experimental,
        surfaces: WORKFLOW_UPDATE_SURFACES,
        capability_field: Some("workflow_update"),
        dynamic_config_key: Some("compat.workflow_update"),
        rpcs: WORKFLOW_UPDATE_RPCS,
        notes: "Workflow updates are visible but remain experimental until protocol-level SDK conformance evidence is added.",
        evidence: &[],
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    const WORKFLOW_SERVICE_PROTO: &str =
        include_str!("../../../proto/upstream/temporal/api/workflowservice/v1/service.proto");
    const OPERATOR_SERVICE_PROTO: &str =
        include_str!("../../../proto/upstream/temporal/api/operatorservice/v1/service.proto");

    #[test]
    fn feature_matrix_is_sorted_by_id() {
        let ids = FEATURE_MATRIX
            .iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(ids, sorted);
    }

    #[test]
    fn every_rpc_is_owned_once() {
        let mut seen = BTreeSet::new();
        for entry in FEATURE_MATRIX {
            for rpc in entry.rpcs {
                assert!(seen.insert(*rpc), "duplicate RPC classification: {rpc}");
            }
        }
        assert!(seen.contains("WorkflowService.StartWorkflowExecution"));
        assert!(seen.contains("WorkflowService.RecordWorkerHeartbeat"));
        assert!(seen.contains("OperatorService.CreateNexusEndpoint"));
    }

    #[test]
    fn workflow_task_lifecycle_maps_all_current_capability_fields() {
        let entry = FEATURE_MATRIX
            .iter()
            .find(|entry| entry.id == "workflow-task-lifecycle")
            .expect("workflow task lifecycle feature");
        let fields = entry.capability_fields().collect::<BTreeSet<_>>();

        assert!(fields.contains("sdk_metadata"));
        assert!(fields.contains("upsert_memo"));
    }

    #[test]
    fn capability_mappings_match_current_get_system_info_capabilities() {
        let upstream = get_system_info_capability_fields()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mapped = FEATURE_MATRIX
            .iter()
            .flat_map(|entry| entry.capability_fields())
            .collect::<Vec<_>>();

        for field in &mapped {
            assert!(
                upstream.contains(field) || FUTURE_CAPABILITY_FIELDS.contains(field),
                "mapped capability field does not exist in current or planned proto: {field}"
            );
        }

        for field in upstream {
            let owners = mapped
                .iter()
                .filter(|mapped| **mapped == field)
                .copied()
                .collect::<Vec<_>>();
            if INTENTIONALLY_UNMAPPED_CAPABILITY_FIELDS.contains(&field) {
                assert!(
                    owners.is_empty(),
                    "intentionally unmapped capability has an owner: {field}"
                );
            } else {
                assert_eq!(
                    owners.len(),
                    1,
                    "capability field must have exactly one matrix owner: {field}"
                );
            }
        }
    }

    #[test]
    fn matrix_classifies_every_upstream_rpc() {
        let declared = FEATURE_MATRIX
            .iter()
            .flat_map(|entry| entry.rpcs.iter().copied())
            .collect::<BTreeSet<_>>();
        let upstream = upstream_rpcs("WorkflowService", WORKFLOW_SERVICE_PROTO)
            .into_iter()
            .chain(upstream_rpcs("OperatorService", OPERATOR_SERVICE_PROTO))
            .collect::<BTreeSet<_>>();

        let missing = upstream.difference(&declared).copied().collect::<Vec<_>>();
        let unknown = declared.difference(&upstream).copied().collect::<Vec<_>>();

        assert!(
            missing.is_empty(),
            "missing RPC classifications: {missing:?}"
        );
        assert!(
            unknown.is_empty(),
            "unknown RPC classifications: {unknown:?}"
        );
    }

    fn upstream_rpcs(service: &'static str, proto: &str) -> Vec<&'static str> {
        proto
            .lines()
            .filter_map(|line| line.trim_start().strip_prefix("rpc "))
            .filter_map(|line| line.split_once([' ', '(']))
            .map(|(name, _)| format!("{service}.{name}"))
            .map(|name| Box::leak(name.into_boxed_str()) as &'static str)
            .collect()
    }

    const INTENTIONALLY_UNMAPPED_CAPABILITY_FIELDS: &[&str] = &["server_scaled_deployments"];
    const FUTURE_CAPABILITY_FIELDS: &[&str] = &["workflow_update"];

    fn get_system_info_capability_fields() -> Vec<&'static str> {
        let response = WORKFLOW_SERVICE_REQUEST_RESPONSE_PROTO
            .split("message GetSystemInfoResponse")
            .nth(1)
            .expect("GetSystemInfoResponse message");
        let capabilities = response
            .split("message Capabilities")
            .nth(1)
            .expect("GetSystemInfoResponse.Capabilities message");

        capabilities
            .lines()
            .map(str::trim)
            .take_while(|line| *line != "}")
            .filter_map(|line| line.strip_prefix("bool "))
            .filter_map(|line| line.split_once(' '))
            .map(|(name, _)| Box::leak(name.to_string().into_boxed_str()) as &'static str)
            .collect()
    }

    const WORKFLOW_SERVICE_REQUEST_RESPONSE_PROTO: &str = include_str!(
        "../../../proto/upstream/temporal/api/workflowservice/v1/request_response.proto"
    );
}
