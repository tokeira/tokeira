use crate::{
    CompatibilityEvidence, CompatibilitySurface, CompatibilitySurfaceKind, ConformanceDisposition,
    DefaultPosture, EnablementKind, FeatureCatalogMetadata, FeatureEnablement, FeatureEntry,
    FeatureOrigin, FeatureState, PolicyMutability, PolicyScope, TemporalMaturity,
};

const NO_PREREQUISITES: &[&str] = &[];
const NO_POLICY_SCOPE: &[PolicyScope] = &[PolicyScope::NotApplicable];
const CLUSTER_SCOPE: &[PolicyScope] = &[PolicyScope::Cluster];
const NAMESPACE_SCOPE: &[PolicyScope] = &[PolicyScope::Namespace];
const TASK_QUEUE_SCOPE: &[PolicyScope] = &[PolicyScope::TaskQueue];
const SCOPED_WORKER_SCOPE: &[PolicyScope] = &[
    PolicyScope::Worker,
    PolicyScope::Namespace,
    PolicyScope::TaskQueue,
];

const TEMPORAL_GA_ENABLED: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TemporalV1_31,
    conformance: ConformanceDisposition::InSurface,
    temporal_maturity: TemporalMaturity::GeneralAvailability,
    temporal_default: DefaultPosture::Enabled,
    tokeira_default: DefaultPosture::Enabled,
    enablement: FeatureEnablement {
        kind: EnablementKind::None,
        reference: None,
    },
    scopes: NO_POLICY_SCOPE,
    mutability: PolicyMutability::Immutable,
    guidance: "Enabled with an empty production configuration.",
    prerequisites: NO_PREREQUISITES,
};

const TEMPORAL_GA_UNAVAILABLE: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TemporalV1_31,
    conformance: ConformanceDisposition::InSurface,
    temporal_maturity: TemporalMaturity::GeneralAvailability,
    temporal_default: DefaultPosture::Enabled,
    tokeira_default: DefaultPosture::Unavailable,
    enablement: FeatureEnablement {
        kind: EnablementKind::Unavailable,
        reference: None,
    },
    scopes: NO_POLICY_SCOPE,
    mutability: PolicyMutability::NotApplicable,
    guidance: "Unavailable in Tokeira; no production enablement mechanism exists.",
    prerequisites: NO_PREREQUISITES,
};

const TEMPORAL_EXPERIMENTAL_EXCLUDED: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TemporalV1_31,
    conformance: ConformanceDisposition::OutOfSurface,
    temporal_maturity: TemporalMaturity::Experimental,
    temporal_default: DefaultPosture::Conditional,
    tokeira_default: DefaultPosture::Unavailable,
    enablement: FeatureEnablement {
        kind: EnablementKind::Unavailable,
        reference: None,
    },
    scopes: NO_POLICY_SCOPE,
    mutability: PolicyMutability::NotApplicable,
    guidance: "Excluded because Temporal v1.31.0 labels this surface experimental.",
    prerequisites: NO_PREREQUISITES,
};

const TEMPORAL_DEPRECATED_EXCLUDED: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TemporalV1_31,
    conformance: ConformanceDisposition::OutOfSurface,
    temporal_maturity: TemporalMaturity::Deprecated,
    temporal_default: DefaultPosture::Conditional,
    tokeira_default: DefaultPosture::Unavailable,
    enablement: FeatureEnablement {
        kind: EnablementKind::Unavailable,
        reference: None,
    },
    scopes: NO_POLICY_SCOPE,
    mutability: PolicyMutability::NotApplicable,
    guidance: "Excluded because Temporal v1.31.0 marks this surface deprecated and provides a GA replacement.",
    prerequisites: NO_PREREQUISITES,
};

const TEMPORAL_EXPERIMENTAL_EXCLUDED_AVAILABLE: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TemporalV1_31,
    conformance: ConformanceDisposition::OutOfSurface,
    temporal_maturity: TemporalMaturity::Experimental,
    temporal_default: DefaultPosture::Conditional,
    tokeira_default: DefaultPosture::Enabled,
    enablement: FeatureEnablement {
        kind: EnablementKind::None,
        reference: None,
    },
    scopes: NO_POLICY_SCOPE,
    mutability: PolicyMutability::Immutable,
    guidance: "Implemented as an experimental Tokeira surface but excluded from the v1.31.0 compatibility claim.",
    prerequisites: NO_PREREQUISITES,
};

const STANDALONE_ACTIVITIES_CATALOG: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TemporalV1_31,
    conformance: ConformanceDisposition::InSurface,
    temporal_maturity: TemporalMaturity::PublicPreview,
    temporal_default: DefaultPosture::Disabled,
    tokeira_default: DefaultPosture::Disabled,
    enablement: FeatureEnablement {
        kind: EnablementKind::Toml,
        reference: Some("policy.compatibility.enable_standalone_activities = true"),
    },
    scopes: NAMESPACE_SCOPE,
    mutability: PolicyMutability::StartupStatic,
    guidance: "Set [policy.compatibility].enable_standalone_activities = true and restart tokeirad.",
    prerequisites: NO_PREREQUISITES,
};

const DEFAULT_REJECTION_CATALOG: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TemporalV1_31,
    conformance: ConformanceDisposition::InSurface,
    temporal_maturity: TemporalMaturity::Deprecated,
    temporal_default: DefaultPosture::Disabled,
    tokeira_default: DefaultPosture::Disabled,
    enablement: FeatureEnablement {
        kind: EnablementKind::Unavailable,
        reference: None,
    },
    scopes: CLUSTER_SCOPE,
    mutability: PolicyMutability::Immutable,
    guidance: "Only v1.31.0 stock-default rejection behavior is supported; the deprecated enabled path is excluded.",
    prerequisites: NO_PREREQUISITES,
};

const NEWER_WIRE_UNAVAILABLE: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::NewerVendoredWire,
    conformance: ConformanceDisposition::OutOfSurface,
    temporal_maturity: TemporalMaturity::Absent,
    temporal_default: DefaultPosture::NotApplicable,
    tokeira_default: DefaultPosture::Unavailable,
    enablement: FeatureEnablement {
        kind: EnablementKind::Unavailable,
        reference: None,
    },
    scopes: NO_POLICY_SCOPE,
    mutability: PolicyMutability::NotApplicable,
    guidance: "Absent from Temporal v1.31.0/API v1.62.8 and outside this compatibility claim.",
    prerequisites: NO_PREREQUISITES,
};

const WORKFLOW_RULES_CATALOG: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TemporalV1_31,
    conformance: ConformanceDisposition::InSurface,
    temporal_maturity: TemporalMaturity::GeneralAvailability,
    temporal_default: DefaultPosture::Disabled,
    tokeira_default: DefaultPosture::Unavailable,
    enablement: FeatureEnablement {
        kind: EnablementKind::ConformanceOnly,
        reference: Some("frontend.workflowRulesAPIsEnabled"),
    },
    scopes: NAMESPACE_SCOPE,
    mutability: PolicyMutability::ConformanceOnly,
    guidance: "The configured enabled path is currently available only to the conformance harness; production exposes no activation setting.",
    prerequisites: NO_PREREQUISITES,
};

const AUTHORIZATION_CATALOG: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TemporalV1_31,
    conformance: ConformanceDisposition::InSurface,
    temporal_maturity: TemporalMaturity::GeneralAvailability,
    temporal_default: DefaultPosture::Disabled,
    tokeira_default: DefaultPosture::Disabled,
    enablement: FeatureEnablement {
        kind: EnablementKind::Toml,
        reference: Some("policy.authorization"),
    },
    scopes: CLUSTER_SCOPE,
    mutability: PolicyMutability::StartupStatic,
    guidance: "Configure [policy.authorization] with at least one identity source and grant, then restart tokeirad.",
    prerequisites: &["Configured JWT issuer or AWS IAM verifier"],
};

const AWS_IAM_AUTHORIZATION_CATALOG: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TokeiraNative,
    conformance: ConformanceDisposition::NotApplicable,
    temporal_maturity: TemporalMaturity::NotApplicable,
    temporal_default: DefaultPosture::NotApplicable,
    tokeira_default: DefaultPosture::Disabled,
    enablement: FeatureEnablement {
        kind: EnablementKind::Toml,
        reference: Some("policy.authorization.aws_iam"),
    },
    scopes: CLUSTER_SCOPE,
    mutability: PolicyMutability::StartupStatic,
    guidance: "Configure [policy.authorization.aws_iam] grants together with authorization, then restart tokeirad.",
    prerequisites: &["AWS identity verification and configured authorization grants"],
};

const SCOPED_WORKER_AUTHORIZATION_CATALOG: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TokeiraNative,
    conformance: ConformanceDisposition::NotApplicable,
    temporal_maturity: TemporalMaturity::NotApplicable,
    temporal_default: DefaultPosture::NotApplicable,
    tokeira_default: DefaultPosture::Disabled,
    enablement: FeatureEnablement {
        kind: EnablementKind::Toml,
        reference: Some(
            "policy.authorization.jwt.issuers[].worker_scopes or policy.authorization.aws_iam.worker_scopes",
        ),
    },
    scopes: SCOPED_WORKER_SCOPE,
    mutability: PolicyMutability::StartupStatic,
    guidance: "Configure one exact subject or verified-ARN Worker scope and restart tokeirad; the standard SDK supplies that external bearer on every Worker RPC.",
    prerequisites: &[
        "Configured JWT issuer or AWS IAM verifier",
        "Exact Worker Deployment and Build ID",
    ],
};

const COMPATIBILITY_METADATA_CATALOG: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TokeiraNative,
    conformance: ConformanceDisposition::NotApplicable,
    temporal_maturity: TemporalMaturity::NotApplicable,
    temporal_default: DefaultPosture::NotApplicable,
    tokeira_default: DefaultPosture::Enabled,
    enablement: FeatureEnablement {
        kind: EnablementKind::None,
        reference: None,
    },
    scopes: CLUSTER_SCOPE,
    mutability: PolicyMutability::Immutable,
    guidance: "Enabled as a Tokeira metadata extension; clients may ignore the separate service.",
    prerequisites: NO_PREREQUISITES,
};

const TASK_QUEUE_MANAGEMENT_CATALOG: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TemporalV1_31,
    conformance: ConformanceDisposition::InSurface,
    temporal_maturity: TemporalMaturity::GeneralAvailability,
    temporal_default: DefaultPosture::Enabled,
    tokeira_default: DefaultPosture::Enabled,
    enablement: FeatureEnablement {
        kind: EnablementKind::PublicApi,
        reference: Some("WorkflowService.UpdateTaskQueueConfig"),
    },
    scopes: TASK_QUEUE_SCOPE,
    mutability: PolicyMutability::DurableLiveApi,
    guidance: "Use UpdateTaskQueueConfig for queue/per-key rates and fairness-weight overrides; priority delivery needs no activation.",
    prerequisites: NO_PREREQUISITES,
};

const USER_FAIRNESS_CATALOG: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TemporalV1_31,
    conformance: ConformanceDisposition::InSurface,
    temporal_maturity: TemporalMaturity::GeneralAvailability,
    temporal_default: DefaultPosture::Disabled,
    tokeira_default: DefaultPosture::Disabled,
    enablement: FeatureEnablement {
        kind: EnablementKind::Toml,
        reference: Some("policy.task_queues.enable_fairness = true"),
    },
    scopes: &[PolicyScope::Cluster, PolicyScope::TaskQueue],
    mutability: PolicyMutability::StartupStatic,
    guidance: "Set [policy.task_queues].enable_fairness = true and restart tokeirad; use UpdateTaskQueueConfig for per-key weights and rates.",
    prerequisites: &["Priority-aware delivery (enabled by default)"],
};

const WORKER_COMPUTE_CONTROLLER_CATALOG: FeatureCatalogMetadata = FeatureCatalogMetadata {
    origin: FeatureOrigin::TemporalV1_31,
    conformance: ConformanceDisposition::OutOfSurface,
    temporal_maturity: TemporalMaturity::Experimental,
    temporal_default: DefaultPosture::Disabled,
    tokeira_default: DefaultPosture::Disabled,
    enablement: FeatureEnablement {
        kind: EnablementKind::Toml,
        reference: Some("policy.worker_compute.enabled = true"),
    },
    scopes: CLUSTER_SCOPE,
    mutability: PolicyMutability::StartupStatic,
    guidance: "Set [policy.worker_compute].enabled = true and restart tokeirad; configured Nexus providers may create billable external capacity.",
    prerequisites: &["Configured remote Nexus endpoint implementing invoke-worker"],
};

const AUTHORIZATION_SURFACES: &[CompatibilitySurface] = &[
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::BehaviouralInvariant,
        identifier: "TemporalAuthorizationInterceptors",
    },
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::HistoryEvent,
        identifier: "HistoryEvent.principal",
    },
];

const AWS_IAM_AUTHORIZATION_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::BehaviouralInvariant,
    identifier: "TokeiraAwsIamBearerAuthorization",
}];

const SCOPED_WORKER_AUTHORIZATION_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::BehaviouralInvariant,
    identifier: "TokeiraScopedWorkerAuthorization",
}];

const COMPATIBILITY_METADATA_SURFACES: &[CompatibilitySurface] = &[
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::Rpc,
        identifier: "CompatibilityService.GetCompatibility",
    },
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::Rpc,
        identifier: "CompatibilityService.ListCompatibilitySurfaces",
    },
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::Rpc,
        identifier: "CompatibilityService.GetFeature",
    },
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::Rpc,
        identifier: "CompatibilityService.GetSdkCompatibility",
    },
];

const USER_FAIRNESS_SURFACES: &[CompatibilitySurface] = &[
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::BehaviouralInvariant,
        identifier: "TaskQueueUserFairnessHandout",
    },
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::RequestField,
        identifier: "Priority.fairness_key",
    },
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::RequestField,
        identifier: "Priority.fairness_weight",
    },
];

const START_EVIDENCE: &[CompatibilityEvidence] = &[CompatibilityEvidence {
    kind: crate::CompatibilityEvidenceKind::Test,
    reference: "apps/tokeirad/tests/grpc_roundtrip.rs",
}];

const MATRIX_AUDIT_EVIDENCE: &[CompatibilityEvidence] = &[CompatibilityEvidence {
    kind: crate::CompatibilityEvidenceKind::ManualReview,
    reference: "docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md",
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

const WORKER_COMPUTE_CONTROLLER_EVIDENCE: &[CompatibilityEvidence] = &[CompatibilityEvidence {
    kind: crate::CompatibilityEvidenceKind::Test,
    reference: ".kiro/specs/worker-compute-controller; provider-neutral lifecycle Properties 1–17",
}];

const WORKER_HEARTBEAT_EVIDENCE: &[CompatibilityEvidence] = &[
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-edge/src/grpc/workflow_service.rs::worker_inventory_round_trips_complete_heartbeats",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-edge/src/worker_inventory.rs::pagination_is_ordered_and_duplicate_free",
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
    "WorkflowService.PollActivityExecution",
    "WorkflowService.RequestCancelActivityExecution",
    "WorkflowService.StartActivityExecution",
    "WorkflowService.TerminateActivityExecution",
];

const ACTIVITY_MANAGEMENT_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkflowScopedActivityManagement",
}];
const ACTIVITY_MANAGEMENT_RPCS: &[&str] = &[
    "WorkflowService.PauseActivity",
    "WorkflowService.ResetActivity",
    "WorkflowService.UnpauseActivity",
    "WorkflowService.UpdateActivityOptions",
];
const ACTIVITY_MANAGEMENT_EVIDENCE: &[CompatibilityEvidence] = &[
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "Temporal functional corpus TestActivityApiResetClientTestSuite @ v1.31.0: 6 pass / 0 fail (repeated fresh-process runs)",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "Temporal functional corpus TestActivityAPIUpdateClientTestSuite @ v1.31.0: 5 pass / 0 fail (2 consecutive fresh-process runs)",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "Temporal functional corpus TestActivityApiBatchUpdateOptionsClientTestSuite @ v1.31.0: 3 pass / 0 fail (2 consecutive fresh-process runs)",
    },
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
const EAGER_WORKFLOW_START_EVIDENCE: &[CompatibilityEvidence] = &[
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "Temporal functional corpus TestEagerWorkflowTestSuite @ v1.31.0: 5 pass / 0 fail / 1 classified skip (3 consecutive runs)",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-edge/src/workflow_service.rs::eager_start_does_not_require_registered_poller",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-storage/src/dsql/codec.rs::legacy_workflow_started_fixture_decodes_and_v2_round_trips",
    },
];
const HTTP_JSON_API_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::BehaviouralInvariant,
    identifier: "WorkflowService+OperatorService.google.api.http",
}];
const HTTP_JSON_API_EVIDENCE: &[CompatibilityEvidence] = &[
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "Temporal functional corpus TestHttpApiTestSuite @ v1.31.0: 11 pass / 0 fail / 0 skip (2 consecutive fresh-process runs)",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-edge/src/http_api: Properties 1-7 and 9-11; apps/tokeirad/src/http_api_transport.rs: Property 8 and layer integration tests",
    },
];
const WORKFLOW_DELETION_EVIDENCE: &[CompatibilityEvidence] = &[
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "Temporal functional corpus TestWorkflowDeleteExecutionSuite @ v1.31.0: 3 pass / 0 fail (2 consecutive runs)",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-storage/src/memory.rs::authoritative workflow deletion Property 5",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-projection/src/visibility_sink.rs::visibility tombstone monotonicity Property 11",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-runtime/src/runtime/lifecycle.rs::running_workflow_deletion_terminates_then_purges",
    },
];
const EMPTY_RPCS: &[&str] = &[];

const LEGACY_VISIBILITY_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.LegacyVisibility",
}];
const LEGACY_VISIBILITY_RPCS: &[&str] = &["WorkflowService.ScanWorkflowExecutions"];

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
    "WorkflowService.PollNexusTaskQueue",
    "WorkflowService.RespondNexusTaskCompleted",
    "WorkflowService.RespondNexusTaskFailed",
];

const NEXUS_OPERATION_EXECUTION_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.NexusOperationExecution",
}];
/// RPCs present in vendored API v1.62.11 but absent from v1.31.0's API v1.62.8.
pub const NEWER_VENDORED_WIRE_RPCS: &[&str] = &[
    "WorkflowService.CountNexusOperationExecutions",
    "WorkflowService.DeleteNexusOperationExecution",
    "WorkflowService.DescribeNexusOperationExecution",
    "WorkflowService.ListNexusOperationExecutions",
    "WorkflowService.PollNexusOperationExecution",
    "WorkflowService.RequestCancelNexusOperationExecution",
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

const REPORTED_PROBLEMS_SURFACES: &[CompatibilitySurface] = &[
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::CapabilityFlag,
        identifier: "NamespaceInfo.capabilities.reported_problems_search_attribute",
    },
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::ResponseField,
        identifier: "WorkflowExecutionInfo.search_attributes.TemporalReportedProblems",
    },
];
const REPORTED_PROBLEMS_EVIDENCE: &[CompatibilityEvidence] = &[
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-runtime/src/runtime/mod.rs::reported_problem_appears_at_default_threshold_and_carries_latest_cause",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "apps/tokeirad/src/lib.rs::reported_problem_search_attribute_has_exact_v131_keyword_list",
    },
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

const TASK_QUEUE_MANAGEMENT_SURFACES: &[CompatibilitySurface] = &[
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::Rpc,
        identifier: "WorkflowService.TaskQueueManagement",
    },
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::RequestField,
        identifier: "StartWorkflowExecutionRequest.priority",
    },
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::CommandAttribute,
        identifier: "ScheduleActivityTaskCommandAttributes.priority",
    },
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::CommandAttribute,
        identifier: "StartChildWorkflowExecutionCommandAttributes.priority",
    },
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::HistoryEvent,
        identifier: "WorkflowExecutionStartedEventAttributes.priority",
    },
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::ResponseField,
        identifier: "DescribeTaskQueueResponse.stats_by_priority_key",
    },
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::BehaviouralInvariant,
        identifier: "TaskQueuePriorityAndUserFairnessHandout",
    },
];
const TASK_QUEUE_MANAGEMENT_RPCS: &[&str] = &[
    "WorkflowService.DescribeTaskQueue",
    "WorkflowService.ListTaskQueuePartitions",
    "WorkflowService.UpdateTaskQueueConfig",
];
const TASK_QUEUE_MANAGEMENT_EVIDENCE: &[CompatibilityEvidence] = &[
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-edge/tests/grpc_new_endpoints.rs::priority_orders_workflow_polls_and_projects_real_band_stats_via_grpc",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "crates/tokeira-runtime/src/task_ordering.rs property tests",
    },
    CompatibilityEvidence {
        kind: crate::CompatibilityEvidenceKind::Test,
        reference: "Temporal functional corpus TestPrioritySuite, TestFairnessSuite, and TestFairnessAutoEnableSuite @ v1.31.0",
    },
];

const VISIBILITY_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.Visibility",
}];
const VISIBILITY_RPCS: &[&str] = &[
    "WorkflowService.CountWorkflowExecutions",
    "WorkflowService.DescribeWorkflowExecution",
    "WorkflowService.ListArchivedWorkflowExecutions",
    "WorkflowService.ListClosedWorkflowExecutions",
    "WorkflowService.ListOpenWorkflowExecutions",
    "WorkflowService.ListWorkflowExecutions",
];

const WORKER_CONFIG_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkerConfig",
}];
const WORKER_CONFIG_RPCS: &[&str] = &[
    "WorkflowService.FetchWorkerConfig",
    "WorkflowService.UpdateWorkerConfig",
];

const WORKER_COMPUTE_CONTROLLER_SURFACES: &[CompatibilitySurface] = &[
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::BehaviouralInvariant,
        identifier: "WorkerComputeController.RemoteNexusNoSync",
    },
    CompatibilitySurface {
        kind: CompatibilitySurfaceKind::BehaviouralInvariant,
        identifier: "ComputeProvider.InvokeWorker",
    },
];

const WORKER_DEPLOYMENT_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkerDeployments",
}];
const WORKER_DEPLOYMENT_PRE_RELEASE_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkerDeploymentsPreRelease",
}];
const DEPLOYMENT_V0_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.DeploymentV0",
}];
const WORKER_DEPLOYMENT_RPCS: &[&str] = &[
    "WorkflowService.DeleteWorkerDeployment",
    "WorkflowService.DeleteWorkerDeploymentVersion",
    "WorkflowService.DescribeWorkerDeployment",
    "WorkflowService.DescribeWorkerDeploymentVersion",
    "WorkflowService.ListWorkerDeployments",
    "WorkflowService.SetWorkerDeploymentCurrentVersion",
    "WorkflowService.SetWorkerDeploymentManager",
    "WorkflowService.SetWorkerDeploymentRampingVersion",
    "WorkflowService.UpdateWorkerDeploymentVersionMetadata",
];
const WORKER_DEPLOYMENT_PRE_RELEASE_RPCS: &[&str] = &[
    "WorkflowService.CreateWorkerDeployment",
    "WorkflowService.CreateWorkerDeploymentVersion",
    "WorkflowService.UpdateWorkerDeploymentVersionComputeConfig",
    "WorkflowService.ValidateWorkerDeploymentVersionComputeConfig",
];
const DEPLOYMENT_V0_RPCS: &[&str] = &[
    "WorkflowService.DescribeDeployment",
    "WorkflowService.GetCurrentDeployment",
    "WorkflowService.GetDeploymentReachability",
    "WorkflowService.ListDeployments",
    "WorkflowService.SetCurrentDeployment",
];

const WORKER_HEARTBEAT_SURFACES: &[CompatibilitySurface] = &[CompatibilitySurface {
    kind: CompatibilitySurfaceKind::Rpc,
    identifier: "WorkflowService.WorkerHeartbeats",
}];
const WORKER_HEARTBEAT_RPCS: &[&str] = &[
    "WorkflowService.DescribeWorker",
    "WorkflowService.ListWorkers",
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
        catalog: STANDALONE_ACTIVITIES_CATALOG,
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
        catalog: TEMPORAL_GA_ENABLED,
        id: "activity-management",
        name: "Workflow-scoped activity management",
        state: FeatureState::Implemented,
        surfaces: ACTIVITY_MANAGEMENT_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: ACTIVITY_MANAGEMENT_RPCS,
        notes: "UpdateActivityOptions, PauseActivity, UnpauseActivity, and ResetActivity implement the served v1.31.0 workflow-scoped activity lifecycle, including id/type/all targeting, retry-policy and restore-original option updates, reset/pause heartbeat flags, and paused-retry parking. Their API comments announce a future deprecation, but v1.31.0 has no replacement RPCs and keeps them in surface.",
        evidence: ACTIVITY_MANAGEMENT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "activity-task-lifecycle",
        name: "Activity task lifecycle",
        state: FeatureState::Partial,
        surfaces: ACTIVITY_TASK_LIFECYCLE_SURFACES,
        capability_field: Some("activity_failure_include_heartbeat"),
        dynamic_config_key: None,
        rpcs: ACTIVITY_TASK_LIFECYCLE_RPCS,
        notes: "Activity polling, heartbeats, and terminal responses exist, but strict Temporal conformance remains partial until SDK matrix coverage is complete.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: AUTHORIZATION_CATALOG,
        id: "authorization",
        name: "Authentication, authorization, and principal attribution",
        state: FeatureState::Implemented,
        surfaces: AUTHORIZATION_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: EMPTY_RPCS,
        notes: "Presence-enabled JWT authentication, authorization, namespace/task-queue access classification, and durable principal attribution match the configured v1.31.0 behavior.",
        evidence: &[CompatibilityEvidence {
            kind: crate::CompatibilityEvidenceKind::Test,
            reference: "Temporal functional corpus TestAuthorizationTestSuite @ v1.31.0: Tier 7.36",
        }],
    },
    FeatureEntry {
        catalog: AWS_IAM_AUTHORIZATION_CATALOG,
        id: "aws-iam-bearer-authorization",
        name: "AWS IAM bearer authorization",
        state: FeatureState::Implemented,
        surfaces: AWS_IAM_AUTHORIZATION_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: EMPTY_RPCS,
        notes: "Tokeira-native AWS IAM bearer verification composes with the same typed grant and authorization model and is outside the Temporal compatibility claim.",
        evidence: &[CompatibilityEvidence {
            kind: crate::CompatibilityEvidenceKind::ManualReview,
            reference: ".kiro/specs/authorization-foundation",
        }],
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "batch-operations",
        name: "Batch operations",
        state: FeatureState::Experimental,
        surfaces: BATCH_OPERATION_SURFACES,
        capability_field: None,
        dynamic_config_key: Some("compat.batch_operations"),
        rpcs: BATCH_OPERATION_RPCS,
        notes: "Batch APIs are visible but remain an experimental operator surface pending compatibility evidence.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
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
        catalog: COMPATIBILITY_METADATA_CATALOG,
        id: "compatibility-metadata",
        name: "Tokeira compatibility metadata service",
        state: FeatureState::Implemented,
        surfaces: COMPATIBILITY_METADATA_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: EMPTY_RPCS,
        notes: "Tokeira's separate compatibility service publishes build pins, feature ownership, SDK evidence, and stable digests without altering Temporal services.",
        evidence: &[CompatibilityEvidence {
            kind: crate::CompatibilityEvidenceKind::Test,
            reference: "crates/tokeira-compatibility-service/src/lib.rs::compatibility_response_contains_static_matrices",
        }],
    },
    FeatureEntry {
        catalog: TEMPORAL_DEPRECATED_EXCLUDED,
        id: "deployment-v0",
        name: "Deployment v0 (deprecated)",
        state: FeatureState::Unsupported,
        surfaces: DEPLOYMENT_V0_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: DEPLOYMENT_V0_RPCS,
        notes: "Temporal v1.31.0 deprecates these five deployment-v0 RPCs in favor of GA Worker Deployments; Tokeira does not expose their enabled behavior.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "eager-workflow-start",
        name: "Eager workflow start",
        state: FeatureState::Implemented,
        surfaces: EAGER_WORKFLOW_START_SURFACES,
        capability_field: Some("eager_workflow_start"),
        dynamic_config_key: None,
        rpcs: EMPTY_RPCS,
        notes: "StartWorkflowExecution atomically commits and returns the first WFT when eager execution is requested and no first-WFT backoff applies. Fresh and immediate request-id retry responses derive from authoritative started-task state; the v1.31.0 enabled default is pinned as a constant rather than an operator knob.",
        evidence: EAGER_WORKFLOW_START_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "http-json-api",
        name: "Temporal HTTP/JSON API gateway",
        state: FeatureState::Implemented,
        surfaces: HTTP_JSON_API_SURFACES,
        capability_field: None,
        dynamic_config_key: Some("frontend.httpAllowedHosts"),
        rpcs: EMPTY_RPCS,
        notes: "WorkflowService and OperatorService google.api.http annotations are discovered from the pinned descriptor set and transcoded on the existing listener into the ordinary Tonic service stack. Host/header policy, protobuf JSON, Temporal payload shorthand, v2/v3 OpenAPI documents, gRPC status translation, and admitted-request metrics match v1.31.0 without adding workflow semantics or another internal service.",
        evidence: HTTP_JSON_API_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_DEPRECATED_EXCLUDED,
        id: "legacy-visibility",
        name: "Legacy visibility",
        state: FeatureState::Unsupported,
        surfaces: LEGACY_VISIBILITY_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: LEGACY_VISIBILITY_RPCS,
        notes: "The deprecated ScanWorkflowExecutions RPC is excluded; the still-served ListOpen, ListClosed, and ListArchived RPCs belong to the ordinary visibility feature.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
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
        catalog: TEMPORAL_GA_ENABLED,
        id: "namespace-management",
        name: "Namespace management",
        state: FeatureState::Partial,
        surfaces: NAMESPACE_MANAGEMENT_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: NAMESPACE_MANAGEMENT_RPCS,
        notes: "Namespace APIs exist but remain partial because upstream namespace semantics are broader than the current implementation.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "nexus-admin",
        name: "Nexus endpoint administration",
        state: FeatureState::Implemented,
        surfaces: NEXUS_ADMIN_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: NEXUS_ADMIN_RPCS,
        notes: "Nexus endpoint CRUD, optimistic update, pagination, validation, and namespace-safe callback routing implement the v1.31.0 GA operator surface.",
        evidence: &[CompatibilityEvidence {
            kind: crate::CompatibilityEvidenceKind::Test,
            reference: "Temporal functional corpus Nexus endpoint admin coverage: Tier 7.35",
        }],
    },
    FeatureEntry {
        catalog: NEWER_WIRE_UNAVAILABLE,
        id: "nexus-operation-executions",
        name: "Nexus operation executions",
        state: FeatureState::Unsupported,
        surfaces: NEXUS_OPERATION_EXECUTION_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: NEWER_VENDORED_WIRE_RPCS,
        notes: "These eight RPCs exist only in vendored API v1.62.11 and are absent from the v1.31.0 server's API v1.62.8.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "nexus-task-transport",
        name: "Nexus task transport",
        state: FeatureState::Implemented,
        surfaces: NEXUS_TASK_TRANSPORT_SURFACES,
        capability_field: Some("nexus"),
        dynamic_config_key: Some("compat.nexus_task_transport"),
        rpcs: NEXUS_TASK_TRANSPORT_RPCS,
        notes: "The three v1.31.0 Nexus worker transport RPCs and workflow operation lifecycle are implemented; the eight newer operation-execution RPCs are classified separately.",
        evidence: &[CompatibilityEvidence {
            kind: crate::CompatibilityEvidenceKind::Test,
            reference: "Temporal functional corpus TestNexusWorkflowTestSuite and TestNexusApiTestSuite @ v1.31.0: Tiers 7.37-7.38",
        }],
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_UNAVAILABLE,
        id: "remote-cluster",
        name: "Remote cluster administration",
        state: FeatureState::Unsupported,
        surfaces: REMOTE_CLUSTER_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: REMOTE_CLUSTER_RPCS,
        notes: "Multi-cluster administration is outside the current deployment model.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "reported-problems-search-attribute",
        name: "Workflow task reported problems",
        state: FeatureState::Partial,
        surfaces: REPORTED_PROBLEMS_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: EMPTY_RPCS,
        notes: "Describe derives the v1.31.0 TemporalReportedProblems KeywordList from kernel-state consecutive-problem accounting (failures and start-to-close timeouts, sticky-suppressed and cleared on WFT success, per failWorkflowTask @ v1.31.0) at the pinned default threshold of five; the last non-transient problem supplies the Failed or TimedOut category pair. The accumulator is durable with the run's hot state. Visibility-index projection of the attribute (v1.31.0 upserts it for ListWorkflowExecutions) remains open — the attribute currently surfaces on Describe only.",
        evidence: REPORTED_PROBLEMS_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "schedules",
        name: "Schedules",
        state: FeatureState::Partial,
        surfaces: SCHEDULE_SURFACES,
        capability_field: Some("supports_schedules"),
        dynamic_config_key: Some("compat.schedules"),
        rpcs: SCHEDULE_RPCS,
        notes: "The public v1.31.0 schedule behavior is conformance-tested; the native schedule store remains process-local, so restart durability is still open.",
        evidence: &[CompatibilityEvidence {
            kind: crate::CompatibilityEvidenceKind::Test,
            reference: "Temporal functional corpus TestScheduleV1 @ v1.31.0: Tier 5.30",
        }],
    },
    FeatureEntry {
        catalog: SCOPED_WORKER_AUTHORIZATION_CATALOG,
        id: "scoped-worker-authorization",
        name: "Scoped Worker authorization",
        state: FeatureState::Implemented,
        surfaces: SCOPED_WORKER_AUTHORIZATION_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: EMPTY_RPCS,
        notes: "Tokeira-native, presence-activated credential attenuation limits a standard SDK Worker to one exact namespace, an allowlist of normal task queues, one exact Deployment Version, and a fixed Worker RPC matrix. Server-authored durable token provenance prevents a scoped credential from completing work it was not authorized to receive.",
        evidence: &[CompatibilityEvidence {
            kind: crate::CompatibilityEvidenceKind::Test,
            reference: ".kiro/specs/scoped-worker-authorization",
        }],
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "search-attributes",
        name: "Search attributes",
        state: FeatureState::Partial,
        surfaces: SEARCH_ATTRIBUTE_SURFACES,
        capability_field: None,
        dynamic_config_key: Some("compat.search_attributes"),
        rpcs: SEARCH_ATTRIBUTE_RPCS,
        notes: "Search-attribute administration interacts with visibility projection and remains experimental.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TASK_QUEUE_MANAGEMENT_CATALOG,
        id: "task-queue-management",
        name: "Task queue management, priority, and fairness",
        state: FeatureState::Implemented,
        surfaces: TASK_QUEUE_MANAGEMENT_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: TASK_QUEUE_MANAGEMENT_RPCS,
        notes: "Priority-aware workflow, activity, child, sticky, and durable-backlog handout follows the v1.31.0 stock defaults. Optional User Fairness, auto-enable, queue/per-key rate shaping, atomic kind-isolated task-queue config updates, and real per-priority statistics are implemented in Tokeira's delivery runtime without matching/history service objects. Public task-queue policy commits through a dedicated CAS repository, survives process replacement, and is hydrated before traffic without becoming workflow history or kernel state.",
        evidence: TASK_QUEUE_MANAGEMENT_EVIDENCE,
    },
    FeatureEntry {
        catalog: USER_FAIRNESS_CATALOG,
        id: "user-fairness",
        name: "Task queue User Fairness",
        state: FeatureState::Implemented,
        surfaces: USER_FAIRNESS_SURFACES,
        capability_field: None,
        dynamic_config_key: Some("matching.enableFairness"),
        rpcs: EMPTY_RPCS,
        notes: "Weighted within-priority handout is disabled by default, preserves metadata while disabled, excludes sticky queues, and composes queue overrides over task-carried weights.",
        evidence: TASK_QUEUE_MANAGEMENT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "visibility",
        name: "Workflow visibility",
        state: FeatureState::Partial,
        surfaces: VISIBILITY_SURFACES,
        capability_field: Some("count_group_by_execution_status"),
        dynamic_config_key: None,
        rpcs: VISIBILITY_RPCS,
        notes: "Visibility list/count/describe APIs are backed by projection, but strict Temporal query compatibility remains partial.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: WORKER_COMPUTE_CONTROLLER_CATALOG,
        id: "worker-compute-controller",
        name: "Worker Compute Controller",
        state: FeatureState::Experimental,
        surfaces: WORKER_COMPUTE_CONTROLLER_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: EMPTY_RPCS,
        notes: "The opt-in controller activates only remote Nexus providers with the no-sync scaler and invoke-worker operation. Rate-based scaling, direct cloud providers, worker-set desired-size updates, scale-down, and proof of worker poll readiness are unavailable. Provider delivery is at least once; providers must deduplicate Action_Request_ID.",
        evidence: WORKER_COMPUTE_CONTROLLER_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_UNAVAILABLE,
        id: "worker-config",
        name: "Worker configuration",
        state: FeatureState::Unsupported,
        surfaces: WORKER_CONFIG_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKER_CONFIG_RPCS,
        notes: "FetchWorkerConfig and UpdateWorkerConfig remain unsupported; live worker inventory belongs to worker-heartbeats.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "worker-deployments",
        name: "Worker deployments",
        state: FeatureState::Implemented,
        surfaces: WORKER_DEPLOYMENT_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKER_DEPLOYMENT_RPCS,
        notes: "The nine GA Worker Deployment RPCs are implemented, including version membership, routing, drainage, limits, metadata, and manager/current/ramping transitions. Deprecated and pre-release companions are cataloged separately.",
        evidence: WORKER_DEPLOYMENT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_EXPERIMENTAL_EXCLUDED_AVAILABLE,
        id: "worker-deployments-pre-release",
        name: "Worker deployments pre-release additions",
        state: FeatureState::Experimental,
        surfaces: WORKER_DEPLOYMENT_PRE_RELEASE_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKER_DEPLOYMENT_PRE_RELEASE_RPCS,
        notes: "Temporal v1.31.0 labels these four Worker Deployment additions pre-release. Tokeira implements them through the same registry but excludes them from the compatibility claim.",
        evidence: WORKER_DEPLOYMENT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "worker-heartbeats",
        name: "Worker heartbeats and live inventory",
        state: FeatureState::Implemented,
        surfaces: WORKER_HEARTBEAT_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKER_HEARTBEAT_RPCS,
        notes: "RecordWorkerHeartbeat, shutdown removal, Nexus-piggyback admission, and lossless DescribeWorker/ListWorkers inventory reads match Temporal v1.31.0's volatile registry behavior.",
        evidence: WORKER_HEARTBEAT_EVIDENCE,
    },
    FeatureEntry {
        catalog: DEFAULT_REJECTION_CATALOG,
        id: "worker-versioning-v1-v2",
        name: "Worker versioning v1/v2 (deprecated)",
        state: FeatureState::Implemented,
        surfaces: WORKER_VERSIONING_SURFACES,
        capability_field: Some("build_id_based_versioning"),
        dynamic_config_key: None,
        rpcs: WORKER_VERSIONING_RPCS,
        notes: "Conformant as stock-default rejections: a default-config Temporal v1.31.0 server refuses all five deprecated RPCs with PERMISSION_DENIED (the versioning gates default off), and tokeira reproduces those exact errors. The enabled-path semantics are out of surface; the owning decision record is .kiro/specs/worker-deployments/reference/v1-v2-conformance-decision.md.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "workflow-cancel-terminate",
        name: "Workflow cancel and terminate",
        state: FeatureState::Partial,
        surfaces: WORKFLOW_CANCEL_TERMINATE_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKFLOW_CANCEL_TERMINATE_RPCS,
        notes: "DeleteWorkflowExecution is proven against the v1.31.0 functional corpus with authoritative state/history purge and monotonic visibility tombstones. The group remains Partial until cancel and terminate independently have broader failure-mode conformance evidence.",
        evidence: WORKFLOW_DELETION_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
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
        catalog: TEMPORAL_EXPERIMENTAL_EXCLUDED,
        id: "workflow-pause",
        name: "Workflow pause",
        state: FeatureState::Unsupported,
        surfaces: WORKFLOW_PAUSE_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKFLOW_PAUSE_RPCS,
        notes: "Pause/unpause workflow APIs are upstream surfaces without current compatibility support.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "workflow-query",
        name: "Workflow query",
        state: FeatureState::Partial,
        surfaces: WORKFLOW_QUERY_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKFLOW_QUERY_RPCS,
        notes: "Query APIs are present but remain partial until ordering, consistency, and SDK behavior are covered.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "workflow-reset",
        name: "Workflow reset",
        state: FeatureState::Partial,
        surfaces: WORKFLOW_RESET_SURFACES,
        capability_field: None,
        dynamic_config_key: None,
        rpcs: WORKFLOW_RESET_RPCS,
        notes: "Reset has an edge surface but remains partial until full reset semantics are verified.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: WORKFLOW_RULES_CATALOG,
        id: "workflow-rules",
        name: "Workflow rules",
        state: FeatureState::Partial,
        surfaces: WORKFLOW_RULE_SURFACES,
        capability_field: None,
        dynamic_config_key: Some("frontend.workflowRulesAPIsEnabled"),
        rpcs: WORKFLOW_RULE_RPCS,
        notes: "The default-off v1.31.0 gate, CRUD surface, target-conformant TriggerWorkflowRule rejection, ActivityType equality predicate, and ActivityPause evaluation at initial and retry dispatch are implemented. The registry is process-local and automatic evaluation does not yet implement the complete visibility/activity predicate language, so restart durability and the broader predicate surface remain open.",
        evidence: &[CompatibilityEvidence {
            kind: crate::CompatibilityEvidenceKind::Test,
            reference: "Temporal functional corpus TestActivityApiRulesClientTestSuite @ v1.31.0: 5 pass / 0 fail (2 consecutive fresh-process runs)",
        }],
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
        id: "workflow-signal",
        name: "Workflow signal",
        state: FeatureState::Partial,
        surfaces: WORKFLOW_SIGNAL_SURFACES,
        capability_field: Some("signal_and_query_header"),
        dynamic_config_key: None,
        rpcs: WORKFLOW_SIGNAL_RPCS,
        notes: "Signal delivery is part of the core workflow surface, but the audit classifies the broader compatibility state as partial.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
    FeatureEntry {
        catalog: TEMPORAL_GA_ENABLED,
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
        catalog: TEMPORAL_GA_ENABLED,
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
        catalog: TEMPORAL_GA_ENABLED,
        id: "workflow-update",
        name: "Workflow updates",
        state: FeatureState::Experimental,
        surfaces: WORKFLOW_UPDATE_SURFACES,
        capability_field: Some("workflow_update"),
        dynamic_config_key: Some("compat.workflow_update"),
        rpcs: WORKFLOW_UPDATE_RPCS,
        notes: "Workflow updates are visible but remain experimental until protocol-level SDK conformance evidence is added.",
        evidence: MATRIX_AUDIT_EVIDENCE,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
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

    #[test]
    fn feature_catalog_is_coherent_and_preserves_the_target_partition() {
        let upstream = upstream_rpcs("WorkflowService", WORKFLOW_SERVICE_PROTO)
            .into_iter()
            .chain(upstream_rpcs("OperatorService", OPERATOR_SERVICE_PROTO))
            .collect::<Vec<_>>();
        let verified =
            crate::verify_feature_catalog(FEATURE_MATRIX, &upstream, NEWER_VENDORED_WIRE_RPCS)
                .expect("canonical feature catalog");

        assert_eq!(verified.target_rpc_count, 121);
        assert_eq!(verified.newer_wire_rpc_count, 8);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: configuration-policy, Property 5: feature-catalog surface ownership
        #[test]
        fn feature_catalog_surface_ownership(
            rotate in any::<usize>(),
            mutation in 0_u8..5,
        ) {
            let upstream = upstream_rpcs("WorkflowService", WORKFLOW_SERVICE_PROTO)
                .into_iter()
                .chain(upstream_rpcs("OperatorService", OPERATOR_SERVICE_PROTO))
                .collect::<Vec<_>>();
            let mut entries = FEATURE_MATRIX.to_vec();
            let length = entries.len();
            entries.rotate_left(rotate % length);
            match mutation {
                0 => {}
                1 => {
                    let remove_at = entries
                        .iter()
                        .position(|entry| !entry.rpcs.is_empty())
                        .expect("catalog owns RPCs");
                    entries.remove(remove_at);
                }
                2 => entries.push(entries[0]),
                3 => {
                    let owner = entries
                        .iter_mut()
                        .find(|entry| !entry.rpcs.is_empty())
                        .expect("catalog owns RPCs");
                    owner.rpcs = &["WorkflowService.Invented"];
                }
                4 => {
                    let owner = entries
                        .iter_mut()
                        .find(|entry| entry.id == "nexus-operation-executions")
                        .expect("newer-wire feature");
                    owner.catalog.origin = FeatureOrigin::TemporalV1_31;
                }
                _ => unreachable!(),
            }

            let result = crate::verify_feature_catalog(
                &entries,
                &upstream,
                NEWER_VENDORED_WIRE_RPCS,
            );
            prop_assert_eq!(result.is_ok(), mutation == 0);
            if let Ok(verified) = result {
                prop_assert_eq!(verified.target_rpc_count, 121);
                prop_assert_eq!(verified.newer_wire_rpc_count, 8);
            }
        }

        // Feature: configuration-policy, Property 6: feature availability and guidance coherence
        #[test]
        fn feature_availability_and_guidance_coherence(mutation in 0_u8..8) {
            let mut entry = FEATURE_MATRIX
                .iter()
                .find(|entry| entry.id == "activity-executions")
                .copied()
                .expect("standalone activity entry");
            match mutation {
                0 => {}
                1 => entry.catalog.origin = FeatureOrigin::NewerVendoredWire,
                2 => entry.catalog.guidance = "",
                3 => entry.catalog.scopes = &[],
                4 => {
                    entry.catalog.enablement.kind = EnablementKind::None;
                    entry.catalog.enablement.reference = Some("impossible");
                }
                5 => entry.state = FeatureState::Unsupported,
                6 => entry.catalog.origin = FeatureOrigin::TokeiraNative,
                7 => entry.evidence = &[],
                _ => unreachable!(),
            }
            let result = crate::feature::validate_feature_metadata(&entry);
            prop_assert_eq!(result.is_ok(), mutation == 0);
        }
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
