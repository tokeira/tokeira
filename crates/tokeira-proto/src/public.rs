//! Public protobuf packages.
//!
//! These modules intentionally mirror the protobuf package structure rather than trying to hide
//! it completely. That makes it easier to compare generated code with the upstream proto files and
//! with Temporal-compatible API documentation.

pub mod temporal {
    pub mod api {
        pub mod activity {
            pub mod v1 {
                tonic::include_proto!("temporal.api.activity.v1");
            }
        }
        pub mod batch {
            pub mod v1 {
                tonic::include_proto!("temporal.api.batch.v1");
            }
        }
        pub mod callback {
            pub mod v1 {
                tonic::include_proto!("temporal.api.callback.v1");
            }
        }
        pub mod command {
            pub mod v1 {
                tonic::include_proto!("temporal.api.command.v1");
            }
        }
        pub mod common {
            pub mod v1 {
                tonic::include_proto!("temporal.api.common.v1");
            }
        }
        pub mod compute {
            pub mod v1 {
                tonic::include_proto!("temporal.api.compute.v1");
            }
        }
        pub mod deployment {
            pub mod v1 {
                tonic::include_proto!("temporal.api.deployment.v1");
            }
        }
        pub mod enums {
            pub mod v1 {
                tonic::include_proto!("temporal.api.enums.v1");
            }
        }
        pub mod errordetails {
            pub mod v1 {
                tonic::include_proto!("temporal.api.errordetails.v1");
            }
        }
        pub mod export {
            pub mod v1 {
                tonic::include_proto!("temporal.api.export.v1");
            }
        }
        pub mod failure {
            pub mod v1 {
                tonic::include_proto!("temporal.api.failure.v1");
            }
        }
        pub mod filter {
            pub mod v1 {
                tonic::include_proto!("temporal.api.filter.v1");
            }
        }
        pub mod history {
            pub mod v1 {
                tonic::include_proto!("temporal.api.history.v1");
            }
        }
        pub mod namespace {
            pub mod v1 {
                tonic::include_proto!("temporal.api.namespace.v1");
            }
        }
        pub mod nexus {
            pub mod v1 {
                tonic::include_proto!("temporal.api.nexus.v1");
            }
        }
        pub mod operatorservice {
            pub mod v1 {
                tonic::include_proto!("temporal.api.operatorservice.v1");
            }
        }
        pub mod protocol {
            pub mod v1 {
                tonic::include_proto!("temporal.api.protocol.v1");
            }
        }
        pub mod protometa {
            pub mod v1 {
                tonic::include_proto!("temporal.api.protometa.v1");
            }
        }
        pub mod query {
            pub mod v1 {
                tonic::include_proto!("temporal.api.query.v1");
            }
        }
        pub mod replication {
            pub mod v1 {
                tonic::include_proto!("temporal.api.replication.v1");
            }
        }
        pub mod rules {
            pub mod v1 {
                tonic::include_proto!("temporal.api.rules.v1");
            }
        }
        pub mod schedule {
            pub mod v1 {
                tonic::include_proto!("temporal.api.schedule.v1");
            }
        }
        pub mod sdk {
            pub mod v1 {
                tonic::include_proto!("temporal.api.sdk.v1");
            }
        }
        pub mod taskqueue {
            pub mod v1 {
                tonic::include_proto!("temporal.api.taskqueue.v1");
            }
        }
        pub mod update {
            pub mod v1 {
                tonic::include_proto!("temporal.api.update.v1");
            }
        }
        pub mod version {
            pub mod v1 {
                tonic::include_proto!("temporal.api.version.v1");
            }
        }
        pub mod worker {
            pub mod v1 {
                tonic::include_proto!("temporal.api.worker.v1");
            }
        }
        pub mod workflow {
            pub mod v1 {
                tonic::include_proto!("temporal.api.workflow.v1");
            }
        }
        pub mod workflowservice {
            pub mod v1 {
                tonic::include_proto!("temporal.api.workflowservice.v1");
            }
        }
    }
}

/// File descriptor set for the public API surface.
///
/// Keeping this available is useful for reflection, compatibility testing, and future HTTP /
/// transcoding or schema-diff tooling.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("tokeira_public_descriptor");

pub use temporal::api::{
    common::v1 as common, enums::v1 as enums, failure::v1 as failure, history::v1 as history,
    operatorservice::v1 as operatorservice, taskqueue::v1 as taskqueue, workflow::v1 as workflow,
    workflowservice::v1 as workflowservice,
};

/// Fully-qualified gRPC service name.
pub const WORKFLOW_SERVICE_NAME: &str = "temporal.api.workflowservice.v1.WorkflowService";

/// Fully-qualified gRPC service name.
pub const OPERATOR_SERVICE_NAME: &str = "temporal.api.operatorservice.v1.OperatorService";

/// HTTP proxy service segment commonly used by the Temporal UI and compatibility HTTP layers.
///
/// The concrete route shape depends on your HTTP adapter, but the default pattern is:
///
/// `/api/v1/{service}/{method}`
pub const WORKFLOW_HTTP_SERVICE: &str = "workflowservice.WorkflowService";

/// HTTP proxy service segment commonly used by the Temporal UI and compatibility HTTP layers.
pub const OPERATOR_HTTP_SERVICE: &str = "operatorservice.OperatorService";

pub fn http_proxy_path(service: &str, method: &str) -> String {
    format!("/api/v1/{service}/{method}")
}
