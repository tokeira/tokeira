//! Public protobuf packages.
//!
//! These modules intentionally mirror the protobuf package structure rather than trying to hide
//! it completely. That makes it easier to compare generated code with the upstream proto files and
//! with Temporal-compatible API documentation.

pub mod temporal {
    pub mod api {
        pub mod common {
            pub mod v1 {
                tonic::include_proto!("temporal.api.common.v1");
            }
        }

        pub mod enums {
            pub mod v1 {
                tonic::include_proto!("temporal.api.enums.v1");
            }
        }

        pub mod workflowservice {
            pub mod v1 {
                tonic::include_proto!("temporal.api.workflowservice.v1");
            }
        }

        pub mod operatorservice {
            pub mod v1 {
                tonic::include_proto!("temporal.api.operatorservice.v1");
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

pub use temporal::api::common::v1 as common;
pub use temporal::api::enums::v1 as enums;
pub use temporal::api::operatorservice::v1 as operatorservice;
pub use temporal::api::workflowservice::v1 as workflowservice;

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
