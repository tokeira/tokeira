//! Public protobuf packages.
//!
//! These modules intentionally mirror the protobuf package structure rather than trying to hide
//! it completely. That makes it easier to compare generated code with the upstream proto files and
//! with Temporal-compatible API documentation.

// Generated code: the codegen's unwraps are its own.
#![allow(clippy::unwrap_used)]
pub mod temporal {
    pub mod api {
        pub mod activity {
            pub mod v1 {
                include!("generated/upstream/temporal.api.activity.v1.rs");
            }
        }
        pub mod batch {
            pub mod v1 {
                include!("generated/upstream/temporal.api.batch.v1.rs");
            }
        }
        pub mod callback {
            pub mod v1 {
                include!("generated/upstream/temporal.api.callback.v1.rs");
            }
        }
        pub mod command {
            pub mod v1 {
                include!("generated/upstream/temporal.api.command.v1.rs");
            }
        }
        pub mod common {
            pub mod v1 {
                include!("generated/upstream/temporal.api.common.v1.rs");
            }
        }
        pub mod compute {
            pub mod v1 {
                include!("generated/upstream/temporal.api.compute.v1.rs");
            }
        }
        pub mod deployment {
            pub mod v1 {
                include!("generated/upstream/temporal.api.deployment.v1.rs");
            }
        }
        pub mod enums {
            pub mod v1 {
                include!("generated/upstream/temporal.api.enums.v1.rs");
            }
        }
        pub mod errordetails {
            pub mod v1 {
                include!("generated/upstream/temporal.api.errordetails.v1.rs");
            }
        }
        pub mod export {
            pub mod v1 {
                include!("generated/upstream/temporal.api.export.v1.rs");
            }
        }
        pub mod failure {
            pub mod v1 {
                include!("generated/upstream/temporal.api.failure.v1.rs");
            }
        }
        pub mod filter {
            pub mod v1 {
                include!("generated/upstream/temporal.api.filter.v1.rs");
            }
        }
        pub mod history {
            pub mod v1 {
                include!("generated/upstream/temporal.api.history.v1.rs");
            }
        }
        pub mod namespace {
            pub mod v1 {
                include!("generated/upstream/temporal.api.namespace.v1.rs");
            }
        }
        pub mod nexus {
            pub mod v1 {
                include!("generated/upstream/temporal.api.nexus.v1.rs");
            }
        }
        pub mod operatorservice {
            pub mod v1 {
                include!("generated/upstream/temporal.api.operatorservice.v1.rs");
            }
        }
        pub mod protocol {
            pub mod v1 {
                include!("generated/upstream/temporal.api.protocol.v1.rs");
            }
        }
        pub mod protometa {
            pub mod v1 {
                include!("generated/upstream/temporal.api.protometa.v1.rs");
            }
        }
        pub mod query {
            pub mod v1 {
                include!("generated/upstream/temporal.api.query.v1.rs");
            }
        }
        pub mod replication {
            pub mod v1 {
                include!("generated/upstream/temporal.api.replication.v1.rs");
            }
        }
        pub mod rules {
            pub mod v1 {
                include!("generated/upstream/temporal.api.rules.v1.rs");
            }
        }
        pub mod schedule {
            pub mod v1 {
                // The vendored Temporal `IntervalSpec` doc comments contain literal
                // `<interval>`, `<phase>`, and `<timezone>` placeholders that rustdoc
                // parses as unclosed HTML tags. The text is upstream proto documentation
                // we mirror verbatim (proto/upstream/), not authored here, so we silence
                // the lint for this generated module rather than rewrite vendored docs.
                #![allow(rustdoc::invalid_html_tags)]
                include!("generated/upstream/temporal.api.schedule.v1.rs");
            }
        }
        pub mod sdk {
            pub mod v1 {
                include!("generated/upstream/temporal.api.sdk.v1.rs");
            }
        }
        pub mod taskqueue {
            pub mod v1 {
                include!("generated/upstream/temporal.api.taskqueue.v1.rs");
            }
        }
        pub mod update {
            pub mod v1 {
                include!("generated/upstream/temporal.api.update.v1.rs");
            }
        }
        pub mod version {
            pub mod v1 {
                include!("generated/upstream/temporal.api.version.v1.rs");
            }
        }
        pub mod worker {
            pub mod v1 {
                include!("generated/upstream/temporal.api.worker.v1.rs");
            }
        }
        pub mod workflow {
            pub mod v1 {
                include!("generated/upstream/temporal.api.workflow.v1.rs");
            }
        }
        pub mod workflowservice {
            pub mod v1 {
                include!("generated/upstream/temporal.api.workflowservice.v1.rs");
            }
        }
    }
    // Server-internal surface. tokeira serves only the AdminService's
    // DescribeMutableState (reset conformance); see the proto for the wire-compat
    // rationale.
    pub mod server {
        pub mod api {
            pub mod adminservice {
                pub mod v1 {
                    include!("generated/upstream/temporal.server.api.adminservice.v1.rs");
                }
            }
        }
    }
}

/// File descriptor set for the public API surface.
///
/// Keeping this available is useful for reflection, compatibility testing, and future HTTP /
/// transcoding or schema-diff tooling.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("generated/upstream/tokeira_public_descriptor.bin");

/// Official Temporal API v1.62.11 OpenAPI v2 document served by the HTTP API.
///
/// This is the decompressed `OpenAPIV2JSONSpec` artifact from the same upstream API
/// release as the vendored protobuf surface. It intentionally remains an opaque byte
/// asset so serving it cannot drift from the pinned upstream document.
pub const OPENAPI_V2_JSON: &[u8] =
    include_bytes!("generated/upstream/openapi/openapiv2.swagger.json");

/// Official Temporal API v1.62.11 OpenAPI v3 document served by the HTTP API.
///
/// This is the decompressed `OpenAPIV3YAMLSpec` artifact from the same upstream API
/// release as the vendored protobuf surface. It intentionally remains an opaque byte
/// asset so serving it cannot drift from the pinned upstream document.
pub const OPENAPI_V3_YAML: &[u8] = include_bytes!("generated/upstream/openapi/openapiv3.yaml");

pub use temporal::{
    api::{
        common::v1 as common, enums::v1 as enums, failure::v1 as failure, history::v1 as history,
        operatorservice::v1 as operatorservice, taskqueue::v1 as taskqueue,
        workflow::v1 as workflow, workflowservice::v1 as workflowservice,
    },
    server::api::adminservice::v1 as adminservice,
};

/// Fully-qualified gRPC service name for the (minimal) AdminService.
pub const ADMIN_SERVICE_NAME: &str = "temporal.server.api.adminservice.v1.AdminService";

/// Fully-qualified gRPC service name.
pub const WORKFLOW_SERVICE_NAME: &str = "temporal.api.workflowservice.v1.WorkflowService";

/// Fully-qualified gRPC service name.
pub const OPERATOR_SERVICE_NAME: &str = "temporal.api.operatorservice.v1.OperatorService";

#[cfg(test)]
mod tests {
    use super::{OPENAPI_V2_JSON, OPENAPI_V3_YAML};

    #[test]
    fn pinned_openapi_documents_are_valid() {
        let v2: serde_json::Value =
            serde_json::from_slice(OPENAPI_V2_JSON).expect("valid official OpenAPI v2 JSON");
        assert_eq!(v2["swagger"], "2.0");

        let v3: serde_yaml::Value =
            serde_yaml::from_slice(OPENAPI_V3_YAML).expect("valid official OpenAPI v3 YAML");
        assert_eq!(v3["openapi"], "3.0.3");
    }
}
