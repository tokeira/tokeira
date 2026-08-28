//! Generated protobuf and gRPC bindings for Tokeira.
//!
//! This crate deliberately separates:
//!
//! - `public`: Temporal-compatible API packages and service constants
//! - `internal`: Tokeira-only runtime/admin packages (tonic/prost — legacy)
//! - `connect`: Tokeira-internal controller surface (buffa + connect-rust)
//! - `conversions`: small, explicit helpers between wire structs and `tokeira-types`
//!
//! The bindings are generated ahead of time and checked in under
//! `src/generated/` (`upstream/` for the Temporal surface, `tokeira/` for
//! Tokeira's own packages), so building this crate needs neither the vendored
//! `proto/` tree nor `protoc`. Regenerate with `cargo run -p proto-sync --
//! generate` after any change to the vendored protos or the codegen stack.
//!
//! The `connect` module is the preferred path for controller ↔ runtime/edge/autoscaler
//! communication. It provides zero-copy view types and the connect-rust service traits.
//! The `internal` module retains the tonic/prost output for code that hasn't migrated.

#![allow(refining_impl_trait_internal, refining_impl_trait_reachable)]
// Generated connect-rpc service markers/servers carry no Debug derive.
#![allow(missing_debug_implementations)]
// The tonic/prost-generated bindings (compiled from the upstream Temporal `.proto`s)
// trip style lints that are not actionable on generated code: large message/oneof
// enums, and doc-list formatting carried verbatim from the upstream proto comments.
#![allow(
    clippy::large_enum_variant,
    clippy::doc_lazy_continuation,
    clippy::doc_overindented_list_items
)]

pub mod conversions;
pub mod internal;
pub mod public;

/// Tokeira-owned provider-neutral Worker Compute Controller contract.
pub mod compute {
    /// Fixed Nexus service implemented by remote worker-compute providers.
    pub const NEXUS_SERVICE_NAME: &str = "tokeira.worker.compute.v1.ComputeProvider";
    /// Fixed synchronous Nexus operation used to request one worker.
    pub const INVOKE_WORKER_OPERATION: &str = "invoke-worker";
    /// Protobuf payload message type for provider requests.
    pub const INVOKE_WORKER_REQUEST_MESSAGE_TYPE: &str = "tokeira.compute.v1.InvokeWorkerRequest";
    /// Protobuf payload message type for provider responses.
    pub const INVOKE_WORKER_RESPONSE_MESSAGE_TYPE: &str = "tokeira.compute.v1.InvokeWorkerResponse";

    /// Version-one worker-compute messages.
    pub mod v1 {
        include!("generated/tokeira/tokeira.compute.v1.rs");
    }
}

/// Connect-rust generated types for the internal controller surface.
///
/// Provides buffa message types (owned + zero-copy views), connect-rust
/// service traits, and generated clients. Speaks Connect, gRPC, and
/// gRPC-Web on the same handlers.
pub mod connect {
    // Generated code: the codegen's unwraps are its own.
    #![allow(clippy::unwrap_used)]
    include!("generated/tokeira/_connectrpc_controller.rs");
}

pub use internal::{admin, controller, runtime};
pub use public::{
    adminservice, common, enums, failure, history, operatorservice, taskqueue, workflow,
    workflowservice,
};

#[cfg(test)]
mod worker_compute_tests {
    use prost::Message;

    use crate::{
        compute::v1::{
            InvokeReason, InvokeWorkerRequest, InvokeWorkerResponse, TaskQueueBinding,
            TaskQueueType,
        },
        public::temporal::api::common::v1::Payload,
    };

    #[test]
    fn worker_compute_messages_round_trip() {
        let request = InvokeWorkerRequest {
            request_id: "action-id".to_owned(),
            namespace: "namespace".to_owned(),
            deployment_name: "deployment".to_owned(),
            build_id: "build".to_owned(),
            scaling_group: "group".to_owned(),
            count: 1,
            task_queues: vec![TaskQueueBinding {
                name: "queue".to_owned(),
                r#type: TaskQueueType::Workflow as i32,
            }],
            provider_type: "provider".to_owned(),
            provider_details: Some(Payload {
                metadata: Default::default(),
                data: b"details".to_vec(),
                external_payloads: Vec::new(),
            }),
            configuration_fingerprint: vec![7; 32],
            reason: InvokeReason::ConfigurationActivation as i32,
        };
        let encoded = request.encode_to_vec();
        assert_eq!(
            InvokeWorkerRequest::decode(encoded.as_slice()).expect("request decodes"),
            request
        );

        let response = InvokeWorkerResponse {
            request_id: "action-id".to_owned(),
        };
        assert_eq!(
            InvokeWorkerResponse::decode(response.encode_to_vec().as_slice())
                .expect("response decodes"),
            response
        );
    }

    #[test]
    fn worker_compute_field_numbers_are_stable() {
        let singleton_tags = [
            (
                InvokeWorkerRequest {
                    request_id: "x".to_owned(),
                    ..InvokeWorkerRequest::default()
                },
                0x0a,
            ),
            (
                InvokeWorkerRequest {
                    namespace: "x".to_owned(),
                    ..InvokeWorkerRequest::default()
                },
                0x12,
            ),
            (
                InvokeWorkerRequest {
                    deployment_name: "x".to_owned(),
                    ..InvokeWorkerRequest::default()
                },
                0x1a,
            ),
            (
                InvokeWorkerRequest {
                    build_id: "x".to_owned(),
                    ..InvokeWorkerRequest::default()
                },
                0x22,
            ),
            (
                InvokeWorkerRequest {
                    scaling_group: "x".to_owned(),
                    ..InvokeWorkerRequest::default()
                },
                0x2a,
            ),
            (
                InvokeWorkerRequest {
                    count: 1,
                    ..InvokeWorkerRequest::default()
                },
                0x30,
            ),
            (
                InvokeWorkerRequest {
                    task_queues: vec![TaskQueueBinding {
                        name: "x".to_owned(),
                        r#type: TaskQueueType::Workflow as i32,
                    }],
                    ..InvokeWorkerRequest::default()
                },
                0x3a,
            ),
            (
                InvokeWorkerRequest {
                    provider_type: "x".to_owned(),
                    ..InvokeWorkerRequest::default()
                },
                0x42,
            ),
            (
                InvokeWorkerRequest {
                    provider_details: Some(Payload::default()),
                    ..InvokeWorkerRequest::default()
                },
                0x4a,
            ),
            (
                InvokeWorkerRequest {
                    configuration_fingerprint: vec![1],
                    ..InvokeWorkerRequest::default()
                },
                0x52,
            ),
            (
                InvokeWorkerRequest {
                    reason: InvokeReason::ConfigurationActivation as i32,
                    ..InvokeWorkerRequest::default()
                },
                0x58,
            ),
        ];

        for (message, expected_tag) in singleton_tags {
            assert_eq!(message.encode_to_vec().first(), Some(&expected_tag));
        }
        assert_eq!(
            InvokeWorkerResponse {
                request_id: "x".to_owned(),
            }
            .encode_to_vec(),
            [0x0a, 0x01, b'x']
        );
    }
}
