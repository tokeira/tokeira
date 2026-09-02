//! AWS resource implementations and SDK client ownership.
//!
//! This crate owns all concrete AWS resource implementations (each implementing
//! `tokeira_iac::Resource`) and the `AwsClients` bundle that holds the AWS SDK
//! clients. Resource implementations and `AwsClients` are registered onto the
//! execution contexts by the host application.

// Resource constructors mirror the underlying AWS API surface, which takes many
// parameters.
#![allow(clippy::too_many_arguments)]

mod clients;
mod context;
mod iam_policy;
pub mod kinds;
pub mod resources;

pub use clients::{
    AwsClients,
    ecr::{
        DefaultEcrClient, EcrAuthorization, EcrClient, EcrClientHandle, EcrError,
        ImageTagMutability, RepositoryDescription, decode_authorization_data,
        ensure_ecr_repositories, ensure_ecr_repository, prepare_ecr_registry,
        prepare_ecr_registry_with_client,
    },
    register_infra_extensions,
};
pub use context::ResourceContext;
