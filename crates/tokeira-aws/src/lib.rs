//! AWS resource implementations and SDK client ownership.
//!
//! This crate owns all concrete AWS resource implementations (each implementing
//! `tokeira_iac::Resource`) and the `AwsClients` bundle that holds the 10 SDK
//! clients. Resource implementations are registered by the `project` crate;
//! `AwsClients` is registered on `ProvisionContext` by the CLI.

mod clients;
mod context;
mod iam_policy;
pub mod remote_workstation;
pub mod remote_workstation_bootstrap;
pub mod resources;

pub use clients::{
    AwsClients,
    ecr::{
        DefaultEcrClient, EcrAuthorization, EcrClient, EcrClientHandle, EcrError,
        ImageTagMutability, RepositoryDescription, decode_authorization_data,
        ensure_ecr_repositories, ensure_ecr_repository,
    },
};
pub use context::ResourceContext;
