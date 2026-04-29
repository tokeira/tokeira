//! AWS resource implementations and SDK client ownership.
//!
//! This crate owns all concrete AWS resource implementations (each implementing
//! `tokeira_iac::Resource`) and the `AwsClients` bundle that holds the 10 SDK
//! clients. Resource implementations are registered by the `project` crate;
//! `AwsClients` is registered on `ProvisionContext` by the CLI.

mod clients;
mod context;
mod iam_policy;
pub mod resources;

pub use clients::AwsClients;
pub use context::ResourceContext;
