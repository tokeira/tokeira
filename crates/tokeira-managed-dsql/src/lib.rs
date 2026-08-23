//! Crash-safe ownership of a dedicated Aurora DSQL cluster for embedded Tokeira.
//!
//! This crate is the narrow AWS control-plane boundary for embedded startup. It owns
//! the durable creation token and canonical cluster identity, while deliberately
//! excluding engine, storage, runtime, CLI, and kernel concerns.

mod aws;
mod control;
mod descriptor;
mod identity;
mod lifecycle;

pub use aws::AwsDsqlControlPlane;
pub use control::{
    ClusterObservation, ClusterStatus, CreateClusterRequest, DeleteClusterRequest,
    DsqlControlError, DsqlControlPlane, RetryableErrorKind, SetDeletionProtectionRequest,
};
pub use descriptor::{
    ClusterDescriptorState, ClusterDescriptorStore, ClusterDescriptorV1, DescriptorError,
    DsqlClientToken, LocalClusterDescriptorStore, VersionedClusterDescriptor,
};
pub use identity::{CanonicalClusterIdentity, IdentityError};
pub use lifecycle::{
    ClusterAction, CreateOrRecoverRequest, LifecycleEnvironment, ManagedDsqlError,
    ManagedDsqlLifecycle, Readiness, ResolvedCluster, RetryPolicy, StartupDeadline,
    SystemLifecycleEnvironment, UsableCluster,
};
