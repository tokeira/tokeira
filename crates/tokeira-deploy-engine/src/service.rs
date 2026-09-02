//! Service lifecycle abstractions.
//!
//! A [`Service`] represents a named deployed workload unit that produces
//! deployment manifests. [`ServiceContext`] carries persisted state and
//! typed extensions for platform access.

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt::Debug,
};

use crate::RuntimeError;

/// Context passed to [`Service::manifests`].
///
/// Extensions provide access to platform handles and configuration without
/// coupling this crate to specific implementations. For example, a Kubernetes
/// platform handle can be registered as an extension so the engine can apply
/// manifests after planning:
///
/// ```ignore
/// ctx.set_extension(K8sPlatform::new().await?);
/// ```
#[derive(Debug)]
pub struct ServiceContext {
    pub state: tokeira_iac::RuntimeState,
    pub infra_state: tokeira_iac::InfraState,
    extensions: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl ServiceContext {
    pub(crate) fn new(
        state: tokeira_iac::RuntimeState,
        infra_state: tokeira_iac::InfraState,
    ) -> Self {
        Self {
            state,
            infra_state,
            extensions: HashMap::new(),
        }
    }

    /// Retrieve a typed extension by type.
    pub fn extension<T: 'static + Send + Sync>(&self) -> Option<&T> {
        self.extensions
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref::<T>())
    }

    /// Register a typed extension.
    pub fn set_extension<T: 'static + Send + Sync>(&mut self, value: T) {
        self.extensions.insert(TypeId::of::<T>(), Box::new(value));
    }

    /// Convenience alias for `set_extension`.
    pub fn insert<T: 'static + Send + Sync>(&mut self, value: T) {
        self.set_extension(value);
    }
}

impl Default for ServiceContext {
    fn default() -> Self {
        Self::new(
            tokeira_iac::RuntimeState::default(),
            tokeira_iac::InfraState::default(),
        )
    }
}

/// A named deployed workload unit that produces deployment manifests.
///
/// Implement this trait for each workload the deployment manages. Services
/// should be pure descriptions of desired runtime state: use
/// [`manifests`](Self::manifests) to generate provider-specific manifests, and
/// let the selected [`crate::Platform`] perform the side effects.
pub trait Service: Debug + Send + Sync {
    /// Author-visible resource type represented by this service.
    fn resource_type(&self) -> &'static str;

    /// Validate the complete desired service before an engine sees it.
    fn validate_input(&self) -> Result<(), String> {
        Ok(())
    }

    /// Complete output names this service may expose to author references.
    fn declared_outputs(&self) -> &'static [&'static str] {
        &[]
    }

    /// Stable service name.
    fn name(&self) -> &str;

    /// Module that owns this service.
    ///
    /// This is used for reporting and should normally match the infrastructure
    /// module that produces the service's prerequisites.
    fn module(&self) -> &str;

    /// Services that must be applied before this one.
    ///
    /// These names should refer to other [`Service::name`] values. The current
    /// facade records the dependency intent for specializations and tests; a
    /// platform implementation may also use it when ordering manifest apply.
    fn dependencies(&self) -> Vec<&str>;

    /// Sanitized platform-owned coordinates needed for live operations.
    ///
    /// This descriptor is an ephemeral hand-off from the realized definition,
    /// not desired state and not a substitute for provider state. It must
    /// contain only non-secret routing facts that an operator capability needs
    /// (for example, a cluster and service name). Images, environment values,
    /// credentials, secret references, and resolved infrastructure outputs do
    /// not belong here. Platforms should deserialize their descriptor into a
    /// closed, validated type before using it.
    fn operations_metadata(&self) -> Option<serde_json::Value> {
        None
    }

    /// Produce the deployment manifests for this service.
    ///
    /// The manifest values are provider-specific JSON documents. They must be
    /// stable for unchanged desired state because the engine hashes them to
    /// decide whether a service needs to be applied.
    fn manifests(&self, ctx: &ServiceContext) -> Result<Vec<serde_json::Value>, RuntimeError>;
}
