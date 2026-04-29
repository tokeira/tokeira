//! Infrastructure module abstraction.
//!
//! A [`Module`] is a named deployment unit with explicit dependencies on
//! other modules. It enumerates the resources it owns given the current
//! state and any registered extensions.
//!
//! Custom deployments usually define a small set of modules that mirror the
//! operator's workflow: for example `network`, `database`, `cluster`, and
//! `services`. A module should be stable and coarse grained. Individual cloud
//! or local objects belong in [`crate::Resource`] implementations returned by
//! [`Module::resources`].

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt::Debug;

use crate::Resource;
use crate::error::IacError;

/// Context passed to [`Module::resources`] for resource assembly.
///
/// Extensions provide access to configuration and other context without
/// coupling this crate to specific config types.
pub struct ModuleContext<'a> {
    pub state: &'a crate::document::InfraState,
    extensions: &'a HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl<'a> ModuleContext<'a> {
    pub fn new(
        state: &'a crate::document::InfraState,
        extensions: &'a HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    ) -> Self {
        Self { state, extensions }
    }

    /// Retrieve a typed extension by type.
    pub fn extension<T: 'static + Send + Sync>(&self) -> Option<&T> {
        self.extensions
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref::<T>())
    }
}

/// A named infrastructure deployment unit with explicit dependencies.
///
/// Implement this trait for each deployment-specific module. Module names are
/// used for selection, ordering, reporting, and persisted resource ownership, so
/// they should be short stable identifiers rather than display labels.
#[async_trait::async_trait]
pub trait Module: Debug + Send + Sync {
    /// Stable module name.
    fn name(&self) -> &str;

    /// Names of modules that must be applied before this one.
    ///
    /// These are module names from [`name`](Self::name), not resource IDs. The
    /// engine uses them to order module expansion before it evaluates resource
    /// dependencies.
    fn dependencies(&self) -> &[&str];

    /// Enumerate desired resources for this module.
    ///
    /// This method converts deployment config and current state into concrete
    /// resource objects. It should not create, update, or delete provider
    /// resources directly; side effects belong in the returned resources'
    /// lifecycle methods.
    fn resources(&self, ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError>;
}
