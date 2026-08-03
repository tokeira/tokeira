//! Concrete provider-kind input, placement, and realization contracts.

use std::{collections::BTreeMap, fmt::Debug};

use crate::{author::LocatedValue, content::ContentIdentity, error::KindError};

/// Logical placement supplied to a provider kind exactly once at execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementContext {
    /// Stable deployment identity used by provider naming policies.
    pub deployment_id: String,
    /// Deployment root admitted by the shell for platform-local resources.
    pub deployment_dir: std::path::PathBuf,
    /// Owning logical module.
    pub module: String,
    /// Logical id within the module.
    pub logical_id: String,
    /// Realized provider resource ids of declared dependencies.
    pub dependencies: Vec<tokeira_iac::ResourceId>,
    /// Desired-content identities of those same dependencies.
    pub dependency_content: BTreeMap<tokeira_iac::ResourceId, ContentIdentity>,
    /// Stable platform/provider tags.
    pub tags: BTreeMap<String, String>,
}

/// Authored input for one concrete provider resource.
pub trait ProviderKind: Debug + Send + Sync {
    /// Stable author-visible kind name.
    fn kind_name(&self) -> &'static str;

    /// Validate authored input without invocation identity or provider access.
    fn validate_input(&self) -> Result<(), KindError>;

    /// Complete output names admitted by the structural graph.
    fn declared_outputs(&self) -> &'static [&'static str];

    /// Provider-owned desired manifest at admitted invocation placement.
    fn desired_manifest(&self, placement: &PlacementContext) -> serde_json::Value;

    /// Realize the kind with invocation-bound identity exactly once at execution.
    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError>;
}

/// Compile-time constructor functions supplied by one concrete platform.
#[derive(Debug, Clone, Copy)]
pub struct KindFunctions<K> {
    /// Test whether an author-visible name belongs to the platform's closed first-party set.
    pub contains: fn(&str) -> bool,
    /// Return provider-owned defaults for `<Kind>::EMPTY`.
    pub defaults: fn(&str) -> Option<LocatedValue>,
    /// Decode one named kind into the platform's concrete kind enum.
    pub decode: fn(&str, LocatedValue) -> Result<K, KindError>,
}
