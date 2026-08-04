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
    /// Directory the interpreted definition source was read from — the
    /// deployment root for a working realization, a retained revision folder
    /// for a baseline. Kinds that read desired-source companion files
    /// resolve them here, so a baseline realization digests the retained
    /// companion rather than the live one.
    pub definition_dir: std::path::PathBuf,
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
#[derive(Debug)]
pub struct KindFunctions<K> {
    /// Complete author-visible kind names — the single inventory a frontend
    /// may enumerate (a definition edited within one engine version can adopt
    /// any kind the engine ships, so no frontend curates below this set).
    /// The platform backs `contains` with the same slice; a platform test
    /// asserts the two agree.
    pub names: &'static [&'static str],
    /// Test whether an author-visible name belongs to the platform's closed first-party set.
    pub contains: fn(&str) -> bool,
    /// Return provider-owned defaults for `<Kind>::EMPTY`.
    pub defaults: fn(&str) -> Option<LocatedValue>,
    /// Decode one named kind into the platform's concrete kind enum.
    pub decode: fn(&str, LocatedValue) -> Result<K, KindError>,
}

// Manual impls: the struct is function pointers and a static slice, so it is
// unconditionally copyable — the derive would demand `K: Copy`, which no
// concrete kind enum satisfies or should.
impl<K> Clone for KindFunctions<K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K> Copy for KindFunctions<K> {}
