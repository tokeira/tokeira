//! Concrete provider-kind input, placement, and realization contracts.

use std::{collections::BTreeMap, fmt::Debug};

use crate::{content::ContentIdentity, error::KindError};

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

/// The vocabulary's decode product: a realizable kind behind the one
/// object-safe contract.
pub type DecodedKind = Box<dyn ProviderKind>;

// Boxed kinds flow through graphs and realization, which are generic over
// `K: ProviderKind` — the box must speak the contract itself.
impl ProviderKind for Box<dyn ProviderKind> {
    fn kind_name(&self) -> &'static str {
        self.as_ref().kind_name()
    }

    fn validate_input(&self) -> Result<(), KindError> {
        self.as_ref().validate_input()
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        self.as_ref().declared_outputs()
    }

    fn desired_manifest(&self, placement: &PlacementContext) -> serde_json::Value {
        self.as_ref().desired_manifest(placement)
    }

    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError> {
        self.as_ref().realize(placement)
    }
}
