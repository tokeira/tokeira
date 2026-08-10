//! The authorable-resource contract and its execution placement.

use std::{collections::BTreeMap, fmt::Debug};

use crate::{content::ContentIdentity, error::KindError};

/// Logical placement supplied to a resource exactly once at execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementContext {
    /// Stable deployment identity used by provider naming policies.
    pub deployment_id: String,
    /// Deployment root admitted by the shell for platform-local resources.
    pub deployment_dir: std::path::PathBuf,
    /// Directory the interpreted definition source was read from — the
    /// deployment root for a working realization, a retained revision folder
    /// for a baseline. Resources that read desired-source companion files
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

/// The contract a resource implements so definitions can author it.
///
/// There is no separate kind entity: the resource itself validates its
/// authored fields, states its desired manifest, and completes itself with
/// placement at execution.
pub trait Kind: Debug + Send + Sync {
    /// The resource's one word — its `TYPE` const.
    fn name(&self) -> &'static str;

    /// Validate authored input without invocation identity or provider access.
    fn validate_input(&self) -> Result<(), KindError>;

    /// Complete output names admitted by the structural graph.
    fn declared_outputs(&self) -> &'static [&'static str];

    /// Provider-owned desired manifest at admitted invocation placement.
    fn desired_manifest(&self, placement: &PlacementContext) -> serde_json::Value;

    /// Realize the resource with invocation-bound identity exactly once at
    /// execution.
    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError>;
}

/// Decode one authored resource from its located value: the serde ceremony
/// every namespace's decode match shares. Deserialization failures carry the
/// authoring location so definition errors point at the source.
pub fn decode<K: Kind + serde::de::DeserializeOwned + 'static>(
    value: crate::author::LocatedValue,
) -> Result<Box<dyn Kind>, KindError> {
    let range = value.range;
    crate::author::from_located_value::<K>(value)
        .map(|kind| Box::new(kind) as Box<dyn Kind>)
        .map_err(|error| KindError::new(error.to_string()).at(error.range().or(range)))
}

// Boxed authored resources flow through graphs and realization, which are
// generic over `K: Kind` — the box must speak the contract itself.
impl Kind for Box<dyn Kind> {
    fn name(&self) -> &'static str {
        self.as_ref().name()
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
