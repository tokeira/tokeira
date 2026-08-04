//! The engine kind library: every provider's complete kind export, aggregated.
//!
//! There is exactly one authoring vocabulary per engine version. Providers
//! export their full kind sets (`tokeira_aws::kinds`, `tokeira_compose::kinds`);
//! this crate unions them into [`EngineKind`] and the engine-owned
//! [`kind_functions`]. Platforms do not curate kinds — a definition edited
//! within one engine version can adopt any kind the engine ships. What a
//! platform *does* own is real: the providers it wires at execution.
//! [`verify_wiring`] enforces that boundary at the earliest pure moment, so a
//! definition naming a kind its platform cannot execute fails at
//! `definition check` with the provider named — never at apply.

use tokeira_aws::kinds as aws;
use tokeira_compose::kinds as compose;
use tokeira_platform::{
    author::LocatedValue,
    error::{KindError, VerificationFinding},
    graph::VerifiedGraph,
    kind::{KindFunctions, PlacementContext, ProviderKind},
};

/// A provider a platform can wire: the execution stack a kind's resources
/// need. Open by construction — new providers extend the enum with their
/// export module, and every match below is exhaustive so the compiler names
/// each site a new provider must visit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    /// AWS SDK stack (`tokeira-aws`).
    Aws,
    /// Docker Compose / deployment-local stack (`tokeira-compose`).
    Compose,
}

impl Provider {
    /// Stable operator-facing provider identifier.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Aws => "aws",
            Self::Compose => "compose",
        }
    }
}

/// Complete author-visible kind names for the whole engine, in provider
/// order. Maintained manually because slice concatenation is not const;
/// [`tests::names_concatenate_provider_inventories`] holds it equal to the
/// provider exports so it cannot drift.
pub const KIND_NAMES: &[&str] = &[
    // aws
    "DsqlCluster",
    "DynamoDbTable",
    // compose
    "LocalStateDir",
    "ObservabilityConfiguration",
    "ServerConfig",
    "Service",
];

/// The engine's kind union: one variant per provider export.
#[derive(Debug)]
pub enum EngineKind {
    /// AWS provider kinds.
    Aws(aws::AwsKind),
    /// Compose provider kinds.
    Compose(compose::ComposeKind),
}

impl EngineKind {
    /// The provider whose execution stack this kind's resources need.
    pub fn provider(&self) -> Provider {
        match self {
            Self::Aws(_) => Provider::Aws,
            Self::Compose(_) => Provider::Compose,
        }
    }
}

impl ProviderKind for EngineKind {
    fn kind_name(&self) -> &'static str {
        match self {
            Self::Aws(kind) => kind.kind_name(),
            Self::Compose(kind) => kind.kind_name(),
        }
    }

    fn validate_input(&self) -> Result<(), KindError> {
        match self {
            Self::Aws(kind) => kind.validate_input(),
            Self::Compose(kind) => kind.validate_input(),
        }
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        match self {
            Self::Aws(kind) => kind.declared_outputs(),
            Self::Compose(kind) => kind.declared_outputs(),
        }
    }

    fn desired_manifest(&self, placement: &PlacementContext) -> serde_json::Value {
        match self {
            Self::Aws(kind) => kind.desired_manifest(placement),
            Self::Compose(kind) => kind.desired_manifest(placement),
        }
    }

    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError> {
        match self {
            Self::Aws(kind) => kind.realize(placement),
            Self::Compose(kind) => kind.realize(placement),
        }
    }
}

/// The provider owning an author-visible kind name, when the name exists.
pub fn provider_of(name: &str) -> Option<Provider> {
    if aws::KIND_NAMES.contains(&name) {
        Some(Provider::Aws)
    } else if compose::KIND_NAMES.contains(&name) {
        Some(Provider::Compose)
    } else {
        None
    }
}

/// The engine-owned kind constructors every definition frontend receives.
pub fn kind_functions() -> KindFunctions<EngineKind> {
    KindFunctions {
        names: KIND_NAMES,
        contains: |name| provider_of(name).is_some(),
        defaults: |name| match provider_of(name)? {
            Provider::Aws => aws::defaults(name),
            Provider::Compose => compose::defaults(name),
        },
        decode,
    }
}

fn decode(name: &str, value: LocatedValue) -> Result<EngineKind, KindError> {
    match provider_of(name) {
        Some(Provider::Aws) => aws::decode(name, value).map(EngineKind::Aws),
        Some(Provider::Compose) => compose::decode(name, value).map(EngineKind::Compose),
        None => Err(KindError::new(format!("unknown kind `{name}`"))),
    }
}

/// Refuse resources whose kinds need a provider the platform does not wire.
///
/// Pure and complete: every finding names the resource, the kind, and the
/// missing provider, so the operator learns at `definition check` what would
/// otherwise surface as a provider failure at apply.
pub fn verify_wiring(
    graph: &VerifiedGraph<EngineKind>,
    platform: &str,
    wired: &[Provider],
) -> Vec<VerificationFinding> {
    graph
        .resources()
        .iter()
        .filter_map(|resource| {
            let provider = resource.kind().provider();
            if wired.contains(&provider) {
                return None;
            }
            Some(VerificationFinding::UnwiredProvider {
                resource: format!("{}/{}", resource.module(), resource.logical_id()),
                provider_kind: resource.kind().kind_name().to_string(),
                provider: provider.as_str().to_string(),
                platform: platform.to_string(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // The engine inventory is exactly the concatenation of the provider
    // exports — the manual const cannot drift from the providers.
    #[test]
    fn names_concatenate_provider_inventories() {
        let expected: Vec<&str> = aws::KIND_NAMES
            .iter()
            .chain(compose::KIND_NAMES.iter())
            .copied()
            .collect();
        assert_eq!(KIND_NAMES, expected.as_slice());
        for name in KIND_NAMES {
            assert!(provider_of(name).is_some());
            assert!((kind_functions().contains)(name));
        }
        assert!(provider_of("NotAKind").is_none());
    }

    // Kind names are globally unique across providers: decode routing by
    // name is only sound if no two providers export the same name.
    #[test]
    fn kind_names_are_globally_unique() {
        let mut seen = std::collections::BTreeSet::new();
        for name in KIND_NAMES {
            assert!(
                seen.insert(name),
                "duplicate kind name `{name}` across providers"
            );
        }
    }
}
