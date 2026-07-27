use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::matrix::FEATURE_MATRIX;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeatureState {
    Implemented,
    Partial,
    Experimental,
    Stubbed,
    Unsupported,
}

/// Release or product lineage that owns a feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FeatureOrigin {
    /// Public behavior present in Temporal server v1.31.0.
    TemporalV1_31,
    /// Wire shape present only in the newer vendored API.
    NewerVendoredWire,
    /// Public Tokeira extension with no Temporal v1.31.0 equivalent.
    TokeiraNative,
}

impl FeatureOrigin {
    /// Stable operator-facing label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::TemporalV1_31 => "temporal-v1.31.0",
            Self::NewerVendoredWire => "newer-vendored-wire",
            Self::TokeiraNative => "tokeira-native",
        }
    }
}

/// Whether a feature participates in the v1.31.0 compatibility claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConformanceDisposition {
    /// The feature is part of the claimed v1.31.0 behavior surface.
    InSurface,
    /// The feature is deliberately outside the claim.
    OutOfSurface,
    /// The feature is Tokeira-owned and has no Temporal conformance meaning.
    NotApplicable,
}

impl ConformanceDisposition {
    /// Stable operator-facing label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::InSurface => "in-surface",
            Self::OutOfSurface => "out-of-surface",
            Self::NotApplicable => "not-applicable",
        }
    }
}

/// Temporal's maturity classification at the compatibility pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TemporalMaturity {
    /// Stable generally available feature.
    GeneralAvailability,
    /// Public preview feature.
    PublicPreview,
    /// Experimental or pre-release feature.
    Experimental,
    /// Deprecated feature retained at the pin.
    Deprecated,
    /// Internal Temporal-only surface.
    Internal,
    /// Feature absent from v1.31.0.
    Absent,
    /// Tokeira-native feature.
    NotApplicable,
}

impl TemporalMaturity {
    /// Stable operator-facing label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::GeneralAvailability => "general-availability",
            Self::PublicPreview => "public-preview",
            Self::Experimental => "experimental",
            Self::Deprecated => "deprecated",
            Self::Internal => "internal",
            Self::Absent => "absent",
            Self::NotApplicable => "not-applicable",
        }
    }
}

/// Default availability or activation posture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DefaultPosture {
    /// Active without operator configuration.
    Enabled,
    /// Inactive until an explicit supported action enables it.
    Disabled,
    /// Determined by a documented request, namespace, worker, or environment condition.
    Conditional,
    /// No usable implementation is available.
    Unavailable,
    /// No default exists for this feature lineage.
    NotApplicable,
}

impl DefaultPosture {
    /// Stable operator-facing label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
            Self::Conditional => "conditional",
            Self::Unavailable => "unavailable",
            Self::NotApplicable => "not-applicable",
        }
    }
}

/// Mechanism through which an operator activates or authors policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EnablementKind {
    /// No activation is needed.
    None,
    /// Typed Tokeira TOML configuration.
    Toml,
    /// Public Temporal API mutation.
    PublicApi,
    /// Runtime derives activation automatically.
    Automatic,
    /// Activation exists only in a conformance build.
    ConformanceOnly,
    /// No activation mechanism exists.
    Unavailable,
}

impl EnablementKind {
    /// Stable operator-facing label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Toml => "toml",
            Self::PublicApi => "public-api",
            Self::Automatic => "automatic",
            Self::ConformanceOnly => "conformance-only",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Scope at which a feature or policy applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PolicyScope {
    /// Whole deployment.
    Cluster,
    /// One namespace.
    Namespace,
    /// One task-queue family and kind.
    TaskQueue,
    /// One workflow execution.
    Workflow,
    /// One worker identity.
    Worker,
    /// Cargo/build profile.
    Build,
    /// No policy scope.
    NotApplicable,
}

impl PolicyScope {
    /// Stable operator-facing label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Cluster => "cluster",
            Self::Namespace => "namespace",
            Self::TaskQueue => "task-queue",
            Self::Workflow => "workflow",
            Self::Worker => "worker",
            Self::Build => "build",
            Self::NotApplicable => "not-applicable",
        }
    }
}

/// Lifecycle of a policy value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PolicyMutability {
    /// Fixed by the release profile.
    Immutable,
    /// Read at process startup; changing it requires restart.
    StartupStatic,
    /// Authored through a durable live public API.
    DurableLiveApi,
    /// Derived automatically from current runtime facts.
    Automatic,
    /// Mutable only in a conformance build.
    ConformanceOnly,
    /// No policy value exists.
    NotApplicable,
}

impl PolicyMutability {
    /// Stable operator-facing label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Immutable => "immutable",
            Self::StartupStatic => "startup-static",
            Self::DurableLiveApi => "durable-live-api",
            Self::Automatic => "automatic",
            Self::ConformanceOnly => "conformance-only",
            Self::NotApplicable => "not-applicable",
        }
    }
}

/// Exact activation mechanism for a feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct FeatureEnablement {
    /// Mechanism kind.
    pub kind: EnablementKind,
    /// Exact TOML path, RPC, condition, or build path.
    pub reference: Option<&'static str>,
}

/// Operator-facing metadata attached to one canonical feature entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct FeatureCatalogMetadata {
    /// Feature lineage.
    pub origin: FeatureOrigin,
    /// Relationship to the v1.31.0 claim.
    pub conformance: ConformanceDisposition,
    /// Temporal maturity at v1.31.0.
    pub temporal_maturity: TemporalMaturity,
    /// Temporal stock default.
    pub temporal_default: DefaultPosture,
    /// Tokeira empty-configuration default.
    pub tokeira_default: DefaultPosture,
    /// Operator activation or authoring mechanism.
    pub enablement: FeatureEnablement,
    /// Policy scopes.
    pub scopes: &'static [PolicyScope],
    /// Policy lifecycle.
    pub mutability: PolicyMutability,
    /// Operator-facing action or unavailability explanation.
    pub guidance: &'static str,
    /// Required external or sibling capabilities.
    pub prerequisites: &'static [&'static str],
}

impl FeatureState {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Implemented => "implemented",
            Self::Partial => "partial",
            Self::Experimental => "experimental",
            Self::Stubbed => "stubbed",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatibilitySurfaceKind {
    Rpc,
    RequestField,
    ResponseField,
    HistoryEvent,
    CommandAttribute,
    EnumVariant,
    CapabilityFlag,
    ErrorDetail,
    BehaviouralInvariant,
}

impl CompatibilitySurfaceKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Rpc => "rpc",
            Self::RequestField => "request-field",
            Self::ResponseField => "response-field",
            Self::HistoryEvent => "history-event",
            Self::CommandAttribute => "command-attribute",
            Self::EnumVariant => "enum-variant",
            Self::CapabilityFlag => "capability-flag",
            Self::ErrorDetail => "error-detail",
            Self::BehaviouralInvariant => "behavioural-invariant",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilitySurface {
    pub kind: CompatibilitySurfaceKind,
    pub identifier: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatibilityEvidenceKind {
    Test,
    ManualReview,
    SdkConformance,
}

impl CompatibilityEvidenceKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Test => "test",
            Self::ManualReview => "manual-review",
            Self::SdkConformance => "sdk-conformance",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilityEvidence {
    pub kind: CompatibilityEvidenceKind,
    pub reference: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct FeatureEntry {
    /// Operator-facing catalog metadata.
    pub catalog: FeatureCatalogMetadata,
    pub id: &'static str,
    pub name: &'static str,
    pub state: FeatureState,
    pub surfaces: &'static [CompatibilitySurface],
    pub capability_field: Option<&'static str>,
    pub dynamic_config_key: Option<&'static str>,
    pub rpcs: &'static [&'static str],
    pub notes: &'static str,
    pub evidence: &'static [CompatibilityEvidence],
}

impl FeatureEntry {
    pub fn capability_fields(&self) -> impl Iterator<Item = &'static str> {
        self.capability_field
            .into_iter()
            .chain(secondary_capability_fields(self.id).iter().copied())
    }
}

/// Successful canonical feature-catalog verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifiedFeatureCatalog {
    /// RPCs belonging to Temporal v1.31.0/API v1.62.8.
    pub target_rpc_count: usize,
    /// RPCs present only in the newer vendored API.
    pub newer_wire_rpc_count: usize,
}

/// Why canonical feature metadata is incoherent or incomplete.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum FeatureCatalogError {
    /// A feature id appears more than once.
    #[error("duplicate feature id: {0}")]
    DuplicateFeature(String),
    /// A machine-denominated surface appears more than once.
    #[error("duplicate feature surface: {0}")]
    DuplicateSurface(String),
    /// An RPC appears more than once.
    #[error("duplicate RPC owner: {0}")]
    DuplicateRpc(String),
    /// An expected vendored RPC is absent.
    #[error("missing vendored RPC owner: {0}")]
    MissingRpc(String),
    /// The catalog owns an RPC outside the vendored denominator.
    #[error("unknown vendored RPC owner: {0}")]
    UnknownRpc(String),
    /// Metadata fields do not form a coherent record.
    #[error("invalid feature metadata for {id}: {reason}")]
    InvalidMetadata {
        /// Feature identity.
        id: String,
        /// Failed invariant.
        reason: &'static str,
    },
}

/// Verify canonical feature ownership and operator-facing metadata.
pub fn verify_feature_catalog(
    entries: &[FeatureEntry],
    vendored_rpcs: &[&str],
    newer_wire_rpcs: &[&str],
) -> Result<VerifiedFeatureCatalog, FeatureCatalogError> {
    let expected = vendored_rpcs.iter().copied().collect::<BTreeSet<_>>();
    if expected.len() != vendored_rpcs.len() {
        return Err(FeatureCatalogError::InvalidMetadata {
            id: "vendored-rpc-denominator".to_owned(),
            reason: "denominator contains duplicates",
        });
    }
    let newer = newer_wire_rpcs.iter().copied().collect::<BTreeSet<_>>();
    if newer.len() != newer_wire_rpcs.len() || !newer.is_subset(&expected) {
        return Err(FeatureCatalogError::InvalidMetadata {
            id: "newer-wire-rpc-denominator".to_owned(),
            reason: "newer-wire set is duplicated or not a vendored subset",
        });
    }

    let mut ids = BTreeSet::new();
    let mut surfaces = BTreeSet::new();
    let mut owners = BTreeMap::new();
    for entry in entries {
        if !ids.insert(entry.id) {
            return Err(FeatureCatalogError::DuplicateFeature(entry.id.to_owned()));
        }
        validate_feature_metadata(entry)?;
        for surface in entry.surfaces {
            let identity = (surface.kind.label(), surface.identifier);
            if !surfaces.insert(identity) {
                return Err(FeatureCatalogError::DuplicateSurface(format!(
                    "{}:{}",
                    identity.0, identity.1
                )));
            }
        }
        for rpc in entry.rpcs {
            if owners.insert(*rpc, entry).is_some() {
                return Err(FeatureCatalogError::DuplicateRpc((*rpc).to_owned()));
            }
        }
    }

    for rpc in &expected {
        if !owners.contains_key(rpc) {
            return Err(FeatureCatalogError::MissingRpc((*rpc).to_owned()));
        }
    }
    for (rpc, owner) in &owners {
        if !expected.contains(rpc) {
            return Err(FeatureCatalogError::UnknownRpc((*rpc).to_owned()));
        }
        let expected_origin = if newer.contains(rpc) {
            FeatureOrigin::NewerVendoredWire
        } else {
            FeatureOrigin::TemporalV1_31
        };
        if owner.catalog.origin != expected_origin {
            return Err(FeatureCatalogError::InvalidMetadata {
                id: owner.id.to_owned(),
                reason: "feature origin does not match its RPC denominator",
            });
        }
    }

    Ok(VerifiedFeatureCatalog {
        target_rpc_count: expected.len() - newer.len(),
        newer_wire_rpc_count: newer.len(),
    })
}

pub(crate) fn validate_feature_metadata(entry: &FeatureEntry) -> Result<(), FeatureCatalogError> {
    if entry.id.trim().is_empty()
        || entry.name.trim().is_empty()
        || entry.notes.trim().is_empty()
        || entry.catalog.guidance.trim().is_empty()
        || entry.catalog.scopes.is_empty()
        || entry.evidence.is_empty()
    {
        return Err(FeatureCatalogError::InvalidMetadata {
            id: entry.id.to_owned(),
            reason: "required catalog metadata is blank",
        });
    }

    match entry.catalog.origin {
        FeatureOrigin::TemporalV1_31 => {
            if entry.catalog.conformance == ConformanceDisposition::NotApplicable
                || matches!(
                    entry.catalog.temporal_maturity,
                    TemporalMaturity::Absent | TemporalMaturity::NotApplicable
                )
                || entry.catalog.temporal_default == DefaultPosture::NotApplicable
            {
                return Err(FeatureCatalogError::InvalidMetadata {
                    id: entry.id.to_owned(),
                    reason: "Temporal feature lacks target-release metadata",
                });
            }
        }
        FeatureOrigin::NewerVendoredWire => {
            if entry.catalog.conformance != ConformanceDisposition::OutOfSurface
                || entry.catalog.temporal_maturity != TemporalMaturity::Absent
                || entry.catalog.temporal_default != DefaultPosture::NotApplicable
            {
                return Err(FeatureCatalogError::InvalidMetadata {
                    id: entry.id.to_owned(),
                    reason: "newer-wire feature is not absent and out of surface",
                });
            }
        }
        FeatureOrigin::TokeiraNative => {
            if entry.catalog.conformance != ConformanceDisposition::NotApplicable
                || entry.catalog.temporal_maturity != TemporalMaturity::NotApplicable
                || entry.catalog.temporal_default != DefaultPosture::NotApplicable
            {
                return Err(FeatureCatalogError::InvalidMetadata {
                    id: entry.id.to_owned(),
                    reason: "Tokeira-native feature claims Temporal metadata",
                });
            }
        }
    }

    if entry.state == FeatureState::Unsupported
        && (entry.catalog.tokeira_default != DefaultPosture::Unavailable
            || entry.catalog.enablement.kind != EnablementKind::Unavailable)
    {
        return Err(FeatureCatalogError::InvalidMetadata {
            id: entry.id.to_owned(),
            reason: "unsupported feature advertises availability",
        });
    }

    let enablement_is_coherent = match entry.catalog.enablement.kind {
        EnablementKind::None => {
            entry.catalog.enablement.reference.is_none()
                && matches!(
                    entry.catalog.mutability,
                    PolicyMutability::Immutable | PolicyMutability::NotApplicable
                )
        }
        EnablementKind::Toml => {
            entry.catalog.enablement.reference.is_some()
                && entry.catalog.mutability == PolicyMutability::StartupStatic
        }
        EnablementKind::PublicApi => {
            entry.catalog.enablement.reference.is_some()
                && entry.catalog.mutability == PolicyMutability::DurableLiveApi
        }
        EnablementKind::Automatic => {
            entry.catalog.enablement.reference.is_some()
                && entry.catalog.mutability == PolicyMutability::Automatic
        }
        EnablementKind::ConformanceOnly => {
            entry.catalog.enablement.reference.is_some()
                && entry.catalog.mutability == PolicyMutability::ConformanceOnly
        }
        EnablementKind::Unavailable => {
            entry.catalog.enablement.reference.is_none()
                && matches!(
                    entry.catalog.mutability,
                    PolicyMutability::Immutable | PolicyMutability::NotApplicable
                )
        }
    };
    if !enablement_is_coherent {
        return Err(FeatureCatalogError::InvalidMetadata {
            id: entry.id.to_owned(),
            reason: "enablement and mutability disagree",
        });
    }

    if entry.catalog.tokeira_default == DefaultPosture::Disabled
        && !matches!(
            entry.catalog.enablement.kind,
            EnablementKind::Toml
                | EnablementKind::PublicApi
                | EnablementKind::Automatic
                | EnablementKind::ConformanceOnly
                | EnablementKind::Unavailable
        )
    {
        return Err(FeatureCatalogError::InvalidMetadata {
            id: entry.id.to_owned(),
            reason: "default-disabled feature has no exact activation guidance",
        });
    }
    Ok(())
}

fn secondary_capability_fields(feature_id: &str) -> &'static [&'static str] {
    match feature_id {
        "workflow-task-lifecycle" => &["upsert_memo"],
        _ => &[],
    }
}

pub trait Feature {
    const ID: &'static str;
    const ENTRY: &'static FeatureEntry;
}

pub const fn lookup_feature_const(id: &'static str) -> &'static FeatureEntry {
    let mut index = 0;
    while index < FEATURE_MATRIX.len() {
        if const_str_eq(FEATURE_MATRIX[index].id, id) {
            return &FEATURE_MATRIX[index];
        }
        index += 1;
    }
    panic!("declare_feature!: id not found in FEATURE_MATRIX")
}

pub const fn const_str_eq(left: &str, right: &str) -> bool {
    let left_bytes = left.as_bytes();
    let right_bytes = right.as_bytes();
    if left_bytes.len() != right_bytes.len() {
        return false;
    }

    let mut index = 0;
    while index < left_bytes.len() {
        if left_bytes[index] != right_bytes[index] {
            return false;
        }
        index += 1;
    }

    true
}

#[macro_export]
macro_rules! declare_feature {
    ($name:ident, $id:literal) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name;

        impl $crate::Feature for $name {
            const ID: &'static str = $id;
            const ENTRY: &'static $crate::FeatureEntry = $crate::lookup_feature_const($id);
        }
    };
}

#[macro_export]
macro_rules! cfg_feature {
    ($feature_id:literal => $($tt:tt)*) => {
        const _: () = {
            let entry = $crate::lookup_feature_const($feature_id);
            match entry.state {
                $crate::FeatureState::Implemented
                | $crate::FeatureState::Partial
                | $crate::FeatureState::Experimental => (),
                $crate::FeatureState::Stubbed => {
                    panic!("cfg_feature!: refusing to compile code gated on a stubbed feature")
                }
                $crate::FeatureState::Unsupported => {
                    panic!("cfg_feature!: refusing to compile code gated on an unsupported feature")
                }
            }
        };
        $($tt)*
    };
}
