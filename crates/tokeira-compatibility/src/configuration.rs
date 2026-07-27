//! Checked classification of Temporal's v1.31.0 configuration surface.
//!
//! Temporal source declarations are immutable evidence; Tokeira classifications
//! are owner-authored product decisions. Keeping the two JSON inputs separate
//! prevents a source refresh from silently rewriting the decisions joined here.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const SOURCE_SNAPSHOT: &str = include_str!("../data/temporal-v1.31.0-settings.json");
const CLASSIFICATION_LEDGER: &str = include_str!("../data/temporal-v1.31.0-classification.json");

/// One production `New*Setting` declaration extracted from Temporal source.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct SettingDeclaration {
    /// Exact Temporal dynamic-setting key.
    pub key: String,
    /// Constructor name used by Temporal.
    pub constructor: String,
    /// Constructor-derived selector scope.
    pub scope: TemporalConfigScope,
    /// Constructor-derived value kind.
    pub value_kind: String,
    /// Source-rendered default expression.
    pub default_expression: String,
    /// Repository-relative source anchor.
    pub source: String,
}

/// Selector scope encoded by a Temporal setting constructor.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum TemporalConfigScope {
    /// Process-global setting.
    Global,
    /// Namespace-name-scoped setting.
    Namespace,
    /// Namespace-id-scoped setting.
    NamespaceID,
    /// Task-queue-scoped setting.
    TaskQueue,
    /// Shard-scoped setting.
    ShardID,
    /// Temporal task-type-scoped setting.
    TaskType,
    /// Nexus destination-scoped setting.
    Destination,
    /// CHASM task-type-scoped setting.
    ChasmTaskType,
}

impl TemporalConfigScope {
    /// Stable label used by generated compatibility documentation.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Namespace => "namespace",
            Self::NamespaceID => "namespace-id",
            Self::TaskQueue => "task-queue",
            Self::ShardID => "shard-id",
            Self::TaskType => "task-type",
            Self::Destination => "destination",
            Self::ChasmTaskType => "chasm-task-type",
        }
    }
}

/// Tokeira's primary treatment of a Temporal configuration item.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConfigurationDisposition {
    /// Policy authored through a Temporal public API.
    PublicApiPolicy,
    /// Typed, startup-static Tokeira deployment policy.
    DeploymentPolicy,
    /// Observable behavior fixed to the release profile.
    PinnedBehavioralConstant,
    /// Internal mechanical policy owned by an adaptive/default runtime control.
    AutoTunedMechanicalSetting,
    /// Test-only typed override with no production raw-key surface.
    ConformanceOnlyOverride,
    /// Temporal topology or excluded behavior with no Tokeira control.
    ArchitecturallyIrrelevantOrExcluded,
    /// Explicit Tokeira product extension outside Temporal compatibility.
    TokeiraNativeExtension,
}

impl ConfigurationDisposition {
    /// Stable label used by generated compatibility documentation.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::PublicApiPolicy => "public API policy",
            Self::DeploymentPolicy => "deployment policy",
            Self::PinnedBehavioralConstant => "pinned behavioral constant",
            Self::AutoTunedMechanicalSetting => "auto-tuned mechanical setting",
            Self::ConformanceOnlyOverride => "conformance-only override",
            Self::ArchitecturallyIrrelevantOrExcluded => "architecturally irrelevant or excluded",
            Self::TokeiraNativeExtension => "Tokeira-native extension",
        }
    }
}

/// Relationship between a setting and the conformance override bridge.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConformanceOverrideDisposition {
    /// The setting has no recognized conformance override.
    None,
    /// A real live consult site honors this override.
    Wired,
    /// The value is used by the pure kernel and cannot be mutated live.
    KernelExcluded,
    /// Tokeira does not enforce the setting.
    NotEnforced,
}

impl ConformanceOverrideDisposition {
    /// Stable label used by generated compatibility documentation.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Wired => "wired",
            Self::KernelExcluded => "kernel-excluded",
            Self::NotEnforced => "not-enforced",
        }
    }
}

/// Owner-authored classification of one dynamic setting.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ConfigurationClassification {
    /// Exact Temporal key joined to [`SettingDeclaration::key`].
    pub temporal_key: String,
    /// Human-readable Temporal default, kept equal to the source expression.
    pub temporal_default: String,
    /// Temporal selector scope.
    pub temporal_scope: TemporalConfigScope,
    /// Primary Tokeira disposition.
    pub classification: ConfigurationDisposition,
    /// Exact Tokeira treatment or exclusion explanation.
    pub tokeira_treatment: String,
    /// Owning crate, spec, or architecture record.
    pub owner: String,
    /// Conformance bridge disposition.
    pub conformance_override: ConformanceOverrideDisposition,
    /// Repository-relative verification anchors.
    pub evidence: Vec<String>,
}

/// Classification of a top-level static Temporal server configuration group.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct StaticConfigurationClassification {
    /// YAML group name from `common/config/config.go`.
    pub group: String,
    /// Primary Tokeira disposition.
    pub classification: ConfigurationDisposition,
    /// Exact Tokeira treatment or exclusion explanation.
    pub tokeira_treatment: String,
    /// Owning crate, spec, or architecture record.
    pub owner: String,
    /// Repository-relative verification anchors.
    pub evidence: Vec<String>,
}

/// Complete owner-authored configuration ledger.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ConfigurationLedger {
    /// One classification for every source declaration.
    pub dynamic_settings: Vec<ConfigurationClassification>,
    /// Relevant top-level static configuration groups.
    pub static_groups: Vec<StaticConfigurationClassification>,
}

/// Minimal conformance-registry projection used by the pure verifier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConformanceKey {
    /// Exact Temporal setting key.
    pub key: String,
    /// Registry disposition.
    pub disposition: ConformanceOverrideDisposition,
}

/// Verified, deterministic join consumed by documentation tooling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedConfigurationLedger {
    /// Dynamic declarations joined in key order.
    pub dynamic_settings: Vec<(SettingDeclaration, ConfigurationClassification)>,
    /// Static groups ordered by group name.
    pub static_groups: Vec<StaticConfigurationClassification>,
    /// Counts by primary disposition.
    pub disposition_counts: BTreeMap<ConfigurationDisposition, usize>,
}

/// Why source or classification data failed verification.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ConfigurationLedgerError {
    /// Checked JSON cannot be decoded.
    #[error("invalid checked configuration JSON: {0}")]
    InvalidJson(String),
    /// A source declaration key appears more than once.
    #[error("duplicate source setting: {0}")]
    DuplicateSource(String),
    /// A classification key appears more than once.
    #[error("duplicate setting classification: {0}")]
    DuplicateClassification(String),
    /// A denominator key has no classification.
    #[error("missing setting classification: {0}")]
    MissingClassification(String),
    /// A classification does not exist in the source denominator.
    #[error("unknown setting classification: {0}")]
    UnknownClassification(String),
    /// A record contains absent or invalid metadata.
    #[error("invalid metadata for {key}: {reason}")]
    InvalidMetadata {
        /// Setting or group identity.
        key: String,
        /// Failed invariant.
        reason: &'static str,
    },
    /// A classification conflicts with the conformance override registry.
    #[error("conformance disposition mismatch for {key}: expected {expected:?}, got {actual:?}")]
    ConformanceMismatch {
        /// Temporal setting key.
        key: String,
        /// Registry-derived disposition.
        expected: ConformanceOverrideDisposition,
        /// Ledger disposition.
        actual: ConformanceOverrideDisposition,
    },
    /// A conformance key is absent from the Temporal denominator.
    #[error("conformance key is absent from the Temporal denominator: {0}")]
    UnknownConformanceKey(String),
}

/// Decode the checked Temporal source snapshot.
pub fn source_snapshot() -> Result<Vec<SettingDeclaration>, ConfigurationLedgerError> {
    serde_json::from_str(SOURCE_SNAPSHOT)
        .map_err(|error| ConfigurationLedgerError::InvalidJson(error.to_string()))
}

/// Decode the checked owner-authored classification ledger.
pub fn classification_ledger() -> Result<ConfigurationLedger, ConfigurationLedgerError> {
    serde_json::from_str(CLASSIFICATION_LEDGER)
        .map_err(|error| ConfigurationLedgerError::InvalidJson(error.to_string()))
}

/// Decode and verify the checked source/classification join.
///
/// The production crate deliberately does not depend on the conformance
/// registry. Its tests independently prove that every non-`none` disposition
/// here agrees with that registry; documentation tooling can therefore consume
/// this verified join without pulling test-only raw-key machinery into a
/// production dependency graph.
pub fn checked_configuration_ledger()
-> Result<VerifiedConfigurationLedger, ConfigurationLedgerError> {
    let declarations = source_snapshot()?;
    let ledger = classification_ledger()?;
    let conformance_keys = ledger
        .dynamic_settings
        .iter()
        .filter(|entry| entry.conformance_override != ConformanceOverrideDisposition::None)
        .map(|entry| ConformanceKey {
            key: entry.temporal_key.clone(),
            disposition: entry.conformance_override,
        })
        .collect::<Vec<_>>();
    verify_configuration_ledger(&declarations, &ledger, &conformance_keys)
}

/// Verify and deterministically join source declarations and owner decisions.
pub fn verify_configuration_ledger(
    declarations: &[SettingDeclaration],
    ledger: &ConfigurationLedger,
    conformance_keys: &[ConformanceKey],
) -> Result<VerifiedConfigurationLedger, ConfigurationLedgerError> {
    let mut source = BTreeMap::new();
    for declaration in declarations {
        validate_source(declaration)?;
        if source
            .insert(declaration.key.clone(), declaration.clone())
            .is_some()
        {
            return Err(ConfigurationLedgerError::DuplicateSource(
                declaration.key.clone(),
            ));
        }
    }

    let mut classifications = BTreeMap::new();
    for classification in &ledger.dynamic_settings {
        validate_classification(classification)?;
        if classifications
            .insert(classification.temporal_key.clone(), classification.clone())
            .is_some()
        {
            return Err(ConfigurationLedgerError::DuplicateClassification(
                classification.temporal_key.clone(),
            ));
        }
    }

    for key in source.keys() {
        if !classifications.contains_key(key) {
            return Err(ConfigurationLedgerError::MissingClassification(key.clone()));
        }
    }
    for key in classifications.keys() {
        if !source.contains_key(key) {
            return Err(ConfigurationLedgerError::UnknownClassification(key.clone()));
        }
    }

    let conformance = conformance_map(conformance_keys, &source)?;
    let mut dynamic_settings = Vec::with_capacity(source.len());
    let mut disposition_counts = BTreeMap::new();
    for (key, declaration) in source {
        let classification = classifications
            .remove(&key)
            .expect("exact key-set equality established above");
        if declaration.scope != classification.temporal_scope {
            return Err(ConfigurationLedgerError::InvalidMetadata {
                key,
                reason: "scope differs from source declaration",
            });
        }
        if declaration.default_expression != classification.temporal_default {
            return Err(ConfigurationLedgerError::InvalidMetadata {
                key,
                reason: "default differs from source declaration",
            });
        }
        let expected = conformance
            .get(&declaration.key)
            .copied()
            .unwrap_or(ConformanceOverrideDisposition::None);
        if classification.conformance_override != expected {
            return Err(ConfigurationLedgerError::ConformanceMismatch {
                key: declaration.key,
                expected,
                actual: classification.conformance_override,
            });
        }
        *disposition_counts
            .entry(classification.classification)
            .or_insert(0) += 1;
        dynamic_settings.push((declaration, classification));
    }

    let mut static_groups = ledger.static_groups.clone();
    static_groups.sort_by(|left, right| left.group.cmp(&right.group));
    let mut seen_groups = BTreeSet::new();
    for group in &static_groups {
        validate_static_group(group)?;
        if !seen_groups.insert(group.group.as_str()) {
            return Err(ConfigurationLedgerError::InvalidMetadata {
                key: group.group.clone(),
                reason: "duplicate static group",
            });
        }
        *disposition_counts.entry(group.classification).or_insert(0) += 1;
    }

    Ok(VerifiedConfigurationLedger {
        dynamic_settings,
        static_groups,
        disposition_counts,
    })
}

fn validate_source(declaration: &SettingDeclaration) -> Result<(), ConfigurationLedgerError> {
    if declaration.key.trim().is_empty()
        || declaration.constructor.trim().is_empty()
        || declaration.value_kind.trim().is_empty()
        || declaration.default_expression.trim().is_empty()
        || !is_repository_relative_evidence(&declaration.source)
    {
        return Err(ConfigurationLedgerError::InvalidMetadata {
            key: declaration.key.clone(),
            reason: "source declaration is incomplete or not repository-relative",
        });
    }
    Ok(())
}

fn validate_classification(
    classification: &ConfigurationClassification,
) -> Result<(), ConfigurationLedgerError> {
    if classification.temporal_key.trim().is_empty()
        || classification.temporal_default.trim().is_empty()
        || classification.tokeira_treatment.trim().is_empty()
        || classification.owner.trim().is_empty()
        || classification.evidence.is_empty()
        || !classification
            .evidence
            .iter()
            .all(|value| is_repository_relative_evidence(value))
    {
        return Err(ConfigurationLedgerError::InvalidMetadata {
            key: classification.temporal_key.clone(),
            reason: "classification metadata is incomplete or not repository-relative",
        });
    }
    Ok(())
}

fn validate_static_group(
    group: &StaticConfigurationClassification,
) -> Result<(), ConfigurationLedgerError> {
    if group.group.trim().is_empty()
        || group.tokeira_treatment.trim().is_empty()
        || group.owner.trim().is_empty()
        || group.evidence.is_empty()
        || !group
            .evidence
            .iter()
            .all(|value| is_repository_relative_evidence(value))
    {
        return Err(ConfigurationLedgerError::InvalidMetadata {
            key: group.group.clone(),
            reason: "static-group metadata is incomplete or not repository-relative",
        });
    }
    Ok(())
}

fn is_repository_relative_evidence(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty()
        && !trimmed.starts_with('/')
        && !trimmed.contains("\\")
        && !trimmed.contains("..")
}

fn conformance_map(
    keys: &[ConformanceKey],
    source: &BTreeMap<String, SettingDeclaration>,
) -> Result<BTreeMap<String, ConformanceOverrideDisposition>, ConfigurationLedgerError> {
    let mut mapped = BTreeMap::new();
    for key in keys {
        let canonical = source
            .keys()
            .find(|candidate| candidate.eq_ignore_ascii_case(&key.key))
            .ok_or_else(|| ConfigurationLedgerError::UnknownConformanceKey(key.key.clone()))?
            .clone();
        if mapped.insert(canonical, key.disposition).is_some() {
            return Err(ConfigurationLedgerError::InvalidMetadata {
                key: key.key.clone(),
                reason: "duplicate conformance key",
            });
        }
    }
    Ok(mapped)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn checked_conformance_keys() -> Vec<ConformanceKey> {
        tokeira_conformance::KEY_CLASSIFICATION
            .iter()
            .map(|spec| ConformanceKey {
                key: spec.key.to_owned(),
                disposition: match spec.disposition {
                    tokeira_conformance::Disposition::Wired => {
                        ConformanceOverrideDisposition::Wired
                    }
                    tokeira_conformance::Disposition::KernelExcluded => {
                        ConformanceOverrideDisposition::KernelExcluded
                    }
                    tokeira_conformance::Disposition::NotEnforced => {
                        ConformanceOverrideDisposition::NotEnforced
                    }
                },
            })
            .collect()
    }

    #[test]
    fn checked_ledger_is_complete_and_source_aware() {
        let declarations = source_snapshot().expect("checked source snapshot");
        let ledger = classification_ledger().expect("checked classification ledger");
        let verified =
            verify_configuration_ledger(&declarations, &ledger, &checked_conformance_keys())
                .expect("complete checked ledger");

        assert_eq!(verified.dynamic_settings.len(), 613);
        assert_eq!(
            verified
                .dynamic_settings
                .iter()
                .filter(|(setting, _)| setting
                    .source
                    .starts_with("common/dynamicconfig/constants.go:"))
                .count(),
            565
        );
        for key in [
            "activity.enableStandalone",
            "matching.enableFairness",
            "matching.priorityLevels",
            "matching.useNewMatcher",
        ] {
            assert!(
                verified
                    .dynamic_settings
                    .iter()
                    .any(|(setting, _)| setting.key == key)
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: configuration-policy, Property 3: source denominator determinism
        #[test]
        fn source_denominator_determinism(
            mut keys in prop::collection::btree_set("[a-z]{1,8}", 1..40)
                .prop_map(|keys| keys.into_iter().collect::<Vec<_>>()),
            rotate in any::<usize>(),
        ) {
            let declarations = keys
                .iter()
                .map(|key| SettingDeclaration {
                    key: key.clone(),
                    constructor: "NewGlobalBoolSetting".to_owned(),
                    scope: TemporalConfigScope::Global,
                    value_kind: "Bool".to_owned(),
                    default_expression: "false".to_owned(),
                    source: format!("common/dynamicconfig/constants.go:{}", key.len()),
                })
                .collect::<Vec<_>>();
            let expected = declarations
                .iter()
                .map(|entry| entry.key.clone())
                .collect::<BTreeSet<_>>();

            let length = keys.len();
            keys.rotate_left(rotate % length);
            let permuted = keys
                .into_iter()
                .map(|key| declarations
                    .iter()
                    .find(|entry| entry.key == key)
                    .expect("generated key exists")
                    .clone())
                .collect::<Vec<_>>();
            let normalized = permuted
                .iter()
                .map(|entry| entry.key.clone())
                .collect::<BTreeSet<_>>();
            prop_assert_eq!(normalized, expected);

            let mut invalid = declarations[0].clone();
            invalid.source = "/absolute/source.go:1".to_owned();
            prop_assert!(validate_source(&invalid).is_err());
        }

        // Feature: configuration-policy, Property 4: classification-ledger exactness
        #[test]
        fn classification_ledger_exactness(mutation in 0_u8..6) {
            let declaration = SettingDeclaration {
                key: "test.setting".to_owned(),
                constructor: "NewGlobalBoolSetting".to_owned(),
                scope: TemporalConfigScope::Global,
                value_kind: "Bool".to_owned(),
                default_expression: "false".to_owned(),
                source: "common/dynamicconfig/constants.go:1".to_owned(),
            };
            let classification = ConfigurationClassification {
                temporal_key: declaration.key.clone(),
                temporal_default: declaration.default_expression.clone(),
                temporal_scope: declaration.scope,
                classification: ConfigurationDisposition::PinnedBehavioralConstant,
                tokeira_treatment: "Pinned to the v1.31.0 default.".to_owned(),
                owner: "crates/tokeira-runtime".to_owned(),
                conformance_override: ConformanceOverrideDisposition::None,
                evidence: vec![declaration.source.clone()],
            };
            let mut declarations = vec![declaration];
            let mut ledger = ConfigurationLedger {
                dynamic_settings: vec![classification],
                static_groups: Vec::new(),
            };
            let mut conformance = Vec::new();
            match mutation {
                0 => {}
                1 => ledger.dynamic_settings.clear(),
                2 => ledger.dynamic_settings.push(ledger.dynamic_settings[0].clone()),
                3 => ledger.dynamic_settings[0].temporal_key = "invented".to_owned(),
                4 => ledger.dynamic_settings[0].owner.clear(),
                5 => {
                    ledger.dynamic_settings[0].conformance_override =
                        ConformanceOverrideDisposition::Wired;
                    conformance.push(ConformanceKey {
                        key: "test.setting".to_owned(),
                        disposition: ConformanceOverrideDisposition::KernelExcluded,
                    });
                }
                _ => unreachable!(),
            }
            let result = verify_configuration_ledger(
                &declarations,
                &ledger,
                &conformance,
            );
            prop_assert_eq!(result.is_ok(), mutation == 0);

            declarations.clear();
        }
    }
}
