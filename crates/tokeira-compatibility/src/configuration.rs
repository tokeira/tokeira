//! Checked classification of Temporal's v1.32.0 target configuration surface.
//!
//! Temporal source declarations are immutable evidence; Tokeira classifications
//! are owner-authored product decisions. Keeping the two JSON inputs separate
//! prevents a source refresh from silently rewriting the decisions joined here.
//! Retired keys preserve migration evidence and any still-live conformance
//! overrides, but never contribute to the target release's denominator.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const SOURCE_SNAPSHOT: &str = include_str!("../data/temporal-v1.32.0-settings.json");
const CLASSIFICATION_LEDGER: &str = include_str!("../data/temporal-v1.32.0-classification.json");

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
    /// Release that introduced this key relative to the previous inventory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub added_in: Option<String>,
    /// Release that removed this key; only valid in the retired collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub removed_in: Option<String>,
    /// Previous keys consolidated or renamed into this declaration.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub renamed_from: Vec<String>,
    /// Previous source expression; the current expression stays in `temporal_default`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_changed_from: Option<String>,
    /// Migration context, including effective values when expressions are symbolic.
    /// The `owner` identifies the delta spec responsible for changed defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_notes: Option<String>,
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
    /// Historical keys absent from the target denominator, including migration notes.
    pub removed_settings: Vec<ConfigurationClassification>,
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
    /// Verified retired keys, sorted separately and excluded from disposition counts.
    pub removed_settings: Vec<ConfigurationClassification>,
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
        .chain(&ledger.removed_settings)
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
        if classification.removed_in.is_some() {
            return Err(ConfigurationLedgerError::InvalidMetadata {
                key: classification.temporal_key.clone(),
                reason: "retired key appears in the target denominator",
            });
        }
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

    let mut removed = BTreeMap::new();
    for classification in &ledger.removed_settings {
        validate_classification(classification)?;
        let key = &classification.temporal_key;
        if classification.removed_in.is_none()
            || classification.change_notes.is_none()
            || source.contains_key(key)
        {
            return Err(ConfigurationLedgerError::InvalidMetadata {
                key: key.clone(),
                reason: "retired key needs removal evidence and must be absent from the target",
            });
        }
        if removed
            .insert(key.clone(), classification.clone())
            .is_some()
        {
            return Err(ConfigurationLedgerError::DuplicateClassification(
                key.clone(),
            ));
        }
    }
    for classification in classifications.values() {
        for previous in &classification.renamed_from {
            if !removed.contains_key(previous) {
                return Err(ConfigurationLedgerError::InvalidMetadata {
                    key: classification.temporal_key.clone(),
                    reason: "rename refers to a key without a retirement record",
                });
            }
        }
    }

    // The target snapshot stays exact even while delta specs still own old
    // consult sites (callback policy and reactivation TTL in v1.31.0). A retired
    // override must match the real registry too; it is not an unknown-key bypass.
    let known_keys = source.keys().chain(removed.keys()).cloned().collect();
    let conformance = conformance_map(conformance_keys, &known_keys)?;
    for classification in removed.values() {
        validate_conformance(classification, &conformance)?;
    }
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
        validate_conformance(&classification, &conformance)?;
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
        removed_settings: removed.into_values().collect(),
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
        || [
            &classification.added_in,
            &classification.removed_in,
            &classification.default_changed_from,
            &classification.change_notes,
        ]
        .into_iter()
        .flatten()
        .any(|value| value.trim().is_empty())
        || classification
            .renamed_from
            .iter()
            .any(|key| key.trim().is_empty())
    {
        return Err(ConfigurationLedgerError::InvalidMetadata {
            key: classification.temporal_key.clone(),
            reason: "classification metadata is incomplete or not repository-relative",
        });
    }
    if let Some(previous) = &classification.default_changed_from
        && (previous == &classification.temporal_default
            || classification.change_notes.is_none()
            || !classification.owner.starts_with(".kiro/specs/v132-"))
    {
        return Err(ConfigurationLedgerError::InvalidMetadata {
            key: classification.temporal_key.clone(),
            reason: "changed default needs distinct values, migration notes, and a delta-spec owner",
        });
    }
    Ok(())
}

fn validate_conformance(
    classification: &ConfigurationClassification,
    conformance: &BTreeMap<String, ConformanceOverrideDisposition>,
) -> Result<(), ConfigurationLedgerError> {
    let expected = conformance
        .get(&classification.temporal_key)
        .copied()
        .unwrap_or(ConformanceOverrideDisposition::None);
    if classification.conformance_override != expected {
        return Err(ConfigurationLedgerError::ConformanceMismatch {
            key: classification.temporal_key.clone(),
            expected,
            actual: classification.conformance_override,
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
    known_keys: &BTreeSet<String>,
) -> Result<BTreeMap<String, ConformanceOverrideDisposition>, ConfigurationLedgerError> {
    let mut mapped = BTreeMap::new();
    for key in keys {
        let canonical = known_keys
            .iter()
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

        assert_eq!(verified.dynamic_settings.len(), 683);
        assert_eq!(
            verified
                .dynamic_settings
                .iter()
                .filter(|(setting, _)| setting
                    .source
                    .starts_with("common/dynamicconfig/constants.go:"))
                .count(),
            627
        );
        for key in [
            "activity.enableStandalone",
            "activity.startDelayEnabled",
            "history.enableStandaloneActivityOperatorCommands",
            "nexusoperation.enableStandalone",
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

    #[test]
    fn migration_records_have_target_evidence_and_existing_owners() {
        let ledger = classification_ledger().expect("checked classification ledger");
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        assert_eq!(ledger.removed_settings.len(), 14);
        assert_eq!(
            ledger
                .dynamic_settings
                .iter()
                .filter(|entry| entry.added_in.is_some())
                .count(),
            84
        );
        assert_eq!(
            ledger
                .dynamic_settings
                .iter()
                .filter(|entry| entry.default_changed_from.is_some())
                .count(),
            12
        );
        for entry in &ledger.dynamic_settings {
            assert!(
                entry
                    .evidence
                    .iter()
                    .any(|anchor| anchor.ends_with(" @ v1.32.0"))
            );
            assert!(
                root.join(&entry.owner).exists(),
                "missing owner for {}",
                entry.temporal_key
            );
            if let Some(added) = &entry.added_in {
                assert_eq!(added, "v1.32.0");
            }
        }
        for entry in &ledger.removed_settings {
            assert_eq!(entry.removed_in.as_deref(), Some("v1.32.0"));
            assert!(root.join(&entry.owner).exists());
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Input order cannot change the checked target join or canonical snapshot bytes.
        // Feature: configuration-policy, Property 3: source denominator determinism
        // Feature: temporal-v1.32-compatibility, Property 8: denominator exactness at v1.32.0
        #[test]
        fn source_denominator_determinism(
            source_order in prop::collection::vec(any::<u64>(), 683),
            ledger_order in prop::collection::vec(any::<u64>(), 683),
        ) {
            let mut declarations = source_snapshot().unwrap();
            let mut ledger = classification_ledger().unwrap();
            let conformance = checked_conformance_keys();
            let expected = verify_configuration_ledger(&declarations, &ledger, &conformance).unwrap();
            let priorities = declarations.iter().enumerate()
                .map(|(index, entry)| (entry.key.clone(), (source_order[index], ledger_order[index])))
                .collect::<BTreeMap<_, _>>();
            declarations.sort_by_key(|entry| priorities[&entry.key].0);
            ledger.dynamic_settings.sort_by_key(|entry| priorities[&entry.temporal_key].1);
            ledger.removed_settings.reverse();
            ledger.static_groups.reverse();
            prop_assert_eq!(verify_configuration_ledger(&declarations, &ledger, &conformance).unwrap(), expected);

            declarations.sort_by(|left, right| left.key.cmp(&right.key));
            let encoded = serde_json::to_string_pretty(&declarations).unwrap() + "\n";
            prop_assert_eq!(encoded, SOURCE_SNAPSHOT);
        }

        // Every drift mutation is rejected, including retired keys still present in the bridge.
        // Feature: configuration-policy, Property 4: classification-ledger exactness
        // Feature: temporal-v1.32-compatibility, Property 8: denominator exactness at v1.32.0
        #[test]
        fn classification_ledger_exactness(index in 0_usize..683, mutation in 0_u8..18) {
            let mut declarations = source_snapshot().unwrap();
            let mut ledger = classification_ledger().unwrap();
            let mut conformance = checked_conformance_keys();
            match mutation {
                0 => {}
                1 => { declarations.remove(index); }
                2 => declarations.push(declarations[index].clone()),
                3 => declarations[index].key = "invented.target.key".to_owned(),
                4 => declarations[index].default_expression.push_str(" changed"),
                5 => { ledger.dynamic_settings.remove(index); }
                6 => ledger.dynamic_settings.push(ledger.dynamic_settings[index].clone()),
                7 => ledger.dynamic_settings[index].temporal_key = "invented.ledger.key".to_owned(),
                8 => ledger.dynamic_settings[index].owner.clear(),
                9 => ledger.dynamic_settings[index].evidence.clear(),
                10 => declarations[index].source = "/absolute/source.go:1".to_owned(),
                11 => ledger.dynamic_settings[index].removed_in = Some("v1.32.0".to_owned()),
                12 => ledger.removed_settings.push(ledger.removed_settings[0].clone()),
                13 => ledger.removed_settings[0].removed_in = None,
                14 => {
                    let entry = ledger.removed_settings.iter_mut()
                        .find(|entry| entry.conformance_override == ConformanceOverrideDisposition::Wired).unwrap();
                    entry.conformance_override = ConformanceOverrideDisposition::None;
                }
                15 => conformance.push(ConformanceKey {
                    key: "unknown.bridge.key".to_owned(),
                    disposition: ConformanceOverrideDisposition::Wired,
                }),
                16 => {
                    let entry = ledger.dynamic_settings.iter_mut()
                        .find(|entry| entry.default_changed_from.is_some()).unwrap();
                    entry.change_notes = None;
                }
                17 => {
                    let entry = ledger.dynamic_settings.iter_mut()
                        .find(|entry| !entry.renamed_from.is_empty()).unwrap();
                    entry.renamed_from.push("unknown.previous.key".to_owned());
                }
                _ => unreachable!(),
            }
            let result = verify_configuration_ledger(&declarations, &ledger, &conformance);
            prop_assert_eq!(result.is_ok(), mutation == 0, "{:?}", result);
        }
    }
}
