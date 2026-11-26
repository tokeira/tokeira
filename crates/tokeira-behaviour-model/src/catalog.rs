//! Evidence validation and explicit coverage strength for pure decisions.
//!
//! The checked-in index contains hashes, not upstream implementation text. CI can
//! resolve every citation without access to the reference checkout. Regeneration
//! is a separate local operation so model evaluation remains pure.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Behavioural authority for this catalog, independent of the vendored API version.
pub const TAG: &str = "v1.31.0";
/// Resolved annotated-tag commit read from the reference repository.
pub const COMMIT: &str = "83881961df2bad7d78d93a2d50778dbc8bd1a2fc";

/// Inclusive, one-based source evidence for one decision row.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Citation {
    /// Function whose behaviour justifies the row.
    pub function: String,
    /// Repository-relative source path.
    pub path: String,
    /// First included line, starting at one.
    pub start: usize,
    /// Last included line.
    pub end: usize,
    /// Immutable release tag.
    pub tag: String,
    /// SHA-256 of the complete source file's exact bytes.
    pub source_digest: String,
    /// SHA-256 of the inclusive lines, retaining their newline bytes.
    pub range_digest: String,
}

/// A branch and its independent source evidence.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionRow {
    /// Guard and resulting output, in evaluation order.
    pub transition: String,
    /// Every row must have at least one supporting range.
    pub citations: Vec<Citation>,
}

/// Calibration state cannot be inferred from passing implementation tests.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Evidence {
    /// A recording can reach the rule, but no corpus has calibrated it.
    AwaitingCalibration,
    /// Recordings cannot select this rule's exact input.
    SourceOnly {
        /// Concrete reason recording cannot reach the domain.
        reason: String,
    },
    /// Independent corpus recordings have calibrated the rule.
    Calibrated {
        /// Stable keys of the successful calibration recordings.
        corpus_cases: Vec<String>,
    },
}

/// Complete contract of an independently sourced decision.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    /// Stable identity used by comparisons and evidence.
    pub key: String,
    /// Public behaviour area.
    pub area: String,
    /// Domain on which the decision is total.
    pub input_domain: String,
    /// Assumptions enforced before invoking the decision.
    pub precondition: String,
    /// Observable result, events, or next state.
    pub observable_outputs: String,
    /// Ordered branches with evidence for each.
    pub rows: Vec<DecisionRow>,
    /// Calibration strength, separate from implementation agreement.
    pub evidence: Evidence,
}

/// File-level source evidence and all cited ranges within it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceFile {
    /// Number of source lines at the pinned tag.
    pub line_count: usize,
    /// SHA-256 over the complete file bytes.
    pub digest: String,
    /// Inclusive `start-end` keys mapped to SHA-256 over exact line bytes.
    pub ranges: BTreeMap<String, String>,
}

/// Source evidence generated locally; CI never reads the reference repository.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceIndex {
    /// Source-index format version.
    pub version: u32,
    /// Pinned release tag.
    pub tag: String,
    /// Commit to which the tag resolved during generation.
    pub commit: String,
    /// Cited source files indexed by repository-relative path.
    pub files: BTreeMap<String, SourceFile>,
}

/// A catalog is rejected as a whole if any row lacks valid independent evidence.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleCatalog {
    /// Source-derived rules with unique keys.
    pub rules: Vec<Rule>,
}

/// Evidence or dependency contract violation.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid model evidence: {0}")]
pub struct ValidationError(pub String);

fn invalid(message: &str) -> ValidationError {
    ValidationError(message.to_owned())
}
fn digest_valid(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit())
}

impl RuleCatalog {
    /// Decode the checked-in rules, without touching the filesystem.
    pub fn checked_in() -> Self {
        serde_json::from_str(include_str!("../rules.json"))
            .expect("checked-in rule catalog is valid JSON")
    }

    /// Resolve all rows against the pinned index; duplicates and missing evidence fail.
    pub fn validate(&self, index: &SourceIndex) -> Result<(), ValidationError> {
        if index.version != 1 || index.tag != TAG || index.commit != COMMIT {
            return Err(invalid("wrong index version, tag, or commit"));
        }
        let mut keys = BTreeSet::new();
        for rule in &self.rules {
            if rule.key.is_empty() || !keys.insert(&rule.key) {
                return Err(invalid("empty or duplicate rule key"));
            }
            if [
                &rule.area,
                &rule.input_domain,
                &rule.precondition,
                &rule.observable_outputs,
            ]
            .iter()
            .any(|s| s.is_empty())
                || rule.rows.is_empty()
            {
                return Err(invalid("incomplete rule contract"));
            }
            match &rule.evidence {
                Evidence::SourceOnly { reason } if reason.is_empty() => {
                    return Err(invalid("source-only reason missing"));
                }
                Evidence::Calibrated { corpus_cases }
                    if corpus_cases.is_empty() || corpus_cases.iter().any(String::is_empty) =>
                {
                    return Err(invalid("calibration case keys missing"));
                }
                _ => {}
            }
            for row in &rule.rows {
                if row.transition.is_empty() || row.citations.is_empty() {
                    return Err(invalid("missing decision-row citation"));
                }
                for citation in &row.citations {
                    let file = index
                        .files
                        .get(&citation.path)
                        .ok_or_else(|| invalid("unindexed source file"))?;
                    if citation.function.is_empty()
                        || citation.tag != TAG
                        || citation.start == 0
                        || citation.end < citation.start
                        || citation.end > file.line_count
                    {
                        return Err(invalid("invalid citation function, tag, or range"));
                    }
                    if !digest_valid(&citation.source_digest)
                        || file.digest != citation.source_digest
                        || !digest_valid(&citation.range_digest)
                        || file
                            .ranges
                            .get(&format!("{}-{}", citation.start, citation.end))
                            != Some(&citation.range_digest)
                    {
                        return Err(invalid("source digest mismatch"));
                    }
                }
            }
        }
        Ok(())
    }

    /// Coverage of a decision in its declared domain; unknown or provisional rules
    /// are invariants-only even if implementation comparisons pass.
    pub fn supports(&self, key: &str) -> Support {
        match self
            .rules
            .iter()
            .find(|r| r.key == key)
            .map(|r| &r.evidence)
        {
            Some(Evidence::SourceOnly { .. }) => Support::SourceOnly,
            Some(Evidence::Calibrated { corpus_cases }) if !corpus_cases.is_empty() => {
                Support::Calibrated
            }
            _ => Support::InvariantsOnly,
        }
    }
}

impl SourceIndex {
    /// Decode immutable embedded source evidence.
    pub fn checked_in() -> Self {
        serde_json::from_str(include_str!("../source-index.json"))
            .expect("checked-in source index is valid JSON")
    }
}

/// Strength of an observation comparison.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Support {
    /// The independent corpus calibrated the applicable decision.
    Calibrated,
    /// The exact domain is supported only by cited source.
    SourceOnly,
    /// No calibrated or source-only decision applies.
    InvariantsOnly,
}

/// Result of exact comparison, retaining the distinction between equality and evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Comparison {
    /// First unequal observation, including an extra or missing observation.
    Diverged(usize),
    /// Exact equality with the stated evidence strength.
    Agreement(Support),
    /// Equality carries no model agreement without adequate coverage.
    InvariantsOnly,
}

/// Locate the first exact divergence without normalizing IDs, times, or payloads.
pub fn compare_exact<T: PartialEq>(expected: &[T], actual: &[T], support: Support) -> Comparison {
    if let Some(index) = expected.iter().zip(actual).position(|(a, b)| a != b) {
        return Comparison::Diverged(index);
    }
    if expected.len() != actual.len() {
        return Comparison::Diverged(expected.len().min(actual.len()));
    }
    if support == Support::InvariantsOnly {
        Comparison::InvariantsOnly
    } else {
        Comparison::Agreement(support)
    }
}

/// Validate the transitive dependency closure, including development/build edges.
///
/// Package IDs, rather than import aliases, must identify nodes. Callers resolve
/// those IDs to package names before constructing this graph.
pub fn validate_dependencies(
    root: &str,
    graph: &BTreeMap<String, Vec<String>>,
) -> Result<(), ValidationError> {
    let mut pending = vec![root.to_owned()];
    let mut seen = BTreeSet::new();
    while let Some(name) = pending.pop() {
        if !seen.insert(name.clone()) {
            continue;
        }
        if [
            "tokeira-kernel",
            "tokeira-runtime",
            "tokeira-edge",
            "tokeira-storage",
            "tokeira-compatibility",
        ]
        .iter()
        .any(|prefix| name == *prefix || name.starts_with(&format!("{prefix}-")))
        {
            return Err(invalid(&format!("forbidden dependency {name}")));
        }
        if let Some(dependencies) = graph.get(&name) {
            pending.extend(dependencies.iter().cloned());
        }
    }
    Ok(())
}
