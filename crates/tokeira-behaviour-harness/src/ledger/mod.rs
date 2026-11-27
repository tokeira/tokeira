//! Reproducible decision, RPC, and area evidence, with explicit gaps.
//!
//! Hand-maintained claims and run observations are separate. Rendering can show
//! stale data safely, but check mode rejects it. No decision result supplies RPC
//! scenario coverage or calibrates the independent catalog.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokeira_behaviour_model::catalog::{Rule, RuleCatalog};

mod check;
mod junit;
mod render;
mod surface;

pub use check::{check_profile_filter, check_sources, comparison_filter};
pub use junit::record_run;
pub use render::{refresh_page, render_page};

#[cfg(test)]
mod tests;

/// An invalid ledger identifies the record or input needing correction.
#[derive(Debug, thiserror::Error)]
#[error("invalid behavioural ledger: {0}")]
pub struct LedgerError(pub String);

pub(crate) fn invalid(message: impl Into<String>) -> LedgerError {
    LedgerError(message.into())
}

/// Maintained disposition, subject to executed evidence before publication.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum Verdict {
    /// Every named comparison must have passed.
    #[serde(rename = "agrees")]
    Agrees,
    /// Every named comparison must have passed after the recorded correction.
    #[serde(rename = "differed and fixed")]
    DifferedAndFixed,
    /// Every named comparison must be ignored for the stated reason and have failed.
    #[serde(rename = "differs, ignored with reason")]
    IgnoredDifference,
    /// No agreement is claimed, even if a test happened to pass.
    #[serde(rename = "not yet tested")]
    NotYetTested,
}

impl Verdict {
    /// Exact public vocabulary, shared by data and generated cells.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Agrees => "agrees",
            Self::DifferedAndFixed => "differed and fixed",
            Self::IgnoredDifference => "differs, ignored with reason",
            Self::NotYetTested => "not yet tested",
        }
    }
}

/// One observed test result; skipped and absent are never execution evidence.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Outcome {
    /// The comparison completed successfully.
    Passed,
    /// The comparison executed and failed its assertions.
    Failed,
    /// The test could not complete normally; this does not confirm a difference.
    Error,
    /// The report explicitly skipped this test.
    Skipped,
    /// The report contained no matching test identity.
    Missing,
}

impl Outcome {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Error => "error",
            Self::Skipped => "skipped",
            Self::Missing => "missing",
        }
    }
}

/// A repository-relative implementation location, maintained independently of runs.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Counterpart {
    /// Source file in this repository.
    pub path: String,
    /// Qualified item or match arm owning the decision.
    pub item: String,
}

/// A library-test identity and the source file containing its declaration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComparisonTest {
    /// Cargo package and nextest library binary ID.
    pub crate_name: String,
    /// Repository-relative Rust file containing the test.
    pub file: String,
    /// Fully qualified libtest name, including modules.
    pub name: String,
}

impl ComparisonTest {
    /// Exact JUnit suite-name plus testcase-name identity; no suffix matching.
    pub fn identity(&self) -> String {
        format!("{}::{}", self.crate_name, self.name)
    }
}

/// Facts replaced together by write mode; never inferred from registration.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunEvidence {
    /// SHA-256 of the typed catalog rule at the time the report was imported.
    pub rule_digest: String,
    /// SHA-256 of the exact JUnit bytes, without machine-local paths or timestamps.
    pub junit_digest: String,
    /// Outcome for every named comparison, including tests absent from the report.
    pub tests: BTreeMap<String, Outcome>,
}

/// Maintained decision facts and the most recently imported run.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionEvidence {
    /// Stable catalog key; exactly one record is required per rule.
    pub rule: String,
    /// Implementation locations, not model functions.
    pub counterparts: Vec<Counterpart>,
    /// Comparisons against the independent model, not model self-tests.
    pub tests: Vec<ComparisonTest>,
    /// Fully qualified wire method paths; empty for background-only decisions.
    pub rpcs: Vec<String>,
    /// Maintained disposition; rendering additionally requires run evidence.
    pub verdict: Verdict,
    /// Explanation of the disposition, including any untested branches.
    pub reason: String,
    /// Correction commit, required only for `differed and fixed`.
    pub fix_commit: Option<String>,
    /// Only write mode creates or replaces these observations.
    pub run: Option<RunEvidence>,
}

/// Versioned, strict evidence input; unknown fields are rejected.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Ledger {
    /// Currently one; future schema changes require an explicit migration.
    pub version: u32,
    /// Exactly one record for each current catalog rule.
    pub decisions: Vec<DecisionEvidence>,
}

/// Hash the complete typed rule, including ordered branches and source digests.
/// JSON whitespace changes alone do not invalidate evidence.
pub fn rule_digest(rule: &Rule) -> String {
    digest(&serde_json::to_vec(rule).expect("catalog rule serializes"))
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

impl Ledger {
    /// Decode evidence without filesystem access; validation is explicit.
    pub fn from_json(json: &str) -> Result<Self, LedgerError> {
        serde_json::from_str(json).map_err(|error| invalid(error.to_string()))
    }

    /// Stable serialization sorts records and unordered maintained references.
    pub fn to_json(&self) -> String {
        let mut sorted = self.clone();
        sorted.decisions.sort_by(|a, b| a.rule.cmp(&b.rule));
        for decision in &mut sorted.decisions {
            decision.tests.sort_by_key(ComparisonTest::identity);
            decision
                .counterparts
                .sort_by(|a, b| (&a.path, &a.item).cmp(&(&b.path, &b.item)));
            decision.rpcs.sort();
        }
        format!(
            "{}\n",
            serde_json::to_string_pretty(&sorted).expect("ledger serializes")
        )
    }

    /// Reject missing, duplicate, unknown, or incoherent records. Staleness is
    /// checked separately so rendering can retain stale evidence as an explicit gap.
    pub fn validate(&self, catalog: &RuleCatalog) -> Result<(), LedgerError> {
        if self.version != 1 {
            return Err(invalid("unsupported evidence version"));
        }
        let expected: BTreeSet<_> = catalog.rules.iter().map(|rule| rule.key.as_str()).collect();
        let mut seen = BTreeSet::new();
        for decision in &self.decisions {
            if !expected.contains(decision.rule.as_str()) || !seen.insert(decision.rule.as_str()) {
                return Err(invalid(format!(
                    "unknown or duplicate record: {}",
                    decision.rule
                )));
            }
            if decision.reason.trim().is_empty()
                || decision.counterparts.is_empty()
                || decision
                    .counterparts
                    .iter()
                    .any(|c| c.path.is_empty() || c.item.is_empty())
                || (decision.tests.is_empty() && decision.verdict != Verdict::NotYetTested)
            {
                return Err(invalid(format!("incomplete record: {}", decision.rule)));
            }
            let fixed = decision.fix_commit.as_deref().is_some_and(|commit| {
                (8..=40).contains(&commit.len()) && commit.bytes().all(|b| b.is_ascii_hexdigit())
            });
            if fixed != (decision.verdict == Verdict::DifferedAndFixed)
                || (decision.fix_commit.is_some() && !fixed)
            {
                return Err(invalid(format!(
                    "invalid correction commit: {}",
                    decision.rule
                )));
            }
            let identities: BTreeSet<_> = decision
                .tests
                .iter()
                .map(ComparisonTest::identity)
                .collect();
            if identities.len() != decision.tests.len() {
                return Err(invalid(format!("duplicate comparison: {}", decision.rule)));
            }
            if let Some(run) = &decision.run
                && (!valid_digest(&run.rule_digest)
                    || !valid_digest(&run.junit_digest)
                    || identities != run.tests.keys().cloned().collect())
            {
                return Err(invalid(format!(
                    "invalid run provenance: {}",
                    decision.rule
                )));
            }
        }
        if seen != expected {
            return Err(invalid(format!(
                "missing records: {:?}",
                expected.difference(&seen).collect::<Vec<_>>()
            )));
        }
        Ok(())
    }

    /// Check mode rejects absent or stale rule digests even when the rendered
    /// verdict is already conservative. A partial run must still be recorded.
    pub fn check_current(&self, catalog: &RuleCatalog) -> Result<(), LedgerError> {
        self.validate(catalog)?;
        for rule in &catalog.rules {
            let decision = self
                .decisions
                .iter()
                .find(|d| d.rule == rule.key)
                .expect("validated key set");
            if decision
                .run
                .as_ref()
                .is_none_or(|run| run.rule_digest != rule_digest(rule))
            {
                return Err(invalid(format!(
                    "{}: stale; rerun comparisons and write mode",
                    rule.key
                )));
            }
        }
        Ok(())
    }
}

impl DecisionEvidence {
    /// Published disposition and reason. Missing, failed, skipped, or stale
    /// evidence cannot inherit an intended agreement from maintained metadata.
    pub fn effective_verdict(&self, rule: &Rule) -> (Verdict, String) {
        let gap = |why: &str| (Verdict::NotYetTested, format!("{why}; {}", self.reason));
        let Some(run) = &self.run else {
            return gap("no recorded run");
        };
        if run.rule_digest != rule_digest(rule) {
            return gap("stale");
        }
        if self.verdict == Verdict::NotYetTested {
            return (self.verdict, self.reason.clone());
        }
        if self.tests.is_empty() {
            return gap("no named comparisons");
        }
        let required = if self.verdict == Verdict::IgnoredDifference {
            Outcome::Failed
        } else {
            Outcome::Passed
        };
        let mut unmet: Vec<_> = self
            .tests
            .iter()
            .filter_map(|test| {
                let outcome = run
                    .tests
                    .get(&test.identity())
                    .copied()
                    .unwrap_or(Outcome::Missing);
                (outcome != required).then(|| {
                    format!(
                        "{}: {} (requires {})",
                        test.identity(),
                        outcome.label(),
                        required.label()
                    )
                })
            })
            .collect();
        unmet.sort();
        if !unmet.is_empty() {
            return gap(&unmet.join("; "));
        }
        (self.verdict, self.reason.clone())
    }
}
