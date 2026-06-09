//! Tier-2 report gates: the pass/fail checks over the classified report.
//!
//! The per-test classified report ([`super::test_report`]) and the authored ledger
//! ([`super::ledger`]) are *data*; the gates are the *policy* that decides whether a
//! Tier-2 run is acceptable. This module owns the three gates the design defines
//! (task 10), each a total function from the report to a structured
//! [`GateOutcome`] — pass, or fail with the specific violations — rather than a panic
//! or a bare bool, so a report can surface *every* violation at once and a caller
//! (CI, an operator) decides how to act.
//!
//! The three gates and what each forbids:
//!
//! - **Ledger totality** ([`ledger_totality_gate`], 10.1, Requirement 3.5): every
//!   non-passing test that ran must carry an authored classification. An
//!   [`TestClassification::Unclassified`] test fails the run — a non-pass without a
//!   rationale is the exact "silent skip" the run-all posture forbids.
//! - **No silent scope inflation** ([`scope_inflation_gate`], 10.3, Requirements
//!   3.6–3.8): every non-pass classification must cite the *right kind* of evidence —
//!   `OutOfPublicScope` an internal surface, `DeliberateDeviation` a spec/PR, `RealGap`
//!   a tracking issue. A classification with mismatched or empty evidence is scope
//!   inflation hiding behind a plausible label, and fails the run.
//! - **Real-gap monotonicity** ([`real_gap_monotonicity_gate`], 10.5, Requirements
//!   4.1–4.2): a test the ledger marks `RealGap` (expect-fail) that now *passes* is a
//!   stale ledger — the gap was fixed but the ledger still claims it. Surfaced as
//!   [`TestClassification::StalePass`], it fails the run so the ledger cannot lag the
//!   implementation; the fix is to flip the entry to a required pass.
//!
//! ## Why structured outcomes, not panics or bools
//!
//! A gate that panicked would stop at the first violation and lose the rest; a bare
//! bool would force the caller to re-derive *why*. Returning the full violation list
//! lets the report show an operator every stale entry and every missing classification
//! in one pass, which is what makes the ledger fixable in one edit cycle rather than
//! whack-a-mole. The gates are pure and side-effect-free (no I/O, no process exit);
//! turning a failed gate into an exit code is the binary's job, not this module's.

use serde::{Deserialize, Serialize};

use super::{
    ledger::{EvidenceRef, LedgerCategory},
    test_report::{TestClassification, TestLedgerReport},
};

/// The kind of gate a violation belongs to, so a combined report can attribute each
/// failure to the policy it broke.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GateKind {
    /// Ledger totality (10.1): a non-passing test lacks a classification.
    LedgerTotality,
    /// No silent scope inflation (10.3): a classification cites the wrong/empty evidence.
    ScopeInflation,
    /// Real-gap monotonicity (10.5): a `RealGap` test now passes (stale ledger).
    RealGapMonotonicity,
}

/// A single gate violation: which gate, which test, and a human-readable reason.
///
/// `test_id` is the per-test key the violation concerns, so an operator can jump
/// straight to the offending entry. `detail` explains the specific breach (e.g. which
/// evidence kind was expected) for a directly-actionable diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateViolation {
    /// The gate this violation belongs to.
    pub gate: GateKind,
    /// The per-test key the violation concerns.
    pub test_id: String,
    /// Human-readable explanation of the specific breach.
    pub detail: String,
}

/// The outcome of evaluating one or more gates: the (possibly empty) violation set.
///
/// A run *passes* iff `violations` is empty ([`GateOutcome::passed`]). Holding the
/// violations rather than a bool is the whole point — see the module docs on why a
/// gate surfaces every breach at once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateOutcome {
    /// Every violation found, across whichever gates were run. Empty means pass.
    pub violations: Vec<GateViolation>,
}

impl GateOutcome {
    /// Whether the gate(s) passed — i.e. no violations were found.
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }
}

/// Ledger-totality gate (10.1): every non-passing test must be classified.
///
/// Walks the classified report and flags each [`TestClassification::Unclassified`] —
/// a test that ran, did not pass, and carries no authored ledger entry. A passing test
/// (`Pass`), a classified non-pass (`Classified`), and even a `StalePass` (which is a
/// *different* failure, owned by the monotonicity gate) do not violate totality. This
/// gate's single concern is: no non-pass is left unexplained (Requirement 3.5).
pub fn ledger_totality_gate(report: &TestLedgerReport) -> GateOutcome {
    let violations = report
        .tests
        .iter()
        .filter(|test| matches!(test.classification, TestClassification::Unclassified))
        .map(|test| GateViolation {
            gate: GateKind::LedgerTotality,
            test_id: test.test_id.clone(),
            detail: format!(
                "test ran with outcome `{:?}` but carries no ledger classification; \
                 every non-passing test must be classified",
                test.outcome
            ),
        })
        .collect();
    GateOutcome { violations }
}

/// No-silent-scope-inflation gate (10.3): each classification cites the right evidence.
///
/// For every `Classified` test, the authored [`EvidenceRef`] must match its
/// [`LedgerCategory`] *and* be non-empty where a citation is required:
///
/// - `RealGap` ⟹ [`EvidenceRef::TrackingIssue`] with a non-empty reference (3.8)
/// - `DeliberateDeviation` ⟹ [`EvidenceRef::SpecOrPr`] with a non-empty reference (3.7)
/// - `OutOfPublicScope` ⟹ [`EvidenceRef::InternalSurface`] with a non-empty tag (3.6)
/// - `Pass` (an operator redundantly classifying a pass) ⟹ no citation required
///
/// A mismatched variant (e.g. an `OutOfPublicScope` entry citing a spec/PR) or an empty
/// reference is scope inflation behind a plausible label, and is flagged. `StalePass`
/// and `Unclassified` are not this gate's concern (they are other gates'), so they are
/// skipped here.
pub fn scope_inflation_gate(report: &TestLedgerReport) -> GateOutcome {
    let mut violations = Vec::new();
    for test in &report.tests {
        let TestClassification::Classified {
            category, evidence, ..
        } = &test.classification
        else {
            continue;
        };
        if let Some(detail) = evidence_mismatch(category, evidence) {
            violations.push(GateViolation {
                gate: GateKind::ScopeInflation,
                test_id: test.test_id.clone(),
                detail,
            });
        }
    }
    GateOutcome { violations }
}

/// Check one category↔evidence pairing; `Some(detail)` is a violation, `None` is fine.
///
/// Encodes the required pairing table from [`scope_inflation_gate`]. The non-empty
/// check is what stops a present-but-blank citation (`TrackingIssue("")`) from passing
/// as evidence.
fn evidence_mismatch(category: &LedgerCategory, evidence: &EvidenceRef) -> Option<String> {
    let non_empty = |reference: &str| !reference.trim().is_empty();
    match (category, evidence) {
        (LedgerCategory::RealGap, EvidenceRef::TrackingIssue(reference))
            if non_empty(reference) =>
        {
            None
        }
        (LedgerCategory::DeliberateDeviation, EvidenceRef::SpecOrPr(reference))
            if non_empty(reference) =>
        {
            None
        }
        (LedgerCategory::OutOfPublicScope, EvidenceRef::InternalSurface(reference))
            if non_empty(reference) =>
        {
            None
        }
        // A pass classified as Pass needs no evidence; NotApplicable is correct.
        (LedgerCategory::Pass, EvidenceRef::NotApplicable) => None,
        // Everything else is a mismatch or an empty citation.
        (category, evidence) => Some(format!(
            "category `{category:?}` requires its matching non-empty evidence; \
             got `{evidence:?}` (scope inflation / missing citation)"
        )),
    }
}

/// Real-gap monotonicity gate (10.5): a `RealGap` test that now passes is stale.
///
/// The classified report already resolves "passed, but the ledger still marks it
/// non-pass" to [`TestClassification::StalePass`]. This gate flags every such test
/// whose stale category is [`LedgerCategory::RealGap`] — the expect-fail that has begun
/// passing and must be flipped to a required pass (Requirements 4.1, 4.2). A `StalePass`
/// carrying some other category (e.g. an operator left a `DeliberateDeviation` on a now-
/// passing test) is still a ledger inconsistency, but monotonicity specifically owns the
/// `RealGap` case the design calls out; other stale categories are reported with their
/// own detail so nothing is silently ignored.
pub fn real_gap_monotonicity_gate(report: &TestLedgerReport) -> GateOutcome {
    let mut violations = Vec::new();
    for test in &report.tests {
        let TestClassification::StalePass { category, .. } = &test.classification else {
            continue;
        };
        let detail = match category {
            LedgerCategory::RealGap => format!(
                "test passes but the ledger still marks it `RealGap` (expect-fail); \
                 flip it to a required pass — the gap is fixed (test_id `{}`)",
                test.test_id
            ),
            other => format!(
                "test passes but the ledger still marks it `{other:?}` (stale non-pass classification)"
            ),
        };
        violations.push(GateViolation {
            gate: GateKind::RealGapMonotonicity,
            test_id: test.test_id.clone(),
            detail,
        });
    }
    GateOutcome { violations }
}

/// Run all three Tier-2 gates and combine their violations into one outcome.
///
/// This is the single entry point a report binary calls: it evaluates ledger totality
/// (10.1), scope inflation (10.3), and real-gap monotonicity (10.5) over the same
/// classified report and concatenates their violations. The run passes iff all three
/// pass. Combining rather than short-circuiting means an operator sees every kind of
/// problem in one go (a missing classification *and* a stale entry *and* a bad
/// citation), which is the fixable-in-one-cycle property the gates exist for.
pub fn evaluate_all_gates(report: &TestLedgerReport) -> GateOutcome {
    let mut violations = Vec::new();
    violations.extend(ledger_totality_gate(report).violations);
    violations.extend(scope_inflation_gate(report).violations);
    violations.extend(real_gap_monotonicity_gate(report).violations);
    GateOutcome { violations }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::test_report::{ClassifiedTest, TestOutcome};

    fn classified(
        test_id: &str,
        outcome: TestOutcome,
        classification: TestClassification,
    ) -> ClassifiedTest {
        ClassifiedTest {
            test_id: test_id.to_owned(),
            outcome,
            classification,
        }
    }

    fn report(tests: Vec<ClassifiedTest>) -> TestLedgerReport {
        TestLedgerReport { tests }
    }

    // Totality: an Unclassified non-pass fails the gate; a Pass and a Classified do not.
    #[test]
    fn totality_flags_only_unclassified() {
        let r = report(vec![
            classified("pkg/Pass", TestOutcome::Pass, TestClassification::Pass),
            classified(
                "pkg/Classified",
                TestOutcome::Fail,
                TestClassification::Classified {
                    category: LedgerCategory::RealGap,
                    rationale: "tracked".to_owned(),
                    evidence: EvidenceRef::TrackingIssue("ISSUE-1".to_owned()),
                },
            ),
            classified(
                "pkg/Unclassified",
                TestOutcome::Fail,
                TestClassification::Unclassified,
            ),
        ]);
        let outcome = ledger_totality_gate(&r);
        assert!(!outcome.passed());
        assert_eq!(outcome.violations.len(), 1);
        assert_eq!(outcome.violations[0].test_id, "pkg/Unclassified");
        assert_eq!(outcome.violations[0].gate, GateKind::LedgerTotality);
    }

    // Scope inflation: correct category/evidence pairings pass.
    #[test]
    fn scope_inflation_accepts_matching_evidence() {
        let r = report(vec![
            classified(
                "pkg/Gap",
                TestOutcome::Fail,
                TestClassification::Classified {
                    category: LedgerCategory::RealGap,
                    rationale: "r".to_owned(),
                    evidence: EvidenceRef::TrackingIssue("ISSUE-1".to_owned()),
                },
            ),
            classified(
                "pkg/Deviation",
                TestOutcome::Fail,
                TestClassification::Classified {
                    category: LedgerCategory::DeliberateDeviation,
                    rationale: "r".to_owned(),
                    evidence: EvidenceRef::SpecOrPr("spec#1".to_owned()),
                },
            ),
            classified(
                "pkg/Scope",
                TestOutcome::Fail,
                TestClassification::Classified {
                    category: LedgerCategory::OutOfPublicScope,
                    rationale: "r".to_owned(),
                    evidence: EvidenceRef::InternalSurface("AdminClient".to_owned()),
                },
            ),
        ]);
        assert!(scope_inflation_gate(&r).passed());
    }

    // Scope inflation: a mismatched evidence variant is flagged.
    #[test]
    fn scope_inflation_flags_mismatched_evidence() {
        let r = report(vec![classified(
            "pkg/Scope",
            TestOutcome::Fail,
            TestClassification::Classified {
                category: LedgerCategory::OutOfPublicScope,
                rationale: "r".to_owned(),
                // Wrong: out-of-scope must cite an internal surface, not a spec/PR.
                evidence: EvidenceRef::SpecOrPr("spec#1".to_owned()),
            },
        )]);
        let outcome = scope_inflation_gate(&r);
        assert!(!outcome.passed());
        assert_eq!(outcome.violations[0].gate, GateKind::ScopeInflation);
    }

    // Scope inflation: a present-but-empty citation is flagged.
    #[test]
    fn scope_inflation_flags_empty_citation() {
        let r = report(vec![classified(
            "pkg/Gap",
            TestOutcome::Fail,
            TestClassification::Classified {
                category: LedgerCategory::RealGap,
                rationale: "r".to_owned(),
                evidence: EvidenceRef::TrackingIssue("   ".to_owned()),
            },
        )]);
        assert!(!scope_inflation_gate(&r).passed());
    }

    // Monotonicity: a RealGap test that now passes (StalePass) fails the gate.
    #[test]
    fn monotonicity_flags_stale_real_gap() {
        let r = report(vec![classified(
            "pkg/Fixed",
            TestOutcome::Pass,
            TestClassification::StalePass {
                category: LedgerCategory::RealGap,
                rationale: "tracked".to_owned(),
                evidence: EvidenceRef::TrackingIssue("ISSUE-1".to_owned()),
            },
        )]);
        let outcome = real_gap_monotonicity_gate(&r);
        assert!(!outcome.passed());
        assert_eq!(outcome.violations[0].gate, GateKind::RealGapMonotonicity);
        assert_eq!(outcome.violations[0].test_id, "pkg/Fixed");
    }

    // A clean report (all passing or correctly classified) passes all gates.
    #[test]
    fn all_gates_pass_on_a_clean_report() {
        let r = report(vec![
            classified("pkg/A", TestOutcome::Pass, TestClassification::Pass),
            classified(
                "pkg/B",
                TestOutcome::Fail,
                TestClassification::Classified {
                    category: LedgerCategory::RealGap,
                    rationale: "tracked".to_owned(),
                    evidence: EvidenceRef::TrackingIssue("ISSUE-2".to_owned()),
                },
            ),
        ]);
        assert!(evaluate_all_gates(&r).passed());
    }

    // evaluate_all_gates surfaces violations from every gate at once.
    #[test]
    fn evaluate_all_gates_combines_every_gate() {
        let r = report(vec![
            classified(
                "pkg/Unclassified",
                TestOutcome::Fail,
                TestClassification::Unclassified,
            ),
            classified(
                "pkg/StaleGap",
                TestOutcome::Pass,
                TestClassification::StalePass {
                    category: LedgerCategory::RealGap,
                    rationale: "tracked".to_owned(),
                    evidence: EvidenceRef::TrackingIssue("ISSUE-3".to_owned()),
                },
            ),
            classified(
                "pkg/BadEvidence",
                TestOutcome::Fail,
                TestClassification::Classified {
                    category: LedgerCategory::OutOfPublicScope,
                    rationale: "r".to_owned(),
                    evidence: EvidenceRef::NotApplicable,
                },
            ),
        ]);
        let outcome = evaluate_all_gates(&r);
        assert!(!outcome.passed());
        let kinds: Vec<GateKind> = outcome.violations.iter().map(|v| v.gate).collect();
        assert!(kinds.contains(&GateKind::LedgerTotality));
        assert!(kinds.contains(&GateKind::RealGapMonotonicity));
        assert!(kinds.contains(&GateKind::ScopeInflation));
    }
}
