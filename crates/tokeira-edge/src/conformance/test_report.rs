//! Per-test classified report: the join of run outcomes against the ledger.
//!
//! Tier 2's run-all executor produces, for one corpus run, a per-test *outcome*
//! capture (the `tokeira_conformance_ledger` distiller, task 8.2): one row per test
//! — at per-test granularity *including every `t.Run` sub-test* — recording what
//! happened (pass / fail / skip / unfinished). Separately, an operator authors a
//! *classification* ledger ([`super::ledger::LedgerEntry`]) that explains every
//! non-passing test: is it a real gap, a deliberate deviation, or out of public
//! scope, and with what evidence.
//!
//! This module performs the join (task 9.2): it marries the mechanical outcome with
//! the authored classification to produce exactly one [`ClassifiedTest`] per test
//! that ran (Requirements 3.2, 3.4). The output is what the report gates (task 10)
//! consume — totality (every non-passing test classified, 10.1) and real-gap
//! monotonicity (a real-gap test that now passes is stale, 10.5).
//!
//! ## Capture vs. classification, kept separate on purpose
//!
//! The outcome capture is pure fact; the ledger is authored judgement. They live in
//! different artifacts and are produced by different actors (the harness vs. an
//! operator), so the join must not blur them: a passing test needs no authored
//! entry, a non-passing test *requires* one, and a passing test that still carries a
//! non-pass authored entry is a *stale* ledger the join surfaces rather than hides.
//! Modelling these as distinct [`TestClassification`] variants makes each case
//! explicit for the gates instead of collapsing them into a lossy "category" guess.
//!
//! ## What this module owns and does not
//!
//! It owns the outcome↔ledger join only. It does **not** author classifications
//! (that is the operator's ledger), implement the gates (task 10), or join the wire
//! coverage ([`super::report`], task 9.1) — those are separate concerns kept in
//! separate modules so each can be tested in isolation.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::ledger::{EvidenceRef, LedgerCategory, LedgerEntry};

/// The mechanical outcome of a single test, as captured from the `go test -json`
/// stream by the Tier-2 distiller (task 8.2).
///
/// These mirror the distiller's kebab-case wire values exactly so the Go capture
/// document deserializes into this type without a translation layer. `Unfinished`
/// is the load-bearing one: under the run-all's per-entrypoint process isolation a
/// panicking test crashes its process, leaving sibling sub-tests that started but
/// never reported a terminal event. The distiller records those as `unfinished`
/// (not dropped), and the join must treat them as *non-passing tests that ran* —
/// they need a classification just like a failure does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TestOutcome {
    /// Ran and passed.
    Pass,
    /// Ran and failed.
    Fail,
    /// Ran and was skipped (`t.Skip`).
    Skip,
    /// Started but never reported a terminal event — the panic-crash sibling
    /// signature under per-entrypoint isolation. A test that ran, so it must be
    /// accounted for.
    Unfinished,
}

impl TestOutcome {
    /// Whether this outcome counts as *passing* for classification purposes.
    ///
    /// Only [`TestOutcome::Pass`] passes. Everything else — fail, skip, and
    /// unfinished — is a non-passing test that ran and therefore requires an
    /// authored ledger classification (Requirement 3.2). `Skip` is deliberately
    /// non-passing: a skipped conformance test is not a demonstrated pass, and
    /// silently treating it as one would let scope be dropped without a rationale.
    pub fn is_passing(self) -> bool {
        matches!(self, TestOutcome::Pass)
    }
}

/// One row of the Tier-2 outcome capture: a per-test identity and its mechanical
/// outcome.
///
/// This mirrors the distiller's (`tokeira_conformance_ledger`) output row so the
/// capture document round-trips into Rust. `test_id` is the per-test key
/// (`<package>/<Test>` including `t.Run` sub-test path); `elapsed_seconds` is
/// retained as reporting detail and is `0.0` for an unfinished test.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TestOutcomeRow {
    /// Per-test key: `<package>/<Test>`, including the full `t.Run` sub-test path.
    pub test_id: String,
    /// The mechanical outcome captured from the run.
    pub outcome: TestOutcome,
    /// Wall-clock seconds from the terminal event; `0.0` for an unfinished test.
    #[serde(default)]
    pub elapsed_seconds: f64,
}

/// The Tier-2 outcome capture document: every test that ran, exactly once.
///
/// This is the deserialization target for the Go distiller's JSON output (task
/// 8.2). The field name matches the distiller's `outcomes` key so the document
/// loads with no remapping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutcomeDocument {
    /// One outcome row per test that ran, at per-test (including sub-test)
    /// granularity.
    pub outcomes: Vec<TestOutcomeRow>,
}

/// The resolved classification of a single test after joining outcome with ledger.
///
/// The four variants are exhaustive over the join's cases and are exactly what the
/// gates (task 10) need to distinguish:
///
/// - [`Pass`](Self::Pass) — ran and passed; no authored entry required or present.
/// - [`Classified`](Self::Classified) — non-passing, with an authored ledger entry
///   explaining it. The expected, healthy state for a known non-pass.
/// - [`Unclassified`](Self::Unclassified) — non-passing with *no* authored entry.
///   The totality gate (10.1) fails the run on this: every non-passing test must be
///   classified.
/// - [`StalePass`](Self::StalePass) — ran and passed, but an authored entry still
///   marks it non-pass (e.g. a `RealGap` expect-fail that now passes). The
///   monotonicity gate (10.5) fails the run on this so the ledger cannot lag the
///   implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum TestClassification {
    /// Ran and passed; no ledger entry needed.
    Pass,
    /// Non-passing with an authored classification.
    Classified {
        /// The authored category (`RealGap` / `DeliberateDeviation` /
        /// `OutOfPublicScope`, or `Pass` if an operator redundantly classified a
        /// pass — preserved verbatim rather than second-guessed).
        category: LedgerCategory,
        /// The authored human-readable rationale.
        rationale: String,
        /// The authored category-keyed evidence.
        evidence: EvidenceRef,
    },
    /// Non-passing with no authored classification — fails the totality gate (10.1).
    Unclassified,
    /// Passed, but an authored entry still marks it non-pass — fails the
    /// monotonicity gate (10.5). Carries the stale entry's category so the gate can
    /// report what the ledger wrongly still claims.
    StalePass {
        /// The non-pass category the stale authored entry still asserts.
        category: LedgerCategory,
        /// The stale entry's rationale, preserved for the gate's diagnostic.
        rationale: String,
        /// The stale entry's evidence, preserved for the gate's diagnostic.
        evidence: EvidenceRef,
    },
}

/// One fully classified test in the joined report: its identity, mechanical
/// outcome, and resolved classification.
///
/// Exactly one of these is produced per test that ran (the join is total over the
/// outcome document), so the report never silently drops or double-counts a test.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassifiedTest {
    /// Per-test key, carried through from the outcome row.
    pub test_id: String,
    /// The mechanical outcome from the run.
    pub outcome: TestOutcome,
    /// The resolved classification after joining with the authored ledger.
    pub classification: TestClassification,
}

/// The joined per-test report: every test that ran, classified.
///
/// This is the ledger-side companion to [`super::report::WireCoverageReport`]: where
/// that joins observed wire traffic against the matrix, this joins per-test outcomes
/// against the authored ledger. The report gates (task 10) consume this directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestLedgerReport {
    /// One classified entry per test that ran, in the outcome document's order.
    pub tests: Vec<ClassifiedTest>,
}

/// Join a run's per-test outcomes against the authored ledger into a classified
/// report (task 9.2).
///
/// For each outcome row — in document order, so the report is stable and
/// diffable — the resolved [`TestClassification`] is decided by the cross of
/// "did it pass?" and "is there an authored entry?":
///
/// | outcome \\ ledger entry | absent          | present (non-pass category)     |
/// |-------------------------|-----------------|---------------------------------|
/// | **pass**                | `Pass`          | `StalePass` (10.5 fails)        |
/// | **non-pass**            | `Unclassified` (10.1 fails) | `Classified`        |
///
/// A present entry whose category is itself `Pass` is treated as agreeing with a
/// passing outcome (`Pass`) and as a redundant-but-honoured classification for a
/// non-passing outcome (`Classified` with category `Pass`) — the join does not
/// invent judgement, it only resolves the documented pairing; the gates decide
/// what is acceptable.
///
/// The join is **total**: every row in `document` yields exactly one
/// [`ClassifiedTest`]. Ledger entries with no matching outcome row are ignored here
/// (a classification for a test that did not run is not a test that ran); surfacing
/// those as a separate "ledger references an unknown test" diagnostic is a gate
/// concern, not part of the per-test classification.
pub fn join_test_ledger(document: &OutcomeDocument, ledger: &[LedgerEntry]) -> TestLedgerReport {
    // Index the authored ledger by test_id for O(1) lookup per outcome row. A
    // duplicate test_id in the ledger keeps the last entry — the join does not
    // adjudicate operator-authored duplicates (a gate concern); it resolves a
    // single classification per outcome row deterministically.
    let by_id: HashMap<&str, &LedgerEntry> = ledger
        .iter()
        .map(|entry| (entry.test_id.as_str(), entry))
        .collect();

    let tests = document
        .outcomes
        .iter()
        .map(|row| ClassifiedTest {
            test_id: row.test_id.clone(),
            outcome: row.outcome,
            classification: classify(row.outcome, by_id.get(row.test_id.as_str()).copied()),
        })
        .collect();

    TestLedgerReport { tests }
}

/// Resolve a single test's classification from its outcome and optional authored
/// entry. See [`join_test_ledger`] for the full pairing table.
fn classify(outcome: TestOutcome, entry: Option<&LedgerEntry>) -> TestClassification {
    match (outcome.is_passing(), entry) {
        // Passed, no authored entry: the clean pass.
        (true, None) => TestClassification::Pass,
        // Passed, but an authored entry marks it non-pass: stale ledger (10.5). A
        // `Pass` category entry on a pass is consistent, so it resolves to Pass.
        (true, Some(entry)) => match entry.category {
            LedgerCategory::Pass => TestClassification::Pass,
            _ => TestClassification::StalePass {
                category: entry.category.clone(),
                rationale: entry.rationale.clone(),
                evidence: entry.evidence.clone(),
            },
        },
        // Non-passing with no authored entry: unclassified (10.1 fails).
        (false, None) => TestClassification::Unclassified,
        // Non-passing with an authored entry: the documented classification.
        (false, Some(entry)) => TestClassification::Classified {
            category: entry.category.clone(),
            rationale: entry.rationale.clone(),
            evidence: entry.evidence.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(test_id: &str, outcome: TestOutcome) -> TestOutcomeRow {
        TestOutcomeRow {
            test_id: test_id.to_owned(),
            outcome,
            elapsed_seconds: 0.0,
        }
    }

    fn real_gap(test_id: &str) -> LedgerEntry {
        LedgerEntry {
            test_id: test_id.to_owned(),
            category: LedgerCategory::RealGap,
            rationale: "tracked gap".to_owned(),
            evidence: EvidenceRef::TrackingIssue("ISSUE-1".to_owned()),
        }
    }

    // A passing test with no authored entry resolves to Pass.
    #[test]
    fn passing_test_with_no_entry_is_pass() {
        let doc = OutcomeDocument {
            outcomes: vec![row("pkg/TestA", TestOutcome::Pass)],
        };
        let report = join_test_ledger(&doc, &[]);
        assert_eq!(report.tests[0].classification, TestClassification::Pass);
    }

    // A failing test with an authored entry resolves to Classified with that entry.
    #[test]
    fn failing_test_with_entry_is_classified() {
        let doc = OutcomeDocument {
            outcomes: vec![row("pkg/TestB", TestOutcome::Fail)],
        };
        let report = join_test_ledger(&doc, &[real_gap("pkg/TestB")]);
        match &report.tests[0].classification {
            TestClassification::Classified { category, .. } => {
                assert_eq!(*category, LedgerCategory::RealGap);
            }
            other => panic!("expected Classified, got {other:?}"),
        }
    }

    // A failing test with no authored entry is Unclassified (10.1 will fail on it).
    #[test]
    fn failing_test_with_no_entry_is_unclassified() {
        let doc = OutcomeDocument {
            outcomes: vec![row("pkg/TestC", TestOutcome::Fail)],
        };
        let report = join_test_ledger(&doc, &[]);
        assert_eq!(
            report.tests[0].classification,
            TestClassification::Unclassified
        );
    }

    // An unfinished test is non-passing: with no entry it is Unclassified, exactly
    // like a failure — a test that ran must be accounted for.
    #[test]
    fn unfinished_test_with_no_entry_is_unclassified() {
        let doc = OutcomeDocument {
            outcomes: vec![row("pkg/TestD", TestOutcome::Unfinished)],
        };
        let report = join_test_ledger(&doc, &[]);
        assert_eq!(
            report.tests[0].classification,
            TestClassification::Unclassified
        );
    }

    // A skipped test is non-passing too: it requires a classification.
    #[test]
    fn skipped_test_with_no_entry_is_unclassified() {
        let doc = OutcomeDocument {
            outcomes: vec![row("pkg/TestE", TestOutcome::Skip)],
        };
        let report = join_test_ledger(&doc, &[]);
        assert_eq!(
            report.tests[0].classification,
            TestClassification::Unclassified
        );
    }

    // A passing test that still carries a non-pass authored entry is StalePass
    // (10.5 will fail on it): the ledger lags the implementation.
    #[test]
    fn passing_test_with_stale_real_gap_entry_is_stale_pass() {
        let doc = OutcomeDocument {
            outcomes: vec![row("pkg/TestF", TestOutcome::Pass)],
        };
        let report = join_test_ledger(&doc, &[real_gap("pkg/TestF")]);
        match &report.tests[0].classification {
            TestClassification::StalePass { category, .. } => {
                assert_eq!(*category, LedgerCategory::RealGap);
            }
            other => panic!("expected StalePass, got {other:?}"),
        }
    }

    // The join is total: one classified entry per outcome row, in document order.
    #[test]
    fn join_is_total_and_order_preserving() {
        let doc = OutcomeDocument {
            outcomes: vec![
                row("pkg/T1", TestOutcome::Pass),
                row("pkg/T2", TestOutcome::Fail),
                row("pkg/T3", TestOutcome::Unfinished),
            ],
        };
        let report = join_test_ledger(&doc, &[real_gap("pkg/T2")]);
        assert_eq!(report.tests.len(), 3);
        assert_eq!(report.tests[0].test_id, "pkg/T1");
        assert_eq!(report.tests[1].test_id, "pkg/T2");
        assert_eq!(report.tests[2].test_id, "pkg/T3");
    }

    // A ledger entry for a test that did not run is ignored by the join (it is not
    // a test that ran); the report contains only outcome-document rows.
    #[test]
    fn ledger_entry_without_matching_outcome_is_ignored() {
        let doc = OutcomeDocument {
            outcomes: vec![row("pkg/Present", TestOutcome::Fail)],
        };
        let report = join_test_ledger(&doc, &[real_gap("pkg/Present"), real_gap("pkg/Absent")]);
        assert_eq!(report.tests.len(), 1);
        assert_eq!(report.tests[0].test_id, "pkg/Present");
    }

    // The outcome document round-trips through the distiller's JSON shape.
    #[test]
    fn outcome_document_deserializes_from_distiller_json() {
        let json = r#"{
            "outcomes": [
                { "test_id": "go.temporal.io/server/tests/TestX", "outcome": "pass", "elapsed_seconds": 0.5 },
                { "test_id": "go.temporal.io/server/tests/TestY/Sub", "outcome": "unfinished", "elapsed_seconds": 0.0 }
            ]
        }"#;
        let doc: OutcomeDocument = serde_json::from_str(json).expect("distiller JSON deserializes");
        assert_eq!(doc.outcomes.len(), 2);
        assert_eq!(doc.outcomes[0].outcome, TestOutcome::Pass);
        assert_eq!(doc.outcomes[1].outcome, TestOutcome::Unfinished);
    }
}
