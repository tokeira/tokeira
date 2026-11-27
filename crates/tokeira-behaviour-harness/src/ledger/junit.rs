//! Import completed nextest library tests with exact, unambiguous identities.
//!
//! A skipped ignored test is not a reproduced difference. Only an executed failure
//! can substantiate that disposition; errors and retries remain incomplete evidence.

use std::collections::BTreeMap;

use serde::Deserialize;
use tokeira_behaviour_model::catalog::RuleCatalog;

use super::{Ledger, LedgerError, Outcome, RunEvidence, digest, invalid, rule_digest};

#[derive(Debug, Deserialize)]
#[serde(rename = "testsuites")]
struct Report {
    #[serde(rename = "testsuite", default)]
    suites: Vec<Suite>,
}

#[derive(Debug, Deserialize)]
struct Suite {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "testcase", default)]
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    #[serde(rename = "@name")]
    name: String,
    #[serde(default)]
    failure: Option<serde::de::IgnoredAny>,
    #[serde(default)]
    error: Option<serde::de::IgnoredAny>,
    #[serde(default)]
    skipped: Option<serde::de::IgnoredAny>,
    #[serde(rename = "flakyFailure", default)]
    flaky_failure: Vec<serde::de::IgnoredAny>,
    #[serde(rename = "flakyError", default)]
    flaky_error: Vec<serde::de::IgnoredAny>,
    #[serde(rename = "rerunFailure", default)]
    rerun_failure: Vec<serde::de::IgnoredAny>,
    #[serde(rename = "rerunError", default)]
    rerun_error: Vec<serde::de::IgnoredAny>,
}

fn outcomes(xml: &str) -> Result<BTreeMap<String, Outcome>, LedgerError> {
    let report: Report =
        quick_xml::de::from_str(xml).map_err(|e| invalid(format!("JUnit: {e}")))?;
    let mut outcomes = BTreeMap::new();
    for suite in report.suites {
        for case in suite.cases {
            let identity = format!("{}::{}", suite.name, case.name);
            let result = if case.error.is_some()
                || !case.flaky_failure.is_empty()
                || !case.flaky_error.is_empty()
                || !case.rerun_failure.is_empty()
                || !case.rerun_error.is_empty()
            {
                Outcome::Error
            } else if case.skipped.is_some() {
                Outcome::Skipped
            } else if case.failure.is_some() {
                Outcome::Failed
            } else {
                Outcome::Passed
            };
            if outcomes.insert(identity.clone(), result).is_some() {
                return Err(invalid(format!("duplicate JUnit identity: {identity}")));
            }
        }
    }
    if outcomes.is_empty() {
        return Err(invalid("JUnit contains no test results"));
    }
    Ok(outcomes)
}

/// Replace every run record from one nextest report, including missing tests.
/// Maintained facts never change. Unknown report tests are harmless, but duplicate
/// identities and reports matching no named comparison are rejected atomically.
/// The caller must run comparisons against the current catalog before importing.
pub fn record_run(
    ledger: &mut Ledger,
    catalog: &RuleCatalog,
    xml: &str,
) -> Result<(), LedgerError> {
    // A changed comparison list invalidates the old run's identities. Validate
    // maintained facts without that superseded run, then replace all outcomes
    // together so a rejected report cannot erase the previous observations.
    let mut updated = ledger.clone();
    for decision in &mut updated.decisions {
        decision.run = None;
    }
    updated.validate(catalog)?;
    let outcomes = outcomes(xml)?;
    if !ledger
        .decisions
        .iter()
        .flat_map(|d| &d.tests)
        .any(|test| outcomes.contains_key(&test.identity()))
    {
        return Err(invalid("JUnit matches no named comparisons"));
    }
    let report_digest = digest(xml.as_bytes());
    for decision in &mut updated.decisions {
        let rule = catalog
            .rules
            .iter()
            .find(|rule| rule.key == decision.rule)
            .expect("validated keys");
        decision.run = Some(RunEvidence {
            rule_digest: rule_digest(rule),
            junit_digest: report_digest.clone(),
            tests: decision
                .tests
                .iter()
                .map(|test| {
                    let identity = test.identity();
                    let outcome = outcomes.get(&identity).copied().unwrap_or(Outcome::Missing);
                    (identity, outcome)
                })
                .collect(),
        });
    }
    *ledger = updated;
    Ok(())
}
