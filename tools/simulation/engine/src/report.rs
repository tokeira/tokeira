//! Aggregate reporting: model-defined signal counters and per-invariant verdicts.
//!
//! Signal names are supplied by the model, so the harness holds no domain
//! vocabulary. The reporter sums signals across seeds and tracks a pass/fail per
//! invariant, marking the whole run failed if any safety invariant ever fails —
//! the same "healthy run" presentation `placement-sim` uses.

use std::collections::BTreeMap;

use crate::invariant::{InvariantOutcome, Violation};

/// Model-defined named counters. The harness never invents counter names; a
/// model increments whatever signals it cares about and the reporter aggregates
/// them. `BTreeMap` keeps report output in stable alphabetical order.
#[derive(Clone, Debug, Default)]
pub struct SignalCounters {
    counts: BTreeMap<&'static str, u64>,
}

impl SignalCounters {
    /// Create an empty counter set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment `name` by one.
    pub fn incr(&mut self, name: &'static str) {
        *self.counts.entry(name).or_insert(0) += 1;
    }

    /// Add `n` to `name`.
    pub fn add(&mut self, name: &'static str, n: u64) {
        *self.counts.entry(name).or_insert(0) += n;
    }

    /// Current value of `name` (zero if never touched).
    pub fn get(&self, name: &'static str) -> u64 {
        self.counts.get(name).copied().unwrap_or(0)
    }

    /// Iterate `(name, value)` in stable order.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, u64)> + '_ {
        self.counts.iter().map(|(k, v)| (*k, *v))
    }

    /// Fold another counter set into this one (used for cross-seed aggregation).
    pub fn merge(&mut self, other: &SignalCounters) {
        for (name, value) in other.iter() {
            self.add(name, value);
        }
    }
}

/// The outcome of a single seed's stress run: its signals and any first
/// violation observed.
#[derive(Clone, Debug)]
pub struct SeedReport {
    /// The seed that produced this run.
    pub seed: u64,
    /// Signals accumulated during the run.
    pub signals: SignalCounters,
    /// The first invariant violation, if the run failed.
    pub violation: Option<Violation>,
}

/// Cross-seed aggregate: summed signals and per-invariant verdicts.
#[derive(Clone, Debug, Default)]
pub struct Report {
    seeds: u64,
    signals: SignalCounters,
    invariant_results: BTreeMap<&'static str, InvariantOutcome>,
    first_failure: Option<(u64, Violation)>,
}

impl Report {
    /// Create a report pre-seeded with every invariant marked `Pass`, so the
    /// output lists all invariants even when none fail.
    pub fn new(initial_invariants: BTreeMap<&'static str, InvariantOutcome>) -> Self {
        Self {
            seeds: 0,
            signals: SignalCounters::new(),
            invariant_results: initial_invariants,
            first_failure: None,
        }
    }

    /// Fold one seed's result into the aggregate.
    pub fn add_seed(&mut self, report: SeedReport) {
        self.seeds += 1;
        self.signals.merge(&report.signals);
        if let Some(violation) = report.violation {
            self.invariant_results
                .insert(violation.invariant, InvariantOutcome::Fail);
            if self.first_failure.is_none() {
                self.first_failure = Some((report.seed, violation));
            }
        }
    }

    /// True only if no invariant is recorded as failed.
    pub fn overall_passed(&self) -> bool {
        self.invariant_results
            .values()
            .all(|o| *o == InvariantOutcome::Pass)
    }

    /// The first failing seed and its violation, if any.
    pub fn first_failure(&self) -> Option<&(u64, Violation)> {
        self.first_failure.as_ref()
    }

    /// Aggregated signals, for callers that want to inspect counts directly.
    pub fn signals(&self) -> &SignalCounters {
        &self.signals
    }

    /// Render the report to stdout in the `placement-sim` style: a header, the
    /// signal counts, then a PASS/FAIL line per invariant and an overall verdict.
    pub fn print(&self, title: &str) {
        println!("{title}");
        println!("  seeds:                 {}", self.seeds);
        for (name, value) in self.signals.iter() {
            println!("  {name:<28} {value}");
        }
        println!("  invariants:");
        for (name, outcome) in &self.invariant_results {
            let label = match outcome {
                InvariantOutcome::Pass => "PASS",
                InvariantOutcome::Fail => "FAIL",
            };
            println!("    {name:<6} {label}");
        }
        if let Some((seed, violation)) = &self.first_failure {
            println!(
                "  first failure: seed {seed}, invariant {}: {}",
                violation.invariant, violation.reason
            );
        }
        println!(
            "  overall: {}",
            if self.overall_passed() {
                "PASS"
            } else {
                "FAIL"
            }
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invariant::InvariantOutcome;

    fn seeded() -> BTreeMap<&'static str, InvariantOutcome> {
        let mut m = BTreeMap::new();
        m.insert("S1", InvariantOutcome::Pass);
        m.insert("L1", InvariantOutcome::Pass);
        m
    }

    #[test]
    fn signals_sum_across_seeds() {
        let mut report = Report::new(seeded());
        let mut s1 = SignalCounters::new();
        s1.add("matches", 3);
        report.add_seed(SeedReport {
            seed: 1,
            signals: s1,
            violation: None,
        });
        let mut s2 = SignalCounters::new();
        s2.add("matches", 4);
        report.add_seed(SeedReport {
            seed: 2,
            signals: s2,
            violation: None,
        });
        assert_eq!(report.signals().get("matches"), 7);
        assert!(report.overall_passed());
    }

    #[test]
    fn any_safety_fail_marks_run_failed_and_records_first() {
        let mut report = Report::new(seeded());
        report.add_seed(SeedReport {
            seed: 5,
            signals: SignalCounters::new(),
            violation: Some(Violation {
                invariant: "S1",
                reason: "boom".into(),
            }),
        });
        report.add_seed(SeedReport {
            seed: 6,
            signals: SignalCounters::new(),
            violation: Some(Violation {
                invariant: "S1",
                reason: "boom again".into(),
            }),
        });
        assert!(!report.overall_passed());
        assert_eq!(report.first_failure().unwrap().0, 5);
    }
}
