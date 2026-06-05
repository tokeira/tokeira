//! Simulation-mode integration tests (delivery-broker-simulator spec tasks
//! 10.1 / 10.2).
//!
//! These drive the full `BrokerModel` through the harness `run_seed` loop —
//! the same path `main.rs` uses — rather than the tiny exhaustive model, so
//! they assert the end-to-end behaviour the CLI demonstrates:
//!
//! - 10.1 healthy run: a faulted-but-bounded multi-seed run keeps every
//!   safety (S1–S7) and liveness (L1–L4) invariant PASS and exercises the
//!   fault paths (crash/sweeper, redelivery, sticky promotion, dedup).
//! - 10.2 determinism: an identical `(seeds, ops, time, fault-config)`
//!   configuration reproduces a byte-identical aggregate (summed signals +
//!   per-invariant verdicts), so any failure is replayable.

use std::collections::BTreeMap;

use broker_sim::{invariants, model::BrokerCfg, model_machine::BrokerModel};
use sim_harness::{run_seed, Report};

/// Run `seeds` of the clean (bug-free) broker through the stress loop and
/// return the aggregate report — the exact construction `main.rs` performs.
fn run_clean(seeds: u64, ops: usize, time_ms: u64) -> Report {
    let registry = invariants::registry();
    let mut report = Report::new(registry.initial_outcomes());
    for seed in 1..=seeds {
        let model = BrokerModel::new(BrokerCfg::default(), ops, time_ms, None);
        let seed_report = run_seed(model, seed, time_ms, &registry, false);
        report.add_seed(seed_report);
    }
    report
}

/// Snapshot a report's observable surface as an ordered map so two runs can be
/// compared for byte-identical equality (signals summed across seeds + the
/// per-invariant PASS/FAIL verdict, encoded as a sentinel key).
fn snapshot(report: &Report) -> BTreeMap<String, u64> {
    let mut map: BTreeMap<String, u64> = report
        .signals()
        .iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect();
    map.insert(
        "@overall_passed".to_string(),
        u64::from(report.overall_passed()),
    );
    map
}

/// Task 10.1 — a bounded-fault multi-seed run keeps every invariant PASS.
///
/// The default workload interleaves the full adversarial fault set (broker
/// crash, worker crash, lease expiry, sticky-TTL expiry, duplicate publish,
/// partition pressure, sustained backlog age) at low probability, so this is a
/// genuine healthy-under-faults run, not a fault-free one.
#[test]
fn healthy_run_passes_all_invariants() {
    let report = run_clean(200, 600, 8_000);
    assert!(
        report.overall_passed(),
        "clean broker must pass all S1–S7 / L1–L4; first failure: {:?}",
        report.first_failure()
    );
}

/// Task 10.1 — the healthy run actually exercises the fault paths, so the PASS
/// above is meaningful rather than a vacuous "no faults fired" pass.
#[test]
fn healthy_run_exercises_fault_paths() {
    let report = run_clean(200, 600, 8_000);
    let signals = report.signals();
    // A crash must be followed by a sweeper rebuild (S5/L1 path).
    assert!(signals.get("broker_crashes") > 0, "no broker crashes fired");
    assert!(
        signals.get("sweeper_rebuilds") > 0,
        "crashes fired but the sweeper never rebuilt — S5/L1 path untested"
    );
    // Dedup, sticky promotion, and normal delivery must all be active.
    assert!(
        signals.get("duplicates_suppressed") > 0,
        "dedup path (S6) never exercised"
    );
    assert!(
        signals.get("sticky_promotions") > 0,
        "sticky promotion (S7) never exercised"
    );
    assert!(
        signals.get("tokens_delivered") > 0,
        "no work was ever delivered"
    );
}

/// Task 10.2 — identical configuration reproduces a byte-identical aggregate.
///
/// Same seeds, ops, time bound, and (empty) bug config ⇒ identical summed
/// signals and identical per-invariant verdicts. This is the load-bearing
/// determinism property: every reported failure is replayable.
#[test]
fn identical_config_is_byte_identical() {
    let a = run_clean(120, 500, 6_000);
    let b = run_clean(120, 500, 6_000);
    assert_eq!(
        snapshot(&a),
        snapshot(&b),
        "identical config produced diverging aggregates — determinism broken"
    );
}

/// Task 10.2 — a single seed reproduces identical per-seed signals, isolating
/// determinism to the seed level (not just the cross-seed sum).
#[test]
fn single_seed_is_reproducible() {
    let registry = invariants::registry();
    let run_once = || {
        let model = BrokerModel::new(BrokerCfg::default(), 500, 6_000, None);
        run_seed(model, 42, 6_000, &registry, false)
    };
    let a = run_once();
    let b = run_once();
    let sig_a: BTreeMap<_, _> = a.signals.iter().collect();
    let sig_b: BTreeMap<_, _> = b.signals.iter().collect();
    assert_eq!(sig_a, sig_b, "seed 42 was not reproducible");
    assert_eq!(
        a.violation.map(|v| v.invariant),
        b.violation.map(|v| v.invariant),
        "seed 42 produced a non-deterministic verdict"
    );
}
