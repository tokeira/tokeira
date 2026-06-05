//! Simulation-mode integration tests for the placement model.
//!
//! These drive the full `PlacementModel` through the engine `run_seed` loop —
//! the same path `main.rs` uses — to assert the end-to-end behaviour the CLI
//! demonstrates:
//!
//! - healthy run: a faulted-but-bounded multi-seed run keeps every safety
//!   invariant (I1–I6) PASS and exercises the membership fault paths (crash,
//!   drain, renewal suppression) plus the fence/repair machinery.
//! - determinism: an identical `(seeds, ops, time)` configuration reproduces a
//!   byte-identical aggregate, so any reported failure is replayable.

use std::collections::BTreeMap;

use placement_sim::{invariants, model::PlacementCfg, model_machine::PlacementModel};
use sim_engine::{run_seed, Report};

/// Run `seeds` of the clean placement model through the stress loop and return
/// the aggregate report — the exact construction `main.rs` performs.
fn run_clean(seeds: u64, ops: usize, time_ms: u64) -> Report {
    let cfg = PlacementCfg {
        ops_per_seed: ops,
        max_time_ms: time_ms,
        ..PlacementCfg::default()
    };
    let registry = invariants::registry();
    let mut report = Report::new(registry.initial_outcomes());
    for seed in 1..=seeds {
        let model = PlacementModel::new(cfg.clone());
        let seed_report = run_seed(model, seed, time_ms, &registry, false);
        report.add_seed(seed_report);
    }
    report
}

/// Snapshot a report's observable surface (summed signals + overall verdict) so
/// two runs can be compared for byte-identical equality.
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

/// A bounded-fault multi-seed run keeps every safety invariant PASS.
#[test]
fn healthy_run_passes_all_invariants() {
    let report = run_clean(120, 600, 6_000);
    assert!(
        report.overall_passed(),
        "clean placement model must pass I1–I6; first failure: {:?}",
        report.first_failure()
    );
}

/// The healthy run actually exercises the membership fault and fence paths, so
/// the PASS above is meaningful rather than a vacuous "nothing happened" pass.
#[test]
fn healthy_run_exercises_fault_and_fence_paths() {
    let report = run_clean(120, 600, 6_000);
    let signals = report.signals();
    assert!(signals.get("crashes") > 0, "no runtime crashes fired");
    assert!(signals.get("drains") > 0, "no graceful drains fired");
    assert!(
        signals.get("renewal_suspensions") > 0,
        "no renewal suppression fired"
    );
    // The fence + repair machinery must engage under those faults.
    assert!(
        signals.get("fence_rejections") > 0,
        "the OCC fence never rejected a stale commit — fault paths untested"
    );
    assert!(
        signals.get("edge_repairs") > 0,
        "the edge never repaired a stale route"
    );
    assert!(
        signals.get("successful_mutations") > 0,
        "no work was ever committed"
    );
}

/// Identical configuration reproduces a byte-identical aggregate.
#[test]
fn identical_config_is_byte_identical() {
    let a = run_clean(80, 500, 6_000);
    let b = run_clean(80, 500, 6_000);
    assert_eq!(
        snapshot(&a),
        snapshot(&b),
        "identical config produced diverging aggregates — determinism broken"
    );
}

/// A single seed reproduces identical per-seed signals and verdict.
#[test]
fn single_seed_is_reproducible() {
    let cfg = PlacementCfg {
        ops_per_seed: 500,
        max_time_ms: 6_000,
        ..PlacementCfg::default()
    };
    let registry = invariants::registry();
    let run_once = || {
        let model = PlacementModel::new(cfg.clone());
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
