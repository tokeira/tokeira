//! Step three: a workflow timer that fires in simulated time.

use turmoil_sim::timer::run_timer_workflow;

const SEED: u64 = 0x7e_6b_1a;

/// Engine logs, filtered by `RUST_LOG`, for diagnosing a failing run.
fn logs() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

// The run itself checks that the timer fired no sooner than its length and
// that the node stamped the firing on the simulation's clock.
#[test]
fn a_timer_fires_in_simulated_time() {
    logs();
    let trace = run_timer_workflow(SEED).expect("the simulation runs to the end");
    assert!(
        trace.completed(),
        "the workflow did not complete:\n{}",
        trace.steps.join("\n")
    );
}

// Invariant: one seed gives one trace, including the times the node stamps on
// the history's events.
#[test]
fn the_same_seed_replays_the_same_timer_trace() {
    logs();
    let first = run_timer_workflow(SEED).expect("the first run runs to the end");
    let second = run_timer_workflow(SEED).expect("the second run runs to the end");
    assert_eq!(first, second);
}
