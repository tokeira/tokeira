//! Step one of the turmoil spike: one engine node and one scripted worker.

use turmoil_sim::run_one_activity_workflow;

const SEED: u64 = 0x7e_6b_1a;

#[test]
fn a_one_activity_workflow_completes_on_a_simulated_node() -> anyhow::Result<()> {
    let trace = run_one_activity_workflow(SEED)?;
    assert!(
        trace.completed(),
        "the workflow did not complete:\n{trace:#?}"
    );
    Ok(())
}

// Invariant: one seed gives one trace — the client observes the same steps at
// the same simulated times on every run.
#[test]
fn the_same_seed_replays_the_same_trace() -> anyhow::Result<()> {
    let first = run_one_activity_workflow(SEED)?;
    let second = run_one_activity_workflow(SEED)?;
    assert_eq!(first, second);
    Ok(())
}
