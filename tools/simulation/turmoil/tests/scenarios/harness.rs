//! Self-tests of the harness: a workflow completes on one simulated node,
//! a timer fires in simulated time, and one seed replays one trace.

use tokeira_proto::enums::EventType;
use turmoil_sim::{
    oracle, topology,
    workload::{self, TIMER},
};

use crate::{SEED, logs};

#[test]
fn a_one_activity_workflow_completes_on_one_node() {
    logs();
    let mut run = topology::single_node(SEED);
    run.worker(workload::one_activity);
    let trace = run.drive([]).expect("the simulation runs to the end");
    assert!(
        trace.completed("workflow"),
        "the workflow did not complete:\n{trace}"
    );
}

// The workload also checks that the node stamps the firing no later than the
// worker receives the task it caused, so the node's clock is the simulation's.
#[test]
fn a_timer_fires_in_simulated_time() {
    logs();
    let mut run = topology::single_node(SEED);
    run.worker(workload::timer);
    let trace = run.drive([]).expect("the simulation runs to the end");
    assert!(
        trace.completed("workflow"),
        "the workflow did not complete:\n{trace}"
    );
    oracle::at_least_apart(
        trace.history("workflow"),
        EventType::TimerStarted,
        EventType::TimerFired,
        TIMER,
    )
    .unwrap_or_else(|error| panic!("{error}:\n{trace}"));
}

// Invariant: one seed gives one trace, including the times the node stamps on
// the history's events.
#[test]
fn the_same_seed_replays_the_same_trace() {
    logs();
    let run_once = || {
        let mut run = topology::single_node(SEED);
        run.worker(workload::timer);
        run.drive([]).expect("the simulation runs to the end")
    };
    assert_eq!(run_once(), run_once());
}
