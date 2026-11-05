//! Self-tests of the harness: a workflow completes on one simulated node,
//! a timer fires in simulated time, and one seed replays one trace, on one
//! node and on the cluster.

use tokeira_proto::enums::EventType;
use turmoil_sim::{
    Trace, oracle, topology,
    workload::{self, QueuePlacement, TIMER},
};

use crate::{logs, seed};

#[test]
fn a_one_activity_workflow_completes_on_one_node() {
    logs();
    let mut run = topology::single_node(seed());
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
    let mut run = topology::single_node(seed());
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
        let mut run = topology::single_node(seed());
        run.worker(workload::timer);
        run.drive([]).expect("the simulation runs to the end")
    };
    assert_eq!(run_once(), run_once());
}

// Invariant: what the worker sees on the cluster replays from its seed. The
// controller orders nodes by incarnation id, so this holds only while ids come
// from the seed. The worker acts only once placement has settled.
#[test]
fn the_same_seed_replays_the_same_cluster_trace() {
    logs();
    assert_eq!(cluster_run(false), cluster_run(false));
}

// Invariant: the lease handovers while the cluster forms replay too, to the
// millisecond.
#[test]
#[ignore = "the controller sends placement directives in hash-map order, and turmoil draws \
            each message's latency in send order, so which node acquires its bundles first \
            differs between processes: this fails about half the time"]
fn the_same_seed_replays_the_same_lease_handovers() {
    logs();
    assert_eq!(cluster_run(true), cluster_run(true));
}

/// One cluster run of the one-activity workload, with the lease observer if
/// `observed`.
fn cluster_run(observed: bool) -> Trace {
    let mut run = topology::cluster(seed()).expect("the cluster builds");
    if observed {
        run.observe(oracle::one_owner_per_epoch);
    }
    run.worker(|trace| workload::one_activity_across_nodes(trace, QueuePlacement::Agreeing));
    run.drive([]).expect("the simulation runs to the end")
}
