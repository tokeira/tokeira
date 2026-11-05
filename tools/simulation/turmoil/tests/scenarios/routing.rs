//! Task routing and queue homes across two nodes.
//!
//! Property: every scheduled task is started or times out; none waits for
//! ever (`EventuallyResolved`, `spec/tla/40_dispatch_handoff`).

use turmoil_sim::{
    topology,
    workload::{self, QueuePlacement},
};

use crate::{SEED, logs};

#[test]
fn a_workflow_completes_when_its_tasks_start_across_nodes() {
    logs();
    let mut run = topology::cluster(SEED).expect("the cluster builds");
    run.worker(|trace| workload::one_activity_across_nodes(trace, QueuePlacement::Agreeing));
    let trace = run.drive([]).expect("the simulation runs to the end");
    assert!(
        trace.completed("workflow"),
        "the workflow did not complete:\n{trace}"
    );
}

// The edge sends an activity poll to the node it computes from the namespace
// name and the Activity kind, and the runtime serves it only at the home it
// computes from the namespace id and the Workflow kind. When those are
// different nodes, neither serves the poll.
#[test]
#[ignore = "fails until the edge and the runtime agree on an activity queue's home"]
fn an_activity_poll_is_served_on_the_node_the_edge_sends_it_to() {
    logs();
    let mut run = topology::cluster(SEED).expect("the cluster builds");
    run.worker(|trace| workload::one_activity_across_nodes(trace, QueuePlacement::Disagreeing));
    let trace = run.drive([]).expect("the simulation runs to the end");
    assert!(
        trace.completed("workflow"),
        "the workflow did not complete:\n{trace}"
    );
}
