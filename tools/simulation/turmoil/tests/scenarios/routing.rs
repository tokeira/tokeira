//! Task routing and queue homes across two nodes.
//!
//! Property: every scheduled task is started or times out; none waits for
//! ever (`EventuallyResolved`, `spec/tla/40_dispatch_handoff`).

use turmoil_sim::{
    topology,
    workload::{self, QueuePlacement},
};

use crate::{logs, seed};

#[test]
fn a_workflow_completes_when_its_tasks_start_across_nodes() {
    logs();
    let mut run = topology::cluster(seed()).expect("the cluster builds");
    run.worker(|trace| workload::one_activity_across_nodes(trace, QueuePlacement::Any));
    let trace = run.drive([]).expect("the simulation runs to the end");
    assert!(
        trace.completed("workflow"),
        "the workflow did not complete:\n{trace}"
    );
}

// The runtime serves an activity poll only at the queue's home. The queue
// chosen here has its home on another node than the bundle a key of the
// namespace name, the queue and the Activity kind picks, so its activity
// completes only if the edge routes activity polls to the home, as the
// runtime computes it, and not by that key.
#[test]
fn an_activity_poll_is_served_on_the_node_the_edge_sends_it_to() {
    logs();
    let mut run = topology::cluster(seed()).expect("the cluster builds");
    run.worker(|trace| workload::one_activity_across_nodes(trace, QueuePlacement::AwayFromNameKey));
    let trace = run.drive([]).expect("the simulation runs to the end");
    assert!(
        trace.completed("workflow"),
        "the workflow did not complete:\n{trace}"
    );
}
