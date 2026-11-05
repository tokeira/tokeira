//! Bundle leases: who owns each bundle, and at which epoch.
//!
//! Property: at most one node owns a bundle at an epoch, and a bundle's
//! epoch never goes back (`OneOwnerPerEpoch`, `spec/tla/30_bundle_lease`).
//! The oracle checks it on every tick against the shared store. The lapse
//! scenarios also check that an owner whose lease lapsed admits nothing, and
//! that another node takes its home over (`bundle-lease-fence`).

use turmoil_sim::{
    Trace,
    faults::{Action, Fault},
    oracle, topology,
    workload::{self, CUT_NODE, HOME_CHOSEN, QueuePlacement},
};

use crate::{SEED, logs};

// The cluster forms by handing every bundle from a previous generation to
// the two nodes, so each bundle changes owner while the workflow runs.
#[test]
fn bundles_keep_one_owner_per_epoch_while_the_cluster_forms() {
    logs();
    let mut run = topology::cluster(SEED).expect("the cluster builds");
    run.observe(oracle::one_owner_per_epoch);
    run.worker(|trace| workload::one_activity_across_nodes(trace, QueuePlacement::Agreeing));
    let trace = run.drive([]).expect("the simulation runs to the end");
    assert!(
        trace.completed("workflow"),
        "the workflow did not complete:\n{trace}"
    );
    for holder in ["previous-generation", "node-a", "node-b"] {
        assert!(
            trace.saw(&format!("is owned by {holder}")),
            "the observer never saw {holder} own a bundle:\n{trace}"
        );
    }
}

/// A cluster run whose worker chooses a workflow homed on [`CUT_NODE`],
/// applies `cut` to that node, waits out its lease, then starts the workflow.
fn through_a_lapse(cut: Action) -> Trace {
    logs();
    let mut run = topology::cluster(SEED).expect("the cluster builds");
    run.observe(oracle::one_owner_per_epoch);
    run.worker(workload::work_on_a_lapsed_home);
    run.drive([Fault::when_seen(HOME_CHOSEN, cut)])
        .expect("the simulation runs to the end")
}

// The node keeps running and reaching its peers, but its renewals stall. Its
// leases lapse, and it must admit nothing on them from then on
// (`bundle-lease-fence`, Property 3: a lapsed owner admits nothing).
#[test]
#[ignore = "the edge's HistoryNotifyingRepository does not pass on bundle_lease_duration, so \
            the runtime never sets its lease deadline and serves the start on a lapsed lease"]
fn a_node_that_cannot_renew_admits_nothing_once_its_leases_lapse() {
    let trace = through_a_lapse(Action::StallRenewals(CUT_NODE));
    assert!(
        trace.saw("lease at epoch") && trace.saw("lapsed"),
        "no lease lapsed:\n{trace}"
    );
    assert!(
        !trace.saw(&format!("StartWorkflowExecution served by {CUT_NODE}")),
        "{CUT_NODE} admitted a start on a lapsed lease:\n{trace}"
    );
}

// Liveness: once the lease on the workflow's home lapses, the other node takes
// the home over at a higher epoch and serves the workflow to completion.
#[test]
#[ignore = "the controller offers a node only bundles nobody owns, whether or not the lease \
            has lapsed, so no node is offered the lapsed home"]
fn a_lapsed_home_is_taken_over_and_its_workflow_completes() {
    let trace = through_a_lapse(Action::StallRenewals(CUT_NODE));
    taken_over(&trace);
}

// As above, with the owner crashed rather than cut off.
#[test]
#[ignore = "the controller offers a node only bundles nobody owns, whether or not the lease \
            has lapsed, so no node is offered the crashed node's home"]
fn a_crashed_nodes_home_is_taken_over_and_its_workflow_completes() {
    let trace = through_a_lapse(Action::Crash(CUT_NODE));
    taken_over(&trace);
}

/// The other node started the workflow, so it owned the home, and the
/// workflow completed.
fn taken_over(trace: &Trace) {
    let other = topology::NODES
        .into_iter()
        .find(|host| *host != CUT_NODE)
        .expect("the cluster has a second node");
    assert!(
        trace.saw(&format!("StartWorkflowExecution served by {other}")),
        "{other} did not take the home over:\n{trace}"
    );
    assert!(
        trace.completed("workflow"),
        "the workflow did not complete:\n{trace}"
    );
}
