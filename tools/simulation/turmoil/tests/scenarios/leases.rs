//! Bundle leases: who owns each bundle, and at which epoch.
//!
//! Property: at most one node owns a bundle at an epoch, and a bundle's
//! epoch never goes back (`OneOwnerPerEpoch`, `spec/tla/30_bundle_lease`).
//! The oracle checks it on every tick against the shared store.

use turmoil_sim::{
    oracle, topology,
    workload::{self, QueuePlacement},
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
