//! The outbound plane: work one run starts or delivers on another run's
//! home, such as a child workflow and its result.
//!
//! Property: each child starts once, and its result reaches its parent once.

use tokeira_proto::enums::EventType;
use turmoil_sim::{oracle, topology, workload};

use crate::{SEED, logs};

#[test]
fn a_child_homed_on_the_other_node_starts_once_and_reports_back_once() {
    logs();
    let mut run = topology::cluster(SEED).expect("the cluster builds");
    run.worker(workload::child_across_homes);
    let trace = run.drive([]).expect("the simulation runs to the end");
    assert!(
        trace.completed("parent"),
        "the parent did not complete:\n{trace}"
    );
    assert!(
        trace.completed("child"),
        "the child did not complete:\n{trace}"
    );
    let check = |role: &str, kind: EventType| {
        oracle::exactly_once(trace.history(role), kind)
            .unwrap_or_else(|error| panic!("{role}: {error}:\n{trace}"));
    };
    check("parent", EventType::StartChildWorkflowExecutionInitiated);
    check("parent", EventType::ChildWorkflowExecutionStarted);
    check("parent", EventType::ChildWorkflowExecutionCompleted);
    check("child", EventType::WorkflowExecutionStarted);
}
