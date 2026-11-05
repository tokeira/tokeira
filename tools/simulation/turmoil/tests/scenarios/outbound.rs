//! The outbound plane: work one run starts or delivers on another run's
//! home, such as a child workflow and its result.
//!
//! Property: each child starts once, and its result reaches its parent once,
//! however the link between the two homes fails and recovers meanwhile.

use std::time::Duration;

use tokeira_proto::enums::EventType;
use turmoil_sim::{
    Trace,
    faults::{Action, Fault},
    oracle,
    topology::{self, NODES},
    workload::{self, COMPLETING_THE_CHILD, STARTING_THE_CHILD},
};

use crate::{logs, seed};

#[test]
fn a_child_homed_on_the_other_node_starts_once_and_reports_back_once() {
    let trace = child_across_homes([]);
    started_once_and_reported_back_once(&trace);
}

// The link between the nodes is held from just before the parent's home
// commits the child's start until ten seconds later. Nothing is lost, only
// late.
#[test]
fn a_child_started_across_a_held_link_starts_once_and_reports_back_once() {
    let trace = child_across_homes(link_down(STARTING_THE_CHILD, Action::Hold, Action::Release));
    started_once_and_reported_back_once(&trace);
}

// As above, with the link partitioned: what is sent meanwhile is lost, and a
// connection open across it stops making progress.
#[test]
fn a_child_started_across_a_partitioned_link_starts_once_and_reports_back_once() {
    let trace = child_across_homes(link_down(
        STARTING_THE_CHILD,
        Action::Partition,
        Action::Repair,
    ));
    started_once_and_reported_back_once(&trace);
}

// The link is partitioned from just before the child's home commits the
// child's completion, so the result's delivery to the parent meets it.
#[test]
fn a_child_result_sent_across_a_partitioned_link_reaches_the_parent_once() {
    let trace = child_across_homes(link_down(
        COMPLETING_THE_CHILD,
        Action::Partition,
        Action::Repair,
    ));
    started_once_and_reported_back_once(&trace);
}

/// How long the link between the nodes stays down.
const LINK_DOWN: Duration = Duration::from_secs(10);

/// Take the link between the two nodes down when the worker records `seen`,
/// and bring it back [`LINK_DOWN`] later.
fn link_down(
    seen: &'static str,
    down: fn(&'static str, &'static str) -> Action,
    up: fn(&'static str, &'static str) -> Action,
) -> [Fault; 2] {
    let [a, b] = NODES;
    [
        Fault::when_seen(seen, down(a, b)),
        Fault::after_seen(seen, LINK_DOWN, up(a, b)),
    ]
}

fn child_across_homes(faults: impl IntoIterator<Item = Fault>) -> Trace {
    logs();
    let mut run = topology::cluster(seed()).expect("the cluster builds");
    run.worker(workload::child_across_homes);
    run.drive(faults).expect("the simulation runs to the end")
}

fn started_once_and_reported_back_once(trace: &Trace) {
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
