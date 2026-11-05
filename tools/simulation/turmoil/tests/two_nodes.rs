//! Step two: a placement controller and two engine nodes on turmoil.

use turmoil_sim::{
    Trace,
    cluster::{QueuePlacement, run_two_node_activity_workflow},
};

const SEED: u64 = 0x7e_6b_1a;

/// Engine logs, filtered by `RUST_LOG`, for diagnosing a failing run.
fn logs() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

fn story(trace: &Trace) -> String {
    trace.steps.join("\n")
}

#[test]
fn a_workflow_completes_when_its_tasks_start_across_nodes() {
    logs();
    let trace = run_two_node_activity_workflow(SEED, QueuePlacement::Agreeing)
        .expect("the simulation runs to the end");
    assert!(
        trace.completed(),
        "the workflow did not complete:\n{}",
        story(&trace)
    );
}

// Defect 1 from the brief: the edge sends an activity poll to the node it
// computes from the namespace name and the Activity kind, and the runtime
// serves it only at the home it computes from the namespace id and the
// Workflow kind. When those are different nodes, neither serves the poll.
#[test]
#[ignore = "fails until the edge and the runtime agree on an activity queue's home (defect 1)"]
fn an_activity_poll_is_served_on_the_node_the_edge_sends_it_to() {
    logs();
    let trace = run_two_node_activity_workflow(SEED, QueuePlacement::Disagreeing)
        .expect("the simulation runs to the end");
    assert!(
        trace.completed(),
        "the workflow did not complete:\n{}",
        story(&trace)
    );
}
