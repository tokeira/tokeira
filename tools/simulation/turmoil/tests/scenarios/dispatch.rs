//! Activity dispatch: attempts, timeouts, retries and completions.
//!
//! Property: each activity attempt starts at most once, and only the live
//! attempt can complete the activity (`AtMostOneStartPerIncarnation`,
//! `spec/tla/40_dispatch_handoff`).

use std::time::Duration;

use tokeira_proto::enums::EventType;
use turmoil_sim::{
    WORKER,
    faults::{Action, Fault},
    oracle,
    topology::{self, NODE},
    workload::{self, ACTIVITY_TIMEOUT, FIRST_ATTEMPT_RECEIVED},
};

use crate::{SEED, logs};

// The worker's completion of the first attempt is held on the network until
// the attempt has timed out and been retried. The late completion must be
// refused, and the retried attempt completes the activity.
#[test]
fn a_completion_held_past_its_timeout_is_refused_and_the_retry_completes() {
    logs();
    let mut run = topology::single_node(SEED);
    run.worker(workload::late_completion);
    let faults = [
        Fault::when_seen(FIRST_ATTEMPT_RECEIVED, Action::Hold(WORKER, NODE)),
        Fault::after_seen(
            FIRST_ATTEMPT_RECEIVED,
            ACTIVITY_TIMEOUT + Duration::from_secs(10),
            Action::Release(WORKER, NODE),
        ),
    ];
    let trace = run.drive(faults).expect("the simulation runs to the end");
    assert!(
        trace.completed("workflow"),
        "the workflow did not complete:\n{trace}"
    );
    assert!(
        trace.saw("completion of attempt 1 refused"),
        "the late completion was not refused:\n{trace}"
    );
    assert!(
        trace.saw("the activity completed on attempt 2"),
        "the retry did not complete the activity:\n{trace}"
    );
    let history = trace.history("workflow");
    for kind in [
        EventType::ActivityTaskScheduled,
        EventType::ActivityTaskStarted,
        EventType::ActivityTaskCompleted,
    ] {
        oracle::exactly_once(history, kind).unwrap_or_else(|error| panic!("{error}:\n{trace}"));
    }
    // The started event records the attempt that completed, which the node
    // could hand out only after the first timed out.
    oracle::at_least_apart(
        history,
        EventType::ActivityTaskScheduled,
        EventType::ActivityTaskStarted,
        ACTIVITY_TIMEOUT,
    )
    .unwrap_or_else(|error| panic!("{error}:\n{trace}"));
}
