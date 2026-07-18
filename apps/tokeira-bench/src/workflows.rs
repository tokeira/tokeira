//! Zero-activity echo workflow used by the bench.
//!
//! The workflow takes a `String` and returns it verbatim. No activity, no
//! timer, no state — just enough shape to measure the raw start→complete
//! round-trip that gRPC edge, runtime, and storage have to sustain.

#![allow(unreachable_pub)]
// The SDK's workflow macros generate public types we cannot add derives to.
#![allow(missing_debug_implementations)]

use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowResult};

#[workflow]
#[derive(Default, Debug)]
pub struct EchoWorkflow;

#[workflow_methods]
impl EchoWorkflow {
    #[run]
    pub async fn run(_ctx: &mut WorkflowContext<Self>, input: String) -> WorkflowResult<String> {
        Ok(input)
    }
}
