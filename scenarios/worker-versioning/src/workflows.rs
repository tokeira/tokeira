//! Shared workflow + activity for the worker-versioning scenario.
//!
//! The same workflow type is registered by every versioned worker. Which build a
//! given run executes on is decided entirely by Tokeira's deployment routing
//! (current/ramping version + the run's versioning behaviour), not by the workflow
//! code — so a single definition is all the scenario needs. The activity returns the
//! build id of the worker that ran it, which is how the starter observes *which*
//! version handled a run.

#![allow(unreachable_pub)]

use std::time::Duration;

use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

/// Workflow that reports the build id of the worker that executed it.
///
/// It calls a single activity whose only job is to echo the build id baked into the
/// worker process. Because the activity runs on whichever worker the task was
/// dispatched to, the returned string is a direct, observable signal of the routing
/// decision the server made for this run.
#[workflow]
#[derive(Default)]
pub struct VersionProbeWorkflow;

#[workflow_methods]
impl VersionProbeWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let build_id = ctx
            .start_activity(
                VersionActivities::report_build_id,
                (),
                ActivityOptions {
                    start_to_close_timeout: Some(Duration::from_secs(10)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(build_id)
    }
}

/// Build id of the worker hosting these activities, injected at worker startup.
pub struct VersionActivities {
    pub build_id: String,
}

#[activities]
impl VersionActivities {
    /// Return the build id of the worker that executed this activity.
    ///
    /// Instance activities take `self: Arc<Self>` as the receiver, so the activity
    /// reads the build id baked into this worker process.
    #[activity]
    pub async fn report_build_id(
        self: std::sync::Arc<Self>,
        _ctx: ActivityContext,
    ) -> Result<String, ActivityError> {
        Ok(self.build_id.clone())
    }
}
