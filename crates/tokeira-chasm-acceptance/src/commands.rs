//! Resource command results and validation. Engine-facing orchestration must keep
//! generation checks inside the transition that mutates the resource.

use thiserror::Error;
use tokeira_chasm::{ChasmError, DeploymentVersionTarget, MutableContext};

use crate::{Operation, Resource, ResourceState, reconcile::stage_start};

/// Initial converged generation. Runtime orchestration supplies the request id to
/// start policy and invokes this only as initial data for a creating transaction.
pub fn create(
    request_id: &str,
    digest: &str,
    target: DeploymentVersionTarget,
    task_queue: &str,
) -> ResourceState {
    ResourceState {
        create_request_id: request_id.into(),
        create_digest: digest.into(),
        desired_digest: digest.into(),
        desired_generation: 1,
        observed_generation: 1,
        target,
        task_queue: task_queue.into(),
        ..Default::default()
    }
}

/// Decide an engine-admitted same-request repeat against the immutable create
/// input, even when updates have since changed the desired digest or generation.
pub fn repeat_create(state: &ResourceState, digest: &str) -> Result<u64, AcceptanceError> {
    if state.create_digest == digest {
        Ok(1)
    } else {
        Err(AcceptanceError::Conflict)
    }
}

/// Apply an optimistic update inside the caller's fenced transition. A mismatch
/// must abort that transition; an active operation keeps its original input.
pub fn update(
    resource: &mut Resource,
    expected_generation: u64,
    digest: &str,
    ctx: &mut dyn MutableContext,
) -> Result<u64, AcceptanceError> {
    let state = resource.state_mut()?;
    if state.desired_generation != expected_generation {
        return Err(AcceptanceError::GenerationMismatch {
            expected: expected_generation,
            actual: state.desired_generation,
        });
    }
    // Visibility indexes signed integers, so reject overflow before any mutation.
    let next = state
        .desired_generation
        .checked_add(1)
        .filter(|generation| *generation <= i64::MAX as u64)
        .ok_or_else(|| {
            ChasmError::Validation("resource generation exceeds visibility integer range".into())
        })?;
    state.desired_generation = next;
    state.desired_digest = digest.into();
    if state.desired_generation > state.observed_generation && state.active_operation.is_none() {
        stage_start(state, ctx)?;
    }
    Ok(next)
}

/// Read the current immutable view without staging work.
pub fn read(resource: &Resource) -> Result<ResourceView, ChasmError> {
    Ok(ResourceView::from(resource.state()?))
}

/// Stable acceptance-level errors; engine failures retain their original cause.
#[derive(Debug, Error)]
pub enum AcceptanceError {
    /// The original create request id was reused with a different input digest.
    #[error("create request was repeated with different input")]
    Conflict,
    /// Another create request already owns this live resource.
    #[error("resource already started as run {run_id}")]
    AlreadyStarted {
        /// Existing run, returned by the engine's business-id policy.
        run_id: String,
    },
    /// An optimistic generation precondition failed without applying an update.
    #[error("expected generation {expected}, actual generation {actual}")]
    GenerationMismatch {
        /// Caller's precondition.
        expected: u64,
        /// Desired generation at the rejected transition's read.
        actual: u64,
    },
    /// Storage, registry or transition failure unrelated to command preconditions.
    #[error(transparent)]
    Engine(#[from] ChasmError),
}

/// Owned read snapshot; history is oldest-first and never exceeds eight operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceView {
    /// Latest requested generation.
    pub desired_generation: u64,
    /// Latest successfully reconciled generation.
    pub observed_generation: u64,
    /// The one outstanding activity, if any.
    pub active_operation: Option<Operation>,
    /// The latest eight terminal operations.
    pub history: Vec<Operation>,
    /// Failure count since the last successful reconciliation.
    pub retry_attempt: u32,
    /// Encoded Temporal Failure, empty when there is no outstanding failure.
    pub last_failure: Vec<u8>,
    /// The same keyword emitted as DeploymentStatus in visibility.
    pub status: &'static str,
}

impl From<&ResourceState> for ResourceView {
    fn from(state: &ResourceState) -> Self {
        Self {
            desired_generation: state.desired_generation,
            observed_generation: state.observed_generation,
            active_operation: state.active_operation.clone(),
            history: state.history.clone(),
            retry_attempt: state.retry_attempt,
            last_failure: state.last_failure.clone(),
            status: state.status(),
        }
    }
}
