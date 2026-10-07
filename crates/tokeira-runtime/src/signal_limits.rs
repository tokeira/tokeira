//! The runtime's part in `signal-update-limits` for signals: answering a signal
//! the run refused as v1.31.0 answers it.
//!
//! The kernel refuses a signal to a closed run (`Reject::RunClosed`) and to a run
//! at the signal limit (`Reject::SignalLimitExceeded`), and the runtime refuses one
//! to a closing run. v1.31.0 checks a SignalWorkflowExecution's request id before
//! any of these (`service/history/api/signalworkflow/api.go:40-66 @ v1.31.0`), so a
//! repeat of a signal the run already applied succeeds. Tokeira's stores check
//! request ids only when they commit, after the kernel, so before a refusal stands
//! the caller asks the store whether the run applied the request id
//! ([`run_applied_request`]). A signal from another workflow reuses its request id
//! on every delivery, so the publisher asks the same question.
//!
//! SignalWithStart's signal to a running run never asks: v1.31.0 checks its count
//! and closing before its request id (`signal_with_start_workflow.go:273-300 @
//! v1.31.0`).

use anyhow::Result;
use tokeira_kernel::Reject;
use tokeira_storage::RunRepository;
use tokeira_types::{ExecutionRef, NamespaceId, RequestId, RunKey, WorkflowId};

use crate::lane::KernelRejected;

/// Why the kernel refused a signal, when it is a refusal a duplicate answers
/// before.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SignalRefusal {
    /// The run had closed.
    RunClosed,
    /// The run had reached the signal limit.
    SignalLimit,
}

impl SignalRefusal {
    /// The refusal `error` carries, if it is one.
    pub(crate) fn of(error: &anyhow::Error) -> Option<Self> {
        match error.downcast_ref::<KernelRejected>()? {
            KernelRejected(Reject::RunClosed(_)) => Some(Self::RunClosed),
            KernelRejected(Reject::SignalLimitExceeded) => Some(Self::SignalLimit),
            _ => None,
        }
    }

    /// The cause a signal from another workflow fails with on its sender:
    /// v1.31.0's target answers a closed run `NotFound` and the signal limit
    /// `InvalidArgument`, which its transfer executor records as these
    /// (`transfer_queue_active_task_executor.go:708-736 @ v1.31.0`). The history
    /// serializer renders each name as its enum value.
    pub(crate) fn external_cause(self) -> &'static str {
        match self {
            Self::RunClosed => EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
            Self::SignalLimit => SIGNAL_COUNT_LIMIT_EXCEEDED,
        }
    }
}

/// `SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND`.
pub(crate) const EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND: &str =
    "EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND";

/// `SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_SIGNAL_COUNT_LIMIT_EXCEEDED`.
pub(crate) const SIGNAL_COUNT_LIMIT_EXCEEDED: &str = "SIGNAL_COUNT_LIMIT_EXCEEDED";

/// Whether the run `run_key` has already applied `request_id`.
///
/// The store keys request ids by workflow, so the record is matched to this run:
/// a request id an earlier run of the workflow applied is new to this one, as
/// v1.31.0 keeps request ids in each run's own state. An empty request id was
/// never recorded.
pub(crate) async fn run_applied_request<R: RunRepository + ?Sized>(
    repo: &R,
    run_key: RunKey,
    namespace_id: NamespaceId,
    workflow_id: &WorkflowId,
    request_id: &RequestId,
) -> Result<bool> {
    if request_id.0.is_empty() {
        return Ok(false);
    }
    let execution = ExecutionRef {
        namespace_id,
        workflow_id: workflow_id.clone(),
        run_id: None,
    };
    Ok(repo
        .lookup_request_dedupe(&execution, request_id)
        .await?
        .is_some_and(|record| record.run_key == run_key))
}
