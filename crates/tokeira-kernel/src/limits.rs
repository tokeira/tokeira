//! Temporal v1.31.0's limits on what a request or a workflow task's completion
//! carries (`payload-admission-limits`, `workflow-task-command-limits`), on
//! how much a run may accumulate (`run-growth-limits`), and on how many signals
//! and updates a run may take (`signal-update-limits`).
//!
//! Tokeira has no setting for them. The edge checks requests against them, the
//! runtime hands them to the kernel for a completion's commands and an update's
//! admission and to the stores for each commit, and the kernel checks the signal
//! limit itself, so each value has one definition here.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// `limit.blobSize.error` (`common/dynamicconfig/constants.go:316-320 @ v1.31.0`).
pub const BLOB_SIZE_LIMIT_ERROR: usize = 2 * 1024 * 1024;
/// `limit.blobSize.warn` (`constants.go:321-325 @ v1.31.0`).
pub const BLOB_SIZE_LIMIT_WARN: usize = 512 * 1024;
/// `limit.memoSize.error` (`constants.go:326-330 @ v1.31.0`).
pub const MEMO_SIZE_LIMIT_ERROR: usize = 2 * 1024 * 1024;
/// `limit.memoSize.warn` (`constants.go:331-335 @ v1.31.0`).
pub const MEMO_SIZE_LIMIT_WARN: usize = 2 * 1024;
/// `frontend.searchAttributesNumberOfKeysLimit` (`constants.go:807-811 @ v1.31.0`).
pub const SEARCH_ATTRIBUTES_NUMBER_OF_KEYS_LIMIT: usize = 100;
/// `frontend.searchAttributesSizeOfValueLimit` (`constants.go:812-816 @ v1.31.0`).
pub const SEARCH_ATTRIBUTES_SIZE_OF_VALUE_LIMIT: usize = 2 * 1024;
/// `frontend.searchAttributesTotalSizeLimit` (`constants.go:817-821 @ v1.31.0`).
pub const SEARCH_ATTRIBUTES_TOTAL_SIZE_LIMIT: usize = 40 * 1024;
/// `component.nexusoperations.limit.operation.concurrency`
/// (`components/nexusoperations/config.go:36-43 @ v1.31.0`).
pub const PENDING_NEXUS_OPERATIONS_LIMIT: usize = 30;
/// The system Nexus endpoint, whose operations' input v1.31.0 doesn't check
/// (`common/nexus/constants.go:9`;
/// `components/nexusoperations/workflow/commands.go:144 @ v1.31.0`).
pub const SYSTEM_NEXUS_ENDPOINT: &str = "__temporal_system";

/// The size above which a payload is over a limit with these warn and error
/// values: `CheckEventBlobSizeLimit` errors only for a size above both
/// (`common/util.go:578-608 @ v1.31.0`).
pub const fn effective_limit(warn: usize, error: usize) -> usize {
    if warn > error { warn } else { error }
}

/// `limit.historySize.error` (`constants.go:360-364 @ v1.31.0`).
pub const HISTORY_SIZE_LIMIT_ERROR: usize = 50 * 1024 * 1024;
/// `limit.historySize.warn` (`constants.go:365-369 @ v1.31.0`).
pub const HISTORY_SIZE_LIMIT_WARN: usize = 10 * 1024 * 1024;
/// `limit.historyCount.error` (`constants.go:376-380 @ v1.31.0`).
pub const HISTORY_COUNT_LIMIT_ERROR: usize = 50 * 1024;
/// `limit.historyCount.warn` (`constants.go:381-385 @ v1.31.0`).
pub const HISTORY_COUNT_LIMIT_WARN: usize = 10 * 1024;
/// `limit.mutableStateSize.error` (`constants.go:397-401 @ v1.31.0`).
pub const MUTABLE_STATE_SIZE_LIMIT_ERROR: usize = 8 * 1024 * 1024;
/// `limit.mutableStateSize.warn` (`constants.go:402-406 @ v1.31.0`).
pub const MUTABLE_STATE_SIZE_LIMIT_WARN: usize = 1024 * 1024;
/// `system.transactionSizeLimit` (`constants.go:138-142`;
/// `common/primitives/constants.go:11 @ v1.31.0`).
pub const TRANSACTION_SIZE_LIMIT: usize = 4 * 1024 * 1024;
/// `history.maximumBufferedEventsBatch` (`constants.go:2340-2344 @ v1.31.0`).
pub const MAXIMUM_BUFFERED_EVENTS_BATCH: usize = 100;
/// `history.maximumBufferedEventsSizeInBytes` (`constants.go:2345-2350 @ v1.31.0`).
pub const MAXIMUM_BUFFERED_EVENTS_SIZE_IN_BYTES: usize = 2 * 1024 * 1024;
/// `limit.mutableStateActivityFailureSize.error` (`constants.go:386-391 @ v1.31.0`).
pub const MUTABLE_STATE_ACTIVITY_FAILURE_SIZE_LIMIT_ERROR: usize = 4 * 1024;

/// The limits a store checks each commit's growth against. The runtime
/// resolves them and puts them on the transitions it commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunGrowthLimits {
    /// The stored History Size above which a write terminates the run.
    pub history_size_error: usize,
    /// The stored History Size above which a write is logged.
    pub history_size_warn: usize,
    /// The event count above which a write terminates the run.
    pub history_count_error: usize,
    /// The event count above which a write is logged.
    pub history_count_warn: usize,
    /// The measured state size above which a write terminates the run.
    pub state_size_error: usize,
    /// The measured state size above which a write is logged.
    pub state_size_warn: usize,
    /// The encoded history batch size above which a write is refused.
    pub transaction_size: usize,
}

impl Default for RunGrowthLimits {
    /// v1.31.0's values.
    fn default() -> Self {
        Self {
            history_size_error: HISTORY_SIZE_LIMIT_ERROR,
            history_size_warn: HISTORY_SIZE_LIMIT_WARN,
            history_count_error: HISTORY_COUNT_LIMIT_ERROR,
            history_count_warn: HISTORY_COUNT_LIMIT_WARN,
            state_size_error: MUTABLE_STATE_SIZE_LIMIT_ERROR,
            state_size_warn: MUTABLE_STATE_SIZE_LIMIT_WARN,
            transaction_size: TRANSACTION_SIZE_LIMIT,
        }
    }
}

/// Which of a run's growth limits a commit breached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunLimit {
    /// The stored History Size was already over its limit.
    HistorySize,
    /// The commit would leave more events than the history count limit.
    HistoryCount,
    /// The commit would leave the measured state over its limit.
    StateSize,
    /// The commit's history batch encodes to more than the transaction size
    /// limit.
    TransactionSize,
}

impl RunLimit {
    /// The reason v1.31.0 terminates a run with for this limit
    /// (`common/util.go:105-111 @ v1.31.0`).
    pub fn termination_reason(self) -> &'static str {
        match self {
            Self::HistorySize => "Workflow history size exceeds limit.",
            Self::HistoryCount => "Workflow history count exceeds limit.",
            Self::StateSize => "Workflow mutable state size exceeds limit.",
            Self::TransactionSize => "Transaction size exceeds limit.",
        }
    }
}

/// A commit a store refused for one of the run's growth limits. Nothing of the
/// commit was written; the caller is answered `InvalidArgument` with
/// [`Self::message`].
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct RunLimitExceeded {
    /// The limit the commit breached.
    pub limit: RunLimit,
    /// v1.31.0's message for the breach.
    pub message: String,
}

impl RunLimitExceeded {
    /// A breach of the history size, count or state size limit, whose message
    /// is the termination reason (`service/history/consts/const.go:81-85 @
    /// v1.31.0`).
    pub fn of(limit: RunLimit) -> Self {
        Self {
            limit,
            message: limit.termination_reason().to_owned(),
        }
    }

    /// A history batch over the transaction size limit
    /// (`common/persistence/history_manager.go:367-373 @ v1.31.0`).
    pub fn transaction_size(size: usize, limit: usize) -> Self {
        Self {
            limit: RunLimit::TransactionSize,
            message: format!("transaction size of {size} bytes exceeds limit of {limit} bytes"),
        }
    }
}

/// `history.maximumSignalsPerExecution` (`constants.go:2351-2355 @ v1.31.0`).
/// The kernel reads it as a constant: no corpus test overrides it.
pub const MAXIMUM_SIGNALS_PER_EXECUTION: u64 = 10_000;
/// `history.maxInFlightUpdates` (`constants.go:2289-2293 @ v1.31.0`).
pub const MAX_IN_FLIGHT_UPDATES: usize = 10;
/// `history.maxInFlightUpdatePayloads` (`constants.go:2294-2298 @ v1.31.0`).
pub const MAX_IN_FLIGHT_UPDATE_PAYLOADS: usize = 20 * 1024 * 1024;
/// `history.maxTotalUpdates` (`constants.go:2299-2303 @ v1.31.0`).
pub const MAX_TOTAL_UPDATES: usize = 2_000;

/// v1.31.0's `InvalidArgument` for a signal to a run at the signal limit
/// (`consts.ErrSignalsLimitExceeded`, `service/history/consts/const.go:60-61
/// @ v1.31.0`).
pub const SIGNAL_LIMIT_EXCEEDED_MESSAGE: &str =
    "exceeded workflow execution limit for signal events";

/// The limits the kernel checks an update against when it admits one, or
/// re-admits one a worker accepted or rejected without the run holding it.
/// The runtime resolves them and puts them on the command.
///
/// A limit of 0 disables its check, as v1.31.0 reads each of them
/// (`update/registry.go:398-453 @ v1.31.0`). Only the Temporal functional
/// harness's build can set one; every other build uses [`Self::default`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateLimits {
    /// The in-flight updates, accepted or held, at which a new update is
    /// refused.
    pub in_flight: usize,
    /// The request bytes of the held updates, with the new request's, at which
    /// a new update is refused.
    pub in_flight_payloads: usize,
    /// The updates in flight and completed at which a new update is refused.
    pub total: usize,
}

impl Default for UpdateLimits {
    /// v1.31.0's values.
    fn default() -> Self {
        Self {
            in_flight: MAX_IN_FLIGHT_UPDATES,
            in_flight_payloads: MAX_IN_FLIGHT_UPDATE_PAYLOADS,
            total: MAX_TOTAL_UPDATES,
        }
    }
}

/// Which of a run's update limits refused an update.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpdateLimit {
    /// The run's in-flight updates reached the in-flight limit.
    InFlight,
    /// The held updates' requests, with the new one, reached the payload limit.
    InFlightPayloads,
    /// The run's updates in flight and completed reached the total limit.
    Total,
}

/// An update the kernel refused for one of the run's update limits; nothing
/// was admitted. The edge answers the in-flight and payload limits with
/// `ResourceExhausted` (cause `CONCURRENT_LIMIT`, scope `NAMESPACE`) and the
/// total with `FailedPrecondition`, each with [`Self::message`].
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct UpdateLimitExceeded {
    /// The limit the update reached.
    pub limit: UpdateLimit,
    /// v1.31.0's message for it.
    pub message: String,
}

impl UpdateLimitExceeded {
    /// The in-flight limit (`checkInFlightLimit`, `registry.go:398-413 @ v1.31.0`).
    pub fn in_flight(limit: usize) -> Self {
        Self {
            limit: UpdateLimit::InFlight,
            message: format!(
                "limit on number of concurrent in-flight updates has been reached ({limit})"
            ),
        }
    }

    /// The payload limit (`payloadSizeLimiter`, `registry.go:415-436 @ v1.31.0`).
    pub fn in_flight_payloads(limit: usize) -> Self {
        Self {
            limit: UpdateLimit::InFlightPayloads,
            message: format!(
                "limit on total payload size of in-flight updates has been reached ({limit} bytes)"
            ),
        }
    }

    /// The total limit (`checkTotalLimit`, `registry.go:438-453 @ v1.31.0`).
    pub fn total(limit: usize) -> Self {
        Self {
            limit: UpdateLimit::Total,
            message: format!(
                "The limit on the total number of distinct updates in this workflow has been \
                 reached ({limit}). Make sure any duplicate updates share an Update ID so the \
                 server can deduplicate them, and consider rejecting updates that you aren't \
                 going to process. You can also Continue-as-New to avoid this; we recommend you \
                 check Continue-as-New Suggested in your Workflow."
            ),
        }
    }
}

/// Check an update with a new id against the run's update limits, in v1.31.0's
/// order: the in-flight count, then the total, then the payload
/// (`FindOrCreate` then `Admit`, `registry.go:226-236`, `update.go:301-311 @
/// v1.31.0`). Each compares with `>=`, as v1.31.0 does: a run at a limit takes
/// no more.
///
/// `in_flight` counts the accepted updates and the held ones, never an
/// admitted update whose request a restart lost, which v1.31.0 would have
/// forgotten (`signal-update-limits` criterion 2.9).
pub fn check_update_admission(
    limits: &UpdateLimits,
    in_flight: usize,
    completed: usize,
    in_flight_request_bytes: u64,
    request_bytes: u64,
) -> Result<(), UpdateLimitExceeded> {
    if limits.in_flight > 0 && in_flight >= limits.in_flight {
        return Err(UpdateLimitExceeded::in_flight(limits.in_flight));
    }
    check_update_total(limits.total, in_flight, completed)?;
    let payload_limit = u64::try_from(limits.in_flight_payloads).unwrap_or(u64::MAX);
    if payload_limit > 0 && in_flight_request_bytes.saturating_add(request_bytes) >= payload_limit {
        return Err(UpdateLimitExceeded::in_flight_payloads(
            limits.in_flight_payloads,
        ));
    }
    Ok(())
}

/// Check the total limit alone, as v1.31.0 does when a worker accepts or
/// rejects an update its registry doesn't hold (`TryResurrect`,
/// `registry.go:238-249 @ v1.31.0`).
pub fn check_update_total(
    total_limit: usize,
    in_flight: usize,
    completed: usize,
) -> Result<(), UpdateLimitExceeded> {
    if total_limit > 0 && in_flight.saturating_add(completed) >= total_limit {
        return Err(UpdateLimitExceeded::total(total_limit));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_limit_messages_are_v1_31_0s() {
        assert_eq!(
            UpdateLimitExceeded::in_flight(MAX_IN_FLIGHT_UPDATES).message,
            "limit on number of concurrent in-flight updates has been reached (10)"
        );
        assert_eq!(
            UpdateLimitExceeded::in_flight_payloads(MAX_IN_FLIGHT_UPDATE_PAYLOADS).message,
            "limit on total payload size of in-flight updates has been reached (20971520 bytes)"
        );
        assert_eq!(
            UpdateLimitExceeded::total(MAX_TOTAL_UPDATES).message,
            "The limit on the total number of distinct updates in this workflow has been reached \
             (2000). Make sure any duplicate updates share an Update ID so the server can \
             deduplicate them, and consider rejecting updates that you aren't going to process. \
             You can also Continue-as-New to avoid this; we recommend you check Continue-as-New \
             Suggested in your Workflow."
        );
    }
}
