//! Temporal v1.31.0's limits on what a request or a workflow task's completion
//! carries (`payload-admission-limits`, `workflow-task-command-limits`), and on
//! how much a run may accumulate (`run-growth-limits`).
//!
//! Tokeira has no setting for them. The edge checks requests against them, the
//! runtime hands them to the kernel for a completion's commands and to the
//! stores for each commit, so each value has one definition here.

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
