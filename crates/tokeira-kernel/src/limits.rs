//! Temporal v1.31.0's limits on what a request or a workflow task's completion
//! carries (`payload-admission-limits`, `workflow-task-command-limits`).
//!
//! Tokeira has no setting for them. The edge checks requests against them and
//! the runtime hands them to the kernel for a completion's commands, so each
//! value has one definition here.

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
