use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Typed value for a single search attribute.
///
/// These variants intentionally mirror the column types we
/// expect to be indexable in a SQL-native visibility store.
/// Adding a new variant here means the projection plane and
/// the visibility query planner both need updating.
///
/// See `docs/architecture/080-sql-visibility.md` for the
/// mapping between these variants and SQL column types.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SearchAttrValue {
    /// Exact-match string (indexed, not full-text).
    Keyword(String),
    /// List of exact-match strings (multi-value keyword).
    KeywordList(Vec<String>),
    /// Signed 64-bit integer.
    Int(i64),
    /// IEEE 754 double-precision float.
    Double(f64),
    /// Boolean flag.
    Bool(bool),
    /// Timestamp with timezone.
    Datetime(OffsetDateTime),
    /// Full-text searchable string.
    Text(String),
}

/// Map of user-defined search attributes attached to a
/// workflow execution.
///
/// The projection plane indexes these values so that
/// visibility queries can filter and sort on them. Keys are
/// attribute names registered in the namespace configuration;
/// values are typed via [`SearchAttrValue`].
///
/// A `BTreeMap` is used for deterministic serialisation order.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchAttributes(pub BTreeMap<String, SearchAttrValue>);

/// Predefined (server-internal) search attributes that user requests may NOT
/// set: `sadefs.Predefined()` minus `sadefs.PredefinedWhiteList()`
/// (`common/searchattribute/sadefs/constants.go:154-192 @ v1.31.0`). The
/// whitelisted predefined attributes (TemporalChangeVersion, BinaryChecksums,
/// BuildIds, BatcherNamespace, BatcherUser, TemporalScheduledStartTime,
/// TemporalScheduledById, TemporalSchedulePaused, TemporalNamespaceDivision,
/// TemporalPauseInfo, TemporalReportedProblems) remain user-settable; the
/// validator rejects only the internal-only remainder
/// (`searchattribute/validator.go:118-120 @ v1.31.0`).
pub fn is_banned_predefined_search_attribute(name: &str) -> bool {
    matches!(
        name,
        "TemporalWorkerDeploymentVersion"
            | "TemporalWorkflowVersioningBehavior"
            | "TemporalWorkerDeployment"
            | "TemporalUsedWorkerDeploymentVersions"
            | "TemporalExternalPayloadCount"
            | "TemporalExternalPayloadSizeBytes"
    )
}
