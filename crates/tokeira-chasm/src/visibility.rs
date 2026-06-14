//! The built-in visibility provider hook (Requirement 10).
//!
//! In CHASM, visibility is a **built-in component concern**, not a bolted-on index
//! (`chasm/lib/visibility/visibility.go @ v1.31.0`): a component declares the search
//! attributes it contributes, and the engine collects them on transition close and
//! emits them as derived projection writes — off the correctness path (`AGENTS §3`).
//!
//! This module owns the **provider** half of that contract, which is pure: a
//! component-level declaration with no I/O. The **sink** half (writing to
//! `tokeira-projection`) lives in the runtime, which calls
//! [`SearchAttributeProvider::search_attributes`] on transition close.

use tokeira_types::{Memo, SearchAttributes as TypedSearchAttributes};

use crate::component::LifecycleState;

/// Component-contributed search attributes, as `(name, value)` string pairs.
///
/// Kept as strings at the substrate boundary for the MVP; richer typed search
/// attribute values ride the existing search-attribute work in the projection
/// plane (Requirement 10.4). For the activity archetype these are `ActivityType`,
/// `ExecutionStatus`, and `TaskQueue`.
pub type SearchAttributes = Vec<(String, String)>;

/// A component's declaration of the search attributes it contributes — the provider
/// half of CHASM's built-in visibility (Requirement 10.1, 10.2).
///
/// A component implements this to surface its discoverable attributes; the engine
/// collects them when a transition closes and forwards them to the projection sink.
/// It is pure: no I/O, no mutation.
///
/// Superseded by [`VisibilityContributor`], which carries the full typed
/// post-transition image rather than only string-pair attributes. The two coexist
/// during the Stage 2 migration; the engine reads [`VisibilityContributor`] and
/// this trait is removed once no caller depends on it.
pub trait SearchAttributeProvider {
    /// The search attributes this component currently contributes.
    fn search_attributes(&self) -> SearchAttributes;
}

/// A component's **typed** visibility contribution, produced on transition close
/// (Requirement 10.2, 10.10). This is the archetype-neutral post-transition image a
/// component knows about itself; the runtime adapter stamps the fields the component
/// cannot know — the version `(authority_epoch, source_transition_seq)` (from the
/// transition's [`VersionedTransition`](crate::versioned_transition::VersionedTransition)),
/// the `namespace_id`/`run_key`, and the `archetype_id` (resolved from the
/// component's FQN via the registry) — before writing it to the shared visibility
/// index.
///
/// Reserved system fields (`archetype`, `status`, `lifecycle_state`, `namespace`,
/// `run_id`, `business_id`, plus `execution_type`/`task_queue`) live in the typed
/// struct fields below and are **never** read from [`search_attributes`](Self::search_attributes);
/// a component therefore cannot spoof a reserved field by declaring a like-named
/// user search attribute (Requirement 10.10). The adapter additionally rejects any
/// reserved name appearing in `search_attributes`.
///
/// `status` is logically a system search attribute but physically a column, so it is
/// carried as [`status_keyword`](Self::status_keyword) (a generic, archetype-
/// interpreted keyword) rather than a workflow-typed enum (design "Record shape").
#[derive(Debug, Clone, PartialEq)]
pub struct VisibilitySnapshot {
    /// Generic, low-cardinality status keyword, interpreted per archetype (e.g. the
    /// activity archetype emits `"Scheduled"`/`"Started"`/`"Completed"`/…). Maps to
    /// the `status_keyword` column.
    pub status_keyword: String,
    /// The component's lifecycle at close; the adapter maps `Running` → `OPEN` and
    /// `Completed`/`Failed` → `CLOSED` for the generic `lifecycle_state` column,
    /// which is distinct from `status` (design "Record shape").
    pub lifecycle_state: LifecycleState,
    /// The execution's type (e.g. the activity type / workflow type), or `None` when
    /// the archetype has no type. Maps to the `execution_type` system column.
    pub execution_type: Option<String>,
    /// The task queue, or `None` when not applicable. Maps to the `task_queue`
    /// system column.
    pub task_queue: Option<String>,
    /// When the execution started, in unix nanoseconds, if known.
    pub start_time_unix_nanos: Option<i64>,
    /// When the execution closed, in unix nanoseconds; `None` while open.
    pub close_time_unix_nanos: Option<i64>,
    /// Typed **user** search attributes (range/order/parse correctness needs types,
    /// so these are not strings — Requirement 10.2). Must not contain a reserved
    /// system-field name; the adapter enforces this (Requirement 10.10).
    pub search_attributes: TypedSearchAttributes,
    /// The execution's memo blob.
    pub memo: Memo,
}

/// A component's typed visibility contribution hook (Requirement 10.2).
///
/// The engine calls [`visibility_snapshot`](Self::visibility_snapshot) when a
/// transition closes and hands the result to the projection adapter. It is pure: no
/// I/O, no mutation. Returning `None` means the component contributes nothing this
/// transition (e.g. its state is not materialized), and no projection write occurs.
pub trait VisibilityContributor {
    /// The typed post-transition visibility image this component contributes, or
    /// `None` to contribute nothing.
    fn visibility_snapshot(&self) -> Option<VisibilitySnapshot>;
}
