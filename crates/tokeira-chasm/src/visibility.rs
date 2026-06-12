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
pub trait SearchAttributeProvider {
    /// The search attributes this component currently contributes.
    fn search_attributes(&self) -> SearchAttributes;
}
