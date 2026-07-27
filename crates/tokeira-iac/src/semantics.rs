//! The change-semantics vocabulary: what a change *does* to a running
//! resource, declared by the kind that owns the provider.
//!
//! Lives in this crate — not in `tokeira-explain` — because the declaration
//! point is a method on [`Resource`](crate::Resource), and this crate cannot
//! depend on a crate that depends on it (recorded as the Feature 1 amendment
//! in `.kiro/specs/explanation-change-semantics`). `tokeira-explain`
//! re-exports the vocabulary so its public surface carries it.
//!
//! Invariants owned here:
//! - **`Unknown` is the default of every field** (umbrella decision D4): a
//!   kind that declares nothing yields uncertainty, never a
//!   confident-sounding default.
//! - **A claim carries its citation structurally**: `EngineFact` and
//!   `ProviderGuarantee` cannot be constructed without one, so an uncited
//!   claim is unrepresentable rather than merely discouraged.

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

/// How the provider effects the change. Distinct from
/// [`ChangeKind`](crate::ChangeKind), which describes *state reconciliation*:
/// a compose service `Update` is effected as `Replaced` because the provider
/// path stops and recreates the container.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LifecycleOperation {
    Created,
    UpdatedInPlace,
    Replaced,
    Deleted,
}

/// Whether the change requires destroying and recreating the resource, and
/// if so in which order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReplacementPolicy {
    NotRequired,
    CreateBeforeDestroy,
    DestroyBeforeCreate,
}

/// The expected availability effect of the change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Disruption {
    None,
    Rolling,
    BriefInterruption,
    UnavailableDuringChange,
}

/// What happens to data the resource holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DataEffect {
    NoDataHeld,
    Preserved,
    Migrated,
    Destroyed,
}

/// Whether applying the inverse change restores the prior state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Reversibility {
    Reversible,
    ReversibleWithDataLoss,
    Irreversible,
}

/// A documentation reference establishing a declared behaviour.
///
/// Constructed `const` at declaration sites (declarations are `const` items
/// by convention precisely so the non-empty assertion evaluates at compile
/// time). Stored as a `Cow` rather than `&'static str` so the artifact can
/// round-trip through JSON — an implementation deviation from the design's
/// literal sketch, recorded here: deserialized citations are owned, `const`
/// ones are borrowed, and equality is by content either way.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Citation(Cow<'static, str>);

impl Citation {
    /// A compile-time-checked citation. Empty citations refuse to build.
    pub const fn new(reference: &'static str) -> Self {
        assert!(!reference.is_empty(), "a citation must name its source");
        Self(Cow::Borrowed(reference))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A value and how firmly Tokeira holds it.
///
/// `Unknown` is the `Default` — the lazy path is the honest path (umbrella
/// D4). `Inference` is Tokeira-derived and renders as such; `EngineFact`
/// cites Tokeira's own code; `ProviderGuarantee` cites the provider's
/// documentation. Not `Copy`: citations own their text after deserialization.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Confidence<T> {
    #[default]
    Unknown,
    Inference(T),
    EngineFact {
        value: T,
        citation: Citation,
    },
    ProviderGuarantee {
        value: T,
        citation: Citation,
    },
}

impl<T> Confidence<T> {
    /// Whether anything at all is held — `Unknown` is the only absence.
    pub fn is_known(&self) -> bool {
        !matches!(self, Confidence::Unknown)
    }

    /// The held value, at any confidence.
    pub fn value(&self) -> Option<&T> {
        match self {
            Confidence::Unknown => None,
            Confidence::Inference(value)
            | Confidence::EngineFact { value, .. }
            | Confidence::ProviderGuarantee { value, .. } => Some(value),
        }
    }
}

/// What a change does to the running resource, in the declaring kind's words.
/// Every field defaults to `Unknown`; the explanation layer turns unknowns
/// into uncertainty, never into silence or optimism.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSemantics {
    pub operation: Confidence<LifecycleOperation>,
    pub replacement: Confidence<ReplacementPolicy>,
    pub disruption: Confidence<Disruption>,
    pub data_effect: Confidence<DataEffect>,
    pub reversibility: Confidence<Reversibility>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Umbrella D4: the default declares nothing, field for field.
    #[test]
    fn the_default_declares_nothing() {
        let semantics = ChangeSemantics::default();
        assert!(!semantics.operation.is_known());
        assert!(!semantics.replacement.is_known());
        assert!(!semantics.disruption.is_known());
        assert!(!semantics.data_effect.is_known());
        assert!(!semantics.reversibility.is_known());
    }

    // A cited claim survives the JSON round trip with content equality —
    // the Cow deviation exists exactly for this.
    #[test]
    fn cited_claims_round_trip() {
        let declared = Confidence::EngineFact {
            value: Disruption::UnavailableDuringChange,
            citation: Citation::new("crates/tokeira-compose/src/lib.rs reconcile_service"),
        };
        let json = serde_json::to_string(&declared).unwrap();
        let back: Confidence<Disruption> = serde_json::from_str(&json).unwrap();
        assert_eq!(declared, back);
    }
}
