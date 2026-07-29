//! The change-semantics vocabulary: what a change *does* to a running
//! resource, declared by the kind that owns the provider.
//!
//! The vocabulary lives beside [`Resource`](crate::Resource) because the
//! declaration point is a method on that trait — the types must live where
//! the trait does. Layers built on top of this crate re-export them rather
//! than defining a vocabulary of their own.
//!
//! Invariants owned here:
//! - **`Unknown` is the default of every field**: a kind that declares
//!   nothing yields uncertainty, never a confident-sounding default. The
//!   lazy path is the honest path.
//! - **A claim carries its citation structurally**: `EngineFact` and
//!   `ProviderGuarantee` cannot be constructed without one, so an uncited
//!   claim is unrepresentable rather than merely discouraged.

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

/// How the provider effects the change. Distinct from
/// [`ChangeKind`](crate::ChangeKind), which describes *state reconciliation*:
/// a provider whose update path stops and recreates the resource effects an
/// `Update` as `Replaced`.
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
    /// Data is affected under the resource's own declared policy — expiry,
    /// retention, lifecycle — rather than by the change itself. The
    /// declaration's statement and citation carry the specific policy.
    Policy,
}

/// Whether applying the inverse change restores the prior state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Reversibility {
    Reversible,
    ReversibleWithDataLoss,
    Irreversible,
}

/// A reference establishing a declared behaviour — the engine's own code,
/// or product documentation.
///
/// The two forms exist because the two claims are different in kind: a code
/// citation names the module identity a reader greps for; a documentation
/// citation carries a title and URL that render as a link, plus optionally
/// the establishing sentence, quoted so the ground survives a moved page.
/// Constructed `const` at declaration sites so the non-empty assertions
/// evaluate at compile time; stored as `Cow` so the serialized form
/// round-trips (deserialized citations are owned, `const` ones borrowed,
/// equality by content either way).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Citation {
    /// The engine's own code, by module identity.
    Code(Cow<'static, str>),
    /// Product documentation: title + URL, with the establishing quote
    /// where one sentence carries the claim.
    Doc {
        title: Cow<'static, str>,
        url: Cow<'static, str>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        quote: Option<Cow<'static, str>>,
    },
}

impl Citation {
    /// A compile-time-checked code citation. Empty citations refuse to build.
    pub const fn code(reference: &'static str) -> Self {
        assert!(!reference.is_empty(), "a citation must name its source");
        Citation::Code(Cow::Borrowed(reference))
    }

    /// A compile-time-checked documentation citation.
    pub const fn doc(title: &'static str, url: &'static str) -> Self {
        assert!(!title.is_empty(), "a documentation citation must be titled");
        assert!(
            !url.is_empty(),
            "a documentation citation must carry its URL"
        );
        Citation::Doc {
            title: Cow::Borrowed(title),
            url: Cow::Borrowed(url),
            quote: None,
        }
    }

    /// A documentation citation carrying its establishing sentence.
    pub const fn doc_quoted(title: &'static str, url: &'static str, quote: &'static str) -> Self {
        assert!(!title.is_empty(), "a documentation citation must be titled");
        assert!(
            !url.is_empty(),
            "a documentation citation must carry its URL"
        );
        assert!(!quote.is_empty(), "an establishing quote must not be empty");
        Citation::Doc {
            title: Cow::Borrowed(title),
            url: Cow::Borrowed(url),
            quote: Some(Cow::Borrowed(quote)),
        }
    }
}

/// A value and how firmly the engine holds it.
///
/// `Unknown` is the `Default` — the lazy path is the honest path.
/// `Inference` is engine-derived and renders as such; `EngineFact`
/// cites the engine's own code; `ProviderGuarantee` cites the provider's
/// documentation. Every tier above `Unknown` carries a citation — an
/// uncited conclusion is as unrepresentable as an uncited fact. Not
/// `Copy`: citations own their text after deserialization.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Confidence<T> {
    #[default]
    Unknown,
    /// A conclusion the engine derives from cited facts — owned as the
    /// engine's own, never presented as a guarantee.
    Inference {
        value: T,
        citation: Citation,
    },
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
            Confidence::Inference { value, .. }
            | Confidence::EngineFact { value, .. }
            | Confidence::ProviderGuarantee { value, .. } => Some(value),
        }
    }

    /// The citation behind the held value, if anything is held.
    pub fn citation(&self) -> Option<&Citation> {
        match self {
            Confidence::Unknown => None,
            Confidence::Inference { citation, .. }
            | Confidence::EngineFact { citation, .. }
            | Confidence::ProviderGuarantee { citation, .. } => Some(citation),
        }
    }
}

/// What a change does to the running resource, in the declaring kind's words.
/// Every field defaults to `Unknown`; consumers surface unknowns as
/// uncertainty, never as silence or optimism.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSemantics {
    pub operation: Confidence<LifecycleOperation>,
    pub replacement: Confidence<ReplacementPolicy>,
    pub disruption: Confidence<Disruption>,
    pub data_effect: Confidence<DataEffect>,
    pub reversibility: Confidence<Reversibility>,
    /// One kind-authored sentence of operator prose stating the change's
    /// mechanism more precisely than the generic template ("it would be
    /// stopped, removed, and recreated from the definition"). Optional: a
    /// renderer falls back to the generic template. Carries the same
    /// research obligation as the fields above.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement: Option<Cow<'static, str>>,
}

/// What a kind needs in order to declare — the declaration point's input.
///
/// A context struct rather than loose parameters so new inputs can be added
/// without breaking every declaration site.
#[derive(Debug)]
pub struct SemanticsContext<'a> {
    /// The engine's reconciliation classification for this change. Distinct
    /// from the declared [`LifecycleOperation`]: a kind may effect an
    /// `Update` as a replacement, and saying so is the point.
    pub kind: crate::ChangeKind,
    /// The recorded state the change acts on; `None` for a creation.
    pub current: Option<&'a crate::ResourceState>,
    /// The field-level differences driving the change; empty for creations
    /// and deletions.
    pub field_diffs: &'a [crate::FieldDiff],
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
            citation: Citation::code("provider::module::reconcile — stop, remove, recreate"),
        };
        let json = serde_json::to_string(&declared).unwrap();
        let back: Confidence<Disruption> = serde_json::from_str(&json).unwrap();
        assert_eq!(declared, back);
    }
}
