//! Per-test ledger data model for Tier-2 functional conformance.
//!
//! Tier 2 runs Temporal's own functional Go suite, unmodified, over the real gRPC
//! wire against a running `tokeirad` (see `.kiro/specs/temporal-functional-conformance`).
//! Its core posture is **run-all, classify-in-report**: every test in the pinned
//! corpus executes, and each outcome is *interpreted* in a report rather than gated
//! at run time. This module owns the persistent shape of that interpretation — the
//! per-test ledger — but nothing else.
//!
//! ## Boundary
//!
//! This is the *data model only*. It deliberately contains no ledger parsing, no
//! report join against the compatibility matrix, and no gate logic — those live in
//! later report-side tasks (9.x/10.x) and would couple this shape to consumers it
//! must stay independent of. Keeping the model free of behaviour lets both the Go
//! fork (which may serialize ledger entries) and the Rust report (which consumes
//! them) agree on one schema without sharing code.
//!
//! ## Why per-test granularity is load-bearing
//!
//! The ledger is keyed at **per-test** granularity — Go package + test name
//! *including `t.Run` sub-test names* (e.g. `pkg/Test/sub`). This is not a stylistic
//! choice: a single failing sub-test in an otherwise-passing file must be classified
//! on its own merits, so that a real-gap fix flips exactly the tests it resolves and
//! a passing sibling sub-test is never tarred by a failing one. Coarser (per-file or
//! per-`Suite`) keying would let scope inflation hide — a whole file classified
//! `out-of-public-scope` because one of its sub-tests touched an internal surface
//! (Requirement 3.2, 3.4; design "Ledger granularity — per-test").
//!
//! ## Why evidence is keyed to the category
//!
//! Each non-passing category carries a *different* kind of justification, and the
//! report gates (task 10.3) enforce that the right kind is present. Modelling the
//! evidence as an enum whose variants line up with the categories makes the
//! category↔evidence relationship explicit in the type system rather than relying on
//! a free-form string plus convention: a `real-gap` entry that forgot its tracking
//! issue, or an `out-of-public-scope` entry that cited a PR instead of the internal
//! surface it touched, is then a visibly wrong variant rather than a plausible-looking
//! string. The mapping is documented on [`EvidenceRef`].

use serde::{Deserialize, Serialize};

/// Classification of a single Tier-2 test outcome.
///
/// Every test that runs is classified into exactly one of these categories in the
/// report. The classification is an *interpretation of a result*, never a gate on
/// whether the test runs (Requirement 3.2, 3.3). The four categories are mutually
/// exclusive and exhaustive over the outcomes Tier 2 admits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LedgerCategory {
    /// Ran and passed. Counts toward the conformance claim.
    Pass,

    /// Ran and failed, but Tokeira *should* pass it and does not yet. Tracked as
    /// expect-fail with a linked tracking issue until fixed, at which point real-gap
    /// monotonicity (Requirement 4) requires it to flip to a required pass.
    RealGap,

    /// Ran and failed because Tokeira *intentionally* differs from Temporal here.
    /// Carries a cited spec/PR rationale and is reviewed on every compat bump so a
    /// deviation cannot quietly become an unexamined gap.
    DeliberateDeviation,

    /// Ran and failed because it depends on a surface outside Tokeira's public claim
    /// (an internal client the Shape-2 onebox does not front). Carries the internal
    /// client surface it touched, derived mechanically from the wire-coverage
    /// observation rather than hand-judged (Requirement 3.6).
    OutOfPublicScope,
}

/// Category-keyed justification for a ledger entry.
///
/// The variant a [`LedgerEntry`] carries must match its [`LedgerCategory`]; the
/// report gates (task 10.3) enforce this. Modelling evidence as a category-aligned
/// enum (rather than a bare `String`) makes the required relationship explicit and
/// machine-checkable:
///
/// | [`LedgerCategory`]      | Expected [`EvidenceRef`]    | Cites                                   |
/// |-------------------------|-----------------------------|-----------------------------------------|
/// | `Pass`                  | [`EvidenceRef::NotApplicable`] | nothing — a pass needs no justification |
/// | `RealGap`               | [`EvidenceRef::TrackingIssue`] | a tracking-issue link (Requirement 3.8) |
/// | `DeliberateDeviation`   | [`EvidenceRef::SpecOrPr`]      | a spec/PR rationale (Requirement 3.7)   |
/// | `OutOfPublicScope`      | [`EvidenceRef::InternalSurface`] | the internal client surface touched (Requirement 3.6) |
///
/// This module does not *enforce* the pairing — that is a report-side gate concern
/// (task 10.3) deliberately kept out of the data model. The type only makes the
/// intended pairing expressible and self-documenting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EvidenceRef {
    /// No evidence required. The expected companion of [`LedgerCategory::Pass`].
    NotApplicable,

    /// A link to a tracking issue for a known gap. The expected companion of
    /// [`LedgerCategory::RealGap`] (Requirement 3.8). The string is the issue
    /// reference (URL or issue key); this model does not constrain its format.
    TrackingIssue(String),

    /// A link to the spec or PR documenting an intentional divergence. The expected
    /// companion of [`LedgerCategory::DeliberateDeviation`] (Requirement 3.7).
    SpecOrPr(String),

    /// A tag naming the internal client surface the test touched (e.g. `AdminClient`,
    /// `HistoryClient`, `MatchingClient`, `OperatorClient` beyond the claimed subset,
    /// the dynamic-config client, or an internal task-poller/cluster hook). The
    /// expected companion of [`LedgerCategory::OutOfPublicScope`] (Requirement 3.6).
    InternalSurface(String),
}

/// One classified Tier-2 test outcome.
///
/// Every test that runs produces exactly one `LedgerEntry`. The entry pairs a
/// per-test identity with its category, a human-readable rationale, and the
/// category-keyed [`EvidenceRef`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerEntry {
    /// The test's identity at **per-test granularity**: Go package + test name,
    /// *including `t.Run` sub-test names* (e.g. `pkg/Test/sub`). A single failing
    /// sub-test in an otherwise-passing file is keyed — and therefore classified —
    /// independently of its siblings (Requirement 3.2, 3.4). See the module docs for
    /// why coarser keying would let scope inflation hide.
    pub test_id: String,

    /// The single category this outcome is classified into.
    pub category: LedgerCategory,

    /// Human-readable explanation of *why* this entry carries its category. This is
    /// prose for a reviewer; the machine-checkable justification lives in `evidence`.
    pub rationale: String,

    /// The category-keyed justification. See [`EvidenceRef`] for the expected
    /// category↔variant pairing.
    pub evidence: EvidenceRef,
}
