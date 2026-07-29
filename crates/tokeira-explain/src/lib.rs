//! The deterministic explanation model — Tokeira's account of what an
//! operation will do or did, and (as later features land) why.
//!
//! This crate **models; it never decides**. Every fact in a
//! [`DeploymentExplanation`] is computed by the engine and carried here
//! verbatim; construction adds structure (evidence identity, uncertainty)
//! and nothing else. The crate's dependency list is its contract: engine
//! types and serialization — no network, no provider, no model client, no
//! provisioner shell (evidence-model Requirement 9).
//!
//! The governing principle (umbrella `operator-explanation`): *the engine
//! establishes truth; the explanation layer establishes meaning; an agent
//! only helps the operator navigate it.*
//!
//! Slots for later features (`semantics`, `cause`, `dependants`, `source`)
//! exist from this crate's first version, carrying explicit not-determined
//! defaults, so Features 2–4 populate data without reshaping the schema
//! (Requirement 8).

pub mod artifact;
pub mod build;
pub mod evidence;
pub mod impacts;
pub mod model;

pub use artifact::ExplainError;
pub use build::{DeploymentContext, explain_applied, explain_plan};
pub use evidence::{EvidenceId, EvidenceIndex, EvidenceKind};
pub use impacts::derive_impacts;
pub use model::{
    Cause, CommittedChange, CommittedOp, DeploymentExplanation, EXPLANATION_SCHEMA_VERSION,
    ExplainedChange, ImpactClass, OperationalImpact, SourceLocation, Uncertainty,
    UncertaintyReason,
};
// The semantics vocabulary is declared beside the `Resource` trait it
// annotates (the Feature 1 amendment recorded in
// `.kiro/specs/explanation-change-semantics`); re-exported here so
// explanation consumers see one surface.
pub use tokeira_iac::{
    ChangeSemantics, Citation, Confidence, DataEffect, Disruption, LifecycleOperation,
    ReplacementPolicy, Reversibility,
};
