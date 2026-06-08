//! Canonical Temporal compatibility metadata.
//!
//! This crate is the shared source of truth for feature state, SDK support
//! claims, compile-time feature gates, and edge dispatch decisions. It contains
//! no I/O and no runtime dependency on the edge or kernel crates, which keeps
//! compatibility policy reusable across binaries and low-level crates.

pub mod coverage;
pub mod digest;
pub mod dispatch;
pub mod feature;
pub mod matrix;
pub mod sdk;

pub use coverage::{
    ExpectedOutcome, RpcClassification,
    expected::expected_outcome,
    index::feature_for_rpc,
    normalize::{rpc_id_to_wire_path, wire_path_to_rpc_id},
    resolve,
};
pub use digest::{feature_matrix_digest, sdk_matrix_digest};
pub use dispatch::{
    DisabledReason, DispatchMetrics, DispatchOutcome, DynamicConfigReader, NoopDispatchMetrics,
    StaticDynamicConfig, dispatch_rpc,
};
pub use feature::{
    CompatibilityEvidence, CompatibilityEvidenceKind, CompatibilitySurface,
    CompatibilitySurfaceKind, Feature, FeatureEntry, FeatureState, const_str_eq,
    lookup_feature_const,
};
pub use matrix::FEATURE_MATRIX;
pub use sdk::{
    IncompatibleVersion, OwnedSdkCompatEntry, SDK_MATRIX, SdkCompatEntry, SdkVerificationState,
};
