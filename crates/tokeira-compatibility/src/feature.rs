use serde::{Deserialize, Serialize};

use crate::matrix::FEATURE_MATRIX;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeatureState {
    Implemented,
    Partial,
    Experimental,
    Stubbed,
    Unsupported,
}

impl FeatureState {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Implemented => "implemented",
            Self::Partial => "partial",
            Self::Experimental => "experimental",
            Self::Stubbed => "stubbed",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatibilitySurfaceKind {
    Rpc,
    RequestField,
    ResponseField,
    HistoryEvent,
    CommandAttribute,
    EnumVariant,
    CapabilityFlag,
    ErrorDetail,
    BehaviouralInvariant,
}

impl CompatibilitySurfaceKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Rpc => "rpc",
            Self::RequestField => "request-field",
            Self::ResponseField => "response-field",
            Self::HistoryEvent => "history-event",
            Self::CommandAttribute => "command-attribute",
            Self::EnumVariant => "enum-variant",
            Self::CapabilityFlag => "capability-flag",
            Self::ErrorDetail => "error-detail",
            Self::BehaviouralInvariant => "behavioural-invariant",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilitySurface {
    pub kind: CompatibilitySurfaceKind,
    pub identifier: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatibilityEvidenceKind {
    Test,
    ManualReview,
    SdkConformance,
}

impl CompatibilityEvidenceKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Test => "test",
            Self::ManualReview => "manual-review",
            Self::SdkConformance => "sdk-conformance",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilityEvidence {
    pub kind: CompatibilityEvidenceKind,
    pub reference: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct FeatureEntry {
    pub id: &'static str,
    pub name: &'static str,
    pub state: FeatureState,
    pub surfaces: &'static [CompatibilitySurface],
    pub capability_field: Option<&'static str>,
    pub dynamic_config_key: Option<&'static str>,
    pub rpcs: &'static [&'static str],
    pub notes: &'static str,
    pub evidence: &'static [CompatibilityEvidence],
}

impl FeatureEntry {
    pub fn capability_fields(&self) -> impl Iterator<Item = &'static str> {
        self.capability_field
            .into_iter()
            .chain(secondary_capability_fields(self.id).iter().copied())
    }
}

fn secondary_capability_fields(feature_id: &str) -> &'static [&'static str] {
    match feature_id {
        "workflow-task-lifecycle" => &["upsert_memo"],
        _ => &[],
    }
}

pub trait Feature {
    const ID: &'static str;
    const ENTRY: &'static FeatureEntry;
}

pub const fn lookup_feature_const(id: &'static str) -> &'static FeatureEntry {
    let mut index = 0;
    while index < FEATURE_MATRIX.len() {
        if const_str_eq(FEATURE_MATRIX[index].id, id) {
            return &FEATURE_MATRIX[index];
        }
        index += 1;
    }
    panic!("declare_feature!: id not found in FEATURE_MATRIX")
}

pub const fn const_str_eq(left: &str, right: &str) -> bool {
    let left_bytes = left.as_bytes();
    let right_bytes = right.as_bytes();
    if left_bytes.len() != right_bytes.len() {
        return false;
    }

    let mut index = 0;
    while index < left_bytes.len() {
        if left_bytes[index] != right_bytes[index] {
            return false;
        }
        index += 1;
    }

    true
}

#[macro_export]
macro_rules! declare_feature {
    ($name:ident, $id:literal) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct $name;

        impl $crate::Feature for $name {
            const ID: &'static str = $id;
            const ENTRY: &'static $crate::FeatureEntry = $crate::lookup_feature_const($id);
        }
    };
}

#[macro_export]
macro_rules! cfg_feature {
    ($feature_id:literal => $($tt:tt)*) => {
        const _: () = {
            let entry = $crate::lookup_feature_const($feature_id);
            match entry.state {
                $crate::FeatureState::Implemented
                | $crate::FeatureState::Partial
                | $crate::FeatureState::Experimental => (),
                $crate::FeatureState::Stubbed => {
                    panic!("cfg_feature!: refusing to compile code gated on a stubbed feature")
                }
                $crate::FeatureState::Unsupported => {
                    panic!("cfg_feature!: refusing to compile code gated on an unsupported feature")
                }
            }
        };
        $($tt)*
    };
}
