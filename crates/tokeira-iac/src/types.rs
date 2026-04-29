//! Orchestrator-facing types for plan output and module selection.

use crate::Module;

/// Controls which modules are included in a composition.
#[derive(Debug, Clone)]
pub enum ModuleSelection {
    All,
    Only(Vec<String>),
    Except(Vec<String>),
}

impl ModuleSelection {
    pub fn includes(&self, name: &str) -> bool {
        match self {
            ModuleSelection::All => true,
            ModuleSelection::Only(names) => names.iter().any(|n| n == name),
            ModuleSelection::Except(names) => !names.iter().any(|n| n == name),
        }
    }
}

/// A composed set of modules ready for plan/apply/destroy.
///
/// Carries three module lists following the deploy-eks pattern:
///
/// - `desired_modules`: what should exist after apply.
/// - `known_modules`: everything the deployment knows how to manage,
///   including modules that are no longer desired but may still have
///   resources in state that need deletion. Must be a superset of desired.
/// - `active_modules`: which module names are in scope for this operation.
///   Used for module-scoped `--module` filtering.
pub struct InfraComposition {
    pub desired_modules: Vec<Box<dyn Module>>,
    pub known_modules: Vec<Box<dyn Module>>,
    pub active_modules: Vec<String>,
}

/// The kind of change detected by the diff engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeKind {
    Create,
    Update,
    Delete,
    NoChange,
}

/// A flat change record suitable for display and reporting.
#[derive(Debug, Clone)]
pub struct Change {
    pub kind: ChangeKind,
    pub resource_type: String,
    pub module: String,
    pub resource: String,
    pub details: Vec<FieldDiff>,
}

/// A single field-level difference within a resource change.
#[derive(Debug, Clone)]
pub struct FieldDiff {
    pub field: String,
    pub before: Option<String>,
    pub after: Option<String>,
}

/// Result of diffing a single resource against its current state.
#[derive(Debug, Clone)]
pub struct ResourceDiff {
    pub kind: ChangeKind,
    pub details: Vec<FieldDiff>,
}
