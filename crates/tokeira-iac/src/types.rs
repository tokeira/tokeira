//! Public types for plan output and module selection.

use std::collections::{BTreeSet, HashMap, VecDeque};

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

/// Direction in which an explicit module selection expands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionDirection {
    /// Plan/apply include transitive prerequisites.
    Prerequisites,
    /// Destroy includes transitive dependants.
    Dependants,
}

/// Expand an explicit selection over real infrastructure modules.
pub fn expand_module_selection(
    modules: &[Box<dyn Module>],
    selection: &ModuleSelection,
    direction: SelectionDirection,
) -> Result<ModuleSelection, crate::IacError> {
    let ModuleSelection::Only(requested) = selection else {
        return Ok(selection.clone());
    };
    if requested.is_empty() {
        return Err(crate::IacError::CompositionInvalid(
            "module selection cannot be empty".to_string(),
        ));
    }
    let supported = modules
        .iter()
        .map(|module| module.name().to_string())
        .collect::<Vec<_>>();
    let known = supported
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let unknown = requested
        .iter()
        .filter(|name| !known.contains(name.as_str()))
        .cloned()
        .collect::<BTreeSet<_>>();
    if !unknown.is_empty() {
        return Err(crate::IacError::CompositionInvalid(format!(
            "unknown modules: {}; supported modules: {}",
            unknown.into_iter().collect::<Vec<_>>().join(", "),
            supported.join(", ")
        )));
    }

    let prerequisites = modules
        .iter()
        .map(|module| {
            (
                module.name().to_string(),
                module
                    .dependencies()
                    .into_iter()
                    .map(str::to_string)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut dependants = HashMap::<String, Vec<String>>::new();
    for (module, dependencies) in &prerequisites {
        for dependency in dependencies {
            dependants
                .entry(dependency.clone())
                .or_default()
                .push(module.clone());
        }
    }
    let mut selected = requested.iter().cloned().collect::<BTreeSet<_>>();
    let mut queue = requested.iter().cloned().collect::<VecDeque<_>>();
    while let Some(module) = queue.pop_front() {
        let adjacent = match direction {
            SelectionDirection::Prerequisites => prerequisites.get(&module),
            SelectionDirection::Dependants => dependants.get(&module),
        };
        for adjacent in adjacent.into_iter().flatten() {
            if selected.insert(adjacent.clone()) {
                queue.push_back(adjacent.clone());
            }
        }
    }
    Ok(ModuleSelection::Only(
        supported
            .into_iter()
            .filter(|module| selected.contains(module))
            .collect(),
    ))
}

#[cfg(test)]
mod selection_tests {
    use super::*;

    #[derive(Debug)]
    struct TestModule {
        name: &'static str,
        dependencies: Vec<&'static str>,
    }

    impl Module for TestModule {
        fn name(&self) -> &str {
            self.name
        }

        fn dependencies(&self) -> Vec<&str> {
            self.dependencies.clone()
        }

        fn resources(
            &self,
            _context: &crate::ModuleContext<'_>,
        ) -> Result<Vec<Box<dyn crate::Resource>>, crate::IacError> {
            Ok(Vec::new())
        }
    }

    fn modules() -> Vec<Box<dyn Module>> {
        vec![
            Box::new(TestModule {
                name: "state",
                dependencies: Vec::new(),
            }),
            Box::new(TestModule {
                name: "database",
                dependencies: vec!["state"],
            }),
            Box::new(TestModule {
                name: "service",
                dependencies: vec!["database"],
            }),
            Box::new(TestModule {
                name: "metrics",
                dependencies: vec!["state"],
            }),
        ]
    }

    fn selected(selection: ModuleSelection) -> Vec<String> {
        let ModuleSelection::Only(selected) = selection else {
            panic!("explicit selection remains explicit");
        };
        selected
    }

    #[test]
    // Feature: platform-builder-abstraction, Property 9: module selection is the required closure.
    fn module_selection_expands_the_directional_transitive_closure() {
        assert_eq!(
            selected(
                expand_module_selection(
                    &modules(),
                    &ModuleSelection::Only(vec!["service".to_string()]),
                    SelectionDirection::Prerequisites,
                )
                .expect("prerequisite closure"),
            ),
            ["state", "database", "service"]
        );
        assert_eq!(
            selected(
                expand_module_selection(
                    &modules(),
                    &ModuleSelection::Only(vec!["state".to_string()]),
                    SelectionDirection::Dependants,
                )
                .expect("dependant closure"),
            ),
            ["state", "database", "service", "metrics"]
        );
    }

    #[test]
    fn module_selection_rejects_empty_and_unknown_requests() {
        assert!(
            expand_module_selection(
                &modules(),
                &ModuleSelection::Only(Vec::new()),
                SelectionDirection::Prerequisites,
            )
            .is_err()
        );
        assert!(
            expand_module_selection(
                &modules(),
                &ModuleSelection::Only(vec!["unknown".to_string()]),
                SelectionDirection::Prerequisites,
            )
            .is_err()
        );
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
#[derive(Debug)]
pub struct InfraComposition {
    pub desired_modules: Vec<Box<dyn Module>>,
    pub known_modules: Vec<Box<dyn Module>>,
    pub active_modules: Vec<String>,
}

/// The kind of change detected by the diff engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChangeKind {
    Create,
    Update,
    /// Delete-then-recreate: an immutable-field change that `update` cannot apply
    /// in place. Destructive — it replaces the live resource (and its data).
    Replace,
    Delete,
    NoChange,
}

impl ChangeKind {
    /// A change that destroys live infrastructure (and any data it holds).
    /// `plan` surfaces these and `apply` requires explicit confirmation.
    pub fn is_destructive(&self) -> bool {
        matches!(self, ChangeKind::Delete | ChangeKind::Replace)
    }
}

/// A flat change record suitable for display and reporting.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Change {
    pub kind: ChangeKind,
    pub resource_type: String,
    pub module: String,
    pub resource: String,
    pub details: Vec<FieldDiff>,
}

impl Change {
    /// Whether this change destroys live infrastructure (Delete or Replace).
    pub(crate) fn is_destructive(&self) -> bool {
        self.kind.is_destructive()
    }
}

/// The destructive changes in a plan (Delete and Replace) — the changes `apply`
/// must surface and confirm before enacting. The engine classifies; the
/// calling shell enforces the confirmation, since the engine cannot prompt.
pub fn destructive_changes(changes: &[Change]) -> Vec<&Change> {
    changes.iter().filter(|c| c.is_destructive()).collect()
}

/// Whether a plan contains any destructive change (Delete or Replace).
pub fn plan_is_destructive(changes: &[Change]) -> bool {
    changes.iter().any(Change::is_destructive)
}

/// A single field-level difference within a resource change.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FieldDiff {
    pub field: String,
    pub before: Option<String>,
    pub after: Option<String>,
}

impl FieldDiff {
    /// A named observation without captured values — the evidence shape for a
    /// resource that detects a change but does not hold the before/after pair
    /// (e.g. "tags changed"). Renders as a bare evidence line, never as
    /// `(none) → (none)`.
    pub fn observation(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            before: None,
            after: None,
        }
    }
}

/// Result of diffing a single resource against its current state.
#[derive(Debug, Clone)]
pub struct ResourceDiff {
    pub kind: ChangeKind,
    pub details: Vec<FieldDiff>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(kind: ChangeKind) -> Change {
        Change {
            kind,
            resource_type: "T".to_string(),
            module: "m".to_string(),
            resource: "r".to_string(),
            details: Vec::new(),
        }
    }

    #[test]
    fn destructive_changes_selects_delete_and_replace() {
        // 11.3b: Delete and Replace are destructive; Create/Update/NoChange are not.
        let changes = vec![
            change(ChangeKind::Create),
            change(ChangeKind::Update),
            change(ChangeKind::Replace),
            change(ChangeKind::Delete),
            change(ChangeKind::NoChange),
        ];
        assert!(plan_is_destructive(&changes));
        let destructive = destructive_changes(&changes);
        assert_eq!(destructive.len(), 2);
        assert!(
            destructive
                .iter()
                .all(|c| matches!(c.kind, ChangeKind::Replace | ChangeKind::Delete))
        );
    }

    #[test]
    fn non_destructive_plan_is_not_flagged() {
        let changes = vec![
            change(ChangeKind::Create),
            change(ChangeKind::Update),
            change(ChangeKind::NoChange),
        ];
        assert!(!plan_is_destructive(&changes));
        assert!(destructive_changes(&changes).is_empty());
    }
}
