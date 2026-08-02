//! Deterministic prerequisite and dependent closure over verified modules.

use std::collections::{BTreeSet, HashMap, VecDeque};

use crate::{error::SelectionError, graph::VerifiedGraph};

/// Direction in which a named module selection expands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionDirection {
    /// Plan/apply include transitive prerequisites.
    Prerequisites,
    /// Destroy includes transitive dependents.
    Dependents,
}

/// Stable effective module selection shared by every engine and report path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveSelection {
    modules: Vec<String>,
}

impl EffectiveSelection {
    /// Borrow selected module names in definition declaration order.
    pub fn modules(&self) -> &[String] {
        &self.modules
    }
}

/// Compute all modules or the requested transitive closure in definition order.
pub fn select_modules(
    graph: &VerifiedGraph,
    requested: Option<&[String]>,
    direction: SelectionDirection,
) -> Result<EffectiveSelection, SelectionError> {
    let supported = graph
        .modules()
        .iter()
        .map(|module| module.name().to_string())
        .collect::<Vec<_>>();
    let Some(requested) = requested else {
        return Ok(EffectiveSelection { modules: supported });
    };
    if requested.is_empty() {
        return Err(SelectionError::Empty { supported });
    }

    let known = supported
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut unknown = Vec::new();
    let mut unknown_seen = BTreeSet::new();
    for name in requested {
        if !known.contains(name.as_str()) && unknown_seen.insert(name.as_str()) {
            unknown.push(name.clone());
        }
    }
    if !unknown.is_empty() {
        return Err(SelectionError::Unknown { unknown, supported });
    }

    let prerequisites = graph
        .modules()
        .iter()
        .map(|module| (module.name(), module.dependencies()))
        .collect::<HashMap<_, _>>();
    let mut dependents = HashMap::<&str, Vec<&str>>::new();
    for module in graph.modules() {
        for dependency in module.dependencies() {
            dependents
                .entry(dependency)
                .or_default()
                .push(module.name());
        }
    }

    let mut selected = requested.iter().cloned().collect::<BTreeSet<_>>();
    let mut queue = requested.iter().cloned().collect::<VecDeque<_>>();
    while let Some(name) = queue.pop_front() {
        let adjacent = match direction {
            SelectionDirection::Prerequisites => prerequisites
                .get(name.as_str())
                .map(|values| values.iter().map(String::as_str).collect::<Vec<_>>())
                .unwrap_or_default(),
            SelectionDirection::Dependents => {
                dependents.get(name.as_str()).cloned().unwrap_or_default()
            }
        };
        for adjacent_name in adjacent {
            if selected.insert(adjacent_name.to_string()) {
                queue.push_back(adjacent_name.to_string());
            }
        }
    }

    Ok(EffectiveSelection {
        modules: supported
            .into_iter()
            .filter(|name| selected.contains(name))
            .collect(),
    })
}
