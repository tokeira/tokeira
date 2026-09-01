//! Cargo-metadata package selection and deterministic publish ordering.

use std::collections::{BTreeMap, BTreeSet};

use cargo_metadata::{DependencyKind, Metadata};

use super::ReleaseError;

/// One publishable package and its internal publish prerequisites.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishableNode {
    /// Cargo package name.
    pub name: String,
    /// Opaque Cargo package identifier used only while interpreting metadata.
    pub package_id: String,
    /// Internal normal/build dependency names.
    pub dependencies: Vec<String>,
}

/// Select the crates.io-publishable workspace closure and order it stably.
pub fn publishable_packages(metadata: &Metadata) -> Result<Vec<PublishableNode>, ReleaseError> {
    let publishable = metadata
        .workspace_packages()
        .into_iter()
        .filter(|package| match &package.publish {
            None => true,
            Some(registries) => registries.iter().any(|registry| registry == "crates-io"),
        })
        .map(|package| (package.id.clone(), package.name.to_string()))
        .collect::<BTreeMap<_, _>>();
    if publishable.is_empty() {
        return Err(ReleaseError::Workspace {
            reason: "the workspace has no crates.io-publishable packages".to_owned(),
        });
    }

    let resolve = metadata
        .resolve
        .as_ref()
        .ok_or_else(|| ReleaseError::Workspace {
            reason: "Cargo metadata did not include a resolved dependency graph".to_owned(),
        })?;
    let mut nodes = Vec::with_capacity(publishable.len());
    for (package_id, name) in &publishable {
        let resolved = resolve
            .nodes
            .iter()
            .find(|node| &node.id == package_id)
            .ok_or_else(|| ReleaseError::Workspace {
                reason: format!("Cargo metadata omitted the resolve node for {name}"),
            })?;
        // Only normal and build edges order publication: they are what the registry
        // index records and what packaging must resolve. Dev-dependencies never reach
        // the index, and counting them would turn `tokeira-chasm-derive`'s dev-dependency
        // on `tokeira-chasm` into a cycle that does not exist for consumers. Cargo's
        // resolve already carries target-specific and enabled-optional edges here.
        let mut dependencies = resolved
            .deps
            .iter()
            .filter(|dependency| publishable.contains_key(&dependency.pkg))
            .filter(|dependency| {
                dependency
                    .dep_kinds
                    .iter()
                    .any(|kind| matches!(kind.kind, DependencyKind::Normal | DependencyKind::Build))
            })
            .filter_map(|dependency| publishable.get(&dependency.pkg).cloned())
            .collect::<Vec<_>>();
        dependencies.sort();
        dependencies.dedup();
        nodes.push(PublishableNode {
            name: name.clone(),
            package_id: package_id.repr.clone(),
            dependencies,
        });
    }
    stable_topological_order(&nodes)
}

/// Resolve direct non-workspace normal/build dependencies that packaging needs from a registry.
pub fn external_publish_dependencies(
    metadata: &Metadata,
) -> Result<Vec<(String, String)>, ReleaseError> {
    let workspace = metadata
        .workspace_members
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let publishable = publishable_packages(metadata)?
        .into_iter()
        .map(|node| node.package_id)
        .collect::<BTreeSet<_>>();
    let resolve = metadata
        .resolve
        .as_ref()
        .ok_or_else(|| ReleaseError::Workspace {
            reason: "Cargo metadata did not include a resolved dependency graph".to_owned(),
        })?;
    let mut external = BTreeSet::new();
    for node in &resolve.nodes {
        if !publishable.contains(&node.id.repr) {
            continue;
        }
        for dependency in &node.deps {
            let publish_edge = dependency
                .dep_kinds
                .iter()
                .any(|kind| matches!(kind.kind, DependencyKind::Normal | DependencyKind::Build));
            if !publish_edge || workspace.contains(&dependency.pkg) {
                continue;
            }
            let package = metadata
                .packages
                .iter()
                .find(|package| package.id == dependency.pkg)
                .ok_or_else(|| ReleaseError::Workspace {
                    reason: format!(
                        "Cargo metadata omitted external dependency {}",
                        dependency.pkg
                    ),
                })?;
            external.insert((package.name.to_string(), package.version.to_string()));
        }
    }
    Ok(external.into_iter().collect())
}

/// Return dependency-first order with a lexical tie break.
pub fn stable_topological_order(
    nodes: &[PublishableNode],
) -> Result<Vec<PublishableNode>, ReleaseError> {
    let by_name = nodes
        .iter()
        .map(|node| (node.name.clone(), node.clone()))
        .collect::<BTreeMap<_, _>>();
    if by_name.len() != nodes.len() {
        return Err(ReleaseError::Workspace {
            reason: "publishable Cargo package names must be unique".to_owned(),
        });
    }

    let mut remaining = by_name
        .iter()
        .map(|(name, node)| {
            let dependencies = node
                .dependencies
                .iter()
                .filter(|dependency| by_name.contains_key(*dependency))
                .cloned()
                .collect::<BTreeSet<_>>();
            (name.clone(), dependencies)
        })
        .collect::<BTreeMap<_, _>>();
    let mut ready = remaining
        .iter()
        .filter(|(_, dependencies)| dependencies.is_empty())
        .map(|(name, _)| name.clone())
        .collect::<BTreeSet<_>>();
    let mut ordered = Vec::with_capacity(nodes.len());

    while let Some(name) = ready.pop_first() {
        let Some(node) = by_name.get(&name) else {
            return Err(ReleaseError::Workspace {
                reason: format!("publish graph lost package {name}"),
            });
        };
        ordered.push(node.clone());
        remaining.remove(&name);
        for (dependent, dependencies) in &mut remaining {
            dependencies.remove(&name);
            if dependencies.is_empty() {
                ready.insert(dependent.clone());
            }
        }
    }

    if !remaining.is_empty() {
        return Err(ReleaseError::PublishGraphCycle {
            packages: remaining.into_keys().collect(),
        });
    }
    Ok(ordered)
}

/// Look up a package by the opaque identifier captured in a graph node.
pub(crate) fn package_by_id<'a>(
    metadata: &'a Metadata,
    package_id: &str,
) -> Option<&'a cargo_metadata::Package> {
    metadata
        .packages
        .iter()
        .find(|package| package.id.repr == package_id)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn reference_order(nodes: &[PublishableNode]) -> Option<Vec<String>> {
        let mut emitted = BTreeSet::new();
        let mut order = Vec::new();
        while order.len() < nodes.len() {
            let candidate = nodes
                .iter()
                .filter(|node| !emitted.contains(&node.name))
                .filter(|node| {
                    node.dependencies
                        .iter()
                        .all(|dependency| emitted.contains(dependency))
                })
                .map(|node| node.name.clone())
                .min()?;
            emitted.insert(candidate.clone());
            order.push(candidate);
        }
        Some(order)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: release-engineering, Property 1: workspace-generic deterministic package plan
        #[test]
        fn generated_acyclic_graph_matches_reference(
            width in 1_usize..20,
            edge_bits in proptest::collection::vec(any::<bool>(), 1..400),
        ) {
            let mut bit = 0;
            let nodes = (0..width)
                .map(|index| {
                    let dependencies = (0..index)
                        .filter(|_| {
                            let selected = edge_bits[bit % edge_bits.len()];
                            bit += 1;
                            selected
                        })
                        .map(|dependency| format!("crate-{dependency:02}"))
                        .collect();
                    PublishableNode {
                        name: format!("crate-{index:02}"),
                        package_id: index.to_string(),
                        dependencies,
                    }
                })
                .rev()
                .collect::<Vec<_>>();
            let actual = stable_topological_order(&nodes).expect("generated graph is acyclic");
            let actual = actual.into_iter().map(|node| node.name).collect::<Vec<_>>();

            prop_assert_eq!(actual, reference_order(&nodes).expect("reference order"));
        }
    }

    #[test]
    fn development_cycle_shaped_link_is_not_a_graph_edge() {
        let nodes = vec![
            PublishableNode {
                name: "tokeira-chasm".to_owned(),
                package_id: "chasm".to_owned(),
                dependencies: vec!["tokeira-chasm-derive".to_owned()],
            },
            PublishableNode {
                name: "tokeira-chasm-derive".to_owned(),
                package_id: "derive".to_owned(),
                // The real reverse link is a dev-dependency and is intentionally absent.
                dependencies: Vec::new(),
            },
        ];

        let ordered = stable_topological_order(&nodes).expect("dev-only reverse link is harmless");
        assert_eq!(ordered[0].name, "tokeira-chasm-derive");
        assert_eq!(ordered[1].name, "tokeira-chasm");
    }
}
