//! Transient structural deployment graph and deterministic validation.

use std::collections::{BTreeSet, HashMap};

use crate::error::{GraphError, GraphFinding};

/// Stable logical reference to one declared resource.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceReference {
    module: String,
    logical_id: String,
}

impl ResourceReference {
    /// Owning logical module.
    pub fn module(&self) -> &str {
        &self.module
    }

    /// Logical resource id within its module.
    pub fn logical_id(&self) -> &str {
        &self.logical_id
    }
}

/// Deferred logical reference to one resource output.
///
/// The resource itself supplies its output contract after realization; this
/// structural handle only proves that the referenced logical resource exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputReference {
    resource: ResourceReference,
    output: String,
}

impl OutputReference {
    /// Referenced logical resource.
    pub fn resource(&self) -> &ResourceReference {
        &self.resource
    }

    /// Declared provider output name.
    pub fn output(&self) -> &str {
        &self.output
    }
}

/// Literal or checked provider output written to runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WritebackValue {
    /// Exact literal string.
    Literal(String),
    /// Provider output resolved through the executed resource set.
    Output(OutputReference),
}

/// One ordered explicit writeback declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritebackEntry {
    key: String,
    value: WritebackValue,
}

impl WritebackEntry {
    /// Declared dotted runtime-config key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Literal or checked output source.
    pub fn value(&self) -> &WritebackValue {
        &self.value
    }
}

/// One completed logical module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleNode {
    name: String,
    dependencies: Vec<String>,
}

impl ModuleNode {
    /// Stable logical name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Ordered module dependencies.
    pub fn dependencies(&self) -> &[String] {
        &self.dependencies
    }
}

/// One completed concrete provider-resource declaration.
#[derive(Debug)]
pub struct ResourceNode<K> {
    reference: ResourceReference,
    kind: K,
    dependencies: Vec<ResourceReference>,
}

impl<K> ResourceNode<K> {
    /// Stable logical reference.
    pub fn reference(&self) -> &ResourceReference {
        &self.reference
    }

    /// Owning logical module.
    pub fn module(&self) -> &str {
        self.reference.module()
    }

    /// Logical id within the module.
    pub fn logical_id(&self) -> &str {
        self.reference.logical_id()
    }

    /// Concrete platform-owned kind.
    pub fn kind(&self) -> &K {
        &self.kind
    }

    /// Ordered logical dependencies.
    pub fn dependencies(&self) -> &[ResourceReference] {
        &self.dependencies
    }
}

/// Mutable graph used only during one definition evaluation.
#[derive(Debug, Default)]
pub struct StructuralGraphBuilder<K> {
    namespaces: Vec<String>,
    modules: Vec<ModuleNode>,
    resources: Vec<ResourceNode<K>>,
    writeback: Vec<WritebackEntry>,
}

impl<K> StructuralGraphBuilder<K> {
    /// Begin one empty transient graph.
    pub fn new() -> Self {
        Self {
            namespaces: Vec::new(),
            modules: Vec::new(),
            resources: Vec::new(),
            writeback: Vec::new(),
        }
    }

    /// Record a namespace in declaration order.
    pub fn add_namespace(&mut self, namespace: impl Into<String>) {
        self.namespaces.push(namespace.into());
    }

    /// Record a module and its ordered prerequisite names.
    pub fn add_module(&mut self, name: impl Into<String>, dependencies: Vec<String>) {
        self.modules.push(ModuleNode {
            name: name.into(),
            dependencies,
        });
    }

    /// Record one concrete resource and return its stable logical reference.
    pub fn add_resource(
        &mut self,
        module: impl Into<String>,
        logical_id: impl Into<String>,
        kind: K,
        dependencies: Vec<ResourceReference>,
    ) -> ResourceReference {
        let reference = ResourceReference {
            module: module.into(),
            logical_id: logical_id.into(),
        };
        self.resources.push(ResourceNode {
            reference: reference.clone(),
            kind,
            dependencies,
        });
        reference
    }

    /// Record one writeback in declaration order.
    pub fn add_writeback(&mut self, key: impl Into<String>, value: WritebackValue) {
        self.writeback.push(WritebackEntry {
            key: key.into(),
            value,
        });
    }

    /// Complete the graph after all structural invariants hold.
    pub fn finish(self) -> Result<VerifiedGraph<K>, GraphError> {
        let findings = self.findings();
        if !findings.is_empty() {
            return Err(GraphError::Invalid(findings));
        }
        Ok(VerifiedGraph {
            namespaces: self.namespaces,
            modules: self.modules,
            resources: self.resources,
            writeback: self.writeback,
        })
    }

    fn findings(&self) -> Vec<GraphFinding> {
        let mut findings = Vec::new();
        duplicates(&self.namespaces, |name| {
            findings.push(GraphFinding::DuplicateNamespace(name.to_string()));
        });

        let module_names = self
            .modules
            .iter()
            .map(|module| module.name.as_str())
            .collect::<BTreeSet<_>>();
        let module_positions = self
            .modules
            .iter()
            .enumerate()
            .map(|(index, module)| (module.name.as_str(), index))
            .collect::<HashMap<_, _>>();
        if module_names.len() != self.modules.len() {
            let mut seen = BTreeSet::new();
            for module in &self.modules {
                if !seen.insert(module.name.as_str()) {
                    findings.push(GraphFinding::DuplicateModule(module.name.clone()));
                }
            }
        }
        for module in &self.modules {
            for dependency in &module.dependencies {
                if !module_names.contains(dependency.as_str()) {
                    findings.push(GraphFinding::UnknownModuleDependency {
                        module: module.name.clone(),
                        dependency: dependency.clone(),
                    });
                } else if module_positions[dependency.as_str()]
                    >= module_positions[module.name.as_str()]
                {
                    findings.push(GraphFinding::ModuleDependencyOrder {
                        module: module.name.clone(),
                        dependency: dependency.clone(),
                    });
                }
            }
        }
        if let Some(cycle) = cycle(self.modules.iter().map(|module| {
            (
                module.name.as_str(),
                module
                    .dependencies
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            )
        })) {
            findings.push(GraphFinding::ModuleCycle(cycle));
        }

        let resource_names = self
            .resources
            .iter()
            .map(|resource| resource.reference.clone())
            .collect::<BTreeSet<_>>();
        let resource_positions = self
            .resources
            .iter()
            .enumerate()
            .map(|(index, resource)| (resource.reference.clone(), index))
            .collect::<HashMap<_, _>>();
        if resource_names.len() != self.resources.len() {
            let mut seen = BTreeSet::new();
            for resource in &self.resources {
                if !seen.insert(resource.reference.clone()) {
                    findings.push(GraphFinding::DuplicateResource {
                        module: resource.module().to_string(),
                        resource: resource.logical_id().to_string(),
                    });
                }
            }
        }
        for resource in &self.resources {
            if !module_names.contains(resource.module()) {
                findings.push(GraphFinding::UnknownResourceModule {
                    module: resource.module().to_string(),
                    resource: resource.logical_id().to_string(),
                });
            }
            for dependency in &resource.dependencies {
                if !resource_names.contains(dependency) {
                    findings.push(GraphFinding::UnknownResourceDependency {
                        module: resource.module().to_string(),
                        resource: resource.logical_id().to_string(),
                        dependency_module: dependency.module.clone(),
                        dependency_resource: dependency.logical_id.clone(),
                    });
                } else if resource_positions[dependency] >= resource_positions[&resource.reference]
                {
                    findings.push(GraphFinding::ResourceDependencyOrder {
                        module: resource.module().to_string(),
                        resource: resource.logical_id().to_string(),
                        dependency_module: dependency.module.clone(),
                        dependency_resource: dependency.logical_id.clone(),
                    });
                }
            }
        }
        let resource_keys = self
            .resources
            .iter()
            .map(|resource| {
                (
                    resource_key(&resource.reference),
                    resource
                        .dependencies
                        .iter()
                        .map(resource_key)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        if let Some(cycle) = cycle(resource_keys.iter().map(|(name, dependencies)| {
            (
                name.as_str(),
                dependencies.iter().map(String::as_str).collect(),
            )
        })) {
            findings.push(GraphFinding::ResourceCycle(cycle));
        }

        let mut writeback = BTreeSet::new();
        for entry in &self.writeback {
            if !writeback.insert(entry.key.as_str()) {
                findings.push(GraphFinding::DuplicateWriteback(entry.key.clone()));
            }
            if let WritebackValue::Output(output) = &entry.value
                && !resource_names.contains(output.resource())
            {
                findings.push(GraphFinding::UnknownWritebackResource {
                    key: entry.key.clone(),
                    module: output.resource.module.clone(),
                    resource: output.resource.logical_id.clone(),
                });
            }
        }
        findings
    }
}

impl<K> StructuralGraphBuilder<K> {
    /// Construct an output reference from a resource declared so far.
    pub fn output(
        &self,
        resource: &ResourceReference,
        output: &str,
    ) -> Result<OutputReference, GraphError> {
        if !self
            .resources
            .iter()
            .any(|node| node.reference() == resource)
        {
            return Err(GraphError::UnknownResource {
                module: resource.module.clone(),
                resource: resource.logical_id.clone(),
            });
        }
        Ok(OutputReference {
            resource: resource.clone(),
            output: output.to_string(),
        })
    }
}

/// Immutable graph retained only for the current evaluation/execution.
#[derive(Debug)]
pub struct VerifiedGraph<K> {
    namespaces: Vec<String>,
    modules: Vec<ModuleNode>,
    resources: Vec<ResourceNode<K>>,
    writeback: Vec<WritebackEntry>,
}

impl<K> VerifiedGraph<K> {
    /// Required namespaces in declaration order.
    pub fn namespaces(&self) -> &[String] {
        &self.namespaces
    }

    /// Modules in declaration order.
    pub fn modules(&self) -> &[ModuleNode] {
        &self.modules
    }

    /// Resources in declaration order.
    pub fn resources(&self) -> &[ResourceNode<K>] {
        &self.resources
    }

    /// Writebacks in declaration order.
    pub fn writeback(&self) -> &[WritebackEntry] {
        &self.writeback
    }
}

fn duplicates(values: &[String], mut report: impl FnMut(&str)) {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value.as_str()) {
            report(value);
        }
    }
}

fn resource_key(reference: &ResourceReference) -> String {
    format!("{}/{}", reference.module, reference.logical_id)
}

fn cycle<'a>(nodes: impl IntoIterator<Item = (&'a str, Vec<&'a str>)>) -> Option<Vec<String>> {
    fn visit(
        name: &str,
        edges: &HashMap<&str, Vec<&str>>,
        marks: &mut HashMap<String, u8>,
        stack: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        match marks.get(name).copied() {
            Some(1) => {
                let start = stack.iter().position(|member| member == name).unwrap_or(0);
                return Some(stack[start..].to_vec());
            }
            Some(2) => return None,
            _ => {}
        }
        marks.insert(name.to_string(), 1);
        stack.push(name.to_string());
        if let Some(dependencies) = edges.get(name) {
            for dependency in dependencies {
                if edges.contains_key(dependency)
                    && let Some(found) = visit(dependency, edges, marks, stack)
                {
                    return Some(found);
                }
            }
        }
        let _ = stack.pop();
        marks.insert(name.to_string(), 2);
        None
    }

    let edges = nodes.into_iter().collect::<HashMap<_, _>>();
    let mut marks = HashMap::new();
    let mut stack = Vec::new();
    for name in edges.keys() {
        if let Some(found) = visit(name, &edges, &mut marks, &mut stack) {
            return Some(found);
        }
    }
    None
}
