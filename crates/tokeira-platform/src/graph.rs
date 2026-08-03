//! Owned deployment graph construction, opaque handles, and immutable completion.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Weak},
};

use crate::{
    artifact::{DeliveryKey, DesiredDocument},
    catalog::ProviderKind,
    error::{GraphError, GraphFinding},
};

#[derive(Debug)]
struct GraphOwner;

/// Opaque handle to the sole deployment graph under construction.
#[derive(Debug, Clone)]
pub struct DeploymentHandle {
    owner: Weak<GraphOwner>,
}

/// Opaque handle to one declared module.
#[derive(Debug, Clone)]
pub struct ModuleHandle {
    owner: Weak<GraphOwner>,
    index: usize,
}

/// Opaque handle to one declared provider resource.
#[derive(Debug, Clone)]
pub struct ResourceHandle {
    owner: Weak<GraphOwner>,
    index: usize,
    module: String,
    logical_id: String,
    kind: String,
    declared_outputs: Arc<[String]>,
}

/// Opaque take-once provider-kind identity owned by an author session.
#[derive(Debug, Clone)]
pub struct KindHandle {
    owner: Weak<()>,
    index: usize,
}

impl KindHandle {
    pub(crate) fn new(owner: Weak<()>, index: usize) -> Self {
        Self { owner, index }
    }

    pub(crate) fn owner(&self) -> &Weak<()> {
        &self.owner
    }

    pub(crate) fn index(&self) -> usize {
        self.index
    }
}

/// Checked logical reference to one declared provider-resource output.
#[derive(Debug, Clone)]
pub struct OutputReference {
    owner: Weak<GraphOwner>,
    module: String,
    resource: String,
    output: String,
}

impl OutputReference {
    /// Owning logical module.
    pub fn module(&self) -> &str {
        &self.module
    }

    /// Logical resource id within the module.
    pub fn resource(&self) -> &str {
        &self.resource
    }

    /// Declared provider output name.
    pub fn output(&self) -> &str {
        &self.output
    }
}

impl ResourceHandle {
    /// Construct a checked output reference without consulting provider state.
    pub fn output(&self, name: &str) -> Result<OutputReference, GraphError> {
        let Some(owner) = self.owner.upgrade() else {
            return Err(GraphError::ExpiredHandle { kind: "resource" });
        };
        drop(owner);
        if !self
            .declared_outputs
            .iter()
            .any(|declared| declared == name)
        {
            return Err(GraphError::UnknownOutput {
                kind: self.kind.clone(),
                output: name.to_string(),
                supported: self.declared_outputs.to_vec(),
            });
        }
        Ok(OutputReference {
            owner: self.owner.clone(),
            module: self.module.clone(),
            resource: self.logical_id.clone(),
            output: name.to_string(),
        })
    }
}

/// Literal or checked provider output written to runtime-server configuration.
#[derive(Debug, Clone)]
pub enum WritebackValue {
    /// Exact literal string.
    Literal(String),
    /// Provider output resolved through the realized physical-resource index.
    Output(OutputReference),
}

/// One ordered explicit writeback declaration.
#[derive(Debug, Clone)]
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

/// Platform-owned workload intent attached to one logical module.
#[derive(Debug, Clone)]
pub struct WorkloadDeclaration {
    /// Logical service from the selected platform service catalog.
    pub service: String,
    /// Logical service dependencies.
    pub dependencies: Vec<String>,
    /// Desired workload capacity.
    pub desired_capacity: u32,
    /// Selected provider-owned delivery mechanics.
    pub delivery: DeliveryKey,
    /// Complete platform-owned provider document.
    pub document: DesiredDocument,
}

/// One completed logical module.
#[derive(Debug)]
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

/// One completed provider-resource declaration.
#[derive(Debug)]
pub struct ResourceNode {
    module: String,
    logical_id: String,
    kind: Box<dyn ProviderKind>,
    dependencies: Vec<ResourceKey>,
}

impl ResourceNode {
    /// Owning module.
    pub fn module(&self) -> &str {
        &self.module
    }

    /// Logical id within the module.
    pub fn logical_id(&self) -> &str {
        &self.logical_id
    }

    /// Selected canonical provider kind.
    pub fn kind(&self) -> &dyn ProviderKind {
        self.kind.as_ref()
    }

    /// Ordered logical resource dependencies.
    pub fn dependencies(&self) -> impl ExactSizeIterator<Item = (&str, &str)> {
        self.dependencies
            .iter()
            .map(|dependency| (dependency.module.as_str(), dependency.logical_id.as_str()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ResourceKey {
    module: String,
    logical_id: String,
}

/// One completed platform-workload declaration.
#[derive(Debug)]
pub struct WorkloadNode {
    module: String,
    declaration: WorkloadDeclaration,
}

impl WorkloadNode {
    /// Owning logical module.
    pub fn module(&self) -> &str {
        &self.module
    }

    /// Complete platform-owned workload intent.
    pub fn declaration(&self) -> &WorkloadDeclaration {
        &self.declaration
    }
}

/// Mutable graph used only during one pure definition evaluation.
#[derive(Debug)]
pub struct DeploymentGraphBuilder {
    owner: Arc<GraphOwner>,
    namespaces: Vec<String>,
    modules: Vec<ModuleNode>,
    resources: Vec<ResourceNode>,
    workloads: Vec<WorkloadNode>,
    writeback: Vec<WritebackEntry>,
    services: BTreeSet<String>,
    deliveries: BTreeSet<String>,
    bootstrap_module: Option<String>,
}

impl DeploymentGraphBuilder {
    /// Construct a graph without selected workload catalogs.
    pub fn new() -> Self {
        Self::with_catalogs(BTreeSet::new(), BTreeSet::new())
    }

    /// Construct a graph with the exact service and delivery inventories selected by a binding.
    pub fn with_catalogs(services: BTreeSet<String>, deliveries: BTreeSet<String>) -> Self {
        Self {
            owner: Arc::new(GraphOwner),
            namespaces: Vec::new(),
            modules: Vec::new(),
            resources: Vec::new(),
            workloads: Vec::new(),
            writeback: Vec::new(),
            services,
            deliveries,
            bootstrap_module: None,
        }
    }

    /// Require the binding's state-bootstrap module at graph completion.
    pub fn require_bootstrap(mut self, module: impl Into<String>) -> Self {
        self.bootstrap_module = Some(module.into());
        self
    }

    /// Return the sole deployment handle accepted by this graph.
    pub fn deployment_handle(&self) -> DeploymentHandle {
        DeploymentHandle {
            owner: Arc::downgrade(&self.owner),
        }
    }

    /// Record a namespace in definition order.
    pub fn add_namespace(
        &mut self,
        deployment: &DeploymentHandle,
        namespace: String,
    ) -> Result<(), GraphError> {
        self.check_graph_owner(&deployment.owner, "deployment")?;
        self.namespaces.push(namespace);
        Ok(())
    }

    /// Declare a module and ordered dependencies.
    pub fn add_module(
        &mut self,
        deployment: &DeploymentHandle,
        name: String,
        dependencies: Vec<ModuleHandle>,
    ) -> Result<ModuleHandle, GraphError> {
        self.check_graph_owner(&deployment.owner, "deployment")?;
        let mut dependency_names = Vec::with_capacity(dependencies.len());
        for dependency in dependencies {
            self.check_graph_owner(&dependency.owner, "module")?;
            let Some(node) = self.modules.get(dependency.index) else {
                return Err(GraphError::ExpiredHandle { kind: "module" });
            };
            dependency_names.push(node.name.clone());
        }
        let index = self.modules.len();
        self.modules.push(ModuleNode {
            name,
            dependencies: dependency_names,
        });
        Ok(ModuleHandle {
            owner: Arc::downgrade(&self.owner),
            index,
        })
    }

    /// Declare one provider resource through an owning module handle.
    pub fn add_resource(
        &mut self,
        module: &ModuleHandle,
        logical_id: String,
        kind: Box<dyn ProviderKind>,
        dependencies: Vec<ResourceHandle>,
    ) -> Result<ResourceHandle, GraphError> {
        self.validate_resource_insertion(module, &dependencies)?;
        let module_node = &self.modules[module.index];
        let module_name = module_node.name.clone();
        let mut dependency_keys = Vec::with_capacity(dependencies.len());
        for dependency in dependencies {
            dependency_keys.push(ResourceKey {
                module: dependency.module,
                logical_id: dependency.logical_id,
            });
        }
        let kind_name = kind.kind_name().to_string();
        let declared_outputs: Arc<[String]> = kind
            .declared_outputs()
            .iter()
            .map(|output| (*output).to_string())
            .collect::<Vec<_>>()
            .into();
        let index = self.resources.len();
        self.resources.push(ResourceNode {
            module: module_name.clone(),
            logical_id: logical_id.clone(),
            kind,
            dependencies: dependency_keys,
        });
        Ok(ResourceHandle {
            owner: Arc::downgrade(&self.owner),
            index,
            module: module_name,
            logical_id,
            kind: kind_name,
            declared_outputs,
        })
    }

    pub(crate) fn validate_resource_insertion(
        &self,
        module: &ModuleHandle,
        dependencies: &[ResourceHandle],
    ) -> Result<(), GraphError> {
        self.check_graph_owner(&module.owner, "module")?;
        if self.modules.get(module.index).is_none() {
            return Err(GraphError::ExpiredHandle { kind: "module" });
        }
        for dependency in dependencies {
            self.check_graph_owner(&dependency.owner, "resource")?;
            if self.resources.get(dependency.index).is_none() {
                return Err(GraphError::ExpiredHandle { kind: "resource" });
            }
        }
        Ok(())
    }

    /// Declare one platform-owned workload under a module.
    pub fn add_workload(
        &mut self,
        module: &ModuleHandle,
        declaration: WorkloadDeclaration,
    ) -> Result<(), GraphError> {
        self.check_graph_owner(&module.owner, "module")?;
        let Some(module_node) = self.modules.get(module.index) else {
            return Err(GraphError::ExpiredHandle { kind: "module" });
        };
        self.workloads.push(WorkloadNode {
            module: module_node.name.clone(),
            declaration,
        });
        Ok(())
    }

    /// Record one explicit writeback declaration in definition order.
    pub fn add_writeback(
        &mut self,
        deployment: &DeploymentHandle,
        key: String,
        value: WritebackValue,
    ) -> Result<(), GraphError> {
        self.check_graph_owner(&deployment.owner, "deployment")?;
        if let WritebackValue::Output(output) = &value {
            self.check_graph_owner(&output.owner, "output")?;
        }
        self.writeback.push(WritebackEntry { key, value });
        Ok(())
    }

    /// Complete the graph after admitting the frontend's final deployment handle.
    pub fn finish_for(self, deployment: DeploymentHandle) -> Result<VerifiedGraph, GraphError> {
        self.check_graph_owner(&deployment.owner, "deployment")?;
        self.finish()
    }

    /// Validate structural invariants and return an immutable graph.
    pub fn finish(self) -> Result<VerifiedGraph, GraphError> {
        let findings = self.findings();
        if !findings.is_empty() {
            return Err(GraphError::Invalid(findings));
        }
        Ok(VerifiedGraph {
            _owner: self.owner,
            namespaces: self.namespaces,
            modules: self.modules,
            resources: self.resources,
            workloads: self.workloads,
            writeback: self.writeback,
        })
    }

    fn findings(&self) -> Vec<GraphFinding> {
        let mut findings = Vec::new();
        let mut modules = BTreeSet::new();
        for module in &self.modules {
            if !modules.insert(module.name.clone()) {
                findings.push(GraphFinding::DuplicateModule(module.name.clone()));
            }
        }
        if let Some(bootstrap) = &self.bootstrap_module
            && !modules.contains(bootstrap)
        {
            findings.push(GraphFinding::MissingBootstrap(bootstrap.clone()));
        }
        for module in &self.modules {
            for dependency in &module.dependencies {
                if !modules.contains(dependency) {
                    findings.push(GraphFinding::UnknownModuleDependency {
                        module: module.name.clone(),
                        dependency: dependency.clone(),
                    });
                }
            }
        }
        if let Some(cycle) = module_cycle(&self.modules) {
            findings.push(GraphFinding::ModuleCycle(cycle));
        }

        let mut resources = BTreeSet::new();
        for resource in &self.resources {
            let identity = (resource.module.clone(), resource.logical_id.clone());
            if !resources.insert(identity) {
                findings.push(GraphFinding::DuplicateResource {
                    module: resource.module.clone(),
                    resource: resource.logical_id.clone(),
                });
            }
        }
        for resource in &self.resources {
            for dependency in &resource.dependencies {
                if !resources.contains(&(dependency.module.clone(), dependency.logical_id.clone()))
                {
                    findings.push(GraphFinding::UnknownResourceDependency {
                        module: resource.module.clone(),
                        resource: resource.logical_id.clone(),
                        dependency_module: dependency.module.clone(),
                        dependency_resource: dependency.logical_id.clone(),
                    });
                }
            }
        }

        let mut writeback = BTreeSet::new();
        for entry in &self.writeback {
            if !writeback.insert(entry.key.clone()) {
                findings.push(GraphFinding::DuplicateWriteback(entry.key.clone()));
            }
        }

        for workload in &self.workloads {
            if !self.services.contains(&workload.declaration.service) {
                findings.push(GraphFinding::UnknownService(
                    workload.declaration.service.clone(),
                ));
            }
            if !self
                .deliveries
                .contains(workload.declaration.delivery.as_str())
            {
                findings.push(GraphFinding::UnknownDelivery(
                    workload.declaration.delivery.as_str().to_string(),
                ));
            }
        }
        findings
    }
}

impl Default for DeploymentGraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// Owner comparison stays entirely in safe Rust by keeping this helper concrete.
impl DeploymentGraphBuilder {
    fn check_graph_owner(
        &self,
        owner: &Weak<GraphOwner>,
        kind: &'static str,
    ) -> Result<(), GraphError> {
        let Some(candidate) = owner.upgrade() else {
            return Err(GraphError::ExpiredHandle { kind });
        };
        if !Arc::ptr_eq(&candidate, &self.owner) {
            return Err(GraphError::ForeignHandle { kind });
        }
        Ok(())
    }
}

fn module_cycle(modules: &[ModuleNode]) -> Option<Vec<String>> {
    fn visit(
        name: &str,
        by_name: &HashMap<&str, &ModuleNode>,
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
        if let Some(module) = by_name.get(name) {
            for dependency in &module.dependencies {
                if let Some(cycle) = visit(dependency, by_name, marks, stack) {
                    return Some(cycle);
                }
            }
        }
        let _ = stack.pop();
        marks.insert(name.to_string(), 2);
        None
    }

    let by_name = modules
        .iter()
        .map(|module| (module.name.as_str(), module))
        .collect::<HashMap<_, _>>();
    let mut marks = HashMap::new();
    let mut stack = Vec::new();
    for module in modules {
        if let Some(cycle) = visit(&module.name, &by_name, &mut marks, &mut stack) {
            return Some(cycle);
        }
    }
    None
}

/// Immutable, structurally verified deployment graph.
#[derive(Debug)]
pub struct VerifiedGraph {
    // Retains handle identity for read-only handles that outlive the builder.
    _owner: Arc<GraphOwner>,
    namespaces: Vec<String>,
    modules: Vec<ModuleNode>,
    resources: Vec<ResourceNode>,
    workloads: Vec<WorkloadNode>,
    writeback: Vec<WritebackEntry>,
}

impl VerifiedGraph {
    /// Required namespaces in declaration order.
    pub fn namespaces(&self) -> &[String] {
        &self.namespaces
    }

    /// Modules in declaration order.
    pub fn modules(&self) -> &[ModuleNode] {
        &self.modules
    }

    /// Provider resources in declaration order.
    pub fn resources(&self) -> &[ResourceNode] {
        &self.resources
    }

    /// Platform workloads in declaration order.
    pub fn workloads(&self) -> &[WorkloadNode] {
        &self.workloads
    }

    /// Explicit writeback declarations in declaration order.
    pub fn writeback(&self) -> &[WritebackEntry] {
        &self.writeback
    }
}

#[cfg(test)]
mod property_tests {
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;

    use super::*;
    use crate::{catalog::PlacementContext, error::KindError};

    #[derive(Debug)]
    struct ProbeKind;

    impl ProviderKind for ProbeKind {
        fn kind_name(&self) -> &'static str {
            "ProbeKind"
        }

        fn validate_input(&self) -> Result<(), KindError> {
            Ok(())
        }

        fn declared_outputs(&self) -> &'static [&'static str] {
            &[]
        }

        fn desired_manifest(&self) -> serde_json::Value {
            serde_json::Value::Null
        }

        fn realize(
            &self,
            _placement: &PlacementContext,
        ) -> Result<Box<dyn tokeira_iac::Resource>, KindError> {
            Err(KindError::new("property probe is never realized"))
        }
    }

    fn reference_valid(graph: &DeploymentGraphBuilder) -> bool {
        let module_names = graph
            .modules
            .iter()
            .map(|module| module.name.as_str())
            .collect::<BTreeSet<_>>();
        if module_names.len() != graph.modules.len()
            || graph.modules.iter().any(|module| {
                module
                    .dependencies
                    .iter()
                    .any(|dependency| !module_names.contains(dependency.as_str()))
            })
        {
            return false;
        }

        let dependencies = graph
            .modules
            .iter()
            .map(|module| {
                (
                    module.name.as_str(),
                    module
                        .dependencies
                        .iter()
                        .map(String::as_str)
                        .collect::<BTreeSet<_>>(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut admitted = BTreeSet::new();
        loop {
            let before = admitted.len();
            for (module, required) in &dependencies {
                if required.is_subset(&admitted) {
                    admitted.insert(*module);
                }
            }
            if admitted.len() == dependencies.len() {
                break;
            }
            if admitted.len() == before {
                return false;
            }
        }

        let resources = graph
            .resources
            .iter()
            .map(|resource| (resource.module.as_str(), resource.logical_id.as_str()))
            .collect::<BTreeSet<_>>();
        if resources.len() != graph.resources.len()
            || graph.resources.iter().any(|resource| {
                resource.dependencies.iter().any(|dependency| {
                    !resources
                        .contains(&(dependency.module.as_str(), dependency.logical_id.as_str()))
                })
            })
        {
            return false;
        }

        graph
            .writeback
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<BTreeSet<_>>()
            .len()
            == graph.writeback.len()
    }

    proptest! {
        // Completion matches an independent validator across valid graphs and one-fault mutations.
        // Feature: platform-builder-abstraction, Property 2: finished graphs are exactly the well-formed graphs
        #[test]
        fn property_2_finish_matches_reference_validator(
            module_count in 2_usize..12,
            resource_count in 2_usize..12,
            fault in 0_u8..7,
        ) {
            let mut graph = DeploymentGraphBuilder::new();
            let deployment = graph.deployment_handle();
            let mut modules = Vec::new();
            for index in 0..module_count {
                modules.push(
                    graph
                        .add_module(&deployment, format!("module-{index}"), Vec::new())
                        .expect("owned module"),
                );
            }
            for index in 0..resource_count {
                graph
                    .add_resource(
                        &modules[index % module_count],
                        format!("resource-{index}"),
                        Box::new(ProbeKind),
                        Vec::new(),
                    )
                    .expect("owned resource");
            }
            graph
                .add_writeback(
                    &deployment,
                    "runtime.first".to_string(),
                    WritebackValue::Literal("one".to_string()),
                )
                .expect("owned writeback");
            graph
                .add_writeback(
                    &deployment,
                    "runtime.second".to_string(),
                    WritebackValue::Literal("two".to_string()),
                )
                .expect("owned writeback");

            match fault {
                0 => {}
                1 => graph.modules[module_count - 1].name = "module-0".to_string(),
                2 => graph.modules[0].dependencies.push("missing".to_string()),
                3 => {
                    graph.modules[0].dependencies.push("module-1".to_string());
                    graph.modules[1].dependencies.push("module-0".to_string());
                }
                4 => {
                    let module = graph.resources[0].module.clone();
                    let logical_id = graph.resources[0].logical_id.clone();
                    graph.resources[resource_count - 1].module = module;
                    graph.resources[resource_count - 1].logical_id = logical_id;
                }
                5 => graph.resources[0].dependencies.push(ResourceKey {
                    module: "module-0".to_string(),
                    logical_id: "missing".to_string(),
                }),
                6 => graph.writeback[1].key = graph.writeback[0].key.clone(),
                _ => unreachable!("the generated fault is bounded"),
            }

            let expected = reference_valid(&graph);
            prop_assert_eq!(graph.finish().is_ok(), expected);
        }
    }
}
