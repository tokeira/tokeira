//! Deterministic logical-to-physical provider projection and explicit writeback.

use std::{collections::BTreeMap, sync::Arc};

use crate::{
    binding::Platform,
    catalog::{DeliveryProjection, PlacementContext, ProviderSet},
    definition::EvaluatedDefinition,
    error::ProjectionError,
    graph::{VerifiedGraph, WritebackValue},
    selection::{EffectiveSelection, SelectionDirection, select_modules},
};

/// Physical ids produced by the one realization used for modules and writeback.
#[derive(Debug, Clone, Default)]
pub struct RealizedResourceIndex {
    ids: BTreeMap<(String, String), tokeira_iac::ResourceId>,
}

impl RealizedResourceIndex {
    /// Resolve a logical module/resource identity to its engine resource id.
    pub fn get(&self, module: &str, resource: &str) -> Option<&tokeira_iac::ResourceId> {
        self.ids.get(&(module.to_string(), resource.to_string()))
    }
}

/// Complete provider-resource realization in definition order.
pub struct RealizedResources {
    /// Logical-to-physical index shared with writeback.
    pub index: RealizedResourceIndex,
    resources: Vec<Box<dyn tokeira_iac::Resource>>,
}

impl std::fmt::Debug for RealizedResources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealizedResources")
            .field("index", &self.index)
            .field("resource_count", &self.resources.len())
            .finish()
    }
}

impl RealizedResources {
    /// Borrow resources in definition declaration order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &dyn tokeira_iac::Resource> {
        self.resources.iter().map(Box::as_ref)
    }

    /// Consume into engine-owned provider resources.
    pub fn into_resources(self) -> Vec<Box<dyn tokeira_iac::Resource>> {
        self.resources
    }
}

/// Realize one verified graph while preserving every logical placement fact.
pub fn realize_resources(
    graph: &VerifiedGraph,
    deployment_id: &str,
    tags: &BTreeMap<String, String>,
) -> Result<RealizedResources, ProjectionError> {
    let mut index = RealizedResourceIndex::default();
    let mut resources = Vec::with_capacity(graph.resources().len());
    for resource in graph.resources() {
        let dependencies = resource
            .dependencies()
            .filter_map(|(module, logical_id)| index.get(module, logical_id).cloned())
            .collect();
        let placement = PlacementContext {
            deployment_id: deployment_id.to_string(),
            module: resource.module().to_string(),
            logical_id: resource.logical_id().to_string(),
            dependencies,
            tags: tags.clone(),
        };
        let realized = resource
            .kind()
            .realize(&placement)
            .map_err(|error| ProjectionError {
                resource: format!("{}/{}", resource.module(), resource.logical_id()),
                provider_kind: resource.kind().kind_name().to_string(),
                message: error.message,
            })?;
        index.ids.insert(
            (
                resource.module().to_string(),
                resource.logical_id().to_string(),
            ),
            realized.resource_id(),
        );
        resources.push(realized);
    }
    Ok(RealizedResources { index, resources })
}

/// Resolve only explicitly declared writebacks in declaration order.
pub fn resolve_writeback(
    graph: &VerifiedGraph,
    index: &RealizedResourceIndex,
    state: &tokeira_iac::InfraState,
) -> Vec<(String, String)> {
    graph
        .writeback()
        .iter()
        .filter_map(|entry| {
            let value = match entry.value() {
                WritebackValue::Literal(value) => Some(value.clone()),
                WritebackValue::Output(output) => {
                    let resource_id = index.get(output.module(), output.resource())?;
                    let resource = state.resources.get(resource_id)?;
                    resource
                        .properties
                        .get(output.output())
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                }
            }?;
            Some((entry.key().to_string(), value))
        })
        .collect()
}

/// Construct the lossless no-change plan outcome for provider-classified reachability issues.
pub fn no_change_issue_outcome(
    issues: Vec<tokeira_iac::PlatformIssue>,
) -> tokeira_iac::PlanOutcome {
    tokeira_iac::PlanOutcome {
        platform_issues: issues,
        ..tokeira_iac::PlanOutcome::default()
    }
}

/// Replace selected module state while retaining every unrelated recorded entry byte-for-byte.
pub fn replace_selected_state(
    recorded: &tokeira_iac::InfraState,
    selected_modules: &[String],
    replacement: &tokeira_iac::InfraState,
) -> tokeira_iac::InfraState {
    let selected = selected_modules
        .iter()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    let mut result = recorded.clone();
    result
        .resources
        .retain(|_, state| !selected.contains(state.module.as_str()));
    result.resources.extend(
        replacement
            .resources
            .iter()
            .filter(|(_, state)| selected.contains(state.module.as_str()))
            .map(|(id, state)| (id.clone(), state.clone())),
    );
    result
}

/// Generic framework projection for one selected platform binding.
#[derive(Debug)]
pub struct FrameworkDeployment<P: Platform> {
    definition: Arc<EvaluatedDefinition<P>>,
    providers: ProviderSet<P>,
}

impl<P: Platform> FrameworkDeployment<P> {
    /// Bind an evaluated definition to the selected provider registrations.
    pub fn new(definition: EvaluatedDefinition<P>, providers: ProviderSet<P>) -> Self {
        Self {
            definition: Arc::new(definition),
            providers,
        }
    }

    /// Borrow the admitted definition.
    pub fn definition(&self) -> &EvaluatedDefinition<P> {
        self.definition.as_ref()
    }

    /// Borrow the selected provider capabilities.
    pub fn providers(&self) -> &ProviderSet<P> {
        &self.providers
    }

    /// Project workloads only for providers that use the deploy-engine universe.
    pub fn services(
        &self,
        deployment_id: &str,
        tags: &BTreeMap<String, String>,
    ) -> Result<Vec<Box<dyn tokeira_deploy_engine::Service>>, crate::error::DeliveryError> {
        let mut services = Vec::new();
        for workload in self.definition.graph.workloads() {
            let delivery = self
                .providers
                .delivery(&workload.declaration().delivery)
                .ok_or_else(|| {
                    crate::error::DeliveryError::new(format!(
                        "delivery `{}` is absent from the selected provider set",
                        workload.declaration().delivery.as_str()
                    ))
                })?;
            let content = crate::artifact::ContentIdentitySet::default();
            if let DeliveryProjection::Workload(service) = delivery.realize(
                workload.declaration(),
                &PlacementContext {
                    deployment_id: deployment_id.to_string(),
                    module: workload.module().to_string(),
                    logical_id: workload.declaration().service.clone(),
                    dependencies: Vec::new(),
                    tags: tags.clone(),
                },
                &content,
            )? {
                services.push(service);
            }
        }
        Ok(services)
    }

    /// Compute one shared effective selection for plan/apply or destroy.
    pub fn select(
        &self,
        requested: Option<&[String]>,
        direction: SelectionDirection,
    ) -> Result<EffectiveSelection, crate::error::SelectionError> {
        select_modules(&self.definition.graph, requested, direction)
    }

    /// Wrap selected logical modules for the existing infrastructure engine.
    pub fn infra_modules(
        &self,
        selection: &EffectiveSelection,
        deployment_id: &str,
        tags: &BTreeMap<String, String>,
    ) -> Vec<Box<dyn tokeira_iac::Module>> {
        let selected = selection
            .modules()
            .iter()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>();
        self.definition
            .graph
            .modules()
            .iter()
            .enumerate()
            .filter(|(_, module)| selected.contains(module.name()))
            .map(|(index, _)| {
                Box::new(FrameworkModule {
                    definition: Arc::clone(&self.definition),
                    providers: self.providers.clone(),
                    module_index: index,
                    deployment_id: deployment_id.to_string(),
                    tags: tags.clone(),
                }) as Box<dyn tokeira_iac::Module>
            })
            .collect()
    }
}

struct FrameworkModule<P: Platform> {
    definition: Arc<EvaluatedDefinition<P>>,
    providers: ProviderSet<P>,
    module_index: usize,
    deployment_id: String,
    tags: BTreeMap<String, String>,
}

impl<P: Platform> std::fmt::Debug for FrameworkModule<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrameworkModule")
            .field(
                "name",
                &self.definition.graph.modules()[self.module_index].name(),
            )
            .field("deployment_id", &self.deployment_id)
            .finish_non_exhaustive()
    }
}

impl<P: Platform> tokeira_iac::Module for FrameworkModule<P> {
    fn name(&self) -> &str {
        self.definition.graph.modules()[self.module_index].name()
    }

    fn dependencies(&self) -> Vec<&str> {
        self.definition.graph.modules()[self.module_index]
            .dependencies()
            .iter()
            .map(String::as_str)
            .collect()
    }

    fn resources(
        &self,
        _ctx: &tokeira_iac::ModuleContext<'_>,
    ) -> Result<Vec<Box<dyn tokeira_iac::Resource>>, tokeira_iac::IacError> {
        let module = self.name().to_string();
        let realized =
            realize_resources(&self.definition.graph, &self.deployment_id, &self.tags)
                .map_err(|error| tokeira_iac::IacError::CompositionInvalid(error.to_string()))?;
        let mut resources = realized
            .into_resources()
            .into_iter()
            .filter(|resource| resource.module() == module)
            .collect::<Vec<_>>();
        for workload in self
            .definition
            .graph
            .workloads()
            .iter()
            .filter(|workload| workload.module() == module)
        {
            let delivery = self
                .providers
                .delivery(&workload.declaration().delivery)
                .ok_or_else(|| {
                    tokeira_iac::IacError::CompositionInvalid(format!(
                        "delivery `{}` is absent from the selected provider set",
                        workload.declaration().delivery.as_str()
                    ))
                })?;
            let projection = delivery
                .realize(
                    workload.declaration(),
                    &PlacementContext {
                        deployment_id: self.deployment_id.clone(),
                        module: module.clone(),
                        logical_id: workload.declaration().service.clone(),
                        dependencies: Vec::new(),
                        tags: self.tags.clone(),
                    },
                    &crate::artifact::ContentIdentitySet::default(),
                )
                .map_err(|error| tokeira_iac::IacError::CompositionInvalid(error.to_string()))?;
            if let DeliveryProjection::Infrastructure(resource) = projection {
                resources.push(resource);
            }
        }
        Ok(resources)
    }
}
