//! The author's builder vocabulary — the only verbs and types the operator's
//! definition may name. It records the deployment shape, then realizes each
//! declared resource/service directly to the engine types.
//!
//! No `Composition` IR, no `KindLibrary`, no `Realizer` trait: a kind *is* a
//! typed Rust struct that knows how to build its engine resource.

use tokeira_compose::ComposeService;
use tokeira_deploy_engine::{self as deploy_engine, RuntimeError, ServiceContext};
use tokeira_iac as iac;

use crate::{context::Cx, kinds::Service};

/// An author-defined kind: a typed struct that realizes to a concrete engine
/// resource. The operator names the struct; `realize` maps it to `tokeira_compose`
/// / `tokeira_aws` underneath.
pub trait Kind {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource>;
}

/// A handle to a declared module (carries only its name).
#[derive(Clone, Debug)]
pub struct ModuleRef {
    name: String,
}

/// A handle to a declared resource, used to reference its outputs (writeback).
#[derive(Clone, Debug)]
pub struct ResourceRef {
    module: String,
    resource: String,
}

impl ResourceRef {
    /// A deferred reference to one of this resource's outputs — resolved from
    /// infra state after apply. The handle *is* the binding to the resource id,
    /// so writeback never names a magic `"module.resource.output"` string.
    pub fn output(&self, name: &str) -> Output {
        Output {
            module: self.module.clone(),
            resource: self.resource.clone(),
            output: name.to_string(),
        }
    }
}

/// A deferred output reference (resolved from `InfraState` post-apply).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Output {
    pub module: String,
    pub resource: String,
    pub output: String,
}

/// A writeback value: a literal, or a deferred resource output. (The typed
/// `|t: &mut TokeiraConfig| …` closure form of Proposal 003 is an interpreter-
/// phase special form; the compiled playground uses explicit key/value pairs.)
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WbValue {
    Const(String),
    Output(Output),
}

impl From<&str> for WbValue {
    fn from(s: &str) -> Self {
        WbValue::Const(s.to_string())
    }
}
impl From<String> for WbValue {
    fn from(s: String) -> Self {
        WbValue::Const(s)
    }
}
impl From<Output> for WbValue {
    fn from(o: Output) -> Self {
        WbValue::Output(o)
    }
}

/// A logical volume anchor — the operator's *path-free* vocabulary for a bind
/// mount. The author resolves it to a concrete `host:container` string at realize
/// time (`kinds::Service::to_compose_service`), so the `.tkd` never names a host
/// path. `State`/`Config` anchor under the deployment's state/config dirs; `Raw`
/// is a vetted constant (the Docker socket) — the only escape hatch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Vol {
    State { sub: String, at: String },
    Config { sub: String, at: String },
    Raw(String),
}

struct Module {
    name: String,
    needs: Vec<String>,
    resources: Vec<ResourceEntry>,
}

struct ResourceEntry {
    id: String,
    kind: Box<dyn Kind>,
}

struct ServiceEntry {
    module: String,
    name: String,
    svc: Service,
}

/// The deployment under construction, returned by the operator's
/// `deployment(cfg, cx)`.
pub struct Deployment {
    namespaces: Vec<String>,
    modules: Vec<Module>,
    services: Vec<ServiceEntry>,
    writeback: Vec<(String, WbValue)>,
}

// Manual impl: modules hold `Box<dyn Kind>` resources with no `Debug`.
impl std::fmt::Debug for Deployment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Deployment")
            .field("namespaces", &self.namespaces)
            .field("modules", &self.modules.len())
            .field("services", &self.services.len())
            .finish_non_exhaustive()
    }
}

impl Deployment {
    /// Start a deployment with the given required namespaces.
    pub fn new(namespaces: &[&str]) -> Self {
        Self {
            namespaces: namespaces.iter().map(|s| s.to_string()).collect(),
            modules: Vec::new(),
            services: Vec::new(),
            writeback: Vec::new(),
        }
    }

    /// Declare a module (a resource grouping) and its module-level deps (names).
    pub fn module(&mut self, name: &str, needs: &[&str]) -> ModuleRef {
        self.modules.push(Module {
            name: name.to_string(),
            needs: needs.iter().map(|s| s.to_string()).collect(),
            resources: Vec::new(),
        });
        ModuleRef {
            name: name.to_string(),
        }
    }

    /// Add a resource of the given kind to a module; returns a handle for outputs.
    pub fn resource(
        &mut self,
        module: &ModuleRef,
        id: &str,
        kind: impl Kind + 'static,
    ) -> ResourceRef {
        self.resource_dyn(module, id, Box::new(kind))
    }

    /// Add an already-boxed kind to a module. The interpreter bridge constructs
    /// kinds as `Box<dyn Kind>` (it cannot name the concrete type), so it needs
    /// this object-safe entry point; [`resource`](Self::resource) delegates here.
    pub fn resource_dyn(
        &mut self,
        module: &ModuleRef,
        id: &str,
        kind: Box<dyn Kind>,
    ) -> ResourceRef {
        let m = self
            .modules
            .iter_mut()
            .find(|m| m.name == module.name)
            .expect("resource() references a module declared with module()");
        m.resources.push(ResourceEntry {
            id: id.to_string(),
            kind,
        });
        ResourceRef {
            module: module.name.clone(),
            resource: id.to_string(),
        }
    }

    /// Declare a service workload, a member of `module`. Its `needs` (deploy-phase
    /// ordering) are name-based, like module deps.
    pub fn service(&mut self, module: &ModuleRef, name: &str, svc: Service) {
        self.services.push(ServiceEntry {
            module: module.name.clone(),
            name: name.to_string(),
            svc,
        });
    }

    /// Record a writeback into the server config: a dotted key sourced from a
    /// literal or a resource output (`Deployment::collect_writeback` analog).
    pub fn writeback(&mut self, key: &str, value: impl Into<WbValue>) {
        self.writeback.push((key.to_string(), value.into()));
    }

    pub fn writeback_entries(&self) -> &[(String, WbValue)] {
        &self.writeback
    }

    // ── inspection / realization (the Deployment adapter + fidelity test) ──

    pub fn namespaces(&self) -> &[String] {
        &self.namespaces
    }

    pub fn module_names(&self) -> Vec<&str> {
        self.modules.iter().map(|m| m.name.as_str()).collect()
    }

    pub fn module_deps(&self, name: &str) -> Option<&[String]> {
        self.modules
            .iter()
            .find(|m| m.name == name)
            .map(|m| m.needs.as_slice())
    }

    pub fn service_names(&self) -> Vec<&str> {
        self.services.iter().map(|s| s.name.as_str()).collect()
    }

    /// The ids of a module's contents: declared resources, then member services
    /// (a compose service is itself an infra resource).
    pub fn resource_ids(&self, module: &str) -> Vec<String> {
        let mut ids: Vec<String> = self
            .modules
            .iter()
            .find(|m| m.name == module)
            .map(|m| m.resources.iter().map(|r| r.id.clone()).collect())
            .unwrap_or_default();
        ids.extend(
            self.services
                .iter()
                .filter(|s| s.module == module)
                .map(|s| s.name.clone()),
        );
        ids
    }

    /// Realize a module to its engine `iac::Resource`s — declared resources plus
    /// member services (each a compose-service infra resource).
    pub fn realize_module(&self, name: &str, cx: &Cx) -> Option<Vec<Box<dyn iac::Resource>>> {
        let module = self.modules.iter().find(|m| m.name == name)?;
        let mut resources: Vec<Box<dyn iac::Resource>> = module
            .resources
            .iter()
            .map(|r| r.kind.realize(cx))
            .collect();
        for s in self.services.iter().filter(|s| s.module == name) {
            resources.push(Box::new(s.svc.to_compose_service(&s.name, cx)));
        }
        Some(resources)
    }

    /// The `(service name, replicas)` pairs declared in this deployment, in
    /// declaration order — the engine adapter's `Ops::desired_replicas` source.
    pub fn service_replicas(&self) -> Vec<(String, u32)> {
        self.services
            .iter()
            .map(|s| (s.name.clone(), s.svc.replicas))
            .collect()
    }

    /// The physical engine `ResourceId` of a declared resource, obtained by
    /// realizing its kind. The adapter uses this to resolve writeback `Output`
    /// handles (logical `module.resource`) against `InfraState`.
    pub fn realize_resource_id(
        &self,
        module: &str,
        resource: &str,
        cx: &Cx,
    ) -> Option<iac::ResourceId> {
        let m = self.modules.iter().find(|m| m.name == module)?;
        let entry = m.resources.iter().find(|r| r.id == resource)?;
        Some(entry.kind.realize(cx).resource_id())
    }

    /// Realize every service to its deploy-engine workload (the deploy phase).
    /// Takes `cx` because the service realizer owns the host-path / AWS-edge /
    /// config-mount mechanics that were relocated out of the operator definition.
    pub fn realize_workloads(&self, cx: &Cx) -> Vec<Box<dyn deploy_engine::Service>> {
        self.services
            .iter()
            .map(|s| {
                Box::new(ComposeWorkload::new(
                    s.svc.to_compose_service(&s.name, cx),
                    s.module.clone(),
                    s.svc.needs.clone(),
                )) as Box<dyn deploy_engine::Service>
            })
            .collect()
    }
}

/// A compose service as a deploy-engine workload (the deploy phase). Carries its
/// owning module and deploy-ordering deps; the manifest is the compose service.
#[derive(Debug)]
struct ComposeWorkload {
    service: ComposeService,
    module: String,
    deps: Vec<&'static str>,
}

impl ComposeWorkload {
    fn new(service: ComposeService, module: String, needs: Vec<String>) -> Self {
        // The deploy-engine `dependencies()` returns `&[&str]`; leak the deploy
        // deps to `'static` so the slice can be returned (a playground shortcut).
        let deps = needs
            .into_iter()
            .map(|s| &*Box::leak(s.into_boxed_str()))
            .collect();
        Self {
            service,
            module,
            deps,
        }
    }
}

impl deploy_engine::Service for ComposeWorkload {
    fn name(&self) -> &str {
        &self.service.name
    }

    fn module(&self) -> &str {
        &self.module
    }

    fn dependencies(&self) -> &[&str] {
        &self.deps
    }

    fn manifests(&self, _ctx: &ServiceContext) -> Result<Vec<serde_json::Value>, RuntimeError> {
        Ok(vec![self.service.to_manifest()])
    }
}
