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

/// The definition's config-files declaration as its consumers need it: the
/// engine id (the dependency edge) and the canonical-manifest digest (the
/// content coupling).
struct DeclaredConfigFiles {
    id: String,
    digest: String,
}

/// An author-defined kind: a typed struct that realizes to a concrete engine
/// resource. The operator names the struct; `realize` maps it to `tokeira_compose`
/// / `tokeira_aws` underneath.
pub trait Kind {
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource>;

    /// The authored desired content as a JSON value — what a desired snapshot
    /// records for this resource. Pure kind data: realization identity comes
    /// from `realize`; nothing environmental belongs here.
    fn manifest(&self) -> serde_json::Value;
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
        // A service that bind-mounts config-anchored volumes (`Vol::Config`)
        // depends on the resource that WRITES them. Without this edge the
        // engine's creation order is free to start the container first —
        // and Docker manufactures a missing bind source as a *directory*,
        // poisoning the path the config writer then fails on (EISDIR).
        // Wired automatically from the typed volume anchor whenever the
        // definition declares the config-files resource; the edge is
        // infra-graph-only and never leaks into the compose manifest.
        let config = self.declared_config_files(cx);
        for s in self.services.iter().filter(|s| s.module == name) {
            resources.push(Box::new(self.realized_service(s, cx, config.as_ref())));
        }
        Some(resources)
    }

    /// Realize one member service — the single path module realization and
    /// the desired snapshot share, so the infra dependency wiring can never
    /// diverge between them.
    fn realized_service(
        &self,
        entry: &ServiceEntry,
        cx: &Cx,
        config: Option<&DeclaredConfigFiles>,
    ) -> ComposeService {
        let mut svc = entry.svc.to_compose_service(&entry.name, cx);
        if let Some(config) = config
            && entry
                .svc
                .volumes
                .iter()
                .any(|v| matches!(v, Vol::Config { .. }))
        {
            svc.resource_dependencies.push(config.id.clone());
            // The content half of the coupling: the consumer's manifest
            // carries the config declaration's digest, so a config-parameter
            // edit diffs the consuming service too — the plan states the
            // update and the apply recreates the container onto the
            // rewritten files. The edge alone only orders creation; it
            // cannot restart a running consumer.
            svc.environment
                .insert("TOKEIRA_CONFIG_DIGEST".into(), config.digest.clone());
        }
        svc
    }

    /// Canonical desired manifests for every resource this definition
    /// realizes: ids from the same realization the engine uses; content from
    /// the authored kind data and the canonical service manifests. Pure — no
    /// provider, no live state, no writes.
    pub fn desired_snapshot(
        &self,
        cx: &Cx,
    ) -> std::collections::BTreeMap<iac::ResourceId, serde_json::Value> {
        use iac::Resource as _;
        let config = self.declared_config_files(cx);
        let mut snapshot = std::collections::BTreeMap::new();
        for module in &self.modules {
            for entry in &module.resources {
                snapshot.insert(entry.kind.realize(cx).resource_id(), entry.kind.manifest());
            }
        }
        for entry in &self.services {
            let svc = self.realized_service(entry, cx, config.as_ref());
            snapshot.insert(
                svc.resource_id(),
                tokeira_compose::canonicalize_manifest(svc.to_manifest()),
            );
        }
        snapshot
    }

    /// The definition's config-files declaration, when any module carries it
    /// (services elsewhere still mount its outputs): the engine id for the
    /// dependency edge, and the digest of the declaration's canonical
    /// manifest for the consumers' content coupling. The digest moves with
    /// any authored parameter — a template change without a parameter change
    /// (an engine upgrade) is the engine-version boundary's business, not a
    /// definition diff.
    fn declared_config_files(&self, cx: &Cx) -> Option<DeclaredConfigFiles> {
        use sha2::{Digest, Sha256};
        let target =
            crate::observability_config::ObservabilityConfigFilesResource::resource_id_value();
        self.modules
            .iter()
            .flat_map(|m| m.resources.iter())
            .find(|r| r.kind.realize(cx).resource_id() == target)
            .map(|r| DeclaredConfigFiles {
                id: target.0.clone(),
                digest: format!(
                    "sha256:{}",
                    hex::encode(Sha256::digest(r.kind.manifest().to_string().as_bytes()))
                ),
            })
    }

    /// Every resource this definition realizes — all modules plus member
    /// services, the same realization the engine composes. The verification
    /// pass runs over this set; pure, like `desired_snapshot`.
    pub fn realized_resources(&self, cx: &Cx) -> Vec<Box<dyn iac::Resource>> {
        let config = self.declared_config_files(cx);
        let mut resources: Vec<Box<dyn iac::Resource>> = self
            .modules
            .iter()
            .flat_map(|m| m.resources.iter())
            .map(|r| r.kind.realize(cx))
            .collect();
        for entry in &self.services {
            resources.push(Box::new(self.realized_service(entry, cx, config.as_ref())));
        }
        resources
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
    deps: Vec<String>,
}

impl ComposeWorkload {
    fn new(service: ComposeService, module: String, needs: Vec<String>) -> Self {
        Self {
            service,
            module,
            deps: needs,
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

    fn dependencies(&self) -> Vec<&str> {
        self.deps.iter().map(String::as_str).collect()
    }

    fn manifests(&self, _ctx: &ServiceContext) -> Result<Vec<serde_json::Value>, RuntimeError> {
        Ok(vec![self.service.to_manifest()])
    }
}
