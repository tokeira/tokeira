//! The author's builder vocabulary — the only verbs and types the operator's
//! `definition.tkd` may name to describe an EKS deployment. It records the
//! deployment shape (namespaces, modules, resources, writeback) and realizes
//! each declared resource directly to an engine [`iac::Resource`].
//!
//! This mirrors `platforms/compose::builder`, with two deliberate omissions:
//! there is **no `service()`/`ComposeWorkload`/`realize_workloads`** and **no
//! `Vol`** vocabulary. EKS drives every Kubernetes object (Deployments included)
//! through the single `InfraEngine` apply path as an `iac::Resource` kind (design
//! → "single InfraEngine path"), so there is no deploy-engine workload to build
//! and no host-path bind-mount to model — a tokeira service Deployment is a
//! `Box<dyn Kind>` like every other K8s object.
//!
//! No `Composition` IR, no `KindLibrary`, no `Realizer` trait: a kind *is* a
//! typed struct that knows how to build its engine resource.

use tokeira_iac as iac;

use crate::context::Cx;

/// An author-defined kind: a typed struct that realizes to one concrete engine
/// resource. The operator names the struct in the `.tkd`; `realize` maps it to a
/// `tokeira_aws` / `tokeira_k8s` `iac::Resource` at plan/apply time.
///
/// Object-safe by construction (no generics, no `Self` return) so the
/// interpreter bridge can hold and place kinds as `Box<dyn Kind>` — it cannot
/// name the concrete type it just constructed.
pub trait Kind {
    /// Build this kind's engine resource. `cx` supplies realize-time identity
    /// (project/region/account) and the sanctioned non-hermetic edge; the `.tkd`
    /// itself stays hermetic (Proposal 003 §4).
    fn realize(&self, cx: &Cx) -> Box<dyn iac::Resource>;
}

/// A handle to a declared module (carries only its name), returned by
/// [`Deployment::module`] and passed back to [`Deployment::resource`].
#[derive(Clone, Debug)]
pub struct ModuleRef {
    name: String,
}

/// A handle to a declared resource, used to reference its outputs for writeback.
#[derive(Clone, Debug)]
pub struct ResourceRef {
    module: String,
    resource: String,
}

impl ResourceRef {
    /// A deferred reference to one of this resource's outputs, resolved from
    /// `InfraState` after apply. The handle *is* the binding to the resource id,
    /// so writeback never names a magic `"module.resource.output"` string
    /// (Proposal 003 §5).
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
    /// Owning module of the referenced resource.
    pub module: String,
    /// Logical id of the referenced resource within its module.
    pub resource: String,
    /// The named output/property to read from the resource's post-apply state.
    pub output: String,
}

/// A writeback value: a literal, or a deferred resource output resolved against
/// `InfraState` after apply. The adapter's `collect_writeback` turns these into
/// the `(dotted-key, value)` pairs projected into the server `tokeirad.toml`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WbValue {
    /// A literal string value (e.g. `infrastructure.storage = "dsql"`).
    Const(String),
    /// A deferred resource output (e.g. the DSQL endpoint), resolved post-apply.
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

/// A declared module: a resource grouping plus its module-level dependencies.
struct Module {
    name: String,
    needs: Vec<String>,
    resources: Vec<ResourceEntry>,
}

/// One declared resource within a module: its logical id and the kind that
/// realizes it.
struct ResourceEntry {
    id: String,
    kind: Box<dyn Kind>,
}

/// The deployment under construction, returned by the operator's
/// `deployment(cfg, cx)` and consumed by the orchestrator adapter.
///
/// It records structure only; realization to engine `iac::Resource`s happens on
/// demand via [`realize_module`](Self::realize_module), so the same builder feeds
/// both planning (desired shape) and apply.
pub struct Deployment {
    namespaces: Vec<String>,
    modules: Vec<Module>,
    writeback: Vec<(String, WbValue)>,
}

// Manual impl: modules hold `Box<dyn Kind>` resources with no `Debug`.
impl std::fmt::Debug for Deployment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Deployment")
            .field("namespaces", &self.namespaces)
            .field("modules", &self.modules.len())
            .finish_non_exhaustive()
    }
}

impl Deployment {
    /// Start a deployment with the given required Kubernetes namespaces.
    pub fn new(namespaces: &[&str]) -> Self {
        Self {
            namespaces: namespaces.iter().map(|s| s.to_string()).collect(),
            modules: Vec::new(),
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

    /// Record a writeback into the server config: a dotted key sourced from a
    /// literal or a resource output (the `collect_writeback` source).
    pub fn writeback(&mut self, key: &str, value: impl Into<WbValue>) {
        self.writeback.push((key.to_string(), value.into()));
    }

    /// The recorded writeback entries, in declaration order.
    pub fn writeback_entries(&self) -> &[(String, WbValue)] {
        &self.writeback
    }

    // ── inspection / realization (the adapter + fidelity tests) ──

    /// The required Kubernetes namespaces (the adapter's `required_namespaces`).
    pub fn namespaces(&self) -> &[String] {
        &self.namespaces
    }

    /// The declared module names, in declaration order.
    pub fn module_names(&self) -> Vec<&str> {
        self.modules.iter().map(|m| m.name.as_str()).collect()
    }

    /// A module's declared dependencies (names), or `None` if no such module.
    pub fn module_deps(&self, name: &str) -> Option<&[String]> {
        self.modules
            .iter()
            .find(|m| m.name == name)
            .map(|m| m.needs.as_slice())
    }

    /// The logical ids of a module's declared resources, in declaration order.
    ///
    /// Unlike the compose builder there are no member services to append — every
    /// EKS resource (K8s objects included) is a declared resource.
    pub fn resource_ids(&self, module: &str) -> Vec<String> {
        self.modules
            .iter()
            .find(|m| m.name == module)
            .map(|m| m.resources.iter().map(|r| r.id.clone()).collect())
            .unwrap_or_default()
    }

    /// Realize a module to its engine `iac::Resource`s, or `None` if no such
    /// module. Each declared resource's kind is realized in declaration order.
    pub fn realize_module(&self, name: &str, cx: &Cx) -> Option<Vec<Box<dyn iac::Resource>>> {
        let module = self.modules.iter().find(|m| m.name == name)?;
        Some(
            module
                .resources
                .iter()
                .map(|r| r.kind.realize(cx))
                .collect(),
        )
    }

    /// The physical engine [`iac::ResourceId`] of a declared resource, obtained
    /// by realizing its kind. The adapter uses this to resolve writeback
    /// [`Output`] handles (logical `module.resource`) against `InfraState`.
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
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tokeira_iac as iac;

    use super::*;

    fn cx() -> Cx {
        Cx {
            project_name: "tokeira".into(),
            region: Some("eu-west-2".into()),
            account_id: Some("123456789012".into()),
            deployment_dir: PathBuf::from("/tmp/deploy"),
        }
    }

    /// A minimal `iac::Resource` used only to exercise the builder's recording
    /// and realization; its lifecycle methods are never driven by these tests.
    #[derive(Debug)]
    struct StubResource {
        id: String,
        module: String,
    }

    impl StubResource {
        fn state(&self) -> iac::ResourceState {
            iac::ResourceState {
                resource_type: iac::ResourceType::new("stub"),
                physical_id: self.id.clone(),
                properties: serde_json::json!({}),
                dependencies: Vec::new(),
                created_at: String::new(),
                updated_at: String::new(),
                module: self.module.clone(),
            }
        }
    }

    #[async_trait::async_trait]
    impl iac::Resource for StubResource {
        fn change_semantics(&self, _ctx: &iac::SemanticsContext<'_>) -> iac::ChangeSemantics {
            iac::ChangeSemantics::default()
        }
        fn resource_type(&self) -> iac::ResourceType {
            iac::ResourceType::new("stub")
        }
        fn resource_id(&self) -> iac::ResourceId {
            iac::ResourceId(self.id.clone())
        }
        fn dependencies(&self) -> Vec<iac::ResourceId> {
            Vec::new()
        }
        fn module(&self) -> &str {
            &self.module
        }
        async fn create(
            &self,
            _ctx: &iac::ProvisionContext,
        ) -> Result<iac::ResourceState, iac::IacError> {
            Ok(self.state())
        }
        async fn update(
            &self,
            _current: &iac::ResourceState,
            _ctx: &iac::ProvisionContext,
        ) -> Result<iac::ResourceState, iac::IacError> {
            Ok(self.state())
        }
        async fn delete(
            &self,
            _current: &iac::ResourceState,
            _ctx: &iac::ProvisionContext,
        ) -> Result<(), iac::IacError> {
            Ok(())
        }
        async fn describe(
            &self,
            _ctx: &iac::ProvisionContext,
        ) -> Result<iac::DescribeResult, iac::IacError> {
            Ok(iac::DescribeResult::Absent)
        }
        fn diff(
            &self,
            _current: &iac::ResourceState,
            _ctx: &iac::ProvisionContext,
        ) -> iac::InternalChange {
            iac::InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        }
    }

    /// A test kind that realizes to a [`StubResource`] carrying a caller-chosen
    /// resource id (so `realize_resource_id` is observable).
    struct StubKind {
        physical_id: String,
        module: String,
    }

    impl Kind for StubKind {
        fn realize(&self, _cx: &Cx) -> Box<dyn iac::Resource> {
            Box::new(StubResource {
                id: self.physical_id.clone(),
                module: self.module.clone(),
            })
        }
    }

    #[test]
    fn records_modules_resources_and_deps_in_order() {
        let mut d = Deployment::new(&["tokeira-system"]);
        let foundation = d.module("foundation", &["remote_state"]);
        d.resource(
            &foundation,
            "vpc",
            StubKind {
                physical_id: "vpc-1".into(),
                module: "foundation".into(),
            },
        );
        d.resource(
            &foundation,
            "dsql",
            StubKind {
                physical_id: "dsql-1".into(),
                module: "foundation".into(),
            },
        );

        assert_eq!(d.namespaces(), ["tokeira-system"]);
        assert_eq!(d.module_names(), ["foundation"]);
        assert_eq!(
            d.module_deps("foundation"),
            Some(&["remote_state".to_string()][..])
        );
        assert_eq!(d.resource_ids("foundation"), ["vpc", "dsql"]);
        assert_eq!(d.module_deps("missing"), None);
    }

    #[test]
    fn realizes_module_and_resource_ids() {
        let mut d = Deployment::new(&["tokeira-system"]);
        let cluster = d.module("cluster", &["foundation"]);
        d.resource(
            &cluster,
            "namespace",
            StubKind {
                physical_id: "namespace/tokeira-system".into(),
                module: "cluster".into(),
            },
        );

        let resources = d.realize_module("cluster", &cx()).expect("cluster module");
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].resource_id().0, "namespace/tokeira-system");
        assert_eq!(resources[0].module(), "cluster");

        // The physical id is obtained by realizing the kind — the seam the
        // adapter uses to resolve writeback Output handles.
        assert_eq!(
            d.realize_resource_id("cluster", "namespace", &cx()),
            Some(iac::ResourceId("namespace/tokeira-system".into()))
        );
        assert_eq!(d.realize_resource_id("cluster", "absent", &cx()), None);
        assert!(d.realize_module("absent", &cx()).is_none());
    }

    #[test]
    fn output_handle_binds_to_its_resource() {
        let mut d = Deployment::new(&["tokeira-system"]);
        let foundation = d.module("foundation", &[]);
        let dsql = d.resource(
            &foundation,
            "cluster",
            StubKind {
                physical_id: "dsql-1".into(),
                module: "foundation".into(),
            },
        );
        let endpoint = dsql.output("private_hostname");
        assert_eq!(endpoint.module, "foundation");
        assert_eq!(endpoint.resource, "cluster");
        assert_eq!(endpoint.output, "private_hostname");
    }

    #[test]
    fn writeback_records_const_and_output_values() {
        let mut d = Deployment::new(&["tokeira-system"]);
        let foundation = d.module("foundation", &[]);
        let dsql = d.resource(
            &foundation,
            "cluster",
            StubKind {
                physical_id: "dsql-1".into(),
                module: "foundation".into(),
            },
        );
        d.writeback("infrastructure.storage", "dsql");
        d.writeback(
            "infrastructure.dsql.endpoint",
            dsql.output("private_hostname"),
        );

        let keys: Vec<&str> = d
            .writeback_entries()
            .iter()
            .map(|(k, _)| k.as_str())
            .collect();
        assert_eq!(
            keys,
            ["infrastructure.storage", "infrastructure.dsql.endpoint"]
        );
        assert!(matches!(&d.writeback_entries()[0].1, WbValue::Const(s) if s == "dsql"));
        assert!(matches!(&d.writeback_entries()[1].1, WbValue::Output(_)));
    }
}
