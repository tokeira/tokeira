//! `.tkd` frontend adapter over the transient structural-definition boundary.
//!
//! Evaluator handles and the name-to-operation table live here. The platform
//! framework receives only a completed graph and a located host-free config
//! value; no interpreter handle or dispatch protocol crosses the crate seam.

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use tokeira_orchestrator::DefinitionFormatId;
use tokeira_platform::{
    author::{LocatedValue, ValueShape, VariantShape},
    binding::{Platform, PlatformBinding},
    catalog::ProviderKind,
    definition::{DefinitionFrontend, FrontendOutput, FrontendSource},
    error::{ContextError, DiagnosticCategory, FrontendDiagnostic, SourceRange},
    graph::{
        DeploymentGraphBuilder, DeploymentHandle, ModuleHandle, OutputReference, ResourceHandle,
        VerifiedGraph, WritebackValue,
    },
};

use crate::{
    HostBridge,
    value::{EvalError, FieldMap, Value, VariantBody},
};

/// `.tkd`-specific access to one platform's typed evaluation context.
///
/// Implementations return host-free values. Platform paths and other ambient
/// objects never become opaque shared tokens; a platform may instead return a
/// normal struct or enum value that its provider kinds decode through Serde.
pub trait TkdContext: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// Complete field names accepted by the `.tkd` subset checker.
    fn fields() -> &'static [&'static str];

    /// Complete method names accepted by the `.tkd` subset checker.
    fn methods() -> &'static [&'static str];

    /// Read one field as a host-free located value.
    fn field(&self, name: &str) -> Result<LocatedValue, ContextError>;

    /// Invoke one pure context method with host-free arguments.
    fn call(&self, method: &str, args: &[LocatedValue]) -> Result<LocatedValue, ContextError>;
}

/// The trusted `.tkd` frontend selected independently of platform identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TkdFrontend {
    format: DefinitionFormatId,
}

impl TkdFrontend {
    /// Construct the canonical first-party `.tkd` frontend.
    pub fn new() -> Self {
        Self {
            format: DefinitionFormatId::new("tkd")
                .expect("the built-in tkd definition-format id is canonical"),
        }
    }
}

impl Default for TkdFrontend {
    fn default() -> Self {
        Self::new()
    }
}

/// Conventional definition-frontend export consumed by generated composition roots.
pub fn frontend() -> TkdFrontend {
    TkdFrontend::new()
}

impl<P> DefinitionFrontend<P> for TkdFrontend
where
    P: Platform,
    P::Context: TkdContext,
{
    fn format(&self) -> &DefinitionFormatId {
        &self.format
    }

    fn evaluate(
        &self,
        source: FrontendSource<'_>,
        context: &P::Context,
        binding: &PlatformBinding<P>,
    ) -> Result<FrontendOutput, FrontendDiagnostic> {
        let source_text =
            std::str::from_utf8(source.bytes).map_err(|error| FrontendDiagnostic {
                format: self.format.clone(),
                source_name: source.source_name.clone(),
                range: None,
                category: DiagnosticCategory::Frontend,
                message: format!("definition source is not UTF-8: {error}"),
            })?;
        let source_map = SourceMap::new(source.bytes);
        let bridge = FrameworkBridge::new(binding, context);
        let (deployment, config) = crate::interpret(source_text, &bridge, context)
            .map_err(|error| diagnostic(&self.format, source, &source_map, error))?;
        let config = value_to_located(config)
            .map_err(|error| diagnostic(&self.format, source, &source_map, error))?;
        let graph = bridge
            .finish_graph(deployment)
            .map_err(|error| diagnostic(&self.format, source, &source_map, error))?;
        Ok(FrontendOutput { config, graph })
    }
}

enum HostValue {
    Deployment(DeploymentHandle),
    Module(ModuleHandle),
    Resource(ResourceHandle),
    Output(OutputReference),
    Kind(Rc<RefCell<Option<Box<dyn ProviderKind>>>>),
    Context,
}

impl Clone for HostValue {
    fn clone(&self) -> Self {
        match self {
            Self::Deployment(value) => Self::Deployment(value.clone()),
            Self::Module(value) => Self::Module(value.clone()),
            Self::Resource(value) => Self::Resource(value.clone()),
            Self::Output(value) => Self::Output(value.clone()),
            Self::Kind(value) => Self::Kind(Rc::clone(value)),
            Self::Context => Self::Context,
        }
    }
}

impl std::fmt::Debug for HostValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Deployment(_) => f.write_str("Deployment"),
            Self::Module(_) => f.write_str("Module"),
            Self::Resource(_) => f.write_str("Resource"),
            Self::Output(_) => f.write_str("Output"),
            Self::Kind(_) => f.write_str("ProviderKind"),
            Self::Context => f.write_str("Context"),
        }
    }
}

struct FrameworkBridge<'a, P: Platform> {
    binding: &'a PlatformBinding<P>,
    context: &'a P::Context,
    graph: RefCell<Option<DeploymentGraphBuilder>>,
    modules: RefCell<BTreeMap<String, ModuleHandle>>,
}

impl<'a, P: Platform> FrameworkBridge<'a, P> {
    fn new(binding: &'a PlatformBinding<P>, context: &'a P::Context) -> Self {
        let graph = DeploymentGraphBuilder::with_catalogs(
            binding.services.identities(),
            binding.providers.delivery_keys(),
        )
        .require_bootstrap(binding.bootstrap_module.clone());
        Self {
            binding,
            context,
            graph: RefCell::new(Some(graph)),
            modules: RefCell::new(BTreeMap::new()),
        }
    }

    fn with_graph<T>(
        &self,
        action: impl FnOnce(
            &mut DeploymentGraphBuilder,
        ) -> Result<T, tokeira_platform::error::GraphError>,
    ) -> Result<T, EvalError> {
        let mut graph = self.graph.borrow_mut();
        let graph = graph
            .as_mut()
            .ok_or_else(|| EvalError::new("the structural graph is already complete"))?;
        action(graph).map_err(|error| EvalError::new(error.to_string()))
    }

    fn finish_graph(&self, deployment: DeploymentHandle) -> Result<VerifiedGraph, EvalError> {
        let graph = self
            .graph
            .borrow_mut()
            .take()
            .ok_or_else(|| EvalError::new("the structural graph is already complete"))?;
        graph
            .finish_for(deployment)
            .map_err(|error| EvalError::new(error.to_string()))
    }

    fn module_dependencies(&self, value: Value<HostValue>) -> Result<Vec<ModuleHandle>, EvalError> {
        let Value::Vec(values) = value else {
            return Err(EvalError::new(
                "module dependencies must be an array of module names or handles",
            ));
        };
        values
            .into_iter()
            .map(|value| match value {
                Value::Host(HostValue::Module(module)) => Ok(module),
                Value::Str(name) => self.modules.borrow().get(&name).cloned().ok_or_else(|| {
                    EvalError::new(format!(
                        "module dependency `{name}` has not been declared yet"
                    ))
                }),
                other => Err(EvalError::new(format!(
                    "module dependencies must be module names or handles, got {other:?}"
                ))),
            })
            .collect()
    }

    fn resource_dependencies(value: Value<HostValue>) -> Result<Vec<ResourceHandle>, EvalError> {
        let Value::Vec(values) = value else {
            return Err(EvalError::new(
                "resource dependencies must be an array of resource handles",
            ));
        };
        values
            .into_iter()
            .map(|value| match value {
                Value::Host(HostValue::Resource(resource)) => Ok(resource),
                other => Err(EvalError::new(format!(
                    "resource dependencies must be resource handles, got {other:?}"
                ))),
            })
            .collect()
    }

    fn add_module(
        &self,
        deployment: &DeploymentHandle,
        args: Vec<Value<HostValue>>,
    ) -> Result<Value<HostValue>, EvalError> {
        let mut args = args.into_iter();
        let name = args
            .next()
            .ok_or_else(|| EvalError::new("Deployment.module expects a module name"))?
            .as_str()?
            .to_string();
        let dependencies = args
            .next()
            .map(|value| self.module_dependencies(value))
            .transpose()?
            .unwrap_or_default();
        if args.next().is_some() {
            return Err(EvalError::new(
                "Deployment.module expects a name and one dependency collection",
            ));
        }
        let module =
            self.with_graph(|graph| graph.add_module(deployment, name.clone(), dependencies))?;
        self.modules.borrow_mut().insert(name, module.clone());
        Ok(Value::Host(HostValue::Module(module)))
    }

    fn add_resource(
        &self,
        module: ModuleHandle,
        args: Vec<Value<HostValue>>,
    ) -> Result<Value<HostValue>, EvalError> {
        let mut args = args.into_iter();
        let logical_id = args
            .next()
            .ok_or_else(|| EvalError::new("resource is missing its logical id"))?
            .as_str()?
            .to_string();
        let kind = match args
            .next()
            .ok_or_else(|| EvalError::new("resource is missing its provider kind"))?
        {
            Value::Host(HostValue::Kind(kind)) => kind,
            other => {
                return Err(EvalError::new(format!(
                    "resource expects a provider kind, got {other:?}"
                )));
            }
        };
        let dependencies = args
            .next()
            .map(Self::resource_dependencies)
            .transpose()?
            .unwrap_or_default();
        if args.next().is_some() {
            return Err(EvalError::new(
                "resource accepts at most one dependency collection",
            ));
        }
        let kind = kind
            .borrow_mut()
            .take()
            .ok_or_else(|| EvalError::new("provider-kind handle was already consumed"))?;
        let resource =
            self.with_graph(|graph| graph.add_resource(&module, logical_id, kind, dependencies))?;
        Ok(Value::Host(HostValue::Resource(resource)))
    }
}

impl<P> HostBridge for FrameworkBridge<'_, P>
where
    P: Platform,
    P::Context: TkdContext,
{
    type Host = HostValue;
    type Cx = P::Context;
    type Output = DeploymentHandle;

    fn is_kind(&self, name: &str) -> bool {
        self.binding.kinds.contains(name)
    }

    fn knows_method(&self, name: &str) -> bool {
        matches!(name, "module" | "resource" | "writeback" | "output")
            || <P::Context as TkdContext>::methods().contains(&name)
    }

    fn knows_assoc(&self, path: &str) -> bool {
        path == "Deployment::new"
    }

    fn kind_defaults(&self, name: &str) -> Option<FieldMap<Self::Host>> {
        match located_to_value(self.binding.kinds.defaults(name)?).ok()? {
            Value::Struct { fields, .. } => Some(fields),
            _ => None,
        }
    }

    fn construct_kind(
        &self,
        name: &str,
        fields: FieldMap<Self::Host>,
        _context: &Self::Cx,
    ) -> Result<Self::Host, EvalError> {
        let input = LocatedValue::new(ValueShape::Struct {
            name: name.to_string(),
            fields: fields
                .into_iter()
                .map(|(name, value)| value_to_located(value).map(|value| (name, value)))
                .collect::<Result<_, _>>()?,
        });
        let kind = self
            .binding
            .kinds
            .decode(name, input)
            .map_err(|error| EvalError::new(error.message))?;
        Ok(HostValue::Kind(Rc::new(RefCell::new(Some(kind)))))
    }

    fn assoc(
        &self,
        path: &str,
        args: Vec<Value<Self::Host>>,
        _context: &Self::Cx,
    ) -> Result<Self::Host, EvalError> {
        if path != "Deployment::new" {
            return Err(EvalError::new(format!(
                "unknown associated function `{path}`"
            )));
        }
        let namespaces = match args.as_slice() {
            [] => Vec::new(),
            [Value::Vec(values)] => values
                .iter()
                .map(|value| value.as_str().map(str::to_string))
                .collect::<Result<Vec<_>, _>>()?,
            _ => {
                return Err(EvalError::new(
                    "Deployment::new expects zero arguments or one namespace array",
                ));
            }
        };
        let deployment = self.with_graph(|graph| Ok(graph.deployment_handle()))?;
        for namespace in namespaces {
            self.with_graph(|graph| graph.add_namespace(&deployment, namespace))?;
        }
        Ok(HostValue::Deployment(deployment))
    }

    fn call_method(
        &self,
        receiver: &Self::Host,
        method: &str,
        mut args: Vec<Value<Self::Host>>,
        _context: &Self::Cx,
    ) -> Result<Value<Self::Host>, EvalError> {
        match (receiver, method) {
            (HostValue::Deployment(deployment), "module") => self.add_module(deployment, args),
            (HostValue::Deployment(_), "resource") => {
                let Some(Value::Host(HostValue::Module(module))) = args.first().cloned() else {
                    return Err(EvalError::new(
                        "Deployment.resource expects a module handle first",
                    ));
                };
                let _ = args.remove(0);
                self.add_resource(module, args)
            }
            (HostValue::Module(module), "resource") => self.add_resource(module.clone(), args),
            (HostValue::Deployment(deployment), "writeback") => {
                if args.len() != 2 {
                    return Err(EvalError::new(
                        "Deployment.writeback expects a dotted key and literal or output",
                    ));
                }
                let key = args.remove(0).as_str()?.to_string();
                let value = match args.remove(0) {
                    Value::Str(value) => WritebackValue::Literal(value),
                    Value::Host(HostValue::Output(output)) => WritebackValue::Output(output),
                    other => {
                        return Err(EvalError::new(format!(
                            "writeback value must be a string or output reference, got {other:?}"
                        )));
                    }
                };
                self.with_graph(|graph| graph.add_writeback(deployment, key, value))?;
                Ok(Value::Unit)
            }
            (HostValue::Resource(resource), "output") => {
                let [value] = args.as_slice() else {
                    return Err(EvalError::new("Resource.output expects one string"));
                };
                let output = resource
                    .output(value.as_str()?)
                    .map_err(|error| EvalError::new(error.to_string()))?;
                Ok(Value::Host(HostValue::Output(output)))
            }
            (HostValue::Context, method) => {
                let args = args
                    .into_iter()
                    .map(value_to_located)
                    .collect::<Result<Vec<_>, _>>()?;
                let value = self
                    .context
                    .call(method, &args)
                    .map_err(|error| EvalError::new(error.to_string()))?;
                located_to_value(value)
            }
            (receiver, method) => Err(EvalError::new(format!(
                "receiver {receiver:?} has no method `{method}`"
            ))),
        }
    }

    fn host_field(&self, host: &Self::Host, field: &str) -> Result<Value<Self::Host>, EvalError> {
        let HostValue::Context = host else {
            return Err(EvalError::new(format!(
                "receiver {host:?} has no field `{field}`"
            )));
        };
        if !<P::Context as TkdContext>::fields().contains(&field) {
            return Err(EvalError::new(format!("unknown context field `{field}`")));
        }
        let value = self
            .context
            .field(field)
            .map_err(|error| EvalError::new(error.to_string()))?;
        located_to_value(value)
    }

    fn cx_host(&self, _context: &Self::Cx) -> Self::Host {
        HostValue::Context
    }

    fn finish(&self, value: Self::Host) -> Result<Self::Output, EvalError> {
        match value {
            HostValue::Deployment(deployment) => Ok(deployment),
            other => Err(EvalError::new(format!(
                "deployment() must return a Deployment, got {other:?}"
            ))),
        }
    }
}

fn value_to_located(value: Value<HostValue>) -> Result<LocatedValue, EvalError> {
    let value = match value {
        Value::Unit => ValueShape::Unit,
        Value::Bool(value) => ValueShape::Bool(value),
        Value::Int(value) => ValueShape::Integer(value),
        Value::Str(value) => ValueShape::String(value),
        Value::Vec(values) | Value::Tuple(values) => ValueShape::Sequence(
            values
                .into_iter()
                .map(value_to_located)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Value::Opt(value) => ValueShape::Option(
            value
                .map(|value| value_to_located(*value).map(Box::new))
                .transpose()?,
        ),
        Value::Struct { ty, fields } => ValueShape::Struct {
            name: ty,
            fields: fields
                .into_iter()
                .map(|(name, value)| value_to_located(value).map(|value| (name, value)))
                .collect::<Result<Vec<_>, _>>()?,
        },
        Value::Enum {
            path,
            variant,
            body,
        } => ValueShape::Enum {
            name: path.ty,
            variant,
            body: match body {
                VariantBody::Unit => VariantShape::Unit,
                VariantBody::Tuple(values) => {
                    VariantShape::Tuple(values.into_iter().map(value_to_located).collect::<Result<
                        Vec<_>,
                        _,
                    >>(
                    )?)
                }
                VariantBody::Struct(fields) => VariantShape::Struct(
                    fields
                        .into_iter()
                        .map(|(name, value)| value_to_located(value).map(|value| (name, value)))
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            },
        },
        Value::Host(handle) => {
            return Err(EvalError::new(format!(
                "frontend handle {handle:?} cannot appear in host-free data"
            )));
        }
    };
    Ok(LocatedValue::new(value))
}

fn located_to_value(value: LocatedValue) -> Result<Value<HostValue>, EvalError> {
    match value.value {
        ValueShape::Unit => Ok(Value::Unit),
        ValueShape::Bool(value) => Ok(Value::Bool(value)),
        ValueShape::Integer(value) => Ok(Value::Int(value)),
        ValueShape::String(value) => Ok(Value::Str(value)),
        ValueShape::Sequence(values) | ValueShape::Tuple(values) => values
            .into_iter()
            .map(located_to_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Vec),
        ValueShape::Option(value) => value
            .map(|value| located_to_value(*value).map(Box::new))
            .transpose()
            .map(Value::Opt),
        ValueShape::Struct { name, fields } => fields
            .into_iter()
            .map(|(field, value)| located_to_value(value).map(|value| (field, value)))
            .collect::<Result<FieldMap<_>, _>>()
            .map(|fields| Value::Struct { ty: name, fields }),
        ValueShape::Enum {
            name,
            variant,
            body,
        } => Ok(Value::Enum {
            path: crate::EnumPath {
                ty: name.clone(),
                segments: vec![name],
            },
            variant,
            body: match body {
                VariantShape::Unit => VariantBody::Unit,
                VariantShape::Tuple(values) => VariantBody::Tuple(
                    values
                        .into_iter()
                        .map(located_to_value)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                VariantShape::Struct(fields) => VariantBody::Struct(
                    fields
                        .into_iter()
                        .map(|(field, value)| located_to_value(value).map(|value| (field, value)))
                        .collect::<Result<FieldMap<_>, _>>()?,
                ),
            },
        }),
        ValueShape::Float(value) => Err(EvalError::new(format!(
            "the tkd runtime cannot represent floating-point value {value}"
        ))),
        ValueShape::Map(_) => Err(EvalError::new(
            "the tkd runtime cannot represent an untyped map",
        )),
    }
}

fn diagnostic(
    format: &DefinitionFormatId,
    source: FrontendSource<'_>,
    source_map: &SourceMap,
    error: EvalError,
) -> FrontendDiagnostic {
    FrontendDiagnostic {
        format: format.clone(),
        source_name: source.source_name.clone(),
        range: error
            .span
            .and_then(|span| source_map.range(span.start(), span.end())),
        category: DiagnosticCategory::Frontend,
        message: error.msg,
    }
}

struct SourceMap {
    line_starts: Vec<usize>,
    source_len: usize,
}

impl SourceMap {
    fn new(source: &[u8]) -> Self {
        let mut line_starts = vec![0];
        for (index, byte) in source.iter().enumerate() {
            if *byte == b'\n' {
                line_starts.push(index + 1);
            }
        }
        Self {
            line_starts,
            source_len: source.len(),
        }
    }

    fn range(
        &self,
        start: proc_macro2::LineColumn,
        end: proc_macro2::LineColumn,
    ) -> Option<SourceRange> {
        let start = self.offset(start)?;
        let end = self.offset(end)?;
        SourceRange::new(start, end).ok()
    }

    fn offset(&self, location: proc_macro2::LineColumn) -> Option<usize> {
        let line = location.line.checked_sub(1)?;
        let start = *self.line_starts.get(line)?;
        start
            .checked_add(location.column)
            .filter(|offset| *offset <= self.source_len)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};
    use tokeira_platform::{
        artifact::ArtifactCatalog,
        binding::{Platform, PlatformBinding, StateBinding, StatePolicy},
        catalog::{
            ImageCatalog, KindRegistration, KindSet, PlacementContext, ProviderKind,
            ProviderKindCatalog, ProviderSet, ServiceCatalog,
        },
        config::{ConfigContract, PlatformConfig},
        context::{ContextArgument, ContextContract, ContextProjection, PlatformContext},
        definition::{
            DefinitionEngine, DefinitionRequest, DefinitionSource, DefinitionSourceName,
            RelativeDefinitionPath,
        },
        error::{ConfigError, ContextError, KindError},
        ops::PlatformOps,
    };

    use super::{TkdContext, TkdFrontend};
    use tokeira_platform::author::LocatedValue;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct TestConfig {
        replicas: u16,
    }

    impl PlatformConfig for TestConfig {
        fn validate(&self) -> Result<(), ConfigError> {
            if self.replicas == 0 {
                return Err(ConfigError::validation(
                    "replicas must be greater than zero",
                ));
            }
            Ok(())
        }
    }

    #[derive(Debug, Clone, Serialize)]
    struct TestContextValue;

    #[derive(Debug, Clone)]
    struct TestContext {
        project: String,
    }

    impl PlatformContext for TestContext {
        type Value = TestContextValue;

        fn fields() -> &'static [&'static str] {
            &["project_name"]
        }

        fn methods() -> &'static [&'static str] {
            &[]
        }

        fn field(&self, name: &str) -> Result<ContextProjection<Self::Value>, ContextError> {
            match name {
                "project_name" => Ok(ContextProjection::Value(LocatedValue::string(
                    &self.project,
                ))),
                _ => Err(ContextError::new(format!("unknown context field `{name}`"))),
            }
        }

        fn call(
            &self,
            method: &str,
            _args: &[ContextArgument<Self::Value>],
        ) -> Result<ContextProjection<Self::Value>, ContextError> {
            Err(ContextError::new(format!(
                "unknown context method `{method}`"
            )))
        }
    }

    impl TkdContext for TestContext {
        fn fields() -> &'static [&'static str] {
            &["project_name"]
        }

        fn methods() -> &'static [&'static str] {
            &[]
        }

        fn field(&self, name: &str) -> Result<LocatedValue, ContextError> {
            match name {
                "project_name" => Ok(LocatedValue::string(&self.project)),
                _ => Err(ContextError::new(format!("unknown context field `{name}`"))),
            }
        }

        fn call(&self, method: &str, _args: &[LocatedValue]) -> Result<LocatedValue, ContextError> {
            Err(ContextError::new(format!(
                "unknown context method `{method}`"
            )))
        }
    }

    #[derive(Debug, Clone)]
    struct TestPlatform;

    impl Platform for TestPlatform {
        type Config = TestConfig;
        type Context = TestContext;

        fn binding(&self) -> PlatformBinding<Self> {
            binding()
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct TestKind {
        suffix: String,
        enabled: bool,
    }

    impl ProviderKind for TestKind {
        fn kind_name(&self) -> &'static str {
            "TestKind"
        }

        fn validate_input(&self) -> Result<(), KindError> {
            if self.suffix.is_empty() {
                return Err(KindError::new("suffix cannot be empty"));
            }
            Ok(())
        }

        fn declared_outputs(&self) -> &'static [&'static str] {
            &["value"]
        }

        fn desired_manifest(&self) -> serde_json::Value {
            serde_json::json!({
                "suffix": self.suffix,
                "enabled": self.enabled,
            })
        }

        fn realize(
            &self,
            _placement: &PlacementContext,
        ) -> Result<Box<dyn tokeira_iac::Resource>, KindError> {
            Err(KindError::new("test realization is intentionally absent"))
        }
    }

    fn kind_defaults() -> serde_json::Map<String, serde_json::Value> {
        serde_json::json!({
            "suffix": "default",
            "enabled": true,
        })
        .as_object()
        .expect("the test kind defaults are an object")
        .clone()
    }

    const KINDS: &[KindRegistration] = &[KindRegistration::typed::<TestKind>(
        "TestKind",
        &["value"],
        Some(kind_defaults),
    )];

    fn binding() -> PlatformBinding<TestPlatform> {
        PlatformBinding::new(
            tokeira_orchestrator::PlatformId::new("test").expect("canonical test platform id"),
            "state",
            ConfigContract::new(),
            ContextContract::new(
                |input| {
                    Ok(TestContext {
                        project: input.deployment_id.clone(),
                    })
                },
                || {
                    Ok(TestContext {
                        project: "authoring".to_string(),
                    })
                },
            ),
            KindSet::new(vec![ProviderKindCatalog {
                provider: "test",
                entries: KINDS,
            }])
            .expect("the test kind catalog is valid"),
            ServiceCatalog::default(),
            ArtifactCatalog::default(),
            ImageCatalog::default(),
            ProviderSet::default(),
            StateBinding::new(StatePolicy::LocalCas),
            PlatformOps::default(),
            Vec::new(),
        )
        .expect("the test platform binding is valid")
    }

    fn source(bytes: &str) -> DefinitionSource {
        DefinitionSource {
            format: tokeira_orchestrator::DefinitionFormatId::new("tkd")
                .expect("canonical tkd format id"),
            source_name: DefinitionSourceName::DeploymentRelative(
                RelativeDefinitionPath::new("definition.tkd").expect("canonical definition path"),
            ),
            bytes: Arc::from(bytes.as_bytes()),
        }
    }

    #[test]
    fn tkd_frontend_returns_one_completed_structural_definition() {
        let engine = DefinitionEngine::new(binding(), TkdFrontend::new());
        let definition = engine
            .evaluate(DefinitionRequest {
                source: source(
                    r#"
struct TestConfig { replicas: u16 }

fn config() -> TestConfig { TestConfig { replicas: 2 } }

fn deployment(cfg: &TestConfig, cx: &Cx) -> Deployment {
    let mut d = Deployment::new(&["default"]);
    let state = d.module("state", &[]);
    let runtime = d.module("runtime", &["state"]);
    let resource = d.resource(
        &runtime,
        "item",
        TestKind { suffix: cx.project_name.clone(), ..TestKind::EMPTY },
    );
    d.writeback("resource.value", resource.output("value"));
    d
}
"#,
                ),
                context: TestContext {
                    project: "sample".to_string(),
                },
            })
            .expect("the generic tkd definition is admitted");

        assert_eq!(definition.config, TestConfig { replicas: 2 });
        assert_eq!(definition.graph.namespaces(), &["default"]);
        assert_eq!(
            definition
                .graph
                .modules()
                .iter()
                .map(|module| module.name())
                .collect::<Vec<_>>(),
            vec!["state", "runtime"]
        );
        assert_eq!(definition.graph.resources().len(), 1);
        assert_eq!(definition.graph.writeback().len(), 1);
    }

    #[test]
    fn tkd_frontend_returns_a_source_range_for_evaluator_failures() {
        let engine = DefinitionEngine::new(binding(), TkdFrontend::new());
        let error = engine
            .evaluate(DefinitionRequest {
                source: source(
                    r#"
struct TestConfig { replicas: u16 }
fn config() -> TestConfig { TestConfig { replicas: 1 } }
fn deployment(cfg: &TestConfig, cx: &Cx) -> Deployment {
    Deployment::new(&[]).missing()
}
"#,
                ),
                context: TestContext {
                    project: "sample".to_string(),
                },
            })
            .expect_err("an unknown evaluator method must be rejected");

        let tokeira_platform::error::DefinitionError::Frontend(diagnostic) = error else {
            panic!("expected a frontend diagnostic");
        };
        assert!(diagnostic.range.is_some());
        assert!(diagnostic.message.contains("missing"));
    }
}
