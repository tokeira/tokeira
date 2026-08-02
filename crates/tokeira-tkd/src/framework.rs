//! Adapter from the `.tkd` interpreter runtime to the language-neutral Authoring Contract.

use std::{cell::RefCell, collections::BTreeMap};

use tokeira_orchestrator::DefinitionFormatId;
use tokeira_platform::{
    author::{
        AuthorArgument, AuthorHandle, AuthorNode, AuthorResult, AuthorSession, AuthorValue,
        AuthorVariantBody,
    },
    binding::Platform,
    definition::{DefinitionFrontend, FrontendOutput, FrontendSource},
    error::{DiagnosticCategory, FrontendDiagnostic, SourceRange},
    graph::DeploymentHandle,
};

use crate::{
    HostBridge,
    value::{EvalError, FieldMap, Value, VariantBody},
};

/// The trusted `.tkd` frontend selected independently of any platform.
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

impl<P: Platform> DefinitionFrontend<P> for TkdFrontend {
    fn format(&self) -> &DefinitionFormatId {
        &self.format
    }

    fn evaluate(
        &self,
        source: FrontendSource<'_>,
        author: &mut AuthorSession<P>,
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
        let bridge = FrameworkBridge::new(author);
        let (deployment, config) =
            crate::interpret(source_text, &bridge, &()).map_err(|error| FrontendDiagnostic {
                format: self.format.clone(),
                source_name: source.source_name.clone(),
                range: error
                    .span
                    .and_then(|span| source_map.range(span.start(), span.end())),
                category: DiagnosticCategory::Frontend,
                message: error.msg,
            })?;
        let config = value_to_author_node(config).map_err(|error| FrontendDiagnostic {
            format: self.format.clone(),
            source_name: source.source_name.clone(),
            range: error
                .span
                .and_then(|span| source_map.range(span.start(), span.end())),
            category: DiagnosticCategory::Config,
            message: error.msg,
        })?;
        Ok(FrontendOutput { config, deployment })
    }
}

struct FrameworkBridge<'a, P: Platform> {
    author: RefCell<&'a mut AuthorSession<P>>,
    schema: tokeira_platform::author::AuthorSchema,
    modules: RefCell<BTreeMap<String, AuthorHandle>>,
}

impl<'a, P: Platform> FrameworkBridge<'a, P> {
    fn new(author: &'a mut AuthorSession<P>) -> Self {
        let schema = author.schema();
        Self {
            author: RefCell::new(author),
            schema,
            modules: RefCell::new(BTreeMap::new()),
        }
    }

    fn invoke(
        &self,
        receiver: AuthorHandle,
        method: &str,
        args: Vec<Value<AuthorHandle>>,
    ) -> Result<Value<AuthorHandle>, EvalError> {
        let arguments = args
            .into_iter()
            .map(value_to_author_argument)
            .collect::<Result<Vec<_>, _>>()?;
        self.author
            .borrow_mut()
            .call(receiver, method, arguments)
            .map_err(author_error)
            .and_then(author_result_to_value)
    }

    fn invoke_module(
        &self,
        receiver: AuthorHandle,
        args: Vec<Value<AuthorHandle>>,
    ) -> Result<Value<AuthorHandle>, EvalError> {
        let mut args = args.into_iter();
        let name = args
            .next()
            .ok_or_else(|| EvalError::new("Deployment.module expects a module name"))?;
        let name_text = name.as_str()?.to_string();
        let mut arguments = vec![AuthorArgument::Value(value_to_author_node(name)?)];
        if let Some(dependencies) = args.next() {
            arguments.extend(self.module_dependencies(dependencies)?);
        }
        if args.next().is_some() {
            return Err(EvalError::new(
                "Deployment.module expects a name and one dependency collection",
            ));
        }
        let result = self
            .author
            .borrow_mut()
            .call(receiver, "module", arguments)
            .map_err(author_error)?;
        let AuthorResult::Handle(handle @ AuthorHandle::Module(_)) = result else {
            return Err(EvalError::new(
                "Deployment.module returned a non-module author value",
            ));
        };
        self.modules.borrow_mut().insert(name_text, handle.clone());
        Ok(Value::Host(handle))
    }

    fn module_dependencies(
        &self,
        value: Value<AuthorHandle>,
    ) -> Result<Vec<AuthorArgument>, EvalError> {
        let Value::Vec(values) = value else {
            return Err(EvalError::new(
                "module dependencies must be an array of module names or handles",
            ));
        };
        values
            .into_iter()
            .map(|value| match value {
                Value::Host(handle @ AuthorHandle::Module(_)) => Ok(AuthorArgument::Handle(handle)),
                Value::Str(name) => self
                    .modules
                    .borrow()
                    .get(&name)
                    .cloned()
                    .map(AuthorArgument::Handle)
                    .ok_or_else(|| {
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

    fn invoke_resource(
        &self,
        args: Vec<Value<AuthorHandle>>,
    ) -> Result<Value<AuthorHandle>, EvalError> {
        let mut args = args.into_iter();
        let module = match args.next() {
            Some(Value::Host(handle @ AuthorHandle::Module(_))) => handle,
            Some(other) => {
                return Err(EvalError::new(format!(
                    "Deployment.resource expects a module handle, got {other:?}"
                )));
            }
            None => {
                return Err(EvalError::new(
                    "Deployment.resource expects a module handle, logical id, and kind",
                ));
            }
        };
        let logical_id = args
            .next()
            .ok_or_else(|| EvalError::new("Deployment.resource is missing its logical id"))?;
        let kind = args
            .next()
            .ok_or_else(|| EvalError::new("Deployment.resource is missing its provider kind"))?;
        let mut arguments = vec![
            value_to_author_argument(logical_id)?,
            value_to_author_argument(kind)?,
        ];
        if let Some(dependencies) = args.next() {
            arguments.extend(resource_dependencies(dependencies)?);
        }
        if args.next().is_some() {
            return Err(EvalError::new(
                "Deployment.resource accepts at most one dependency collection",
            ));
        }
        self.author
            .borrow_mut()
            .call(module, "resource", arguments)
            .map_err(author_error)
            .and_then(author_result_to_value)
    }

    fn invoke_module_resource(
        &self,
        receiver: AuthorHandle,
        args: Vec<Value<AuthorHandle>>,
    ) -> Result<Value<AuthorHandle>, EvalError> {
        let mut args = args.into_iter();
        let logical_id = args
            .next()
            .ok_or_else(|| EvalError::new("Module.resource is missing its logical id"))?;
        let kind = args
            .next()
            .ok_or_else(|| EvalError::new("Module.resource is missing its provider kind"))?;
        let mut arguments = vec![
            value_to_author_argument(logical_id)?,
            value_to_author_argument(kind)?,
        ];
        if let Some(dependencies) = args.next() {
            arguments.extend(resource_dependencies(dependencies)?);
        }
        if args.next().is_some() {
            return Err(EvalError::new(
                "Module.resource accepts at most one dependency collection",
            ));
        }
        self.author
            .borrow_mut()
            .call(receiver, "resource", arguments)
            .map_err(author_error)
            .and_then(author_result_to_value)
    }
}

impl<P: Platform> HostBridge for FrameworkBridge<'_, P> {
    type Host = AuthorHandle;
    type Cx = ();
    type Output = DeploymentHandle;

    fn is_kind(&self, name: &str) -> bool {
        self.schema.kinds.iter().any(|kind| kind.name == name)
    }

    fn knows_method(&self, name: &str) -> bool {
        name == "resource"
            || self
                .schema
                .receivers
                .iter()
                .any(|receiver| receiver.methods.iter().any(|method| method == name))
            || self
                .schema
                .context_methods
                .iter()
                .any(|method| method == name)
    }

    fn knows_assoc(&self, path: &str) -> bool {
        let name = path.replace("::", ".");
        self.schema
            .associated_functions
            .iter()
            .any(|function| function.name == name)
    }

    fn kind_defaults(&self, name: &str) -> Option<FieldMap<Self::Host>> {
        let defaults = self
            .schema
            .kinds
            .iter()
            .find(|kind| kind.name == name)?
            .defaults
            .clone()?;
        match author_node_to_value(defaults).ok()? {
            Value::Struct { fields, .. } => Some(fields),
            _ => None,
        }
    }

    fn construct_kind(
        &self,
        name: &str,
        fields: FieldMap<Self::Host>,
        _cx: &Self::Cx,
    ) -> Result<Self::Host, EvalError> {
        let input = AuthorNode::new(AuthorValue::Struct {
            name: name.to_string(),
            fields: fields
                .into_iter()
                .map(|(name, value)| value_to_author_node(value).map(|value| (name, value)))
                .collect::<Result<Vec<_>, _>>()?,
        });
        self.author
            .borrow_mut()
            .construct_kind(name, input)
            .map(AuthorHandle::Kind)
            .map_err(author_error)
    }

    fn assoc(
        &self,
        path: &str,
        args: Vec<Value<Self::Host>>,
        _cx: &Self::Cx,
    ) -> Result<Self::Host, EvalError> {
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
        let name = path.replace("::", ".");
        let result = self
            .author
            .borrow_mut()
            .associated(&name, Vec::new())
            .map_err(author_error)?;
        let AuthorResult::Handle(handle @ AuthorHandle::Deployment(_)) = result else {
            return Err(EvalError::new(
                "Deployment::new returned a non-deployment author value",
            ));
        };
        for namespace in namespaces {
            self.author
                .borrow_mut()
                .call(
                    handle.clone(),
                    "namespace",
                    vec![AuthorArgument::Value(AuthorNode::string(namespace))],
                )
                .map_err(author_error)?;
        }
        Ok(handle)
    }

    fn call_method(
        &self,
        recv: &Self::Host,
        method: &str,
        args: Vec<Value<Self::Host>>,
        _cx: &Self::Cx,
    ) -> Result<Value<Self::Host>, EvalError> {
        match (recv, method) {
            (AuthorHandle::Deployment(_), "module") => self.invoke_module(recv.clone(), args),
            (AuthorHandle::Deployment(_), "resource") => self.invoke_resource(args),
            (AuthorHandle::Module(_), "resource") => {
                self.invoke_module_resource(recv.clone(), args)
            }
            _ => self.invoke(recv.clone(), method, args),
        }
    }

    fn host_field(&self, host: &Self::Host, field: &str) -> Result<Value<Self::Host>, EvalError> {
        self.author
            .borrow_mut()
            .field(host.clone(), field)
            .map_err(author_error)
            .and_then(author_result_to_value)
    }

    fn cx_host(&self, _cx: &Self::Cx) -> Self::Host {
        AuthorHandle::Context(self.author.borrow().context_handle())
    }

    fn finish(&self, ret: Self::Host) -> Result<Self::Output, EvalError> {
        match ret {
            AuthorHandle::Deployment(deployment) => Ok(deployment),
            other => Err(EvalError::new(format!(
                "deployment() must return a Deployment, got {other:?}"
            ))),
        }
    }
}

fn resource_dependencies(value: Value<AuthorHandle>) -> Result<Vec<AuthorArgument>, EvalError> {
    let Value::Vec(values) = value else {
        return Err(EvalError::new(
            "resource dependencies must be an array of resource handles",
        ));
    };
    values
        .into_iter()
        .map(|value| match value {
            Value::Host(handle @ AuthorHandle::Resource(_)) => Ok(AuthorArgument::Handle(handle)),
            other => Err(EvalError::new(format!(
                "resource dependencies must be resource handles, got {other:?}"
            ))),
        })
        .collect()
}

fn value_to_author_argument(value: Value<AuthorHandle>) -> Result<AuthorArgument, EvalError> {
    match value {
        Value::Host(handle) => Ok(AuthorArgument::Handle(handle)),
        value => value_to_author_node(value).map(AuthorArgument::Value),
    }
}

fn value_to_author_node(value: Value<AuthorHandle>) -> Result<AuthorNode, EvalError> {
    let value = match value {
        Value::Unit => AuthorValue::Unit,
        Value::Bool(value) => AuthorValue::Bool(value),
        Value::Int(value) => AuthorValue::Integer(value),
        Value::Str(value) => AuthorValue::String(value),
        Value::Vec(values) => AuthorValue::Sequence(
            values
                .into_iter()
                .map(value_to_author_node)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Value::Tuple(values) => AuthorValue::Tuple(
            values
                .into_iter()
                .map(value_to_author_node)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Value::Opt(value) => AuthorValue::Option(
            value
                .map(|value| value_to_author_node(*value).map(Box::new))
                .transpose()?,
        ),
        Value::Struct { ty, fields } => AuthorValue::Struct {
            name: ty,
            fields: fields
                .into_iter()
                .map(|(name, value)| value_to_author_node(value).map(|value| (name, value)))
                .collect::<Result<Vec<_>, _>>()?,
        },
        Value::Enum {
            path,
            variant,
            body,
        } => AuthorValue::Enum {
            name: path.ty,
            variant,
            body: match body {
                VariantBody::Unit => AuthorVariantBody::Unit,
                VariantBody::Tuple(values) => AuthorVariantBody::Tuple(
                    values
                        .into_iter()
                        .map(value_to_author_node)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                VariantBody::Struct(fields) => AuthorVariantBody::Struct(
                    fields
                        .into_iter()
                        .map(|(name, value)| value_to_author_node(value).map(|value| (name, value)))
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            },
        },
        Value::Host(AuthorHandle::ContextValue(token)) => AuthorValue::ContextToken(token),
        Value::Host(handle) => {
            return Err(EvalError::new(format!(
                "author handle {handle:?} cannot appear in host-free data"
            )));
        }
    };
    Ok(AuthorNode::new(value))
}

fn author_node_to_value(node: AuthorNode) -> Result<Value<AuthorHandle>, EvalError> {
    match node.value {
        AuthorValue::Unit => Ok(Value::Unit),
        AuthorValue::Bool(value) => Ok(Value::Bool(value)),
        AuthorValue::Integer(value) => Ok(Value::Int(value)),
        AuthorValue::String(value) => Ok(Value::Str(value)),
        AuthorValue::Sequence(values) => values
            .into_iter()
            .map(author_node_to_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Vec),
        AuthorValue::Tuple(values) => values
            .into_iter()
            .map(author_node_to_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Tuple),
        AuthorValue::Option(value) => value
            .map(|value| author_node_to_value(*value).map(Box::new))
            .transpose()
            .map(Value::Opt),
        AuthorValue::Struct { name, fields } => fields
            .into_iter()
            .map(|(field, value)| author_node_to_value(value).map(|value| (field, value)))
            .collect::<Result<FieldMap<_>, _>>()
            .map(|fields| Value::Struct { ty: name, fields }),
        AuthorValue::Enum {
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
                AuthorVariantBody::Unit => VariantBody::Unit,
                AuthorVariantBody::Tuple(values) => VariantBody::Tuple(
                    values
                        .into_iter()
                        .map(author_node_to_value)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                AuthorVariantBody::Struct(fields) => VariantBody::Struct(
                    fields
                        .into_iter()
                        .map(|(field, value)| {
                            author_node_to_value(value).map(|value| (field, value))
                        })
                        .collect::<Result<FieldMap<_>, _>>()?,
                ),
            },
        }),
        AuthorValue::ContextToken(token) => Ok(Value::Host(AuthorHandle::ContextValue(token))),
        AuthorValue::Float(value) => Err(EvalError::new(format!(
            "the tkd runtime cannot represent floating-point value {value}"
        ))),
        AuthorValue::Map(_) => Err(EvalError::new(
            "the tkd runtime cannot represent an untyped map",
        )),
    }
}

fn author_result_to_value(result: AuthorResult) -> Result<Value<AuthorHandle>, EvalError> {
    match result {
        AuthorResult::Handle(handle) => Ok(Value::Host(handle)),
        AuthorResult::Value(value) => author_node_to_value(value),
    }
}

fn author_error(error: tokeira_platform::error::AuthorError) -> EvalError {
    EvalError::new(error.message)
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
    use tokeira_orchestrator::DefinitionFormatId;
    use tokeira_platform::{
        artifact::ArtifactCatalog,
        author::AuthorNode,
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

    use super::TkdFrontend;

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
                "project_name" => Ok(ContextProjection::Value(AuthorNode::string(&self.project))),
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

        fn validate(&self) -> Result<(), KindError> {
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
            format: DefinitionFormatId::new("tkd").expect("canonical tkd format id"),
            source_name: DefinitionSourceName::DeploymentRelative(
                RelativeDefinitionPath::new("definition.tkd").expect("canonical definition path"),
            ),
            bytes: Arc::from(bytes.as_bytes()),
        }
    }

    #[test]
    fn tkd_frontend_drives_only_the_language_neutral_author_session() {
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
    fn tkd_frontend_returns_a_source_range_for_authoring_failures() {
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
            .expect_err("an unknown author method must be rejected");

        let tokeira_platform::error::DefinitionError::Frontend(diagnostic) = error else {
            panic!("expected a frontend diagnostic");
        };
        assert!(diagnostic.range.is_some());
        assert!(diagnostic.message.contains("missing"));
    }
}
