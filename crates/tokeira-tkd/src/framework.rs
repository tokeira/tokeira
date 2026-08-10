//! `.tkd` adapter for the transient structural-definition boundary.
//!
//! Evaluator handles and the name-to-operation table live only in this module.
//! The platform boundary receives ordinary typed context and one completed
//! graph; no interpreter object or dispatch protocol crosses that boundary.

use std::{cell::RefCell, fmt, rc::Rc};

use serde::Serialize;
use tokeira_orchestrator::DefinitionFormatId;
use tokeira_platform::{
    author::{LocatedValue, ValueShape, VariantShape},
    definition::{DefinitionFrontend, FrontendOutput, FrontendSource, Namespace},
    error::{DiagnosticCategory, FrontendDiagnostic, SourceRange},
    graph::{
        OutputReference, ResourceReference, StructuralGraphBuilder, VerifiedGraph, WritebackValue,
    },
    kind::DecodedKind,
};

use crate::{
    HostBridge,
    value::{EvalError, FieldMap, Value, VariantBody},
};

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

impl DefinitionFrontend for TkdFrontend {
    fn format(&self) -> &DefinitionFormatId {
        &self.format
    }

    fn evaluate<C>(
        &self,
        source: FrontendSource<'_>,
        context: &C,
        namespaces: &[Namespace],
        parts: &dyn tokeira_platform::definition::SourceResolver,
    ) -> Result<FrontendOutput, FrontendDiagnostic>
    where
        C: Serialize,
    {
        let source_text =
            std::str::from_utf8(source.bytes).map_err(|error| FrontendDiagnostic {
                format: self.format.clone(),
                source_name: source.source_name.clone(),
                range: None,
                category: DiagnosticCategory::Frontend,
                message: format!("definition source is not UTF-8: {error}"),
            })?;
        let source_map = SourceMap::new(source.bytes);
        let context = context_fields(context)
            .map_err(|error| diagnostic(&self.format, source, &source_map, error))?;
        let bridge = FrameworkBridge::new(namespaces, context);
        let (graph, config) = crate::interpret(source_text, &bridge, &(), parts)
            .map_err(|error| diagnostic(&self.format, source, &source_map, error))?;
        let config = value_to_located(config)
            .map_err(|error| diagnostic(&self.format, source, &source_map, error))?;
        Ok(FrontendOutput { config, graph })
    }

    fn retarget_check<C>(
        &self,
        prior: FrontendSource<'_>,
        current: FrontendSource<'_>,
        context: &C,
        namespaces: &[Namespace],
        prior_parts: &dyn tokeira_platform::definition::SourceResolver,
        current_parts: &dyn tokeira_platform::definition::SourceResolver,
    ) -> Result<(), Vec<String>>
    where
        C: Serialize,
    {
        // Both revisions run the same full interpretation as evaluate() —
        // one evaluation path for checking and execution — and the diff runs
        // over the host-free config values. The *current* source supplies the
        // `#[create]` admission metadata: the annotation set an operator is
        // editing under is the one that gates them. Each side resolves its
        // parts through its own resolver — the retained set for the prior,
        // the live set for the current — so a multi-document definition
        // compares as the set it was.
        let evaluate_config = |source: FrontendSource<'_>,
                               parts: &dyn tokeira_platform::definition::SourceResolver,
                               label: &str|
         -> Result<crate::Value<HostValue>, Vec<String>> {
            let text = std::str::from_utf8(source.bytes)
                .map_err(|error| vec![format!("{label} definition is not UTF-8: {error}")])?;
            let fields = context_fields(context).map_err(|error| vec![error.msg.clone()])?;
            let bridge = FrameworkBridge::new(namespaces, fields);
            let (_, config) = crate::interpret(text, &bridge, &(), parts)
                .map_err(|error| vec![format!("{label} definition: {}", error.msg)])?;
            Ok(config)
        };
        let prior_config = evaluate_config(prior, prior_parts, "prior")?;
        let current_config = evaluate_config(current, current_parts, "current")?;
        let current_text = std::str::from_utf8(current.bytes)
            .map_err(|error| vec![format!("current definition is not UTF-8: {error}")])?;
        crate::retarget_check(current_text, current_parts, &prior_config, &current_config)
    }
}

enum HostValue {
    Deployment,
    Module(String),
    Resource(ResourceReference),
    Output(OutputReference),
    Kind(Rc<RefCell<Option<DecodedKind>>>),
    Context(FieldMap<HostValue>),
}

impl Clone for HostValue {
    fn clone(&self) -> Self {
        match self {
            Self::Deployment => Self::Deployment,
            Self::Module(value) => Self::Module(value.clone()),
            Self::Resource(value) => Self::Resource(value.clone()),
            Self::Output(value) => Self::Output(value.clone()),
            Self::Kind(value) => Self::Kind(Rc::clone(value)),
            Self::Context(value) => Self::Context(value.clone()),
        }
    }
}

impl fmt::Debug for HostValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deployment => f.write_str("Deployment"),
            Self::Module(value) => f.debug_tuple("Module").field(value).finish(),
            Self::Resource(value) => f.debug_tuple("Resource").field(value).finish(),
            Self::Output(value) => f.debug_tuple("Output").field(value).finish(),
            Self::Kind(_) => f.write_str("Kind"),
            Self::Context(_) => f.write_str("Context"),
        }
    }
}

struct FrameworkBridge<'a> {
    namespaces: &'a [Namespace],
    context: FieldMap<HostValue>,
    graph: RefCell<Option<StructuralGraphBuilder<DecodedKind>>>,
}

impl<'a> FrameworkBridge<'a> {
    fn new(namespaces: &'a [Namespace], context: FieldMap<HostValue>) -> Self {
        Self {
            namespaces,
            context,
            graph: RefCell::new(Some(StructuralGraphBuilder::new())),
        }
    }

    fn contains(&self, name: &str) -> bool {
        self.namespaces
            .iter()
            .any(|namespace| namespace.kinds.contains(&name))
    }

    fn decode(&self, name: &str, input: LocatedValue) -> Result<DecodedKind, EvalError> {
        let namespace = self
            .namespaces
            .iter()
            .find(|namespace| namespace.kinds.contains(&name))
            .ok_or_else(|| EvalError::new(format!("unknown resource kind `{name}`")))?;
        (namespace.decode)(name, input)
            .ok_or_else(|| {
                EvalError::new(format!(
                    "namespace `{}` advertises resource kind `{name}` but cannot decode it",
                    namespace.name
                ))
            })?
            .map_err(|error| EvalError::new(error.message))
    }

    fn with_graph<T>(
        &self,
        action: impl FnOnce(&mut StructuralGraphBuilder<DecodedKind>) -> Result<T, EvalError>,
    ) -> Result<T, EvalError> {
        let mut graph = self.graph.borrow_mut();
        let graph = graph
            .as_mut()
            .ok_or_else(|| EvalError::new("the structural graph is already complete"))?;
        action(graph)
    }

    fn module_dependencies(value: Value<HostValue>) -> Result<Vec<String>, EvalError> {
        let Value::Vec(values) = value else {
            return Err(EvalError::new(
                "module dependencies must be an array of module names or handles",
            ));
        };
        values
            .into_iter()
            .map(|value| match value {
                Value::Host(HostValue::Module(module)) | Value::Str(module) => Ok(module),
                other => Err(EvalError::new(format!(
                    "module dependencies must be module names or handles, got {other:?}"
                ))),
            })
            .collect()
    }

    fn resource_dependencies(value: Value<HostValue>) -> Result<Vec<ResourceReference>, EvalError> {
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

    fn add_module(&self, args: Vec<Value<HostValue>>) -> Result<Value<HostValue>, EvalError> {
        let mut args = args.into_iter();
        let name = args
            .next()
            .ok_or_else(|| EvalError::new("Deployment.module expects a module name"))?
            .as_str()?
            .to_string();
        let dependencies = args
            .next()
            .map(Self::module_dependencies)
            .transpose()?
            .unwrap_or_default();
        if args.next().is_some() {
            return Err(EvalError::new(
                "Deployment.module expects a name and one dependency collection",
            ));
        }
        self.with_graph(|graph| {
            graph.add_module(name.clone(), dependencies);
            Ok(())
        })?;
        Ok(Value::Host(HostValue::Module(name)))
    }

    fn add_resource(
        &self,
        module: String,
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
        let resource = self
            .with_graph(|graph| Ok(graph.add_resource(module, logical_id, kind, dependencies)))?;
        Ok(Value::Host(HostValue::Resource(resource)))
    }
}

impl HostBridge for FrameworkBridge<'_> {
    type Host = HostValue;
    type Cx = ();
    type Output = VerifiedGraph<DecodedKind>;

    fn is_kind(&self, name: &str) -> bool {
        self.contains(name)
    }

    fn knows_method(&self, name: &str) -> bool {
        matches!(name, "module" | "resource" | "writeback" | "output")
    }

    fn knows_assoc(&self, path: &str) -> bool {
        path == "Deployment::new"
    }

    fn kind_defaults(&self, name: &str) -> Option<FieldMap<Self::Host>> {
        let namespace = self
            .namespaces
            .iter()
            .find(|namespace| namespace.kinds.contains(&name))?;
        let defaults = (namespace.defaults?)(name)?;
        match located_to_value(defaults).ok()? {
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
        let kind = self.decode(name, input)?;
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
        self.with_graph(|graph| {
            for namespace in namespaces {
                graph.add_namespace(namespace);
            }
            Ok(())
        })?;
        Ok(HostValue::Deployment)
    }

    fn call_method(
        &self,
        receiver: &Self::Host,
        method: &str,
        mut args: Vec<Value<Self::Host>>,
        _context: &Self::Cx,
    ) -> Result<Value<Self::Host>, EvalError> {
        match (receiver, method) {
            (HostValue::Deployment, "module") => self.add_module(args),
            (HostValue::Deployment, "resource") => {
                let Some(Value::Host(HostValue::Module(module))) = args.first().cloned() else {
                    return Err(EvalError::new(
                        "Deployment.resource expects a module handle first",
                    ));
                };
                let _ = args.remove(0);
                self.add_resource(module, args)
            }
            (HostValue::Module(module), "resource") => self.add_resource(module.clone(), args),
            (HostValue::Deployment, "writeback") => {
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
                self.with_graph(|graph| {
                    graph.add_writeback(key, value);
                    Ok(())
                })?;
                Ok(Value::Unit)
            }
            (HostValue::Resource(resource), "output") => {
                let [value] = args.as_slice() else {
                    return Err(EvalError::new("Resource.output expects one string"));
                };
                let output = self.with_graph(|graph| {
                    graph
                        .output(resource, value.as_str()?)
                        .map_err(|error| EvalError::new(error.to_string()))
                })?;
                Ok(Value::Host(HostValue::Output(output)))
            }
            (receiver, method) => Err(EvalError::new(format!(
                "receiver {receiver:?} has no method `{method}`"
            ))),
        }
    }

    fn host_field(&self, host: &Self::Host, field: &str) -> Result<Value<Self::Host>, EvalError> {
        let HostValue::Context(fields) = host else {
            return Err(EvalError::new(format!(
                "receiver {host:?} has no field `{field}`"
            )));
        };
        fields
            .get(field)
            .cloned()
            .ok_or_else(|| EvalError::new(format!("unknown context field `{field}`")))
    }

    fn cx_host(&self, _context: &Self::Cx) -> Self::Host {
        HostValue::Context(self.context.clone())
    }

    fn finish(&self, value: Self::Host) -> Result<Self::Output, EvalError> {
        if !matches!(value, HostValue::Deployment) {
            return Err(EvalError::new(format!(
                "deployment() must return a Deployment, got {value:?}"
            )));
        }
        self.graph
            .borrow_mut()
            .take()
            .ok_or_else(|| EvalError::new("the structural graph is already complete"))?
            .finish()
            .map_err(|error| EvalError::new(error.to_string()))
    }
}

fn context_fields<C: Serialize>(context: &C) -> Result<FieldMap<HostValue>, EvalError> {
    let value = serde_json::to_value(context)
        .map_err(|error| EvalError::new(format!("cannot encode platform context: {error}")))?;
    let serde_json::Value::Object(fields) = value else {
        return Err(EvalError::new(
            "platform context must serialize as a struct",
        ));
    };
    fields
        .into_iter()
        .map(|(name, value)| json_to_value(value).map(|value| (name, value)))
        .collect()
}

fn json_to_value(value: serde_json::Value) -> Result<Value<HostValue>, EvalError> {
    match value {
        serde_json::Value::Null => Ok(Value::Opt(None)),
        serde_json::Value::Bool(value) => Ok(Value::Bool(value)),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(i128::from)
            .or_else(|| value.as_u64().map(i128::from))
            .map(Value::Int)
            .ok_or_else(|| EvalError::new("floating-point context values are not supported")),
        serde_json::Value::String(value) => Ok(Value::Str(value)),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(json_to_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Vec),
        serde_json::Value::Object(fields) => fields
            .into_iter()
            .map(|(name, value)| json_to_value(value).map(|value| (name, value)))
            .collect::<Result<FieldMap<_>, _>>()
            .map(|fields| Value::Struct {
                ty: "ContextValue".to_string(),
                fields,
            }),
    }
}

fn value_to_located(value: Value<HostValue>) -> Result<LocatedValue, EvalError> {
    let value = match value {
        Value::Unit => ValueShape::Unit,
        Value::Bool(value) => ValueShape::Bool(value),
        Value::Int(value) => ValueShape::Integer(value),
        Value::Str(value) => ValueShape::String(value),
        Value::Vec(values) => ValueShape::Sequence(
            values
                .into_iter()
                .map(value_to_located)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Value::Tuple(_) => {
            return Err(EvalError::new(
                "tuple values are not admitted across the definition-frontend boundary",
            ));
        }
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
                VariantBody::Tuple(mut values) if values.len() == 1 => {
                    VariantShape::Newtype(Box::new(value_to_located(values.remove(0))?))
                }
                VariantBody::Tuple(_) => {
                    return Err(EvalError::new(
                        "tuple enum variants are not admitted across the definition-frontend boundary",
                    ));
                }
                VariantBody::Struct(_) => {
                    return Err(EvalError::new(
                        "struct enum variants are not admitted across the definition-frontend boundary",
                    ));
                }
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
        ValueShape::Sequence(values) => values
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
                VariantShape::Newtype(value) => VariantBody::Tuple(vec![located_to_value(*value)?]),
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
    use serde::{Deserialize, Serialize};
    use tokeira_platform::{
        author::{LocatedValue, from_located_value},
        definition::{DefinitionFrontend, DefinitionSourceName, FrontendSource, Namespace},
        error::KindError,
        kind::{DecodedKind, Kind, PlacementContext},
    };

    use super::TkdFrontend;

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    struct TestConfig {
        replicas: u16,
    }

    #[derive(Debug, Clone, Serialize)]
    struct TestContext {
        project_name: String,
    }

    #[derive(Debug)]
    struct TestKind;

    impl Kind<TestResource> for TestKind {
        fn realize(&self, _placement: &PlacementContext) -> Result<TestResource, KindError> {
            Ok(TestResource)
        }
    }

    #[derive(Debug)]
    struct TestResource;

    #[allow(clippy::manual_async_fn)]
    impl tokeira_iac::Resource for TestResource {
        fn resource_type(&self) -> tokeira_iac::ResourceType {
            tokeira_iac::ResourceType::new("TestResource")
        }

        fn declared_outputs(&self) -> &'static [&'static str] {
            &["endpoint"]
        }

        fn resource_id(&self) -> tokeira_iac::ResourceId {
            tokeira_iac::ResourceId("test-resource".to_string())
        }

        fn dependencies(&self) -> Vec<tokeira_iac::ResourceId> {
            Vec::new()
        }

        fn module(&self) -> &str {
            "test"
        }

        fn create<'life0, 'life1, 'async_trait>(
            &'life0 self,
            _ctx: &'life1 tokeira_iac::ProvisionContext,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<tokeira_iac::ResourceState, tokeira_iac::IacError>,
                    > + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async { unreachable!("frontend tests never execute resource lifecycle") })
        }

        fn update<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            _current: &'life1 tokeira_iac::ResourceState,
            _ctx: &'life2 tokeira_iac::ProvisionContext,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<tokeira_iac::ResourceState, tokeira_iac::IacError>,
                    > + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async { unreachable!("frontend tests never execute resource lifecycle") })
        }

        fn delete<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            _current: &'life1 tokeira_iac::ResourceState,
            _ctx: &'life2 tokeira_iac::ProvisionContext,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<(), tokeira_iac::IacError>>
                    + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async { unreachable!("frontend tests never execute resource lifecycle") })
        }

        fn describe<'life0, 'life1, 'async_trait>(
            &'life0 self,
            _ctx: &'life1 tokeira_iac::ProvisionContext,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<tokeira_iac::DescribeResult, tokeira_iac::IacError>,
                    > + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async { unreachable!("frontend tests never execute resource lifecycle") })
        }

        fn diff(
            &self,
            _current: &tokeira_iac::ResourceState,
            _ctx: &tokeira_iac::ProvisionContext,
        ) -> tokeira_iac::InternalChange {
            unreachable!("frontend tests never execute resource lifecycle")
        }

        fn change_semantics(
            &self,
            _ctx: &tokeira_iac::SemanticsContext<'_>,
        ) -> tokeira_iac::ChangeSemantics {
            tokeira_iac::ChangeSemantics::default()
        }
    }

    fn decode_test_resource(
        name: &str,
        _value: LocatedValue,
    ) -> Option<Result<DecodedKind, KindError>> {
        (name == "TestResource").then(|| {
            Ok(DecodedKind::resource::<TestKind, TestResource>(
                "TestResource",
                TestKind,
            ))
        })
    }

    fn namespaces() -> [Namespace; 1] {
        [Namespace {
            name: "test",
            kinds: &["TestResource"],
            defaults: None,
            decode: decode_test_resource,
        }]
    }

    #[test]
    fn evaluates_typed_context_to_completed_graph_and_config() {
        let source_name = DefinitionSourceName::AuthoringPath("definition.tkd".into());
        let source = br#"
            struct Config { replicas: u16 }
            fn config() -> Config { Config { replicas: 2 } }
            fn deployment(cfg: Config, cx: Context) -> Deployment {
                let d = Deployment::new(&["default"]);
                let core = d.module("core", vec![]);
                let resource = core.resource("resource", TestResource {});
                d.writeback("endpoint", resource.output("endpoint"));
                let _ = cx.project_name;
                d
            }
        "#;
        let output = TkdFrontend::new()
            .evaluate(
                FrontendSource {
                    source_name: &source_name,
                    bytes: source,
                },
                &TestContext {
                    project_name: "example".to_string(),
                },
                &namespaces(),
                &tokeira_platform::definition::NoPartSources,
            )
            .expect("definition should evaluate");
        let config: TestConfig = from_located_value(output.config).expect("config should decode");
        assert_eq!(config, TestConfig { replicas: 2 });
        assert_eq!(output.graph.resources().len(), 1);
        assert_eq!(output.graph.writeback().len(), 1);
    }

    fn retarget_source(mode: &str, replicas: u16) -> Vec<u8> {
        format!(
            r#"
            struct Config {{ #[create] mode: String, replicas: u16 }}
            fn config() -> Config {{
                Config {{ mode: "{mode}".into(), replicas: {replicas} }}
            }}
            fn deployment(cfg: Config, cx: Context) -> Deployment {{
                let d = Deployment::new(&["default"]);
                let core = d.module("core", vec![]);
                core.resource("resource", TestResource {{}});
                let _ = cx.project_name;
                d
            }}
            "#
        )
        .into_bytes()
    }

    #[test]
    fn retarget_check_refuses_a_create_change_and_admits_the_rest() {
        let source_name = DefinitionSourceName::AuthoringPath("definition.tkd".into());
        let frontend = TkdFrontend::new();
        let context = TestContext {
            project_name: "example".to_string(),
        };
        let prior = retarget_source("dsql", 1);

        // A `#[create]` field changed → refused, naming the field.
        let retargeted = retarget_source("in-memory", 1);
        let messages = frontend
            .retarget_check(
                FrontendSource {
                    source_name: &source_name,
                    bytes: &prior,
                },
                FrontendSource {
                    source_name: &source_name,
                    bytes: &retargeted,
                },
                &context,
                &namespaces(),
                &tokeira_platform::definition::NoPartSources,
                &tokeira_platform::definition::NoPartSources,
            )
            .expect_err("a create-time change is a retarget");
        assert!(
            messages.iter().any(|m| m.contains("mode")),
            "refusal names the field: {messages:?}"
        );

        // Only an ordinary field changed → admitted.
        let reconciled = retarget_source("dsql", 3);
        frontend
            .retarget_check(
                FrontendSource {
                    source_name: &source_name,
                    bytes: &prior,
                },
                FrontendSource {
                    source_name: &source_name,
                    bytes: &reconciled,
                },
                &context,
                &namespaces(),
                &tokeira_platform::definition::NoPartSources,
                &tokeira_platform::definition::NoPartSources,
            )
            .expect("a non-create edit reconciles");
    }
}
