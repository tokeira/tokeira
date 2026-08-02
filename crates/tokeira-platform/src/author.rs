//! Host-free definition values and generic authoring dispatch.
//!
//! Frontends convert their own syntax/runtime values into [`AuthorNode`] and
//! keep opaque [`AuthorHandle`] wrappers in the frontend runtime. Provider and
//! platform crates therefore never receive parser values or host objects.

use std::{fmt, sync::Weak};

use serde::de::{
    self, DeserializeOwned, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess,
    VariantAccess, Visitor,
};

use crate::{
    binding::{Platform, PlatformBinding},
    context::{ContextArgument, ContextProjection, PlatformContext},
    error::{AuthorError, SourceRange},
    graph::{
        DeploymentGraphBuilder, DeploymentHandle, KindHandle, ModuleHandle, OutputReference,
        ResourceHandle, VerifiedGraph,
    },
};

/// A frontend-neutral value with an optional source byte range.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthorNode {
    /// Host-free value shape.
    pub value: AuthorValue,
    /// Most specific frontend-supplied byte range for this value.
    pub range: Option<SourceRange>,
}

impl AuthorNode {
    /// Construct an unlocated host-free value.
    pub fn new(value: AuthorValue) -> Self {
        Self { value, range: None }
    }

    /// Attach a frontend-supplied byte range.
    pub fn located(mut self, range: SourceRange) -> Self {
        self.range = Some(range);
        self
    }

    /// Construct a string value.
    pub fn string(value: impl Into<String>) -> Self {
        Self::new(AuthorValue::String(value.into()))
    }
}

/// Host-free Serde-shaped value admitted from every definition frontend.
#[derive(Debug, Clone, PartialEq)]
pub enum AuthorValue {
    /// Serde unit.
    Unit,
    /// Boolean scalar.
    Bool(bool),
    /// Signed integer scalar, widened so frontends do not choose a Rust width.
    Integer(i128),
    /// Floating-point scalar.
    Float(f64),
    /// UTF-8 string scalar.
    String(String),
    /// Variable-length sequence.
    Sequence(Vec<AuthorNode>),
    /// Fixed-shape tuple.
    Tuple(Vec<AuthorNode>),
    /// Explicit option shape.
    Option(Option<Box<AuthorNode>>),
    /// Ordered map entries.
    Map(Vec<(AuthorNode, AuthorNode)>),
    /// Named struct shape; the type name is diagnostic metadata.
    Struct {
        /// Frontend-provided type name.
        name: String,
        /// Fields in source declaration order.
        fields: Vec<(String, AuthorNode)>,
    },
    /// Named externally tagged enum shape.
    Enum {
        /// Frontend-provided enum name.
        name: String,
        /// Selected variant.
        variant: String,
        /// Variant payload.
        body: AuthorVariantBody,
    },
    /// Opaque typed platform-context value interned by one author session.
    ContextToken(ContextToken),
}

/// Payload shape of an externally tagged author enum.
#[derive(Debug, Clone, PartialEq)]
pub enum AuthorVariantBody {
    /// Unit variant.
    Unit,
    /// Tuple or newtype variant.
    Tuple(Vec<AuthorNode>),
    /// Struct variant.
    Struct(Vec<(String, AuthorNode)>),
}

/// Opaque identity of one typed value stored by an [`AuthorSession`].
#[derive(Debug, Clone)]
pub struct ContextToken {
    owner: Weak<()>,
    index: usize,
}

impl PartialEq for ContextToken {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.owner.ptr_eq(&other.owner)
    }
}

impl Eq for ContextToken {}

/// Serde admission error retaining the most specific source range encountered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorDecodeError {
    message: String,
    range: Option<SourceRange>,
}

impl AuthorDecodeError {
    fn at(message: impl Into<String>, range: Option<SourceRange>) -> Self {
        Self {
            message: message.into(),
            range,
        }
    }

    fn custom(message: impl Into<String>) -> Self {
        Self::at(message, None)
    }

    /// Borrow the Serde failure detail.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Most specific source range involved in the failure.
    pub fn range(&self) -> Option<SourceRange> {
        self.range
    }
}

impl fmt::Display for AuthorDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for AuthorDecodeError {}

impl de::Error for AuthorDecodeError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self::at(message.to_string(), None)
    }
}

/// Decode a host-free author value through ordinary Serde data-model semantics.
pub fn from_author_node<T: DeserializeOwned>(node: AuthorNode) -> Result<T, AuthorDecodeError> {
    T::deserialize(node)
}

fn value_kind(value: &AuthorValue) -> &'static str {
    match value {
        AuthorValue::Unit => "unit",
        AuthorValue::Bool(_) => "boolean",
        AuthorValue::Integer(_) => "integer",
        AuthorValue::Float(_) => "float",
        AuthorValue::String(_) => "string",
        AuthorValue::Sequence(_) => "sequence",
        AuthorValue::Tuple(_) => "tuple",
        AuthorValue::Option(_) => "option",
        AuthorValue::Map(_) => "map",
        AuthorValue::Struct { .. } => "struct",
        AuthorValue::Enum { .. } => "enum",
        AuthorValue::ContextToken(_) => "platform context token",
    }
}

fn mismatch(expected: &str, node: &AuthorNode) -> AuthorDecodeError {
    AuthorDecodeError::at(
        format!("expected {expected}, found {}", value_kind(&node.value)),
        node.range,
    )
}

macro_rules! deserialize_signed {
    ($method:ident, $visit:ident, $ty:ty) => {
        fn $method<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            let range = self.range;
            match self.value {
                AuthorValue::Integer(value) => <$ty>::try_from(value)
                    .map_err(|_| {
                        AuthorDecodeError::at(format!("integer {value} is out of range"), range)
                    })
                    .and_then(|value| visitor.$visit(value)),
                value => Err(mismatch(stringify!($ty), &AuthorNode { value, range })),
            }
        }
    };
}

macro_rules! deserialize_unsigned {
    ($method:ident, $visit:ident, $ty:ty) => {
        fn $method<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: Visitor<'de>,
        {
            let range = self.range;
            match self.value {
                AuthorValue::Integer(value) => <$ty>::try_from(value)
                    .map_err(|_| {
                        AuthorDecodeError::at(format!("integer {value} is out of range"), range)
                    })
                    .and_then(|value| visitor.$visit(value)),
                value => Err(mismatch(stringify!($ty), &AuthorNode { value, range })),
            }
        }
    };
}

impl<'de> de::Deserializer<'de> for AuthorNode {
    type Error = AuthorDecodeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            AuthorValue::Unit => visitor.visit_unit(),
            AuthorValue::Bool(value) => visitor.visit_bool(value),
            AuthorValue::Integer(value) => visitor.visit_i128(value),
            AuthorValue::Float(value) => visitor.visit_f64(value),
            AuthorValue::String(value) => visitor.visit_string(value),
            AuthorValue::Sequence(values) | AuthorValue::Tuple(values) => {
                visitor.visit_seq(NodeSeqAccess::new(values))
            }
            AuthorValue::Option(None) => visitor.visit_none(),
            AuthorValue::Option(Some(value)) => visitor.visit_some(*value),
            AuthorValue::Map(entries) => visitor.visit_map(NodeMapAccess::new(entries)),
            AuthorValue::Struct { fields, .. } => visitor.visit_map(StructMapAccess::new(fields)),
            AuthorValue::Enum { variant, body, .. } => {
                visitor.visit_enum(NodeEnumAccess { variant, body })
            }
            AuthorValue::ContextToken(_) => Err(AuthorDecodeError::at(
                "platform context tokens cannot be decoded as configuration or provider input",
                range,
            )),
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            AuthorValue::Bool(value) => visitor.visit_bool(value),
            value => Err(mismatch("boolean", &AuthorNode { value, range })),
        }
    }

    deserialize_signed!(deserialize_i8, visit_i8, i8);
    deserialize_signed!(deserialize_i16, visit_i16, i16);
    deserialize_signed!(deserialize_i32, visit_i32, i32);
    deserialize_signed!(deserialize_i64, visit_i64, i64);
    deserialize_signed!(deserialize_i128, visit_i128, i128);
    deserialize_unsigned!(deserialize_u8, visit_u8, u8);
    deserialize_unsigned!(deserialize_u16, visit_u16, u16);
    deserialize_unsigned!(deserialize_u32, visit_u32, u32);
    deserialize_unsigned!(deserialize_u64, visit_u64, u64);
    deserialize_unsigned!(deserialize_u128, visit_u128, u128);

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            AuthorValue::Float(value) => visitor.visit_f32(value as f32),
            AuthorValue::Integer(value) => visitor.visit_f32(value as f32),
            value => Err(mismatch("number", &AuthorNode { value, range })),
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            AuthorValue::Float(value) => visitor.visit_f64(value),
            AuthorValue::Integer(value) => visitor.visit_f64(value as f64),
            value => Err(mismatch("number", &AuthorNode { value, range })),
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            AuthorValue::String(value) => {
                let mut chars = value.chars();
                let Some(character) = chars.next() else {
                    return Err(AuthorDecodeError::at(
                        "expected one character, found an empty string",
                        range,
                    ));
                };
                if chars.next().is_some() {
                    return Err(AuthorDecodeError::at(
                        "expected one character, found a longer string",
                        range,
                    ));
                }
                visitor.visit_char(character)
            }
            value => Err(mismatch("character", &AuthorNode { value, range })),
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            AuthorValue::String(value) => visitor.visit_string(value),
            value => Err(mismatch("string", &AuthorNode { value, range })),
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            AuthorValue::String(value) => visitor.visit_byte_buf(value.into_bytes()),
            value => Err(mismatch("byte string", &AuthorNode { value, range })),
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            AuthorValue::Option(None) => visitor.visit_none(),
            AuthorValue::Option(Some(value)) => visitor.visit_some(*value),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            AuthorValue::Unit => visitor.visit_unit(),
            value => Err(mismatch("unit", &AuthorNode { value, range })),
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            AuthorValue::Sequence(values) | AuthorValue::Tuple(values) => {
                visitor.visit_seq(NodeSeqAccess::new(values))
            }
            value => Err(mismatch("sequence", &AuthorNode { value, range })),
        }
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            AuthorValue::Map(entries) => visitor.visit_map(NodeMapAccess::new(entries)),
            AuthorValue::Struct { fields, .. } => visitor.visit_map(StructMapAccess::new(fields)),
            value => Err(mismatch("map", &AuthorNode { value, range })),
        }
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            AuthorValue::Enum { variant, body, .. } => {
                visitor.visit_enum(NodeEnumAccess { variant, body })
            }
            AuthorValue::String(variant) => visitor.visit_enum(variant.into_deserializer()),
            value => Err(mismatch("enum", &AuthorNode { value, range })),
        }
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }
}

struct NodeSeqAccess {
    values: std::vec::IntoIter<AuthorNode>,
}

impl NodeSeqAccess {
    fn new(values: Vec<AuthorNode>) -> Self {
        Self {
            values: values.into_iter(),
        }
    }
}

impl<'de> SeqAccess<'de> for NodeSeqAccess {
    type Error = AuthorDecodeError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        self.values
            .next()
            .map(|value| seed.deserialize(value))
            .transpose()
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.values.len())
    }
}

struct NodeMapAccess {
    entries: std::vec::IntoIter<(AuthorNode, AuthorNode)>,
    value: Option<AuthorNode>,
}

impl NodeMapAccess {
    fn new(entries: Vec<(AuthorNode, AuthorNode)>) -> Self {
        Self {
            entries: entries.into_iter(),
            value: None,
        }
    }
}

impl<'de> MapAccess<'de> for NodeMapAccess {
    type Error = AuthorDecodeError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        let Some((key, value)) = self.entries.next() else {
            return Ok(None);
        };
        self.value = Some(value);
        seed.deserialize(key).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let value = self
            .value
            .take()
            .ok_or_else(|| AuthorDecodeError::custom("map value requested before map key"))?;
        seed.deserialize(value)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.entries.len())
    }
}

struct StructMapAccess {
    fields: std::vec::IntoIter<(String, AuthorNode)>,
    value: Option<AuthorNode>,
}

impl StructMapAccess {
    fn new(fields: Vec<(String, AuthorNode)>) -> Self {
        Self {
            fields: fields.into_iter(),
            value: None,
        }
    }
}

impl<'de> MapAccess<'de> for StructMapAccess {
    type Error = AuthorDecodeError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        let Some((key, value)) = self.fields.next() else {
            return Ok(None);
        };
        self.value = Some(value);
        seed.deserialize(key.into_deserializer()).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let value = self
            .value
            .take()
            .ok_or_else(|| AuthorDecodeError::custom("struct value requested before field name"))?;
        seed.deserialize(value)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.fields.len())
    }
}

struct NodeEnumAccess {
    variant: String,
    body: AuthorVariantBody,
}

impl<'de> EnumAccess<'de> for NodeEnumAccess {
    type Error = AuthorDecodeError;
    type Variant = NodeVariantAccess;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let variant = seed.deserialize(self.variant.into_deserializer())?;
        Ok((variant, NodeVariantAccess { body: self.body }))
    }
}

struct NodeVariantAccess {
    body: AuthorVariantBody,
}

impl<'de> VariantAccess<'de> for NodeVariantAccess {
    type Error = AuthorDecodeError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        match self.body {
            AuthorVariantBody::Unit => Ok(()),
            _ => Err(AuthorDecodeError::custom("expected a unit enum variant")),
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.body {
            AuthorVariantBody::Tuple(mut values) if values.len() == 1 => {
                seed.deserialize(values.remove(0))
            }
            _ => Err(AuthorDecodeError::custom(
                "expected a single-value enum variant",
            )),
        }
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.body {
            AuthorVariantBody::Tuple(values) => visitor.visit_seq(NodeSeqAccess::new(values)),
            _ => Err(AuthorDecodeError::custom("expected a tuple enum variant")),
        }
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.body {
            AuthorVariantBody::Struct(fields) => visitor.visit_map(StructMapAccess::new(fields)),
            _ => Err(AuthorDecodeError::custom("expected a struct enum variant")),
        }
    }
}

/// Opaque frontend-side identity for a framework authoring value.
#[derive(Debug, Clone)]
pub enum AuthorHandle {
    /// The sole deployment graph under construction.
    Deployment(DeploymentHandle),
    /// A declared module.
    Module(ModuleHandle),
    /// A declared provider resource.
    Resource(ResourceHandle),
    /// A checked provider-resource output reference.
    Output(OutputReference),
    /// A constructed but not yet installed provider kind.
    Kind(KindHandle),
    /// The immutable platform context receiver.
    Context(ContextHandle),
    /// One typed context value interned by this session.
    ContextValue(ContextToken),
}

/// Opaque identity of the immutable platform context receiver.
#[derive(Debug, Clone)]
pub struct ContextHandle {
    owner: Weak<()>,
}

impl PartialEq for ContextHandle {
    fn eq(&self, other: &Self) -> bool {
        self.owner.ptr_eq(&other.owner)
    }
}

impl Eq for ContextHandle {}

/// Argument admitted by generic authoring dispatch.
#[derive(Debug, Clone)]
pub enum AuthorArgument {
    /// Host-free data.
    Value(AuthorNode),
    /// Opaque framework identity.
    Handle(AuthorHandle),
}

/// Result returned to a definition frontend.
#[derive(Debug, Clone)]
pub enum AuthorResult {
    /// Host-free data.
    Value(AuthorNode),
    /// Opaque framework identity.
    Handle(AuthorHandle),
}

/// Schema for one associated authoring function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssociatedFunctionSchema {
    /// Stable dispatch name.
    pub name: String,
}

/// Schema for methods and fields on one receiver kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverSchema {
    /// Stable receiver name.
    pub receiver: String,
    /// Complete method inventory.
    pub methods: Vec<String>,
    /// Complete field inventory.
    pub fields: Vec<String>,
}

/// Schema for one selected provider kind.
#[derive(Debug, Clone, PartialEq)]
pub struct KindSchema {
    /// Stable kind name.
    pub name: String,
    /// Provider-owned default field values used by frontends for `..EMPTY` syntax.
    pub defaults: Option<AuthorNode>,
    /// Declared output names.
    pub outputs: Vec<String>,
}

/// Discoverable, frontend-neutral authoring vocabulary.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthorSchema {
    /// Standard associated functions.
    pub associated_functions: Vec<AssociatedFunctionSchema>,
    /// Standard receiver methods and fields.
    pub receivers: Vec<ReceiverSchema>,
    /// Selected first-party kinds.
    pub kinds: Vec<KindSchema>,
    /// Platform context fields.
    pub context_fields: Vec<String>,
    /// Platform context methods.
    pub context_methods: Vec<String>,
}

/// One pure definition evaluation over a selected platform binding.
#[derive(Debug)]
pub struct AuthorSession<P: Platform> {
    binding: PlatformBinding<P>,
    context: P::Context,
    graph: DeploymentGraphBuilder,
    owner: std::sync::Arc<()>,
    context_values: Vec<<P::Context as PlatformContext>::Value>,
    kinds: Vec<Option<Box<dyn crate::catalog::ProviderKind>>>,
}

impl<P: Platform> AuthorSession<P> {
    /// Begin an in-memory authoring evaluation.
    pub fn new(binding: PlatformBinding<P>, context: P::Context) -> Self {
        let graph = DeploymentGraphBuilder::with_catalogs(
            binding.services.identities(),
            binding.providers.delivery_keys(),
        )
        .require_bootstrap(binding.bootstrap_module.clone());
        Self {
            binding,
            context,
            graph,
            owner: std::sync::Arc::new(()),
            context_values: Vec::new(),
            kinds: Vec::new(),
        }
    }

    /// Return the complete callable schema for the selected binding.
    pub fn schema(&self) -> AuthorSchema {
        AuthorSchema {
            associated_functions: vec![AssociatedFunctionSchema {
                name: "Deployment.new".into(),
            }],
            receivers: vec![
                ReceiverSchema {
                    receiver: "Deployment".into(),
                    methods: vec!["namespace".into(), "module".into(), "writeback".into()],
                    fields: Vec::new(),
                },
                ReceiverSchema {
                    receiver: "Module".into(),
                    methods: vec!["resource".into(), "workload".into()],
                    fields: Vec::new(),
                },
                ReceiverSchema {
                    receiver: "Resource".into(),
                    methods: vec!["output".into()],
                    fields: Vec::new(),
                },
            ],
            kinds: self.binding.kinds.schemas(),
            context_fields: P::Context::fields()
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            context_methods: P::Context::methods()
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
        }
    }

    /// Return the immutable platform context receiver for this session.
    pub fn context_handle(&self) -> ContextHandle {
        ContextHandle {
            owner: std::sync::Arc::downgrade(&self.owner),
        }
    }

    /// Construct one selected first-party provider kind as a take-once value.
    pub fn construct_kind(
        &mut self,
        name: &str,
        input: AuthorNode,
    ) -> Result<KindHandle, AuthorError> {
        let range = input.range;
        let kind = self
            .binding
            .kinds
            .decode(name, input)
            .map_err(|error| AuthorError::new(error.message).at(error.range.or(range)))?;
        let index = self.kinds.len();
        self.kinds.push(Some(kind));
        Ok(KindHandle::new(
            std::sync::Arc::downgrade(&self.owner),
            index,
        ))
    }

    /// Invoke a standard receiver method.
    pub fn call(
        &mut self,
        receiver: AuthorHandle,
        method: &str,
        args: Vec<AuthorArgument>,
    ) -> Result<AuthorResult, AuthorError> {
        match receiver {
            AuthorHandle::Deployment(handle) => self.call_deployment(handle, method, args),
            AuthorHandle::Module(handle) => self.call_module(handle, method, args),
            AuthorHandle::Resource(handle) => self.call_resource(handle, method, args),
            AuthorHandle::Context(handle) => self.call_context(handle, method, args),
            other => Err(AuthorError::new(format!(
                "receiver {} has no method `{method}`",
                handle_name(&other)
            ))),
        }
    }

    /// Invoke a standard associated function.
    pub fn associated(
        &mut self,
        name: &str,
        args: Vec<AuthorArgument>,
    ) -> Result<AuthorResult, AuthorError> {
        if name != "Deployment.new" {
            return Err(AuthorError::new(format!(
                "unknown associated function `{name}`; supported: Deployment.new"
            )));
        }
        if !args.is_empty() {
            return Err(AuthorError::new("Deployment.new accepts no arguments"));
        }
        Ok(AuthorResult::Handle(AuthorHandle::Deployment(
            self.graph.deployment_handle(),
        )))
    }

    /// Read a standard or platform-context field.
    pub fn field(
        &mut self,
        receiver: AuthorHandle,
        name: &str,
    ) -> Result<AuthorResult, AuthorError> {
        let AuthorHandle::Context(handle) = receiver else {
            return Err(AuthorError::new(format!(
                "receiver {} has no field `{name}`",
                handle_name(&receiver)
            )));
        };
        self.check_session_owner(&handle.owner, "context")?;
        let projection = self.context.field(name).map_err(|error| {
            AuthorError::new(format!(
                "{}; supported fields: {}",
                error,
                P::Context::fields().join(", ")
            ))
        })?;
        Ok(self.intern_projection(projection))
    }

    /// Complete the graph after validating the frontend's returned deployment handle.
    pub fn finish(
        self,
        deployment: DeploymentHandle,
    ) -> Result<VerifiedGraph, crate::error::GraphError> {
        self.graph.finish_for(deployment)
    }

    fn check_session_owner(&self, owner: &Weak<()>, kind: &'static str) -> Result<(), AuthorError> {
        let Some(owner) = owner.upgrade() else {
            return Err(AuthorError::new(format!("the {kind} handle has expired")));
        };
        if !std::sync::Arc::ptr_eq(&owner, &self.owner) {
            return Err(AuthorError::new(format!(
                "the {kind} handle belongs to another author session"
            )));
        }
        Ok(())
    }

    fn intern_projection(
        &mut self,
        projection: ContextProjection<<P::Context as PlatformContext>::Value>,
    ) -> AuthorResult {
        match projection {
            ContextProjection::Value(value) => AuthorResult::Value(value),
            ContextProjection::Token(value) => {
                let index = self.context_values.len();
                self.context_values.push(value);
                AuthorResult::Handle(AuthorHandle::ContextValue(ContextToken {
                    owner: std::sync::Arc::downgrade(&self.owner),
                    index,
                }))
            }
        }
    }

    fn call_deployment(
        &mut self,
        handle: DeploymentHandle,
        method: &str,
        args: Vec<AuthorArgument>,
    ) -> Result<AuthorResult, AuthorError> {
        match method {
            "namespace" => {
                let [AuthorArgument::Value(value)] = args.as_slice() else {
                    return Err(AuthorError::new("Deployment.namespace expects one string"));
                };
                let namespace = node_string(value)?;
                self.graph.add_namespace(&handle, namespace)?;
                Ok(AuthorResult::Value(AuthorNode::new(AuthorValue::Unit)))
            }
            "module" => {
                let Some(AuthorArgument::Value(name)) = args.first() else {
                    return Err(AuthorError::new(
                        "Deployment.module expects a name followed by module dependencies",
                    ));
                };
                let name = node_string(name)?;
                let dependencies = args[1..]
                    .iter()
                    .map(|arg| match arg {
                        AuthorArgument::Handle(AuthorHandle::Module(handle)) => Ok(handle.clone()),
                        _ => Err(AuthorError::new(
                            "Deployment.module dependencies must be module handles",
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let module = self.graph.add_module(&handle, name, dependencies)?;
                Ok(AuthorResult::Handle(AuthorHandle::Module(module)))
            }
            "writeback" => {
                let [AuthorArgument::Value(key), value] = args.as_slice() else {
                    return Err(AuthorError::new(
                        "Deployment.writeback expects a dotted key and literal or output",
                    ));
                };
                let key = node_string(key)?;
                let value = match value {
                    AuthorArgument::Value(value) => {
                        crate::graph::WritebackValue::Literal(node_string(value)?)
                    }
                    AuthorArgument::Handle(AuthorHandle::Output(output)) => {
                        crate::graph::WritebackValue::Output(output.clone())
                    }
                    _ => {
                        return Err(AuthorError::new(
                            "writeback value must be a string or output reference",
                        ));
                    }
                };
                self.graph.add_writeback(&handle, key, value)?;
                Ok(AuthorResult::Value(AuthorNode::new(AuthorValue::Unit)))
            }
            _ => Err(AuthorError::new(format!(
                "unknown Deployment method `{method}`; supported: namespace, module, writeback"
            ))),
        }
    }

    fn call_module(
        &mut self,
        handle: ModuleHandle,
        method: &str,
        args: Vec<AuthorArgument>,
    ) -> Result<AuthorResult, AuthorError> {
        match method {
            "resource" => {
                let [
                    AuthorArgument::Value(logical_id),
                    AuthorArgument::Handle(AuthorHandle::Kind(kind)),
                    dependencies @ ..,
                ] = args.as_slice()
                else {
                    return Err(AuthorError::new(
                        "Module.resource expects a logical id, kind, and optional resource dependencies",
                    ));
                };
                self.check_session_owner(kind.owner(), "provider-kind")?;
                let Some(cell) = self.kinds.get_mut(kind.index()) else {
                    return Err(AuthorError::new("provider-kind handle index is invalid"));
                };
                let provider_kind = cell
                    .take()
                    .ok_or_else(|| AuthorError::from(crate::error::GraphError::ConsumedKind))?;
                let dependencies = dependencies
                    .iter()
                    .map(|arg| match arg {
                        AuthorArgument::Handle(AuthorHandle::Resource(resource)) => {
                            Ok(resource.clone())
                        }
                        _ => Err(AuthorError::new(
                            "Module.resource dependencies must be resource handles",
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.graph
                    .validate_resource_insertion(&handle, &dependencies)?;
                let resource = self.graph.add_resource(
                    &handle,
                    node_string(logical_id)?,
                    provider_kind,
                    dependencies,
                )?;
                Ok(AuthorResult::Handle(AuthorHandle::Resource(resource)))
            }
            "workload" => {
                let [
                    AuthorArgument::Value(service),
                    AuthorArgument::Value(capacity),
                ] = args.as_slice()
                else {
                    return Err(AuthorError::new(
                        "Module.workload expects a logical service and desired capacity",
                    ));
                };
                let service_id = node_string(service)?;
                let capacity = node_u32(capacity)?;
                let service = self.binding.services.get(&service_id).ok_or_else(|| {
                    AuthorError::new(format!(
                        "unknown platform service `{service_id}`; supported: {}",
                        self.binding
                            .services
                            .identities()
                            .into_iter()
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                })?;
                let declaration = crate::graph::WorkloadDeclaration {
                    service: service.logical_id.clone(),
                    dependencies: service.placement.needs.clone(),
                    desired_capacity: capacity,
                    delivery: service.delivery.clone(),
                    document: service.document.clone(),
                };
                self.graph.add_workload(&handle, declaration)?;
                Ok(AuthorResult::Value(AuthorNode::new(AuthorValue::Unit)))
            }
            _ => Err(AuthorError::new(format!(
                "unknown Module method `{method}`; supported: resource, workload"
            ))),
        }
    }

    fn call_resource(
        &mut self,
        handle: ResourceHandle,
        method: &str,
        args: Vec<AuthorArgument>,
    ) -> Result<AuthorResult, AuthorError> {
        if method != "output" {
            return Err(AuthorError::new(format!(
                "unknown Resource method `{method}`; supported: output"
            )));
        }
        let [AuthorArgument::Value(name)] = args.as_slice() else {
            return Err(AuthorError::new("Resource.output expects one string"));
        };
        let output = handle.output(&node_string(name)?)?;
        Ok(AuthorResult::Handle(AuthorHandle::Output(output)))
    }

    fn call_context(
        &mut self,
        handle: ContextHandle,
        method: &str,
        args: Vec<AuthorArgument>,
    ) -> Result<AuthorResult, AuthorError> {
        self.check_session_owner(&handle.owner, "context")?;
        let args =
            args.into_iter()
                .map(|argument| match argument {
                    AuthorArgument::Value(value) => Ok(ContextArgument::Value(value)),
                    AuthorArgument::Handle(AuthorHandle::ContextValue(token)) => {
                        self.check_session_owner(&token.owner, "context-value")?;
                        let value = self.context_values.get(token.index).ok_or_else(|| {
                            AuthorError::new("context-value handle index is invalid")
                        })?;
                        Ok(ContextArgument::Token(value.clone()))
                    }
                    _ => Err(AuthorError::new(
                        "context methods accept host-free values or context-value handles",
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
        let projection = self.context.call(method, &args).map_err(|error| {
            AuthorError::new(format!(
                "{}; supported methods: {}",
                error,
                P::Context::methods().join(", ")
            ))
        })?;
        Ok(self.intern_projection(projection))
    }
}

fn node_string(node: &AuthorNode) -> Result<String, AuthorError> {
    match &node.value {
        AuthorValue::String(value) => Ok(value.clone()),
        _ => Err(AuthorError::new(format!(
            "expected string, found {}",
            value_kind(&node.value)
        ))
        .at(node.range)),
    }
}

fn node_u32(node: &AuthorNode) -> Result<u32, AuthorError> {
    match &node.value {
        AuthorValue::Integer(value) => u32::try_from(*value).map_err(|_| {
            AuthorError::new(format!("integer {value} is outside the u32 range")).at(node.range)
        }),
        _ => Err(AuthorError::new(format!(
            "expected integer, found {}",
            value_kind(&node.value)
        ))
        .at(node.range)),
    }
}

fn handle_name(handle: &AuthorHandle) -> &'static str {
    match handle {
        AuthorHandle::Deployment(_) => "Deployment",
        AuthorHandle::Module(_) => "Module",
        AuthorHandle::Resource(_) => "Resource",
        AuthorHandle::Output(_) => "Output",
        AuthorHandle::Kind(_) => "ProviderKind",
        AuthorHandle::Context(_) => "Context",
        AuthorHandle::ContextValue(_) => "ContextValue",
    }
}
