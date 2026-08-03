//! Located, host-free values returned by definition frontends.
//!
//! Frontends keep evaluator handles private and return only this Serde-shaped
//! tree plus a completed structural graph. The tree is transient and decoded
//! immediately into typed platform configuration or provider-kind input.

use std::fmt;

use serde::de::{
    self, DeserializeOwned, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess,
    VariantAccess, Visitor,
};

use crate::error::SourceRange;

/// A frontend-neutral value with an optional source byte range.
#[derive(Debug, Clone, PartialEq)]
pub struct LocatedValue {
    /// Host-free value shape.
    pub value: ValueShape,
    /// Most specific frontend-supplied byte range for this value.
    pub range: Option<SourceRange>,
}

impl LocatedValue {
    /// Construct an unlocated host-free value.
    pub fn new(value: ValueShape) -> Self {
        Self { value, range: None }
    }

    /// Attach a frontend-supplied byte range.
    pub fn located(mut self, range: SourceRange) -> Self {
        self.range = Some(range);
        self
    }

    /// Construct a string value.
    pub fn string(value: impl Into<String>) -> Self {
        Self::new(ValueShape::String(value.into()))
    }
}

/// Host-free Serde-shaped value admitted from every definition frontend.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueShape {
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
    Sequence(Vec<LocatedValue>),
    /// Explicit option shape.
    Option(Option<Box<LocatedValue>>),
    /// Ordered map entries.
    Map(Vec<(LocatedValue, LocatedValue)>),
    /// Named struct shape; the type name is diagnostic metadata.
    Struct {
        /// Frontend-provided type name.
        name: String,
        /// Fields in source declaration order.
        fields: Vec<(String, LocatedValue)>,
    },
    /// Named externally tagged enum shape.
    Enum {
        /// Frontend-provided enum name.
        name: String,
        /// Selected variant.
        variant: String,
        /// Variant payload.
        body: VariantShape,
    },
}

/// Payload shape of an externally tagged author enum.
#[derive(Debug, Clone, PartialEq)]
pub enum VariantShape {
    /// Unit variant.
    Unit,
    /// Single-value variant.
    Newtype(Box<LocatedValue>),
}

/// Serde admission error retaining the most specific source range encountered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueDecodeError {
    message: String,
    range: Option<SourceRange>,
}

impl ValueDecodeError {
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

impl fmt::Display for ValueDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ValueDecodeError {}

impl de::Error for ValueDecodeError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self::at(message.to_string(), None)
    }
}

/// Decode a host-free author value through ordinary Serde data-model semantics.
pub fn from_located_value<T: DeserializeOwned>(node: LocatedValue) -> Result<T, ValueDecodeError> {
    T::deserialize(node)
}

fn value_kind(value: &ValueShape) -> &'static str {
    match value {
        ValueShape::Unit => "unit",
        ValueShape::Bool(_) => "boolean",
        ValueShape::Integer(_) => "integer",
        ValueShape::Float(_) => "float",
        ValueShape::String(_) => "string",
        ValueShape::Sequence(_) => "sequence",
        ValueShape::Option(_) => "option",
        ValueShape::Map(_) => "map",
        ValueShape::Struct { .. } => "struct",
        ValueShape::Enum { .. } => "enum",
    }
}

fn mismatch(expected: &str, node: &LocatedValue) -> ValueDecodeError {
    ValueDecodeError::at(
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
                ValueShape::Integer(value) => <$ty>::try_from(value)
                    .map_err(|_| {
                        ValueDecodeError::at(format!("integer {value} is out of range"), range)
                    })
                    .and_then(|value| visitor.$visit(value)),
                value => Err(mismatch(stringify!($ty), &LocatedValue { value, range })),
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
                ValueShape::Integer(value) => <$ty>::try_from(value)
                    .map_err(|_| {
                        ValueDecodeError::at(format!("integer {value} is out of range"), range)
                    })
                    .and_then(|value| visitor.$visit(value)),
                value => Err(mismatch(stringify!($ty), &LocatedValue { value, range })),
            }
        }
    };
}

impl<'de> de::Deserializer<'de> for LocatedValue {
    type Error = ValueDecodeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            ValueShape::Unit => visitor.visit_unit(),
            ValueShape::Bool(value) => visitor.visit_bool(value),
            ValueShape::Integer(value) => visitor.visit_i128(value),
            ValueShape::Float(value) => visitor.visit_f64(value),
            ValueShape::String(value) => visitor.visit_string(value),
            ValueShape::Sequence(values) => visitor.visit_seq(NodeSeqAccess::new(values)),
            ValueShape::Option(None) => visitor.visit_none(),
            ValueShape::Option(Some(value)) => visitor.visit_some(*value),
            ValueShape::Map(entries) => visitor.visit_map(NodeMapAccess::new(entries)),
            ValueShape::Struct { fields, .. } => visitor.visit_map(StructMapAccess::new(fields)),
            ValueShape::Enum { variant, body, .. } => {
                visitor.visit_enum(NodeEnumAccess { variant, body })
            }
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            ValueShape::Bool(value) => visitor.visit_bool(value),
            value => Err(mismatch("boolean", &LocatedValue { value, range })),
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
            ValueShape::Float(value) => visitor.visit_f32(value as f32),
            ValueShape::Integer(value) => visitor.visit_f32(value as f32),
            value => Err(mismatch("number", &LocatedValue { value, range })),
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            ValueShape::Float(value) => visitor.visit_f64(value),
            ValueShape::Integer(value) => visitor.visit_f64(value as f64),
            value => Err(mismatch("number", &LocatedValue { value, range })),
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            ValueShape::String(value) => {
                let mut chars = value.chars();
                let Some(character) = chars.next() else {
                    return Err(ValueDecodeError::at(
                        "expected one character, found an empty string",
                        range,
                    ));
                };
                if chars.next().is_some() {
                    return Err(ValueDecodeError::at(
                        "expected one character, found a longer string",
                        range,
                    ));
                }
                visitor.visit_char(character)
            }
            value => Err(mismatch("character", &LocatedValue { value, range })),
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
            ValueShape::String(value) => visitor.visit_string(value),
            value => Err(mismatch("string", &LocatedValue { value, range })),
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            ValueShape::String(value) => visitor.visit_byte_buf(value.into_bytes()),
            value => Err(mismatch("byte string", &LocatedValue { value, range })),
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
            ValueShape::Option(None) => visitor.visit_none(),
            ValueShape::Option(Some(value)) => visitor.visit_some(*value),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let range = self.range;
        match self.value {
            ValueShape::Unit => visitor.visit_unit(),
            value => Err(mismatch("unit", &LocatedValue { value, range })),
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
            ValueShape::Sequence(values) => visitor.visit_seq(NodeSeqAccess::new(values)),
            value => Err(mismatch("sequence", &LocatedValue { value, range })),
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
            ValueShape::Map(entries) => visitor.visit_map(NodeMapAccess::new(entries)),
            ValueShape::Struct { fields, .. } => visitor.visit_map(StructMapAccess::new(fields)),
            value => Err(mismatch("map", &LocatedValue { value, range })),
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
            ValueShape::Enum { variant, body, .. } => {
                visitor.visit_enum(NodeEnumAccess { variant, body })
            }
            ValueShape::String(variant) => visitor.visit_enum(variant.into_deserializer()),
            value => Err(mismatch("enum", &LocatedValue { value, range })),
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
    values: std::vec::IntoIter<LocatedValue>,
}

impl NodeSeqAccess {
    fn new(values: Vec<LocatedValue>) -> Self {
        Self {
            values: values.into_iter(),
        }
    }
}

impl<'de> SeqAccess<'de> for NodeSeqAccess {
    type Error = ValueDecodeError;

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
    entries: std::vec::IntoIter<(LocatedValue, LocatedValue)>,
    value: Option<LocatedValue>,
}

impl NodeMapAccess {
    fn new(entries: Vec<(LocatedValue, LocatedValue)>) -> Self {
        Self {
            entries: entries.into_iter(),
            value: None,
        }
    }
}

impl<'de> MapAccess<'de> for NodeMapAccess {
    type Error = ValueDecodeError;

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
            .ok_or_else(|| ValueDecodeError::custom("map value requested before map key"))?;
        seed.deserialize(value)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.entries.len())
    }
}

struct StructMapAccess {
    fields: std::vec::IntoIter<(String, LocatedValue)>,
    value: Option<LocatedValue>,
}

impl StructMapAccess {
    fn new(fields: Vec<(String, LocatedValue)>) -> Self {
        Self {
            fields: fields.into_iter(),
            value: None,
        }
    }
}

impl<'de> MapAccess<'de> for StructMapAccess {
    type Error = ValueDecodeError;

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
            .ok_or_else(|| ValueDecodeError::custom("struct value requested before field name"))?;
        seed.deserialize(value)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.fields.len())
    }
}

struct NodeEnumAccess {
    variant: String,
    body: VariantShape,
}

impl<'de> EnumAccess<'de> for NodeEnumAccess {
    type Error = ValueDecodeError;
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
    body: VariantShape,
}

impl<'de> VariantAccess<'de> for NodeVariantAccess {
    type Error = ValueDecodeError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        match self.body {
            VariantShape::Unit => Ok(()),
            _ => Err(ValueDecodeError::custom("expected a unit enum variant")),
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.body {
            VariantShape::Newtype(value) => seed.deserialize(*value),
            _ => Err(ValueDecodeError::custom(
                "expected a single-value enum variant",
            )),
        }
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let _ = visitor;
        Err(ValueDecodeError::custom(
            "tuple enum variants are not admitted",
        ))
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let _ = visitor;
        Err(ValueDecodeError::custom(
            "struct enum variants are not admitted",
        ))
    }
}
