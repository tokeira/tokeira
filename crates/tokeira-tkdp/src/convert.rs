//! Conversion of the sandbox's structural result into the platform contract.
//!
//! The driver's final expression crosses once as plain data: the operator's
//! config value plus the deployment envelope. This module decodes that
//! `MontyObject` mechanically — dataclasses become `Struct` shapes (the
//! deserializer's enum-position admission supplies variant semantics when a
//! decode target expects an enum) — merges provider defaults under authored
//! kind fields, decodes kinds through the engine functions, and drives
//! `StructuralGraphBuilder` in declaration order.
//!
//! Values carry no ranges — Monty objects do not remember construction sites
//! — so locations come from name correlation: graph invariants make module
//! names, `(module, resource)` ids, and writeback keys unique, and preflight
//! recorded each builder-verb call with a literal name argument. Kind-decode
//! and structural findings land on their declaring call; anything
//! uncorrelated falls back to the `deployment` entrypoint's range.

use monty_types::MontyObject;
use ruff_text_size::TextRange;
use tokeira_platform::{
    author::{LocatedValue, ValueShape},
    definition::FrontendOutput,
    error::GraphError,
    graph::{ResourceReference, StructuralGraphBuilder, WritebackValue},
    kind::{KindFunctions, ProviderKind},
};

use crate::preflight::CallSite;

/// A conversion failure in operator terms.
#[derive(Debug)]
pub struct ConvertError {
    /// Actionable detail.
    pub message: String,
    /// Declaring call's range when correlation found one.
    pub range: TextRange,
}

impl ConvertError {
    fn new(message: impl Into<String>, range: TextRange) -> Self {
        Self {
            message: message.into(),
            range,
        }
    }
}

/// Decodes the driver's result into the frontend output.
pub fn convert<K: ProviderKind + 'static>(
    result: MontyObject,
    kinds: &KindFunctions<K>,
    sites: &[CallSite],
    fallback: TextRange,
) -> Result<FrontendOutput<K>, ConvertError> {
    let site = |verb: &str, name: &str| {
        sites
            .iter()
            .find(|site| site.verb == verb && site.name == name)
            .map_or(fallback, |site| site.range)
    };

    let mut top = expect_dict(result, "structural result", fallback)?;
    let config_value = take(&mut top, "config", fallback)?;
    let deployment = take(&mut top, "deployment", fallback)?;
    let mut envelope = expect_dict(deployment, "deployment envelope", fallback)?;

    let mut builder: StructuralGraphBuilder<K> = StructuralGraphBuilder::new();
    // References returned by `add_resource`, keyed for dependency and
    // writeback lookup. Envelope order is declaration order, and a handle can
    // only reference an earlier declaration, so lookup-before-insert is
    // complete.
    let mut declared: Vec<(String, String, ResourceReference)> = Vec::new();

    for namespace in expect_list(take(&mut envelope, "namespaces", fallback)?, fallback)? {
        builder.add_namespace(expect_string(namespace, "namespace", fallback)?);
    }

    for module in expect_list(take(&mut envelope, "modules", fallback)?, fallback)? {
        let mut module = expect_dict(module, "module entry", fallback)?;
        let name = expect_string(
            take(&mut module, "name", fallback)?,
            "module name",
            fallback,
        )?;
        let deps = expect_list(take(&mut module, "deps", fallback)?, site("module", &name))?
            .into_iter()
            .map(|dep| expect_string(dep, "module dependency", site("module", &name)))
            .collect::<Result<Vec<_>, _>>()?;
        builder.add_module(name, deps);
    }

    for resource in expect_list(take(&mut envelope, "resources", fallback)?, fallback)? {
        let mut resource = expect_dict(resource, "resource entry", fallback)?;
        let id = expect_string(
            take(&mut resource, "id", fallback)?,
            "resource id",
            fallback,
        )?;
        let range = site("resource", &id);
        let module = expect_string(
            take(&mut resource, "module", range)?,
            "resource module",
            range,
        )?;
        let kind_name = expect_string(take(&mut resource, "kind", range)?, "resource kind", range)?;
        let kwargs = expect_dict(take(&mut resource, "kwargs", range)?, "kind kwargs", range)?;
        // Kwargs become a named struct directly (not a generic map), so the
        // provider-defaults merge and struct-shaped decode both apply.
        let authored = LocatedValue::new(ValueShape::Struct {
            name: kind_name.clone(),
            fields: kwargs
                .into_iter()
                .map(|(key, value)| {
                    let key = expect_string(key, "kind keyword", range)?;
                    Ok((key, to_located(value, range)?))
                })
                .collect::<Result<Vec<_>, ConvertError>>()?,
        });
        let merged = merge_defaults((kinds.defaults)(&kind_name), authored, &kind_name);
        let kind = (kinds.decode)(&kind_name, merged).map_err(|error| {
            ConvertError::new(
                format!(
                    "kind `{kind_name}` for resource `{module}/{id}`: {}",
                    error.message
                ),
                range,
            )
        })?;
        let deps = expect_list(take(&mut resource, "deps", range)?, range)?
            .into_iter()
            .map(|dep| {
                let mut dep = expect_dict(dep, "resource dependency", range)?;
                let dep_module =
                    expect_string(take(&mut dep, "module", range)?, "dependency module", range)?;
                let dep_id = expect_string(take(&mut dep, "id", range)?, "dependency id", range)?;
                declared
                    .iter()
                    .find(|(m, i, _)| *m == dep_module && *i == dep_id)
                    .map(|(_, _, reference)| reference.clone())
                    .ok_or_else(|| {
                        ConvertError::new(
                            format!("dependency `{dep_module}/{dep_id}` is not declared"),
                            range,
                        )
                    })
            })
            .collect::<Result<Vec<_>, ConvertError>>()?;
        let reference = builder.add_resource(module.clone(), id.clone(), kind, deps);
        declared.push((module, id, reference));
    }

    for writeback in expect_list(take(&mut envelope, "writebacks", fallback)?, fallback)? {
        let mut writeback = expect_dict(writeback, "writeback entry", fallback)?;
        let key = expect_string(
            take(&mut writeback, "key", fallback)?,
            "writeback key",
            fallback,
        )?;
        let range = site("writeback", &key);
        let mut value = expect_dict(
            take(&mut writeback, "value", range)?,
            "writeback value",
            range,
        )?;
        let value = if let Some(literal) = remove(&mut value, "literal") {
            WritebackValue::Literal(expect_string(literal, "writeback literal", range)?)
        } else if let Some(output) = remove(&mut value, "output") {
            let mut output = expect_dict(output, "writeback output", range)?;
            let module =
                expect_string(take(&mut output, "module", range)?, "output module", range)?;
            let resource = expect_string(
                take(&mut output, "resource", range)?,
                "output resource",
                range,
            )?;
            let name = expect_string(take(&mut output, "output", range)?, "output name", range)?;
            let reference = declared
                .iter()
                .find(|(m, i, _)| *m == module && *i == resource)
                .map(|(_, _, reference)| reference.clone())
                .ok_or_else(|| {
                    ConvertError::new(
                        format!("output source `{module}/{resource}` is not declared"),
                        range,
                    )
                })?;
            let output = builder
                .output(&reference, &name)
                .map_err(|error| located_graph_error(error, sites, range))?;
            WritebackValue::Output(output)
        } else {
            return Err(ConvertError::new(
                "writeback value must be a literal or an output",
                range,
            ));
        };
        builder.add_writeback(key, value);
    }

    let graph = builder
        .finish()
        .map_err(|error| located_graph_error(error, sites, fallback))?;
    Ok(FrontendOutput {
        config: to_located(config_value, fallback)?,
        graph,
    })
}

/// Best-effort location for a graph error via the names its variants carry.
fn located_graph_error(error: GraphError, sites: &[CallSite], fallback: TextRange) -> ConvertError {
    let named = |name: &str| {
        sites
            .iter()
            .find(|site| site.name == name)
            .map(|site| site.range)
    };
    let range = match &error {
        GraphError::UnknownResource { resource, .. } => named(resource),
        GraphError::UnknownOutput { .. } => None,
        GraphError::Invalid(_) => None,
    }
    .unwrap_or(fallback);
    ConvertError::new(error.to_string(), range)
}

/// Converts a sandbox value to a host-free located value (ranges absent:
/// Monty objects carry no construction sites — the documented v1 boundary).
pub fn to_located(value: MontyObject, at: TextRange) -> Result<LocatedValue, ConvertError> {
    let shape = match value {
        MontyObject::None => ValueShape::Option(None),
        MontyObject::Bool(value) => ValueShape::Bool(value),
        MontyObject::Int(value) => ValueShape::Integer(i128::from(value)),
        MontyObject::Float(value) => ValueShape::Float(value),
        MontyObject::String(value) => ValueShape::String(value),
        MontyObject::List(items) | MontyObject::Tuple(items) => ValueShape::Sequence(
            items
                .into_iter()
                .map(|item| to_located(item, at))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        MontyObject::Dict(pairs) => {
            let mut pairs: Vec<(MontyObject, MontyObject)> = pairs.into_iter().collect();
            // The facade exporter tags dataclass instances (native in-sandbox
            // dataclasses cross the boundary as opaque `Repr` values — probed
            // at the pin — so the sandbox exports structure itself).
            let tagged = pairs.iter().position(
                |(k, _)| matches!(k, MontyObject::String(s) if s == "__tokeira_internal_struct"),
            );
            if let Some(index) = tagged {
                let (_, name) = pairs.remove(index);
                let name = expect_string(name, "struct tag", at)?;
                let fields = take(&mut pairs, "fields", at)?;
                let fields = expect_list(fields, at)?
                    .into_iter()
                    .map(|entry| {
                        let mut entry = expect_list(entry, at)?;
                        if entry.len() != 2 {
                            return Err(ConvertError::new("malformed struct field entry", at));
                        }
                        let value = entry.pop().expect("length checked");
                        let key =
                            expect_string(entry.pop().expect("length checked"), "field name", at)?;
                        Ok((key, to_located(value, at)?))
                    })
                    .collect::<Result<Vec<_>, ConvertError>>()?;
                return Ok(LocatedValue::new(ValueShape::Struct { name, fields }));
            }
            return Ok(LocatedValue::new(ValueShape::Map(
                pairs
                    .into_iter()
                    .map(|(key, value)| Ok((to_located(key, at)?, to_located(value, at)?)))
                    .collect::<Result<Vec<_>, ConvertError>>()?,
            )));
        }
        MontyObject::Dataclass {
            name,
            field_names,
            attrs,
            ..
        } => {
            // Declared fields in definition order; extra instance attrs are
            // not part of the authored value shape.
            let mut attrs: Vec<(MontyObject, MontyObject)> = attrs.into_iter().collect();
            let mut fields = Vec::with_capacity(field_names.len());
            for field in field_names {
                let index = attrs
                    .iter()
                    .position(|(k, _)| matches!(k, MontyObject::String(s) if *s == field));
                let Some(index) = index else {
                    return Err(ConvertError::new(
                        format!("dataclass `{name}` is missing declared field `{field}`"),
                        at,
                    ));
                };
                let (_, value) = attrs.remove(index);
                fields.push((field, to_located(value, at)?));
            }
            ValueShape::Struct { name, fields }
        }
        MontyObject::Repr(rendered) => {
            return Err(ConvertError::new(
                format!(
                    "value `{rendered}` did not export structurally; only dataclasses, \
                     scalars, strings, lists, and dicts are admissible in a definition result"
                ),
                at,
            ));
        }
        other => {
            return Err(ConvertError::new(
                format!(
                    "value of kind `{}` is not admissible in a definition result",
                    kind_of(&other)
                ),
                at,
            ));
        }
    };
    Ok(LocatedValue::new(shape))
}

/// Provider defaults under authored fields: authored values win, defaulted
/// fields the author omitted survive, authored-only fields append.
fn merge_defaults(
    defaults: Option<LocatedValue>,
    authored: LocatedValue,
    kind_name: &str,
) -> LocatedValue {
    let Some(LocatedValue {
        value: ValueShape::Struct {
            fields: default_fields,
            ..
        },
        ..
    }) = defaults
    else {
        return authored;
    };
    let LocatedValue {
        value: ValueShape::Struct {
            fields: authored_fields,
            ..
        },
        range,
    } = authored
    else {
        return authored;
    };
    let mut merged = default_fields;
    for (name, value) in authored_fields {
        if let Some(slot) = merged.iter_mut().find(|(existing, _)| *existing == name) {
            slot.1 = value;
        } else {
            merged.push((name, value));
        }
    }
    LocatedValue {
        value: ValueShape::Struct {
            name: kind_name.to_string(),
            fields: merged,
        },
        range,
    }
}

fn kind_of(value: &MontyObject) -> String {
    format!("{value:?}")
        .split([' ', '(', '{'])
        .next()
        .unwrap_or("unsupported")
        .to_string()
}

type Dict = Vec<(MontyObject, MontyObject)>;

fn expect_dict(value: MontyObject, what: &str, at: TextRange) -> Result<Dict, ConvertError> {
    match value {
        MontyObject::Dict(pairs) => Ok(pairs.into_iter().collect()),
        _ => Err(ConvertError::new(format!("{what} must be a dict"), at)),
    }
}

fn expect_list(value: MontyObject, at: TextRange) -> Result<Vec<MontyObject>, ConvertError> {
    match value {
        MontyObject::List(items) | MontyObject::Tuple(items) => Ok(items),
        _ => Err(ConvertError::new("expected a list", at)),
    }
}

fn expect_string(value: MontyObject, what: &str, at: TextRange) -> Result<String, ConvertError> {
    match value {
        MontyObject::String(value) => Ok(value),
        _ => Err(ConvertError::new(format!("{what} must be a string"), at)),
    }
}

fn take(dict: &mut Dict, key: &str, at: TextRange) -> Result<MontyObject, ConvertError> {
    remove(dict, key).ok_or_else(|| ConvertError::new(format!("missing `{key}` entry"), at))
}

fn remove(dict: &mut Dict, key: &str) -> Option<MontyObject> {
    let index = dict
        .iter()
        .position(|(k, _)| matches!(k, MontyObject::String(s) if s == key))?;
    Some(dict.remove(index).1)
}
