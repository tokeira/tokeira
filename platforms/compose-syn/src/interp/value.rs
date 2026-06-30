//! The interpreter's runtime value model.
//!
//! One [`Value`] enum spans config values (modelled generically from the `.tkd`'s
//! own `struct`/`enum` AST) and the opaque [`HostObj`] handles that cross into the
//! author bridge ([`super::registry`]). The config-vs-author split is the dispatch
//! spine: a named type defined *in* the `.tkd` is a [`Value::Struct`]/[`Value::Enum`];
//! a named author type (kinds, builder handles) is a [`Value::Host`].
//!
//! This module + `registry` are the *host adapter* — the only interpreter modules
//! permitted to name `crate::builder`/`crate::kinds`.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::builder::{self, Kind, ModuleRef, Output, ResourceRef, Vol};
use crate::context::Cx;
use crate::kinds;

/// A field map — the generic image of a struct literal's evaluated fields.
pub type FieldMap = BTreeMap<String, Value>;

/// A runtime value. Scalars/collections are obvious; `Struct`/`Enum` model the
/// `.tkd`-defined config types; `Host` wraps opaque author objects.
#[derive(Clone, Debug)]
pub enum Value {
    Unit,
    Bool(bool),
    /// One integer ladder for `u16`/`u32`/`u64` — range-checked at the host edge.
    Int(i128),
    Str(String),
    /// `[..]`, `&[..]`, `vec![..]` — always, including empty.
    Vec(Vec<Value>),
    /// `(a, b)` — env pairs and the like.
    Tuple(Vec<Value>),
    /// `Some(x)` / `None`.
    Opt(Option<Box<Value>>),
    /// A config struct defined in the `.tkd`.
    Struct { ty: String, fields: FieldMap },
    /// A config enum value defined in the `.tkd`.
    Enum { path: EnumPath, variant: String, body: VariantBody },
    /// An opaque author handle (builder/kind/cx/vol).
    Host(HostObj),
}

/// The payload of an enum value.
#[derive(Clone, Debug, PartialEq)]
pub enum VariantBody {
    Unit,
    Tuple(Vec<Value>),
    Struct(FieldMap),
}

/// The path an enum literal used — `{ty:"DsqlMode", segments:["DsqlMode"]}` or
/// `{ty:"Storage", segments:["Storage","Dsql"]}`. Carrying the full path
/// disambiguates a config enum from any future host enum sharing a leaf name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnumPath {
    pub ty: String,
    pub segments: Vec<String>,
}

/// The closed set of opaque author handles. Dispatch keys on [`HostKind`], so a
/// receiver-type error is structural, never a runtime downcast failure.
#[derive(Clone)]
pub enum HostObj {
    Deployment(Rc<RefCell<builder::Deployment>>),
    Module(ModuleRef),
    Resource(ResourceRef),
    Output(Output),
    /// A constructed-but-unplaced kind; consumed once when handed to the builder.
    Kind(Rc<RefCell<Option<HostKindVal>>>),
    /// A logical volume anchor produced by `cx.state/config/docker_sock`.
    Vol(Vol),
    /// The injected runtime context.
    Cx(Rc<Cx>),
}

/// A constructed kind awaiting placement. `Service` is concrete (the builder's
/// `service()` takes `kinds::Service`); every other kind is a `Box<dyn Kind>`
/// placed via `resource_dyn`.
pub enum HostKindVal {
    Boxed(Box<dyn Kind>),
    Service(kinds::Service),
}

/// The dispatch tag for a [`HostObj`] — the method-table key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HostKind {
    Deployment,
    Module,
    Resource,
    Output,
    Kind,
    Vol,
    Cx,
}

impl HostObj {
    pub fn kind(&self) -> HostKind {
        match self {
            HostObj::Deployment(_) => HostKind::Deployment,
            HostObj::Module(_) => HostKind::Module,
            HostObj::Resource(_) => HostKind::Resource,
            HostObj::Output(_) => HostKind::Output,
            HostObj::Kind(_) => HostKind::Kind,
            HostObj::Vol(_) => HostKind::Vol,
            HostObj::Cx(_) => HostKind::Cx,
        }
    }
}

impl std::fmt::Debug for HostObj {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Host::{:?}", self.kind())
    }
}

/// `Value: PartialEq` backs the `#[create]` retarget diff (config values only).
/// Host handles are never value-comparable: comparing them trips a debug assert
/// and returns `false`, and the diff path asserts no host appears in its domain.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (Unit, Unit) => true,
            (Bool(a), Bool(b)) => a == b,
            (Int(a), Int(b)) => a == b,
            (Str(a), Str(b)) => a == b,
            (Vec(a), Vec(b)) => a == b,
            (Tuple(a), Tuple(b)) => a == b,
            (Opt(a), Opt(b)) => a == b,
            (Struct { ty: t1, fields: f1 }, Struct { ty: t2, fields: f2 }) => t1 == t2 && f1 == f2,
            (Enum { path: p1, variant: v1, body: b1 }, Enum { path: p2, variant: v2, body: b2 }) => {
                p1 == p2 && v1 == v2 && b1 == b2
            }
            (Host(_), Host(_)) => {
                debug_assert!(false, "Host values are not comparable");
                false
            }
            _ => false,
        }
    }
}

impl Value {
    /// Does this value (transitively) contain a host handle? Guards the
    /// `#[create]`/`#[require]` diff domain, which must be config-only.
    pub fn contains_host(&self) -> bool {
        match self {
            Value::Host(_) => true,
            Value::Vec(xs) | Value::Tuple(xs) => xs.iter().any(Value::contains_host),
            Value::Opt(Some(b)) => b.contains_host(),
            Value::Struct { fields, .. } => fields.values().any(Value::contains_host),
            Value::Enum { body, .. } => match body {
                VariantBody::Tuple(xs) => xs.iter().any(Value::contains_host),
                VariantBody::Struct(fs) => fs.values().any(Value::contains_host),
                VariantBody::Unit => false,
            },
            _ => false,
        }
    }
}

// ── Errors ──────────────────────────────────────────────────────────────────

/// An evaluation error. `span` (when present) points at the offending `.tkd`
/// node; the no-panic invariant means every operator-reachable failure is one
/// of these, never a `panic!`.
#[derive(Debug, Clone)]
pub struct EvalError {
    pub msg: String,
    pub span: Option<proc_macro2::Span>,
}

impl EvalError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self { msg: msg.into(), span: None }
    }

    pub fn at(msg: impl Into<String>, span: proc_macro2::Span) -> Self {
        Self { msg: msg.into(), span: Some(span) }
    }
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for EvalError {}

// ── FieldMap unpacking (the per-kind ctor surface) ──────────────────────────

/// Ergonomic, total field extraction for the kind constructors. Every `take_*`
/// *removes* the key, so a trailing [`expect_empty`](FieldMapExt::expect_empty)
/// catches an unknown/misspelled `.tkd` field — total coverage without reflection.
pub trait FieldMapExt {
    fn take(&mut self, key: &str) -> Result<Value, EvalError>;
    fn take_str(&mut self, key: &str) -> Result<String, EvalError>;
    fn take_bool(&mut self, key: &str) -> Result<bool, EvalError>;
    fn take_u16(&mut self, key: &str) -> Result<u16, EvalError>;
    fn take_u32(&mut self, key: &str) -> Result<u32, EvalError>;
    fn take_opt_str(&mut self, key: &str) -> Result<Option<String>, EvalError>;
    fn take_vec_str(&mut self, key: &str) -> Result<Vec<String>, EvalError>;
    fn take_vec_u16(&mut self, key: &str) -> Result<Vec<u16>, EvalError>;
    fn take_pairs(&mut self, key: &str) -> Result<Vec<(String, String)>, EvalError>;
    fn take_vols(&mut self, key: &str) -> Result<Vec<Vol>, EvalError>;
    /// The enum value at `key`, asserting its declared type is `expect_ty`.
    fn take_enum(&mut self, key: &str, expect_ty: &str) -> Result<String, EvalError>;
    fn expect_empty(&self) -> Result<(), EvalError>;
}

impl FieldMapExt for FieldMap {
    fn take(&mut self, key: &str) -> Result<Value, EvalError> {
        self.remove(key)
            .ok_or_else(|| EvalError::new(format!("missing field `{key}`")))
    }

    fn take_str(&mut self, key: &str) -> Result<String, EvalError> {
        match self.take(key)? {
            Value::Str(s) => Ok(s),
            v => Err(EvalError::new(format!("field `{key}`: expected string, got {v:?}"))),
        }
    }

    fn take_bool(&mut self, key: &str) -> Result<bool, EvalError> {
        match self.take(key)? {
            Value::Bool(b) => Ok(b),
            v => Err(EvalError::new(format!("field `{key}`: expected bool, got {v:?}"))),
        }
    }

    fn take_u16(&mut self, key: &str) -> Result<u16, EvalError> {
        let n = take_int(self, key)?;
        u16::try_from(n).map_err(|_| EvalError::new(format!("field `{key}`: {n} out of u16 range")))
    }

    fn take_u32(&mut self, key: &str) -> Result<u32, EvalError> {
        let n = take_int(self, key)?;
        u32::try_from(n).map_err(|_| EvalError::new(format!("field `{key}`: {n} out of u32 range")))
    }

    fn take_opt_str(&mut self, key: &str) -> Result<Option<String>, EvalError> {
        match self.take(key)? {
            Value::Opt(None) => Ok(None),
            Value::Opt(Some(b)) => match *b {
                Value::Str(s) => Ok(Some(s)),
                v => Err(EvalError::new(format!("field `{key}`: expected Option<string>, got Some({v:?})"))),
            },
            v => Err(EvalError::new(format!("field `{key}`: expected Option<string>, got {v:?}"))),
        }
    }

    fn take_vec_str(&mut self, key: &str) -> Result<Vec<String>, EvalError> {
        match self.take(key)? {
            Value::Vec(items) => items
                .into_iter()
                .map(|v| match v {
                    Value::Str(s) => Ok(s),
                    other => Err(EvalError::new(format!("field `{key}`: expected [string], got element {other:?}"))),
                })
                .collect(),
            v => Err(EvalError::new(format!("field `{key}`: expected an array, got {v:?}"))),
        }
    }

    fn take_vec_u16(&mut self, key: &str) -> Result<Vec<u16>, EvalError> {
        match self.take(key)? {
            Value::Vec(items) => items
                .into_iter()
                .map(|v| match v {
                    Value::Int(n) => u16::try_from(n)
                        .map_err(|_| EvalError::new(format!("field `{key}`: {n} out of u16 range"))),
                    other => Err(EvalError::new(format!("field `{key}`: expected [u16], got element {other:?}"))),
                })
                .collect(),
            v => Err(EvalError::new(format!("field `{key}`: expected an array, got {v:?}"))),
        }
    }

    fn take_pairs(&mut self, key: &str) -> Result<Vec<(String, String)>, EvalError> {
        match self.take(key)? {
            Value::Vec(items) => items
                .into_iter()
                .map(|v| match v {
                    Value::Tuple(t) if t.len() == 2 => {
                        let mut it = t.into_iter();
                        let k = expect_str(it.next().unwrap(), key)?;
                        let val = expect_str(it.next().unwrap(), key)?;
                        Ok((k, val))
                    }
                    other => Err(EvalError::new(format!("field `{key}`: expected [(string, string)], got element {other:?}"))),
                })
                .collect(),
            v => Err(EvalError::new(format!("field `{key}`: expected an array, got {v:?}"))),
        }
    }

    fn take_vols(&mut self, key: &str) -> Result<Vec<Vol>, EvalError> {
        match self.take(key)? {
            Value::Vec(items) => items
                .into_iter()
                .map(|v| match v {
                    Value::Host(HostObj::Vol(vol)) => Ok(vol),
                    other => Err(EvalError::new(format!("field `{key}`: expected [Vol], got element {other:?}"))),
                })
                .collect(),
            v => Err(EvalError::new(format!("field `{key}`: expected an array, got {v:?}"))),
        }
    }

    fn take_enum(&mut self, key: &str, expect_ty: &str) -> Result<String, EvalError> {
        match self.take(key)? {
            Value::Enum { path, variant, .. } => {
                if path.ty != expect_ty {
                    return Err(EvalError::new(format!(
                        "field `{key}`: expected enum `{expect_ty}`, got `{}`",
                        path.ty
                    )));
                }
                Ok(variant)
            }
            v => Err(EvalError::new(format!("field `{key}`: expected enum `{expect_ty}`, got {v:?}"))),
        }
    }

    fn expect_empty(&self) -> Result<(), EvalError> {
        if let Some(unknown) = self.keys().next() {
            return Err(EvalError::new(format!("unknown field `{unknown}`")));
        }
        Ok(())
    }
}

fn take_int(f: &mut FieldMap, key: &str) -> Result<i128, EvalError> {
    match f.take(key)? {
        Value::Int(n) => Ok(n),
        v => Err(EvalError::new(format!("field `{key}`: expected integer, got {v:?}"))),
    }
}

fn expect_str(v: Value, key: &str) -> Result<String, EvalError> {
    match v {
        Value::Str(s) => Ok(s),
        other => Err(EvalError::new(format!("field `{key}`: expected string, got {other:?}"))),
    }
}

// ── Argument coercion (the method-shim surface) ─────────────────────────────

impl Value {
    pub fn as_str(&self) -> Result<&str, EvalError> {
        match self {
            Value::Str(s) => Ok(s),
            v => Err(EvalError::new(format!("expected string, got {v:?}"))),
        }
    }

    /// A `&[&str]`-shaped argument as owned strings (for `module`/`Deployment::new`).
    pub fn as_str_vec(&self) -> Result<Vec<String>, EvalError> {
        match self {
            Value::Vec(items) => items
                .iter()
                .map(|v| v.as_str().map(str::to_string))
                .collect(),
            v => Err(EvalError::new(format!("expected an array of strings, got {v:?}"))),
        }
    }

    pub fn as_host_module(&self) -> Result<ModuleRef, EvalError> {
        match self {
            Value::Host(HostObj::Module(m)) => Ok(m.clone()),
            v => Err(EvalError::new(format!("expected a module handle, got {v:?}"))),
        }
    }

    pub fn as_host_output(&self) -> Result<Output, EvalError> {
        match self {
            Value::Host(HostObj::Output(o)) => Ok(o.clone()),
            v => Err(EvalError::new(format!("expected an output handle, got {v:?}"))),
        }
    }

    pub fn as_vol(&self) -> Result<Vol, EvalError> {
        match self {
            Value::Host(HostObj::Vol(v)) => Ok(v.clone()),
            v => Err(EvalError::new(format!("expected a volume, got {v:?}"))),
        }
    }

    /// Consume an unplaced `Box<dyn Kind>` handle (take-once).
    pub fn take_boxed_kind(&self) -> Result<Box<dyn Kind>, EvalError> {
        match self {
            Value::Host(HostObj::Kind(cell)) => match cell.borrow_mut().take() {
                Some(HostKindVal::Boxed(b)) => Ok(b),
                Some(HostKindVal::Service(_)) => {
                    Err(EvalError::new("expected a resource kind, got a service"))
                }
                None => Err(EvalError::new("kind handle already consumed")),
            },
            v => Err(EvalError::new(format!("expected a kind handle, got {v:?}"))),
        }
    }

    /// Consume an unplaced `kinds::Service` handle (take-once).
    pub fn take_service(&self) -> Result<kinds::Service, EvalError> {
        match self {
            Value::Host(HostObj::Kind(cell)) => match cell.borrow_mut().take() {
                Some(HostKindVal::Service(s)) => Ok(s),
                Some(HostKindVal::Boxed(_)) => {
                    Err(EvalError::new("expected a service, got a resource kind"))
                }
                None => Err(EvalError::new("service handle already consumed")),
            },
            v => Err(EvalError::new(format!("expected a service handle, got {v:?}"))),
        }
    }
}

/// Wrap a boxed kind as a take-once host handle.
pub fn host_boxed(kind: Box<dyn Kind>) -> HostObj {
    HostObj::Kind(Rc::new(RefCell::new(Some(HostKindVal::Boxed(kind)))))
}

/// Wrap a concrete service as a take-once host handle.
pub fn host_service(svc: kinds::Service) -> HostObj {
    HostObj::Kind(Rc::new(RefCell::new(Some(HostKindVal::Service(svc)))))
}
