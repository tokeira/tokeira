//! The interpreter's runtime value model — generic over a platform host type `H`.
//!
//! One [`Value<H>`] enum spans config values (modelled generically from the
//! `.tkd`'s own `struct`/`enum` AST) and the opaque `H` handles that cross into
//! the platform's [`crate::bridge::HostBridge`]. The config-vs-author split is the
//! dispatch spine: a named type defined *in* the `.tkd` is a [`Value::Struct`]/
//! [`Value::Enum`]; a named author type (kinds, builder handles) is a
//! [`Value::Host`], opaque to the core.
//!
//! This crate names no platform type: the host is the type parameter `H`, and
//! every host operation is routed through the bridge. That is what lets a single
//! interpreter serve every `syn` platform (`compose-syn`, `eks`, …).

use std::collections::BTreeMap;

/// A field map — the generic image of a struct literal's evaluated fields.
pub type FieldMap<H> = BTreeMap<String, Value<H>>;

/// A runtime value. Scalars/collections are obvious; `Struct`/`Enum` model the
/// `.tkd`-defined config types; `Host` wraps the platform's opaque handle.
#[derive(Clone, Debug)]
pub enum Value<H> {
    Unit,
    Bool(bool),
    /// One integer ladder for `u16`/`u32`/`u64` — range-checked at the host edge.
    Int(i128),
    Str(String),
    /// `[..]`, `&[..]`, `vec![..]` — always, including empty.
    Vec(Vec<Value<H>>),
    /// `(a, b)` — env pairs and the like.
    Tuple(Vec<Value<H>>),
    /// `Some(x)` / `None`.
    Opt(Option<Box<Value<H>>>),
    /// A config struct defined in the `.tkd`.
    Struct {
        ty: String,
        fields: FieldMap<H>,
    },
    /// A config enum value defined in the `.tkd`.
    Enum {
        path: EnumPath,
        variant: String,
        body: VariantBody<H>,
    },
    /// An opaque author handle — decomposed only by the platform bridge.
    Host(H),
}

/// The payload of an enum value.
#[derive(Clone, Debug)]
pub enum VariantBody<H> {
    Unit,
    Tuple(Vec<Value<H>>),
    Struct(FieldMap<H>),
}

/// The path an enum literal used — `{ty:"DsqlMode", segments:["DsqlMode"]}` or
/// `{ty:"Storage", segments:["Storage","Dsql"]}`. Carrying the full path
/// disambiguates a config enum from any future host enum sharing a leaf name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnumPath {
    pub ty: String,
    pub segments: Vec<String>,
}

// `PartialEq` on values (config diff domain). Impl'd manually with NO `H:
// PartialEq` bound: host handles are never value-comparable, so the `Host` arm
// trips a debug assert and returns `false` — matching the compose-era
// semantics. Config values being diffed are never hosts.
impl<H> PartialEq for Value<H> {
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
            (
                Enum {
                    path: p1,
                    variant: v1,
                    body: b1,
                },
                Enum {
                    path: p2,
                    variant: v2,
                    body: b2,
                },
            ) => p1 == p2 && v1 == v2 && b1 == b2,
            (Host(_), Host(_)) => {
                debug_assert!(false, "Host values are not comparable");
                false
            }
            _ => false,
        }
    }
}

impl<H> PartialEq for VariantBody<H> {
    fn eq(&self, other: &Self) -> bool {
        use VariantBody::*;
        match (self, other) {
            (Unit, Unit) => true,
            (Tuple(a), Tuple(b)) => a == b,
            (Struct(a), Struct(b)) => a == b,
            _ => false,
        }
    }
}

impl<H> Value<H> {
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

impl<H: std::fmt::Debug> Value<H> {
    /// A string value, for method/argument coercion at the bridge edge.
    pub fn as_str(&self) -> Result<&str, EvalError> {
        match self {
            Value::Str(s) => Ok(s),
            v => Err(EvalError::new(format!("expected string, got {v:?}"))),
        }
    }

    /// A `&[&str]`-shaped argument as owned strings (`module`/`Deployment::new`).
    pub fn as_str_vec(&self) -> Result<Vec<String>, EvalError> {
        match self {
            Value::Vec(items) => items
                .iter()
                .map(|v| v.as_str().map(str::to_string))
                .collect(),
            v => Err(EvalError::new(format!(
                "expected an array of strings, got {v:?}"
            ))),
        }
    }
}

// ── Errors ──────────────────────────────────────────────────────────────────

/// An evaluation error. `span` (when present) points at the offending `.tkd`
/// node; the no-panic invariant means every operator-reachable failure is one of
/// these, never a `panic!`.
#[derive(Debug, Clone)]
pub struct EvalError {
    pub msg: String,
    pub span: Option<proc_macro2::Span>,
}

impl EvalError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self {
            msg: msg.into(),
            span: None,
        }
    }

    pub fn at(msg: impl Into<String>, span: proc_macro2::Span) -> Self {
        Self {
            msg: msg.into(),
            span: Some(span),
        }
    }
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for EvalError {}

// ── FieldMap unpacking (the host-free config-scalar surface) ────────────────

/// Ergonomic, total field extraction for the *host-free* config scalars. Every
/// `take_*` *removes* the key, so a trailing [`expect_empty`](FieldMapExt::expect_empty)
/// catches an unknown/misspelled `.tkd` field — total coverage without reflection.
///
/// Host-typed extraction (kind handles, volumes, module/output refs) is not here:
/// it names platform types and lives platform-side in the bridge.
pub trait FieldMapExt<H> {
    fn take(&mut self, key: &str) -> Result<Value<H>, EvalError>;
    fn take_str(&mut self, key: &str) -> Result<String, EvalError>;
    fn take_bool(&mut self, key: &str) -> Result<bool, EvalError>;
    fn take_u16(&mut self, key: &str) -> Result<u16, EvalError>;
    fn take_u32(&mut self, key: &str) -> Result<u32, EvalError>;
    fn take_opt_str(&mut self, key: &str) -> Result<Option<String>, EvalError>;
    fn take_vec_str(&mut self, key: &str) -> Result<Vec<String>, EvalError>;
    fn take_vec_u16(&mut self, key: &str) -> Result<Vec<u16>, EvalError>;
    fn take_pairs(&mut self, key: &str) -> Result<Vec<(String, String)>, EvalError>;
    /// The enum value at `key`, asserting its declared type is `expect_ty`.
    fn take_enum(&mut self, key: &str, expect_ty: &str) -> Result<String, EvalError>;
    fn expect_empty(&self) -> Result<(), EvalError>;
}

impl<H: std::fmt::Debug> FieldMapExt<H> for FieldMap<H> {
    fn take(&mut self, key: &str) -> Result<Value<H>, EvalError> {
        self.remove(key)
            .ok_or_else(|| EvalError::new(format!("missing field `{key}`")))
    }

    fn take_str(&mut self, key: &str) -> Result<String, EvalError> {
        match self.take(key)? {
            Value::Str(s) => Ok(s),
            v => Err(EvalError::new(format!(
                "field `{key}`: expected string, got {v:?}"
            ))),
        }
    }

    fn take_bool(&mut self, key: &str) -> Result<bool, EvalError> {
        match self.take(key)? {
            Value::Bool(b) => Ok(b),
            v => Err(EvalError::new(format!(
                "field `{key}`: expected bool, got {v:?}"
            ))),
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
                v => Err(EvalError::new(format!(
                    "field `{key}`: expected Option<string>, got Some({v:?})"
                ))),
            },
            v => Err(EvalError::new(format!(
                "field `{key}`: expected Option<string>, got {v:?}"
            ))),
        }
    }

    fn take_vec_str(&mut self, key: &str) -> Result<Vec<String>, EvalError> {
        match self.take(key)? {
            Value::Vec(items) => items
                .into_iter()
                .map(|v| match v {
                    Value::Str(s) => Ok(s),
                    other => Err(EvalError::new(format!(
                        "field `{key}`: expected [string], got element {other:?}"
                    ))),
                })
                .collect(),
            v => Err(EvalError::new(format!(
                "field `{key}`: expected an array, got {v:?}"
            ))),
        }
    }

    fn take_vec_u16(&mut self, key: &str) -> Result<Vec<u16>, EvalError> {
        match self.take(key)? {
            Value::Vec(items) => items
                .into_iter()
                .map(|v| match v {
                    Value::Int(n) => u16::try_from(n).map_err(|_| {
                        EvalError::new(format!("field `{key}`: {n} out of u16 range"))
                    }),
                    other => Err(EvalError::new(format!(
                        "field `{key}`: expected [u16], got element {other:?}"
                    ))),
                })
                .collect(),
            v => Err(EvalError::new(format!(
                "field `{key}`: expected an array, got {v:?}"
            ))),
        }
    }

    fn take_pairs(&mut self, key: &str) -> Result<Vec<(String, String)>, EvalError> {
        match self.take(key)? {
            Value::Vec(items) => items
                .into_iter()
                .map(|v| match v {
                    Value::Tuple(t) if t.len() == 2 => {
                        let mut it = t.into_iter();
                        let k = expect_str(
                            it.next().expect("tuple length 2 checked in this arm"),
                            key,
                        )?;
                        let val = expect_str(
                            it.next().expect("tuple length 2 checked in this arm"),
                            key,
                        )?;
                        Ok((k, val))
                    }
                    other => Err(EvalError::new(format!(
                        "field `{key}`: expected [(string, string)], got element {other:?}"
                    ))),
                })
                .collect(),
            v => Err(EvalError::new(format!(
                "field `{key}`: expected an array, got {v:?}"
            ))),
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
            v => Err(EvalError::new(format!(
                "field `{key}`: expected enum `{expect_ty}`, got {v:?}"
            ))),
        }
    }

    fn expect_empty(&self) -> Result<(), EvalError> {
        if let Some(unknown) = self.keys().next() {
            return Err(EvalError::new(format!("unknown field `{unknown}`")));
        }
        Ok(())
    }
}

fn take_int<H: std::fmt::Debug>(f: &mut FieldMap<H>, key: &str) -> Result<i128, EvalError> {
    match f.take(key)? {
        Value::Int(n) => Ok(n),
        v => Err(EvalError::new(format!(
            "field `{key}`: expected integer, got {v:?}"
        ))),
    }
}

fn expect_str<H: std::fmt::Debug>(v: Value<H>, key: &str) -> Result<String, EvalError> {
    match v {
        Value::Str(s) => Ok(s),
        other => Err(EvalError::new(format!(
            "field `{key}`: expected string, got {other:?}"
        ))),
    }
}
