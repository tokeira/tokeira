//! Config admission (Proposal 004 §8): `#[create]` create-time-immutability and
//! `#[require(expr)]` constraints. Both gate the config *before* `deployment()`;
//! neither is visible to the structure. Engine-agnostic — names only `syn`, the
//! value model, and the evaluator.

use super::eval::Interp;
use super::value::{EvalError, FieldMap, Value, VariantBody};

/// A `#[require(expr)]` clause attached to a config type.
pub struct RequireClause {
    pub scope: String,
    pub expr: syn::Expr,
}

/// The admission schema extracted from the `.tkd`'s attributes.
pub struct Admission {
    /// `(type, field)` pairs marked `#[create]` (create-time-immutable).
    pub creates: Vec<(String, String)>,
    /// `#[require(...)]` constraint clauses.
    pub requires: Vec<RequireClause>,
}

/// Extract `#[create]`/`#[require]` from a parsed file.
pub fn extract(file: &syn::File) -> Admission {
    let mut creates = Vec::new();
    let mut requires = Vec::new();
    for item in &file.items {
        match item {
            syn::Item::Struct(s) => {
                let ty = s.ident.to_string();
                push_requires(&s.attrs, &ty, &mut requires);
                if let syn::Fields::Named(named) = &s.fields {
                    for f in &named.named {
                        if has_attr(&f.attrs, "create")
                            && let Some(id) = &f.ident
                        {
                            creates.push((ty.clone(), id.to_string()));
                        }
                    }
                }
            }
            syn::Item::Enum(e) => push_requires(&e.attrs, &e.ident.to_string(), &mut requires),
            syn::Item::Impl(im) => {
                if let syn::Type::Path(tp) = &*im.self_ty
                    && let Some(seg) = tp.path.segments.last()
                {
                    push_requires(&im.attrs, &seg.ident.to_string(), &mut requires);
                }
            }
            _ => {}
        }
    }
    Admission { creates, requires }
}

fn has_attr(attrs: &[syn::Attribute], name: &str) -> bool {
    attrs.iter().any(|a| a.path().is_ident(name))
}

fn push_requires(attrs: &[syn::Attribute], scope: &str, out: &mut Vec<RequireClause>) {
    for a in attrs {
        if a.path().is_ident("require")
            && let Ok(expr) = a.parse_args::<syn::Expr>()
        {
            out.push(RequireClause { scope: scope.to_string(), expr });
        }
    }
}

/// `#[create]` retarget check: every create-time-immutable field must be
/// structurally unchanged between the recorded config and the new one. A change
/// is a *retarget* — refused, not reconciled (003 §7).
pub fn check_retarget(adm: &Admission, old: &Value, new: &Value) -> Result<(), EvalError> {
    // The diff domain must be config-only. A host handle here means a kind leaked
    // into config (rejected at interpret-time, but guard anyway so this can never
    // panic on an adversarial recorded config).
    if old.contains_host() || new.contains_host() {
        return Err(EvalError::new(
            "config values must be host-free; a kind leaked into the config surface",
        ));
    }
    diff(&adm.creates, old, new)
}

/// Recurse through the config value in lockstep, flagging any changed `#[create]`
/// field. Mirrors `collect_struct_fields`' structural reach so a create field
/// nested inside an enum variant / Option / Vec is not missed.
fn diff(creates: &[(String, String)], old: &Value, new: &Value) -> Result<(), EvalError> {
    match (old, new) {
        (Value::Struct { ty: t1, fields: f1 }, Value::Struct { ty: t2, fields: f2 }) if t1 == t2 => {
            for (ty, field) in creates.iter().filter(|(ty, _)| ty == t1) {
                if f1.get(field) != f2.get(field) {
                    return Err(EvalError::new(format!(
                        "`{ty}.{field}` is create-time-immutable; changing it is a retarget, refused (not reconciled)"
                    )));
                }
            }
            for (k, av) in f1 {
                if let Some(bv) = f2.get(k) {
                    diff(creates, av, bv)?;
                }
            }
        }
        (Value::Enum { variant: v1, body: b1, .. }, Value::Enum { variant: v2, body: b2, .. })
            if v1 == v2 =>
        {
            match (b1, b2) {
                (VariantBody::Struct(f1), VariantBody::Struct(f2)) => {
                    for (k, av) in f1 {
                        if let Some(bv) = f2.get(k) {
                            diff(creates, av, bv)?;
                        }
                    }
                }
                (VariantBody::Tuple(x1), VariantBody::Tuple(x2)) => {
                    for (av, bv) in x1.iter().zip(x2) {
                        diff(creates, av, bv)?;
                    }
                }
                _ => {}
            }
        }
        (Value::Opt(Some(a)), Value::Opt(Some(b))) => diff(creates, a, b)?,
        (Value::Vec(x1), Value::Vec(x2)) | (Value::Tuple(x1), Value::Tuple(x2)) => {
            for (av, bv) in x1.iter().zip(x2) {
                diff(creates, av, bv)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// `#[require]` check: each clause's expression, evaluated against the config in
/// the scope type's field environment, must be `true`.
pub fn check_requires(interp: &Interp, adm: &Admission, cfg: &Value) -> Result<(), EvalError> {
    for clause in &adm.requires {
        let mut instances = Vec::new();
        collect_struct_fields(cfg, &clause.scope, &mut instances);
        if instances.is_empty() {
            return Err(EvalError::new(format!(
                "#[require] scope `{}` is not present in the resolved config",
                clause.scope
            )));
        }
        // The constraint must hold for EVERY instance of the scope type.
        for fields in instances {
            match interp.eval_with_fields(&clause.expr, fields)? {
                Value::Bool(true) => {}
                _ => {
                    return Err(EvalError::new(format!(
                        "config constraint failed: #[require] on `{}`",
                        clause.scope
                    )))
                }
            }
        }
    }
    Ok(())
}

/// Collect the fields of EVERY `Struct { ty == scope, .. }` reachable in `cfg`.
fn collect_struct_fields<'a>(v: &'a Value, scope: &str, out: &mut Vec<&'a FieldMap>) {
    match v {
        Value::Struct { ty, fields } => {
            if ty == scope {
                out.push(fields);
            }
            for sub in fields.values() {
                collect_struct_fields(sub, scope, out);
            }
        }
        Value::Opt(Some(b)) => collect_struct_fields(b, scope, out),
        Value::Vec(xs) | Value::Tuple(xs) => {
            for x in xs {
                collect_struct_fields(x, scope, out);
            }
        }
        Value::Enum { body, .. } => match body {
            VariantBody::Struct(f) => {
                for x in f.values() {
                    collect_struct_fields(x, scope, out);
                }
            }
            VariantBody::Tuple(xs) => {
                for x in xs {
                    collect_struct_fields(x, scope, out);
                }
            }
            VariantBody::Unit => {}
        },
        _ => {}
    }
}
