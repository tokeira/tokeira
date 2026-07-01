//! The `.tkd` interpreter — parses the operator's deployment definition (Rust
//! syntax, via `syn`) and walks it into a real [`crate::builder::Deployment`],
//! byte-identical to the compiled `definition.rs` (Proposal 004).
//!
//! Module boundary (for the eventual lift into a generic `tokeira-tkd` crate):
//! only [`value`] and [`registry`] — the **host adapter** — may name
//! `crate::builder`/`crate::kinds`. The pure passes (`eval`/`schema`) stay
//! engine-agnostic.

pub mod admission;
pub mod eval;
pub mod registry;
pub mod schema;
pub mod subset;
pub mod value;

use std::{cell::RefCell, rc::Rc};

use crate::{builder::Deployment, context::Cx};

pub use value::{EvalError, Value};

/// Parse and interpret a `.tkd` deployment definition, producing the realized
/// [`Deployment`] (byte-identical to the compiled `definition.rs`) and the
/// resolved config value the operator authored. The subset is enforced *before*
/// evaluation, so a definition outside the allow-list is rejected, never run.
pub fn interpret(src: &str, cx: &Cx) -> Result<(Deployment, Value), EvalError> {
    let file = syn::parse_file(src).map_err(|e| EvalError::new(format!("parse error: {e}")))?;
    let (types, fns) = schema::collect(&file)?;
    let reg = registry::Registry::compose();
    subset::check(&file, &reg, &types).map_err(|d| {
        EvalError::new(format!(
            "definition is outside the interpreted subset:\n{d}"
        ))
    })?;
    let interp = eval::Interp {
        reg: &reg,
        types: &types,
        fns: &fns,
        cx,
    };

    let cfg = interp.eval_fn("config", vec![])?;
    // config() is the operator surface and must stay host-free: a kind/builder
    // value (an author handle) must never materialize inside config data, or the
    // admission diff would carry an incomparable host. Kinds belong to deployment().
    if cfg.contains_host() {
        return Err(EvalError::new(
            "config() must be host-free; an author kind cannot appear in the config surface",
        ));
    }
    let adm = admission::extract(&file);
    admission::check_requires(&interp, &adm, &cfg)?;
    let dep = eval_deployment(&interp, cfg.clone())?;
    Ok((dep, cfg))
}

/// Check whether moving from `old` config to `new` config is a *retarget* —
/// i.e. whether any `#[create]` (create-time-immutable) field changed. The apply
/// layer calls this against the recorded config before reconciling.
pub fn retarget_check(src: &str, old: &Value, new: &Value) -> Result<(), Vec<String>> {
    let file = syn::parse_file(src).map_err(|e| vec![format!("parse error: {e}")])?;
    let adm = admission::extract(&file);
    admission::check_retarget(&adm, old, new).map_err(|e| vec![e.msg])
}

/// Validate a `.tkd` against the interpreted subset, returning all violation
/// messages (no parse/eval). The allow-list is reject-by-default.
pub fn validate(src: &str) -> Result<(), Vec<String>> {
    let file = syn::parse_file(src).map_err(|e| vec![format!("parse error: {e}")])?;
    let (types, _fns) = schema::collect(&file).map_err(|e| vec![e.msg])?;
    let reg = registry::Registry::compose();
    subset::check(&file, &reg, &types).map_err(Diagnostics::into_messages)
}

use subset::Diagnostics;

/// Evaluate just `deployment(cfg, cx)` against a (possibly operator-edited)
/// config value. Used by [`interpret`] and by tests that vary the config.
pub fn eval_deployment(interp: &eval::Interp, cfg: Value) -> Result<Deployment, EvalError> {
    let cx_val = Value::Host(value::HostObj::Cx(Rc::new(interp.cx.clone())));
    let dep_val = interp.eval_fn("deployment", vec![cfg, cx_val])?;
    match dep_val {
        Value::Host(value::HostObj::Deployment(rc)) => Rc::try_unwrap(rc)
            .map(RefCell::into_inner)
            .map_err(|_| EvalError::new("the deployment handle is still referenced at return")),
        other => Err(EvalError::new(format!(
            "deployment() must return a Deployment, got {other:?}"
        ))),
    }
}
