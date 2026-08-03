//! `tokeira-tkd` — the `.tkd` definition frontend and its sandboxed `syn` interpreter.
//!
//! Parses a `.tkd` (Rust syntax, via `syn`), enforces the interpreted subset
//! (reject-by-default allow-list), and drives the language-neutral
//! [`TkdFrontend`] adapter. Evaluator handles remain private to this crate; the
//! shared platform boundary receives one completed transient definition.
//!
//! The passes: [`schema`] (type/fn tables + `#[create]`/`#[require]`), [`subset`]
//! (the allow-list), [`eval`] (the tree walk), [`admission`] (retarget + require).
//! Platform crates supply bindings and never receive parser or runtime values.

pub mod admission;
pub mod bridge;
pub mod eval;
mod framework;
pub mod schema;
pub mod subset;
pub mod value;

pub use bridge::HostBridge;
pub use framework::{TkdFrontend, frontend};
pub use subset::{Diagnostic, Diagnostics};
pub use value::{EnumPath, EvalError, FieldMap, FieldMapExt, Value, VariantBody};

/// Parse and interpret a `.tkd` deployment definition, producing the platform's
/// realized deployment ([`HostBridge::Output`]) and the resolved config value the
/// operator authored. The subset is enforced *before* evaluation, so a definition
/// outside the allow-list is rejected, never run.
pub fn interpret<B: HostBridge>(
    src: &str,
    bridge: &B,
    cx: &B::Cx,
) -> Result<(B::Output, Value<B::Host>), EvalError> {
    let file = syn::parse_file(src).map_err(|e| EvalError::new(located_parse_error(&e)))?;
    let (types, fns) = schema::collect(&file)?;
    subset::check(&file, bridge, &types).map_err(|d| {
        let span = d.0.first().map(|diagnostic| diagnostic.span);
        EvalError::new(format!(
            "definition is outside the interpreted subset:\n{d}"
        ))
        .with_optional_span(span)
    })?;
    let interp = eval::Interp {
        bridge,
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

/// Validate a `.tkd` against the interpreted subset, returning all violation
/// messages (no parse/eval). The allow-list is reject-by-default.
pub fn validate<B: HostBridge>(src: &str, bridge: &B) -> Result<(), Vec<String>> {
    let file = syn::parse_file(src).map_err(|e| vec![located_parse_error(&e)])?;
    let (types, _fns) = schema::collect(&file).map_err(|e| vec![e.msg])?;
    subset::check(&file, bridge, &types).map_err(Diagnostics::into_messages)
}

/// Locate a parse failure for the operator: syn's message alone ("expected
/// `,`") is useless in a definition of any size; the span carries the line.
/// proc-macro2 columns are 0-based — report 1-based, matching every editor.
fn located_parse_error(e: &syn::Error) -> String {
    let start = e.span().start();
    format!(
        "parse error at line {}, column {}: {e}",
        start.line,
        start.column + 1
    )
}

/// Check whether moving from `old` config to `new` config is a *retarget* — i.e.
/// whether any `#[create]` (create-time-immutable) field changed. The apply layer
/// calls this against the recorded config before reconciling. Config values are
/// host-free, so this is independent of the platform host type.
pub fn retarget_check<H>(src: &str, old: &Value<H>, new: &Value<H>) -> Result<(), Vec<String>> {
    let file = syn::parse_file(src).map_err(|e| vec![located_parse_error(&e)])?;
    let adm = admission::extract(&file);
    admission::check_retarget(&adm, old, new).map_err(|e| vec![e.msg])
}

/// Evaluate just `deployment(cfg, cx)` against a (possibly operator-edited)
/// config value, unwrapping the return host into the platform's deployment.
pub fn eval_deployment<B: HostBridge>(
    interp: &eval::Interp<B>,
    cfg: Value<B::Host>,
) -> Result<B::Output, EvalError> {
    let cx_host = interp.bridge.cx_host(interp.cx);
    let dep_val = interp.eval_fn("deployment", vec![cfg, Value::Host(cx_host)])?;
    match dep_val {
        Value::Host(h) => interp.bridge.finish(h),
        other => Err(EvalError::new(format!(
            "deployment() must return a Deployment, got {other:?}"
        ))),
    }
}
