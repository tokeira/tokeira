//! `tokeira-tkd` — the platform-agnostic `syn` deployment-definition interpreter.
//!
//! Parses a `.tkd` (Rust syntax, via `syn`), enforces the interpreted subset
//! (reject-by-default allow-list), and walks it into the platform's deployment
//! type. It is generic over a platform-supplied [`HostBridge`]: the core holds
//! host values opaquely and routes every host operation through the bridge, so it
//! names no concrete kind and needs no `Box<dyn Any>`. `compose-syn` and `eks`
//! each implement one bridge and share this interpreter (Proposals 003/004).
//!
//! The passes: [`schema`] (type/fn tables + `#[create]`/`#[require]`), [`subset`]
//! (the allow-list), [`eval`] (the tree walk), [`admission`] (retarget + require).
//! Only the *platform's* bridge names platform types; every module here is
//! engine-agnostic.

pub mod admission;
pub mod bridge;
pub mod eval;
pub mod schema;
pub mod subset;
pub mod value;

pub use bridge::HostBridge;
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
    let file = syn::parse_file(src).map_err(|e| EvalError::new(format!("parse error: {e}")))?;
    let (types, fns) = schema::collect(&file)?;
    subset::check(&file, bridge, &types).map_err(|d| {
        EvalError::new(format!(
            "definition is outside the interpreted subset:\n{d}"
        ))
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
    let file = syn::parse_file(src).map_err(|e| vec![format!("parse error: {e}")])?;
    let (types, _fns) = schema::collect(&file).map_err(|e| vec![e.msg])?;
    subset::check(&file, bridge, &types).map_err(Diagnostics::into_messages)
}

/// Check whether moving from `old` config to `new` config is a *retarget* — i.e.
/// whether any `#[create]` (create-time-immutable) field changed. The apply layer
/// calls this against the recorded config before reconciling. Config values are
/// host-free, so this is independent of the platform host type.
pub fn retarget_check<H>(src: &str, old: &Value<H>, new: &Value<H>) -> Result<(), Vec<String>> {
    let file = syn::parse_file(src).map_err(|e| vec![format!("parse error: {e}")])?;
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
