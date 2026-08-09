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
pub mod parts;
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
    part_sources: &dyn tokeira_platform::definition::SourceResolver,
) -> Result<(B::Output, Value<B::Host>), EvalError> {
    let file = syn::parse_file(src).map_err(|e| EvalError::new(located_parse_error(&e)))?;
    let (types, fns) = schema::collect(&file)?;
    // The load owns everything pre-evaluation: part resolution, `use`
    // validation and acyclicity, every document's subset pass against its
    // effective types, and the set's merged admission.
    let scopes = parts::load(&file, types, fns, bridge, part_sources)?;
    let interp = eval::Interp {
        bridge,
        scope: eval::Scope::root(&scopes),
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
    admission::check_requires(&interp, &scopes.admission, &cfg)?;
    let dep = eval_deployment(&interp, cfg.clone())?;
    Ok((dep, cfg))
}

/// Validate a `.tkd` against the interpreted subset, returning all violation
/// messages (no parse/eval). The allow-list is reject-by-default.
pub fn validate<B: HostBridge>(
    src: &str,
    bridge: &B,
    part_sources: &dyn tokeira_platform::definition::SourceResolver,
) -> Result<(), Vec<String>> {
    let file = syn::parse_file(src).map_err(|e| vec![located_parse_error(&e)])?;
    let (types, fns) = schema::collect(&file).map_err(|e| vec![e.msg])?;
    parts::load(&file, types, fns, bridge, part_sources)
        .map(|_| ())
        .map_err(|e| vec![e.msg])
}

/// Locate a parse failure for the operator: syn's message alone ("expected
/// `,`") is useless in a definition of any size; the span carries the line.
/// proc-macro2 columns are 0-based — report 1-based, matching every editor.
pub(crate) fn located_parse_error(e: &syn::Error) -> String {
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
///
/// `#[create]` may sit in any document of the set — a model part carries the
/// configuration types — so the annotation scan resolves the root's declared
/// parts and reads the whole set (a parse-only pass; nothing evaluates).
pub fn retarget_check<H>(
    src: &str,
    part_sources: &dyn tokeira_platform::definition::SourceResolver,
    old: &Value<H>,
    new: &Value<H>,
) -> Result<(), Vec<String>> {
    let file = syn::parse_file(src).map_err(|e| vec![located_parse_error(&e)])?;
    let mut adm = admission::extract(&file);
    for item in &file.items {
        let syn::Item::Mod(declared) = item else {
            continue;
        };
        let name = declared.ident.to_string();
        let bytes = part_sources
            .resolve(&name)
            .map_err(|e| vec![e.to_string()])?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| vec![format!("{name}.tkd: part source is not UTF-8: {e}")])?;
        let part_file = syn::parse_file(text)
            .map_err(|e| vec![format!("{name}.tkd: {}", located_parse_error(&e))])?;
        let part_adm = admission::extract(&part_file);
        adm.creates.extend(part_adm.creates);
        adm.requires.extend(part_adm.requires);
    }
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
