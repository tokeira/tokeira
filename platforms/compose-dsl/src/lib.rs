//! The `compose-dsl` platform: a compose deployment whose infra+services are
//! defined by a platform-DSL **deployment definition** rather than compiled
//! `ComposeConfig` + module/service Rust.
//!
//! This crate is the seam between the generic compiler (`tokeira-platform-dsl`)
//! and the concrete compose resource kinds (`tokeira-compose`). It does two
//! things:
//!
//! 1. **Compile** a deployment definition (`<deployment_dir>/compose.platform`)
//!    through the full pipeline — lex → parse → resolve → type-check → evaluate
//!    — against the compose kind library and an injected [`RuntimeContext`],
//!    yielding the neutral [`Composition`] IR.
//! 2. **Translate** that IR into concrete [`ComposeService`]s — the containers
//!    the deploy engine reconciles.
//!
//! It deliberately reuses the existing compose resource kinds; the DSL only
//! changes *how the composition is described*, never how a kind behaves. The
//! `Deployment`/`Ops` trait wiring and `tkr` registration build on this core in
//! the next slice.
//!
//! Multi-file `use` import assembly (Requirement 13) is wired through
//! [`compile_deployment`]: the deployment directory is assembled into a
//! path-sorted, digest-bearing [`SourceSet`] (with fail-closed import
//! containment) and composed into one program before the resolve→type-check→
//! evaluate phases run. [`compile_source`] keeps the single-source path for
//! tests and callers that already hold the text.

use std::{collections::HashMap, path::Path};

use thiserror::Error;
use tokeira_compose::ComposeService;
use tokeira_platform_dsl::{
    Composition, Diag, ItemRole, KindLibrary, RuntimeContext, SourceSet, Value, assemble,
    compose_program, evaluate, lex, parse, resolve, typeck, value::LoweredItem,
};

pub mod authored;
pub mod deployment;

/// The root definition file within a deployment directory.
pub const ROOT_DEFINITION: &str = "compose.platform";

/// Errors from compiling or translating a compose-dsl deployment definition.
#[derive(Debug, Error)]
pub enum DslError {
    /// Compilation produced one or more error diagnostics. Import/read failures
    /// surface here too: assembly reports an unreadable or out-of-bounds file as
    /// a located diagnostic rather than a distinct I/O error.
    #[error("deployment definition failed to compile:\n{0}")]
    Compile(String),
    /// The compiled composition could not be translated to compose services.
    #[error("translation error: {0}")]
    Translate(String),
}

/// Compile the deployment definition under `deployment_dir` into the neutral
/// [`Composition`] IR, against `ctx`.
///
/// Assembles the full `.platform` file set — the `compose.platform` root plus
/// every file reachable by contained `use` imports — into a path-sorted
/// [`SourceSet`], composes it into one program, and runs the resolve→type-check
/// →evaluate phases. Stops at the first phase that produces error diagnostics,
/// so no partial composition is returned (Property 6). The result is
/// deterministic given the file set and `ctx` (Property 2 / Property 19).
pub fn compile_deployment(
    deployment_dir: &Path,
    ctx: &RuntimeContext,
) -> Result<Composition, DslError> {
    // Import assembly is the only filesystem access and is fail-closed: a
    // containment, cycle, or bound violation aborts here with located
    // diagnostics rendered against the partial composed source (Requirement 13).
    let set = assemble(deployment_dir, ROOT_DEFINITION).map_err(|err| {
        DslError::Compile(render_diagnostics(&err.composed_source, &err.diagnostics))
    })?;
    compile_set(&set, ctx)
}

/// Compile an already-assembled [`SourceSet`] into the [`Composition`] IR.
///
/// Composes the path-sorted file set into one program, then runs
/// resolve→type-check→evaluate against the composed virtual source so every
/// diagnostic locates correctly across files.
pub fn compile_set(set: &SourceSet, ctx: &RuntimeContext) -> Result<Composition, DslError> {
    let source = set.composed_source();
    let (program, compose_diags) = compose_program(set);
    fail_if_errors(source, &compose_diags)?;
    let program = program.ok_or_else(|| {
        DslError::Compile("not a deployment definition (missing `platform` header)".into())
    })?;
    compile_checked(source, program, ctx)
}

/// Compile a definition from an in-memory single source string (used by tests
/// and callers that hold one file's text rather than a deployment directory).
pub fn compile_source(source: &str, ctx: &RuntimeContext) -> Result<Composition, DslError> {
    let (tokens, lex_diags) = lex(source);
    fail_if_errors(source, &lex_diags)?;

    let (program, parse_diags) = parse(&tokens, source.len());
    fail_if_errors(source, &parse_diags)?;
    let program = program.ok_or_else(|| {
        DslError::Compile("not a deployment definition (missing `platform` header)".into())
    })?;

    compile_checked(source, program, ctx)
}

/// Run the resolve→type-check→evaluate phases over a parsed program, rendering
/// any diagnostic against `source`. Shared by the single-source and assembled
/// paths so both report identically.
fn compile_checked(
    source: &str,
    program: tokeira_platform_dsl::ast::Program,
    ctx: &RuntimeContext,
) -> Result<Composition, DslError> {
    let kinds = KindLibrary::compose();
    fail_if_errors(source, &resolve(&program, &kinds))?;
    fail_if_errors(source, &typeck(&program, &kinds))?;
    evaluate(&program, ctx).map_err(|diags| DslError::Compile(render_diagnostics(source, &diags)))
}

/// Translate a compiled [`Composition`] into the compose services it describes,
/// in module-then-declaration order.
///
/// Every `ComposeService`-kind item (role [`ItemRole::Service`]) becomes a
/// [`ComposeService`]; its `depends_on` edges carry over verbatim. Non-service
/// items (infra resources, images) are handled by the infra/image translation
/// (next slice) and are skipped here.
pub fn translate_services(composition: &Composition) -> Result<Vec<ComposeService>, DslError> {
    let mut services = Vec::new();
    for module in &composition.modules {
        for item in &module.items {
            if item.role == ItemRole::Service && item.kind == "ComposeService" {
                services.push(compose_service_from(item)?);
            }
        }
    }
    Ok(services)
}

/// Build a [`ComposeService`] from a service-kind [`LoweredItem`].
pub(crate) fn compose_service_from(item: &LoweredItem) -> Result<ComposeService, DslError> {
    let where_ = || format!("service `{}`", item.id);
    Ok(ComposeService {
        name: item.id.clone(),
        image: required_str(item, "image").map_err(|e| translate(&where_(), e))?,
        ports: optional_str_list(item, "ports").map_err(|e| translate(&where_(), e))?,
        volumes: optional_str_list(item, "volumes").map_err(|e| translate(&where_(), e))?,
        environment: optional_str_map(item, "env").map_err(|e| translate(&where_(), e))?,
        command: optional_str_list(item, "command").map_err(|e| translate(&where_(), e))?,
        depends_on: item.depends_on.clone(),
        healthcheck: None,
    })
}

/// A required string field, surfaced as a translation [`DslError`].
pub(crate) fn required_str_field(item: &LoweredItem, field: &str) -> Result<String, DslError> {
    required_str(item, field).map_err(|e| translate(&format!("`{}`", item.id), e))
}

/// An optional string field (absent → `None`).
pub(crate) fn optional_str_field(item: &LoweredItem, field: &str) -> Option<String> {
    match item.fields.get(field) {
        Some(Value::Str(s)) | Some(Value::Path(s)) => Some(s.clone()),
        _ => None,
    }
}

/// A required `u16` field (e.g. a port), surfaced as a translation [`DslError`].
pub(crate) fn required_u16_field(item: &LoweredItem, field: &str) -> Result<u16, DslError> {
    match item.fields.get(field) {
        Some(Value::Int(n)) if (0..=u16::MAX as i64).contains(n) => Ok(*n as u16),
        Some(_) => Err(translate(
            &format!("`{}`", item.id),
            format!("field `{field}` must be a port (0..=65535)"),
        )),
        None => Err(translate(
            &format!("`{}`", item.id),
            format!("missing required field `{field}`"),
        )),
    }
}

/// The `replicas` field of a service item, defaulting to 1.
pub(crate) fn replicas_of(item: &LoweredItem) -> u32 {
    match item.fields.get("replicas") {
        Some(Value::Int(n)) if *n >= 0 => *n as u32,
        _ => 1,
    }
}

// ── Value extraction ──────────────────────────────────────────────────

fn required_str(item: &LoweredItem, field: &str) -> Result<String, String> {
    match item.fields.get(field) {
        Some(value) => as_str(value, field),
        None => Err(format!("missing required field `{field}`")),
    }
}

fn optional_str_list(item: &LoweredItem, field: &str) -> Result<Vec<String>, String> {
    match item.fields.get(field) {
        None | Some(Value::Absent) => Ok(Vec::new()),
        Some(Value::List(items)) => items.iter().map(|v| as_str(v, field)).collect(),
        Some(_) => Err(format!("field `{field}` must be a list of strings")),
    }
}

fn optional_str_map(item: &LoweredItem, field: &str) -> Result<HashMap<String, String>, String> {
    match item.fields.get(field) {
        None | Some(Value::Absent) => Ok(HashMap::new()),
        Some(Value::Record(map)) => map
            .iter()
            .map(|(key, value)| Ok((key.clone(), as_str(value, field)?)))
            .collect(),
        Some(_) => Err(format!("field `{field}` must be a record of strings")),
    }
}

fn as_str(value: &Value, field: &str) -> Result<String, String> {
    match value {
        Value::Str(s) | Value::Path(s) => Ok(s.clone()),
        // An output reference is a deferred value the engine resolves at apply;
        // compose service fields require a concrete string at translate time.
        Value::Output(_) => Err(format!(
            "field `{field}` is an output reference; compose services need a concrete value"
        )),
        other => Err(format!("field `{field}` must be a string, got {other:?}")),
    }
}

fn translate(where_: &str, message: String) -> DslError {
    DslError::Translate(format!("{where_}: {message}"))
}

// ── Diagnostics ─────────────────────────────────────────────────────────

fn fail_if_errors(source: &str, diags: &[Diag]) -> Result<(), DslError> {
    if diags.iter().any(Diag::is_error) {
        Err(DslError::Compile(render_diagnostics(source, diags)))
    } else {
        Ok(())
    }
}

/// Render diagnostics as `line:col: message` lines. A richer `ariadne`
/// rendering can replace this without changing the contract.
fn render_diagnostics(source: &str, diags: &[Diag]) -> String {
    diags
        .iter()
        .map(|diag| {
            let (line, col) = line_col(source, diag.span.start);
            let hint = diag
                .hint
                .as_ref()
                .map(|h| format!(" (hint: {h})"))
                .unwrap_or_default();
            format!("{line}:{col}: {}{hint}", diag.message)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn line_col(source: &str, offset: usize) -> (usize, usize) {
    let mut line = 1;
    let mut col = 1;
    for (index, ch) in source.char_indices() {
        if index >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    (line, col)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> RuntimeContext {
        RuntimeContext {
            deployment_dir: "/dep".into(),
            home: "/home/u".into(),
            region: "eu-west-2".into(),
        }
    }

    const DEFINITION: &str = r#"platform compose {
        input storage: Storage = InMemory

        let state_dir  = ctx.deployment_dir / ".tokeira-state"
        let config_dir = ctx.deployment_dir / "config"

        module local_state { resource state_dir_res = LocalStateDir { } }

        module observability {
            depends_on [ local_state ]
            service mimir = ComposeService {
                image: "grafana/mimir:3.0.6",
                ports: [ "9009:9009" ],
                volumes: [ bind(state_dir / "mimir", "/data", rw) ],
                command: [ "--config.file=/etc/mimir/mimir.yaml" ],
            }
            service grafana = ComposeService {
                image: "grafana/grafana-oss:12.4.3",
                ports: [ port(3000) ],
                env: { "GF_SECURITY_ADMIN_USER": "admin" },
                depends_on: [ mimir ],
            }
        }

        module runtime {
            depends_on [ observability ]
            service tokeirad = ComposeService {
                image: "tokeirad:latest",
                ports: [ port(7233), port(9090) ],
                volumes: [ bind(ctx.deployment_dir / "tokeirad.toml", "/etc/tokeira/tokeirad.toml", ro) ],
                env: { "TOKEIRA_CONFIG": "/etc/tokeira/tokeirad.toml" },
            }
        }
    }"#;

    #[test]
    fn compiles_and_translates_definition_to_compose_services() {
        let composition = compile_source(DEFINITION, &ctx()).expect("compiles");
        let services = translate_services(&composition).expect("translates");

        let names: Vec<&str> = services.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["mimir", "grafana", "tokeirad"]);

        let tokeirad = services.iter().find(|s| s.name == "tokeirad").unwrap();
        assert_eq!(tokeirad.image, "tokeirad:latest");
        assert_eq!(tokeirad.ports, vec!["7233:7233", "9090:9090"]);
        assert_eq!(
            tokeirad.volumes,
            vec!["/dep/tokeirad.toml:/etc/tokeira/tokeirad.toml:ro"]
        );
        assert_eq!(
            tokeirad
                .environment
                .get("TOKEIRA_CONFIG")
                .map(String::as_str),
            Some("/etc/tokeira/tokeirad.toml")
        );

        let grafana = services.iter().find(|s| s.name == "grafana").unwrap();
        assert_eq!(grafana.ports, vec!["3000:3000"]);
        assert_eq!(grafana.depends_on, vec!["mimir"]);

        let mimir = services.iter().find(|s| s.name == "mimir").unwrap();
        assert_eq!(mimir.volumes, vec!["/dep/.tokeira-state/mimir:/data"]);
    }

    #[test]
    fn compile_error_is_reported_with_location() {
        // `image` is required on ComposeService; omit it.
        let bad = r#"platform compose {
            module m { service s = ComposeService { ports: [ port(1) ] } }
        }"#;
        let err = compile_source(bad, &ctx()).expect_err("should fail to compile");
        let DslError::Compile(message) = err else {
            panic!("expected a compile error, got {err:?}");
        };
        assert!(
            message.contains("missing required field `image`"),
            "got: {message}"
        );
    }

    #[test]
    fn reads_definition_from_a_deployment_directory() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(ROOT_DEFINITION), DEFINITION).unwrap();
        let composition = compile_deployment(dir.path(), &ctx()).expect("compiles from dir");
        let services = translate_services(&composition).expect("translates");
        assert_eq!(services.len(), 3);
    }

    #[test]
    fn composes_a_multi_file_definition_across_imports() {
        // The same deployment split across files: the root holds inputs + shared
        // lets + `use`s; each module lives in its own file. Whole-program name
        // resolution lets `runtime` depend on `observability` and reference the
        // root's `state_dir` (Requirement 13).
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(ROOT_DEFINITION),
            r#"platform compose {
                use "observability.platform"
                use "runtime.platform"
                let state_dir = ctx.deployment_dir / ".tokeira-state"
                module local_state { resource state_dir_res = LocalStateDir { } }
            }"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("observability.platform"),
            r#"platform compose {
                module observability {
                    depends_on [ local_state ]
                    service mimir = ComposeService {
                        image: "grafana/mimir:3.0.6",
                        volumes: [ bind(state_dir / "mimir", "/data", rw) ],
                    }
                }
            }"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("runtime.platform"),
            r#"platform compose {
                module runtime {
                    depends_on [ observability ]
                    service tokeirad = ComposeService {
                        image: "tokeirad:latest",
                        ports: [ port(7233) ],
                    }
                }
            }"#,
        )
        .unwrap();

        let composition = compile_deployment(dir.path(), &ctx()).expect("compiles multi-file");
        let services = translate_services(&composition).expect("translates");
        let names: Vec<&str> = services.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["mimir", "tokeirad"]);
        let mimir = services.iter().find(|s| s.name == "mimir").unwrap();
        assert_eq!(mimir.volumes, vec!["/dep/.tokeira-state/mimir:/data"]);
    }

    #[test]
    fn import_containment_violation_fails_compilation() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(ROOT_DEFINITION),
            r#"platform compose { use "../escape.platform" }"#,
        )
        .unwrap();
        let err = compile_deployment(dir.path(), &ctx()).expect_err("must fail closed");
        let DslError::Compile(message) = err else {
            panic!("expected a compile error, got {err:?}");
        };
        assert!(message.contains("must not contain `..`"), "got: {message}");
    }
}
