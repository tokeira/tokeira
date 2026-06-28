//! The canonical compose `.platform` definition shipped in this crate (task 7.3).
//!
//! This module embeds the platform author's structural artifact — the `.platform`
//! file set under `platform/` — and exposes the create-time hand-off that writes
//! it onto a deployment root. It is the DSL analog of the compiled compose
//! platform (`platforms/compose/src/{config,compose,modules,services,images,
//! observability_config}.rs`): the same modules, services, DSQL infra, images,
//! and writeback, expressed as a deployment definition rather than Rust.
//!
//! Ownership boundary (Req 16): the definition is authored here, in the platform
//! crate, not generated for the operator. `tkr deployment create` persists this
//! set into the deployment ([`write_authored_definition`] is that hand-off, used
//! by task 11.2); every subsequent apply compiles the *persisted* copy, not this
//! embedded one (Req 16.3 / Property 23). The persistence/retention contract
//! itself (storage, versioning against the compiling `tkp`, rollback) belongs to
//! the `platform-provisioner-binary` (tkp) spec.

use std::{io, path::Path};

/// The authored definition as `(relative_path, contents)` pairs, embedded at
/// compile time.
///
/// The root `compose.platform` `use`s the others; the order here is irrelevant
/// because import assembly composes the set in path-sorted order (Req 13.4).
/// Listing every file (not just the root) is what lets the create-time hand-off
/// materialize the whole set without re-reading the crate at apply time.
pub const AUTHORED_DEFINITION: &[(&str, &str)] = &[
    (
        "compose.platform",
        include_str!("../platform/compose.platform"),
    ),
    (
        "images.platform",
        include_str!("../platform/images.platform"),
    ),
    ("infra.platform", include_str!("../platform/infra.platform")),
    (
        "observability.platform",
        include_str!("../platform/observability.platform"),
    ),
    (
        "runtime.platform",
        include_str!("../platform/runtime.platform"),
    ),
];

/// Write the authored definition onto `deployment_dir` — the create-time hand-off
/// (task 11.2).
///
/// Each file is written verbatim at the deployment root (depth 0, within the
/// Req 13.3 layout). The provisioner records the resulting `(relative_path,
/// sha256)` set and its digest; that retention contract is owned by the
/// `platform-provisioner-binary` spec, not here. Returns the first I/O error.
pub fn write_authored_definition(deployment_dir: &Path) -> io::Result<()> {
    for (rel, contents) in AUTHORED_DEFINITION {
        std::fs::write(deployment_dir.join(rel), contents)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_platform_dsl::{
        KindLibrary, RuntimeContext, Value, assemble, compose_program, evaluate_with_inputs,
        resolve, typeck,
    };

    use crate::{ROOT_DEFINITION, compile_deployment, translate_services};

    fn ctx() -> RuntimeContext {
        RuntimeContext {
            deployment_dir: "/dep".into(),
            home: "/home/u".into(),
            region: "eu-west-2".into(),
        }
    }

    /// The authored multi-file definition compiles and, under the default
    /// InMemory storage, lowers to the same composition the compiled compose
    /// platform produces: dsql absent, the four observability services + tokeirad
    /// in module-then-declaration order, with the parity volumes/ports/edges.
    #[test]
    fn authored_definition_compiles_and_matches_compose_parity() {
        let dir = tempfile::tempdir().unwrap();
        write_authored_definition(dir.path()).unwrap();

        let composition =
            compile_deployment(dir.path(), &ctx()).expect("authored definition compiles");

        // InMemory default → the conditional dsql module is absent.
        let modules: Vec<&str> = composition
            .modules
            .iter()
            .map(|m| m.name.as_str())
            .collect();
        assert_eq!(modules, vec!["local_state", "observability", "runtime"]);

        let services = translate_services(&composition).expect("translates");
        let names: Vec<&str> = services.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["mimir", "loki", "grafana", "alloy", "tokeirad"]);

        // tokeirad: grpc + metrics ports, tokeirad.toml mounted read-only.
        let tokeirad = services.iter().find(|s| s.name == "tokeirad").unwrap();
        assert_eq!(tokeirad.image, "tokeirad:latest");
        assert_eq!(tokeirad.ports, vec!["7233:7233", "9090:9090"]);
        assert_eq!(
            tokeirad.volumes,
            vec!["/dep/tokeirad.toml:/etc/tokeira/tokeirad.toml:ro"]
        );

        // mimir volumes mirror `compose_services` (state + config + rules mounts).
        let mimir = services.iter().find(|s| s.name == "mimir").unwrap();
        assert_eq!(
            mimir.volumes,
            vec![
                "/dep/.tokeira-state/mimir:/data",
                "/dep/config/mimir.yaml:/etc/mimir/mimir.yaml",
                "/dep/config/mimir/rules:/data/mimir/rules",
            ]
        );

        // grafana depends on the config-files resource then mimir + loki.
        let grafana = services.iter().find(|s| s.name == "grafana").unwrap();
        assert_eq!(grafana.ports, vec!["3000:3000"]);
        assert_eq!(
            grafana.depends_on,
            vec!["observability_config", "mimir", "loki"]
        );
    }

    /// Parity must hold in *both* storage modes (Property 13). The compile phases
    /// (resolve + type-check) are mode-independent and validate the Dsql arms
    /// structurally; here we also *evaluate* under a Dsql payload so the dsql
    /// module, its writeback output-refs, and the tokeirad `aws_auth`/`AWS_REGION`
    /// arms are exercised rather than skipped.
    #[test]
    fn authored_definition_compiles_under_dsql_storage() {
        let dir = tempfile::tempdir().unwrap();
        write_authored_definition(dir.path()).unwrap();

        let set = assemble(dir.path(), ROOT_DEFINITION).expect("assembles");
        let (program, diags) = compose_program(&set);
        assert!(diags.is_empty(), "compose diags: {diags:?}");
        let program = program.expect("program");

        let kinds = KindLibrary::compose();
        assert!(
            resolve(&program, &kinds).is_empty(),
            "resolve diags: {:?}",
            resolve(&program, &kinds)
        );
        assert!(
            typeck(&program, &kinds).is_empty(),
            "typeck diags: {:?}",
            typeck(&program, &kinds)
        );

        // Supply a Dsql storage payload (the managed-cluster shape an operator
        // would provide at create), then evaluate.
        let mut payload = std::collections::BTreeMap::new();
        payload.insert("mode".to_string(), Value::Str("Managed".into()));
        payload.insert("region".to_string(), Value::Str("eu-west-2".into()));
        let mut inputs = std::collections::HashMap::new();
        inputs.insert(
            "storage".to_string(),
            Value::Variant {
                name: "Dsql".into(),
                payload: Some(Box::new(Value::Record(payload))),
            },
        );

        let composition =
            evaluate_with_inputs(&program, &ctx(), &inputs).expect("dsql composition");

        // The dsql module is now present, ahead of observability + runtime.
        let modules: Vec<&str> = composition
            .modules
            .iter()
            .map(|m| m.name.as_str())
            .collect();
        assert_eq!(
            modules,
            vec!["local_state", "dsql", "observability", "runtime"]
        );

        // Writeback resolves to the three output references (endpoint + tables).
        let keys: Vec<&str> = composition
            .writeback
            .iter()
            .map(|w| w.key.as_str())
            .collect();
        assert_eq!(
            keys,
            vec![
                "infrastructure.dsql.endpoint",
                "infrastructure.dsql.rate_limiter_table",
                "infrastructure.dsql.conn_lease_table",
            ]
        );
    }

    /// The embedded set is self-contained: the root is present and every `use`
    /// target it references is embedded too (so the create-time hand-off writes a
    /// definition that assembles without a missing-file error).
    #[test]
    fn authored_definition_file_set_is_self_contained() {
        let dir = tempfile::tempdir().unwrap();
        write_authored_definition(dir.path()).unwrap();
        let set = assemble(dir.path(), ROOT_DEFINITION).expect("assembles");
        // Five files: root + four `use`d modules.
        assert_eq!(set.files().len(), 5);
    }
}
